"""
dbt helper utilities.

Provides:
- `DBTCLI` : lightweight wrapper to locate and run the `dbt` CLI.
- `DBTProject` : helper for loading a dbt project (dbt_project.yml).
- `DBTManifest` : parser and lightweight accessor for `manifest.json`.

Design goals:
- Keep API small and composable (composition over inheritance).
- Safe, clear error messages and docstrings for discoverability.
- Consistent JSON-serializable return values for manifest accessors.

Example:
    from dbtui.dbt_client import DBTCLI, DBTProject

    cli = DBTCLI()  # will try to find `dbt` on PATH
    project = DBTProject(project_path="./jaffle_shop_duckdb/")
    payload = project.get_manifest_json(include="model")

    # run `dbt compile`
    result = cli.run(["compile"], cwd=project.project_path, timeout=30_000)
    print(result.returncode, result.stdout)

    # async run with streaming (for TUI integration)
    async for event in cli.run_async(["build", "-s", "my_model"], cwd=project.project_path):
        print(event)  # DBTStreamEvent with .stream ("stdout"/"stderr"/"status") and .text
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Union

import yaml
from dbt_artifacts_parser.parser import parse_manifest

__all__ = ["DBTCLI", "DBTProject", "DBTManifest", "DBTCommand", "DBTStreamEvent"]


@dataclass
class DBTRunResult:
    """
    Simple wrapper for subprocess results from running a dbt CLI command.

    Attributes:
        returncode: Process exit code.
        stdout: Captured standard output (text).
        stderr: Captured standard error (text).
        command: The full command that was run.
    """

    returncode: int
    stdout: str
    stderr: str
    command: List[str]


class DBTCommand(Enum):
    """Supported dbt commands that can be run against a selected node."""

    BUILD = "build"
    BUILD_UPSTREAM = "build_upstream"
    BUILD_DOWNSTREAM = "build_downstream"
    BUILD_FULL = "build_full"
    COMPILE = "compile"
    TEST = "test"
    RUN = "run"
    SHOW = "show"

    @property
    def display_name(self) -> str:
        """Human-readable label for the command."""
        return _COMMAND_LABELS.get(self, self.value)

    def to_args(self, node_name: str) -> List[str]:
        """Convert command + node name into a list of dbt CLI arguments.

        Args:
            node_name: The dbt node name (e.g. ``"my_model"``).

        Returns:
            A list of strings suitable for passing to ``DBTCLI.run``.
        """
        if self == DBTCommand.BUILD:
            return ["build", "--select", node_name]
        if self == DBTCommand.BUILD_UPSTREAM:
            return ["build", "--select", f"+{node_name}"]
        if self == DBTCommand.BUILD_DOWNSTREAM:
            return ["build", "--select", f"{node_name}+"]
        if self == DBTCommand.BUILD_FULL:
            return ["build", "--select", f"+{node_name}+"]
        if self == DBTCommand.COMPILE:
            return ["compile", "--select", node_name]
        if self == DBTCommand.TEST:
            return ["test", "--select", node_name]
        if self == DBTCommand.RUN:
            return ["run", "--select", node_name]
        if self == DBTCommand.SHOW:
            return [
                "show",
                "--select",
                node_name,
                "--output",
                "json",
                "--log-format",
                "json",
            ]
        # No fallback — every variant must be handled explicitly.
        raise ValueError(f"Unhandled command: {self}")

    @classmethod
    def for_resource_type(cls, resource_type: str) -> List["DBTCommand"]:
        """Return the list of applicable commands for a given dbt resource type.

        Args:
            resource_type: One of ``"model"``, ``"seed"``, ``"source"``, etc.

        Returns:
            Ordered list of ``DBTCommand`` members that make sense for the type.
        """
        if resource_type == "model":
            return [
                cls.BUILD,
                cls.BUILD_UPSTREAM,
                cls.BUILD_DOWNSTREAM,
                cls.BUILD_FULL,
                cls.COMPILE,
                cls.RUN,
                cls.TEST,
                cls.SHOW,
            ]
        if resource_type == "seed":
            return [cls.BUILD, cls.TEST, cls.SHOW]
        if resource_type == "source":
            return [cls.TEST]
        # Default: offer build and compile
        return [cls.BUILD, cls.COMPILE]


_COMMAND_LABELS = {
    DBTCommand.BUILD: "Build",
    DBTCommand.BUILD_UPSTREAM: "Build +upstream",
    DBTCommand.BUILD_DOWNSTREAM: "Build downstream+",
    DBTCommand.BUILD_FULL: "Build +full+",
    DBTCommand.COMPILE: "Compile",
    DBTCommand.TEST: "Test",
    DBTCommand.RUN: "Run",
    DBTCommand.SHOW: "Show",
}


@dataclass
class DBTStreamEvent:
    """A single event emitted during async dbt command execution.

    Attributes:
        stream: One of ``"stdout"``, ``"stderr"``, or ``"status"``.
                ``"status"`` is used for lifecycle messages (started / finished / error).
        text: The line of text.
        exit_code: Only set on the final status event.
    """

    stream: str  # "stdout" | "stderr" | "status"
    text: str
    exit_code: int | None = None  # only set on the final status event


class DBTCLI:
    """
    Wrapper for invoking the `dbt` command-line binary.

    This class locates the `dbt` executable by default using `shutil.which("dbt")`.
    You can override the path by passing `path="..."`.

    Methods:
        run(args, cwd=None, env=None, timeout=None, capture_output=True):
            Run the dbt command with the provided arguments (synchronous).
        run_async(args, cwd=None, env=None):
            Run a dbt command asynchronously, yielding DBTStreamEvent objects
            as stdout/stderr lines arrive.  Ideal for TUI integration.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        if path:
            self.path = str(Path(path).resolve())
        else:
            self.path = shutil.which("dbt")

    def available(self) -> bool:
        """
        Return True if the dbt binary path is known / available.
        """
        return bool(self.path)

    def run(
        self,
        args: List[str],
        cwd: Optional[Union[str, os.PathLike[str]]] = None,
        env: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        capture_output: bool = True,
    ) -> DBTRunResult:
        """
        Run a dbt CLI command (synchronous / blocking).

        Args:
            args: list of CLI arguments (e.g. ["compile", "--models", "my_model"])
            cwd: working directory to run dbt in (defaults to current process cwd)
            env: extra environment variables (merged with os.environ)
            timeout: seconds to wait before killing the process
            capture_output: whether to capture stdout/stderr

        Returns:
            DBTRunResult containing returncode, stdout, stderr, and the full command.

        Raises:
            FileNotFoundError: if the dbt binary cannot be located.
        """
        if not self.available():
            raise FileNotFoundError(
                "dbt binary not found. Please install dbt or provide a path to DBTCLI."
            )

        assert self.path is not None
        cmd = [self.path] + list(args)
        run_env = os.environ.copy()
        if env:
            run_env.update(env)

        completed = subprocess.run(
            cmd,
            cwd=cwd,
            env=run_env,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.PIPE if capture_output else None,
            timeout=timeout,
            text=True,
        )

        stdout = completed.stdout if completed.stdout is not None else ""
        stderr = completed.stderr if completed.stderr is not None else ""

        return DBTRunResult(
            returncode=completed.returncode, stdout=stdout, stderr=stderr, command=cmd
        )

    async def run_async(
        self,
        args: List[str],
        cwd: Optional[Union[str, os.PathLike[str]]] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> AsyncIterator[DBTStreamEvent]:
        """Run a dbt CLI command asynchronously, streaming output line-by-line.

        Yields ``DBTStreamEvent`` objects as lines are produced:
          - ``stream="status"`` for lifecycle messages (command started / finished).
          - ``stream="stdout"`` for each stdout line.
          - ``stream="stderr"`` for each stderr line.

        This is designed for consumption by a Textual worker so the TUI stays
        responsive while the command executes.

        Args:
            args: CLI arguments (e.g. ``["build", "--select", "my_model"]``).
            cwd: Working directory.
            env: Extra environment variables.

        Yields:
            DBTStreamEvent instances.

        Raises:
            FileNotFoundError: if the dbt binary cannot be located.
        """
        if not self.available():
            raise FileNotFoundError(
                "dbt binary not found. Please install dbt or provide a path to DBTCLI."
            )

        assert self.path is not None
        cmd = [self.path] + list(args)
        run_env = os.environ.copy()
        if env:
            run_env.update(env)

        cmd_str = " ".join(cmd)
        yield DBTStreamEvent(stream="status", text=f"▶ Running: {cmd_str}")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            env=run_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        queue: asyncio.Queue[DBTStreamEvent | None] = asyncio.Queue()

        async def _read_stream(stream: asyncio.StreamReader, name: str) -> None:
            while True:
                line_bytes = await stream.readline()
                if not line_bytes:
                    break
                line = line_bytes.decode("utf-8", errors="replace").rstrip("\n\r")
                await queue.put(DBTStreamEvent(stream=name, text=line))

        stdout_task = asyncio.create_task(
            _read_stream(process.stdout, "stdout")  # type: ignore[arg-type]
        )
        stderr_task = asyncio.create_task(
            _read_stream(process.stderr, "stderr")  # type: ignore[arg-type]
        )

        # Sentinel task: wait for both readers to finish, then push None
        async def _signal_done() -> None:
            await asyncio.gather(stdout_task, stderr_task)
            await queue.put(None)

        done_task = asyncio.create_task(_signal_done())

        while True:
            event = await queue.get()
            if event is None:
                break
            yield event

        await done_task

        returncode = await process.wait()

        if returncode == 0:
            yield DBTStreamEvent(
                stream="status",
                text="✔ Command completed successfully (exit 0)",
                exit_code=0,
            )
        else:
            yield DBTStreamEvent(
                stream="status",
                text=f"✘ Command failed (exit {returncode})",
                exit_code=returncode,
            )


class DBTManifest:
    """
    Parser and lightweight accessor for `manifest.json`.
    Provides JSON-serializable accessors for full/raw and parsed/native artifact data.
    """

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._native = parse_manifest(manifest=data)
        self._nodes_json_cache: Dict[str, Dict[str, Any]] | None = None
        self._sources_json_cache: Dict[str, Dict[str, Any]] | None = None
        self._macros_json_cache: Dict[str, Dict[str, Any]] | None = None

    @classmethod
    def from_file(cls, path: Path) -> "DBTManifest":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(data)

    @property
    def native(self) -> Any:
        """The underlying dbt-artifacts-parser result."""
        return self._native

    @property
    def raw(self) -> Dict[str, Any]:
        """The raw manifest payload loaded from `manifest.json`."""
        return self._data

    @property
    def nodes(self) -> Dict[str, Any]:
        """Map of node unique_id to Node object."""
        return self._native.nodes or {}

    @property
    def sources(self) -> Dict[str, Any]:
        """Map of source unique_id to Source object."""
        return self._native.sources or {}

    @property
    def macros(self) -> Dict[str, Any]:
        """Map of macro unique_id to Macro object."""
        return self._native.macros or {}

    @staticmethod
    def _to_jsonable(obj: Any) -> Any:
        """
        Convert a manifest object into JSON-serializable data.

        Handles Pydantic v1 (`dict`) and v2 (`model_dump`) style models.
        Falls back to primitives and safe string conversion.
        """
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return obj
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, dict):
            return {str(k): DBTManifest._to_jsonable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [DBTManifest._to_jsonable(v) for v in obj]

        model_dump = getattr(obj, "model_dump", None)
        if callable(model_dump):
            try:
                return model_dump(mode="json")
            except TypeError:
                return model_dump()

        dict_method = getattr(obj, "dict", None)
        if callable(dict_method):
            return dict_method()

        # Last resort: preserve structure as text instead of raising.
        return str(obj)

    def nodes_json(self) -> Dict[str, Dict[str, Any]]:
        """Return all nodes as JSON-serializable dicts."""
        if self._nodes_json_cache is None:
            self._nodes_json_cache = {
                uid: self._to_jsonable(node) for uid, node in self.nodes.items()
            }
        return self._nodes_json_cache

    def sources_json(self) -> Dict[str, Dict[str, Any]]:
        """Return all sources as JSON-serializable dicts."""
        if self._sources_json_cache is None:
            self._sources_json_cache = {
                uid: self._to_jsonable(src) for uid, src in self.sources.items()
            }
        return self._sources_json_cache

    def macros_json(self) -> Dict[str, Dict[str, Any]]:
        """Return all macros as JSON-serializable dicts."""
        if self._macros_json_cache is None:
            self._macros_json_cache = {
                uid: self._to_jsonable(macro) for uid, macro in self.macros.items()
            }
        return self._macros_json_cache


class DBTProject:
    """
    Helper to represent and interact with a dbt project on disk.

    Responsibilities:
    - locate target/manifest.json and parse it via DBTManifest
    - provide convenience helpers to return JSON-serializable manifest objects
      for all resources or filtered resource types (model/seed/source/test/macro)
    """

    VALID_INCLUDE = {"all", "model", "seed", "source", "test", "macro"}

    def __init__(self, project_path: Optional[str] = None) -> None:
        # Determine project path
        if project_path is None:
            cwd = Path.cwd()
            if (cwd / "dbt_project.yml").exists():
                self.project_path = cwd
            else:
                raise FileNotFoundError(
                    "No project_path provided and dbt_project.yml not found in cwd."
                )
        else:
            self.project_path = Path(project_path).resolve()

        self.project_yaml_file = self.project_path / "dbt_project.yml"
        if not self.project_yaml_file.exists():
            raise FileNotFoundError(
                f"dbt_project.yml not found at {self.project_yaml_file}"
            )

        self._project_yaml: Optional[Dict[str, Any]] = None
        self._manifest: Optional[DBTManifest] = None
        self._load_project_yaml()

    def _load_project_yaml(self) -> None:
        """Internal: read and parse dbt_project.yml into `_project_yaml`."""
        try:
            with self.project_yaml_file.open("r", encoding="utf-8") as fh:
                self._project_yaml = yaml.safe_load(fh) or {}
        except yaml.YAMLError as exc:
            raise ValueError(
                f"Failed to parse {self.project_yaml_file}: {exc}"
            ) from exc

    @property
    def project_yaml(self) -> Dict[str, Any]:
        """The parsed contents of `dbt_project.yml`."""
        return self._project_yaml or {}

    @property
    def project_name(self) -> Optional[str]:
        """Convenience: return the name of the project, if declared in dbt_project.yml."""
        return self.project_yaml.get("name")

    def manifest_path(self) -> Path:
        """
        Return the expected manifest.json path within the project's target directory.
        """
        return (
            self.project_path
            / self.project_yaml.get("target-path", "target")
            / "manifest.json"
        )

    def has_manifest(self) -> bool:
        """Return True if `target/manifest.json` exists."""
        return self.manifest_path().exists()

    def load_manifest(self, reload: bool = False) -> Optional[DBTManifest]:
        """
        Load and return a DBTManifest instance if the manifest exists.

        Args:
            reload: if True, force re-read even if already loaded.

        Returns:
            DBTManifest instance or None if manifest.json not present.
        """
        if self._manifest is not None and not reload:
            return self._manifest

        manifest_file = self.manifest_path()
        if not manifest_file.exists():
            return None

        self._manifest = DBTManifest.from_file(manifest_file)
        return self._manifest

    def _manifest_or_empty(self) -> Optional[DBTManifest]:
        """Internal helper to load manifest and return None if unavailable."""
        return self.load_manifest()

    @staticmethod
    def _filter_nodes_by_resource_type(
        nodes: Dict[str, Dict[str, Any]], resource_type: str
    ) -> Dict[str, Dict[str, Any]]:
        return {
            uid: node
            for uid, node in nodes.items()
            if isinstance(node, dict) and node.get("resource_type") == resource_type
        }

    def get_manifest_json(self, include: str = "all") -> Dict[str, Any]:
        """
        Return manifest data as JSON-serializable dicts.

        Args:
            include:
                - "all"   -> returns grouped payload:
                    {"nodes": ..., "sources": ..., "macros": ...}
                - "model" -> model nodes only
                - "seed"  -> seed nodes only
                - "source"-> sources only
                - "test"  -> test nodes only
                - "macro" -> macros only

        Returns:
            Dict[str, Any] JSON-serializable payload.
            Empty dict if manifest is missing.

        Raises:
            ValueError: if include is not a supported value.
        """
        include = include.lower().strip()
        if include not in self.VALID_INCLUDE:
            raise ValueError(
                f"Invalid include='{include}'. Expected one of {sorted(self.VALID_INCLUDE)}."
            )

        manifest = self._manifest_or_empty()
        if not manifest:
            return {}

        if include == "all":
            return {
                "nodes": manifest.nodes_json(),
                "sources": manifest.sources_json(),
                "macros": manifest.macros_json(),
            }
        if include == "model":
            return self._filter_nodes_by_resource_type(manifest.nodes_json(), "model")
        if include == "seed":
            return self._filter_nodes_by_resource_type(manifest.nodes_json(), "seed")
        if include == "test":
            return self._filter_nodes_by_resource_type(manifest.nodes_json(), "test")
        if include == "source":
            return manifest.sources_json()
        if include == "macro":
            return manifest.macros_json()

        return {}

    def get_nodes(self) -> Dict[str, Any]:
        """Return all nodes as JSON-serializable dicts."""
        return self.get_manifest_json(include="all").get("nodes", {})

    def get_models(self) -> Dict[str, Any]:
        """Return model nodes as JSON-serializable dicts."""
        return self.get_manifest_json(include="model")

    def get_seeds(self) -> Dict[str, Any]:
        """Return seed nodes as JSON-serializable dicts."""
        return self.get_manifest_json(include="seed")

    def get_tests(self) -> Dict[str, Any]:
        """Return test nodes as JSON-serializable dicts."""
        return self.get_manifest_json(include="test")

    def get_sources(self) -> Dict[str, Any]:
        """Return sources as JSON-serializable dicts."""
        return self.get_manifest_json(include="source")

    def get_macros(self) -> Dict[str, Any]:
        """Return macros as JSON-serializable dicts."""
        return self.get_manifest_json(include="macro")

    def get_manifest_raw_json(self) -> Dict[str, Any]:
        """
        Return the raw manifest.json payload as loaded from disk.
        Useful when you need the exact canonical artifact structure.
        """
        manifest = self._manifest_or_empty()
        if not manifest:
            return {}
        return manifest.raw
