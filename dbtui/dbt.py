"""
dbt helper utilities.

Provides:
- `DBTCLI` : lightweight wrapper to locate and run the `dbt` CLI.
- `DBTProject` : helper for loading a dbt project (dbt_project.yml).
- `DBTManifest` : parser and lightweight accessor for `manifest.json`.

Design goals:
- Keep API small and composable (composition over inheritance).
- Safe, clear error messages and docstrings for discoverability.
- Simple helpers to list models / sources and run dbt commands programmatically.

Example:
    from dbtui.dbt import DBTCLI, DBTProject

    cli = DBTCLI()  # will try to find `dbt` on PATH
    project = DBTProject(project_path="./jaffle_shop_duckdb/")
    manifest = project.load_manifest()  # returns DBTManifest or None if not present

    # run `dbt compile`
    result = cli.run(["compile"], cwd=project.project_path, timeout=30_000)
    print(result.returncode, result.stdout)
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from dbt_artifacts_parser.parser import parse_manifest

__all__ = ["DBTCLI", "DBTProject", "DBTManifest"]


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


class DBTCLI:
    """
    Wrapper for invoking the `dbt` command-line binary.

    This class locates the `dbt` executable by default using `shutil.which("dbt")`.
    You can override the path by passing `path="..."`.

    Methods:
        run(args, cwd=None, env=None, timeout=None, capture_output=True):
            Run the dbt command with the provided arguments.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        if path:
            self.path = path
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
        Run a dbt CLI command.

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


class DBTManifest:
    """
    Parser and lightweight accessor for `manifest.json`.
    """

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._native = parse_manifest(manifest=data)

    @classmethod
    def from_file(cls, path: Path) -> DBTManifest:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(data)

    @property
    def native(self) -> Any:
        """The underlying dbt-artifacts-parser result."""
        return self._native

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


class DBTProject:
    """
    Helper to represent and interact with a dbt project on disk.

    Responsibilities:
    - locate target/manifest.json and parse it via DBTManifest
    - provide convenience helpers to list models and sources

    Usage:
        p = DBTProject(project_path=".")
        print(p.project_yaml)
        manifest = p.load_manifest()  # returns DBTManifest or None
    """

    def __init__(self, project_path: Optional[str] = None) -> None:
        # Determine project path
        if project_path is None:
            # fallback to cwd if dbt_project.yml present
            cwd = Path.cwd()
            if (cwd / "dbt_project.yml").exists():
                self.project_path = cwd
            else:
                raise FileNotFoundError(
                    "No project_path provided and dbt_project.yml not found in cwd."
                )
        else:
            self.project_path = Path(project_path).resolve()

        # Primary files
        self.project_yaml_file = self.project_path / "dbt_project.yml"
        if not self.project_yaml_file.exists():
            raise FileNotFoundError(
                f"dbt_project.yml not found at {self.project_yaml_file}"
            )

        self._project_yaml: Optional[Dict[str, Any]] = None
        self._manifest: Optional[DBTManifest] = None

        # Immediately load project YAML
        self._load_project_yaml()

    def _load_project_yaml(self) -> None:
        """Internal: read and parse dbt_project.yml into `_project_yaml`."""
        with self.project_yaml_file.open("r", encoding="utf-8") as fh:
            self._project_yaml = yaml.safe_load(fh) or {}

    @property
    def project_yaml(self) -> Dict[str, Any]:
        """The parsed contents of `dbt_project.yml`."""
        return self._project_yaml or {}

    @property
    def project_name(self) -> Optional[str]:
        """Convenience: return the name of the project, if declared in dbt_project.yml"""
        return self.project_yaml.get("name")

    def manifest_path(self) -> Path:
        """
        Return the expected manifest.json path within the project's `target/` directory.

        Note: dbt writes `manifest.json` to `<project_root>/target/manifest.json` by default.
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

    def get_nodes(self) -> Dict[str, Any]:
        """
        Convenience: return all nodes from the manifest, if loaded.

        Returns:
            Dict of node unique_id to Node object, or empty dict if no manifest.
        """
        manifest = self.load_manifest()
        if manifest:
            return manifest.nodes
        return {}

    def get_models(self) -> Dict[str, Any]:
        """
        Convenience: return all model nodes from the manifest, if loaded.

        Returns:
            Dict of model unique_id to Node object, or empty dict if no manifest.
        """
        manifest = self.load_manifest()
        if manifest:
            return {
                uid: node
                for uid, node in manifest.nodes.items()
                if node.resource_type == "model"
            }
        return {}

    def get_seeds(self) -> Dict[str, Any]:
        """
        Convenience: return all seed nodes from the manifest, if loaded.

        Returns:
            Dict of seed unique_id to Node object, or empty dict if no manifest.
        """
        manifest = self.load_manifest()
        if manifest:
            return {
                uid: node
                for uid, node in manifest.nodes.items()
                if node.resource_type == "seed"
            }
        return {}

    def get_tests(self) -> Dict[str, Any]:
        """
        Convenience: return all test nodes from the manifest, if loaded.

        Returns:
            Dict of test unique_id to Node object, or empty dict if no manifest.
        """
        manifest = self.load_manifest()
        if manifest:
            return {
                uid: node
                for uid, node in manifest.nodes.items()
                if node.resource_type == "test"
            }
        return {}

    def get_sources(self) -> Dict[str, Any]:
        """
        Convenience: return all sources from the manifest, if loaded.

        Returns:
            Dict of source unique_id to Source object, or empty dict if no manifest.
        """
        manifest = self.load_manifest()
        if manifest:
            return manifest.sources
        return {}

    def get_macros(self) -> Dict[str, Any]:
        """
        Convenience: return all macros from the manifest, if loaded.

        Returns:
            Dict of macro unique_id to Macro object, or empty dict if no manifest.
        """
        manifest = self.load_manifest()
        if manifest:
            return manifest.macros
        return {}


if __name__ == "__main__":
    cli = DBTCLI(
        path="../jaffle_shop_duckdb/venv/bin/dbt"
    )  # will try to find `dbt` on PATH
    project = DBTProject(project_path="./jaffle_shop_duckdb/")
    project.load_manifest()  # returns DBTManifest or None if not present

    print(project.get_nodes().keys())

    # run `dbt compile`
    # result = cli.run(["debug"], cwd=project.project_path, timeout=30_000)
    # print(result.returncode, result.stdout)
