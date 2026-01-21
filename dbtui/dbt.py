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
from typing import Any, Dict, List, Optional, Tuple

import yaml

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
        cwd: Optional[str] = None,
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
    Lightweight accessor for a dbt `manifest.json` file.

    The manifest is parsed into a dictionary internally. This helper exposes
    convenience methods for common lookups (models, tests, sources).

    Usage:
        manifest = DBTManifest.from_path("/path/to/target/manifest.json")
        for node in manifest.list_nodes():
            ...
    """

    def __init__(self, manifest_data: Dict[str, Any]) -> None:
        self._data = manifest_data

        # manifest structure: keys like "nodes", "sources", "macros", "parents", etc.
        self.nodes: Dict[str, Dict[str, Any]] = manifest_data.get("nodes", {})
        self.sources: Dict[str, Dict[str, Any]] = manifest_data.get("sources", {})
        self.macros: Dict[str, Dict[str, Any]] = manifest_data.get("macros", {})
        self.metadata: Dict[str, Any] = manifest_data.get("metadata", {})

    @classmethod
    def from_path(cls, path: str) -> "DBTManifest":
        """
        Load manifest.json from a filesystem path and return a DBTManifest instance.

        Raises:
            FileNotFoundError: if the file does not exist.
            json.JSONDecodeError: if the file is not valid JSON.
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"manifest.json not found at {path!r}")
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return cls(data)

    def list_nodes(self, resource_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return a list of node dicts. Optionally filter by resource_type (e.g. "model").
        """
        nodes = list(self.nodes.values())
        if resource_type:
            return [n for n in nodes if n.get("resource_type") == resource_type]
        return nodes

    def get_node(self, unique_id: str) -> Optional[Dict[str, Any]]:
        """
        Return the node dictionary for the given unique_id, or None if not present.
        """
        return self.nodes.get(unique_id)

    def list_sources(self) -> List[Dict[str, Any]]:
        """
        Return a list of sources from the manifest.
        """
        return list(self.sources.values())

    def find_models_by_package(self, package_name: str) -> List[Dict[str, Any]]:
        """
        Return nodes that belong to a specific package.
        """
        return [n for n in self.nodes.values() if n.get("package_name") == package_name]


class DBTProject:
    """
    Helper to represent and interact with a dbt project on disk.

    Responsibilities:
    - locate and load `dbt_project.yml`
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
        return self.project_path / "target" / "manifest.json"

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

        self._manifest = DBTManifest.from_path(str(manifest_file))
        return self._manifest

    def list_models(self) -> List[Dict[str, Any]]:
        """
        List models defined in the manifest (if present). Returns an empty list if no manifest.
        """
        manifest = self.load_manifest()
        if not manifest:
            return []
        return manifest.list_nodes(resource_type="model")

    def list_sources(self) -> List[Dict[str, Any]]:
        """
        List sources defined in the manifest (if present).
        """
        manifest = self.load_manifest()
        if not manifest:
            return []
        return manifest.list_sources()

    def reload(self) -> None:
        """
        Reload project metadata and manifest.
        Useful if files changed on disk.
        """
        self._load_project_yaml()
        # force manifest reload next time
        self._manifest = None

    def path_for(self, relative: str) -> Path:
        """
        Return an absolute Path for a file relative to the project root.

        Example:
            p.path_for('models/my_model.sql')
        """
        return (self.project_path / relative).resolve()

    def find_model_file_paths(self) -> List[Path]:
        """
        A simple heuristic to find model files on disk by looking in `models/` directory
        and commonly named subdirs. Returns absolute paths.
        """
        model_dir = self.project_path / "models"
        if not model_dir.exists():
            return []
        paths: List[Path] = []
        for p in model_dir.rglob("*.sql"):
            # skip hidden files
            if p.name.startswith("."):
                continue
            paths.append(p.resolve())
        return paths


if __name__ == "__main__":
    cli = DBTCLI()  # will try to find `dbt` on PATH
    project = DBTProject(project_path="./jaffle_shop_duckdb/")
    manifest = project.load_manifest()  # returns DBTManifest or None if not present

    # run `dbt compile`
    result = cli.run(["compile"], cwd=project.project_path, timeout=30_000)
    print(result.returncode, result.stdout)
