"""
Persistent storage for dbt project entries.

Projects are stored as JSON in ~/.local/share/dbtui/projects.json so they
persist across sessions.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

DEFAULT_STORE_DIR = Path.home() / ".local" / "share" / "dbtui"
DEFAULT_STORE_FILE = DEFAULT_STORE_DIR / "projects.json"


@dataclass
class ProjectEntry:
    """A single dbt project reference.

    Attributes:
        project_path: Absolute path to the dbt project directory.
        dbt_path: Path to the dbt executable. If None, uses global ``dbt`` on PATH.
    """

    project_path: str
    dbt_path: Optional[str] = None

    @property
    def resolved_dbt_path(self) -> Optional[str]:
        """Return the dbt executable path, falling back to ``shutil.which('dbt')``."""
        if self.dbt_path:
            return self.dbt_path
        return shutil.which("dbt")

    @property
    def name(self) -> Optional[str]:
        """Try to read the project name from ``dbt_project.yml``."""
        try:
            import yaml

            yml = Path(self.project_path) / "dbt_project.yml"
            if yml.exists():
                with yml.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                return data.get("name")
        except Exception:
            pass
        return Path(self.project_path).name

    @property
    def display_name(self) -> str:
        """Human-readable name for UI display."""
        return self.name or Path(self.project_path).name

    def is_valid(self) -> bool:
        """Check if the project path contains a ``dbt_project.yml``."""
        return (Path(self.project_path) / "dbt_project.yml").exists()

    def __eq__(self, other):
        if not isinstance(other, ProjectEntry):
            return NotImplemented
        return str(Path(self.project_path).resolve()) == str(
            Path(other.project_path).resolve()
        )

    def __hash__(self):
        return hash(str(Path(self.project_path).resolve()))


class ProjectStore:
    """Manages a list of ``ProjectEntry`` objects with JSON persistence.

    The store file defaults to ``~/.local/share/dbtui/projects.json``.
    """

    def __init__(self, store_path: Optional[Path] = None) -> None:
        self.store_path = store_path or DEFAULT_STORE_FILE
        self._entries: List[ProjectEntry] = []
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def entries(self) -> List[ProjectEntry]:
        """Return a copy of the current project entries."""
        return list(self._entries)

    def add(self, entry: ProjectEntry) -> bool:
        """Add a project entry if it isn't already present.

        Returns True if the entry was added, False if it was a duplicate.
        """
        resolved = str(Path(entry.project_path).resolve())
        entry = ProjectEntry(project_path=resolved, dbt_path=entry.dbt_path)

        if entry in self._entries:
            return False
        self._entries.append(entry)
        self._save()
        return True

    def remove(self, project_path: str) -> bool:
        """Remove a project entry by its path.

        Returns True if an entry was removed.
        """
        resolved = str(Path(project_path).resolve())
        before = len(self._entries)
        self._entries = [
            e for e in self._entries if str(Path(e.project_path).resolve()) != resolved
        ]
        if len(self._entries) < before:
            self._save()
            return True
        return False

    def get(self, project_path: str) -> Optional[ProjectEntry]:
        """Look up a project entry by path."""
        resolved = str(Path(project_path).resolve())
        for e in self._entries:
            if str(Path(e.project_path).resolve()) == resolved:
                return e
        return None

    def update(self, entry: ProjectEntry) -> None:
        """Update an existing entry (matched by project_path) or add it."""
        resolved = str(Path(entry.project_path).resolve())
        for i, existing in enumerate(self._entries):
            if str(Path(existing.project_path).resolve()) == resolved:
                self._entries[i] = ProjectEntry(
                    project_path=resolved, dbt_path=entry.dbt_path
                )
                self._save()
                return
        self.add(entry)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load project entries from the store file."""
        if not self.store_path.exists():
            self._entries = []
            return
        try:
            with self.store_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            self._entries = [ProjectEntry(**item) for item in data]
        except (json.JSONDecodeError, TypeError, KeyError):
            self._entries = []

    def _save(self) -> None:
        """Persist current entries to the store file."""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        with self.store_path.open("w", encoding="utf-8") as f:
            json.dump([asdict(e) for e in self._entries], f, indent=2)
