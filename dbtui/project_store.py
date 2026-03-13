"""
Persistent storage for dbt project entries.

Projects are stored as JSON in a platform-appropriate data directory so they
persist across sessions.

On Linux the default location respects ``XDG_DATA_HOME``
(falling back to ``~/.local/share/dbtui/projects.json``).
On macOS it uses ``~/Library/Application Support/dbtui/projects.json``.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def _default_store_dir() -> Path:
    """Return the data directory, respecting XDG_DATA_HOME on Linux."""
    import sys

    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg) / "dbtui"
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "dbtui"
    return Path.home() / ".local" / "share" / "dbtui"


DEFAULT_STORE_DIR = _default_store_dir()
DEFAULT_STORE_FILE = DEFAULT_STORE_DIR / "projects.json"


@dataclass
class ProjectEntry:
    """A single dbt project reference.

    Attributes:
        project_path: Absolute path to the dbt project directory.
            Paths are stored already resolved (see :meth:`ProjectStore.add`).
        dbt_path: Path to the dbt executable. If None, uses global ``dbt`` on PATH.
    """

    project_path: str
    dbt_path: Optional[str] = None

    def __post_init__(self):
        # _cached_name is NOT a dataclass field, so dataclasses.asdict()
        # will never include it in serialisation output.
        self._cached_name: Optional[str] = None

    @property
    def resolved_dbt_path(self) -> Optional[str]:
        """Return the dbt executable path, falling back to ``shutil.which('dbt')``."""
        if self.dbt_path:
            return self.dbt_path
        return shutil.which("dbt")

    @property
    def name(self) -> Optional[str]:
        """Try to read the project name from ``dbt_project.yml``.

        The result is cached so the YAML file is only parsed once per
        ``ProjectEntry`` instance.
        """
        if self._cached_name is not None:
            return self._cached_name
        try:
            import yaml

            yml = Path(self.project_path) / "dbt_project.yml"
            if yml.exists():
                with yml.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                self._cached_name = data.get("name")
                return self._cached_name
        except (OSError, Exception) as exc:
            logger.warning(
                "Failed to read project name from %s: %s", self.project_path, exc
            )
        return Path(self.project_path).name

    @property
    def display_name(self) -> str:
        """Human-readable name for UI display."""
        return self.name or Path(self.project_path).name

    def is_valid(self) -> bool:
        """Check if the project path contains a ``dbt_project.yml``."""
        return (Path(self.project_path) / "dbt_project.yml").exists()

    # Note: __eq__ and __hash__ defensively call resolve() because external
    # callers may construct ProjectEntry instances with non-resolved paths.
    # Internally, ProjectStore.add() ensures that stored paths are already
    # resolved, so the resolve() calls are technically redundant for entries
    # that come from the store – but keeping them is safer for correctness.

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

    The store file defaults to a platform-appropriate location under a
    ``dbtui`` data directory (see :func:`_default_store_dir`).
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

        The *project_path* is resolved to an absolute path before storing so
        that subsequent look-ups work regardless of how the path was originally
        specified.

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
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            logger.warning("Failed to load project store %s: %s", self.store_path, exc)
            self._entries = []

    def _save(self) -> None:
        """Persist current entries to the store file (atomic write)."""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path: Optional[str] = None
        try:
            fd, tmp_path = tempfile.mkstemp(dir=self.store_path.parent, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump([asdict(e) for e in self._entries], f, indent=2)
            os.replace(tmp_path, self.store_path)
        except OSError:
            # Clean up temp file on failure
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
            raise
