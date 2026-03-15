"""
Modal screen for adding or editing a dbt project.

When no ``existing`` entry is provided the modal operates in **add** mode.
When an ``existing`` ``ProjectEntry`` is supplied it operates in **edit** mode:
the project path field is pre-filled and read-only, while the dbt executable
path can be changed.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static


@dataclass
class ProjectModalResult:
    """Returned when the user confirms the modal.

    Attributes:
        project_path: Absolute path to the dbt project directory.
        dbt_path: Path to the dbt executable, or ``None`` for auto-detect.
        is_edit: ``True`` when the result comes from an edit operation.
    """

    project_path: str
    dbt_path: Optional[str] = None
    is_edit: bool = False


# Keep the old name around as an alias so existing imports keep working.
AddProjectResult = ProjectModalResult


class ProjectModal(ModalScreen[Optional[ProjectModalResult]]):
    """Modal dialog for adding or editing a dbt project.

    Pass ``existing`` (a ``ProjectEntry`` or any object with
    ``project_path`` / ``dbt_path`` attributes) to open the modal in
    **edit** mode.  Otherwise it opens in **add** mode.

    Returns a ``ProjectModalResult`` on success, or ``None`` if the user
    cancels.
    """

    # Alias kept for backward compatibility with code that references
    # ``AddProjectModal.Result``.
    Result = ProjectModalResult

    DEFAULT_CSS = """
    ProjectModal {
        align: center middle;
    }

    #project-modal-dialog {
        width: 70;
        height: auto;
        max-height: 24;
        border: thick $secondary;
        background: $surface;
        padding: 1 2;
    }

    #project-modal-title {
        text-style: bold;
        width: 100%;
        content-align: center middle;
        text-align: center;
        margin-bottom: 1;
    }

    .project-modal-label {
        margin-top: 1;
        margin-bottom: 0;
        color: $text;
        text-style: bold;
    }

    .project-modal-hint {
        color: $text-muted;
        text-style: italic;
        margin-bottom: 0;
    }

    #project-modal-error {
        color: $error;
        text-style: bold;
        margin-top: 1;
        height: auto;
        display: none;
    }

    #project-modal-buttons {
        margin-top: 1;
        width: 100%;
        height: auto;
        align: center middle;
    }

    #project-modal-buttons Button {
        margin: 0 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False),
    ]

    def __init__(self, existing=None, **kwargs) -> None:
        """
        Args:
            existing: An object with ``project_path`` and optional ``dbt_path``
                      attributes (e.g. a ``ProjectEntry``).  When provided the
                      modal opens in *edit* mode.
        """
        super().__init__(**kwargs)
        self._existing = existing
        self._is_edit = existing is not None

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        title = "Edit dbt Project" if self._is_edit else "Add dbt Project"

        # Pre-fill values when editing
        initial_project_path = ""
        initial_dbt_path = ""
        if self._existing is not None:
            initial_project_path = getattr(self._existing, "project_path", "") or ""
            initial_dbt_path = getattr(self._existing, "dbt_path", "") or ""

        with Vertical(id="project-modal-dialog"):
            yield Label(title, id="project-modal-title")

            yield Label("Project Path", classes="project-modal-label")
            yield Input(
                value=initial_project_path,
                placeholder="/path/to/your/dbt/project",
                id="input-project-path",
                disabled=self._is_edit,
            )
            if self._is_edit:
                yield Label(
                    "Project path cannot be changed (remove & re-add instead)",
                    classes="project-modal-hint",
                )
            else:
                yield Label(
                    "Directory containing dbt_project.yml",
                    classes="project-modal-hint",
                )

            yield Label("dbt Executable Path (optional)", classes="project-modal-label")
            yield Input(
                value=initial_dbt_path,
                placeholder="Leave empty to use global dbt",
                id="input-dbt-path",
            )
            yield Label(
                "Path to dbt binary; blank = auto-detect from PATH",
                classes="project-modal-hint",
            )

            yield Static("", id="project-modal-error")

            with Horizontal(id="project-modal-buttons"):
                btn_label = "Save" if self._is_edit else "Add"
                yield Button(btn_label, variant="primary", id="btn-confirm")
                yield Button("Cancel", variant="default", id="btn-cancel")

    def on_mount(self) -> None:
        if self._is_edit:
            # In edit mode the path is locked — focus the dbt input directly
            self.query_one("#input-dbt-path", Input).focus()
        else:
            self.query_one("#input-project-path", Input).focus()

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    @on(Button.Pressed, "#btn-confirm")
    def _on_confirm(self, event: Button.Pressed) -> None:
        self._try_submit()

    @on(Button.Pressed, "#btn-cancel")
    def _on_cancel(self, event: Button.Pressed) -> None:
        self.dismiss(None)

    @on(Input.Submitted)
    def _on_input_submitted(self, event: Input.Submitted) -> None:
        self._try_submit()

    def action_cancel(self) -> None:
        self.dismiss(None)

    # ------------------------------------------------------------------
    # Validation & submit
    # ------------------------------------------------------------------

    def _try_submit(self) -> None:
        """Validate inputs and dismiss with result, or show an error."""
        error_label = self.query_one("#project-modal-error", Static)

        project_path_raw = self.query_one("#input-project-path", Input).value.strip()
        dbt_path_raw = self.query_one("#input-dbt-path", Input).value.strip()

        # -- Validate project path --
        if not project_path_raw:
            error_label.update("⚠ Project path is required.")
            error_label.display = True
            return

        project_path = Path(project_path_raw).expanduser().resolve()
        if not project_path.exists():
            error_label.update(f"⚠ Directory does not exist: {project_path}")
            error_label.display = True
            return

        if not (project_path / "dbt_project.yml").exists():
            error_label.update(f"⚠ No dbt_project.yml found in {project_path}")
            error_label.display = True
            return

        # -- Validate dbt path (optional) --
        dbt_path: Optional[str] = None
        if dbt_path_raw:
            dbt_resolved = Path(dbt_path_raw).expanduser().resolve()
            if not dbt_resolved.exists():
                error_label.update(f"⚠ dbt executable not found: {dbt_resolved}")
                error_label.display = True
                return
            dbt_path = str(dbt_resolved)

        self.dismiss(
            ProjectModalResult(
                project_path=str(project_path),
                dbt_path=dbt_path,
                is_edit=self._is_edit,
            )
        )


# ---------------------------------------------------------------------------
# Backward-compatible aliases so ``from add_project_modal import …`` still
# works for any code that hasn't been updated yet.
# ---------------------------------------------------------------------------
AddProjectModal = ProjectModal
