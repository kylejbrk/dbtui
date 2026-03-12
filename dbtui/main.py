import argparse
from pathlib import Path

from add_project_modal import ProjectModal, ProjectModalResult
from command_screen import CommandScreen
from dbt import DBTCLI, DBTCommand, DBTProject
from node_details import NodeDetailsWidget
from project_store import ProjectEntry, ProjectStore
from show_screen import ShowScreen
from sidebar import SideBar
from textual.app import App
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Footer, Header


class DBTUI(App):
    CSS_PATH = "dbtui.tcss"
    BINDINGS = [
        ("q", "quit", "Quit"),
        Binding("ctrl+h", "focus_sidebar", "Focus Sidebar", show=False),
        Binding("ctrl+l", "focus_details", "Focus Details", show=False),
        Binding("tab", "toggle_pane", "Toggle Pane", show=False),
        Binding("a", "toggle_sidebar", "Toggle Sidebar", show=False),
    ]

    def __init__(self, project_path=None, dbt_path=None):
        super().__init__()
        self.initial_project_path = project_path
        self.initial_dbt_path = dbt_path

        # Persistent project store
        self.store = ProjectStore()

        # Auto-detect: if launched from (or given) a dbt project dir, register it
        self._auto_add_initial_project()

        # Build the live list of (ProjectEntry, DBTProject | None) tuples
        self._projects: list[tuple[ProjectEntry, DBTProject | None]] = []
        self._reload_projects()

    # ------------------------------------------------------------------
    # Project helpers
    # ------------------------------------------------------------------

    def _auto_add_initial_project(self) -> None:
        """Register the initial project (CLI arg or cwd) in the store."""
        if self.initial_project_path:
            path = str(Path(self.initial_project_path).resolve())
        else:
            cwd = Path.cwd()
            if (cwd / "dbt_project.yml").exists():
                path = str(cwd)
            else:
                return

        entry = ProjectEntry(
            project_path=path,
            dbt_path=self.initial_dbt_path,
        )
        self.store.add(entry)

    def _reload_projects(self) -> None:
        """Rebuild the in-memory project list from the store."""
        self._projects = []
        for entry in self.store.entries:
            dbt_project = self._load_dbt_project(entry)
            self._projects.append((entry, dbt_project))

    @staticmethod
    def _load_dbt_project(entry: ProjectEntry) -> DBTProject | None:
        """Try to instantiate a DBTProject; return None on failure."""
        try:
            proj = DBTProject(project_path=entry.project_path)
            proj.load_manifest()
            return proj
        except Exception:
            return None

    def _cli_for_entry(self, entry: ProjectEntry) -> DBTCLI:
        """Return a DBTCLI configured for the given project entry."""
        return DBTCLI(path=entry.resolved_dbt_path)

    def _find_entry_for_node(self, node_data: dict) -> ProjectEntry | None:
        """Given a sidebar node's data dict, find its owning ProjectEntry."""
        # Walk up: the node data itself won't have project_path, but the
        # sidebar stores it in the project-level tree node.  However the
        # node_details panel receives the raw manifest node dict.  We match
        # by checking which project contains the unique_id.
        uid = node_data.get("unique_id", "")
        for entry, dbt_project in self._projects:
            if dbt_project is None:
                continue
            nodes = dbt_project.get_nodes()
            sources = dbt_project.get_sources()
            if uid in nodes or uid in sources:
                return entry
        # Fallback: first project
        if self._projects:
            return self._projects[0][0]
        return None

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self):
        yield Header()
        with Horizontal():
            yield SideBar(
                label="node_tree",
                projects=self._projects,
                id="sidebar",
            )
            yield NodeDetailsWidget(id="node_details")
        yield Footer()

    def on_mount(self):
        self.title = "DBTUI"
        if self._projects:
            first_entry = self._projects[0][0]
            self.sub_title = first_entry.display_name
        else:
            self.sub_title = "No projects"

        sidebar = self.query_one("#sidebar")
        sidebar.border_title = "Projects"

    # ------------------------------------------------------------------
    # Pane focus switching (vim-style)
    # ------------------------------------------------------------------

    def action_focus_sidebar(self) -> None:
        self.query_one("#sidebar", SideBar).focus()

    def action_focus_details(self) -> None:
        self.query_one("#node_details", NodeDetailsWidget).focus()

    def action_toggle_pane(self) -> None:
        sidebar = self.query_one("#sidebar", SideBar)
        details = self.query_one("#node_details", NodeDetailsWidget)
        if sidebar.has_focus or sidebar.has_focus_within:
            details.focus()
        else:
            sidebar.focus()

    def action_toggle_sidebar(self) -> None:
        sidebar = self.query_one("#sidebar", SideBar)
        sidebar.display = not sidebar.display
        if not sidebar.display:
            self.query_one("#node_details", NodeDetailsWidget).focus()
        else:
            sidebar.focus()

    # ------------------------------------------------------------------
    # Add / remove project via sidebar messages
    # ------------------------------------------------------------------

    def on_side_bar_add_project_requested(
        self, event: SideBar.AddProjectRequested
    ) -> None:
        """Open the Project modal in add mode."""
        self.push_screen(ProjectModal(), callback=self._on_project_modal_result)

    def on_side_bar_edit_project_requested(
        self, event: SideBar.EditProjectRequested
    ) -> None:
        """Open the Project modal in edit mode for the selected project."""
        entry = self.store.get(event.project_path)
        if entry is None:
            self.notify("Project not found in store.", severity="warning")
            return
        self.push_screen(
            ProjectModal(existing=entry), callback=self._on_project_modal_result
        )

    def _on_project_modal_result(self, result: ProjectModalResult | None) -> None:
        if result is None:
            return

        entry = ProjectEntry(
            project_path=result.project_path,
            dbt_path=result.dbt_path,
        )

        if result.is_edit:
            self.store.update(entry)
            self._reload_projects()
            sidebar = self.query_one("#sidebar", SideBar)
            sidebar.set_projects(self._projects)
            self.notify(f"Updated project: {entry.display_name}")
        else:
            added = self.store.add(entry)
            if not added:
                self.notify("Project already registered.", severity="warning")
                return

            self._reload_projects()
            sidebar = self.query_one("#sidebar", SideBar)
            sidebar.set_projects(self._projects)
            self.notify(f"Added project: {entry.display_name}")

            # Update subtitle if this is the first project
            if len(self._projects) == 1:
                self.sub_title = entry.display_name

    def on_side_bar_remove_project_requested(
        self, event: SideBar.RemoveProjectRequested
    ) -> None:
        """Remove a project from the store and refresh the sidebar."""
        removed = self.store.remove(event.project_path)
        if removed:
            self._reload_projects()
            sidebar = self.query_one("#sidebar", SideBar)
            sidebar.set_projects(self._projects)
            self.notify("Project removed.")
        else:
            self.notify("Project not found in store.", severity="warning")

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def on_tree_node_selected(self, event: SideBar.NodeSelected) -> None:
        details_widget = self.query_one("#node_details", NodeDetailsWidget)
        if event.node.data and isinstance(event.node.data, dict):
            # Skip project-level nodes (they have "entry" key)
            if "entry" in event.node.data:
                return
            details_widget.update_details(event.node.data)
        else:
            details_widget.update_details({})

    def on_node_details_widget_command_requested(
        self, event: NodeDetailsWidget.CommandRequested
    ) -> None:
        """Handle a dbt command request from the node details panel."""
        # Find which project owns the node
        node_data = getattr(event, "_node_data", None)
        entry = None
        if node_data:
            entry = self._find_entry_for_node(node_data)

        # Fallback: try to match by looking through all projects
        if entry is None:
            for e, proj in self._projects:
                if proj is None:
                    continue
                nodes = proj.get_nodes()
                sources = proj.get_sources()
                for uid in list(nodes.keys()) + list(sources.keys()):
                    name = nodes.get(uid, sources.get(uid, {})).get("name", "")
                    if name == event.node_name:
                        entry = e
                        break
                if entry:
                    break

        # Last resort: first project
        if entry is None and self._projects:
            entry = self._projects[0][0]

        if entry is None:
            self.notify("No project found for this node.", severity="error")
            return

        cli = self._cli_for_entry(entry)
        if not cli.available():
            self.notify(
                "dbt binary not found. Install dbt or configure the dbt path.",
                severity="error",
            )
            return

        if event.command == DBTCommand.SHOW:
            screen = ShowScreen(
                cli=cli,
                node_name=event.node_name,
                project_path=entry.project_path,
            )
        else:
            screen = CommandScreen(
                cli=cli,
                command=event.command,
                node_name=event.node_name,
                project_path=entry.project_path,
            )
        self.push_screen(screen)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBT TUI for viewing models")
    parser.add_argument(
        "--project-path",
        type=str,
        default=None,
        help="Path to dbt project directory.",
    )
    parser.add_argument(
        "--dbt-path",
        type=str,
        default=None,
        help="Path to dbt executable.",
    )
    args = parser.parse_args()

    app = DBTUI(args.project_path, args.dbt_path)
    app.run()
