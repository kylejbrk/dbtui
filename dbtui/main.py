import argparse

from command_screen import CommandScreen
from dbt import DBTCLI, DBTCommand, DBTProject
from node_details import NodeDetailsWidget
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
        self.project_path = project_path
        self.dbt_path = dbt_path

        self.cli = DBTCLI(path=self.dbt_path)
        self.project = DBTProject(project_path=self.project_path)

    def compose(self):
        yield Header()
        with Horizontal():
            # Pass the project as the single source-of-truth to the SideBar.
            yield SideBar(
                label="node_tree",
                project=self.project,
                id="sidebar",
            )
            yield NodeDetailsWidget(id="node_details")
        yield Footer()

    def on_mount(self):
        self.title = "DBTUI"
        if self.project.project_name:
            self.sub_title = self.project.project_name

        sidebar = self.query_one("#sidebar")
        sidebar.border_title = "Projects"

    # ------------------------------------------------------------------
    # Pane focus switching (vim-style)
    # ------------------------------------------------------------------

    def action_focus_sidebar(self) -> None:
        """Move focus to the sidebar (Ctrl+h)."""
        self.query_one("#sidebar", SideBar).focus()

    def action_focus_details(self) -> None:
        """Move focus to the node details panel (Ctrl+l)."""
        self.query_one("#node_details", NodeDetailsWidget).focus()

    def action_toggle_pane(self) -> None:
        """Toggle focus between sidebar and node details (Tab)."""
        sidebar = self.query_one("#sidebar", SideBar)
        details = self.query_one("#node_details", NodeDetailsWidget)
        if sidebar.has_focus or sidebar.has_focus_within:
            details.focus()
        else:
            sidebar.focus()

    def action_toggle_sidebar(self) -> None:
        """Toggle the sidebar visibility (a)."""
        sidebar = self.query_one("#sidebar", SideBar)
        sidebar.display = not sidebar.display
        if not sidebar.display:
            # Sidebar hidden — move focus to details
            self.query_one("#node_details", NodeDetailsWidget).focus()
        else:
            # Sidebar shown — move focus to it
            sidebar.focus()

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def on_tree_node_selected(self, event: SideBar.NodeSelected) -> None:
        details_widget = self.query_one("#node_details", NodeDetailsWidget)
        if event.node.data:
            details_widget.update_details(event.node.data)
        else:
            details_widget.update_details({})

    def on_node_details_widget_command_requested(
        self, event: NodeDetailsWidget.CommandRequested
    ) -> None:
        """Handle a dbt command request from the node details panel."""
        if not self.cli.available():
            self.notify(
                "dbt binary not found. Install dbt or pass --dbt-path.",
                severity="error",
            )
            return

        if event.command == DBTCommand.SHOW:
            screen = ShowScreen(
                cli=self.cli,
                node_name=event.node_name,
                project_path=self.project.project_path,
            )
        else:
            screen = CommandScreen(
                cli=self.cli,
                command=event.command,
                node_name=event.node_name,
                project_path=self.project.project_path,
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
        "--dbt-path", type=str, default=None, help="Path to dbt executable."
    )
    args = parser.parse_args()

    app = DBTUI(args.project_path, args.dbt_path)
    app.run()
