import argparse

from dbt import DBTCLI, DBTProject
from node_details import NodeDetailsWidget
from sidebar import SideBar
from textual.app import App
from textual.containers import Horizontal
from textual.widgets import Footer, Header


class DBTUI(App):
    CSS_PATH = "dbtui.tcss"
    BINDINGS = [("q", "quit", "Quit")]

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

    def on_tree_node_selected(self, event: SideBar.NodeSelected) -> None:
        details_widget = self.query_one("#node_details", NodeDetailsWidget)
        if event.node.data:
            details_widget.update_details(event.node.data)
        else:
            details_widget.update_details({})


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
