import argparse

from dbt import DBTCLI, DBTProject
from node_details import NodeDetailsWidget
from textual.app import App
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import Footer, Header, Tree


class SideBar(Tree):
    def __init__(self, project=None, include=("model", "source", "seed"), **kwargs):
        """
        SideBar consumes a DBTProject (single source-of-truth) and populates
        models/sources/seeds from its manifest JSON.

        Args:
            project: DBTProject instance (or None)
            include: iterable of resource types to show (subset of "model","source","seed")
        """
        super().__init__(**kwargs)
        self.project = project
        self.include = set(include)
        self.show_root = False
        self.guide = 2

    def on_mount(self):
        self.populate()

    def populate(self):
        # Clear any existing root children if re-populating
        try:
            # textual Tree does not offer a direct clear, so recreate root children by removing them
            for child in list(self.root.children):
                self.root.remove(child)
        except Exception:
            pass

        if not self.project:
            return

        manifest = self.project.get_manifest_json("all") or {}
        nodes = manifest.get("nodes", {})
        sources = manifest.get("sources", {})

        if "model" in self.include:
            model_tree = self.root.add("Models", expand=True)
            # nodes is {uid: node_dict}
            for uid, node in sorted(
                nodes.items(), key=lambda kv: kv[1].get("name", kv[0])
            ):
                if node.get("resource_type") == "model":
                    display_name = node.get("name", uid)
                    model_tree.add_leaf(display_name, data=node)

        if "source" in self.include:
            source_tree = self.root.add("Sources", expand=True)
            for uid, src in sorted(
                sources.items(), key=lambda kv: kv[1].get("name", kv[0])
            ):
                display_name = src.get("name", uid)
                source_tree.add_leaf(display_name, data=src)

        if "seed" in self.include:
            seed_tree = self.root.add("Seeds", expand=True)
            for uid, node in sorted(
                nodes.items(), key=lambda kv: kv[1].get("name", kv[0])
            ):
                if node.get("resource_type") == "seed":
                    display_name = node.get("name", uid)
                    seed_tree.add_leaf(display_name, data=node)


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

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
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
