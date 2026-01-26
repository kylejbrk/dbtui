import argparse

from dbt import DBTCLI, DBTProject
from textual.app import App
from textual.containers import Horizontal, ScrollableContainer
from textual.widgets import Header, Pretty, Tree


class SideBar(Tree):
    def __init__(self, models=None, sources=None, seeds=None, **kwargs):
        super().__init__(**kwargs)
        self.models = models
        self.sources = sources
        self.seeds = seeds
        self.show_root = False
        self.guide = 2

    def on_mount(self):
        self.populate()

    def populate(self):
        if self.models:
            model_tree = self.root.add("Models", expand=True)
            for model in self.models:
                model_tree.add_leaf(model.get("name"), data=model)

        if self.sources:
            source_tree = self.root.add("Sources", expand=True)
            for source in self.sources:
                source_tree.add_leaf(source.get("name"), data=source)

        if self.seeds:
            seed_tree = self.root.add("Seeds", expand=True)
            for seed in self.seeds:
                seed_tree.add_leaf(seed.get("name"), data=seed)


class DBTUI(App):
    CSS_PATH = "dbtui.tcss"

    def __init__(self, project_path=None, dbt_path=None):
        super().__init__()
        self.project_path = project_path
        self.dbt_path = dbt_path

        self.cli = DBTCLI(path=self.dbt_path)
        self.project = DBTProject(project_path=self.project_path)

    def compose(self):
        yield Header()
        with Horizontal():
            yield SideBar(
                label="node_tree",
                models=self.project.list_models(),
                sources=self.project.list_sources(),
                seeds=self.project.list_seeds(),
                id="sidebar",
            )
            yield ScrollableContainer(
                Pretty("Select a node to view details.", id="node_details")
            )

    def on_mount(self):
        self.title = "DBTUI"
        if self.project.project_name:
            self.sub_title = self.project.project_name

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        details_widget = self.query_one("#node_details", Pretty)
        if event.node.data:
            details_widget.update(event.node.data)
        else:
            details_widget.update("No data available for this node")


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
