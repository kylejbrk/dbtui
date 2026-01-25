import argparse

from dbt import DBTCLI, DBTProject
from textual.app import App
from textual.containers import Container, Horizontal
from textual.widgets import Header, Static, Tree


class NodeTree(Tree):
    def __init__(self, project, **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.show_root = False
        self.guide = 2

    def on_mount(self):
        self.populate()

    def populate(self):
        model_tree = self.root.add("Models", expand=True)
        for model in self.project.list_models():
            model_tree.add_leaf(model.get("name"), data=model)

        source_tree = self.root.add("Sources", expand=True)
        for source in self.project.list_sources():
            source_tree.add_leaf(source.get("name"), data=source)


class DBTUI(App):
    def __init__(self, project_path=None, dbt_path=None):
        super().__init__()
        self.project_path = project_path
        self.dbt_path = dbt_path

        self.cli = DBTCLI(path=self.dbt_path)
        self.project = DBTProject(project_path=self.project_path)

    def compose(self):
        yield Header()
        with Horizontal():
            yield Container(
                NodeTree(
                    label="node_tree",
                    project=self.project,
                    id="node_selector",
                )
            )
            yield Container(Static("Select a node to view details", id="node_details"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBT TUI for viewing models")
    parser.add_argument(
        "--project-path",
        type=str,
        default=None,
        help="Path to dbt project directory.",
    )
    parser.add_argument(
        "--dbt_path", type=str, default=None, help="Path to dbt executable."
    )
    args = parser.parse_args()

    app = DBTUI(args.project_path, args.dbt_path)
    app.run()
