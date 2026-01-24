import argparse
from typing import cast

from dbt import DBTCLI, DBTProject
from textual.app import App
from textual.widgets import Header, Label, ListItem, ListView


class NodeSelector(ListView):
    def __init__(self, models=None, **kwargs):
        super().__init__(**kwargs)
        self.models = models or []

    def on_mount(self):
        self.populate()

    def populate(self):
        for model in self.models:
            name = (
                model.get("name", "Unknown") if isinstance(model, dict) else str(model)
            )
            self.append(ListItem(Label(name)))


class DBTUI(App):
    def __init__(self, project_path=None, dbt_path=None):
        super().__init__()
        self.project_path = project_path
        self.dbt_path = dbt_path

        self.cli = DBTCLI(path=self.dbt_path)
        self.project = DBTProject(project_path=self.project_path)

    def compose(self):
        yield Header()
        yield NodeSelector(models=self.project.list_models(), id="node_selector")


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
