import argparse
from pathlib import Path

from dbt import DBTProject
from textual.app import App
from textual.widgets import Header, Label, ListItem, ListView


class DBTUI(App):
    def __init__(self, project_path=None):
        super().__init__()
        self.project_path = Path(project_path)

    def compose(self):
        yield Header()
        self.list_view = ListView()
        yield self.list_view

    async def on_mount(self):
        project = DBTProject(str(self.project_path))
        model_paths = project.find_model_file_paths()
        for path in model_paths:
            name = path.stem
            self.list_view.append(ListItem(Label(name)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DBT TUI for viewing models")
    parser.add_argument(
        "--project-path",
        type=str,
        default=None,
        help="Defaults to current directory",
    )
    args = parser.parse_args()

    app = DBTUI(args.project_path)
    app.run()
