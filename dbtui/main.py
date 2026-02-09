import argparse

from dbt import DBTCLI, DBTProject
from textual.app import App
from textual.containers import Horizontal, ScrollableContainer, VerticalScroll
from textual.widgets import (
    Collapsible,
    Footer,
    Header,
    Label,
    Markdown,
    Pretty,
    Static,
    Tree,
)


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
                model_tree.add_leaf(model, data=self.models[model])

        if self.sources:
            source_tree = self.root.add("Sources", expand=True)
            for source in self.sources:
                source_tree.add_leaf(source, data=self.sources[source])

        if self.seeds:
            seed_tree = self.root.add("Seeds", expand=True)
            for seed in self.seeds:
                seed_tree.add_leaf(seed, data=self.seeds[seed])


class NodeDetails(VerticalScroll):
    def compose(self):
        yield Static(
            "Select a node to view details", id="node_title", classes="section-title"
        )
        yield Static("", id="node_description")
        yield Static("", id="node_location")

        with Collapsible(title="Columns", collapsed=False, id="columns_section"):
            yield Static("", id="node_columns")

        with Collapsible(title="Dependencies", collapsed=False, id="deps_section"):
            yield Static("", id="node_parents")
            yield Static("", id="node_children")

        with Collapsible(title="Compiled SQL", collapsed=True, id="sql_section"):
            yield Static("", id="node_sql")

    def update_details(self, details: dict):
        """Update the node details display with new data."""
        if not details:
            self.query_one("#node_title", Static).update(
                "Select a node to view details"
            )
            self.query_one("#node_description", Static).update("")
            self.query_one("#node_location", Static).update("")
            self.query_one("#node_columns", Static).update("")
            self.query_one("#node_parents", Static).update("")
            self.query_one("#node_children", Static).update("")
            self.query_one("#node_sql", Static).update("")
            return

        # Title and basic info
        name = details.get("name", "Unknown")
        resource_type = details.get("resource_type", "node")
        materialized = details.get("config", {}).get("materialized", "")
        mat_str = f" ({materialized})" if materialized else ""
        self.query_one("#node_title", Static).update(
            f"{resource_type.title()}: {name}{mat_str}"
        )

        # Description
        description = details.get("description", "No description available")
        self.query_one("#node_description", Static).update(description)

        # Location info
        database = details.get("database", "")
        schema = details.get("schema", "")
        location = f"📍 {database}.{schema}.{name}" if database and schema else ""
        self.query_one("#node_location", Static).update(location)

        # Columns
        columns = details.get("columns", {})
        if columns:
            col_lines = []
            for col_name, col_info in columns.items():
                data_type = col_info.get("data_type") or "unknown"
                col_desc = col_info.get("description", "")
                desc_str = f" - {col_desc}" if col_desc else ""
                col_lines.append(f"  • {col_name} [{data_type}]{desc_str}")
            self.query_one("#node_columns", Static).update("\n".join(col_lines))
        else:
            self.query_one("#node_columns", Static).update("  No columns defined")

        # Dependencies (parents)
        depends_on = details.get("depends_on", {})
        parent_nodes = depends_on.get("nodes", [])
        if parent_nodes:
            parent_names = [n.split(".")[-1] for n in parent_nodes]
            self.query_one("#node_parents", Static).update(
                f"⬆️ Parents: {', '.join(parent_names)}"
            )
        else:
            self.query_one("#node_parents", Static).update("⬆️ Parents: None")

        # Children (if available - usually needs to be computed separately)
        children = details.get("children", [])
        if children:
            child_names = [n.split(".")[-1] for n in children]
            self.query_one("#node_children", Static).update(
                f"⬇️ Children: {', '.join(child_names)}"
            )
        else:
            self.query_one("#node_children", Static).update("⬇️ Children: None")

        # Compiled SQL (fallback to raw_code if compiled not available)
        compiled_sql = details.get("compiled_code") or details.get("raw_code", "")
        if compiled_sql:
            self.query_one("#node_sql", Static).update(compiled_sql)
        else:
            self.query_one("#node_sql", Static).update("No SQL available")


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
            yield SideBar(
                label="node_tree",
                models=self.project.get_models(),
                sources=self.project.get_sources(),
                seeds=self.project.get_seeds(),
                id="sidebar",
            )
            with ScrollableContainer(id="details_container"):
                with VerticalScroll():
                    yield Pretty("", id="node_details")
        yield Footer()

    def on_mount(self):
        self.title = "DBTUI"
        if self.project.project_name:
            self.sub_title = self.project.project_name

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        details_widget = self.query_one("#node_details", Pretty)
        if event.node.data:
            details_widget.update(event.node.data)
        else:
            details_widget.update({})


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
