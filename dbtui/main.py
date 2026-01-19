from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Label, ListItem, ListView, Tree

# ----- dummy manifest -----
DUMMY_MANIFEST = {
    "nodes": {
        "model.project.customers": {
            "name": "customers",
            "resource_type": "model",
            "depends_on": {"nodes": ["source.project.raw_customers"]},
        },
        "model.project.orders": {
            "name": "orders",
            "resource_type": "model",
            "depends_on": {"nodes": ["model.project.customers"]},
        },
        "model.project.payments": {
            "name": "payments",
            "resource_type": "model",
            "depends_on": {"nodes": ["model.project.orders"]},
        },
        "source.project.raw_customers": {
            "name": "raw_customers",
            "resource_type": "source",
            "depends_on": {"nodes": []},
        },
    }
}

# ----- lineage graph -----
from collections import defaultdict


def ui_id(node_id: str) -> str:
    return node_id.replace(".", "_").replace(":", "_")


class LineageGraph:
    def __init__(self, manifest):
        self.nodes = manifest["nodes"]
        self.parents = defaultdict(list)
        self.children = defaultdict(list)

        for node_id, node in self.nodes.items():
            for parent in node["depends_on"]["nodes"]:
                self.parents[node_id].append(parent)
                self.children[parent].append(node_id)

    def label(self, node_id):
        node = self.nodes[node_id]
        return f"{node['resource_type']}: {node['name']}"


# ----- Textual app -----
class LineageApp(App):
    CSS = """
    Tree {
        border: round white;
        padding: 1;
    }
    ListView {
        border: round white;
        width: 30%;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            yield ListView(id="model_list")
            yield Tree("Lineage", id="lineage_tree")
        yield Footer()

    def on_mount(self):
        self.graph = LineageGraph(DUMMY_MANIFEST)

        model_list = self.query_one("#model_list", ListView)
        for node_id, node in self.graph.nodes.items():
            if node["resource_type"] == "model":
                item = ListItem(
                    Label(node["name"]),
                    id=ui_id(node_id),  # safe for Textual
                )
                item.node_id = node_id  # real dbt unique_id
                model_list.append(item)

    def on_list_view_selected(self, event: ListView.Selected):
        real_node_id = event.item.node_id
        self.show_lineage(real_node_id)

    def show_lineage(self, node_id: str):
        tree = self.query_one("#lineage_tree", Tree)
        tree.clear()

        root = tree.root
        root.label = self.graph.label(node_id)

        parents = root.add("⬆ parents")
        for p in self.graph.parents.get(node_id, []):
            parents.add(self.graph.label(p))

        children = root.add("⬇ children")
        for c in self.graph.children.get(node_id, []):
            children.add(self.graph.label(c))

        tree.root.expand_all()


if __name__ == "__main__":
    LineageApp().run()
