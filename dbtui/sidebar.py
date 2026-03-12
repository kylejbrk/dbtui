from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, Tuple

from textual.binding import Binding
from textual.widgets import Tree
from textual.widgets._tree import TreeNode


class SideBar(Tree):
    """Project sidebar that displays dbt nodes grouped by resource type and schema.

    Tree structure:
        Models (N)
        ├── schema_a (M)
        │   ├── model_one
        │   └── model_two
        └── schema_b (K)
            └── model_three
        Sources (N)
        └── …
        Seeds (N)
        └── …

    Vim-style navigation is layered on top of the default Tree bindings:
        j / k  — move cursor down / up
        h      — collapse current node or jump to parent
        l      — expand current node or move into first child
        o      — toggle expand / collapse
        enter  — select (emits NodeSelected)
        g      — jump to first node
        G      — jump to last node
    """

    BINDINGS = [
        # Vim cursor movement
        Binding("j", "cursor_down", "Cursor Down", show=False),
        Binding("k", "cursor_up", "Cursor Up", show=False),
        Binding("h", "collapse_or_parent", "Collapse / Parent", show=False),
        Binding("l", "expand_or_child", "Expand / Child", show=False),
        Binding("o", "toggle_node", "Toggle", show=False),
        Binding("enter", "select_cursor", "Select", show=False),
        Binding("g", "scroll_home", "First node", show=False),
        Binding("G", "scroll_end", "Last node", show=False, key_display="G"),
    ]

    def __init__(
        self,
        project=None,
        include: Iterable[str] = ("model", "source", "seed"),
        **kwargs,
    ):
        """
        Args:
            project: DBTProject instance (or None).
            include: resource types to show (subset of "model", "source", "seed").
        """
        super().__init__(**kwargs)
        self.project = project
        self.include = set(include)
        self.show_root = False
        self.guide_depth = 3

    # ------------------------------------------------------------------
    # Vim-specific actions
    # ------------------------------------------------------------------

    def action_collapse_or_parent(self) -> None:
        """Collapse the current node if expanded, otherwise jump to its parent."""
        node = self.cursor_node
        if node is None:
            return

        if node.allow_expand and node.is_expanded:
            node.collapse()
        elif node.parent is not None and node.parent is not self.root:
            self.select_node(node.parent)
            self.scroll_to_node(node.parent)

    def action_expand_or_child(self) -> None:
        """Expand the current node if collapsed, otherwise move to its first child."""
        node = self.cursor_node
        if node is None:
            return

        if node.allow_expand and not node.is_expanded:
            node.expand()
        elif node.allow_expand and node.is_expanded and node.children:
            first_child = node.children[0]
            self.select_node(first_child)
            self.scroll_to_node(first_child)

    # ------------------------------------------------------------------
    # Population
    # ------------------------------------------------------------------

    def on_mount(self) -> None:
        self.populate()

    def populate(self) -> None:
        """(Re-)populate the tree from the project manifest, grouped by schema."""
        try:
            self.clear()
        except Exception:
            pass

        if not self.project:
            return

        if "model" in self.include:
            models = self.project.get_models()
            self._add_resource_group("Models", models)

        if "source" in self.include:
            sources = self.project.get_sources()
            self._add_resource_group("Sources", sources)

        if "seed" in self.include:
            seeds = self.project.get_seeds()
            self._add_resource_group("Seeds", seeds)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _group_by_database(
        items: Dict[str, Dict[str, Any]],
    ) -> Dict[str, list[Tuple[str, Dict[str, Any]]]]:
        """Group *items* (uid → node dict) by their ``"database"`` field.

        Returns a dict mapping database name to a sorted list of (uid, node) tuples.
        Nodes without a database are placed under ``"default"``.
        """
        grouped: dict[str, list[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
        for uid, node in items.items():
            database = node.get("database") or "default"
            grouped[database].append((uid, node))

        # Sort nodes inside each database group by name
        for database in grouped:
            grouped[database].sort(key=lambda kv: kv[1].get("name", kv[0]).lower())

        return dict(grouped)

    def _add_resource_group(
        self,
        label: str,
        items: Dict[str, Dict[str, Any]],
        skip_intermediate: bool = False,
    ) -> None:
        """Add a top-level resource-type branch with schema sub-groups."""
        total = len(items)
        resource_node: TreeNode = self.root.add(
            f"{label} ({total})",
            expand=total > 0,
        )

        grouped = self._group_by_database(items)

        # If every node lives in the same database, skip the intermediate level
        if skip_intermediate and len(grouped) == 1:
            database, members = next(iter(grouped.items()))
            for uid, node in members:
                display_name = node.get("name", uid)
                resource_node.add_leaf(display_name, data=node)
            return

        # Multiple database → add a sub-branch per database
        for database in sorted(grouped, key=str.lower):
            members = grouped[database]
            database_node = resource_node.add(
                f"{database} ({len(members)})",
                expand=False,
            )
            for uid, node in members:
                display_name = node.get("name", uid)
                database_node.add_leaf(display_name, data=node)
