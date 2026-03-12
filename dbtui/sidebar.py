from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from textual.binding import Binding
from textual.message import Message
from textual.widgets import Tree
from textual.widgets._tree import TreeNode


class SideBar(Tree):
    """Project sidebar that displays one or more dbt projects, each with its
    own subtree of models / sources / seeds grouped by schema.

    Tree structure (multi-project):
        ▶ my_project (/path/to/project)
        ├── Models (N)
        │   ├── schema_a (M)
        │   │   ├── model_one
        │   │   └── model_two
        │   └── schema_b (K)
        │       └── model_three
        ├── Sources (N)
        │   └── …
        └── Seeds (N)
            └── …
        ▶ another_project (/other/path)
        └── …

    If there is only a single project the structure is identical – the
    top-level node is just the project name.

    Vim-style navigation is layered on top of the default Tree bindings:
        j / k  — move cursor down / up
        h      — collapse current node or jump to parent
        l      — expand current node or move into first child
        o      — toggle expand / collapse
        enter  — select (emits NodeSelected)
        g      — jump to first node
        G      — jump to last node
        p      — add a new project (fires ``AddProjectRequested`` message)
        d      — remove the project under cursor
        e      — edit the project under cursor
    """

    class AddProjectRequested(Message):
        """Message posted when the user presses the add-project key."""

    class RemoveProjectRequested(Message):
        """Posted when the user requests removal of the project under cursor."""

        def __init__(self, project_path: str) -> None:
            super().__init__()
            self.project_path = project_path

    class EditProjectRequested(Message):
        """Posted when the user requests editing the project under cursor."""

        def __init__(self, project_path: str) -> None:
            super().__init__()
            self.project_path = project_path

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
        # Project management
        Binding("p", "add_project", "Add Project", show=True),
        Binding("e", "edit_project", "Edit Project", show=True),
        Binding("d", "remove_project", "Remove Project", show=False),
    ]

    def __init__(
        self,
        projects: Optional[List] = None,
        include: Iterable[str] = ("model", "source", "seed"),
        **kwargs,
    ):
        """
        Args:
            projects: List of (ProjectEntry, DBTProject) tuples, or None.
            include: resource types to show (subset of "model", "source", "seed").
        """
        super().__init__(**kwargs)
        # List of (ProjectEntry, DBTProject | None) tuples
        self._projects: List[Tuple[Any, Any]] = projects or []
        self.include = set(include)
        self.show_root = False
        self.guide_depth = 3

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def set_projects(self, projects: List[Tuple[Any, Any]]) -> None:
        """Replace the current project list and repopulate."""
        self._projects = list(projects)
        self.populate()

    def add_project(self, entry: Any, dbt_project: Any) -> None:
        """Append a single project and repopulate."""
        self._projects.append((entry, dbt_project))
        self.populate()

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

    def action_add_project(self) -> None:
        """Request adding a new project (handled by the App)."""
        self.post_message(self.AddProjectRequested())

    def action_edit_project(self) -> None:
        """Request editing the project whose tree node is currently selected."""
        project_path = self._project_path_from_cursor()
        if project_path:
            self.post_message(self.EditProjectRequested(project_path))

    def action_remove_project(self) -> None:
        """Request removal of the project whose tree node is currently selected."""
        project_path = self._project_path_from_cursor()
        if project_path:
            self.post_message(self.RemoveProjectRequested(project_path))

    def _project_path_from_cursor(self) -> str | None:
        """Walk up from the cursor node to find the project-level node and return its path."""
        node = self.cursor_node
        if node is None:
            return None

        # Walk up to find the project-level node (direct child of root)
        while node.parent is not None and node.parent is not self.root:
            node = node.parent

        # The project node stores its project_path in data
        if node.parent is self.root and isinstance(node.data, dict):
            return node.data.get("project_path")
        return None

    # ------------------------------------------------------------------
    # Population
    # ------------------------------------------------------------------

    def on_mount(self) -> None:
        self.populate()

    def populate(self) -> None:
        """(Re-)populate the tree from all registered projects."""
        try:
            self.clear()
        except Exception:
            pass

        if not self._projects:
            # Show a helpful placeholder
            self.root.add_leaf("[dim italic]Press [bold]p[/bold] to add a project[/]")
            return

        for entry, dbt_project in self._projects:
            self._add_project_subtree(entry, dbt_project)

    def _add_project_subtree(self, entry: Any, dbt_project: Any) -> None:
        """Add a top-level branch for a single dbt project."""
        project_label = getattr(entry, "display_name", None) or "unknown"
        project_node: TreeNode = self.root.add(
            f"📁 {project_label}",
            expand=True,
            data={"project_path": getattr(entry, "project_path", ""), "entry": entry},
        )

        if dbt_project is None:
            project_node.add_leaf("[dim]manifest not found[/]")
            return

        if "model" in self.include:
            models = dbt_project.get_models()
            self._add_resource_group(project_node, "Models", models)

        if "source" in self.include:
            sources = dbt_project.get_sources()
            self._add_resource_group(project_node, "Sources", sources)

        if "seed" in self.include:
            seeds = dbt_project.get_seeds()
            self._add_resource_group(project_node, "Seeds", seeds)

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
        parent_node: TreeNode,
        label: str,
        items: Dict[str, Dict[str, Any]],
        skip_intermediate: bool = False,
    ) -> None:
        """Add a resource-type branch with schema sub-groups under *parent_node*."""
        total = len(items)
        resource_node: TreeNode = parent_node.add(
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
