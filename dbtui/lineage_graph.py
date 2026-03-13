"""
Lineage graph widget – git-graph style DAG visualisation.

Shows the currently selected node in the centre (vertically), with parent
nodes stacked above and child nodes below.  Nodes are connected by pipe
characters to convey the lineage, similar to ``git log --graph``.

Each node line is selectable (highlighted on hover / cursor) so the user
can navigate the graph and jump to related nodes.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from textual import on
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.message import Message
from textual.widgets import Static

logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────


def _short_name(unique_id: str) -> str:
    """Extract a human-friendly short name from a dbt unique_id.

    ``model.jaffle_shop.stg_customers`` → ``stg_customers``
    ``source.jaffle_shop.raw.customers`` → ``raw.customers``
    """
    parts = unique_id.split(".")
    if len(parts) >= 2:
        resource_type = parts[0]
        if resource_type == "source" and len(parts) >= 4:
            return ".".join(parts[2:])
        return parts[-1]
    return unique_id


def _resource_icon(unique_id: str) -> str:
    """Return a small icon prefix based on resource type."""
    if unique_id.startswith("model."):
        return "⬡"
    if unique_id.startswith("source."):
        return "◈"
    if unique_id.startswith("seed."):
        return "◇"
    if unique_id.startswith("test."):
        return "◎"
    if unique_id.startswith("snapshot."):
        return "◫"
    if unique_id.startswith("exposure."):
        return "◉"
    return "○"


# ── Styled line widgets ─────────────────────────────────────────────


class _PipeLine(Static):
    """A non-selectable pipe connector line (``│``)."""

    DEFAULT_CSS = """
    _PipeLine {
        height: 1;
        width: 100%;
        color: $text-muted;
        padding: 0 0 0 4;
    }
    """


class _GraphNode(Static):
    """A selectable node row in the lineage graph."""

    can_focus = True

    DEFAULT_CSS = """
    _GraphNode {
        height: 1;
        width: 100%;
        padding: 0 0 0 2;
    }

    _GraphNode:hover {
        background: $boost;
    }

    _GraphNode:focus {
        background: $accent 30%;
        text-style: bold;
    }

    _GraphNode.ancestor {
        color: $text-muted;
    }

    _GraphNode.selected-node {
        color: $text;
        text-style: bold;
        background: $accent 20%;
    }

    _GraphNode.descendant {
        color: $text-muted;
    }
    """

    class Clicked(Message):
        """Fired when a graph node is clicked or activated."""

        def __init__(self, unique_id: str) -> None:
            super().__init__()
            self.unique_id = unique_id

    def __init__(self, unique_id: str, label: str, css_class: str = "", **kwargs):
        super().__init__(label, **kwargs)
        self._unique_id = unique_id
        if css_class:
            self.add_class(css_class)

    def on_click(self) -> None:
        self.post_message(self.Clicked(self._unique_id))

    def on_key(self, event) -> None:
        if event.key == "enter":
            self.post_message(self.Clicked(self._unique_id))
            event.stop()


class _SectionLabel(Static):
    """Dim label for "Parents" / "Children" section headers."""

    DEFAULT_CSS = """
    _SectionLabel {
        height: 1;
        width: 100%;
        color: $text-muted;
        text-style: italic;
        padding: 0 0 0 1;
        margin-top: 1;
    }
    """


class _EmptyGraphState(Static):
    """Shown when there's no node selected or no lineage data."""

    DEFAULT_CSS = """
    _EmptyGraphState {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: $text-muted;
        text-style: italic;
    }
    """


# ── Main widget ─────────────────────────────────────────────────────


class LineageGraphWidget(VerticalScroll):
    """Git-graph style lineage visualisation for a dbt node.

    Layout (top to bottom):
        ─ ancestors (furthest first)
        │
        ● selected node
        │
        ─ descendants (nearest first)
    """

    BINDINGS = [
        Binding("j", "scroll_down", "Down", show=False),
        Binding("k", "scroll_up", "Up", show=False),
        Binding("g", "scroll_home", "Top", show=False),
        Binding("G", "scroll_end", "Bottom", show=False, key_display="G"),
        Binding("escape", "focus_sidebar", "Back to Sidebar", show=False),
    ]

    DEFAULT_CSS = """
    LineageGraphWidget {
        padding: 1 2;
    }
    """

    class NodeNavigated(Message):
        """Posted when the user clicks a node in the graph to navigate to it.

        The app should look up the full node data by ``unique_id`` and update
        both the sidebar selection and the details panel.
        """

        def __init__(self, unique_id: str) -> None:
            super().__init__()
            self.unique_id = unique_id

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._node_details: Dict[str, Any] = {}
        self._all_nodes: Dict[str, Dict[str, Any]] = {}
        self._all_sources: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_manifest_data(
        self,
        all_nodes: Dict[str, Dict[str, Any]],
        all_sources: Dict[str, Dict[str, Any]],
    ) -> None:
        """Provide the full node and source maps so the widget can resolve lineage."""
        self._all_nodes = all_nodes or {}
        self._all_sources = all_sources or {}

    def update_node(self, node_details: Dict[str, Any]) -> None:
        """Update the graph for a new selected node."""
        self._node_details = node_details or {}
        self._rebuild_graph()

    def clear_node(self) -> None:
        self._node_details = {}
        self._rebuild_graph()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_focus_sidebar(self) -> None:
        sidebar = self.app.query_one("#sidebar")
        sidebar.focus()

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    @on(_GraphNode.Clicked)
    def _on_graph_node_clicked(self, event: _GraphNode.Clicked) -> None:
        self.post_message(self.NodeNavigated(event.unique_id))

    # ------------------------------------------------------------------
    # Graph building
    # ------------------------------------------------------------------

    def _lookup_node(self, uid: str) -> Optional[Dict[str, Any]]:
        """Resolve a unique_id to its node dict."""
        if uid in self._all_nodes:
            return self._all_nodes[uid]
        if uid in self._all_sources:
            return self._all_sources[uid]
        return None

    def _get_parent_ids(self, node: Dict[str, Any]) -> List[str]:
        """Return parent unique_ids for a node."""
        dep = node.get("depends_on") or {}
        nodes = dep.get("nodes") or []
        # Filter out tests – they clutter the graph
        return [n for n in nodes if not n.startswith("test.")]

    def _get_child_ids(self, node: Dict[str, Any]) -> List[str]:
        """Return child unique_ids for a node."""
        children = node.get("child_ids") or []
        return [c for c in children if not c.startswith("test.")]

    def _collect_ancestors(
        self, start_uid: str, max_depth: int = 20
    ) -> List[List[str]]:
        """BFS upward, returning layers of ancestor unique_ids.

        Returns a list of layers where index 0 is the immediate parents,
        index 1 is grandparents, etc.
        """
        visited: Set[str] = {start_uid}
        layers: List[List[str]] = []
        current_layer = [start_uid]

        for _ in range(max_depth):
            next_layer: List[str] = []
            for uid in current_layer:
                node = self._lookup_node(uid)
                if node is None:
                    continue
                for pid in self._get_parent_ids(node):
                    if pid not in visited:
                        visited.add(pid)
                        next_layer.append(pid)
            if not next_layer:
                break
            layers.append(next_layer)
            current_layer = next_layer

        return layers

    def _collect_descendants(
        self, start_uid: str, max_depth: int = 20
    ) -> List[List[str]]:
        """BFS downward, returning layers of descendant unique_ids."""
        visited: Set[str] = {start_uid}
        layers: List[List[str]] = []
        current_layer = [start_uid]

        for _ in range(max_depth):
            next_layer: List[str] = []
            for uid in current_layer:
                node = self._lookup_node(uid)
                if node is None:
                    continue
                for cid in self._get_child_ids(node):
                    if cid not in visited:
                        visited.add(cid)
                        next_layer.append(cid)
            if not next_layer:
                break
            layers.append(next_layer)
            current_layer = next_layer

        return layers

    def _rebuild_graph(self) -> None:
        """Tear down and rebuild the visual graph."""
        # Remove all existing children
        self.remove_children()

        details = self._node_details
        if not details:
            self.mount(_EmptyGraphState("Select a node to view its lineage"))
            return

        uid = details.get("unique_id", "")
        if not uid:
            self.mount(_EmptyGraphState("Node has no unique_id"))
            return

        # ── Collect lineage ──────────────────────────────────────
        ancestor_layers = self._collect_ancestors(uid)
        descendant_layers = self._collect_descendants(uid)

        widgets_to_mount = []

        # ── Ancestors (furthest first → reversed) ────────────────
        if ancestor_layers:
            widgets_to_mount.append(
                _SectionLabel(
                    f"  ↑ Upstream ({sum(len(layer) for layer in ancestor_layers)})"
                )
            )
            # Reverse so the most distant ancestors are at the top
            for depth, layer in enumerate(reversed(ancestor_layers)):
                indent_level = depth
                for i, ancestor_uid in enumerate(layer):
                    icon = _resource_icon(ancestor_uid)
                    name = _short_name(ancestor_uid)
                    # Build the visual prefix
                    prefix = self._make_prefix(
                        indent_level, is_first=(i == 0), is_last=(i == len(layer) - 1)
                    )
                    label = f"{prefix}{icon} {name}"
                    widgets_to_mount.append(
                        _GraphNode(
                            ancestor_uid,
                            label,
                            css_class="ancestor",
                        )
                    )
                # Pipe connector between layers
                pipe_prefix = "    " * (indent_level + 1) + "│"
                widgets_to_mount.append(_PipeLine(pipe_prefix))

        # ── Selected node ────────────────────────────────────────
        icon = _resource_icon(uid)
        name = details.get("name") or _short_name(uid)
        resource_type = (details.get("resource_type") or "").upper()
        selected_label = f"  ● {icon} [bold]{name}[/bold]  [{resource_type}]"
        widgets_to_mount.append(
            _GraphNode(uid, selected_label, css_class="selected-node")
        )

        # ── Descendants (nearest first) ──────────────────────────
        if descendant_layers:
            # Pipe from selected node to first child layer
            widgets_to_mount.append(_PipeLine("    │"))
            widgets_to_mount.append(
                _SectionLabel(
                    f"  ↓ Downstream ({sum(len(layer) for layer in descendant_layers)})"
                )
            )
            for depth, layer in enumerate(descendant_layers):
                indent_level = depth
                for i, desc_uid in enumerate(layer):
                    icon = _resource_icon(desc_uid)
                    name = _short_name(desc_uid)
                    prefix = self._make_prefix(
                        indent_level, is_first=(i == 0), is_last=(i == len(layer) - 1)
                    )
                    label = f"{prefix}{icon} {name}"
                    widgets_to_mount.append(
                        _GraphNode(
                            desc_uid,
                            label,
                            css_class="descendant",
                        )
                    )
                # Pipe connector between descendant layers (unless last)
                if depth < len(descendant_layers) - 1:
                    pipe_prefix = "    " * (indent_level + 1) + "│"
                    widgets_to_mount.append(_PipeLine(pipe_prefix))

        if not ancestor_layers and not descendant_layers:
            widgets_to_mount.append(_PipeLine(""))
            widgets_to_mount.append(_SectionLabel("  No upstream or downstream nodes"))

        self.mount_all(widgets_to_mount)

        # Scroll so the selected node is roughly centred
        self.call_after_refresh(self._scroll_to_selected)

    def _scroll_to_selected(self) -> None:
        """Scroll so that the selected node is visible, roughly centred."""
        for child in self.children:
            if isinstance(child, _GraphNode) and child.has_class("selected-node"):
                self.scroll_to_widget(child, animate=False, top=True)
                break

    @staticmethod
    def _make_prefix(indent_level: int, is_first: bool, is_last: bool) -> str:
        """Build a git-graph style prefix string.

        Uses box-drawing characters to mimic ``git log --graph``::

            ├── node_a
            ├── node_b
            └── node_c
        """
        base = "    " * indent_level
        if is_first and is_last:
            # Single node in layer
            return f"{base}  ├─ "
        if is_first:
            return f"{base}  ├─ "
        if is_last:
            return f"{base}  └─ "
        return f"{base}  ├─ "
