from __future__ import annotations

from typing import Any

from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.message import Message
from textual.widgets import Collapsible, DataTable, Label, Rule, Static

from dbtui.dbt_client import DBTCommand


class _Badge(Static):
    """Small inline badge for resource type."""

    DEFAULT_CSS = """
    _Badge {
        background: $accent;
        color: $text;
        padding: 0 1;
        text-style: bold;
        width: auto;
        height: 1;
    }
    """


class _SectionTitle(Static):
    """Styled section heading."""

    DEFAULT_CSS = """
    _SectionTitle {
        text-style: bold;
        color: $accent;
        margin-top: 1;
        margin-bottom: 0;
        width: 100%;
    }
    """


class _MetaKey(Static):
    """Label for a metadata key."""

    DEFAULT_CSS = """
    _MetaKey {
        color: $text-muted;
        width: 18;
        text-style: bold;
    }
    """


class _MetaValue(Static):
    """Label for a metadata value."""

    DEFAULT_CSS = """
    _MetaValue {
        width: 1fr;
    }
    """


class _MetaRow(Horizontal):
    """A single key-value row in the metadata section."""

    DEFAULT_CSS = """
    _MetaRow {
        height: 1;
        width: 100%;
    }
    """

    def __init__(self, key: str, value: str, row_id: str, **kwargs):
        super().__init__(**kwargs)
        self._key = key
        self._value = value
        self._row_id = row_id

    def compose(self):
        yield _MetaKey(f"  {self._key}")
        yield _MetaValue(self._value, id=self._row_id)


class _EmptyState(Static):
    """Shown when no node is selected."""

    DEFAULT_CSS = """
    _EmptyState {
        width: 100%;
        height: 100%;
        content-align: center middle;
        text-align: center;
        color: $text-muted;
        text-style: italic;
        padding-top: 3;
    }
    """


class _CommandHint(Static):
    """Displays available command key bindings for the selected node."""

    DEFAULT_CSS = """
    _CommandHint {
        width: 100%;
        height: auto;
        padding: 0 2;
        color: $text-muted;
    }
    """


class NodeDetailsWidget(VerticalScroll):
    """Details panel for the currently selected dbt node.

    Displays node information in clean, organised sections:
      - Header with name and resource-type badge
      - Command shortcuts bar
      - Metadata grid (database, schema, materialization, path, tags, etc.)
      - Description
      - Columns table
      - Dependencies (parents / children)
      - SQL code (raw and compiled) in collapsible sections

    Key bindings (active when a node is selected):
        b — Build the selected node
        B — Build +upstream
        d — Build downstream+
        F — Build +full+
        c — Compile
        r — Run
        t — Test
        s — Show (preview query results)
    """

    # ------------------------------------------------------------------
    # Messages
    # ------------------------------------------------------------------

    class CommandRequested(Message):
        """Posted when the user triggers a dbt command on the current node.

        Attributes:
            command: The ``DBTCommand`` enum member.
            node_name: The name of the selected dbt node.
            node_details: The full node dict for context.
        """

        def __init__(
            self,
            command: DBTCommand,
            node_name: str,
            node_details: dict[str, Any],
        ) -> None:
            super().__init__()
            self.command = command
            self.node_name = node_name
            self.node_details = node_details

    # ------------------------------------------------------------------
    # Bindings
    # ------------------------------------------------------------------

    BINDINGS = [
        # Vim-style scrolling
        Binding("j", "scroll_down", "Scroll Down", show=False),
        Binding("k", "scroll_up", "Scroll Up", show=False),
        Binding("g", "scroll_home", "Top", show=False),
        Binding("G", "scroll_end", "Bottom", show=False, key_display="G"),
        # dbt commands
        Binding("b", "dbt_build", "Build", show=True),
        Binding(
            "B", "dbt_build_upstream", "Build +upstream", show=True, key_display="B"
        ),
        Binding("d", "dbt_build_downstream", "Build downstream+", show=True),
        Binding("F", "dbt_build_full", "Build +full+", show=True, key_display="F"),
        Binding("c", "dbt_compile", "Compile", show=True),
        Binding("r", "dbt_run", "Run", show=True),
        Binding("t", "dbt_test", "Test", show=True),
        Binding("s", "dbt_show", "Show", show=True),
        Binding("escape", "focus_sidebar", "Back to Sidebar", show=False),
    ]

    DEFAULT_CSS = """
    NodeDetailsWidget {
        padding: 1 2;
    }

    #nd-header {
        width: 100%;
        height: auto;
    }

    #nd-node-name {
        text-style: bold;
        width: 1fr;
    }

    #nd-header-row {
        height: 1;
        width: 100%;
        margin-bottom: 0;
    }

    #nd-commands-section {
        width: 100%;
        height: auto;
        margin-bottom: 0;
    }

    #nd-description-text {
        color: $text;
        margin-left: 2;
        margin-bottom: 1;
        width: 100%;
    }

    #nd-columns-table {
        margin-left: 2;
        margin-bottom: 1;
        max-height: 20;
        width: 100%;
    }

    #nd-depends-on-section {
        margin-left: 2;
        margin-bottom: 1;
        width: 100%;
    }

    #nd-dependents-section {
        margin-left: 2;
        margin-bottom: 1;
        width: 100%;
    }

    .nd-dep-item {
        color: $text;
        margin-left: 0;
        height: 1;
    }

    .nd-sql-block {
        padding: 1 2;
        background: $surface;
        margin-left: 2;
        margin-bottom: 1;
        width: 100%;
        overflow-x: auto;
    }
    """

    def __init__(self, node_details: dict[str, Any] | None = None, **kwargs):
        super().__init__(**kwargs)
        self._node_details: dict[str, Any] = node_details or {}
        self._current_node_name: str | None = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fmt(value: Any) -> str:
        """Format a value for display, returning 'N/A' for empty values."""
        if value is None or value == "" or value == []:
            return "—"
        if isinstance(value, list):
            return ", ".join(str(v) for v in value)
        return str(value)

    @staticmethod
    def _resource_label(resource_type: str) -> str:
        return resource_type.upper() if resource_type else "UNKNOWN"

    def _get_materialized(self, details: dict[str, Any]) -> str:
        config = details.get("config") or {}
        mat = config.get("materialized") or config.get("materialization")
        return self._fmt(mat)

    def _get_tags(self, details: dict[str, Any]) -> str:
        tags = details.get("tags")
        return self._fmt(tags)

    def _get_columns(self, details: dict[str, Any]) -> list[dict[str, Any]]:
        """Return a list of column dicts from the node details."""
        raw = details.get("columns") or {}
        if isinstance(raw, dict):
            return list(raw.values())
        if isinstance(raw, list):
            return raw
        return []

    def _get_depends_on_nodes(self, details: dict[str, Any]) -> list[str]:
        dep = details.get("depends_on") or {}
        nodes = dep.get("nodes") or []
        return [str(n) for n in nodes] if nodes else []

    def _get_children(self, details: dict[str, Any]) -> list[str]:
        return [str(c) for c in (details.get("child_ids") or [])]

    def _get_sql(self, details: dict[str, Any], kind: str = "raw") -> str:
        """Get SQL code from node. kind is 'raw' or 'compiled'."""
        if kind == "raw":
            return details.get("raw_code") or details.get("raw_sql") or ""
        return details.get("compiled_code") or details.get("compiled_sql") or ""

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self):
        # Empty state (shown until a node is selected)
        yield _EmptyState(
            "Select a node from the sidebar to view details", id="nd-empty"
        )

        # All detail sections live inside a hidden container
        with Vertical(id="nd-content"):
            # -- Header --
            with Horizontal(id="nd-header-row"):
                yield Label("", id="nd-node-name")
                yield _Badge("", id="nd-badge")

            yield Rule(line_style="heavy")

            # -- Commands --
            yield _SectionTitle("🚀  Commands")
            yield _CommandHint("", id="nd-commands-section")

            yield Rule()

            # -- Metadata --
            yield _SectionTitle("ℹ  Metadata")
            yield _MetaRow("Database", "—", "nd-meta-database")
            yield _MetaRow("Schema", "—", "nd-meta-schema")
            yield _MetaRow("Materialized", "—", "nd-meta-materialized")
            yield _MetaRow("Package", "—", "nd-meta-package")
            yield _MetaRow("Path", "—", "nd-meta-path")
            yield _MetaRow("Tags", "—", "nd-meta-tags")
            yield _MetaRow("Unique ID", "—", "nd-meta-uid")

            yield Rule()

            # -- Description --
            yield _SectionTitle("📝  Description")
            yield Static("—", id="nd-description-text")

            yield Rule()

            # -- Columns --
            yield _SectionTitle("🏛  Columns")
            yield DataTable(id="nd-columns-table", show_cursor=False)

            yield Rule()

            # -- Dependencies --
            yield _SectionTitle("⬆  Depends On")
            yield Vertical(id="nd-depends-on-section")

            yield _SectionTitle("⬇  Referenced By")
            yield Vertical(id="nd-dependents-section")

            yield Rule()

            # -- SQL --
            with Collapsible(
                title="Raw SQL", collapsed=True, id="nd-raw-sql-collapsible"
            ):
                yield Static("", id="nd-raw-sql", classes="nd-sql-block")

            with Collapsible(
                title="Compiled SQL", collapsed=True, id="nd-compiled-sql-collapsible"
            ):
                yield Static("", id="nd-compiled-sql", classes="nd-sql-block")

    def on_mount(self):
        # Set up columns table headers
        table = self.query_one("#nd-columns-table", DataTable)
        table.add_columns("Column", "Type", "Description")

        # Hide content initially, show empty state
        self.query_one("#nd-content").display = False

        # If we already have details, render them
        if self._node_details:
            self._refresh_content(self._node_details)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_details(self, node_details: dict[str, Any] | None) -> None:
        self._node_details = node_details or {}
        self._current_node_name = (
            self._node_details.get("name")
            or self._node_details.get("unique_id")
            or None
        )
        self._refresh_content(self._node_details)

    def clear_details(self) -> None:
        self.update_details({})

    # ------------------------------------------------------------------
    # dbt command actions
    # ------------------------------------------------------------------

    def _post_command(self, command: DBTCommand) -> None:
        """Post a ``CommandRequested`` message if a node is currently selected."""
        if not self._node_details or not self._current_node_name:
            self.notify("No node selected", severity="warning")
            return
        resource_type = self._node_details.get("resource_type", "")
        available = DBTCommand.for_resource_type(resource_type)
        if command not in available:
            self.notify(
                f"'{command.display_name}' is not available for {resource_type} nodes",
                severity="warning",
            )
            return
        self.post_message(
            self.CommandRequested(
                command=command,
                node_name=self._current_node_name,
                node_details=self._node_details,
            )
        )

    def action_focus_sidebar(self) -> None:
        """Refocus the sidebar."""
        sidebar = self.screen.query_one("#sidebar")
        if sidebar:
            sidebar.focus()

    def action_dbt_build(self) -> None:
        self._post_command(DBTCommand.BUILD)

    def action_dbt_build_upstream(self) -> None:
        self._post_command(DBTCommand.BUILD_UPSTREAM)

    def action_dbt_build_downstream(self) -> None:
        self._post_command(DBTCommand.BUILD_DOWNSTREAM)

    def action_dbt_build_full(self) -> None:
        self._post_command(DBTCommand.BUILD_FULL)

    def action_dbt_compile(self) -> None:
        self._post_command(DBTCommand.COMPILE)

    def action_dbt_run(self) -> None:
        self._post_command(DBTCommand.RUN)

    def action_dbt_test(self) -> None:
        self._post_command(DBTCommand.TEST)

    def action_dbt_show(self) -> None:
        self._post_command(DBTCommand.SHOW)

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _refresh_content(self, details: dict[str, Any]) -> None:
        """Re-render every section to reflect *details*."""
        has_data = bool(details)

        # Toggle empty state vs content
        self.query_one("#nd-empty").display = not has_data
        self.query_one("#nd-content").display = has_data

        if not has_data:
            return

        with self.app.batch_update():
            # -- Header --
            name = details.get("name") or details.get("unique_id") or "Unnamed"
            resource_type = details.get("resource_type") or "unknown"

            self.query_one("#nd-node-name", Label).update(f"  {name}")

            # -- Commands hint --
            cmds = DBTCommand.for_resource_type(resource_type)
            _key_map = {
                DBTCommand.BUILD: "b",
                DBTCommand.BUILD_UPSTREAM: "B",
                DBTCommand.BUILD_DOWNSTREAM: "d",
                DBTCommand.BUILD_FULL: "F",
                DBTCommand.COMPILE: "c",
                DBTCommand.RUN: "r",
                DBTCommand.TEST: "t",
                DBTCommand.SHOW: "s",
            }
            hints = "  ".join(
                f"[bold]{_key_map.get(c, '?')}[/bold]={c.display_name}" for c in cmds
            )
            self.query_one("#nd-commands-section", _CommandHint).update(f"  {hints}")
            badge = self.query_one("#nd-badge", _Badge)
            badge.update(f" {self._resource_label(resource_type)} ")

            # -- Metadata rows --
            self.query_one("#nd-meta-database", _MetaValue).update(
                self._fmt(details.get("database"))
            )
            self.query_one("#nd-meta-schema", _MetaValue).update(
                self._fmt(details.get("schema"))
            )
            self.query_one("#nd-meta-materialized", _MetaValue).update(
                self._get_materialized(details)
            )
            self.query_one("#nd-meta-package", _MetaValue).update(
                self._fmt(details.get("package_name"))
            )
            self.query_one("#nd-meta-path", _MetaValue).update(
                self._fmt(details.get("original_file_path") or details.get("path"))
            )
            self.query_one("#nd-meta-tags", _MetaValue).update(self._get_tags(details))
            self.query_one("#nd-meta-uid", _MetaValue).update(
                self._fmt(details.get("unique_id"))
            )

            # -- Description --
            desc = details.get("description") or "—"
            self.query_one("#nd-description-text", Static).update(desc)

            # -- Columns table --
            table = self.query_one("#nd-columns-table", DataTable)
            table.clear()
            columns = self._get_columns(details)
            if columns:
                for col in columns:
                    col_name = (
                        col.get("name", "—") if isinstance(col, dict) else str(col)
                    )
                    col_type = (
                        col.get("data_type") or col.get("dtype") or col.get("type", "—")
                        if isinstance(col, dict)
                        else "—"
                    )
                    col_desc = (
                        col.get("description", "—") if isinstance(col, dict) else "—"
                    )
                    table.add_row(
                        str(col_name),
                        str(col_type) if col_type else "—",
                        str(col_desc) if col_desc else "—",
                    )
                table.display = True
            else:
                table.display = False

            # -- Depends On --
            depends_container = self.query_one("#nd-depends-on-section", Vertical)
            depends_container.remove_children()
            parents = self._get_depends_on_nodes(details)
            if parents:
                depends_container.mount_all(
                    [Label(f"  • {p}", classes="nd-dep-item") for p in parents]
                )
            else:
                depends_container.mount(Label("  —", classes="nd-dep-item"))

            # -- Referenced By --
            children_container = self.query_one("#nd-dependents-section", Vertical)
            children_container.remove_children()
            children = self._get_children(details)
            if children:
                children_container.mount_all(
                    [Label(f"  • {c}", classes="nd-dep-item") for c in children]
                )
            else:
                children_container.mount(Label("  —", classes="nd-dep-item"))

            # -- SQL --
            raw_sql = self._get_sql(details, "raw")
            compiled_sql = self._get_sql(details, "compiled")

            raw_collapsible = self.query_one("#nd-raw-sql-collapsible", Collapsible)
            compiled_collapsible = self.query_one(
                "#nd-compiled-sql-collapsible", Collapsible
            )

            if raw_sql.strip():
                self.query_one("#nd-raw-sql", Static).update(raw_sql)
                raw_collapsible.display = True
            else:
                raw_collapsible.display = False

            if compiled_sql.strip():
                self.query_one("#nd-compiled-sql", Static).update(compiled_sql)
                compiled_collapsible.display = True
            else:
                compiled_collapsible.display = False

            self.scroll_home(animate=False)
