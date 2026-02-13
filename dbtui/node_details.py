from __future__ import annotations

from typing import Any

from textual.containers import VerticalScroll
from textual.widgets import Label, Pretty


class NodeDetailsWidget(VerticalScroll):
    """Details panel for the currently selected dbt node.

    Keep mounted child widgets stable and only update their content.
    This pattern is simple and easy to extend with additional sections.
    """

    def __init__(self, node_details: dict[str, Any] | None = None, **kwargs):
        super().__init__(**kwargs)
        self._node_details: dict[str, Any] = node_details or {}

    def compose(self):
        yield Label("Node Details", id="node_details_title")
        yield Label("", id="node_details_summary")
        yield Pretty(self._node_details, id="node_details_pretty")

    def _format_value(self, value: Any) -> str:
        if value is None or value == "":
            return "N/A"
        return str(value)

    def _build_summary(self, details: dict[str, Any]) -> str:
        resource_type = self._format_value(details.get("resource_type"))
        name = self._format_value(details.get("name"))
        path = self._format_value(
            details.get("original_file_path") or details.get("path")
        )
        return f"Type: {resource_type} | Name: {name} | Path: {path}"

    def update_details(self, node_details: dict[str, Any] | None) -> None:
        self._node_details = node_details or {}

        summary = self.query_one("#node_details_summary", Label)
        summary.update(self._build_summary(self._node_details))

        pretty = self.query_one("#node_details_pretty", Pretty)
        pretty.update(self._node_details)

    def clear_details(self) -> None:
        self.update_details({})
