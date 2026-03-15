"""Shared UI widgets used across multiple screens."""

from __future__ import annotations

from textual.widgets import Static


class StatusBadge(Static):
    """Small badge showing running / success / error status."""

    DEFAULT_CSS = """
    StatusBadge {
        width: auto;
        height: 1;
        padding: 0 1;
        text-style: bold;
        margin-bottom: 1;
        margin-left: 2;
    }
    StatusBadge.running {
        background: $warning;
        color: $text;
    }
    StatusBadge.success {
        background: $success;
        color: $text;
    }
    StatusBadge.error {
        background: $error;
        color: $text;
    }
    """


class ScreenHeader(Static):
    """Header bar for modal screens."""

    DEFAULT_CSS = """
    ScreenHeader {
        width: 100%;
        height: auto;
        padding: 1 2;
        background: $surface;
        text-style: bold;
        color: $text;
        border-bottom: solid $secondary;
    }
    """
