"""Simple yes/no confirmation modal."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static


class ConfirmModal(ModalScreen[bool]):
    """Modal dialog asking for yes/no confirmation.

    Returns ``True`` if confirmed, ``False`` if cancelled.
    """

    DEFAULT_CSS = """
    ConfirmModal {
        align: center middle;
    }

    #confirm-dialog {
        width: 50;
        height: auto;
        max-height: 12;
        border: thick $error;
        background: $surface;
        padding: 1 2;
    }

    #confirm-title {
        text-style: bold;
        width: 100%;
        content-align: center middle;
        text-align: center;
        margin-bottom: 1;
    }

    #confirm-message {
        width: 100%;
        text-align: center;
        margin-bottom: 1;
    }

    #confirm-buttons {
        width: 100%;
        height: auto;
        align: center middle;
    }

    #confirm-buttons Button {
        margin: 0 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=False),
        Binding("y", "confirm", "Yes", show=False),
        Binding("n", "cancel", "No", show=False),
    ]

    def __init__(self, message: str, title: str = "Confirm", **kwargs) -> None:
        super().__init__(**kwargs)
        self._message = message
        self._title = title

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Label(self._title, id="confirm-title")
            yield Static(self._message, id="confirm-message")
            with Horizontal(id="confirm-buttons"):
                yield Button("Yes", variant="error", id="btn-yes")
                yield Button("No", variant="default", id="btn-no")

    def on_mount(self) -> None:
        self.query_one("#btn-no", Button).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "btn-yes")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)
