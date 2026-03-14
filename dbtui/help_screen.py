"""Help screen showing all available keybindings."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Label, Rule, Static

_HELP_TEXT = """\
[bold #FF694B]── Global ──[/]
  [bold]q[/]         Quit
  [bold]Tab[/]       Switch pane
  [bold]\\[/]         Toggle sidebar
  [bold]1[/]         Focus sidebar
  [bold]2[/]         Focus details
  [bold]?[/]         Show this help

[bold #FF694B]── Sidebar (Tree) ──[/]
  [bold]j / k[/]     Move cursor down / up
  [bold]h[/]         Collapse node or go to parent
  [bold]l[/]         Expand node or go to first child
  [bold]o[/]         Toggle expand / collapse
  [bold]Enter[/]     Select node
  [bold]g / G[/]     Jump to first / last node
  [bold]p[/]         Add project
  [bold]e[/]         Edit project
  [bold]x[/]         Remove project

[bold #FF694B]── Node Details ──[/]
  [bold]j / k[/]     Scroll down / up
  [bold]g / G[/]     Scroll to top / bottom
  [bold]Escape[/]    Back to sidebar

[bold #FF694B]── dbt Commands (in Details pane) ──[/]
  [bold]b[/]         Build
  [bold]B[/]         Build +upstream
  [bold]d[/]         Build downstream+
  [bold]F[/]         Build +full+
  [bold]c[/]         Compile
  [bold]r[/]         Run
  [bold]t[/]         Test
  [bold]s[/]         Show (preview data)

[bold #FF694B]── Modal Screens ──[/]
  [bold]Escape[/]    Close / Cancel
  [bold]q[/]         Close command output
"""


class HelpScreen(ModalScreen[None]):
    """Modal overlay displaying all keybindings."""

    DEFAULT_CSS = """
    HelpScreen {
        align: center middle;
    }

    #help-dialog {
        width: 60;
        height: 80%;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }

    #help-title {
        text-style: bold;
        width: 100%;
        text-align: center;
        margin-bottom: 1;
        color: $accent;
    }

    #help-content {
        width: 100%;
        height: 1fr;
    }

    #help-footer {
        width: 100%;
        height: 1;
        dock: bottom;
        text-align: center;
        color: $text-muted;
    }
    """

    BINDINGS = [
        Binding("escape", "close", "Close", show=False),
        Binding("q", "close", "Close", show=False),
        Binding("question_mark", "close", "Close", show=False),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(id="help-dialog"):
            yield Label("⌨  Keybindings", id="help-title")
            yield Rule()
            with VerticalScroll(id="help-content"):
                yield Static(_HELP_TEXT, markup=True)
            yield Static(
                "Press [bold]Esc[/bold] or [bold]?[/bold] to close",
                id="help-footer",
                markup=True,
            )

    def action_close(self) -> None:
        self.dismiss(None)
