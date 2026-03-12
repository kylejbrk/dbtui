"""
Modal screen for executing dbt CLI commands with live output streaming.

Provides ``CommandScreen``, a Textual ``ModalScreen`` that:
- Displays the command being run and a spinner/status indicator.
- Streams stdout and stderr into a scrollable log panel in real time.
- Allows the user to dismiss the screen once the command finishes (or cancel early).
- Returns the final exit status to the caller via ``dismiss(result)``.

Usage (from within a Textual App)::

    from dbtui.command_screen import CommandScreen
    from dbtui.dbt import DBTCLI, DBTCommand

    def action_build_node(self) -> None:
        screen = CommandScreen(
            cli=self.cli,
            command=DBTCommand.BUILD,
            node_name="my_model",
            project_path=self.project.project_path,
        )
        self.app.push_screen(screen)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from dbt import DBTCLI, DBTCommand, DBTStreamEvent
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Label, Rule, Static


class _CommandHeader(Static):
    """Displays the command name and current execution status."""

    DEFAULT_CSS = """
    _CommandHeader {
        width: 100%;
        height: auto;
        padding: 1 2;
        background: $surface;
        text-style: bold;
        color: $text;
    }
    """


class _StatusBadge(Static):
    """Small badge showing running / success / error."""

    DEFAULT_CSS = """
    _StatusBadge {
        width: auto;
        height: 1;
        padding: 0 1;
        text-style: bold;
        margin-bottom: 1;
        margin-left: 2;
    }
    _StatusBadge.running {
        background: $warning;
        color: $text;
    }
    _StatusBadge.success {
        background: $success;
        color: $text;
    }
    _StatusBadge.error {
        background: $error;
        color: $text;
    }
    """


class _LogLine(Static):
    """A single line of command output."""

    DEFAULT_CSS = """
    _LogLine {
        width: 100%;
        height: auto;
        padding: 0 2;
        color: $text;
    }
    _LogLine.stderr {
        color: $error;
    }
    _LogLine.status-line {
        color: $accent;
        text-style: italic;
    }
    """

    def __init__(self, content: str, markup: bool = False, **kwargs):
        super().__init__(content, markup=markup, **kwargs)


class CommandScreen(ModalScreen[Optional[int]]):
    """Modal screen that runs a dbt command and streams output.

    The screen covers the terminal with a translucent backdrop.
    A central panel shows the command header, a live-scrolling log area,
    and a footer bar with available actions.

    On dismiss, the screen returns the process exit code (``int``) or
    ``None`` if the command was cancelled before completing.
    """

    DEFAULT_CSS = """
    CommandScreen {
        align: center middle;
    }

    #cmd-dialog {
        width: 90%;
        height: 85%;
        border: thick $accent;
        background: $surface;
        padding: 0;
    }

    #cmd-log-scroll {
        width: 100%;
        height: 1fr;
        background: $panel;
        padding: 0;
    }

    #cmd-footer-hint {
        width: 100%;
        height: 1;
        dock: bottom;
        background: $accent;
        color: $text;
        text-align: center;
        text-style: bold;
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("escape", "close_screen", "Close", show=True),
        Binding("q", "close_screen", "Close", show=True),
    ]

    def __init__(
        self,
        cli: DBTCLI,
        command: DBTCommand,
        node_name: str,
        project_path: Union[str, Path],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cli = cli
        self.command = command
        self.node_name = node_name
        self.project_path = project_path
        self._exit_code: Optional[int] = None
        self._finished = False

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        with Vertical(id="cmd-dialog"):
            cmd_args = self.command.to_args(self.node_name)
            cmd_display = f"dbt {' '.join(cmd_args)}"
            yield _CommandHeader(
                f"⚡ {self.command.display_name}  ─  [bold]{self.node_name}[/bold]",
                id="cmd-header",
            )
            yield Label(
                f"  $ {cmd_display}",
                id="cmd-command-line",
            )
            yield _StatusBadge(" ⏳ RUNNING ", id="cmd-status", classes="running")
            yield Rule(line_style="heavy")
            yield VerticalScroll(id="cmd-log-scroll")
            yield Static(
                " Press [bold]Esc[/bold] or [bold]q[/bold] to close ",
                id="cmd-footer-hint",
            )

    def on_mount(self) -> None:
        self._run_command()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_close_screen(self) -> None:
        """Dismiss the screen, returning the exit code."""
        self.dismiss(self._exit_code)

    # ------------------------------------------------------------------
    # Command execution (Textual worker)
    # ------------------------------------------------------------------

    @work(thread=False, exclusive=True, group="dbt_command")
    async def _run_command(self) -> None:
        """Execute the dbt command asynchronously and stream output to the log."""
        log_scroll = self.query_one("#cmd-log-scroll", VerticalScroll)

        try:
            args = self.command.to_args(self.node_name)
            async for event in self.cli.run_async(args, cwd=self.project_path):
                self._append_log_line(log_scroll, event)
                # Parse exit code from status messages
                if event.stream == "status" and "exit" in event.text:
                    if "exit 0" in event.text:
                        self._exit_code = 0
                    else:
                        # Try to extract exit code
                        try:
                            code_str = event.text.rsplit("exit ", 1)[-1].rstrip(")")
                            self._exit_code = int(code_str)
                        except (ValueError, IndexError):
                            self._exit_code = 1

        except FileNotFoundError as exc:
            await log_scroll.mount(_LogLine(f"ERROR: {exc}", classes="stderr"))
            self._exit_code = 127

        except Exception as exc:
            await log_scroll.mount(_LogLine(f"ERROR: {exc}", classes="stderr"))
            self._exit_code = 1

        # Update status badge
        self._finished = True
        badge = self.query_one("#cmd-status", _StatusBadge)
        if self._exit_code == 0:
            badge.update(" ✔ SUCCESS ")
            badge.remove_class("running")
            badge.add_class("success")
        else:
            badge.update(f" ✘ FAILED (exit {self._exit_code}) ")
            badge.remove_class("running")
            badge.add_class("error")

    def _append_log_line(
        self, log_scroll: VerticalScroll, event: DBTStreamEvent
    ) -> None:
        """Mount a new log line widget and auto-scroll to bottom."""
        if event.stream == "stderr":
            css_class = "stderr"
        elif event.stream == "status":
            css_class = "status-line"
        else:
            css_class = ""

        # Status lines are ours (safe markup); stdout/stderr come from dbt (no markup)
        use_markup = event.stream == "status"
        line = _LogLine(event.text, markup=use_markup, classes=css_class)
        log_scroll.mount(line)
        line.scroll_visible(animate=False)
