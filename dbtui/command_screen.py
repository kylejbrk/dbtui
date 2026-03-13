"""
Modal screen for executing dbt CLI commands with live output streaming.

Provides ``CommandScreen``, a Textual ``ModalScreen`` that:
- Displays the command being run and a spinner/status indicator.
- Streams stdout and stderr into a scrollable log panel in real time.
- Allows the user to dismiss the screen once the command finishes (or cancel early).
- Returns the final exit status to the caller via ``dismiss(result)``.

Usage (from within a Textual App)::

    from dbtui.command_screen import CommandScreen
    from dbtui.dbt_client import DBTCLI, DBTCommand

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

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, RichLog, Rule, Static

from dbtui.dbt_client import DBTCLI, DBTCommand, DBTStreamEvent
from dbtui.widgets import ScreenHeader, StatusBadge


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

    #cmd-log {
        width: 100%;
        height: 1fr;
        background: $panel;
        padding: 0 2;
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
            yield ScreenHeader(
                f"⚡ {self.command.display_name}  ─  [bold]{self.node_name}[/bold]",
                id="cmd-header",
            )
            yield Label(
                f"  $ {cmd_display}",
                id="cmd-command-line",
            )
            yield StatusBadge(" ⏳ RUNNING ", id="cmd-status", classes="running")
            yield Rule(line_style="heavy")
            yield RichLog(id="cmd-log", wrap=True, highlight=False, markup=False)
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
        """Cancel any running command and dismiss the screen."""
        self.workers.cancel_group(self, "dbt_command")
        self.dismiss(self._exit_code)

    # ------------------------------------------------------------------
    # Command execution (Textual worker)
    # ------------------------------------------------------------------

    @work(thread=False, exclusive=True, group="dbt_command")
    async def _run_command(self) -> None:
        """Execute the dbt command asynchronously and stream output to the log."""
        log = self.query_one("#cmd-log", RichLog)

        try:
            args = self.command.to_args(self.node_name)
            async for event in self.cli.run_async(args, cwd=self.project_path):
                self._append_log_line(log, event)
                if event.exit_code is not None:
                    self._exit_code = event.exit_code

        except FileNotFoundError as exc:
            log.write(Text(f"ERROR: {exc}", style="bold red"))
            self._exit_code = 127

        except Exception as exc:
            log.write(Text(f"ERROR: {exc}", style="bold red"))
            self._exit_code = 1

        # Update status badge
        self._finished = True
        badge = self.query_one("#cmd-status", StatusBadge)
        if self._exit_code == 0:
            badge.update(" ✔ SUCCESS ")
            badge.remove_class("running")
            badge.add_class("success")
        else:
            badge.update(f" ✘ FAILED (exit {self._exit_code}) ")
            badge.remove_class("running")
            badge.add_class("error")

    def _append_log_line(self, log: RichLog, event: DBTStreamEvent) -> None:
        """Append a line to the log widget."""
        if event.stream == "stderr":
            log.write(Text(event.text, style="red"))
        elif event.stream == "status":
            log.write(Text(event.text, style="italic cyan"))
        else:
            log.write(event.text)
