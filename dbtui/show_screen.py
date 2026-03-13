"""
Modal screen for ``dbt show`` with structured table output.

Runs ``dbt show --select <node> --output json --log-format json``, parses the
JSON-lines output to extract the ``preview`` payload (a JSON-encoded list of
row-objects), and renders the result in a Textual ``DataTable``.

While the command is running the user sees a spinner/status badge.  Once
complete, the preview rows are shown in a scrollable table.  If the command
fails, the raw stderr/stdout is displayed so the user can diagnose the issue.

Usage (from within a Textual App)::

    from dbtui.show_screen import ShowScreen

    screen = ShowScreen(
        cli=self.cli,
        node_name="my_model",
        project_path=self.project.project_path,
    )
    self.app.push_screen(screen)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import DataTable, Label, RichLog, Rule, Static

from dbtui.dbt_client import DBTCLI, DBTCommand
from dbtui.widgets import ScreenHeader, StatusBadge


class ShowScreen(ModalScreen[Optional[int]]):
    """Modal screen that runs ``dbt show`` and renders the preview as a table.

    The screen covers the terminal with a translucent backdrop.  A central
    panel contains a header, status badge, and either a ``DataTable`` with the
    query results or an error log if the command failed.

    On dismiss the screen returns the process exit code (``int``) or ``None``
    if closed before the command completed.
    """

    DEFAULT_CSS = """
    ShowScreen {
        align: center middle;
    }

    #show-dialog {
        width: 90%;
        height: 85%;
        border: thick $accent;
        background: $surface;
        padding: 0;
    }

    #show-results-scroll {
        width: 100%;
        height: 1fr;
        padding: 0;
    }

    #show-table {
        width: 100%;
        height: 1fr;
        margin: 0 1;
    }

    #show-error-scroll {
        width: 100%;
        height: 1fr;
        background: $panel;
        padding: 0;
    }

    #show-row-count {
        width: 100%;
        height: 1;
        padding: 0 2;
        color: $text-muted;
        text-style: italic;
    }

    #show-footer-hint {
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
        node_name: str,
        project_path: Union[str, Path],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.cli = cli
        self.command = DBTCommand.SHOW
        self.node_name = node_name
        self.project_path = project_path
        self._exit_code: Optional[int] = None
        self._finished = False

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        with Vertical(id="show-dialog"):
            cmd_args = self.command.to_args(self.node_name)
            cmd_display = f"dbt {' '.join(cmd_args)}"
            yield ScreenHeader(
                f"👁  Show  ─  [bold]{self.node_name}[/bold]",
                id="show-header",
            )
            yield Label(
                f"  $ {cmd_display}",
                id="show-command-line",
            )
            yield StatusBadge(" ⏳ RUNNING ", id="show-status", classes="running")
            yield Rule(line_style="heavy")
            yield Label("", id="show-row-count")
            yield VerticalScroll(
                DataTable(id="show-table", show_cursor=True),
                id="show-results-scroll",
            )
            yield RichLog(
                id="show-error-scroll", wrap=True, highlight=False, markup=False
            )
            yield Static(
                " Press [bold]Esc[/bold] or [bold]q[/bold] to close ",
                id="show-footer-hint",
            )

    def on_mount(self) -> None:
        # Hide results and error sections until we know the outcome
        self.query_one("#show-results-scroll").display = False
        self.query_one("#show-error-scroll").display = False
        self.query_one("#show-row-count").display = False
        self._run_show()

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def action_close_screen(self) -> None:
        """Cancel any running command and dismiss the screen."""
        self.workers.cancel_group(self, "dbt_show")
        self.dismiss(self._exit_code)

    # ------------------------------------------------------------------
    # JSON parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_preview_from_json_lines(raw_output: str) -> List[Dict[str, Any]]:
        """Extract preview rows from ``dbt show --output json --log-format json``.

        dbt emits one JSON object per line when ``--log-format json`` is used.
        We look for the line that contains a ``"preview"`` key inside
        ``"data"`` — its value is a JSON-encoded string holding a list of
        row-objects.

        Returns:
            A list of dicts, one per row, with column names as keys.
            Empty list if parsing fails or no preview is found.
        """
        rows: List[Dict[str, Any]] = []
        for line in raw_output.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            # The preview lives at obj["data"]["preview"]
            data = obj.get("data") if isinstance(obj, dict) else None
            if not isinstance(data, dict):
                continue

            preview_raw = data.get("preview")
            if preview_raw is None:
                continue

            # preview_raw is a JSON-encoded string: '[{"col": "val"}, ...]'
            if isinstance(preview_raw, str):
                try:
                    parsed = json.loads(preview_raw)
                except json.JSONDecodeError:
                    continue
            elif isinstance(preview_raw, list):
                parsed = preview_raw
            else:
                continue

            if isinstance(parsed, list):
                rows = parsed
                break  # We found the preview, stop scanning

        return rows

    # ------------------------------------------------------------------
    # Command execution (Textual worker)
    # ------------------------------------------------------------------

    @work(thread=False, exclusive=True, group="dbt_show")
    async def _run_show(self) -> None:
        """Execute ``dbt show`` asynchronously and render the results."""
        collected_stdout: list[str] = []
        collected_stderr: list[str] = []

        try:
            args = self.command.to_args(self.node_name)
            async for event in self.cli.run_async(args, cwd=self.project_path):
                if event.exit_code is not None:
                    self._exit_code = event.exit_code
                if event.stream == "stdout":
                    collected_stdout.append(event.text)
                elif event.stream == "stderr":
                    collected_stderr.append(event.text)

        except FileNotFoundError as exc:
            collected_stderr.append(f"ERROR: {exc}")
            self._exit_code = 127

        except Exception as exc:
            collected_stderr.append(f"ERROR: {exc}")
            self._exit_code = 1

        self._finished = True

        # Try to parse preview rows from stdout
        full_stdout = "\n".join(collected_stdout)
        rows = self._parse_preview_from_json_lines(full_stdout)

        badge = self.query_one("#show-status", StatusBadge)

        if self._exit_code == 0 and rows:
            self._render_table(rows)
            badge.update(" ✔ SUCCESS ")
            badge.remove_class("running")
            badge.add_class("success")
        elif self._exit_code == 0 and not rows:
            # Command succeeded but no preview data found — show raw output
            badge.update(" ✔ DONE (no preview rows) ")
            badge.remove_class("running")
            badge.add_class("success")
            self._show_raw_output(collected_stdout, collected_stderr)
        else:
            badge.update(f" ✘ FAILED (exit {self._exit_code}) ")
            badge.remove_class("running")
            badge.add_class("error")
            self._show_raw_output(collected_stdout, collected_stderr)

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------

    def _render_table(self, rows: List[Dict[str, Any]]) -> None:
        """Populate the DataTable with the preview rows."""
        if not rows:
            return

        table = self.query_one("#show-table", DataTable)

        # Use keys from the first row as column headers
        columns = list(rows[0].keys())
        for col in columns:
            table.add_column(str(col), key=str(col))

        for row in rows:
            values = [str(row.get(col, "")) for col in columns]
            table.add_row(*values)

        self.query_one("#show-results-scroll").display = True
        row_count_label = self.query_one("#show-row-count", Label)
        row_count_label.update(
            f"  {len(rows)} row{'s' if len(rows) != 1 else ''}, "
            f"{len(columns)} column{'s' if len(columns) != 1 else ''}"
        )
        row_count_label.display = True

    def _show_raw_output(
        self, stdout_lines: list[str], stderr_lines: list[str]
    ) -> None:
        """Fall back to showing the raw command output when parsing fails."""
        error_scroll = self.query_one("#show-error-scroll", RichLog)
        error_scroll.display = True

        all_lines = stdout_lines + stderr_lines
        if not all_lines:
            all_lines = ["(no output)"]

        for line_text in all_lines:
            error_scroll.write(Text(line_text, style="red"))
