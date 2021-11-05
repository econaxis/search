import selectors
from selectors import DefaultSelector
from typing import Optional

from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel

from search_lib import _SearchRetType
import sys
import termios


def _patch_lflag(attrs: int) -> int:
    return attrs & ~(termios.ECHO | termios.ICANON | termios.IEXTEN | termios.ISIG)


def _patch_iflag(attrs: int) -> int:
    return attrs & ~(
        # Disable XON/XOFF flow control on output and input.
        # (Don't capture Ctrl-S and Ctrl-Q.)
        # Like executing: "stty -ixon."
            termios.IXON
            | termios.IXOFF

            # Don't translate carriage return into newline on input.
            # |termios.ICRNL   Enter instead of ^M
            | termios.INLCR
            | termios.IGNCR
    )


def setup_term():
    import tty, termios, sys

    newattr = termios.tcgetattr(sys.stdin.fileno())
    newattr[tty.LFLAG] = _patch_lflag(newattr[tty.LFLAG])
    newattr[tty.IFLAG] = _patch_iflag(newattr[tty.IFLAG])
    newattr[tty.CC][termios.VMIN] = 1
    termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, newattr)




class QueryConsole:
    def __init__(self):
        setup_term()
        layout = Layout()
        layout.split(
            Layout(Panel("top"), name="top", ratio=10),
            Layout(Panel("query"), name="query", ratio=2)
        )

        self.sel = DefaultSelector()
        self.sel.register(sys.stdin.fileno(), selectors.EVENT_READ, sys.stdin)

        self.layout = layout
        self.live = Live(layout)
        self.query = ""
        self.prevquery = ""
        self.live.__enter__()
        self.valid = True

    def run_event_loop(self) -> Optional[list[str]]:
        events = self.sel.select(0.1)
        for key, mask in events:
            ch = key.data.read(1)
            print(ord(ch))
            if ch == 'q':
                self.valid = False
                self.live.__exit__(None, None, None)

            if ord(ch) == 10:
                self.query = ""
            if ord(ch) == 8 or ord(ch) == 127:
                self.query = self.query[:-1]

            if 0 <= ord(ch) - ord('a') < 26 or 0 <= ord(ch) - ord('A') < 26 or ch == ' ':
                self.query += ch

        self.layout["query"].update(Panel(self.query))
        self.live.refresh()
        if self.query != "" and self.query != self.prevquery:
            terms = self.query.upper().split(" ")
            terms = list(filter(lambda k: len(k) > 0, terms))
            self.prevquery = self.query
            return terms

    def set_results(self, sr: _SearchRetType):
        self.layout["top"].update(Panel(sr.__rich__()))
