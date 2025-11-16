"""
Pretty output formatting for CLI.

Provides consistent, beautiful terminal output with DataK9 branding.

Author: Daniel Edge
"""

from colorama import Fore, Style, Back
import os


class PrettyOutput:
    """
    Pretty output formatter for DataK9 CLI.

    Provides consistent, branded terminal output with colors, boxes,
    and visual hierarchy.
    """

    # Color scheme
    PRIMARY = Fore.CYAN
    SUCCESS = Fore.GREEN
    WARNING = Fore.YELLOW
    ERROR = Fore.RED
    INFO = Fore.BLUE
    HEADER = Fore.WHITE + Style.BRIGHT
    DIM = Style.DIM
    RESET = Style.RESET_ALL

    # Symbols
    CHECK = "✓"
    CROSS = "✗"
    ARROW = "→"
    DOT = "•"
    WARN = "⚠"
    INFO_SYMBOL = "ℹ"

    @staticmethod
    def get_terminal_width():
        """Get terminal width, default to 80 if cannot determine."""
        try:
            return os.get_terminal_size().columns
        except:
            return 80

    @staticmethod
    def header(text, width=None):
        """
        Print a major header with box drawing.

        Args:
            text: Header text
            width: Box width (default: terminal width)
        """
        if width is None:
            width = min(PrettyOutput.get_terminal_width(), 80)

        padding = (width - len(text) - 2) // 2
        line = "═" * width

        print(f"\n{PrettyOutput.PRIMARY}╔{line}╗")
        print(f"║{' ' * padding}{text}{' ' * (width - len(text) - padding)}║")
        print(f"╚{line}╝{PrettyOutput.RESET}\n")

    @staticmethod
    def section(text, width=None):
        """
        Print a section header.

        Args:
            text: Section text
            width: Line width (default: terminal width)
        """
        if width is None:
            width = min(PrettyOutput.get_terminal_width(), 80)

        line = "─" * width
        print(f"\n{PrettyOutput.HEADER}{line}")
        print(f"{PrettyOutput.ARROW} {text}")
        print(f"{line}{PrettyOutput.RESET}\n")

    @staticmethod
    def subsection(text):
        """Print a subsection header."""
        print(f"\n{PrettyOutput.HEADER}{text}:{PrettyOutput.RESET}")

    @staticmethod
    def success(message, indent=0):
        """Print a success message with checkmark."""
        spaces = " " * indent
        print(f"{spaces}{PrettyOutput.SUCCESS}{PrettyOutput.CHECK}{PrettyOutput.RESET} {message}")

    @staticmethod
    def error(message, indent=0):
        """Print an error message with cross."""
        spaces = " " * indent
        print(f"{spaces}{PrettyOutput.ERROR}{PrettyOutput.CROSS}{PrettyOutput.RESET} {message}")

    @staticmethod
    def warning(message, indent=0):
        """Print a warning message."""
        spaces = " " * indent
        print(f"{spaces}{PrettyOutput.WARNING}{PrettyOutput.WARN}{PrettyOutput.RESET} {message}")

    @staticmethod
    def info(message, indent=0):
        """Print an info message."""
        spaces = " " * indent
        print(f"{spaces}{PrettyOutput.INFO}{PrettyOutput.INFO_SYMBOL}{PrettyOutput.RESET} {message}")

    @staticmethod
    def item(message, indent=0):
        """Print a list item."""
        spaces = " " * indent
        print(f"{spaces}{PrettyOutput.DIM}{PrettyOutput.DOT}{PrettyOutput.RESET} {message}")

    @staticmethod
    def key_value(key, value, indent=0, value_color=None):
        """
        Print a key-value pair.

        Args:
            key: Key text
            value: Value text
            indent: Indentation level
            value_color: Optional color for value
        """
        spaces = " " * indent
        if value_color:
            print(f"{spaces}{PrettyOutput.DIM}{key}:{PrettyOutput.RESET} {value_color}{value}{PrettyOutput.RESET}")
        else:
            print(f"{spaces}{PrettyOutput.DIM}{key}:{PrettyOutput.RESET} {value}")

    @staticmethod
    def progress(current, total, message=""):
        """
        Print a progress indicator.

        Args:
            current: Current item number
            total: Total items
            message: Optional message
        """
        percentage = (current / total) * 100 if total > 0 else 0
        bar_length = 30
        filled = int(bar_length * current / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_length - filled)

        print(f"  {PrettyOutput.PRIMARY}[{current}/{total}]{PrettyOutput.RESET} ", end="")
        print(f"{PrettyOutput.HEADER}{bar}{PrettyOutput.RESET} ", end="")
        print(f"{percentage:.0f}% {message}")

    @staticmethod
    def status_badge(status_text, passed=True):
        """
        Print a status badge.

        Args:
            status_text: Status text
            passed: True for success style, False for error style
        """
        if passed:
            color = PrettyOutput.SUCCESS
            bg = Back.GREEN
        else:
            color = PrettyOutput.ERROR
            bg = Back.RED

        return f"{bg}{Fore.BLACK} {status_text} {PrettyOutput.RESET}"

    @staticmethod
    def summary_box(title, items, width=None):
        """
        Print a summary box with items.

        Args:
            title: Box title
            items: List of (key, value, color) tuples
            width: Box width (default: 60)
        """
        if width is None:
            width = 60

        # Top border
        print(f"\n{PrettyOutput.PRIMARY}┌{'─' * (width - 2)}┐{PrettyOutput.RESET}")

        # Title
        title_padding = (width - len(title) - 4) // 2
        print(f"{PrettyOutput.PRIMARY}│{PrettyOutput.RESET} {' ' * title_padding}{PrettyOutput.HEADER}{title}{PrettyOutput.RESET}{' ' * (width - len(title) - title_padding - 4)} {PrettyOutput.PRIMARY}│{PrettyOutput.RESET}")

        # Separator
        print(f"{PrettyOutput.PRIMARY}├{'─' * (width - 2)}┤{PrettyOutput.RESET}")

        # Items
        for key, value, color in items:
            value_str = str(value)
            key_len = len(key)
            value_len = len(value_str)
            padding = width - key_len - value_len - 6

            print(f"{PrettyOutput.PRIMARY}│{PrettyOutput.RESET}  {PrettyOutput.DIM}{key}:{PrettyOutput.RESET}{' ' * padding}{color}{value_str}{PrettyOutput.RESET}  {PrettyOutput.PRIMARY}│{PrettyOutput.RESET}")

        # Bottom border
        print(f"{PrettyOutput.PRIMARY}└{'─' * (width - 2)}┘{PrettyOutput.RESET}\n")

    @staticmethod
    def logo():
        """Print DataK9 ASCII logo from datak9.txt file."""
        # Construct path to logo file: resources/images/datak9.txt
        # From validation_framework/core/pretty_output.py, go up 2 levels to project root
        logo_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            'resources', 'images', 'datak9.txt'
        )

        try:
            with open(logo_path, 'r', encoding='utf-8') as f:
                logo_text = f.read()
            print(f"{PrettyOutput.PRIMARY}{logo_text}{PrettyOutput.RESET}")
        except FileNotFoundError:
            # Fallback to simple text logo if file not found
            print(f"{PrettyOutput.PRIMARY}")
            print("   ____        _        _  ___  ___")
            print("  |  _ \\  __ _| |_ __ _| |/ / |/ _ \\")
            print("  | | | |/ _` | __/ _` | ' /| | (_) |")
            print("  | |_| | (_| | || (_| | . \\| |\\__, |")
            print("  |____/ \\__,_|\\__\\__,_|_|\\_\\_|  /_/")
            print(f"{PrettyOutput.RESET}")

        print(f"{PrettyOutput.DIM}{'Data Validation Framework'.center(45)}{PrettyOutput.RESET}\n")

    @staticmethod
    def divider(char="─", width=None):
        """Print a horizontal divider."""
        if width is None:
            width = min(PrettyOutput.get_terminal_width(), 80)
        print(f"{PrettyOutput.DIM}{char * width}{PrettyOutput.RESET}")

    @staticmethod
    def blank_line():
        """Print a blank line."""
        print()
