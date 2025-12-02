"""
Pretty output formatting for CLI.

Provides consistent, beautiful terminal output with DataK9 branding.
Unified formatting for profiler, validator, and all CLI commands.
"""

from colorama import Fore, Style, Back
import os
import sys


class PrettyOutput:
    """
    Pretty output formatter for DataK9 CLI.

    Provides consistent, branded terminal output with colors, boxes,
    and visual hierarchy. Used across profiler, validator, and all
    CLI commands for a unified look and feel.
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
    MUTED = Fore.WHITE + Style.DIM

    # Symbols
    CHECK = "✓"
    CROSS = "✗"
    ARROW = "→"
    DOT = "•"
    WARN = "⚠"
    INFO_SYMBOL = "ℹ"
    MAGNIFY = "🔍"
    BRAIN = "🧠"
    CHART = "📊"
    FILE = "📄"
    CLOCK = "⏱"
    SPARKLE = "✨"

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

    @staticmethod
    def task_start(message, icon=None):
        """
        Print a task starting message.

        Args:
            message: Task description
            icon: Optional emoji icon (default: magnifying glass)
        """
        icon = icon or PrettyOutput.MAGNIFY
        print(f"\n{icon} {PrettyOutput.HEADER}{message}{PrettyOutput.RESET}")

    @staticmethod
    def task_complete(message, duration=None):
        """
        Print a task completion message.

        Args:
            message: Completion message
            duration: Optional duration in seconds
        """
        if duration is not None:
            print(f"{PrettyOutput.SUCCESS}{PrettyOutput.CHECK}{PrettyOutput.RESET} {message} {PrettyOutput.DIM}({duration:.1f}s){PrettyOutput.RESET}")
        else:
            print(f"{PrettyOutput.SUCCESS}{PrettyOutput.CHECK}{PrettyOutput.RESET} {message}")

    @staticmethod
    def metric(label, value, color=None, indent=2):
        """
        Print a metric with label and value.

        Args:
            label: Metric label
            value: Metric value
            color: Optional color for value
            indent: Indentation spaces
        """
        spaces = " " * indent
        color = color or PrettyOutput.PRIMARY
        print(f"{spaces}{PrettyOutput.DIM}{label}:{PrettyOutput.RESET} {color}{value}{PrettyOutput.RESET}")

    @staticmethod
    def output_file(label, path, indent=2):
        """
        Print an output file path.

        Args:
            label: File type label (e.g., "HTML", "JSON")
            path: File path
            indent: Indentation spaces
        """
        spaces = " " * indent
        print(f"{spaces}{PrettyOutput.ARROW} {PrettyOutput.DIM}{label}:{PrettyOutput.RESET} {path}")

    @staticmethod
    def finding(message, severity="info", indent=4):
        """
        Print a finding or insight.

        Args:
            message: Finding text
            severity: One of "high", "medium", "low", "info"
            indent: Indentation spaces
        """
        spaces = " " * indent
        icons = {
            "high": f"{Fore.RED}●{PrettyOutput.RESET}",
            "medium": f"{Fore.YELLOW}●{PrettyOutput.RESET}",
            "low": f"{Fore.GREEN}●{PrettyOutput.RESET}",
            "info": f"{PrettyOutput.DIM}•{PrettyOutput.RESET}"
        }
        icon = icons.get(severity, icons["info"])
        print(f"{spaces}{icon} {message}")

    @staticmethod
    def quality_indicator(score, width=20):
        """
        Return a visual quality indicator bar.

        Args:
            score: Quality score 0-100
            width: Bar width in characters

        Returns:
            Formatted quality bar string
        """
        filled = int(width * score / 100)
        empty = width - filled

        if score >= 90:
            color = Fore.GREEN
        elif score >= 70:
            color = Fore.YELLOW
        else:
            color = Fore.RED

        bar = f"{color}{'█' * filled}{PrettyOutput.DIM}{'░' * empty}{PrettyOutput.RESET}"
        return f"{bar} {score:.0f}%"

    @staticmethod
    def profile_summary(rows, cols, quality, duration, size_str=None):
        """
        Print a compact profile summary line.

        Args:
            rows: Number of rows
            cols: Number of columns
            quality: Quality score 0-100
            duration: Processing time in seconds
            size_str: Optional file size string
        """
        quality_bar = PrettyOutput.quality_indicator(quality, width=15)

        parts = [
            f"{PrettyOutput.PRIMARY}{rows:,}{PrettyOutput.RESET} rows",
            f"{PrettyOutput.PRIMARY}{cols}{PrettyOutput.RESET} cols",
            f"Quality: {quality_bar}",
            f"{PrettyOutput.DIM}{duration:.1f}s{PrettyOutput.RESET}"
        ]

        if size_str:
            parts.insert(2, f"{PrettyOutput.DIM}{size_str}{PrettyOutput.RESET}")

        print(f"\n{PrettyOutput.CHECK} {' │ '.join(parts)}")

    @staticmethod
    def ml_summary(total_issues, severity, key_findings=None, analyzed_rows=None):
        """
        Print ML analysis summary.

        Args:
            total_issues: Number of issues found
            severity: Severity level (high, medium, low, none)
            key_findings: List of key finding strings
            analyzed_rows: Number of rows analyzed
        """
        severity_styles = {
            "high": (Fore.RED, "●"),
            "medium": (Fore.YELLOW, "●"),
            "low": (Fore.GREEN, "●"),
            "none": (Fore.GREEN, PrettyOutput.CHECK)
        }

        color, icon = severity_styles.get(severity, (Fore.WHITE, "•"))

        print(f"\n{PrettyOutput.BRAIN} {PrettyOutput.HEADER}ML Analysis{PrettyOutput.RESET}")
        print(f"  {color}{icon}{PrettyOutput.RESET} {total_issues:,} potential issues ({severity} severity)")

        if key_findings:
            for finding in key_findings[:3]:
                print(f"    {PrettyOutput.DIM}•{PrettyOutput.RESET} {finding}")

        if analyzed_rows:
            print(f"  {PrettyOutput.DIM}{PrettyOutput.ARROW} Analyzed {analyzed_rows:,} rows{PrettyOutput.RESET}")

    @staticmethod
    def validation_result(passed, errors=0, warnings=0, duration=None):
        """
        Print validation result summary.

        Args:
            passed: Whether validation passed
            errors: Number of errors
            warnings: Number of warnings
            duration: Optional duration in seconds
        """
        if passed and errors == 0 and warnings == 0:
            status = f"{PrettyOutput.SUCCESS}{PrettyOutput.CHECK} PASSED{PrettyOutput.RESET}"
        elif errors > 0:
            status = f"{PrettyOutput.ERROR}{PrettyOutput.CROSS} FAILED{PrettyOutput.RESET}"
        else:
            status = f"{PrettyOutput.WARNING}{PrettyOutput.WARN} WARNINGS{PrettyOutput.RESET}"

        parts = [status]
        if errors > 0:
            parts.append(f"{PrettyOutput.ERROR}{errors} errors{PrettyOutput.RESET}")
        if warnings > 0:
            parts.append(f"{PrettyOutput.WARNING}{warnings} warnings{PrettyOutput.RESET}")
        if duration:
            parts.append(f"{PrettyOutput.DIM}{duration:.1f}s{PrettyOutput.RESET}")

        print(f"\n{'  │  '.join(parts)}")

    @staticmethod
    def compact_table(headers, rows, col_widths=None):
        """
        Print a compact table.

        Args:
            headers: List of header strings
            rows: List of row tuples
            col_widths: Optional list of column widths
        """
        if not col_widths:
            col_widths = [max(len(str(h)), max(len(str(r[i])) for r in rows) if rows else 0)
                         for i, h in enumerate(headers)]

        # Header
        header_str = "  ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
        print(f"  {PrettyOutput.HEADER}{header_str}{PrettyOutput.RESET}")
        print(f"  {PrettyOutput.DIM}{'─' * len(header_str)}{PrettyOutput.RESET}")

        # Rows
        for row in rows:
            row_str = "  ".join(f"{str(v):<{col_widths[i]}}" for i, v in enumerate(row))
            print(f"  {row_str}")
