"""
Path pattern expansion utilities for date/time and context-based filename generation.

Supports patterns like:
- {date} -> 2025-11-22
- {time} -> 14-30-45
- {timestamp} -> 20251122_143045
- {datetime} -> 2025-11-22_14-30-45
- {job_name} -> My_Job_Name (sanitized)
- {file_name} -> data_file (from source file)
- {table_name} -> customers (from database table)
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from validation_framework.core.logging_config import get_logger

logger = get_logger(__name__)


class PathPatternExpander:
    """
    Expand date/time and context patterns in output file paths.

    Provides automatic timestamping and context-aware filename generation
    to prevent file overwrites and improve audit trails.

    Examples:
        >>> expander = PathPatternExpander()
        >>> context = {'job_name': 'Customer Validation'}
        >>> expander.expand('report_{job_name}_{timestamp}.html', context)
        'report_Customer_Validation_20251122_143045.html'

        >>> expander.expand('reports/{date}/validation_{time}.html', context)
        'reports/2025-11-22/validation_14-30-45.html'
    """

    # Known patterns that should be expanded
    KNOWN_PATTERNS = {
        'date', 'time', 'timestamp', 'datetime',
        'job_name', 'file_name', 'table_name'
    }

    # Characters that are invalid in filenames across platforms
    INVALID_FILENAME_CHARS = r'[<>:"/\\|?*\x00-\x1f]'

    # Maximum filename length (conservative for cross-platform compatibility)
    MAX_FILENAME_LENGTH = 200

    def __init__(self, run_timestamp: Optional[datetime] = None):
        """
        Initialize pattern expander.

        Args:
            run_timestamp: Optional timestamp to use for all expansions.
                         If None, uses current time at first expansion.
        """
        self._run_timestamp = run_timestamp
        self._timestamp_generated = False

    def expand(self, path_template: str, context: Optional[Dict[str, str]] = None) -> str:
        """
        Expand patterns in path template.

        Args:
            path_template: Path with patterns like {date}, {time}, {job_name}, etc.
            context: Dictionary with context values (job_name, file_name, etc.)

        Returns:
            Expanded path with all patterns replaced

        Examples:
            >>> expander = PathPatternExpander()
            >>> expander.expand('report_{date}.html', {})
            'report_2025-11-22.html'

            >>> expander.expand('reports/{job_name}/data_{timestamp}.json',
            ...                 {'job_name': 'My Job'})
            'reports/My_Job/data_20251122_143045.json'
        """
        if not path_template:
            return path_template

        # Initialize context if not provided
        if context is None:
            context = {}

        # Generate timestamp on first use (ensures consistency across multiple calls)
        if not self._timestamp_generated:
            if self._run_timestamp is None:
                self._run_timestamp = datetime.now()
            self._timestamp_generated = True

        # Build replacement dictionary
        replacements = self._build_replacements(context)

        # Expand patterns
        expanded = path_template
        for pattern, value in replacements.items():
            expanded = expanded.replace(f'{{{pattern}}}', value)

        # Check for unknown patterns and warn
        self._warn_unknown_patterns(expanded, path_template)

        # Ensure parent directories exist
        self._ensure_directory(expanded)

        return expanded

    def _build_replacements(self, context: Dict[str, str]) -> Dict[str, str]:
        """
        Build dictionary of pattern replacements.

        Args:
            context: User-provided context values

        Returns:
            Dictionary mapping pattern names to replacement values
        """
        replacements = {}

        # Date/time patterns (always available)
        replacements['date'] = self._run_timestamp.strftime('%Y-%m-%d')
        replacements['time'] = self._run_timestamp.strftime('%H-%M-%S')  # Hyphens for filesystem safety
        replacements['timestamp'] = self._run_timestamp.strftime('%Y%m%d_%H%M%S')
        replacements['datetime'] = self._run_timestamp.strftime('%Y-%m-%d_%H-%M-%S')

        # Context-based patterns (if provided)
        if 'job_name' in context:
            replacements['job_name'] = self._sanitize_filename(context['job_name'])

        if 'file_name' in context:
            replacements['file_name'] = self._sanitize_filename(context['file_name'])

        if 'table_name' in context:
            replacements['table_name'] = self._sanitize_filename(context['table_name'])

        return replacements

    def _sanitize_filename(self, name: str) -> str:
        """
        Sanitize filename component for filesystem safety.

        Removes or replaces characters that are invalid in filenames across
        Windows, Linux, and macOS.

        Args:
            name: Filename or path component to sanitize

        Returns:
            Sanitized filename safe for all filesystems

        Examples:
            >>> expander = PathPatternExpander()
            >>> expander._sanitize_filename('My Data / Report (2024)')
            'My_Data_Report_2024'
        """
        if not name:
            return name

        # Replace invalid characters with underscores
        sanitized = re.sub(self.INVALID_FILENAME_CHARS, '_', name)

        # Replace spaces with underscores
        sanitized = sanitized.replace(' ', '_')

        # Remove repeated underscores
        sanitized = re.sub(r'_+', '_', sanitized)

        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')

        # Trim to maximum length
        if len(sanitized) > self.MAX_FILENAME_LENGTH:
            sanitized = sanitized[:self.MAX_FILENAME_LENGTH]
            logger.warning(f"Filename truncated to {self.MAX_FILENAME_LENGTH} characters: {sanitized}")

        return sanitized

    def _warn_unknown_patterns(self, expanded: str, original: str) -> None:
        """
        Warn about unknown patterns that were not expanded.

        Args:
            expanded: Path after pattern expansion
            original: Original path template
        """
        # Find remaining patterns in expanded path
        remaining_patterns = set(re.findall(r'\{(\w+)\}', expanded))

        # Filter out known patterns that might still be in the path
        unknown_patterns = remaining_patterns - self.KNOWN_PATTERNS

        if unknown_patterns:
            logger.warning(
                f"Unknown pattern(s) in path template: {', '.join(sorted(unknown_patterns))}. "
                f"Original: {original}, Expanded: {expanded}. "
                f"Known patterns: {', '.join(sorted(self.KNOWN_PATTERNS))}"
            )

    def _ensure_directory(self, path: str) -> None:
        """
        Ensure parent directory exists for the given path.

        Creates all necessary parent directories if they don't exist.

        Args:
            path: File path whose parent directory should be created
        """
        try:
            path_obj = Path(path)
            parent_dir = path_obj.parent

            # Only create if path has a parent directory component
            if parent_dir and str(parent_dir) != '.':
                parent_dir.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Created directory: {parent_dir}")

        except OSError as e:
            logger.error(f"Failed to create directory for {path}: {e}")
            # Don't raise - let the file write operation fail with a clear error

    @staticmethod
    def has_patterns(path_template: str) -> bool:
        """
        Check if path template contains any pattern placeholders.

        Args:
            path_template: Path string to check

        Returns:
            True if path contains pattern placeholders like {date}, {time}, etc.

        Examples:
            >>> PathPatternExpander.has_patterns('report_{date}.html')
            True
            >>> PathPatternExpander.has_patterns('report.html')
            False
        """
        return bool(re.search(r'\{\w+\}', path_template))


# Convenience function for one-off expansions
def expand_path_patterns(path_template: str, context: Optional[Dict[str, str]] = None,
                        run_timestamp: Optional[datetime] = None) -> str:
    """
    Convenience function to expand patterns in a path template.

    Args:
        path_template: Path with patterns like {date}, {time}, {job_name}, etc.
        context: Dictionary with context values (job_name, file_name, etc.)
        run_timestamp: Optional timestamp to use for expansion

    Returns:
        Expanded path with all patterns replaced

    Examples:
        >>> expand_path_patterns('report_{date}.html')
        'report_2025-11-22.html'

        >>> expand_path_patterns('reports/{job_name}_{timestamp}.json',
        ...                      {'job_name': 'Customer Data'})
        'reports/Customer_Data_20251122_143045.json'
    """
    expander = PathPatternExpander(run_timestamp=run_timestamp)
    return expander.expand(path_template, context)
