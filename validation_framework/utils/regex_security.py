"""
Regex security utilities for preventing ReDoS (Regular Expression Denial of Service) attacks.

This module provides validation and safe compilation of user-provided regex patterns
to prevent catastrophic backtracking and excessive execution time.

Security measures:
1. Pattern complexity validation (detects nested quantifiers)
2. Pattern length limits
3. Timeout-protected regex matching (using signal on Unix/Linux)

Author: Daniel Edge
"""

import re
import signal
import platform
import logging
from typing import Optional, Pattern
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Configuration
MAX_PATTERN_LENGTH = 500  # Maximum regex pattern length
REGEX_TIMEOUT_SECONDS = 5  # Timeout for regex matching operations
IS_WINDOWS = platform.system() == 'Windows'


class RegexSecurityError(Exception):
    """Raised when a regex pattern fails security validation."""
    pass


class RegexTimeoutError(Exception):
    """Raised when a regex operation exceeds the timeout limit."""
    pass


def validate_regex_pattern(pattern: str, max_length: int = MAX_PATTERN_LENGTH) -> None:
    """
    Validate a regex pattern for security issues.

    Checks for:
    1. Pattern length (prevents extremely long patterns)
    2. Nested quantifiers (major ReDoS vector)
    3. Excessive quantifier complexity

    Args:
        pattern: Regular expression pattern to validate
        max_length: Maximum allowed pattern length

    Raises:
        RegexSecurityError: If pattern fails security validation

    Examples:
        >>> validate_regex_pattern(r'^[a-z]+$')  # Safe pattern
        >>> validate_regex_pattern(r'(a+)+')  # Raises RegexSecurityError (nested quantifiers)
    """
    # Check 1: Pattern length limit
    if len(pattern) > max_length:
        raise RegexSecurityError(
            f"Regex pattern exceeds maximum length of {max_length} characters. "
            f"Current length: {len(pattern)}. This prevents potentially malicious or "
            f"overly complex patterns."
        )

    # Check 2: Detect nested quantifiers (catastrophic backtracking risk)
    # Patterns like (a+)+, (a*)+, (a+)*, etc. can cause exponential time complexity
    nested_quantifier_pattern = r'\([^)]*[*+{][^)]*\)[*+{]'
    if re.search(nested_quantifier_pattern, pattern):
        raise RegexSecurityError(
            f"Regex pattern contains nested quantifiers which can cause catastrophic "
            f"backtracking (ReDoS vulnerability). Pattern: {pattern[:100]}... "
            f"Example: (a+)+ or (a*)* are dangerous. Use (a+) or a+ instead."
        )

    # Check 3: Excessive quantifier ranges
    # {n,m} where m is very large can be problematic
    range_quantifier_pattern = r'\{(\d+),(\d+)\}'
    for match in re.finditer(range_quantifier_pattern, pattern):
        min_val = int(match.group(1))
        max_val = int(match.group(2))
        if max_val > 10000:
            raise RegexSecurityError(
                f"Regex pattern contains excessive quantifier range {{{min_val},{max_val}}}. "
                f"Maximum allowed is {{n,10000}}. This prevents potentially slow matching."
            )
        if max_val < min_val:
            raise RegexSecurityError(
                f"Invalid quantifier range {{{min_val},{max_val}}} - max must be >= min"
            )

    # Check 4: Test pattern compilation
    try:
        re.compile(pattern)
    except re.error as e:
        raise RegexSecurityError(f"Invalid regex syntax: {e}")


@contextmanager
def regex_timeout(seconds: int = REGEX_TIMEOUT_SECONDS):
    """
    Context manager that enforces a timeout on regex operations.

    On Unix/Linux: Uses SIGALRM for precise timeout
    On Windows: No timeout enforcement (logs warning)

    Args:
        seconds: Timeout in seconds

    Raises:
        RegexTimeoutError: If regex operation exceeds timeout

    Example:
        >>> with regex_timeout(5):
        ...     result = regex.search(untrusted_input)
    """
    if IS_WINDOWS:
        # Windows doesn't support SIGALRM - skip timeout enforcement
        # Log once as debug message
        logger.debug("Regex timeout not available on Windows - continuing without timeout protection")
        yield
        return

    def timeout_handler(signum, frame):
        raise RegexTimeoutError(f"Regex operation timed out after {seconds} seconds")

    # Set up the timeout
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)

    try:
        yield
    finally:
        # Restore original handler and cancel alarm
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def safe_regex_compile(pattern: str, flags: int = 0, validate: bool = True) -> Pattern:
    """
    Safely compile a regex pattern with security validation.

    Args:
        pattern: Regular expression pattern
        flags: Regex flags (re.IGNORECASE, etc.)
        validate: Whether to perform security validation (default: True)

    Returns:
        Compiled regex pattern object

    Raises:
        RegexSecurityError: If pattern fails security validation

    Example:
        >>> regex = safe_regex_compile(r'^[a-z]+@[a-z]+\\.[a-z]+$')
        >>> regex.match('user@example.com')
    """
    if validate:
        validate_regex_pattern(pattern)

    try:
        return re.compile(pattern, flags)
    except re.error as e:
        raise RegexSecurityError(f"Failed to compile regex pattern: {e}")


def safe_regex_search(regex: Pattern, text: str, timeout_seconds: int = REGEX_TIMEOUT_SECONDS) -> Optional[re.Match]:
    """
    Perform regex search with timeout protection.

    Args:
        regex: Compiled regex pattern
        text: Text to search
        timeout_seconds: Timeout in seconds (Unix/Linux only)

    Returns:
        Match object if found, None otherwise

    Raises:
        RegexTimeoutError: If search exceeds timeout (Unix/Linux only)

    Example:
        >>> regex = safe_regex_compile(r'\\d+')
        >>> match = safe_regex_search(regex, 'abc123def')
    """
    if IS_WINDOWS:
        # No timeout on Windows - direct search
        return regex.search(text)

    with regex_timeout(timeout_seconds):
        return regex.search(text)


def safe_regex_match(regex: Pattern, text: str, timeout_seconds: int = REGEX_TIMEOUT_SECONDS) -> Optional[re.Match]:
    """
    Perform regex match with timeout protection.

    Args:
        regex: Compiled regex pattern
        text: Text to match
        timeout_seconds: Timeout in seconds (Unix/Linux only)

    Returns:
        Match object if matched, None otherwise

    Raises:
        RegexTimeoutError: If match exceeds timeout (Unix/Linux only)

    Example:
        >>> regex = safe_regex_compile(r'^\\d+$')
        >>> match = safe_regex_match(regex, '12345')
    """
    if IS_WINDOWS:
        # No timeout on Windows - direct match
        return regex.match(text)

    with regex_timeout(timeout_seconds):
        return regex.match(text)
