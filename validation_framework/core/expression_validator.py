"""
Expression validator for pandas DataFrame.eval() security hardening.

This module provides security validation for user-provided expressions
used in pandas DataFrame.eval() calls, protecting against:
- Denial of service via complex/recursive expressions
- Information disclosure through error messages
- Potential code injection attacks

Author: Daniel Edge
"""

import re
from typing import Optional
from validation_framework.core.exceptions import DataK9Exception, ErrorSeverity


class ExpressionValidationError(DataK9Exception):
    """
    Expression validation failed - expression is not safe to evaluate.

    Raised when a user-provided expression contains disallowed patterns,
    exceeds complexity limits, or violates security constraints.

    This is a security-focused exception that prevents potentially
    dangerous expressions from being evaluated.

    Attributes:
        reason (str): Why the expression was rejected

    Example:
        >>> raise ExpressionValidationError(
        ...     "Expression contains disallowed pattern",
        ...     reason="contains_dunder_methods"
        ... )
    """

    def __init__(
        self,
        message: str,
        reason: Optional[str] = None,
        validation_name: Optional[str] = None
    ):
        """
        Initialize expression validation error.

        Args:
            message: Error description (no expression content for security)
            reason: Security violation reason code
            validation_name: Name of validation that triggered this
        """
        super().__init__(
            message,
            severity=ErrorSeverity.RECOVERABLE,
            details={
                'reason': reason,
                'validation_name': validation_name
            }
        )
        self.reason = reason


# Security configuration constants
MAX_EXPRESSION_LENGTH = 1000  # Maximum characters in expression
MAX_OPERATOR_COUNT = 50       # Maximum number of operators
MAX_NESTING_DEPTH = 10        # Maximum parenthesis nesting depth

# Disallowed patterns (security risk)
DISALLOWED_PATTERNS = [
    # Python dunder methods (double underscore)
    r'__\w+__',
    # Dangerous builtin functions
    r'\bimport\b',
    r'\bexec\b',
    r'\beval\b',
    r'\bcompile\b',
    r'\bopen\b',
    r'\bfile\b',
    r'\binput\b',
    r'\bglobals\b',
    r'\blocals\b',
    r'\bvars\b',
    r'\bdir\b',
    # Attribute access that could be dangerous
    r'\bgetattr\b',
    r'\bsetattr\b',
    r'\bdelattr\b',
    r'\bhasattr\b',
    # Lambda and function definition
    r'\blambda\b',
    r'\bdef\b',
    # Class operations
    r'\bclass\b',
    # Module/package operations
    r'\b__import__\b',
    # System operations
    r'\bexit\b',
    r'\bquit\b',
]

# Compiled regex patterns for performance
DISALLOWED_REGEX = re.compile('|'.join(DISALLOWED_PATTERNS), re.IGNORECASE)

# Operator characters used for counting complexity
OPERATOR_CHARS = set('+-*/%<>=!&|~^')


def validate_expression(
    expr: str,
    validation_name: Optional[str] = None
) -> str:
    """
    Validate and sanitize a pandas DataFrame.eval() expression.

    This function performs comprehensive security checks on user-provided
    expressions before they are passed to pandas DataFrame.eval(). It checks:

    1. Maximum length constraint (prevent DoS)
    2. Operator complexity (prevent DoS via nested operations)
    3. Parenthesis nesting depth (prevent stack overflow)
    4. Disallowed patterns (prevent code injection)

    Args:
        expr: Expression string to validate
        validation_name: Optional validation name for context in errors

    Returns:
        Sanitized expression (stripped whitespace) if valid

    Raises:
        ExpressionValidationError: If expression fails any security check

    Example:
        >>> validate_expression("age >= 18 AND status == 'ACTIVE'")
        "age >= 18 AND status == 'ACTIVE'"

        >>> validate_expression("__import__('os').system('ls')")
        ExpressionValidationError: Expression contains disallowed pattern
    """
    if not expr:
        raise ExpressionValidationError(
            "Expression cannot be empty",
            reason="empty_expression",
            validation_name=validation_name
        )

    # Check 1: Maximum length (DoS protection)
    if len(expr) > MAX_EXPRESSION_LENGTH:
        raise ExpressionValidationError(
            f"Expression exceeds maximum length of {MAX_EXPRESSION_LENGTH} characters",
            reason="exceeds_max_length",
            validation_name=validation_name
        )

    # Check 2: Operator count (complexity limit)
    operator_count = sum(1 for char in expr if char in OPERATOR_CHARS)
    if operator_count > MAX_OPERATOR_COUNT:
        raise ExpressionValidationError(
            f"Expression exceeds maximum operator count of {MAX_OPERATOR_COUNT}",
            reason="exceeds_operator_limit",
            validation_name=validation_name
        )

    # Check 3: Parenthesis nesting depth (stack overflow protection)
    max_depth = _check_nesting_depth(expr)
    if max_depth > MAX_NESTING_DEPTH:
        raise ExpressionValidationError(
            f"Expression nesting depth ({max_depth}) exceeds maximum of {MAX_NESTING_DEPTH}",
            reason="exceeds_nesting_depth",
            validation_name=validation_name
        )

    # Check 4: Disallowed patterns (code injection protection)
    match = DISALLOWED_REGEX.search(expr)
    if match:
        # Don't include the actual pattern in error message (security)
        raise ExpressionValidationError(
            "Expression contains disallowed pattern or function",
            reason="disallowed_pattern",
            validation_name=validation_name
        )

    # Expression passed all checks - return sanitized version
    return expr.strip()


def _check_nesting_depth(expr: str) -> int:
    """
    Calculate maximum parenthesis nesting depth.

    Args:
        expr: Expression to analyze

    Returns:
        Maximum nesting depth (0 if no parentheses)

    Example:
        >>> _check_nesting_depth("((a + b) * (c + d))")
        2
        >>> _check_nesting_depth("a + b + c")
        0
    """
    max_depth = 0
    current_depth = 0

    for char in expr:
        if char == '(':
            current_depth += 1
            max_depth = max(max_depth, current_depth)
        elif char == ')':
            current_depth -= 1
            # Note: We don't validate matching parens here - pandas will catch that

    return max_depth


def get_safe_error_message(expr: str, error: Exception) -> str:
    """
    Create a safe error message that doesn't leak expression content.

    When expression evaluation fails, we need to provide helpful error
    messages without exposing the full expression content (which might
    contain sensitive data or security-relevant patterns).

    Args:
        expr: Expression that failed (not included in output)
        error: Original exception

    Returns:
        Safe error message with no expression content

    Example:
        >>> get_safe_error_message("secret_field == 'value'", ValueError("invalid"))
        "Expression evaluation failed: invalid. Please check column names and syntax."
    """
    # Extract error type and message only - no expression content
    error_type = type(error).__name__
    error_msg = str(error)

    # Sanitize error message - remove any quoted strings that might be from expression
    error_msg = re.sub(r"'[^']*'", "'...'", error_msg)
    error_msg = re.sub(r'"[^"]*"', '"..."', error_msg)

    return (
        f"Expression evaluation failed ({error_type}). "
        f"Please check that all referenced columns exist and the syntax is valid pandas query syntax. "
        f"Error: {error_msg}"
    )
