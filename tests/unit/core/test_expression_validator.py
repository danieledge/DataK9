"""
Unit tests for expression validator security module.

Tests validate that the expression validator properly protects against:
- Denial of service via complex expressions
- Code injection attempts
- Information disclosure through error messages

Author: Daniel Edge
"""

import pytest
from validation_framework.core.expression_validator import (
    validate_expression,
    ExpressionValidationError,
    get_safe_error_message,
    _check_nesting_depth,
    MAX_EXPRESSION_LENGTH,
    MAX_OPERATOR_COUNT,
    MAX_NESTING_DEPTH
)


class TestValidExpression:
    """Test cases for valid expressions that should pass validation."""

    def test_simple_comparison(self):
        """Simple comparison should pass."""
        expr = "age >= 18"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_logical_and(self):
        """Logical AND should pass."""
        expr = "age >= 18 & status == 'ACTIVE'"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_logical_or(self):
        """Logical OR should pass."""
        expr = "age < 18 | age > 65"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_logical_not(self):
        """Logical NOT should pass."""
        expr = "~is_deleted"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_complex_business_rule(self):
        """Complex but valid business rule should pass."""
        expr = "(account_type == 'SAVINGS' & interest_rate > 0) | (account_type != 'SAVINGS')"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_arithmetic_operations(self):
        """Arithmetic operations should pass."""
        expr = "amount * 1.1 > 1000"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_string_comparison(self):
        """String comparison with quotes should pass."""
        expr = "status == 'ACTIVE' | status == 'PENDING'"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_numeric_range(self):
        """Numeric range check should pass."""
        expr = "amount >= 0 & amount <= 1000000"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_field_names_with_underscores(self):
        """Field names with single underscores should pass."""
        expr = "customer_id > 0 & account_status == 'ACTIVE'"
        result = validate_expression(expr)
        assert result == expr.strip()

    def test_whitespace_stripped(self):
        """Whitespace should be stripped from result."""
        expr = "  age >= 18  "
        result = validate_expression(expr)
        assert result == "age >= 18"


class TestInvalidExpressionLength:
    """Test cases for expressions that exceed maximum length."""

    def test_exceeds_max_length(self):
        """Expression exceeding max length should fail."""
        # Create expression longer than MAX_EXPRESSION_LENGTH
        expr = "a" * (MAX_EXPRESSION_LENGTH + 1)
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        assert "exceeds maximum length" in exc_info.value.message
        assert exc_info.value.reason == "exceeds_max_length"

    def test_at_max_length(self):
        """Expression at exactly max length should pass."""
        # Create expression at MAX_EXPRESSION_LENGTH
        expr = "a" * MAX_EXPRESSION_LENGTH
        result = validate_expression(expr)
        assert len(result) == MAX_EXPRESSION_LENGTH

    def test_empty_expression(self):
        """Empty expression should fail."""
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression("")
        assert "cannot be empty" in exc_info.value.message
        assert exc_info.value.reason == "empty_expression"


class TestInvalidOperatorCount:
    """Test cases for expressions with too many operators."""

    def test_exceeds_operator_limit(self):
        """Expression with too many operators should fail."""
        # Create expression with more than MAX_OPERATOR_COUNT operators
        # Use simple operators to avoid hitting length limit
        expr = " + ".join(["a"] * (MAX_OPERATOR_COUNT + 2))  # Creates a+a+a+... with 51+ operators
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        assert "exceeds maximum operator count" in exc_info.value.message
        assert exc_info.value.reason == "exceeds_operator_limit"

    def test_at_operator_limit(self):
        """Expression at exactly operator limit should pass."""
        # Create expression with MAX_OPERATOR_COUNT operators (use + for simplicity)
        operators_used = 0
        parts = []
        for i in range(MAX_OPERATOR_COUNT):
            if operators_used < MAX_OPERATOR_COUNT:
                if i == 0:
                    parts.append("a")
                    operators_used += 1  # for the + that will follow
                else:
                    parts.append(" + 1")
                    operators_used += 1
        expr = "".join(parts[:MAX_OPERATOR_COUNT])
        # Should pass if we're at the limit
        result = validate_expression(expr)
        assert result is not None


class TestInvalidNestingDepth:
    """Test cases for expressions with excessive nesting."""

    def test_exceeds_nesting_depth(self):
        """Expression with too deep nesting should fail."""
        # Create expression with nesting deeper than MAX_NESTING_DEPTH
        expr = "(" * (MAX_NESTING_DEPTH + 1) + "a" + ")" * (MAX_NESTING_DEPTH + 1)
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        assert "nesting depth" in exc_info.value.message
        assert "exceeds maximum" in exc_info.value.message
        assert exc_info.value.reason == "exceeds_nesting_depth"

    def test_at_nesting_depth_limit(self):
        """Expression at exactly nesting limit should pass."""
        # Create expression with MAX_NESTING_DEPTH nesting
        expr = "(" * MAX_NESTING_DEPTH + "a" + ")" * MAX_NESTING_DEPTH
        result = validate_expression(expr)
        assert result is not None

    def test_check_nesting_depth_helper(self):
        """Test the nesting depth calculation helper."""
        assert _check_nesting_depth("a + b") == 0
        assert _check_nesting_depth("(a + b)") == 1
        assert _check_nesting_depth("((a + b))") == 2
        assert _check_nesting_depth("((a + b) * (c + d))") == 2
        assert _check_nesting_depth("(((a)))") == 3


class TestDisallowedPatterns:
    """Test cases for expressions containing disallowed patterns."""

    def test_dunder_methods(self):
        """Expressions with dunder methods should fail."""
        dangerous_exprs = [
            "__import__('os')",
            "df.__class__",
            "__builtins__",
            "__dict__",
            "__name__",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message
            assert exc_info.value.reason == "disallowed_pattern"

    def test_import_keyword(self):
        """Expressions with import should fail."""
        dangerous_exprs = [
            "import os",
            "from os import system",
            "__import__('subprocess')",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_exec_eval_compile(self):
        """Expressions with exec/eval/compile should fail."""
        dangerous_exprs = [
            "eval('1+1')",
            "exec('print(1)')",
            "compile('a=1', '<string>', 'exec')",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_file_operations(self):
        """Expressions with file operations should fail."""
        dangerous_exprs = [
            "open('/etc/passwd')",
            "file('/etc/passwd')",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_globals_locals_vars(self):
        """Expressions with globals/locals/vars should fail."""
        dangerous_exprs = [
            "globals()",
            "locals()",
            "vars()",
            "dir()",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_attribute_manipulation(self):
        """Expressions with attribute manipulation should fail."""
        dangerous_exprs = [
            "getattr(obj, 'attr')",
            "setattr(obj, 'attr', val)",
            "delattr(obj, 'attr')",
            "hasattr(obj, 'attr')",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_lambda_and_functions(self):
        """Expressions with lambda/function definitions should fail."""
        dangerous_exprs = [
            "lambda x: x + 1",
            "def func(): pass",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_class_keyword(self):
        """Expressions with class keyword should fail."""
        expr = "class MyClass: pass"
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        assert "disallowed pattern" in exc_info.value.message

    def test_system_operations(self):
        """Expressions with exit/quit should fail."""
        dangerous_exprs = [
            "exit()",
            "quit()",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError) as exc_info:
                validate_expression(expr)
            assert "disallowed pattern" in exc_info.value.message

    def test_input_function(self):
        """Expressions with input() should fail."""
        expr = "input('Enter password: ')"
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        assert "disallowed pattern" in exc_info.value.message


class TestValidationNameContext:
    """Test that validation_name is properly included in errors."""

    def test_validation_name_in_error(self):
        """Validation name should be included in error details."""
        expr = "__import__('os')"
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr, validation_name="TestValidation")
        assert exc_info.value.details['validation_name'] == "TestValidation"

    def test_validation_name_optional(self):
        """Validation name should be optional."""
        expr = "__import__('os')"
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        # Should still raise error, just without validation_name in details
        assert "disallowed pattern" in exc_info.value.message


class TestSafeErrorMessages:
    """Test that error messages don't leak expression content."""

    def test_safe_error_message_sanitizes_quotes(self):
        """Error messages should not include quoted strings from expression."""
        expr = "secret_field == 'password123'"
        error = ValueError("column 'secret_field' not found")
        safe_msg = get_safe_error_message(expr, error)

        # Should not contain the actual value 'password123'
        assert "password123" not in safe_msg
        # Should contain sanitized placeholder
        assert "..." in safe_msg
        # Should contain helpful guidance
        assert "Expression evaluation failed" in safe_msg
        assert "pandas query syntax" in safe_msg

    def test_safe_error_message_includes_error_type(self):
        """Safe error message should include error type."""
        expr = "invalid_syntax"
        error = KeyError("column_name")
        safe_msg = get_safe_error_message(expr, error)

        assert "KeyError" in safe_msg

    def test_safe_error_message_no_expression_content(self):
        """Safe error message should never include full expression."""
        expr = "highly_sensitive_field == 'secret_value'"
        error = ValueError("some error")
        safe_msg = get_safe_error_message(expr, error)

        # Expression content should not be in message
        assert "highly_sensitive_field" not in safe_msg
        assert "secret_value" not in safe_msg


class TestBackwardCompatibility:
    """Test that valid business rules from existing configs still work."""

    def test_typical_age_check(self):
        """Typical age validation should work."""
        expr = "age >= 18"
        result = validate_expression(expr)
        assert result is not None

    def test_typical_range_check(self):
        """Typical range validation should work."""
        expr = "amount >= 0 & amount <= 1000000"
        result = validate_expression(expr)
        assert result is not None

    def test_typical_status_check(self):
        """Typical status validation should work."""
        expr = "(account_type == 'SAVINGS' & interest_rate > 0) | (account_type != 'SAVINGS')"
        result = validate_expression(expr)
        assert result is not None

    def test_typical_date_check(self):
        """Typical date validation should work."""
        expr = "transaction_date <= today"
        result = validate_expression(expr)
        assert result is not None

    def test_multiple_conditions(self):
        """Multiple conditions with AND/OR should work."""
        expr = "age >= 18 & status == 'ACTIVE' & balance > 0"
        result = validate_expression(expr)
        assert result is not None


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_character_expression(self):
        """Single character expression should pass."""
        expr = "a"
        result = validate_expression(expr)
        assert result == "a"

    def test_expression_with_numbers(self):
        """Expression with numbers should pass."""
        expr = "123 + 456"
        result = validate_expression(expr)
        assert result is not None

    def test_expression_with_dots(self):
        """Expression with dots (decimals) should pass."""
        expr = "price > 99.99"
        result = validate_expression(expr)
        assert result is not None

    def test_case_insensitive_pattern_matching(self):
        """Disallowed patterns should be case-insensitive."""
        dangerous_exprs = [
            "IMPORT os",
            "Import os",
            "EVAL('1+1')",
            "Eval('1+1')",
        ]
        for expr in dangerous_exprs:
            with pytest.raises(ExpressionValidationError):
                validate_expression(expr)

    def test_expression_with_single_underscore(self):
        """Single underscores in field names should pass."""
        expr = "customer_id > 0"
        result = validate_expression(expr)
        assert result is not None

    def test_unbalanced_parens_not_caught(self):
        """Unbalanced parentheses are not our concern - pandas will catch it."""
        # We only check nesting depth, not balance
        expr = "((a + b)"  # Unbalanced but depth is only 2
        result = validate_expression(expr)  # Should pass our validation
        assert result is not None  # Pandas will catch the syntax error later


class TestExceptionDetails:
    """Test that exception details are properly structured."""

    def test_exception_has_severity(self):
        """ExpressionValidationError should have RECOVERABLE severity."""
        expr = "__import__('os')"
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr)
        from validation_framework.core.exceptions import ErrorSeverity
        assert exc_info.value.severity == ErrorSeverity.RECOVERABLE

    def test_exception_to_dict(self):
        """Exception should serialize to dict properly."""
        expr = "import os"
        with pytest.raises(ExpressionValidationError) as exc_info:
            validate_expression(expr, validation_name="TestRule")

        exc_dict = exc_info.value.to_dict()
        assert exc_dict['type'] == 'ExpressionValidationError'
        assert 'disallowed pattern' in exc_dict['message']
        assert exc_dict['details']['reason'] == 'disallowed_pattern'
        assert exc_dict['details']['validation_name'] == 'TestRule'
