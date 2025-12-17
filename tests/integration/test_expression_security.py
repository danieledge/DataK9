"""
Integration tests for expression validator security hardening.

These tests verify that the expression validator properly integrates
with the validation framework and prevents security issues end-to-end.

Author: Daniel Edge
"""

import pytest
import pandas as pd
from validation_framework.validations.builtin.inline_checks import InlineBusinessRuleCheck
from validation_framework.validations.base import DataValidationRule
from validation_framework.core.results import Severity


class TestBusinessRuleSecurityHardening:
    """Integration tests for business rule expression security."""

    def test_valid_business_rule_still_works(self):
        """Valid business rules should continue to work after hardening."""
        rule = InlineBusinessRuleCheck(
            name="AgeCheck",
            severity=Severity.ERROR,
            params={
                "rule": "age >= 18",
                "description": "Must be 18 or older",
                "error_message": "Age below minimum"
            }
        )

        df = pd.DataFrame({
            'age': [25, 17, 30, 16, 21]
        })

        result = rule.validate(iter([df]), {})

        assert result.failed_count == 2  # Two under 18
        assert not result.passed

    def test_dangerous_import_blocked(self):
        """Business rules with import statements should be blocked."""
        rule = InlineBusinessRuleCheck(
            name="MaliciousRule",
            severity=Severity.ERROR,
            params={
                "rule": "__import__('os').system('ls')",
                "description": "Malicious rule",
                "error_message": "Security violation"
            }
        )

        df = pd.DataFrame({'age': [25]})

        result = rule.validate(iter([df]), {})

        # Should fail security validation
        assert not result.passed
        assert "security validation" in result.message.lower()

    def test_dangerous_eval_blocked(self):
        """Business rules with eval should be blocked."""
        rule = InlineBusinessRuleCheck(
            name="EvalRule",
            severity=Severity.ERROR,
            params={
                "rule": "eval('1+1')",
                "description": "Eval rule",
                "error_message": "Eval violation"
            }
        )

        df = pd.DataFrame({'age': [25]})

        result = rule.validate(iter([df]), {})

        assert not result.passed
        assert "security validation" in result.message.lower()

    def test_dangerous_globals_blocked(self):
        """Business rules with globals() should be blocked."""
        rule = InlineBusinessRuleCheck(
            name="GlobalsRule",
            severity=Severity.ERROR,
            params={
                "rule": "globals()['age'] > 18",
                "description": "Globals rule",
                "error_message": "Globals violation"
            }
        )

        df = pd.DataFrame({'age': [25]})

        result = rule.validate(iter([df]), {})

        assert not result.passed
        assert "security validation" in result.message.lower()

    def test_overly_complex_expression_blocked(self):
        """Extremely complex expressions should be blocked (DoS protection)."""
        # Create expression with too many operators
        rule_expr = " + ".join(["age"] * 60)  # 59 operators (exceeds limit of 50)

        rule = InlineBusinessRuleCheck(
            name="ComplexRule",
            severity=Severity.ERROR,
            params={
                "rule": rule_expr,
                "description": "Complex rule",
                "error_message": "Complexity violation"
            }
        )

        df = pd.DataFrame({'age': [25]})

        result = rule.validate(iter([df]), {})

        assert not result.passed
        assert "security validation" in result.message.lower()

    def test_deeply_nested_expression_blocked(self):
        """Deeply nested expressions should be blocked (stack overflow protection)."""
        # Create expression with excessive nesting
        rule_expr = "(" * 15 + "age > 18" + ")" * 15

        rule = InlineBusinessRuleCheck(
            name="NestedRule",
            severity=Severity.ERROR,
            params={
                "rule": rule_expr,
                "description": "Nested rule",
                "error_message": "Nesting violation"
            }
        )

        df = pd.DataFrame({'age': [25]})

        result = rule.validate(iter([df]), {})

        assert not result.passed
        assert "security validation" in result.message.lower()

    def test_complex_valid_rule_works(self):
        """Complex but valid business rules should work."""
        rule = InlineBusinessRuleCheck(
            name="ComplexValidRule",
            severity=Severity.ERROR,
            params={
                "rule": "(account_type == 'SAVINGS' & interest_rate > 0) | (account_type != 'SAVINGS')",
                "description": "Savings accounts must have interest",
                "error_message": "Invalid savings account"
            }
        )

        df = pd.DataFrame({
            'account_type': ['SAVINGS', 'CHECKING', 'SAVINGS', 'CHECKING'],
            'interest_rate': [1.5, 0.0, 0.0, 0.0]
        })

        result = rule.validate(iter([df]), {})

        # One SAVINGS account with 0 interest rate should fail
        assert result.failed_count == 1
        assert not result.passed


class TestConditionalSecurityHardening:
    """Integration tests for conditional expression security."""

    def test_valid_condition_still_works(self):
        """Valid conditions should continue to work after hardening."""
        from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck

        rule = MandatoryFieldCheck(
            name="ConditionalMandatory",
            severity=Severity.ERROR,
            params={"field": "email"},
            condition="account_type == 'PREMIUM'"
        )

        df = pd.DataFrame({
            'account_type': ['PREMIUM', 'BASIC', 'PREMIUM', 'BASIC'],
            'email': ['a@b.com', None, None, 'c@d.com']
        })

        result = rule.validate(iter([df]), {})

        # Only one PREMIUM account without email (row 2)
        assert result.failed_count == 1

    def test_dangerous_condition_blocked(self):
        """Conditions with dangerous patterns should be blocked."""
        from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck

        rule = MandatoryFieldCheck(
            name="MaliciousCondition",
            severity=Severity.ERROR,
            params={"fields": ["email"]},
            condition="__import__('os').system('ls')"
        )

        df = pd.DataFrame({
            'account_type': ['PREMIUM'],
            'email': ['a@b.com']
        })

        # Should raise ConditionEvaluationError (not caught in MandatoryFieldCheck)
        from validation_framework.core.exceptions import ConditionEvaluationError
        with pytest.raises(ConditionEvaluationError) as exc_info:
            result = rule.validate(iter([df]), {})

        assert "security validation" in str(exc_info.value).lower()

    def test_condition_with_globals_blocked(self):
        """Conditions with globals() should be blocked."""
        from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck

        rule = MandatoryFieldCheck(
            name="GlobalsCondition",
            severity=Severity.ERROR,
            params={"fields": ["email"]},
            condition="globals()['account_type'] == 'PREMIUM'"
        )

        df = pd.DataFrame({
            'account_type': ['PREMIUM'],
            'email': ['a@b.com']
        })

        from validation_framework.core.exceptions import ConditionEvaluationError
        with pytest.raises(ConditionEvaluationError) as exc_info:
            result = rule.validate(iter([df]), {})

        assert "security validation" in str(exc_info.value).lower()


class TestErrorMessageSafety:
    """Test that error messages don't leak sensitive information."""

    def test_error_message_sanitizes_content(self):
        """Error messages should not expose expression content."""
        rule = InlineBusinessRuleCheck(
            name="SecretRule",
            severity=Severity.ERROR,
            params={
                "rule": "secret_password == 'password123'",
                "description": "Check secret",
                "error_message": "Secret check failed"
            }
        )

        # DataFrame without the column - will cause eval error
        df = pd.DataFrame({'age': [25]})

        # Should raise ValueError with safe error message
        with pytest.raises(ValueError) as exc_info:
            result = rule.validate(iter([df]), {})

        error_msg = str(exc_info.value)
        # Error message should not contain 'password123'
        assert 'password123' not in error_msg
        # Error message should not contain 'secret_password'
        assert 'secret_password' not in error_msg
        # Should contain helpful guidance
        assert 'pandas query syntax' in error_msg.lower() or 'expression' in error_msg.lower()


class TestBackwardCompatibility:
    """Ensure existing validations continue to work."""

    def test_existing_age_validation(self):
        """Existing age validation should work."""
        rule = InlineBusinessRuleCheck(
            name="AgeCheck",
            severity=Severity.ERROR,
            params={
                "rule": "age >= 18",
                "description": "Must be 18+",
                "error_message": "Too young"
            }
        )

        df = pd.DataFrame({'age': [20, 15, 30]})
        result = rule.validate(iter([df]), {})

        assert result.failed_count == 1

    def test_existing_amount_range_validation(self):
        """Existing amount range validation should work."""
        rule = InlineBusinessRuleCheck(
            name="AmountRange",
            severity=Severity.ERROR,
            params={
                "rule": "amount >= 0 AND amount <= 1000000",
                "description": "Amount in range",
                "error_message": "Amount out of range"
            }
        )

        df = pd.DataFrame({'amount': [100, -50, 999999, 2000000]})
        result = rule.validate(iter([df]), {})

        assert result.failed_count == 2  # Negative and too large

    def test_existing_conditional_validation(self):
        """Existing conditional validation should work."""
        rule = InlineBusinessRuleCheck(
            name="ConditionalCheck",
            severity=Severity.ERROR,
            params={
                "rule": "(type == 'A' AND value > 100) OR (type != 'A')",
                "description": "Type A must have value > 100",
                "error_message": "Type A constraint failed"
            }
        )

        df = pd.DataFrame({
            'type': ['A', 'B', 'A', 'B'],
            'value': [150, 50, 50, 200]
        })
        result = rule.validate(iter([df]), {})

        assert result.failed_count == 1  # Type A with value 50
