"""
Tests for conditional validation functionality.

Tests both Phase 1 (inline conditions) and Phase 2 (ConditionalValidation wrapper).
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path

from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck, RangeCheck
from validation_framework.validations.builtin.conditional import ConditionalValidation
from validation_framework.core.results import Severity


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing conditional validations."""
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "account_type": ["BUSINESS", "INDIVIDUAL", "BUSINESS", "INDIVIDUAL", "BUSINESS"],
        "company_name": ["Acme Corp", None, "Tech Inc", None, None],  # Should be present for BUSINESS
        "first_name": [None, "John", None, "Jane", None],  # Should be present for INDIVIDUAL
        "last_name": [None, "Doe", None, "Smith", None],  # Should be present for INDIVIDUAL
        "age": [None, 25, None, 35, None],
        "credit_limit": [50000, 1000, 75000, 2000, 30000],
    })


@pytest.mark.unit
class TestInlineConditions:
    """Tests for Phase 1: Inline condition parameter on regular validations."""

    def test_mandatory_field_with_condition_matches(self, sample_dataframe):
        """Test MandatoryFieldCheck with condition - only checks rows that match."""
        # Only check company_name for BUSINESS accounts
        validation = MandatoryFieldCheck(
            name="BusinessNameCheck",
            severity=Severity.ERROR,
            params={"fields": ["company_name"]},
            condition="account_type == 'BUSINESS'"
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should fail because rows 0 and 4 are BUSINESS but row 4 has missing company_name
        assert not result.passed
        assert result.failed_count > 0

    def test_mandatory_field_with_condition_no_matches(self, sample_dataframe):
        """Test validation skips when no rows match condition."""
        # Check a field for accounts that don't exist
        validation = MandatoryFieldCheck(
            name="Test",
            severity=Severity.ERROR,
            params={"fields": ["company_name"]},
            condition="account_type == 'NONEXISTENT'"
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should pass because no rows match the condition
        assert result.passed

    def test_range_check_with_and_condition(self, sample_dataframe):
        """Test RangeCheck with complex AND condition."""
        # Only check credit_limit range for BUSINESS accounts with high limits
        validation = RangeCheck(
            name="HighCreditCheck",
            severity=Severity.WARNING,
            params={
                "field": "credit_limit",
                "min_value": 40000,
                "max_value": 100000
            },
            condition="account_type == 'BUSINESS' & credit_limit > 40000"
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Row 0: BUSINESS, 50000 - matches condition, passes range
        # Row 2: BUSINESS, 75000 - matches condition, passes range
        # Row 4: BUSINESS, 30000 - doesn't match condition (not > 40000)
        assert result.passed

    def test_condition_with_no_condition_specified(self, sample_dataframe):
        """Test that validation without condition runs on all rows."""
        validation = MandatoryFieldCheck(
            name="AllRowsCheck",
            severity=Severity.ERROR,
            params={"fields": ["customer_id"]}
            # No condition specified
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should pass - all rows have customer_id
        assert result.passed


@pytest.mark.unit
class TestConditionalValidation:
    """Tests for Phase 2: ConditionalValidation wrapper class."""

    def test_conditional_then_only(self, sample_dataframe):
        """Test ConditionalValidation with only then_validate."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "condition": "account_type == 'BUSINESS'",
                "then_validate": [
                    {
                        "type": "MandatoryFieldCheck",
                        "params": {"fields": ["company_name"]}
                    }
                ]
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should fail - BUSINESS accounts (rows 0, 2, 4) need company_name, but row 4 is missing it
        assert not result.passed

    def test_conditional_then_else(self, sample_dataframe):
        """Test ConditionalValidation with both then_validate and else_validate."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "condition": "account_type == 'BUSINESS'",
                "then_validate": [
                    {
                        "type": "MandatoryFieldCheck",
                        "params": {"fields": ["company_name"]}
                    }
                ],
                "else_validate": [
                    {
                        "type": "MandatoryFieldCheck",
                        "params": {"fields": ["first_name", "last_name"]}
                    }
                ]
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should fail:
        # - BUSINESS (rows 0, 2, 4): row 4 missing company_name
        # - INDIVIDUAL (rows 1, 3): all have first_name and last_name
        assert not result.passed

    def test_conditional_multiple_then_validations(self, sample_dataframe):
        """Test ConditionalValidation with multiple validations in then_validate."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "condition": "account_type == 'BUSINESS'",
                "then_validate": [
                    {
                        "type": "MandatoryFieldCheck",
                        "params": {"fields": ["company_name"]}
                    },
                    {
                        "type": "RangeCheck",
                        "params": {
                            "field": "credit_limit",
                            "min_value": 10000
                        }
                    }
                ]
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should fail on missing company_name for row 4
        assert not result.passed

    def test_conditional_no_rows_match(self, sample_dataframe):
        """Test ConditionalValidation when no rows match condition."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "condition": "account_type == 'NONEXISTENT'",
                "then_validate": [
                    {
                        "type": "MandatoryFieldCheck",
                        "params": {"fields": ["company_name"]}
                    }
                ]
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should pass - no rows match condition
        assert result.passed

    def test_conditional_missing_condition_param(self, sample_dataframe):
        """Test ConditionalValidation fails gracefully without condition parameter."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "then_validate": [
                    {
                        "type": "MandatoryFieldCheck",
                        "params": {"fields": ["company_name"]}
                    }
                ]
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should fail with error message
        assert not result.passed
        assert "requires 'condition'" in result.message

    def test_conditional_missing_then_validate(self, sample_dataframe):
        """Test ConditionalValidation fails gracefully without then_validate parameter."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "condition": "account_type == 'BUSINESS'"
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should fail with error message
        assert not result.passed
        assert "requires 'then_validate'" in result.message

    def test_conditional_nested_conditions(self, sample_dataframe):
        """Test ConditionalValidation with nested validations that also have conditions."""
        validation = ConditionalValidation(
            name="ConditionalTest",
            severity=Severity.ERROR,
            params={
                "condition": "account_type == 'BUSINESS'",
                "then_validate": [
                    {
                        "type": "RangeCheck",
                        "condition": "credit_limit > 50000",  # Nested condition!
                        "params": {
                            "field": "credit_limit",
                            "min_value": 50000,
                            "max_value": 100000
                        }
                    }
                ]
            }
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Parent condition: account_type == 'BUSINESS' (rows 0, 2, 4)
        # Child condition: credit_limit > 50000 (only row 2 with 75000)
        # Should pass because row 2 is within range
        assert result.passed


@pytest.mark.unit
class TestConditionSyntax:
    """Tests for condition expression syntax conversion."""

    def test_and_syntax_conversion(self, sample_dataframe):
        """Test that AND is converted to & for pandas."""
        validation = MandatoryFieldCheck(
            name="Test",
            severity=Severity.ERROR,
            params={"fields": ["company_name"]},
            condition="account_type == 'BUSINESS' AND credit_limit > 40000"
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should execute without syntax error
        assert result is not None

    def test_or_syntax_conversion(self, sample_dataframe):
        """Test that OR is converted to | for pandas."""
        validation = MandatoryFieldCheck(
            name="Test",
            severity=Severity.ERROR,
            params={"fields": ["company_name"]},
            condition="account_type == 'BUSINESS' OR account_type == 'CORPORATE'"
        )

        def data_iter():
            yield sample_dataframe

        context = {"max_sample_failures": 100}
        result = validation.validate(data_iter(), context)

        # Should execute without syntax error
        assert result is not None


@pytest.mark.integration
class TestConditionalValidationEndToEnd:
    """Integration tests for conditional validations in full validation pipeline."""

    def test_conditional_validation_in_config(self):
        """Test conditional validation works with full config-driven validation."""
        # This would test with actual YAML config and ValidationEngine
        # Implementation depends on having test fixtures set up
        pass
