"""
Comprehensive tests for field-level validation rules.

This test suite covers all field validation checks including mandatory fields,
regex patterns, valid values, ranges, dates, string lengths, and numeric precision.

Author: Daniel Edge
"""

import pytest
import pandas as pd
from datetime import datetime, date

from validation_framework.validations.builtin.field_checks import (
    MandatoryFieldCheck,
    RegexCheck,
    ValidValuesCheck,
    RangeCheck,
    DateFormatCheck
)
from validation_framework.validations.builtin.advanced_checks import (
    StringLengthCheck,
    NumericPrecisionCheck
)
from validation_framework.core.results import Severity
from tests.conftest import create_data_iterator


# ============================================================================
# MANDATORY FIELD CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestMandatoryFieldCheck:
    """Test MandatoryFieldCheck validation."""

    def test_all_fields_present(self):
        """Test validation passes when all mandatory fields have values."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["a@test.com", "b@test.com", "c@test.com"]
        })

        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["id", "name", "email"]}
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True
        assert result.failed_count == 0

    def test_missing_values_detected(self):
        """Test validation fails when fields have missing values."""
        df = pd.DataFrame({
            "id": [1, None, 3],
            "name": ["Alice", "Bob", None],
            "email": ["a@test.com", "b@test.com", "c@test.com"]
        })

        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["id", "name"]}
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count > 0

    def test_empty_strings_as_missing(self):
        """Test that empty strings are treated as missing values."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "", "Charlie"]
        })

        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["name"]}
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 1

    def test_nonexistent_field_raises_error(self):
        """Test validation handles non-existent fields appropriately."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["nonexistent_field"]}
        )

        # Should either skip gracefully or raise error
        try:
            result = validation.validate(create_data_iterator(df), {})
            # If it doesn't raise, it should fail validation
            assert result.passed is False
        except (KeyError, ValueError):
            # Acceptable to raise error for missing field
            pass


# ============================================================================
# REGEX CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestRegexCheck:
    """Test RegexCheck validation."""

    def test_email_pattern_validation(self):
        """Test email validation using regex pattern."""
        df = pd.DataFrame({
            "email": ["user@example.com", "test@test.org", "admin@company.net"]
        })

        validation = RegexCheck(
            name="RegexCheck",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r'^[\w\.-]+@[\w\.-]+\.\w+$'
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_invalid_email_detected(self):
        """Test that invalid emails are detected."""
        df = pd.DataFrame({
            "email": ["valid@test.com", "invalid-email", "also@bad"]
        })

        validation = RegexCheck(
            name="RegexCheck",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r'^[\w\.-]+@[\w\.-]+\.\w+$'
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 2

    def test_phone_number_pattern(self):
        """Test phone number validation."""
        df = pd.DataFrame({
            "phone": ["555-1234", "555-5678", "555-9999"]
        })

        validation = RegexCheck(
            name="RegexCheck",
            severity=Severity.ERROR,
            params={
                "field": "phone",
                "pattern": r'^\d{3}-\d{4}$'
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_postal_code_pattern(self):
        """Test postal code validation."""
        df = pd.DataFrame({
            "zip": ["12345", "67890", "11111"]
        })

        validation = RegexCheck(
            name="RegexCheck",
            severity=Severity.ERROR,
            params={
                "field": "zip",
                "pattern": r'^\d{5}$'
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True


# ============================================================================
# VALID VALUES CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestValidValuesCheck:
    """Test ValidValuesCheck validation."""

    def test_all_values_in_list(self):
        """Test validation passes when all values are in allowed list."""
        df = pd.DataFrame({
            "status": ["active", "inactive", "active", "pending"]
        })

        validation = ValidValuesCheck(
            name="ValidValuesCheck",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "valid_values": ["active", "inactive", "pending"]
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_invalid_values_detected(self):
        """Test validation fails for values not in allowed list."""
        df = pd.DataFrame({
            "status": ["active", "invalid", "deleted", "pending"]
        })

        validation = ValidValuesCheck(
            name="ValidValuesCheck",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "valid_values": ["active", "inactive", "pending"]
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 2

    def test_case_sensitive_validation(self):
        """Test that validation is case-sensitive."""
        df = pd.DataFrame({
            "status": ["Active", "INACTIVE", "pending"]
        })

        validation = ValidValuesCheck(
            name="ValidValuesCheck",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "valid_values": ["active", "inactive", "pending"]
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        # Should fail because of case mismatch
        assert result.passed is False

    def test_numeric_valid_values(self):
        """Test validation with numeric valid values."""
        df = pd.DataFrame({
            "priority": ["1", "2", "3", "1", "2"]
        })

        validation = ValidValuesCheck(
            name="ValidValuesCheck",
            severity=Severity.ERROR,
            params={
                "field": "priority",
                "valid_values": ["1", "2", "3"]
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True


# ============================================================================
# RANGE CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestRangeCheck:
    """Test RangeCheck validation."""

    def test_values_within_range(self):
        """Test validation passes when all values are within range."""
        df = pd.DataFrame({
            "age": [25, 30, 35, 40, 45]
        })

        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "age",
                "min_value": 18,
                "max_value": 65
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_values_outside_range_detected(self):
        """Test validation fails when values are outside range."""
        df = pd.DataFrame({
            "age": [15, 25, 70, 30, 100]
        })

        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "age",
                "min_value": 18,
                "max_value": 65
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 3

    def test_min_value_only(self):
        """Test range check with only minimum value."""
        df = pd.DataFrame({
            "salary": [30000, 50000, 75000, 100000]
        })

        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "salary",
                "min_value": 25000,
                "max_value": None
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_max_value_only(self):
        """Test range check with only maximum value."""
        df = pd.DataFrame({
            "discount": [0.05, 0.10, 0.15, 0.20]
        })

        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "discount",
                "min_value": None,
                "max_value": 0.25
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_boundary_values(self):
        """Test that boundary values are included in valid range."""
        df = pd.DataFrame({
            "score": [0, 50, 100]
        })

        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "score",
                "min_value": 0,
                "max_value": 100
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True


# ============================================================================
# DATE FORMAT CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestDateFormatCheck:
    """Test DateFormatCheck validation."""

    def test_valid_date_format(self):
        """Test validation passes for correct date format."""
        df = pd.DataFrame({
            "date": ["2025-01-01", "2025-02-15", "2025-12-31"]
        })

        validation = DateFormatCheck(
            name="DateFormatCheck",
            severity=Severity.ERROR,
            params={
                "field": "date",
                "format": "%Y-%m-%d"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_invalid_date_format_detected(self):
        """Test validation fails for incorrect date format."""
        df = pd.DataFrame({
            "date": ["2025-01-01", "01/15/2025", "2025-13-45"]
        })

        validation = DateFormatCheck(
            name="DateFormatCheck",
            severity=Severity.ERROR,
            params={
                "field": "date",
                "format": "%Y-%m-%d"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False

    def test_us_date_format(self):
        """Test US date format validation."""
        df = pd.DataFrame({
            "date": ["01/15/2025", "02/28/2025", "12/31/2025"]
        })

        validation = DateFormatCheck(
            name="DateFormatCheck",
            severity=Severity.ERROR,
            params={
                "field": "date",
                "format": "%m/%d/%Y"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_datetime_format(self):
        """Test datetime format with time component."""
        df = pd.DataFrame({
            "timestamp": ["2025-01-01 10:30:00", "2025-02-15 14:45:30"]
        })

        validation = DateFormatCheck(
            name="DateFormatCheck",
            severity=Severity.ERROR,
            params={
                "field": "timestamp",
                "format": "%Y-%m-%d %H:%M:%S"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True


# ============================================================================
# STRING LENGTH CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestStringLengthCheck:
    """Test StringLengthCheck validation."""

    def test_strings_within_length_range(self):
        """Test validation passes when strings are within length range."""
        df = pd.DataFrame({
            "name": ["Alice", "Bob", "Charlie", "Diana"]
        })

        validation = StringLengthCheck(
            name="StringLengthCheck",
            severity=Severity.ERROR,
            params={
                "field": "name",
                "min_length": 3,
                "max_length": 10
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_strings_too_short_detected(self):
        """Test validation fails when strings are too short."""
        df = pd.DataFrame({
            "code": ["AB", "XYZ", "ABCD", "A"]
        })

        validation = StringLengthCheck(
            name="StringLengthCheck",
            severity=Severity.ERROR,
            params={
                "field": "code",
                "min_length": 3,
                "max_length": 10
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 2

    def test_strings_too_long_detected(self):
        """Test validation fails when strings are too long."""
        df = pd.DataFrame({
            "description": ["Short", "Medium length", "This is a very long description"]
        })

        validation = StringLengthCheck(
            name="StringLengthCheck",
            severity=Severity.ERROR,
            params={
                "field": "description",
                "min_length": 1,
                "max_length": 15
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False


# ============================================================================
# NUMERIC PRECISION CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestNumericPrecisionCheck:
    """Test NumericPrecisionCheck validation."""

    def test_correct_decimal_places(self):
        """Test validation passes for correct decimal precision."""
        df = pd.DataFrame({
            "amount": [10.50, 25.75, 100.00, 33.25]
        })

        validation = NumericPrecisionCheck(
            name="NumericPrecisionCheck",
            severity=Severity.ERROR,
            params={
                "field": "amount",
                "max_decimal_places": 2
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_excessive_decimals_detected(self):
        """Test validation fails when decimals exceed limit."""
        df = pd.DataFrame({
            "price": [10.50, 25.123, 100.4567]
        })

        validation = NumericPrecisionCheck(
            name="NumericPrecisionCheck",
            severity=Severity.ERROR,
            params={
                "field": "price",
                "max_decimal_places": 2
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False

    def test_integers_pass_precision_check(self):
        """Test that integers pass precision check."""
        df = pd.DataFrame({
            "count": [10, 25, 100, 33]
        })

        validation = NumericPrecisionCheck(
            name="NumericPrecisionCheck",
            severity=Severity.ERROR,
            params={
                "field": "count",
                "max_decimal_places": 0
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
