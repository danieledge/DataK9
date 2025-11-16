"""
Unit tests for validation rules.

Tests core validation logic for built-in validation types.
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path

from validation_framework.core.results import Severity, ValidationResult
from validation_framework.validations.builtin.file_checks import EmptyFileCheck, RowCountRangeCheck
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck, RangeCheck, RegexCheck
from validation_framework.validations.builtin.record_checks import DuplicateRowCheck


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", None, "Eve"],
        "email": ["alice@test.com", "bob@test.com", "invalid", "david@test.com", None],
        "age": [25, 30, 35, 40, 45],
        "balance": [100.50, 200.75, 300.00, 400.25, 500.50]
    })


@pytest.fixture
def temp_csv_file(sample_dataframe):
    """Create a temporary CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_dataframe.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink()


@pytest.mark.unit
class TestEmptyFileCheck:
    """Tests for EmptyFileCheck validation."""

    def test_empty_file_check_fails_for_empty_file(self):
        """Test that empty file check fails for empty files."""
        # Create truly empty file (0 bytes)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            # Don't write anything - create 0 byte file
            temp_path = f.name

        try:
            validation = EmptyFileCheck(
                name="EmptyFileCheck",
                severity=Severity.ERROR,
                params={}
            )

            context = {
                "file_path": temp_path,
                "is_empty": True,
                "estimated_rows": 0
            }

            result = validation.validate_file(context)

            assert result.passed is False
            assert "empty" in result.message.lower()
            assert result.severity == Severity.ERROR
        finally:
            Path(temp_path).unlink()

    def test_empty_file_check_passes_for_non_empty_file(self):
        """Test that empty file check passes for non-empty files."""
        # Create non-empty file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col1,col2\nval1,val2\n")  # Header + data
            temp_path = f.name

        try:
            validation = EmptyFileCheck(
                name="EmptyFileCheck",
                severity=Severity.ERROR,
                params={}
            )

            context = {
                "file_path": temp_path,
                "is_empty": False,
                "estimated_rows": 100
            }

            result = validation.validate_file(context)

            assert result.passed is True
            assert result.severity == Severity.ERROR
        finally:
            Path(temp_path).unlink()


@pytest.mark.unit
class TestRowCountRangeCheck:
    """Tests for RowCountRangeCheck validation."""

    def test_row_count_within_range_passes(self):
        """Test that row count within range passes."""
        validation = RowCountRangeCheck(
            name="RowCountRangeCheck",
            severity=Severity.WARNING,
            params={"min_rows": 1, "max_rows": 100}
        )

        context = {
            "estimated_rows": 50
        }

        result = validation.validate_file(context)

        assert result.passed is True

    def test_row_count_below_minimum_fails(self):
        """Test that row count below minimum fails."""
        validation = RowCountRangeCheck(
            name="RowCountRangeCheck",
            severity=Severity.ERROR,
            params={"min_rows": 100, "max_rows": 1000}
        )

        context = {
            "estimated_rows": 50
        }

        result = validation.validate_file(context)

        assert result.passed is False
        assert "below minimum" in result.message.lower()

    def test_row_count_above_maximum_fails(self):
        """Test that row count above maximum fails."""
        validation = RowCountRangeCheck(
            name="RowCountRangeCheck",
            severity=Severity.WARNING,
            params={"min_rows": 1, "max_rows": 10}
        )

        context = {
            "estimated_rows": 100
        }

        result = validation.validate_file(context)

        assert result.passed is False
        assert "exceeds maximum" in result.message.lower()


@pytest.mark.unit
class TestMandatoryFieldCheck:
    """Tests for MandatoryFieldCheck validation."""

    def test_mandatory_field_all_present_passes(self, sample_dataframe):
        """Test that mandatory field check passes when all values present."""
        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["id", "age"]}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is True

    def test_mandatory_field_missing_values_fails(self, sample_dataframe):
        """Test that mandatory field check fails when values missing."""
        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["name", "email"]}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert result.failed_count > 0
        assert len(result.sample_failures) > 0

    def test_mandatory_field_missing_column_fails(self, sample_dataframe):
        """Test that mandatory field check fails when column doesn't exist."""
        validation = MandatoryFieldCheck(
            name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            params={"fields": ["nonexistent_column"]}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert "not found" in result.message.lower()


@pytest.mark.unit
class TestRangeCheck:
    """Tests for RangeCheck validation."""

    def test_range_check_all_within_range_passes(self, sample_dataframe):
        """Test that range check passes when all values in range."""
        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "age",
                "min_value": 0,
                "max_value": 100
            }
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is True

    def test_range_check_values_outside_range_fails(self):
        """Test that range check fails when values outside range."""
        df = pd.DataFrame({
            "value": [1, 2, 150, 4, 200]  # 150 and 200 are outside range
        })

        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "value",
                "min_value": 0,
                "max_value": 100
            }
        )

        data_iterator = iter([df])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert result.failed_count == 2
        assert len(result.sample_failures) == 2

    def test_range_check_missing_field_fails(self, sample_dataframe):
        """Test that range check fails when field doesn't exist."""
        validation = RangeCheck(
            name="RangeCheck",
            severity=Severity.ERROR,
            params={
                "field": "nonexistent",
                "min_value": 0,
                "max_value": 100
            }
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert "not found" in result.message.lower()


@pytest.mark.unit
class TestRegexCheck:
    """Tests for RegexCheck validation."""

    def test_regex_check_valid_emails_passes(self):
        """Test that regex check passes for valid email addresses."""
        df = pd.DataFrame({
            "email": ["test@example.com", "user@domain.org", "name@test.co.uk"]
        })

        validation = RegexCheck(
            name="RegexCheck",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            }
        )

        data_iterator = iter([df])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is True

    def test_regex_check_invalid_pattern_fails(self, sample_dataframe):
        """Test that regex check fails for invalid patterns."""
        validation = RegexCheck(
            name="RegexCheck",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            }
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert result.failed_count >= 1  # "invalid" doesn't match pattern


@pytest.mark.unit
class TestDuplicateRowCheck:
    """Tests for DuplicateRowCheck validation."""

    def test_duplicate_check_no_duplicates_passes(self, sample_dataframe):
        """Test that duplicate check passes when no duplicates."""
        validation = DuplicateRowCheck(
            name="DuplicateRowCheck",
            severity=Severity.ERROR,
            params={"key_fields": ["id"]}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is True

    def test_duplicate_check_with_duplicates_fails(self):
        """Test that duplicate check fails when duplicates exist."""
        df = pd.DataFrame({
            "id": [1, 2, 2, 3, 3],  # 2 and 3 are duplicated
            "name": ["A", "B", "B", "C", "C"]
        })

        validation = DuplicateRowCheck(
            name="DuplicateRowCheck",
            severity=Severity.ERROR,
            params={"key_fields": ["id"]}
        )

        data_iterator = iter([df])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert result.failed_count == 2  # Two duplicate keys

    def test_duplicate_check_missing_key_field_fails(self, sample_dataframe):
        """Test that duplicate check fails when key field doesn't exist."""
        validation = DuplicateRowCheck(
            name="DuplicateRowCheck",
            severity=Severity.ERROR,
            params={"key_fields": ["nonexistent"]}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert result.passed is False
        assert "not found" in result.message.lower()


@pytest.mark.unit
class TestValidationResultCreation:
    """Tests for ValidationResult creation."""

    def test_validation_result_structure(self, sample_dataframe):
        """Test that validation result has correct structure."""
        validation = RangeCheck(
            name="TestValidation",
            severity=Severity.WARNING,
            params={"field": "age", "min_value": 0, "max_value": 100}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)

        assert isinstance(result, ValidationResult)
        assert result.rule_name == "TestValidation"
        assert result.severity == Severity.WARNING
        assert isinstance(result.passed, bool)
        assert isinstance(result.message, str)
        assert isinstance(result.failed_count, int)
        assert isinstance(result.total_count, int)
        assert isinstance(result.sample_failures, list)

    def test_validation_result_to_dict(self, sample_dataframe):
        """Test converting validation result to dictionary."""
        validation = MandatoryFieldCheck(
            name="TestCheck",
            severity=Severity.ERROR,
            params={"fields": ["id"]}
        )

        data_iterator = iter([sample_dataframe])
        context = {}

        result = validation.validate(data_iterator, context)
        result_dict = result.to_dict()

        assert result_dict["rule_name"] == "TestCheck"
        assert result_dict["severity"] == "ERROR"
        assert "passed" in result_dict
        assert "message" in result_dict
        assert "success_rate" in result_dict
