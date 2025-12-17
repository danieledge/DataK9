"""
Comprehensive tests for async validators.

Expands coverage beyond basic tests to include edge cases, error handling,
and integration scenarios.

Author: Daniel Edge
"""

import pytest
import asyncio
import pandas as pd
from typing import AsyncIterator

from validation_framework.validations.async_validators import (
    AsyncSchemaMatchCheck,
    AsyncLookupCheck,
    AsyncRegexCheck
)
from validation_framework.core.results import Severity, ValidationResult
from validation_framework.core.exceptions import (
    ParameterValidationError,
    ColumnNotFoundError,
    ValidationExecutionError
)


class TestAsyncSchemaMatchCheckComprehensive:
    """Comprehensive tests for AsyncSchemaMatchCheck."""

    @pytest.mark.asyncio
    async def test_schema_match_all_types(self):
        """Test schema validation with various data types."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "int_col": "integer",
                    "float_col": "float",
                    "str_col": "string",
                    "date_col": "date",
                    "bool_col": "boolean"
                }
            }
        )

        context = {
            "columns": ["int_col", "float_col", "str_col", "date_col", "bool_col"],
            "dtypes": {
                "int_col": "int64",
                "float_col": "float64",
                "str_col": "object",
                "date_col": "datetime64",
                "bool_col": "bool"
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_schema_type_mismatch(self):
        """Test schema validation fails with type mismatch."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "id": "integer",
                    "value": "string"  # Expected string
                }
            }
        )

        context = {
            "columns": ["id", "value"],
            "dtypes": {
                "id": "int64",
                "value": "float64"  # Actual float
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is False
        assert "Type mismatches" in result.message
        assert "value" in result.message

    @pytest.mark.asyncio
    async def test_schema_strict_mode_extra_columns(self):
        """Test strict mode rejects extra columns."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "id": "integer",
                    "name": "string"
                },
                "strict": True
            }
        )

        context = {
            "columns": ["id", "name", "extra_col"],
            "dtypes": {
                "id": "int64",
                "name": "object",
                "extra_col": "float64"
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is False
        assert "Unexpected columns" in result.message
        assert "extra_col" in result.message

    @pytest.mark.asyncio
    async def test_schema_check_order_correct(self):
        """Test column order validation passes."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "col1": "integer",
                    "col2": "string",
                    "col3": "float"
                },
                "check_order": True
            }
        )

        context = {
            "columns": ["col1", "col2", "col3"],
            "dtypes": {
                "col1": "int64",
                "col2": "object",
                "col3": "float64"
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_schema_check_order_wrong(self):
        """Test column order validation fails."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "col1": "integer",
                    "col2": "string",
                    "col3": "float"
                },
                "check_order": True
            }
        )

        context = {
            "columns": ["col2", "col1", "col3"],  # Wrong order
            "dtypes": {
                "col1": "int64",
                "col2": "object",
                "col3": "float64"
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is False
        assert "order mismatch" in result.message.lower()

    @pytest.mark.asyncio
    async def test_schema_no_columns_in_context(self):
        """Test handling of missing columns in context."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {"id": "integer"}
            }
        )

        context = {}  # No columns

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is False
        assert "Could not determine" in result.message

    @pytest.mark.asyncio
    async def test_schema_no_expected_schema(self):
        """Test error when no expected_schema provided."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={}  # No expected_schema
        )

        context = {
            "columns": ["id"],
            "dtypes": {"id": "int64"}
        }

        async def empty_iter():
            return
            yield

        with pytest.raises(ParameterValidationError):
            await validator.validate_file_async(context)

    @pytest.mark.asyncio
    async def test_schema_type_any_accepts_all(self):
        """Test that 'any' type accepts any actual type."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "flexible_col": "any"
                }
            }
        )

        context = {
            "columns": ["flexible_col"],
            "dtypes": {"flexible_col": "whatever_type"}
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_schema_number_type_accepts_int_or_float(self):
        """Test that 'number' type accepts both int and float."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "num1": "number",
                    "num2": "number"
                }
            }
        )

        context = {
            "columns": ["num1", "num2"],
            "dtypes": {
                "num1": "int64",
                "num2": "float64"
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_file_async(context)
        assert result.passed is True


class TestAsyncLookupCheckComprehensive:
    """Comprehensive tests for AsyncLookupCheck."""

    async def create_data_iterator(self, df: pd.DataFrame) -> AsyncIterator[pd.DataFrame]:
        """Helper to create async data iterator."""
        yield df

    @pytest.mark.asyncio
    async def test_lookup_allow_all_valid(self):
        """Test allow lookup with all valid values."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "reference_values": ["active", "inactive", "pending"],
                "check_type": "allow"
            }
        )

        df = pd.DataFrame({
            "status": ["active", "inactive", "pending", "active", "inactive"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is True

    @pytest.mark.asyncio
    async def test_lookup_allow_some_invalid(self):
        """Test allow lookup with some invalid values."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "reference_values": ["active", "inactive"],
                "check_type": "allow"
            }
        )

        df = pd.DataFrame({
            "status": ["active", "invalid", "inactive", "bad_value"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is False
        assert result.failed_count == 2

    @pytest.mark.asyncio
    async def test_lookup_deny_mode(self):
        """Test deny mode blocks specific values."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "reference_values": ["blocked", "banned"],
                "check_type": "deny"
            }
        )

        df = pd.DataFrame({
            "status": ["active", "blocked", "inactive", "banned"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is False
        assert result.failed_count == 2

    @pytest.mark.asyncio
    async def test_lookup_missing_field_parameter(self):
        """Test error when field parameter is missing."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "reference_values": ["A", "B", "C"]
            }
        )

        df = pd.DataFrame({"col": [1, 2, 3]})

        with pytest.raises(ParameterValidationError):
            await validator.validate_async(
                self.create_data_iterator(df),
                {}
            )

    @pytest.mark.asyncio
    async def test_lookup_no_reference_data(self):
        """Test error when no reference data provided."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "status"
            }
        )

        df = pd.DataFrame({"status": ["active"]})

        with pytest.raises(ParameterValidationError):
            await validator.validate_async(
                self.create_data_iterator(df),
                {}
            )

    @pytest.mark.asyncio
    async def test_lookup_column_not_found(self):
        """Test error when field doesn't exist in data."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "missing_column",
                "reference_values": ["A", "B"]
            }
        )

        df = pd.DataFrame({"actual_column": [1, 2, 3]})

        with pytest.raises(ColumnNotFoundError):
            await validator.validate_async(
                self.create_data_iterator(df),
                {}
            )

    @pytest.mark.asyncio
    async def test_lookup_with_null_values(self):
        """Test lookup handles null values correctly."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "status",
                "reference_values": ["active", "inactive"],
                "check_type": "allow"
            }
        )

        df = pd.DataFrame({
            "status": ["active", None, "inactive", pd.NA, "active"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        # Nulls should be skipped, only non-null values validated
        assert result.passed is True

    @pytest.mark.asyncio
    async def test_lookup_large_reference_list(self):
        """Test lookup with large reference list."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "code",
                "reference_values": [str(i) for i in range(10000)],
                "check_type": "allow"
            }
        )

        df = pd.DataFrame({
            "code": ["42", "100", "999", "5000"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is True

    @pytest.mark.asyncio
    async def test_lookup_chunked_processing(self):
        """Test lookup works across multiple chunks."""
        validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "category",
                "reference_values": ["A", "B", "C"],
                "check_type": "allow"
            }
        )

        async def multi_chunk_iterator():
            yield pd.DataFrame({"category": ["A", "B"]})
            yield pd.DataFrame({"category": ["C", "A"]})
            yield pd.DataFrame({"category": ["B", "D"]})  # D is invalid

        result = await validator.validate_async(multi_chunk_iterator(), {})

        assert result.passed is False
        assert result.failed_count == 1


class TestAsyncRegexCheckComprehensive:
    """Comprehensive tests for AsyncRegexCheck."""

    async def create_data_iterator(self, df: pd.DataFrame) -> AsyncIterator[pd.DataFrame]:
        """Helper to create async data iterator."""
        yield df

    @pytest.mark.asyncio
    async def test_regex_email_validation(self):
        """Test email regex validation."""
        validator = AsyncRegexCheck(
            name="email_check",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "should_match": True,
                "description": "Valid email format"
            }
        )

        df = pd.DataFrame({
            "email": ["test@example.com", "user@domain.org", "valid@email.co.uk"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is True

    @pytest.mark.asyncio
    async def test_regex_invalid_emails(self):
        """Test regex catches invalid emails."""
        validator = AsyncRegexCheck(
            name="email_check",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "should_match": True
            }
        )

        df = pd.DataFrame({
            "email": ["valid@example.com", "invalid.email", "no-at-sign.com"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is False
        assert result.failed_count == 2

    @pytest.mark.asyncio
    async def test_regex_phone_number(self):
        """Test phone number pattern validation."""
        validator = AsyncRegexCheck(
            name="phone_check",
            severity=Severity.ERROR,
            params={
                "field": "phone",
                "pattern": r"^\d{3}-\d{3}-\d{4}$",
                "should_match": True,
                "description": "Phone format: XXX-XXX-XXXX"
            }
        )

        df = pd.DataFrame({
            "phone": ["123-456-7890", "555-123-4567"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is True

    @pytest.mark.asyncio
    async def test_regex_should_not_match(self):
        """Test regex with should_match=False."""
        validator = AsyncRegexCheck(
            name="no_special_chars",
            severity=Severity.ERROR,
            params={
                "field": "username",
                "pattern": r"[!@#$%^&*()]",
                "should_match": False,  # Should NOT contain special chars
                "description": "No special characters allowed"
            }
        )

        df = pd.DataFrame({
            "username": ["john_doe", "jane_smith", "user123"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is True

    @pytest.mark.asyncio
    async def test_regex_should_not_match_fails(self):
        """Test regex should_match=False catches violations."""
        validator = AsyncRegexCheck(
            name="no_special_chars",
            severity=Severity.ERROR,
            params={
                "field": "username",
                "pattern": r"[!@#$%^&*()]",
                "should_match": False
            }
        )

        df = pd.DataFrame({
            "username": ["good_user", "bad@user", "another!user"]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is False
        assert result.failed_count == 2

    @pytest.mark.asyncio
    async def test_regex_invalid_pattern(self):
        """Test handling of invalid regex pattern."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "data",
                "pattern": "[invalid(regex",  # Invalid pattern
                "should_match": True
            }
        )

        df = pd.DataFrame({"data": ["test"]})

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is False
        assert "Invalid regex pattern" in result.message

    @pytest.mark.asyncio
    async def test_regex_missing_parameters(self):
        """Test error when required parameters missing."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "data"
                # Missing pattern
            }
        )

        df = pd.DataFrame({"data": ["test"]})

        with pytest.raises(ParameterValidationError):
            await validator.validate_async(
                self.create_data_iterator(df),
                {}
            )

    @pytest.mark.asyncio
    async def test_regex_column_not_found(self):
        """Test error when column doesn't exist."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "missing_field",
                "pattern": r".*"
            }
        )

        df = pd.DataFrame({"actual_field": ["data"]})

        with pytest.raises(ColumnNotFoundError):
            await validator.validate_async(
                self.create_data_iterator(df),
                {}
            )

    @pytest.mark.asyncio
    async def test_regex_with_null_values(self):
        """Test regex handles null values correctly."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "code",
                "pattern": r"^[A-Z]{3}\d{3}$",
                "should_match": True
            }
        )

        df = pd.DataFrame({
            "code": ["ABC123", None, "XYZ789", pd.NA]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        # Nulls are skipped
        assert result.passed is True
        assert result.total_count == 4

    @pytest.mark.asyncio
    async def test_regex_case_sensitivity(self):
        """Test case-sensitive regex matching."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "code",
                "pattern": r"^[A-Z]+$",  # Only uppercase
                "should_match": True
            }
        )

        df = pd.DataFrame({
            "code": ["ABC", "XYZ", "abc"]  # Last one is lowercase
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is False
        assert result.failed_count == 1

    @pytest.mark.asyncio
    async def test_regex_complex_pattern(self):
        """Test complex regex pattern."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "url",
                "pattern": r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$",
                "should_match": True,
                "description": "Valid URL"
            }
        )

        df = pd.DataFrame({
            "url": [
                "https://example.com",
                "http://test.org/path",
                "https://sub.domain.co.uk/page?query=1"
            ]
        })

        result = await validator.validate_async(
            self.create_data_iterator(df),
            {}
        )

        assert result.passed is True

    @pytest.mark.asyncio
    async def test_regex_chunked_processing(self):
        """Test regex validation across multiple chunks."""
        validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "code",
                "pattern": r"^\d{4}$",
                "should_match": True
            }
        )

        async def multi_chunk_iterator():
            yield pd.DataFrame({"code": ["1234", "5678"]})
            yield pd.DataFrame({"code": ["9012", "3456"]})
            yield pd.DataFrame({"code": ["7890", "BAD"]})  # Last one invalid

        result = await validator.validate_async(multi_chunk_iterator(), {})

        assert result.passed is False
        assert result.failed_count == 1
        assert result.total_count == 6
