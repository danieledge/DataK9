"""
Comprehensive tests for schema-level validation rules.

This test suite covers all schema validation checks including schema matching
and column presence validation.

Author: Daniel Edge
"""

import pytest
import pandas as pd

from validation_framework.validations.builtin.schema_checks import (
    SchemaMatchCheck,
    ColumnPresenceCheck
)
from validation_framework.core.results import Severity
from tests.conftest import create_data_iterator


# ============================================================================
# SCHEMA MATCH CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestSchemaMatchCheck:
    """Test SchemaMatchCheck validation."""
    
    def test_exact_schema_match(self):
        """Test validation passes when schema matches exactly."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35]
        })

        expected_schema = {
            "id": "int64",
            "name": "object",
            "age": "int64"
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )

        # Create context with columns and dtypes (FileValidationRule doesn't use data_iterator)
        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is True
    
    def test_schema_mismatch_detected(self):
        """Test validation fails when schema doesn't match."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": ["25", "30", "35"]  # String instead of int
        })

        expected_schema = {
            "id": "int64",
            "name": "object",
            "age": "int64"
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )

        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is False
    
    def test_missing_column_detected(self):
        """Test validation fails when expected column is missing."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        expected_schema = {
            "id": "int64",
            "name": "object",
            "age": "int64"  # This column is missing
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )

        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is False
    
    def test_extra_column_detected(self):
        """Test validation handles extra columns appropriately."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "extra_column": [100, 200, 300]  # Extra column
        })

        expected_schema = {
            "id": "int64",
            "name": "object",
            "age": "int64"
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema, "strict": True}
        )

        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        result = validation.validate(create_data_iterator(df), context)

        # Should fail in strict mode with extra columns
        assert result.passed is False
    
    def test_float_vs_int_type_mismatch(self):
        """Test detection of numeric type mismatches."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "amount": [100.5, 200.7, 300.9]  # Float
        })

        expected_schema = {
            "id": "int64",
            "amount": "int64"  # Expected int, got float
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )

        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is False


# ============================================================================
# COLUMN PRESENCE CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestColumnPresenceCheck:
    """Test ColumnPresenceCheck validation."""
    
    def test_all_required_columns_present(self):
        """Test validation passes when all required columns are present."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            "age": [25, 30, 35]
        })

        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id", "name", "email"]}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is True
    
    def test_missing_column_detected(self):
        """Test validation fails when required column is missing."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id", "name", "email"]}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is False

    def test_multiple_missing_columns_detected(self):
        """Test detection of multiple missing columns."""
        df = pd.DataFrame({
            "id": [1, 2, 3]
        })

        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id", "name", "email", "age", "phone"]}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is False
        # Should report multiple missing columns
        assert "name" in result.message or "email" in result.message

    def test_case_sensitive_column_names(self):
        """Test that column name matching is case-sensitive."""
        df = pd.DataFrame({
            "ID": [1, 2, 3],
            "Name": ["Alice", "Bob", "Charlie"]
        })

        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id", "name"]}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        # Should fail because column names don't match case
        assert result.passed is False

    def test_extra_columns_allowed(self):
        """Test that extra columns don't cause validation to fail."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            "extra1": [100, 200, 300],
            "extra2": ["X", "Y", "Z"]
        })

        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id", "name", "email"]}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        # Extra columns should not cause failure
        assert result.passed is True

    def test_empty_required_columns_list(self):
        """Test validation with empty required columns list."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        # This should raise a ParameterValidationError, so expect False result
        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": []}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        # Empty requirements should fail validation (ParameterValidationError)
        assert result.passed is False

    def test_single_column_requirement(self):
        """Test validation with single required column."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "other_data": ["A", "B", "C"]
        })

        validation = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id"]}
        )

        context = {"columns": list(df.columns)}
        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is True


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestSchemaValidationsIntegration:
    """Integration tests for schema-level validations."""
    
    def test_combined_schema_validations(self):
        """Test schema match and column presence together."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "email": ["a@test.com", "b@test.com", "c@test.com"]
        })

        # Schema match check
        expected_schema = {
            "id": "int64",
            "name": "object",
            "age": "int64",
            "email": "object"
        }

        schema_check = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )
        column_check = ColumnPresenceCheck(
            name="ColumnPresenceCheck",
            severity=Severity.ERROR,
            params={"required_columns": ["id", "name", "age", "email"]}
        )

        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        schema_result = schema_check.validate(create_data_iterator(df), context)
        column_result = column_check.validate(create_data_iterator(df), context)

        # Both should pass
        assert schema_result.passed is True
        assert column_result.passed is True
    
    def test_schema_evolution_detection(self):
        """Test detection of schema changes (evolution)."""
        # Original schema
        original_df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        # Evolved schema (new column added)
        evolved_df = pd.DataFrame({
            "id": [4, 5, 6],
            "name": ["Diana", "Eve", "Frank"],
            "new_column": ["A", "B", "C"]
        })

        expected_schema = {
            "id": "int64",
            "name": "object"
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )

        # Original should pass
        original_context = {
            "columns": list(original_df.columns),
            "dtypes": {col: str(original_df[col].dtype) for col in original_df.columns}
        }
        original_result = validation.validate(create_data_iterator(original_df), original_context)
        assert original_result.passed is True

        # Evolved should pass (not strict mode by default)
        evolved_context = {
            "columns": list(evolved_df.columns),
            "dtypes": {col: str(evolved_df[col].dtype) for col in evolved_df.columns}
        }
        evolved_result = validation.validate(create_data_iterator(evolved_df), evolved_context)
        # Should pass because strict mode is False by default (extra columns allowed)
        assert evolved_result.passed is True
    
    def test_type_coercion_scenarios(self):
        """Test schema validation with type coercion scenarios."""
        # DataFrame with mixed types that could be coerced
        df = pd.DataFrame({
            "id": ["1", "2", "3"],  # Strings that look like numbers
            "amount": [100, 200, 300]
        })

        expected_schema = {
            "id": "object",  # String type expected
            "amount": "int64"
        }

        validation = SchemaMatchCheck(
            name="SchemaMatchCheck",
            severity=Severity.ERROR,
            params={"expected_schema": expected_schema}
        )

        context = {
            "columns": list(df.columns),
            "dtypes": {col: str(df[col].dtype) for col in df.columns}
        }

        result = validation.validate(create_data_iterator(df), context)

        assert result.passed is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
