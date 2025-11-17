"""
Schema validation rules.

These validations check the structure and data types of the dataset:
- Expected columns presence
- Column data types
- Column order

Author: Daniel Edge
"""

from typing import Iterator, Dict, Any
import pandas as pd
from validation_framework.validations.base import FileValidationRule, ValidationResult
from validation_framework.core.exceptions import (
    ParameterValidationError,
    ValidationExecutionError
)


class SchemaMatchCheck(FileValidationRule):
    """
    Validates that the dataset contains expected columns with correct data types.

    This is typically one of the first validations to run, as it verifies
    the fundamental structure of the data.

    Configuration:
        params:
            expected_schema (dict): Mapping of column names to expected types
                                   Supported types: 'string', 'integer', 'float',
                                   'number' (int or float), 'date', 'boolean', 'any'
            strict (bool): If True, no extra columns allowed (default: False)
            check_order (bool): If True, columns must be in specified order (default: False)

    Example YAML:
        - type: "SchemaMatchCheck"
          severity: "ERROR"
          params:
            expected_schema:
              customer_id: "integer"
              name: "string"
              balance: "float"
              created_date: "date"
            strict: false
            check_order: false
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        schema = self.params.get("expected_schema", {})
        return f"Validates schema matches expected structure ({len(schema)} columns)"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Validate schema matches expectations.

        Args:
            context: Must contain 'columns' and 'dtypes' from file metadata

        Returns:
            ValidationResult indicating schema validity
        """
        try:
            expected_schema = self.params.get("expected_schema", {})
            if not expected_schema:
                raise ParameterValidationError(
                    "No expected schema specified",
                    validation_name=self.name,
                    parameter="expected_schema",
                    value=None
                )

            strict = self.params.get("strict", False)
            check_order = self.params.get("check_order", False)

            # Get actual columns and types from context
            actual_columns = context.get("columns", [])
            actual_dtypes = context.get("dtypes", {})

            if not actual_columns:
                return self._create_result(
                    passed=False,
                    message="Could not determine file columns",
                    failed_count=1,
                )

            issues = []

            # Check for missing columns
            expected_cols = set(expected_schema.keys())
            actual_cols = set(actual_columns)

            missing_cols = expected_cols - actual_cols
            if missing_cols:
                issues.append(f"Missing columns: {', '.join(sorted(missing_cols))}")

            # Check for extra columns (if strict mode)
            if strict:
                extra_cols = actual_cols - expected_cols
                if extra_cols:
                    issues.append(f"Unexpected columns: {', '.join(sorted(extra_cols))}")

            # Check column order (if requested)
            if check_order:
                expected_order = list(expected_schema.keys())
                # Filter to only columns that exist in both
                common_cols = [c for c in expected_order if c in actual_columns]
                actual_order = [c for c in actual_columns if c in expected_cols]

                if common_cols != actual_order:
                    issues.append(f"Column order mismatch. Expected: {common_cols}, Actual: {actual_order}")

            # Check data types for common columns
            type_mismatches = []
            for col in expected_cols.intersection(actual_cols):
                expected_type = expected_schema[col].lower()
                actual_type = str(actual_dtypes.get(col, '')).lower()

                if not self._types_match(expected_type, actual_type):
                    type_mismatches.append(f"{col} (expected: {expected_type}, actual: {actual_type})")

            if type_mismatches:
                issues.append(f"Type mismatches: {', '.join(type_mismatches)}")

            # Create result
            if issues:
                return self._create_result(
                    passed=False,
                    message=f"Schema validation failed: {'; '.join(issues)}",
                    failed_count=len(issues),
                    total_count=len(expected_schema),
                )

            return self._create_result(
                passed=True,
                message=f"Schema matches expected structure ({len(expected_schema)} columns validated)",
                total_count=len(expected_schema),
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during schema validation: {str(e)}",
                failed_count=1,
            )

    def _types_match(self, expected: str, actual: str) -> bool:
        """
        Check if actual type matches expected type.

        Handles pandas type mappings and type aliases.
        """
        # Type mappings
        type_groups = {
            'string': ['object', 'string', 'str'],
            'integer': ['int', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'],
            'float': ['float', 'float16', 'float32', 'float64'],
            'number': ['int', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64',
                      'float', 'float16', 'float32', 'float64'],
            'date': ['datetime', 'datetime64', 'date'],
            'boolean': ['bool', 'boolean'],
            'any': ['*'],  # Matches anything
        }

        # Handle 'any' type
        if expected == 'any':
            return True

        # Get acceptable actual types for the expected type
        acceptable_types = type_groups.get(expected, [expected])

        # Check if actual matches any acceptable type
        for acceptable in acceptable_types:
            if acceptable in actual:
                return True

        return False


class ColumnPresenceCheck(FileValidationRule):
    """
    Validates that specific required columns are present.

    Simpler than SchemaMatchCheck - just verifies column existence.

    Configuration:
        params:
            required_columns (list): List of required column names
            case_sensitive (bool): Whether column names are case-sensitive (default: True)

    Example YAML:
        - type: "ColumnPresenceCheck"
          severity: "ERROR"
          params:
            required_columns: ["id", "name", "email", "created_date"]
            case_sensitive: true
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        cols = self.params.get("required_columns", [])
        return f"Checks presence of required columns: {', '.join(cols)}"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Check if required columns are present.

        Args:
            context: Must contain 'columns' from file metadata

        Returns:
            ValidationResult indicating column presence
        """
        try:
            required_columns = self.params.get("required_columns", [])
            if not required_columns:
                raise ParameterValidationError(
                    "No required columns specified",
                    validation_name=self.name,
                    parameter="required_columns",
                    value=None
                )

            case_sensitive = self.params.get("case_sensitive", True)

            # Get actual columns
            actual_columns = context.get("columns", [])
            if not actual_columns:
                return self._create_result(
                    passed=False,
                    message="Could not determine file columns",
                    failed_count=1,
                )

            # Normalize case if needed
            if not case_sensitive:
                actual_columns = [c.lower() for c in actual_columns]
                required_columns = [c.lower() for c in required_columns]

            # Check for missing columns
            actual_set = set(actual_columns)
            required_set = set(required_columns)

            missing = required_set - actual_set

            if missing:
                return self._create_result(
                    passed=False,
                    message=f"Missing required columns: {', '.join(sorted(missing))}",
                    failed_count=len(missing),
                    total_count=len(required_columns),
                )

            return self._create_result(
                passed=True,
                message=f"All {len(required_columns)} required columns are present",
                total_count=len(required_columns),
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error checking column presence: {str(e)}",
                failed_count=1,
            )
