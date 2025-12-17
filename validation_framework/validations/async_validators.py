"""
Async implementations of common validation rules.

These validators use async/await for non-blocking I/O operations,
particularly beneficial for database lookups, API calls, and concurrent processing.

Author: Daniel Edge
"""

import asyncio
import re
from typing import AsyncIterator, Dict, Any, List, Set, Optional
import pandas as pd
import logging

from validation_framework.validations.async_base import (
    AsyncFileValidationRule,
    AsyncDataValidationRule
)
from validation_framework.core.results import ValidationResult
from validation_framework.core.exceptions import (
    ColumnNotFoundError,
    ParameterValidationError,
    ValidationExecutionError
)
from validation_framework.core.constants import MAX_SAMPLE_FAILURES

logger = logging.getLogger(__name__)


class AsyncSchemaMatchCheck(AsyncFileValidationRule):
    """
    Async schema validation.

    Validates that the dataset contains expected columns with correct data types.
    Runs file metadata checks asynchronously without blocking.

    Configuration:
        params:
            expected_schema (dict): Mapping of column names to expected types
            strict (bool): If True, no extra columns allowed (default: False)
            check_order (bool): If True, columns must be in specified order (default: False)

    Example YAML:
        - type: "AsyncSchemaMatchCheck"
          severity: "ERROR"
          params:
            expected_schema:
              customer_id: "integer"
              name: "string"
              balance: "float"
            strict: false
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        schema = self.params.get("expected_schema", {})
        return f"Async schema validation ({len(schema)} columns)"

    async def validate_file_async(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Validate schema matches expectations asynchronously.

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

            # Run schema validation in executor to avoid blocking
            loop = asyncio.get_event_loop()
            issues = await loop.run_in_executor(
                None,
                self._validate_schema,
                expected_schema,
                actual_columns,
                actual_dtypes,
                strict,
                check_order
            )

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
                message=f"Schema matches expected structure ({len(expected_schema)} columns)",
                total_count=len(expected_schema),
            )

        except ParameterValidationError:
            raise
        except (KeyError, AttributeError, TypeError, ValueError) as e:
            logger.debug(f"Schema validation error: {e}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error during schema validation: {str(e)}",
                failed_count=1,
            )
        except Exception as e:
            logger.error(f"Unexpected error in schema validation: {e}", exc_info=True)
            raise ValidationExecutionError(
                f"Unexpected error during schema validation: {str(e)}",
                validation_name=self.name
            )

    def _validate_schema(
        self,
        expected_schema: Dict[str, str],
        actual_columns: List[str],
        actual_dtypes: Dict[str, str],
        strict: bool,
        check_order: bool
    ) -> List[str]:
        """
        Validate schema (runs in executor).

        Args:
            expected_schema: Expected column types
            actual_columns: Actual column names
            actual_dtypes: Actual column types
            strict: Check for extra columns
            check_order: Check column order

        Returns:
            List of issue descriptions
        """
        issues = []

        expected_cols = set(expected_schema.keys())
        actual_cols = set(actual_columns)

        # Check for missing columns
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
            common_cols = [c for c in expected_order if c in actual_columns]
            actual_order = [c for c in actual_columns if c in expected_cols]

            if common_cols != actual_order:
                issues.append(f"Column order mismatch")

        # Check data types
        type_mismatches = []
        for col in expected_cols.intersection(actual_cols):
            expected_type = expected_schema[col].lower()
            actual_type = str(actual_dtypes.get(col, '')).lower()

            if not self._types_match(expected_type, actual_type):
                type_mismatches.append(f"{col} (expected: {expected_type}, actual: {actual_type})")

        if type_mismatches:
            issues.append(f"Type mismatches: {', '.join(type_mismatches)}")

        return issues

    def _types_match(self, expected: str, actual: str) -> bool:
        """Check if actual type matches expected type."""
        type_groups = {
            'string': ['object', 'string', 'str'],
            'integer': ['int', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'],
            'float': ['float', 'float16', 'float32', 'float64'],
            'number': ['int', 'int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64',
                      'float', 'float16', 'float32', 'float64'],
            'date': ['datetime', 'datetime64', 'date'],
            'boolean': ['bool', 'boolean'],
            'any': ['*'],
        }

        if expected == 'any':
            return True

        acceptable_types = type_groups.get(expected, [expected])

        for acceptable in acceptable_types:
            if acceptable in actual:
                return True

        return False


class AsyncLookupCheck(AsyncDataValidationRule):
    """
    Async lookup validation against reference data.

    Validates field values against a reference list or database table.
    Supports both in-memory lookups and async database queries.

    Configuration:
        params:
            field (str): Field to validate
            reference_values (list, optional): In-memory reference list
            reference_table (str, optional): Database table name
            reference_column (str, optional): Database column to check against
            connection_string (str, optional): Database connection string
            check_type (str): 'allow' or 'deny' (default: 'allow')
            description (str): Validation description

    Example YAML (in-memory):
        - type: "AsyncLookupCheck"
          severity: "ERROR"
          params:
            field: "country_code"
            check_type: "allow"
            reference_values: ["US", "UK", "CA"]
            description: "Valid country codes"

    Example YAML (database):
        - type: "AsyncLookupCheck"
          severity: "ERROR"
          params:
            field: "customer_id"
            reference_table: "customers"
            reference_column: "id"
            connection_string: "postgresql://user:pass@localhost/db"
            description: "Customer ID must exist"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        return self.params.get("description", "Async reference data lookup")

    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate against reference data asynchronously.

        Args:
            data_iterator: Async iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with lookup validation results
        """
        try:
            field = self.params.get("field")
            reference_values = self.params.get("reference_values")
            reference_table = self.params.get("reference_table")
            check_type = self.params.get("check_type", "allow").lower()

            if not field:
                raise ParameterValidationError(
                    "Missing required parameter: field",
                    validation_name=self.name,
                    parameter="field",
                    value=None
                )

            # Load reference data
            if reference_values:
                reference_set = await self._load_reference_list(reference_values)
            elif reference_table:
                reference_set = await self._load_reference_from_db(
                    reference_table,
                    self.params.get("reference_column", "id"),
                    self.params.get("connection_string")
                )
            else:
                raise ParameterValidationError(
                    "Must specify either reference_values or reference_table",
                    validation_name=self.name,
                    parameter="reference_values/reference_table",
                    value=None
                )

            # Validate data chunks
            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)
            invalid_values = set()

            async for chunk in data_iterator:
                if field not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=field,
                        available_columns=list(chunk.columns)
                    )

                # Process chunk in executor
                loop = asyncio.get_event_loop()
                chunk_failures = await loop.run_in_executor(
                    None,
                    self._validate_chunk,
                    chunk,
                    field,
                    reference_set,
                    check_type,
                    total_rows,
                    max_samples - len(failed_rows)
                )

                for failure in chunk_failures:
                    invalid_values.add(failure["value"])
                    failed_rows.append(failure)

                total_rows += len(chunk)

            # Create result
            if failed_rows:
                invalid_list = ', '.join(sorted(invalid_values)[:10])
                description = self.params.get("description", "Lookup check")
                return self._create_result(
                    passed=False,
                    message=f"{description} - {len(failed_rows)} values failed. Invalid: {invalid_list}",
                    failed_count=len(failed_rows),
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            description = self.params.get("description", "Lookup check")
            return self._create_result(
                passed=True,
                message=f"{description} - All {total_rows} values passed",
                total_count=total_rows,
            )

        except (ParameterValidationError, ColumnNotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in async lookup check: {e}", exc_info=True)
            raise ValidationExecutionError(
                f"Unexpected error during lookup check: {str(e)}",
                validation_name=self.name
            )

    async def _load_reference_list(self, values: List) -> Set:
        """
        Load reference values from list asynchronously.

        Args:
            values: List of reference values

        Returns:
            Set of reference values
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, set, values)

    async def _load_reference_from_db(
        self,
        table: str,
        column: str,
        connection_string: Optional[str]
    ) -> Set:
        """
        Load reference values from database asynchronously.

        Args:
            table: Table name
            column: Column name
            connection_string: Database connection string

        Returns:
            Set of reference values

        Raises:
            ParameterValidationError: If connection_string is missing
        """
        if not connection_string:
            raise ParameterValidationError(
                "connection_string required for database lookups",
                validation_name=self.name,
                parameter="connection_string",
                value=None
            )

        try:
            from sqlalchemy import create_engine
        except ImportError:
            raise ValidationExecutionError(
                "SQLAlchemy required for database lookups. Install with: pip install sqlalchemy",
                validation_name=self.name
            )

        # Run database query in executor
        loop = asyncio.get_event_loop()

        def _load_from_db():
            engine = create_engine(connection_string)
            try:
                query = f"SELECT DISTINCT {column} FROM {table}"
                df = pd.read_sql_query(query, engine)
                return set(df[column].tolist())
            finally:
                engine.dispose()

        return await loop.run_in_executor(None, _load_from_db)

    def _validate_chunk(
        self,
        chunk: pd.DataFrame,
        field: str,
        reference_set: Set,
        check_type: str,
        offset: int,
        max_samples: int
    ) -> List[Dict[str, Any]]:
        """
        Validate chunk against reference data (runs in executor).

        Args:
            chunk: Data chunk
            field: Field name
            reference_set: Reference value set
            check_type: 'allow' or 'deny'
            offset: Row offset for reporting
            max_samples: Maximum samples to collect

        Returns:
            List of failure records
        """
        failures = []

        for idx, value in chunk[field].dropna().items():
            if len(failures) >= max_samples:
                break

            value_str = str(value)

            if check_type == "allow":
                if value_str not in reference_set:
                    failures.append({
                        "row": int(offset + idx),
                        "field": field,
                        "value": value_str,
                        "message": "Value not in approved list"
                    })
            else:  # deny
                if value_str in reference_set:
                    failures.append({
                        "row": int(offset + idx),
                        "field": field,
                        "value": value_str,
                        "message": "Value is in blocked list"
                    })

        return failures


class AsyncRegexCheck(AsyncDataValidationRule):
    """
    Async regex pattern validation.

    Validates field values against regex patterns with non-blocking execution.
    Pre-compiles regex patterns for performance.

    Configuration:
        params:
            field (str): Field to validate
            pattern (str): Regex pattern to match
            description (str): Validation description
            should_match (bool): True if values SHOULD match, False if NOT (default: True)

    Example YAML:
        - type: "AsyncRegexCheck"
          severity: "ERROR"
          params:
            field: "email"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            description: "Valid email format"
            should_match: true
    """

    def __init__(self, name: str, severity, params: Dict[str, Any] = None, condition: str = None):
        """Initialize with pre-compiled regex pattern."""
        super().__init__(name, severity, params, condition)

        pattern = self.params.get("pattern")
        if pattern:
            try:
                self.compiled_regex = re.compile(pattern)
                self.regex_error = None
            except re.error as e:
                self.compiled_regex = None
                self.regex_error = str(e)
        else:
            self.compiled_regex = None
            self.regex_error = "No pattern specified"

    def get_description(self) -> str:
        """Get human-readable description."""
        custom_desc = self.params.get("description")
        if custom_desc:
            return f"Async {custom_desc}"

        field = self.params.get("field", "unknown")
        pattern = self.params.get("pattern", "")
        return f"Async regex check on '{field}': {pattern}"

    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate field values against regex pattern asynchronously.

        Args:
            data_iterator: Async iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with regex validation results
        """
        try:
            field = self.params.get("field")
            pattern = self.params.get("pattern")
            should_match = self.params.get("should_match", True)
            description = self.params.get("description", "Regex validation")

            if not field or not pattern:
                raise ParameterValidationError(
                    "Missing required parameters: field and pattern",
                    validation_name=self.name,
                    parameter="field/pattern",
                    value=None
                )

            if self.regex_error:
                return self._create_result(
                    passed=False,
                    message=f"Invalid regex pattern: {self.regex_error}",
                    failed_count=1,
                )

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

            async for chunk in data_iterator:
                if field not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=field,
                        available_columns=list(chunk.columns)
                    )

                # Process chunk in executor
                loop = asyncio.get_event_loop()
                chunk_failures = await loop.run_in_executor(
                    None,
                    self._validate_chunk,
                    chunk,
                    field,
                    should_match,
                    description,
                    total_rows,
                    max_samples - len(failed_rows)
                )

                failed_rows.extend(chunk_failures)
                total_rows += len(chunk)

            # Create result
            if failed_rows:
                return self._create_result(
                    passed=False,
                    message=f"{description} - {len(failed_rows)} values failed validation",
                    failed_count=len(failed_rows),
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"{description} - All {total_rows} values passed",
                total_count=total_rows,
            )

        except (ParameterValidationError, ColumnNotFoundError):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in async regex check: {e}", exc_info=True)
            raise ValidationExecutionError(
                f"Unexpected error during regex check: {str(e)}",
                validation_name=self.name
            )

    def _validate_chunk(
        self,
        chunk: pd.DataFrame,
        field: str,
        should_match: bool,
        description: str,
        offset: int,
        max_samples: int
    ) -> List[Dict[str, Any]]:
        """
        Validate chunk against regex pattern (runs in executor).

        Args:
            chunk: Data chunk
            field: Field name
            should_match: Whether values should match pattern
            description: Validation description
            offset: Row offset for reporting
            max_samples: Maximum samples to collect

        Returns:
            List of failure records
        """
        failures = []

        for idx, value in chunk[field].dropna().items():
            if len(failures) >= max_samples:
                break

            matches = bool(self.compiled_regex.search(str(value)))
            failed = (matches and not should_match) or (not matches and should_match)

            if failed:
                if should_match:
                    msg = f"{description} - Value does not match expected pattern"
                else:
                    msg = f"{description} - Value should NOT contain this pattern"

                failures.append({
                    "row": int(offset + idx),
                    "field": field,
                    "value": str(value),
                    "message": msg
                })

        return failures
