"""
Field-level validation rules.

These validations check individual field (column) values:
- Mandatory field checks (null/empty detection)
- Regular expression pattern matching
- Valid values (enumeration)
- Range checks (numeric and date)
- Data type validation
- String length constraints
- Date format validation
"""

from typing import Iterator, Dict, Any, List, Set
import pandas as pd
import re
from datetime import datetime
from dateutil import parser as date_parser
from validation_framework.validations.base import DataValidationRule, ValidationResult


class MandatoryFieldCheck(DataValidationRule):
    """
    Validates that specified fields are not null or empty.

    This is one of the most common and critical validations. It checks for:
    - NULL values (pd.NA, None, np.nan)
    - Empty strings ('')
    - Whitespace-only strings ('  ')

    Configuration:
        params:
            fields (list): List of field names to check
            allow_whitespace (bool): If False, whitespace-only values fail (default: False)

    Example YAML:
        - type: "MandatoryFieldCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id", "email", "transaction_date"]
            allow_whitespace: false
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        fields = self.params.get("fields", [])
        return f"Checks that required fields are not empty: {', '.join(fields)}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check for missing values in mandatory fields across all chunks.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of any missing values found
        """
        try:
            fields = self.params.get("fields", [])
            if not fields:
                return self._create_result(
                    passed=False,
                    message="No fields specified for mandatory check",
                    failed_count=1,
                )

            allow_whitespace = self.params.get("allow_whitespace", False)

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", 100)

            # Process each chunk
            for chunk_idx, chunk in enumerate(data_iterator):
                # Verify fields exist
                missing_fields = [f for f in fields if f not in chunk.columns]
                if missing_fields:
                    return self._create_result(
                        passed=False,
                        message=f"Fields not found in data: {', '.join(missing_fields)}",
                        failed_count=1,
                    )

                # Apply conditional filter if condition is specified
                if self.condition:
                    condition_mask = self._evaluate_condition(chunk)
                    # Only validate rows that match the condition
                    rows_to_check = chunk[condition_mask]

                    # If no rows match condition in this chunk, skip validation
                    if len(rows_to_check) == 0:
                        total_rows += len(chunk)
                        continue
                else:
                    rows_to_check = chunk

                # Check each required field
                for field in fields:
                    # Find rows with missing values (check only rows that meet condition)
                    mask = rows_to_check[field].isna()

                    # Also check for empty strings if not allowing whitespace
                    if not allow_whitespace and rows_to_check[field].dtype == 'object':
                        # Convert to string and check for empty/whitespace
                        mask = mask | (rows_to_check[field].astype(str).str.strip() == '')

                    # Find failed row indices
                    failed_indices = rows_to_check[mask].index.tolist()

                    # Collect samples
                    for idx in failed_indices:
                        if len(failed_rows) < max_samples:
                            failed_rows.append({
                                "row": int(total_rows + idx),
                                "field": field,
                                "value": str(chunk.loc[idx, field]),
                                "message": f"Missing or empty value in mandatory field '{field}'"
                            })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} rows with missing mandatory field values",
                    failed_count=failed_count,
                    total_count=total_rows * len(fields),  # Total checks performed
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All mandatory fields contain values across {total_rows} rows",
                total_count=total_rows * len(fields),
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during mandatory field check: {str(e)}",
                failed_count=1,
            )


class RegexCheck(DataValidationRule):
    """
    Validates field values against a regular expression pattern.

    This is a powerful validation for format checking:
    - Email addresses
    - Phone numbers
    - Account numbers
    - Postal codes
    - Custom patterns

    Configuration:
        params:
            field (str): Field name to validate
            pattern (str): Regular expression pattern
            message (str, optional): Custom error message
            invert (bool): If True, values should NOT match pattern (default: False)

    Example YAML:
        # Email validation
        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "email"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            message: "Invalid email format"

        # Account number validation (exactly 8 digits)
        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "account_number"
            pattern: "^\\d{8}$"
            message: "Account number must be exactly 8 digits"
    """

    def __init__(self, name: str, severity, params: Dict[str, Any] = None, condition: str = None):
        """
        Initialize RegexCheck with pre-compiled regex pattern for performance.

        Args:
            name: Validation rule name
            severity: Severity level (ERROR or WARNING)
            params: Parameters including 'pattern' to compile
            condition: Optional conditional expression
        """
        super().__init__(name, severity, params, condition)

        # Pre-compile regex pattern for performance (avoids recompilation on each chunk)
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
        field = self.params.get("field", "unknown")
        pattern = self.params.get("pattern", "")
        return f"Validates '{field}' against pattern: {pattern}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Validate field values against regex pattern across all chunks.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of non-matching values
        """
        try:
            field = self.params.get("field")
            if not field:
                return self._create_result(
                    passed=False,
                    message="No field specified for regex check",
                    failed_count=1,
                )

            pattern = self.params.get("pattern")
            if not pattern:
                return self._create_result(
                    passed=False,
                    message="No pattern specified for regex check",
                    failed_count=1,
                )

            custom_message = self.params.get("message", f"Value does not match pattern: {pattern}")
            invert = self.params.get("invert", False)

            # Use pre-compiled regex pattern (compiled in __init__ for performance)
            if self.regex_error:
                return self._create_result(
                    passed=False,
                    message=f"Invalid regex pattern: {self.regex_error}",
                    failed_count=1,
                )

            regex = self.compiled_regex

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", 100)

            # Process each chunk
            for chunk_idx, chunk in enumerate(data_iterator):
                # Verify field exists
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field not found in data: {field}",
                        failed_count=1,
                    )

                # Apply conditional filter if condition is specified
                if self.condition:
                    condition_mask = self._evaluate_condition(chunk)
                    rows_to_check = chunk[condition_mask]

                    # If no rows match condition in this chunk, skip validation
                    if len(rows_to_check) == 0:
                        total_rows += len(chunk)
                        continue
                else:
                    rows_to_check = chunk

                # Convert to string for regex matching (skip nulls)
                field_values = rows_to_check[field].dropna().astype(str)

                # Test each value against pattern
                for idx, value in field_values.items():
                    matches = bool(regex.match(value))

                    # Check if validation fails (considering invert flag)
                    failed = (matches and invert) or (not matches and not invert)

                    if failed and len(failed_rows) < max_samples:
                        failed_rows.append({
                            "row": int(total_rows + idx),
                            "field": field,
                            "value": value,
                            "message": custom_message
                        })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} values that do not match pattern",
                    failed_count=failed_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_rows} values match the expected pattern",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during regex check: {str(e)}",
                failed_count=1,
            )


class ValidValuesCheck(DataValidationRule):
    """
    Validates that field values are within a specified set of valid values.

    Useful for enumerated fields like status codes, categories, etc.

    Configuration:
        params:
            field (str): Field name to validate
            valid_values (list): List of acceptable values
            case_sensitive (bool): Whether comparison is case-sensitive (default: True)

    Example YAML:
        - type: "ValidValuesCheck"
          severity: "ERROR"
          params:
            field: "status"
            valid_values: ["ACTIVE", "INACTIVE", "PENDING"]
            case_sensitive: true
    """

    def __init__(self, name: str, severity, params: Dict[str, Any] = None, condition: str = None):
        """
        Initialize ValidValuesCheck with pre-computed valid set for performance.

        Args:
            name: Validation rule name
            severity: Severity level (ERROR or WARNING)
            params: Parameters including 'valid_values' and 'case_sensitive'
            condition: Optional conditional expression
        """
        super().__init__(name, severity, params, condition)

        # Pre-compute valid set for efficient lookup (avoids recreation on each chunk)
        valid_values = self.params.get("valid_values", [])
        case_sensitive = self.params.get("case_sensitive", True)

        if case_sensitive:
            self.valid_set = set(valid_values)
        else:
            self.valid_set = {str(v).lower() for v in valid_values}

        self.case_sensitive = case_sensitive

    def get_description(self) -> str:
        """Get human-readable description."""
        field = self.params.get("field", "unknown")
        valid_values = self.params.get("valid_values", [])
        return f"Checks '{field}' contains only valid values: {', '.join(map(str, valid_values))}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Validate field values are in the allowed set.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of invalid values
        """
        try:
            field = self.params.get("field")
            if not field:
                return self._create_result(
                    passed=False,
                    message="No field specified for valid values check",
                    failed_count=1,
                )

            valid_values = self.params.get("valid_values", [])
            if not valid_values:
                return self._create_result(
                    passed=False,
                    message="No valid values specified",
                    failed_count=1,
                )

            # Use pre-computed valid set (computed in __init__ for performance)
            valid_set = self.valid_set
            case_sensitive = self.case_sensitive

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", 100)
            invalid_values_found: Set[str] = set()

            # Process each chunk
            for chunk_idx, chunk in enumerate(data_iterator):
                # Verify field exists
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field not found in data: {field}",
                        failed_count=1,
                    )

                # Apply conditional filter if condition is specified
                if self.condition:
                    condition_mask = self._evaluate_condition(chunk)
                    rows_to_check = chunk[condition_mask]

                    # If no rows match condition in this chunk, skip validation
                    if len(rows_to_check) == 0:
                        total_rows += len(chunk)
                        continue
                else:
                    rows_to_check = chunk

                # Check each value (skip nulls)
                field_values = rows_to_check[field].dropna()

                for idx, value in field_values.items():
                    check_value = str(value) if case_sensitive else str(value).lower()

                    if check_value not in valid_set:
                        invalid_values_found.add(str(value))

                        if len(failed_rows) < max_samples:
                            failed_rows.append({
                                "row": int(total_rows + idx),
                                "field": field,
                                "value": str(value),
                                "message": f"Invalid value '{value}'. Expected one of: {', '.join(map(str, valid_values))}"
                            })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} invalid values. Unique invalid values: {', '.join(sorted(invalid_values_found)[:10])}",
                    failed_count=failed_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_rows} values are valid",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during valid values check: {str(e)}",
                failed_count=1,
            )


class RangeCheck(DataValidationRule):
    """
    Validates that numeric field values fall within a specified range.

    Configuration:
        params:
            field (str): Field name to validate
            min_value (float, optional): Minimum acceptable value (inclusive)
            max_value (float, optional): Maximum acceptable value (inclusive)

    Example YAML:
        - type: "RangeCheck"
          severity: "WARNING"
          params:
            field: "transaction_amount"
            min_value: 0
            max_value: 1000000
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        field = self.params.get("field", "unknown")
        min_val = self.params.get("min_value")
        max_val = self.params.get("max_value")

        if min_val is not None and max_val is not None:
            return f"Checks '{field}' is between {min_val} and {max_val}"
        elif min_val is not None:
            return f"Checks '{field}' is at least {min_val}"
        elif max_val is not None:
            return f"Checks '{field}' is at most {max_val}"
        else:
            return f"Checks '{field}' range (no limits specified)"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Validate numeric values are within range.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of out-of-range values
        """
        try:
            field = self.params.get("field")
            if not field:
                return self._create_result(
                    passed=False,
                    message="No field specified for range check",
                    failed_count=1,
                )

            min_value = self.params.get("min_value")
            max_value = self.params.get("max_value")

            if min_value is None and max_value is None:
                return self._create_result(
                    passed=False,
                    message="No range limits specified (need min_value or max_value)",
                    failed_count=1,
                )

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", 100)

            # Process each chunk
            for chunk_idx, chunk in enumerate(data_iterator):
                # Verify field exists
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field not found in data: {field}",
                        failed_count=1,
                    )

                # Apply conditional filter if condition is specified
                if self.condition:
                    condition_mask = self._evaluate_condition(chunk)
                    rows_to_check = chunk[condition_mask]

                    # If no rows match condition in this chunk, skip validation
                    if len(rows_to_check) == 0:
                        total_rows += len(chunk)
                        continue
                else:
                    rows_to_check = chunk

                # Convert to numeric if needed
                try:
                    field_values = pd.to_numeric(rows_to_check[field], errors='coerce')
                except Exception:
                    return self._create_result(
                        passed=False,
                        message=f"Field '{field}' cannot be converted to numeric",
                        failed_count=1,
                    )

                # Check range violations (skip nulls)
                for idx, value in field_values.dropna().items():
                    out_of_range = False
                    message = ""

                    if min_value is not None and value < min_value:
                        out_of_range = True
                        message = f"Value {value} is below minimum {min_value}"
                    elif max_value is not None and value > max_value:
                        out_of_range = True
                        message = f"Value {value} exceeds maximum {max_value}"

                    if out_of_range and len(failed_rows) < max_samples:
                        failed_rows.append({
                            "row": int(total_rows + idx),
                            "field": field,
                            "value": float(value),
                            "message": message
                        })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} values outside acceptable range",
                    failed_count=failed_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_rows} values are within acceptable range",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during range check: {str(e)}",
                failed_count=1,
            )


class DateFormatCheck(DataValidationRule):
    """
    Validates that date fields conform to a specified format.

    Configuration:
        params:
            field (str): Field name to validate
            format (str): Expected date format (strftime format)
                         Examples: "%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"
            allow_null (bool): Whether null values are acceptable (default: True)

    Example YAML:
        - type: "DateFormatCheck"
          severity: "ERROR"
          params:
            field: "transaction_date"
            format: "%Y-%m-%d"
            allow_null: false
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        field = self.params.get("field", "unknown")
        format_str = self.params.get("format", "unknown")
        return f"Checks '{field}' matches date format: {format_str}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Validate date field format.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of invalid dates
        """
        try:
            field = self.params.get("field")
            if not field:
                return self._create_result(
                    passed=False,
                    message="No field specified for date format check",
                    failed_count=1,
                )

            date_format = self.params.get("format")
            if not date_format:
                return self._create_result(
                    passed=False,
                    message="No date format specified",
                    failed_count=1,
                )

            allow_null = self.params.get("allow_null", True)

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", 100)

            # Process each chunk
            for chunk_idx, chunk in enumerate(data_iterator):
                # Verify field exists
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field not found in data: {field}",
                        failed_count=1,
                    )

                # Apply conditional filter if condition is specified
                if self.condition:
                    condition_mask = self._evaluate_condition(chunk)
                    rows_to_check = chunk[condition_mask]

                    # If no rows match condition in this chunk, skip validation
                    if len(rows_to_check) == 0:
                        total_rows += len(chunk)
                        continue
                else:
                    rows_to_check = chunk

                # Check each value
                for idx, value in rows_to_check[field].items():
                    # Handle nulls
                    if pd.isna(value):
                        if not allow_null and len(failed_rows) < max_samples:
                            failed_rows.append({
                                "row": int(total_rows + idx),
                                "field": field,
                                "value": str(value),
                                "message": "Null value not allowed"
                            })
                        continue

                    # Try to parse date with specified format
                    try:
                        datetime.strptime(str(value), date_format)
                    except (ValueError, TypeError) as e:
                        if len(failed_rows) < max_samples:
                            failed_rows.append({
                                "row": int(total_rows + idx),
                                "field": field,
                                "value": str(value),
                                "message": f"Invalid date format. Expected: {date_format}"
                            })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} dates with invalid format",
                    failed_count=failed_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_rows} dates match expected format {date_format}",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during date format check: {str(e)}",
                failed_count=1,
            )
