"""
Inline custom validation checks - Bespoke validations defined in YAML.

These validations allow users to define custom checks without
writing Python code. All configuration is done in the YAML file.

Author: daniel edge
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
import re
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.exceptions import (
    ColumnNotFoundError,
    ParameterValidationError
)
from validation_framework.core.constants import MAX_SAMPLE_FAILURES


class InlineRegexCheck(DataValidationRule):
    """
    Define custom regex validation directly in YAML config.

    Perfect for defining custom patterns without coding.

    Configuration:
        params:
            field (str): Field to validate
            pattern (str): Regex pattern to match
            description (str): Human-readable description of what you're checking
            should_match (bool): True if values SHOULD match pattern, False if they should NOT (default: True)

    Example YAML - Validate UK Postcode:
        - type: "InlineRegexCheck"
          severity: "ERROR"
          params:
            field: "postcode"
            pattern: "^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"
            description: "UK postcode format (e.g., SW1A 1AA)"

    Example YAML - Validate Tax ID:
        - type: "InlineRegexCheck"
          severity: "ERROR"
          params:
            field: "tax_id"
            pattern: "^\\d{3}-\\d{2}-\\d{4}$"
            description: "US SSN format (###-##-####)"

    Example YAML - Check no special characters:
        - type: "InlineRegexCheck"
          severity: "WARNING"
          params:
            field: "customer_name"
            pattern: "[^a-zA-Z0-9\\s]"
            description: "Names should not contain special characters"
            should_match: false
    """

    def __init__(self, name: str, severity, params: Dict[str, Any] = None, condition: str = None):
        """
        Initialize InlineRegexCheck with pre-compiled regex pattern for performance.

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
        custom_desc = self.params.get("description")
        if custom_desc:
            return custom_desc

        field = self.params.get("field", "unknown")
        pattern = self.params.get("pattern", "")
        return f"Custom regex check on '{field}': {pattern}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """Validate field values against custom regex pattern."""
        try:
            field = self.params.get("field")
            pattern = self.params.get("pattern")
            should_match = self.params.get("should_match", True)
            description = self.params.get("description", "Custom validation")

            if not field or not pattern:
                raise ParameterValidationError(
                    "Missing required parameters: field and pattern",
                    validation_name=self.name,
                    parameter="field/pattern",
                    value=None
                )

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
            max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

            # Process chunks
            for chunk in data_iterator:
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field '{field}' not found",
                        failed_count=1,
                    )

                # Check each value
                for idx, value in chunk[field].dropna().items():
                    matches = bool(regex.search(str(value)))
                    failed = (matches and not should_match) or (not matches and should_match)

                    if failed and len(failed_rows) < max_samples:
                        if should_match:
                            msg = f"{description} - Value does not match expected pattern"
                        else:
                            msg = f"{description} - Value should NOT contain this pattern"

                        failed_rows.append({
                            "row": int(total_rows + idx),
                            "field": field,
                            "value": str(value),
                            "message": msg
                        })

                total_rows += len(chunk)

            # Create result
            if failed_rows:
                return self._create_result(
                    passed=False,
                    message=f"{description} - Found {len(failed_rows)} values that failed validation",
                    failed_count=len(failed_rows),
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"{description} - All {total_rows} values passed validation",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during validation: {str(e)}",
                failed_count=1,
            )


class InlineBusinessRuleCheck(DataValidationRule):
    """
    Define custom business rules using simple expressions in YAML.

    Users can define rules using SQL-like WHERE clause syntax.

    Configuration:
        params:
            rule (str): Business rule expression (SQL-like WHERE clause)
            description (str): What the rule checks
            error_message (str): Message to show when rule fails

    Example YAML - Age must be 18 or older:
        - type: "InlineBusinessRuleCheck"
          severity: "ERROR"
          params:
            rule: "age >= 18"
            description: "Customer must be 18 or older"
            error_message: "Age is below minimum requirement of 18"

    Example YAML - Amount within range:
        - type: "InlineBusinessRuleCheck"
          severity: "WARNING"
          params:
            rule: "amount >= 0 AND amount <= 1000000"
            description: "Transaction amount must be positive and reasonable"
            error_message: "Amount is outside acceptable range (0 to 1,000,000)"

    Example YAML - Date is in past:
        - type: "InlineBusinessRuleCheck"
          severity: "ERROR"
          params:
            rule: "transaction_date <= today"
            description: "Transaction date cannot be in the future"
            error_message: "Transaction date is in the future"

    Example YAML - Conditional logic:
        - type: "InlineBusinessRuleCheck"
          severity: "ERROR"
          params:
            rule: "(account_type == 'SAVINGS' AND interest_rate > 0) OR (account_type != 'SAVINGS')"
            description: "Savings accounts must have interest rate"
            error_message: "Savings account has zero interest rate"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        return self.params.get("description", "Custom business rule")

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """Validate data against business rule."""
        try:
            rule = self.params.get("rule")
            description = self.params.get("description", "Custom business rule")
            error_message = self.params.get("error_message", "Business rule violation")

            if not rule:
                return self._create_result(
                    passed=False,
                    message="No rule specified",
                    failed_count=1,
                )

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

            # Process chunks
            for chunk in data_iterator:
                try:
                    # Prepare evaluation context
                    eval_context = self._prepare_eval_context(chunk)

                    # Convert SQL-like syntax to pandas query
                    pandas_query = self._convert_to_pandas_query(rule)

                    # Execute query - rows that PASS the rule
                    try:
                        passing_mask = chunk.eval(pandas_query)
                    except Exception:
                        # Fallback: try as direct eval
                        passing_mask = eval(pandas_query, {"__builtins__": {}}, eval_context)

                    # Find failing rows (NOT passing)
                    failing_indices = chunk[~passing_mask].index.tolist()

                    # Collect samples
                    for idx in failing_indices:
                        if len(failed_rows) < max_samples:
                            row_data = chunk.loc[idx].to_dict()
                            failed_rows.append({
                                "row": int(total_rows + idx),
                                "values": {k: str(v)[:50] for k, v in list(row_data.items())[:5]},  # First 5 columns
                                "message": error_message
                            })

                except Exception as e:
                    return self._create_result(
                        passed=False,
                        message=f"Error evaluating rule: {str(e)}",
                        failed_count=1,
                    )

                total_rows += len(chunk)

            # Create result
            if failed_rows:
                return self._create_result(
                    passed=False,
                    message=f"{description} - {len(failed_rows)} rows failed business rule",
                    failed_count=len(failed_rows),
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"{description} - All {total_rows} rows passed business rule",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during business rule check: {str(e)}",
                failed_count=1,
            )

    def _prepare_eval_context(self, df: pd.DataFrame) -> dict:
        """Prepare evaluation context with today's date etc."""
        from datetime import datetime
        return {
            'today': datetime.now().date(),
            'now': datetime.now(),
        }

    def _convert_to_pandas_query(self, rule: str) -> str:
        """Convert SQL-like syntax to pandas query syntax."""
        # Replace SQL keywords with pandas equivalents
        query = rule
        query = query.replace(" AND ", " & ")
        query = query.replace(" OR ", " | ")
        query = query.replace(" NOT ", " ~ ")
        query = query.replace("==", "==")
        query = query.replace("!=", "!=")
        return query


class InlineLookupCheck(DataValidationRule):
    """
    Validate against a reference data list defined in YAML.

    Users can define allowed/disallowed values directly in config.

    Configuration:
        params:
            field (str): Field to check
            reference_values (list): List of valid values
            check_type (str): 'allow' or 'deny' (default: 'allow')
            description (str): What you're checking

    Example YAML - Allowed countries:
        - type: "InlineLookupCheck"
          severity: "ERROR"
          params:
            field: "country_code"
            check_type: "allow"
            reference_values: ["US", "UK", "CA", "AU", "NZ"]
            description: "Only accept customers from supported countries"

    Example YAML - Blocked domains:
        - type: "InlineLookupCheck"
          severity: "WARNING"
          params:
            field: "email_domain"
            check_type: "deny"
            reference_values: ["tempmail.com", "throwaway.email", "guerrillamail.com"]
            description: "Block temporary email providers"

    Example YAML - Valid product codes:
        - type: "InlineLookupCheck"
          severity: "ERROR"
          params:
            field: "product_code"
            check_type: "allow"
            reference_values: ["PROD-001", "PROD-002", "PROD-003", "PROD-004"]
            description: "Product code must be in approved list"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        return self.params.get("description", "Reference data lookup")

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """Validate against reference list."""
        try:
            field = self.params.get("field")
            reference_values = self.params.get("reference_values", [])
            check_type = self.params.get("check_type", "allow").lower()
            description = self.params.get("description", "Reference data check")

            if not field or not reference_values:
                raise ParameterValidationError(
                    "Missing required parameters: field and reference_values",
                    validation_name=self.name,
                    parameter="field/reference_values",
                    value=None
                )

            # Convert to set for efficient lookup
            reference_set = set(reference_values)

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)
            invalid_values = set()

            # Process chunks
            for chunk in data_iterator:
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field '{field}' not found",
                        failed_count=1,
                    )

                # Check each value
                for idx, value in chunk[field].dropna().items():
                    value_str = str(value)

                    if check_type == "allow":
                        # Value must be IN the reference list
                        if value_str not in reference_set:
                            invalid_values.add(value_str)
                            if len(failed_rows) < max_samples:
                                failed_rows.append({
                                    "row": int(total_rows + idx),
                                    "field": field,
                                    "value": value_str,
                                    "message": f"Value not in approved list. Allowed: {', '.join(list(reference_values)[:5])}"
                                })
                    else:  # deny
                        # Value must NOT be IN the reference list
                        if value_str in reference_set:
                            invalid_values.add(value_str)
                            if len(failed_rows) < max_samples:
                                failed_rows.append({
                                    "row": int(total_rows + idx),
                                    "field": field,
                                    "value": value_str,
                                    "message": f"Value is in blocked list"
                                })

                total_rows += len(chunk)

            # Create result
            if failed_rows:
                invalid_list = ', '.join(sorted(invalid_values)[:10])
                return self._create_result(
                    passed=False,
                    message=f"{description} - {len(failed_rows)} values failed. Invalid values: {invalid_list}",
                    failed_count=len(failed_rows),
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"{description} - All {total_rows} values passed lookup check",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during lookup check: {str(e)}",
                failed_count=1,
            )
