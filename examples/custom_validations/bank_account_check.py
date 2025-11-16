"""
Example custom validation: Bank Account Number Check

This demonstrates how to create custom validations for business-specific rules.
Custom validations can be reused across different projects.

Author: daniel edge
"""

from typing import Iterator, Dict, Any
import pandas as pd
from validation_framework.validations.base import DataValidationRule, ValidationResult


class BankAccountCheck(DataValidationRule):
    """
    Validates bank account numbers conform to expected format.

    This example validates that account numbers are exactly 8 digits.
    Customize the validation logic for your specific requirements.

    Configuration:
        params:
            field (str): Field name containing account numbers
            length (int, optional): Expected length (default: 8)
            allow_leading_zeros (bool, optional): Allow leading zeros (default: True)

    Example YAML:
        - type: "BankAccountCheck"
          severity: "ERROR"
          params:
            field: "account_number"
            length: 8
            allow_leading_zeros: true
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        field = self.params.get("field", "unknown")
        length = self.params.get("length", 8)
        return f"Validates '{field}' as {length}-digit bank account number"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Validate bank account numbers across all chunks.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of invalid account numbers
        """
        try:
            field = self.params.get("field")
            if not field:
                return self._create_result(
                    passed=False,
                    message="No field specified for bank account check",
                    failed_count=1,
                )

            expected_length = self.params.get("length", 8)
            allow_leading_zeros = self.params.get("allow_leading_zeros", True)

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

                # Check each account number (skip nulls)
                field_values = chunk[field].dropna()

                for idx, value in field_values.items():
                    account_str = str(value).strip()

                    # Validate account number
                    is_valid, error_message = self._validate_account_number(
                        account_str,
                        expected_length,
                        allow_leading_zeros
                    )

                    if not is_valid and len(failed_rows) < max_samples:
                        failed_rows.append({
                            "row": int(total_rows + idx),
                            "field": field,
                            "value": account_str,
                            "message": error_message
                        })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} invalid bank account numbers",
                    failed_count=failed_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_rows} bank account numbers are valid",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during bank account check: {str(e)}",
                failed_count=1,
            )

    def _validate_account_number(
        self,
        account: str,
        expected_length: int,
        allow_leading_zeros: bool
    ) -> tuple:
        """
        Validate a single account number.

        Args:
            account: Account number string
            expected_length: Expected length
            allow_leading_zeros: Whether leading zeros are allowed

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check if it's all digits
        if not account.isdigit():
            return False, "Account number must contain only digits"

        # Check length
        if len(account) != expected_length:
            return False, f"Account number must be exactly {expected_length} digits (found {len(account)})"

        # Check leading zeros if not allowed
        if not allow_leading_zeros and account[0] == '0':
            return False, "Account number cannot start with zero"

        return True, ""


# Additional custom validation example: IBAN Check
class IBANCheck(DataValidationRule):
    """
    Validates International Bank Account Numbers (IBAN).

    This is a more complex example showing validation with
    country-specific rules and check digit verification.

    Configuration:
        params:
            field (str): Field name containing IBAN
            countries (list, optional): List of allowed country codes

    Example YAML:
        - type: "IBANCheck"
          severity: "ERROR"
          params:
            field: "iban"
            countries: ["GB", "DE", "FR"]
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        field = self.params.get("field", "unknown")
        return f"Validates '{field}' as valid IBAN"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """Validate IBAN format and check digit."""
        try:
            field = self.params.get("field")
            if not field:
                return self._create_result(
                    passed=False,
                    message="No field specified for IBAN check",
                    failed_count=1,
                )

            allowed_countries = self.params.get("countries", [])

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", 100)

            # Process each chunk
            for chunk in data_iterator:
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field not found in data: {field}",
                        failed_count=1,
                    )

                field_values = chunk[field].dropna()

                for idx, value in field_values.items():
                    iban = str(value).replace(" ", "").upper()

                    # Validate IBAN
                    is_valid, error_message = self._validate_iban(iban, allowed_countries)

                    if not is_valid and len(failed_rows) < max_samples:
                        failed_rows.append({
                            "row": int(total_rows + idx),
                            "field": field,
                            "value": str(value),
                            "message": error_message
                        })

                total_rows += len(chunk)

            # Create result
            failed_count = len(failed_rows)

            if failed_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {failed_count} invalid IBANs",
                    failed_count=failed_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_rows} IBANs are valid",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during IBAN check: {str(e)}",
                failed_count=1,
            )

    def _validate_iban(self, iban: str, allowed_countries: list) -> tuple:
        """
        Validate IBAN format and check digit.

        This is a simplified validation. For production use,
        implement full IBAN validation with country-specific rules.

        Args:
            iban: IBAN string
            allowed_countries: List of allowed country codes

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Basic format check: 2 letters + 2 digits + up to 30 alphanumeric
        if len(iban) < 15 or len(iban) > 34:
            return False, "IBAN length must be between 15 and 34 characters"

        if not iban[:2].isalpha():
            return False, "IBAN must start with 2-letter country code"

        if not iban[2:4].isdigit():
            return False, "IBAN check digits (positions 3-4) must be numeric"

        country_code = iban[:2]

        # Check allowed countries
        if allowed_countries and country_code not in allowed_countries:
            return False, f"Country code '{country_code}' not in allowed list: {allowed_countries}"

        # Note: Full IBAN check digit validation (mod-97) would go here
        # For brevity, this example only does format checks

        return True, ""


# To use these custom validations, register them in your code:
#
# from validation_framework.core.registry import register_validation
# from examples.custom_validations.bank_account_check import BankAccountCheck, IBANCheck
#
# register_validation("BankAccountCheck", BankAccountCheck)
# register_validation("IBANCheck", IBANCheck)
