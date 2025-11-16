"""
Tests for inline/bespoke validation checks.

Tests InlineRegexCheck, InlineBusinessRuleCheck, and InlineLookupCheck
which allow users to define custom validations in YAML without writing code.
"""

import pytest
import pandas as pd
from validation_framework.validations.builtin.inline_checks import (
    InlineRegexCheck,
    InlineBusinessRuleCheck,
    InlineLookupCheck
)
from validation_framework.core.results import Severity
from tests.conftest import create_data_iterator


@pytest.mark.unit
class TestInlineRegexCheck:
    """Tests for InlineRegexCheck - custom regex patterns in YAML."""

    def test_inline_regex_pattern_match(self, sample_dataframe):
        """Test that inline regex correctly matches valid patterns."""
        validation = InlineRegexCheck(
            name="EmailPatternCheck",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "description": "Valid email format",
                "should_match": True
            }
        )

        data_iterator = create_data_iterator(sample_dataframe)
        context = {"max_sample_failures": 100}
        result = validation.validate(data_iterator, context)

        # Should fail because "invalid" is not a valid email
        assert result.passed is False
        assert result.failed_count > 0
        assert "invalid" in str(result.sample_failures)

    def test_inline_regex_uk_postcode(self):
        """Test UK postcode validation with inline regex."""
        df = pd.DataFrame({
            "postcode": ["SW1A 1AA", "M1 1AE", "INVALID", "B33 8TH", "SW1A1AA"]
        })

        validation = InlineRegexCheck(
            name="UKPostcode",
            severity=Severity.ERROR,
            params={
                "field": "postcode",
                "pattern": r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$",
                "description": "UK postcode format"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for "INVALID" and "SW1A1AA" (no space)
        assert result.passed is False
        assert result.failed_count == 2

    def test_inline_regex_should_not_match(self):
        """Test regex with should_match=False (value should NOT match pattern)."""
        df = pd.DataFrame({
            "customer_name": ["Alice Smith", "Bob#123", "Charlie!", "David Jones"]
        })

        validation = InlineRegexCheck(
            name="NoSpecialChars",
            severity=Severity.WARNING,
            params={
                "field": "customer_name",
                "pattern": r"[^a-zA-Z\s]",  # Special characters
                "description": "Names should not contain special characters",
                "should_match": False  # Should NOT match pattern
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for "Bob#123" and "Charlie!" (they contain special chars)
        assert result.passed is False
        assert result.failed_count == 2
        assert any("Bob#123" in str(f) for f in result.sample_failures)

    def test_inline_regex_us_ssn_format(self):
        """Test US SSN format validation."""
        df = pd.DataFrame({
            "tax_id": ["123-45-6789", "987-65-4321", "12345678", "123-45-67890"]
        })

        validation = InlineRegexCheck(
            name="SSNFormat",
            severity=Severity.ERROR,
            params={
                "field": "tax_id",
                "pattern": r"^\d{3}-\d{2}-\d{4}$",
                "description": "US SSN format (###-##-####)"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for "12345678" (no dashes) and "123-45-67890" (too many digits)
        assert result.passed is False
        assert result.failed_count == 2

    def test_inline_regex_invalid_pattern(self):
        """Test that invalid regex pattern is handled gracefully."""
        validation = InlineRegexCheck(
            name="InvalidPattern",
            severity=Severity.ERROR,
            params={
                "field": "test",
                "pattern": "[invalid(regex",  # Invalid regex
                "description": "This will fail"
            }
        )

        df = pd.DataFrame({"test": ["value"]})
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "Invalid regex pattern" in result.message

    def test_inline_regex_missing_field(self):
        """Test inline regex with non-existent field."""
        df = pd.DataFrame({"existing_field": [1, 2, 3]})

        validation = InlineRegexCheck(
            name="MissingField",
            severity=Severity.ERROR,
            params={
                "field": "nonexistent_field",
                "pattern": r".*",
                "description": "Field doesn't exist"
            }
        )

        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "not found" in result.message.lower()


@pytest.mark.unit
class TestInlineBusinessRuleCheck:
    """Tests for InlineBusinessRuleCheck - custom business rules in YAML."""

    def test_business_rule_age_minimum(self):
        """Test simple business rule: age >= 18."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3, 4, 5],
            "age": [25, 17, 30, 16, 45]
        })

        validation = InlineBusinessRuleCheck(
            name="AgeMinimum",
            severity=Severity.ERROR,
            params={
                "rule": "age >= 18",
                "description": "Customer must be 18 or older",
                "error_message": "Age is below minimum requirement of 18"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for ages 17 and 16
        assert result.passed is False
        assert result.failed_count == 2

    def test_business_rule_amount_range(self):
        """Test business rule with AND logic: amount in range."""
        df = pd.DataFrame({
            "transaction_id": [1, 2, 3, 4],
            "amount": [500, -100, 2000000, 750]
        })

        validation = InlineBusinessRuleCheck(
            name="AmountRange",
            severity=Severity.WARNING,
            params={
                "rule": "amount >= 0 & amount <= 1000000",
                "description": "Transaction amount must be positive and reasonable",
                "error_message": "Amount is outside acceptable range"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for -100 and 2000000
        assert result.passed is False
        assert result.failed_count == 2

    def test_business_rule_conditional_logic(self):
        """Test complex business rule with OR logic."""
        df = pd.DataFrame({
            "account_type": ["SAVINGS", "CHECKING", "SAVINGS", "CHECKING"],
            "interest_rate": [2.5, 0.0, 0.0, 0.5]
        })

        validation = InlineBusinessRuleCheck(
            name="SavingsInterestRule",
            severity=Severity.ERROR,
            params={
                "rule": "(account_type == 'SAVINGS' & interest_rate > 0) | (account_type != 'SAVINGS')",
                "description": "Savings accounts must have interest rate",
                "error_message": "Savings account has zero interest rate"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for row 3 (SAVINGS with 0.0 interest)
        assert result.passed is False
        assert result.failed_count == 1

    def test_business_rule_all_pass(self):
        """Test business rule where all rows pass."""
        df = pd.DataFrame({
            "price": [10.0, 25.0, 50.0, 100.0],
            "discount": [1.0, 5.0, 10.0, 20.0]
        })

        validation = InlineBusinessRuleCheck(
            name="DiscountRule",
            severity=Severity.ERROR,
            params={
                "rule": "discount < price",
                "description": "Discount must be less than price",
                "error_message": "Discount exceeds price"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        assert result.passed is True
        assert result.failed_count == 0

    def test_business_rule_no_rule_specified(self):
        """Test error handling when no rule is specified."""
        df = pd.DataFrame({"field": [1, 2, 3]})

        validation = InlineBusinessRuleCheck(
            name="NoRule",
            severity=Severity.ERROR,
            params={
                "description": "Missing rule"
            }
        )

        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "No rule specified" in result.message


@pytest.mark.unit
class TestInlineLookupCheck:
    """Tests for InlineLookupCheck - reference data validation in YAML."""

    def test_lookup_allow_valid_countries(self):
        """Test lookup check with allowed values list."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3, 4, 5],
            "country_code": ["US", "UK", "FR", "CA", "DE"]
        })

        validation = InlineLookupCheck(
            name="AllowedCountries",
            severity=Severity.ERROR,
            params={
                "field": "country_code",
                "check_type": "allow",
                "reference_values": ["US", "UK", "CA", "AU", "NZ"],
                "description": "Only accept customers from supported countries"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for FR and DE (not in allowed list)
        assert result.passed is False
        assert result.failed_count == 2
        assert any("FR" in str(f) or "DE" in str(f) for f in result.sample_failures)

    def test_lookup_deny_blocked_domains(self):
        """Test lookup check with denied values list."""
        df = pd.DataFrame({
            "email_domain": ["gmail.com", "tempmail.com", "yahoo.com", "throwaway.email"]
        })

        validation = InlineLookupCheck(
            name="BlockedDomains",
            severity=Severity.WARNING,
            params={
                "field": "email_domain",
                "check_type": "deny",
                "reference_values": ["tempmail.com", "throwaway.email", "guerrillamail.com"],
                "description": "Block temporary email providers"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        # Should fail for tempmail.com and throwaway.email
        assert result.passed is False
        assert result.failed_count == 2

    def test_lookup_product_codes_all_valid(self):
        """Test lookup where all values are valid."""
        df = pd.DataFrame({
            "product_code": ["PROD-001", "PROD-002", "PROD-003"]
        })

        validation = InlineLookupCheck(
            name="ValidProductCodes",
            severity=Severity.ERROR,
            params={
                "field": "product_code",
                "check_type": "allow",
                "reference_values": ["PROD-001", "PROD-002", "PROD-003", "PROD-004"],
                "description": "Product code must be in approved list"
            }
        )

        result = validation.validate(create_data_iterator(df), {"max_sample_failures": 100})

        assert result.passed is True
        assert result.failed_count == 0

    def test_lookup_missing_field(self):
        """Test lookup with non-existent field."""
        df = pd.DataFrame({"other_field": [1, 2, 3]})

        validation = InlineLookupCheck(
            name="MissingField",
            severity=Severity.ERROR,
            params={
                "field": "nonexistent",
                "reference_values": ["A", "B"],
                "description": "Field doesn't exist"
            }
        )

        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "not found" in result.message.lower()

    def test_lookup_no_reference_values(self):
        """Test lookup without reference values."""
        df = pd.DataFrame({"field": ["A", "B"]})

        validation = InlineLookupCheck(
            name="NoRefValues",
            severity=Severity.ERROR,
            params={
                "field": "field",
                "description": "Missing reference values"
            }
        )

        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "Missing required parameters" in result.message


@pytest.mark.integration
class TestInlineValidationsIntegration:
    """Integration tests combining multiple inline validations."""

    def test_combined_inline_checks(self):
        """Test multiple inline validation checks on the same dataset."""
        df = pd.DataFrame({
            "customer_id": ["CUST-001", "CUST-002", "INVALID", "CUST-004"],
            "age": [25, 17, 30, 45],
            "country": ["US", "UK", "FR", "CA"],
            "email": ["test@gmail.com", "bad@tempmail.com", "good@yahoo.com", "user@test.com"]
        })

        # Regex check for customer ID format
        regex_check = InlineRegexCheck(
            name="CustomerIDFormat",
            severity=Severity.ERROR,
            params={
                "field": "customer_id",
                "pattern": r"^CUST-\d{3}$",
                "description": "Customer ID must be CUST-###"
            }
        )

        # Business rule for age
        age_check = InlineBusinessRuleCheck(
            name="AgeCheck",
            severity=Severity.ERROR,
            params={
                "rule": "age >= 18",
                "description": "Must be 18+",
                "error_message": "Underage"
            }
        )

        # Lookup check for country
        country_check = InlineLookupCheck(
            name="AllowedCountry",
            severity=Severity.ERROR,
            params={
                "field": "country",
                "check_type": "allow",
                "reference_values": ["US", "UK", "CA"],
                "description": "Supported countries"
            }
        )

        # Lookup check for blocked email domains
        email_check = InlineLookupCheck(
            name="BlockedEmails",
            severity=Severity.WARNING,
            params={
                "field": "email",
                "check_type": "deny",
                "reference_values": ["bad@tempmail.com"],
                "description": "Block temp emails"
            }
        )

        context = {"max_sample_failures": 100}

        regex_result = regex_check.validate(create_data_iterator(df), context)
        age_result = age_check.validate(create_data_iterator(df), context)
        country_result = country_check.validate(create_data_iterator(df), context)
        email_result = email_check.validate(create_data_iterator(df), context)

        # Regex should fail for "INVALID"
        assert regex_result.passed is False
        assert regex_result.failed_count == 1

        # Age should fail for 17
        assert age_result.passed is False
        assert age_result.failed_count == 1

        # Country should fail for "FR"
        assert country_result.passed is False
        assert country_result.failed_count == 1

        # Email should fail for tempmail
        assert email_result.passed is False
        assert email_result.failed_count == 1
