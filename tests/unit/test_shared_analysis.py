"""
Tests for shared analysis modules.

Tests credit card detection, outlier detection, and postal code detection
to ensure consistent behavior between profiler and validator.
"""

import pytest
import pandas as pd
import numpy as np


class TestCreditCardDetection:
    """Tests for credit card detection with Luhn validation."""

    def test_luhn_check_valid_cards(self):
        """Test Luhn algorithm with known valid credit card numbers."""
        from validation_framework.shared_analysis.credit_card_detection import luhn_check

        # Known valid test numbers (standard test cards)
        valid_numbers = [
            "4532015112830366",  # Visa
            "5425233430109903",  # MasterCard
            "374245455400126",   # Amex
            "6011000990139424",  # Discover
            "4111111111111111",  # Visa test card
        ]

        for number in valid_numbers:
            assert luhn_check(number), f"Expected {number} to pass Luhn check"

    def test_luhn_check_invalid_cards(self):
        """Test Luhn algorithm rejects invalid numbers."""
        from validation_framework.shared_analysis.credit_card_detection import luhn_check

        # Invalid numbers (random digits that fail Luhn)
        # Note: 0000000000000000 is technically valid (0%10=0)
        invalid_numbers = [
            "1234567890123456",
            "9999999999999999",
            "4532015112830367",  # One digit off from valid
            "1111111111111112",  # Checksum fails
        ]

        for number in invalid_numbers:
            assert not luhn_check(number), f"Expected {number} to fail Luhn check"

    def test_luhn_check_short_numbers(self):
        """Test Luhn rejects numbers that are too short."""
        from validation_framework.shared_analysis.credit_card_detection import luhn_check

        assert not luhn_check("123456")
        assert not luhn_check("1234")
        assert not luhn_check("1")

    def test_is_valid_credit_card_semantic_exclusion(self):
        """Test semantic exclusion based on column name."""
        from validation_framework.shared_analysis.credit_card_detection import is_valid_credit_card

        valid_cc = "4532015112830366"

        # Should be valid without column name
        assert is_valid_credit_card(valid_cc)

        # Should be excluded with account-related column names
        assert not is_valid_credit_card(valid_cc, column_name="account_id")
        assert not is_valid_credit_card(valid_cc, column_name="customer_number")
        assert not is_valid_credit_card(valid_cc, column_name="order_reference")
        assert not is_valid_credit_card(valid_cc, column_name="transaction_id")

    def test_detect_credit_cards_pandas(self):
        """Test credit card detection in pandas Series."""
        from validation_framework.shared_analysis.credit_card_detection import detect_credit_cards

        # Mix of valid and invalid numbers
        data = pd.Series([
            "4532015112830366",  # Valid
            "5425233430109903",  # Valid
            "1234567890123456",  # Invalid (fails Luhn)
            "not a card",       # Not a number
            None,               # Null
            "4111111111111111",  # Valid
        ])

        # Get details to understand what's happening
        details = detect_credit_cards(data, return_details=True)

        # Should find pattern matches for all 16-digit numbers
        assert details['pattern_matches'] >= 3, f"Expected at least 3 pattern matches, got {details['pattern_matches']}"

        # Should have 3 that pass Luhn (first 2 valid + last valid, but not 1234567890123456)
        assert details['luhn_valid'] == 3, f"Expected 3 Luhn valid, got {details['luhn_valid']}"

    def test_detect_credit_cards_with_details(self):
        """Test credit card detection with detailed results."""
        from validation_framework.shared_analysis.credit_card_detection import detect_credit_cards

        data = pd.Series([
            "4532015112830366",
            "5425233430109903",
            "1234567890123456",
        ])

        results = detect_credit_cards(data, return_details=True)

        assert 'count' in results
        assert 'pattern_matches' in results
        assert 'luhn_valid' in results
        assert results['pattern_matches'] == 3  # All match pattern
        assert results['luhn_valid'] == 2  # Only 2 pass Luhn


class TestOutlierDetection:
    """Tests for statistical outlier detection."""

    def test_detect_outliers_zscore_normal_data(self):
        """Test Z-score detection on normally distributed data."""
        from validation_framework.shared_analysis.outlier_detection import detect_outliers_zscore

        # Normal distribution with clear outliers
        np.random.seed(42)
        normal_data = np.random.normal(100, 10, 1000)

        # Add clear outliers
        data_with_outliers = np.append(normal_data, [200, 250, 0, -50])

        series = pd.Series(data_with_outliers)
        outlier_count = detect_outliers_zscore(series, threshold=3.0)

        # Should detect the extreme outliers
        assert outlier_count >= 2, f"Expected at least 2 outliers, got {outlier_count}"

    def test_detect_outliers_zscore_with_mask(self):
        """Test Z-score returns mask when requested."""
        from validation_framework.shared_analysis.outlier_detection import detect_outliers_zscore

        data = pd.Series([1, 2, 3, 4, 5, 100])  # 100 is outlier

        count, mask = detect_outliers_zscore(data, threshold=2.0, return_mask=True)

        assert count > 0
        assert len(mask) == 6
        assert mask[-1] == True  # 100 should be flagged

    def test_detect_outliers_iqr(self):
        """Test IQR outlier detection."""
        from validation_framework.shared_analysis.outlier_detection import detect_outliers_iqr

        # Data with clear outliers
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, -50])

        count = detect_outliers_iqr(data, multiplier=1.5)
        assert count >= 2, f"Expected at least 2 outliers, got {count}"

    def test_detect_outliers_iqr_with_bounds(self):
        """Test IQR returns bounds when requested."""
        from validation_framework.shared_analysis.outlier_detection import detect_outliers_iqr

        data = pd.Series(range(1, 101))  # 1 to 100

        results = detect_outliers_iqr(data, return_bounds=True)

        assert 'Q1' in results
        assert 'Q3' in results
        assert 'IQR' in results
        assert 'lower_bound' in results
        assert 'upper_bound' in results
        assert results['Q1'] == 25.75
        assert results['Q3'] == 75.25

    def test_detect_outliers_combined(self):
        """Test combined Z-score and IQR detection."""
        from validation_framework.shared_analysis.outlier_detection import detect_outliers

        np.random.seed(42)
        data = pd.Series(np.append(np.random.normal(50, 5, 100), [200, -100]))

        results = detect_outliers(data, method='combined', return_details=True)

        assert 'zscore' in results
        assert 'iqr' in results
        assert results['count'] >= 0

    def test_detect_outliers_empty_series(self):
        """Test outlier detection handles empty series."""
        from validation_framework.shared_analysis.outlier_detection import detect_outliers_zscore

        empty_series = pd.Series([], dtype=float)
        count = detect_outliers_zscore(empty_series)
        assert count == 0


class TestPostalCodeDetection:
    """Tests for postal code detection with decimal rejection."""

    def test_is_valid_postal_code_us(self):
        """Test US ZIP code validation."""
        from validation_framework.shared_analysis.postal_code_detection import is_valid_postal_code

        # Valid US ZIP codes
        assert is_valid_postal_code("12345")
        assert is_valid_postal_code("12345-6789")
        assert is_valid_postal_code("90210")

        # Invalid
        assert not is_valid_postal_code("123")  # Too short
        assert not is_valid_postal_code("123456789012")  # Too long

    def test_is_valid_postal_code_decimal_rejection(self):
        """Test that decimal numbers are rejected."""
        from validation_framework.shared_analysis.postal_code_detection import is_valid_postal_code

        # These should NOT be detected as postal codes
        assert not is_valid_postal_code("0.04781")
        assert not is_valid_postal_code("12345.67")
        assert not is_valid_postal_code("1.23e4")
        assert not is_valid_postal_code(0.04781)
        assert not is_valid_postal_code(12345.67)

    def test_is_valid_postal_code_uk(self):
        """Test UK postcode validation."""
        from validation_framework.shared_analysis.postal_code_detection import is_valid_postal_code

        assert is_valid_postal_code("SW1A 1AA", country='uk')
        assert is_valid_postal_code("EC1A 1BB", country='uk')
        assert is_valid_postal_code("M1 1AE", country='uk')

    def test_is_valid_postal_code_canada(self):
        """Test Canadian postal code validation."""
        from validation_framework.shared_analysis.postal_code_detection import is_valid_postal_code

        assert is_valid_postal_code("K1A 0B1", country='ca')
        assert is_valid_postal_code("M5H 2N2", country='ca')

    def test_detect_postal_codes_pandas(self):
        """Test postal code detection in pandas Series."""
        from validation_framework.shared_analysis.postal_code_detection import detect_postal_codes

        data = pd.Series([
            "12345",      # Valid US ZIP
            "90210",      # Valid US ZIP
            "0.04781",    # Decimal - should be rejected
            "not a zip",  # Not a postal code
            None,         # Null
        ])

        count = detect_postal_codes(data)
        assert count == 2, f"Expected 2 postal codes, got {count}"

    def test_detect_postal_codes_with_details(self):
        """Test postal code detection with detailed results."""
        from validation_framework.shared_analysis.postal_code_detection import detect_postal_codes

        data = pd.Series([
            "12345",
            "90210",
            "12345-6789",
            "0.5",
        ])

        results = detect_postal_codes(data, return_details=True)

        assert 'count' in results
        assert 'formats' in results
        assert 'rejected_decimals' in results
        assert results['rejected_decimals'] == 1

    def test_detect_postal_codes_float_column(self):
        """Test that float columns are properly rejected."""
        from validation_framework.shared_analysis.postal_code_detection import detect_postal_codes

        # Simulate a column that looks like it could have postal codes but is actually floats
        float_data = pd.Series([0.12345, 0.90210, 0.55555, 0.11111])

        count = detect_postal_codes(float_data)
        assert count == 0, f"Expected 0 postal codes in float data, got {count}"


class TestSharedAnalysisIntegration:
    """Integration tests for shared analysis modules."""

    def test_imports_work(self):
        """Test that all imports from shared_analysis work."""
        from validation_framework.shared_analysis import (
            luhn_check,
            detect_credit_cards,
            is_valid_credit_card,
            detect_outliers_zscore,
            detect_outliers_iqr,
            detect_outliers,
            detect_postal_codes,
            is_valid_postal_code,
        )

        # Just verify imports succeed
        assert callable(luhn_check)
        assert callable(detect_credit_cards)
        assert callable(is_valid_credit_card)
        assert callable(detect_outliers_zscore)
        assert callable(detect_outliers_iqr)
        assert callable(detect_outliers)
        assert callable(detect_postal_codes)
        assert callable(is_valid_postal_code)

    def test_all_functions_handle_none(self):
        """Test that all functions handle None values gracefully."""
        from validation_framework.shared_analysis import (
            luhn_check,
            is_valid_credit_card,
            is_valid_postal_code,
        )

        # Should not raise exceptions
        assert not luhn_check(None)
        assert not is_valid_credit_card(None)
        assert not is_valid_postal_code(None)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
