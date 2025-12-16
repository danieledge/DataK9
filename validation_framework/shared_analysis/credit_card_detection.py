"""
Credit card detection with Luhn algorithm validation.

This module provides functions to detect and validate credit card numbers
using the Luhn algorithm (also known as the "modulus 10" algorithm).

The Luhn algorithm is a simple checksum formula used to validate credit card
numbers, IMEI numbers, National Provider Identifier numbers, and more.

Algorithm:
    1. From the rightmost digit, double every second digit
    2. If doubling results in a number > 9, subtract 9
    3. Sum all the digits
    4. If the total modulo 10 is 0, the number is valid

Usage:
    from validation_framework.shared_analysis import luhn_check, detect_credit_cards

    # Check single number
    is_valid = luhn_check("4532015112830366")  # True

    # Detect in a pandas Series
    count = detect_credit_cards(df['card_number'])
"""

import re
from typing import Union, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

# Credit card pattern: 13-19 digits (supports most major card types)
# Visa: 16 digits starting with 4
# MasterCard: 16 digits starting with 51-55 or 2221-2720
# Amex: 15 digits starting with 34 or 37
# Discover: 16 digits starting with 6011, 644-649, or 65
CREDIT_CARD_PATTERN = r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|6(?:011|5[0-9]{2})[0-9]{12}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|(?:2131|1800|35\d{3})\d{11}|[0-9]{13,19})\b'

# Semantic exclusion patterns - column names that should NOT be flagged as credit cards
SEMANTIC_EXCLUSIONS = [
    'account', 'acct', 'id', 'identifier', 'code', 'number', 'num', 'no',
    'reference', 'ref', 'invoice', 'order', 'transaction', 'trx', 'txn',
    'serial', 'batch', 'ticket', 'confirmation', 'tracking', 'sequence',
    'customer', 'member', 'employee', 'user', 'product', 'sku', 'item',
    'phone', 'mobile', 'fax', 'postal', 'zip', 'ssn', 'tin', 'ein',
]


def luhn_check(number: Union[str, int]) -> bool:
    """
    Validate a number using the Luhn algorithm.

    Args:
        number: Credit card number as string or integer

    Returns:
        True if the number passes Luhn validation, False otherwise

    Example:
        >>> luhn_check("4532015112830366")
        True
        >>> luhn_check("1234567890123456")
        False
    """
    # Convert to string and extract only digits
    number_str = str(number)
    digits = [int(d) for d in number_str if d.isdigit()]

    # Credit cards are typically 13-19 digits
    if len(digits) < 13 or len(digits) > 19:
        return False

    # Luhn algorithm
    # Reverse the digits
    digits = digits[::-1]

    # Double every second digit (starting from index 1)
    for i in range(1, len(digits), 2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9

    # Sum all digits and check if divisible by 10
    total = sum(digits)
    return total % 10 == 0


def is_valid_credit_card(value: Any, column_name: Optional[str] = None) -> bool:
    """
    Check if a value is a valid credit card number.

    This function combines pattern matching with Luhn validation and
    semantic exclusion based on column name.

    Args:
        value: Value to check
        column_name: Optional column name for semantic exclusion

    Returns:
        True if the value appears to be a valid credit card number

    Example:
        >>> is_valid_credit_card("4532015112830366")
        True
        >>> is_valid_credit_card("4532015112830366", column_name="account_id")
        False  # Excluded due to semantic pattern
    """
    if value is None:
        return False

    # Check semantic exclusion based on column name
    if column_name:
        col_lower = column_name.lower()
        for exclusion in SEMANTIC_EXCLUSIONS:
            if exclusion in col_lower:
                return False

    # Convert to string
    value_str = str(value).strip()

    # Quick length check
    digits_only = re.sub(r'\D', '', value_str)
    if len(digits_only) < 13 or len(digits_only) > 19:
        return False

    # Pattern match
    if not re.match(CREDIT_CARD_PATTERN, digits_only):
        return False

    # Luhn validation
    return luhn_check(digits_only)


def detect_credit_cards(
    series: Any,
    column_name: Optional[str] = None,
    luhn_threshold: float = 0.8,
    return_details: bool = False
) -> Union[int, dict]:
    """
    Detect credit card numbers in a pandas or Polars series.

    Uses pattern matching followed by Luhn validation to reduce false positives.
    Supports semantic exclusion based on column name.

    Args:
        series: Pandas Series or Polars Series to analyze
        column_name: Optional column name for semantic exclusion
        luhn_threshold: Minimum percentage of matches that must pass Luhn (0.0-1.0)
        return_details: If True, return detailed statistics

    Returns:
        If return_details=False: Count of valid credit card numbers
        If return_details=True: Dict with counts, percentages, and validation stats

    Example:
        >>> count = detect_credit_cards(df['payment_card'])
        >>> details = detect_credit_cards(df['payment_card'], return_details=True)
    """
    # Check semantic exclusion first
    if column_name:
        col_lower = column_name.lower()
        for exclusion in SEMANTIC_EXCLUSIONS:
            if exclusion in col_lower:
                if return_details:
                    return {
                        'count': 0,
                        'pattern_matches': 0,
                        'luhn_valid': 0,
                        'excluded_reason': f'Column name contains "{exclusion}"'
                    }
                return 0

    try:
        # Detect backend (Polars vs Pandas) - check for Polars-specific method
        is_polars = hasattr(series, 'drop_nulls') and not hasattr(series, 'dropna')

        if is_polars:
            # Polars path
            import polars as pl

            try:
                # Filter to non-null string values
                str_series = series.cast(pl.Utf8, strict=False).drop_nulls()

                # Pattern match
                pattern_matches = str_series.filter(
                    str_series.str.contains(CREDIT_CARD_PATTERN)
                )

                if pattern_matches.len() == 0:
                    if return_details:
                        return {'count': 0, 'pattern_matches': 0, 'luhn_valid': 0}
                    return 0

                # Luhn validation
                luhn_valid = sum(1 for v in pattern_matches.to_list() if luhn_check(v))
            except Exception:
                # Fallback to pandas-style processing
                is_polars = False

        if not is_polars:
            # Pandas path
            import pandas as pd

            # Convert to string and drop nulls
            str_series = series.astype(str).replace(['nan', 'None', ''], pd.NA).dropna()

            # Pattern match - extract digits only for pattern matching
            digits_only = str_series.str.replace(r'\D', '', regex=True)
            pattern_mask = digits_only.str.match(r'^[0-9]{13,19}$', na=False)
            pattern_matches = str_series[pattern_mask]

            if len(pattern_matches) == 0:
                if return_details:
                    return {'count': 0, 'pattern_matches': 0, 'luhn_valid': 0}
                return 0

            # Luhn validation
            luhn_valid = sum(1 for v in pattern_matches.values if luhn_check(v))

        # Calculate statistics
        pattern_count = len(pattern_matches) if not is_polars else pattern_matches.len()
        luhn_ratio = luhn_valid / pattern_count if pattern_count > 0 else 0

        # Only count as credit cards if sufficient Luhn validation passes
        valid_count = luhn_valid if luhn_ratio >= luhn_threshold else 0

        if return_details:
            return {
                'count': valid_count,
                'pattern_matches': pattern_count,
                'luhn_valid': luhn_valid,
                'luhn_ratio': luhn_ratio,
                'threshold_met': luhn_ratio >= luhn_threshold
            }

        return valid_count

    except Exception as e:
        logger.debug(f"Credit card detection error: {e}")
        if return_details:
            return {'count': 0, 'pattern_matches': 0, 'luhn_valid': 0, 'error': str(e)}
        return 0
