"""
Postal code detection with decimal rejection.

This module provides functions to detect postal codes while avoiding
false positives on decimal numbers (e.g., 0.04781 should not match).

Supports multiple postal code formats:
    - US ZIP codes: 5 digits or 5+4 format (12345, 12345-6789)
    - UK postcodes: AA9A 9AA format
    - Canadian: A9A 9A9 format
    - Generic: 4-10 digit codes

Usage:
    from validation_framework.shared_analysis import detect_postal_codes

    # Detect postal codes in a series
    count = detect_postal_codes(df['address_field'])

    # With detailed results
    results = detect_postal_codes(df['zip'], return_details=True)
"""

import re
from typing import Union, Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

# Postal code patterns by country/format
POSTAL_PATTERNS = {
    # US ZIP codes (5 digits or ZIP+4)
    'us_zip': r'^\d{5}(?:-\d{4})?$',
    # UK postcodes
    'uk_postcode': r'^[A-Z]{1,2}[0-9][0-9A-Z]?\s*[0-9][A-Z]{2}$',
    # Canadian postal codes
    'ca_postal': r'^[A-Z][0-9][A-Z]\s*[0-9][A-Z][0-9]$',
    # German PLZ (5 digits)
    'de_plz': r'^[0-9]{5}$',
    # French postal codes (5 digits)
    'fr_postal': r'^[0-9]{5}$',
    # Generic numeric (4-10 digits, no decimals)
    'generic': r'^[0-9]{4,10}$',
}

# Combined pattern for quick detection
QUICK_POSTAL_PATTERN = r'^\d{4,10}$|^[A-Z]{1,2}[0-9]'

# Patterns that indicate NOT a postal code (decimal numbers, scientific notation)
EXCLUSION_PATTERNS = [
    r'\.',           # Contains decimal point
    r'[eE][+-]?\d',  # Scientific notation
    r'^0\d',         # Leading zero followed by digit (except valid zips)
]


def is_valid_postal_code(
    value: Any,
    country: Optional[str] = None,
    reject_decimals: bool = True
) -> bool:
    """
    Check if a value is a valid postal code.

    Args:
        value: Value to check
        country: Optional country code to use specific format ('us', 'uk', 'ca', 'de', 'fr')
        reject_decimals: If True, reject values that look like decimal numbers

    Returns:
        True if the value appears to be a valid postal code

    Example:
        >>> is_valid_postal_code("12345")
        True
        >>> is_valid_postal_code("0.04781")
        False
        >>> is_valid_postal_code("SW1A 1AA", country='uk')
        True
    """
    if value is None:
        return False

    value_str = str(value).strip().upper()

    if not value_str:
        return False

    # Decimal rejection - critical for avoiding false positives
    if reject_decimals:
        # Check for decimal point
        if '.' in str(value):
            return False

        # Check for scientific notation
        if re.search(r'[eE][+-]?\d', value_str):
            return False

        # Reject values that are clearly floating point
        try:
            float_val = float(value)
            if float_val != int(float_val):
                return False
        except (ValueError, TypeError):
            pass

    # Country-specific validation
    if country:
        country = country.lower()
        if country == 'us':
            return bool(re.match(POSTAL_PATTERNS['us_zip'], value_str))
        elif country == 'uk':
            return bool(re.match(POSTAL_PATTERNS['uk_postcode'], value_str, re.IGNORECASE))
        elif country == 'ca':
            return bool(re.match(POSTAL_PATTERNS['ca_postal'], value_str, re.IGNORECASE))
        elif country in ('de', 'fr'):
            return bool(re.match(r'^[0-9]{5}$', value_str))

    # Generic validation
    # Check against multiple patterns
    for pattern_name, pattern in POSTAL_PATTERNS.items():
        if re.match(pattern, value_str, re.IGNORECASE):
            return True

    return False


def detect_postal_codes(
    series: Any,
    column_name: Optional[str] = None,
    reject_decimals: bool = True,
    min_match_ratio: float = 0.5,
    return_details: bool = False
) -> Union[int, Dict[str, Any]]:
    """
    Detect postal codes in a pandas or Polars series.

    Uses pattern matching with decimal rejection to avoid false positives
    on numeric columns containing floats.

    Args:
        series: Pandas Series or Polars Series to analyze
        column_name: Optional column name for semantic hints
        reject_decimals: If True, reject decimal numbers (default True)
        min_match_ratio: Minimum ratio of matches to consider column as postal codes
        return_details: If True, return detailed statistics

    Returns:
        If return_details=False: Count of postal code matches
        If return_details=True: Dict with counts and format breakdown

    Example:
        >>> count = detect_postal_codes(df['zip_code'])
        >>> details = detect_postal_codes(df['zip_code'], return_details=True)
    """
    format_counts = {
        'us_zip': 0,
        'uk_postcode': 0,
        'ca_postal': 0,
        'de_plz': 0,
        'generic': 0,
        'rejected_decimals': 0,
    }

    try:
        # Detect backend (Polars vs Pandas)
        is_polars = hasattr(series, 'drop_nulls')

        if is_polars:
            try:
                values = series.drop_nulls().to_list()
            except AttributeError:
                # Fallback for pandas
                values = series.dropna().tolist()
        else:
            values = series.dropna().tolist()

        total_values = len(values)
        if total_values == 0:
            if return_details:
                return {'count': 0, 'formats': format_counts, 'total_checked': 0}
            return 0

        valid_count = 0

        for value in values:
            value_str = str(value).strip().upper()

            # Decimal rejection
            if reject_decimals:
                # Check for decimal point in original value
                if '.' in str(value):
                    format_counts['rejected_decimals'] += 1
                    continue

                # Check for scientific notation
                if re.search(r'[eE][+-]?\d', str(value)):
                    format_counts['rejected_decimals'] += 1
                    continue

                # Reject floating point values
                try:
                    float_val = float(value)
                    if float_val != int(float_val):
                        format_counts['rejected_decimals'] += 1
                        continue
                except (ValueError, TypeError):
                    pass

            # Check specific formats
            matched = False
            if re.match(POSTAL_PATTERNS['us_zip'], value_str):
                format_counts['us_zip'] += 1
                matched = True
            elif re.match(POSTAL_PATTERNS['uk_postcode'], value_str, re.IGNORECASE):
                format_counts['uk_postcode'] += 1
                matched = True
            elif re.match(POSTAL_PATTERNS['ca_postal'], value_str, re.IGNORECASE):
                format_counts['ca_postal'] += 1
                matched = True
            elif re.match(POSTAL_PATTERNS['de_plz'], value_str):
                format_counts['de_plz'] += 1
                matched = True
            elif re.match(POSTAL_PATTERNS['generic'], value_str):
                format_counts['generic'] += 1
                matched = True

            if matched:
                valid_count += 1

        # Calculate match ratio
        match_ratio = valid_count / total_values if total_values > 0 else 0

        # Determine dominant format
        dominant_format = max(format_counts.items(), key=lambda x: x[1] if x[0] != 'rejected_decimals' else 0)

        if return_details:
            return {
                'count': valid_count,
                'total_checked': total_values,
                'match_ratio': match_ratio,
                'formats': format_counts,
                'dominant_format': dominant_format[0] if dominant_format[1] > 0 else None,
                'rejected_decimals': format_counts['rejected_decimals'],
                'is_postal_column': match_ratio >= min_match_ratio
            }

        return valid_count

    except Exception as e:
        logger.debug(f"Postal code detection error: {e}")
        if return_details:
            return {'count': 0, 'formats': format_counts, 'error': str(e)}
        return 0


def infer_postal_format(series: Any, sample_size: int = 1000) -> Optional[str]:
    """
    Infer the postal code format from a series.

    Args:
        series: Pandas or Polars series
        sample_size: Number of values to sample for inference

    Returns:
        Inferred format name ('us_zip', 'uk_postcode', etc.) or None
    """
    results = detect_postal_codes(series, return_details=True)

    if results.get('count', 0) == 0:
        return None

    return results.get('dominant_format')
