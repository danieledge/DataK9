"""
Shared analysis modules for DataK9 Profiler and Validator.

These modules provide reusable analysis functions that are used by both
the profiler and validator components to ensure consistent behavior
and avoid code duplication.

Modules:
    - credit_card_detection: Credit card detection with Luhn validation
    - outlier_detection: Statistical outlier detection (Z-score, IQR)
    - postal_code_detection: Postal code detection with decimal rejection
"""

from validation_framework.shared_analysis.credit_card_detection import (
    luhn_check,
    detect_credit_cards,
    is_valid_credit_card,
)
from validation_framework.shared_analysis.outlier_detection import (
    detect_outliers_zscore,
    detect_outliers_iqr,
    detect_outliers,
)
from validation_framework.shared_analysis.postal_code_detection import (
    detect_postal_codes,
    is_valid_postal_code,
)

__all__ = [
    # Credit card detection
    'luhn_check',
    'detect_credit_cards',
    'is_valid_credit_card',
    # Outlier detection
    'detect_outliers_zscore',
    'detect_outliers_iqr',
    'detect_outliers',
    # Postal code detection
    'detect_postal_codes',
    'is_valid_postal_code',
]
