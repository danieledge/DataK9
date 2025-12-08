"""
Generic visualization fallback utilities for data profiling.

Provides fallback renderers and helper functions to ensure no visualization
ever appears blank, regardless of dataset characteristics.

Key Principles:
- NO dataset-specific logic (no hardcoded column names)
- All conditions based on data characteristics only
- Generic, reusable fallback messages and visualizations
"""

import numpy as np
import pandas as pd
import re
from typing import Dict, Any, Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# ROBUST NUMERIC DETECTION
# ============================================================================

def coerce_to_numeric(series: pd.Series) -> Tuple[pd.Series, bool]:
    """
    Robustly coerce a series to numeric values.

    A column is considered numeric if:
    - It can be coerced to float (pd.to_numeric with errors="coerce")
    - After coercion, it has > 1 unique numeric value
    - It is not binary (≤ 3 unique numeric values)

    Works for:
    - float columns
    - integer columns
    - numeric strings (e.g. "100", "20.5", " ")
    - mixed dtype columns that become numeric after coercion

    Args:
        series: Input pandas Series

    Returns:
        Tuple of (coerced_series, is_valid_numeric)
        - coerced_series: Series with numeric values (NaN for non-numeric)
        - is_valid_numeric: True if suitable for numeric analysis
    """
    if series is None or len(series) == 0:
        return pd.Series(dtype=float), False

    # Try coercion
    try:
        coerced = pd.to_numeric(series, errors='coerce')
    except Exception as e:
        logger.debug(f"Numeric coercion failed: {e}")
        return pd.Series(dtype=float), False

    # Check validity
    valid_values = coerced.dropna()

    if len(valid_values) < 2:
        return coerced, False

    unique_count = valid_values.nunique()

    # Binary/low-cardinality check (≤ 3 unique values = likely categorical encoded as numeric)
    if unique_count <= 3:
        return coerced, False

    return coerced, True


def is_numeric_for_analysis(series: pd.Series, min_unique: int = 4) -> bool:
    """
    Check if a series is suitable for numeric analysis (Benford, IQR, etc.).

    More strict than basic numeric check - requires sufficient unique values
    and variation for meaningful statistical analysis.

    Args:
        series: Input series
        min_unique: Minimum unique values required (default 4)

    Returns:
        True if suitable for numeric analysis
    """
    _, is_valid = coerce_to_numeric(series)
    if not is_valid:
        return False

    coerced = pd.to_numeric(series, errors='coerce').dropna()
    return coerced.nunique() >= min_unique


# ============================================================================
# BENFORD'S LAW APPLICABILITY
# ============================================================================

def should_apply_benford_generic(
    series: pd.Series,
    col_name: str = "",
    min_values: int = 100,
    min_unique: int = 20,
    min_magnitude_span: float = 1.5
) -> Tuple[bool, str]:
    """
    Generic check if Benford's Law should be applied to a column.

    NO dataset-specific logic - operates purely on data characteristics.

    Benford's Law works well for data that:
    1. Has positive values
    2. Spans multiple orders of magnitude (~2+ orders)
    3. Has sufficient observations (at least 100)
    4. Has enough unique values (not low-cardinality)

    Args:
        series: Input series (will be coerced to numeric)
        col_name: Column name (used only for semantic hints, not hardcoded)
        min_values: Minimum positive values required
        min_unique: Minimum unique values required
        min_magnitude_span: Minimum log10 span required

    Returns:
        Tuple of (should_apply, reason)
        - should_apply: True if Benford analysis is appropriate
        - reason: Generic explanation of why/why not
    """
    # Coerce to numeric
    coerced, is_valid = coerce_to_numeric(series)

    if not is_valid:
        return False, "Column does not contain sufficient numeric values"

    # Get positive non-zero values only (Benford requires positive numbers)
    valid = coerced.dropna()
    positive_values = valid[valid > 0]

    # Criterion 1: Need sufficient observations
    if len(positive_values) < min_values:
        return False, f"Insufficient positive values ({len(positive_values)} < {min_values})"

    # Criterion 2: Need sufficient unique values (not low-cardinality)
    unique_count = len(np.unique(positive_values))
    if unique_count < min_unique:
        return False, f"Insufficient unique values ({unique_count} < {min_unique}) - data may be categorical"

    # Criterion 3: Data should span multiple orders of magnitude
    min_val = np.min(positive_values)
    max_val = np.max(positive_values)

    if min_val <= 0:
        return False, "Values include non-positive numbers"

    magnitude_span = np.log10(max_val / min_val)
    if magnitude_span < min_magnitude_span:
        return False, f"Value range too narrow ({magnitude_span:.1f} < {min_magnitude_span} orders of magnitude)"

    # Generic semantic checks (no hardcoded column names)
    col_lower = col_name.lower()

    # Exclude patterns that suggest identifiers (generic patterns)
    identifier_patterns = ['_id', 'id_', 'code', '_code', 'index', 'row', 'sequence', 'version']
    if any(pattern in col_lower for pattern in identifier_patterns):
        return False, "Column name suggests identifier/code (not suitable for Benford)"

    # Binary/flag indicators
    if col_lower.startswith(('is_', 'has_', 'was_', 'can_')) or col_lower.endswith(('_flag', '_indicator')):
        return False, "Column appears to be a binary flag"

    # Exclude averaged/engineered/derived features (Benford assumes naturally occurring data)
    # Averages, ratios, and engineered features are mathematically derived
    # and don't follow Benford's Law naturally - deviation is expected, not suspicious
    engineered_patterns = [
        'avg', 'ave', 'mean', 'ratio', 'pct', 'percent', 'rate', 'median', 'mode',
        'per_', '_per', 'proportion', 'share', 'fraction', 'normalized', 'scaled',
        'index', 'score', 'coefficient', 'factor', 'multiplier', 'weight',
        'occupancy', 'occup', 'density', 'intensity', 'frequency'
    ]
    if any(pattern in col_lower for pattern in engineered_patterns):
        return False, "Column appears to be an averaged, derived, or engineered metric (not suitable for Benford)"

    # Check for bounded data patterns (0-1 ratios, percentages, demographic bounds)
    min_val = np.min(positive_values)
    max_val = np.max(positive_values)

    # Percentage-like bounded data (0-1 or 0-100)
    if (0 <= min_val <= 1 and 0 < max_val <= 1) or (0 <= min_val <= 100 and 0 < max_val <= 100):
        # Check if values cluster like a percentage (many near boundaries)
        if max_val <= 1:
            return False, "Values appear to be proportions/percentages (0-1 range)"
        elif max_val <= 100 and (np.mean(positive_values) < 100 and np.percentile(positive_values, 90) <= 100):
            return False, "Values appear to be percentages or bounded demographic data (0-100 range)"

    # Demographic bounds check (age-like fields)
    age_patterns = ['age', 'year', 'years', 'month', 'months']
    if any(pattern in col_lower for pattern in age_patterns):
        if 0 <= min_val and max_val <= 150:  # Reasonable human age/year bound
            return False, "Column appears to be age or duration data (bounded range)"

    return True, "Column is suitable for Benford's Law analysis"


def extract_benford_digits(series: pd.Series) -> Tuple[pd.Series, bool, str]:
    """
    Extract first significant digits for Benford analysis.

    Guarantees one of two outcomes:
    A) A valid series of first digits (1-9)
    B) Empty series with explanation of why extraction failed

    Uses safe regex: r"([1-9])" to extract first significant digit.

    Args:
        series: Input series (will be coerced to numeric)

    Returns:
        Tuple of (digit_series, is_valid, reason)
    """
    coerced, is_valid = coerce_to_numeric(series)

    if not is_valid:
        return pd.Series(dtype=int), False, "Column is not numeric"

    # Get positive values
    valid = coerced.dropna()
    positive_values = valid[valid > 0]

    if len(positive_values) < 100:
        return pd.Series(dtype=int), False, f"Insufficient positive values ({len(positive_values)})"

    # Extract first significant digit using safe method
    def extract_first_digit(x):
        """Extract first digit using regex - safe handling."""
        try:
            s = str(abs(float(x)))
            # Remove leading zeros and decimal point
            s = s.lstrip('0').lstrip('.')
            if not s:
                return 0
            # Find first digit 1-9
            match = re.search(r'([1-9])', s)
            if match:
                return int(match.group(1))
            return 0
        except (ValueError, TypeError):
            return 0

    first_digits = positive_values.apply(extract_first_digit)
    first_digits = first_digits[first_digits > 0]  # Only digits 1-9

    if len(first_digits) < 100:
        return pd.Series(dtype=int), False, f"Insufficient valid first digits ({len(first_digits)})"

    return first_digits, True, "Successfully extracted first digits"


# ============================================================================
# FALLBACK RENDERERS
# ============================================================================

def render_empty_visualization_html(
    title: str,
    reason: str,
    icon: str = "📊",
    suggested_action: str = "",
    css_class: str = "fallback-viz"
) -> str:
    """
    Render a fallback content block for an empty/unavailable visualization.

    Args:
        title: Chart/visualization title
        reason: Generic reason why visualization is not available
        icon: Emoji icon to display
        suggested_action: Optional suggestion for user
        css_class: CSS class for styling

    Returns:
        HTML string for fallback content
    """
    action_html = f'''
        <p style="color: var(--text-muted); font-size: 0.85em; margin-top: 12px;">
            💡 <strong>Tip:</strong> {suggested_action}
        </p>
    ''' if suggested_action else ''

    return f'''
        <div class="{css_class}" style="padding: 24px; background: var(--bg-card); border-radius: 8px; text-align: center;">
            <div style="font-size: 2em; margin-bottom: 12px;">{icon}</div>
            <h4 style="margin: 0 0 8px 0; color: var(--text-primary);">{title}</h4>
            <p style="color: var(--text-muted); margin: 0; max-width: 500px; margin: 0 auto;">
                {reason}
            </p>
            {action_html}
        </div>
    '''


def render_benford_fallback_html(col_name: str, reason: str) -> str:
    """
    Render fallback for Benford's Law when not applicable.

    Args:
        col_name: Column name
        reason: Generic reason why Benford is not applicable

    Returns:
        HTML string for Benford fallback
    """
    return render_empty_visualization_html(
        title=f"Benford's Law Not Applicable: {col_name}",
        reason=reason,
        icon="📐",
        suggested_action="Benford's Law works best with data spanning multiple orders of magnitude, such as financial transactions, population counts, or invoice amounts."
    )


def render_iqr_fallback_html(reason: str) -> str:
    """
    Render fallback for IQR outlier chart when no outliers or no valid data.

    Args:
        reason: Generic reason for fallback

    Returns:
        HTML string for IQR fallback
    """
    return render_empty_visualization_html(
        title="IQR Outlier Analysis",
        reason=reason,
        icon="📊",
        suggested_action="This analysis requires numeric fields with sufficient variation. Datasets with monetary amounts, measurements, or counts typically work well."
    )


def get_outlier_chart_data(
    outlier_rates: Dict[str, float],
    outlier_counts: Optional[Dict[str, int]] = None
) -> Tuple[List[str], List[float], bool, str]:
    """
    Prepare outlier chart data, ensuring non-empty result.

    If all outlier rates are zero, returns the data anyway (zero-height bars)
    rather than producing a blank chart.

    Args:
        outlier_rates: Dict of field -> outlier percentage
        outlier_counts: Optional dict of field -> outlier count

    Returns:
        Tuple of (fields, values, has_outliers, message)
        - fields: List of field names
        - values: List of outlier percentages
        - has_outliers: True if any field has outliers > 0
        - message: Informational message about results
    """
    if not outlier_rates:
        return [], [], False, "No numeric fields available for outlier analysis"

    fields = list(outlier_rates.keys())
    values = [outlier_rates[f] for f in fields]

    has_outliers = any(v > 0 for v in values)

    if has_outliers:
        max_outlier_field = fields[values.index(max(values))]
        message = f"{len(fields)} fields analyzed, highest outlier rate in '{max_outlier_field}'"
    else:
        message = f"No IQR outliers detected across {len(fields)} numeric fields"

    return fields, values, has_outliers, message


def validate_chart_data(
    data: Any,
    labels: Optional[Any] = None,
    min_length: int = 1
) -> Tuple[bool, str]:
    """
    Validate data arrays before charting.

    Checks for:
    - Empty arrays
    - Length mismatches
    - All NaN values
    - Insufficient data points

    Args:
        data: Data array (list or numpy array)
        labels: Optional labels array (must match data length if provided)
        min_length: Minimum required data points

    Returns:
        Tuple of (is_valid, reason)
    """
    # Check for None
    if data is None:
        return False, "Data array is None"

    # Convert to list if needed
    try:
        data_list = list(data) if not isinstance(data, list) else data
    except (TypeError, ValueError):
        return False, "Cannot convert data to list"

    # Check empty
    if len(data_list) == 0:
        return False, "Data array is empty"

    # Check minimum length
    if len(data_list) < min_length:
        return False, f"Insufficient data points ({len(data_list)} < {min_length})"

    # Check all NaN
    try:
        valid_count = sum(1 for x in data_list if x is not None and not (isinstance(x, float) and np.isnan(x)))
        if valid_count == 0:
            return False, "All values are NaN or None"
    except (TypeError, ValueError):
        pass  # Non-numeric data, skip NaN check

    # Check label length match
    if labels is not None:
        try:
            labels_list = list(labels) if not isinstance(labels, list) else labels
            if len(labels_list) != len(data_list):
                return False, f"Label count ({len(labels_list)}) doesn't match data count ({len(data_list)})"
        except (TypeError, ValueError):
            return False, "Cannot validate labels"

    return True, "Data is valid"


# ============================================================================
# GENERIC DATA CHARACTERISTIC CHECKS
# ============================================================================

def is_binary_column(series: pd.Series) -> bool:
    """Check if column is binary (exactly 2 unique non-null values)."""
    unique = series.dropna().nunique()
    return unique == 2


def is_low_cardinality(series: pd.Series, threshold: int = 10) -> bool:
    """Check if column has low cardinality (few unique values)."""
    unique = series.dropna().nunique()
    return unique <= threshold


def is_identifier_like(col_name: str, series: pd.Series) -> bool:
    """
    Check if column appears to be an identifier based on characteristics.

    Generic detection - no hardcoded column names.
    """
    col_lower = col_name.lower()

    # Name-based patterns
    id_patterns = ['_id', 'id_', '_pk', 'pk_', '_key', 'key_', '_uuid', 'uuid_',
                   '_guid', 'guid_', 'code', 'index', 'sequence', 'rownum']
    if any(p in col_lower for p in id_patterns) or col_lower in ('id', 'pk', 'key'):
        return True

    # Data-based patterns
    if series.dtype == 'object':
        # High uniqueness ratio for strings suggests identifier
        unique_ratio = series.nunique() / len(series.dropna()) if len(series.dropna()) > 0 else 0
        if unique_ratio > 0.9:
            return True

    return False


def is_bounded_data(series: pd.Series, lower: float = 0, upper: float = 100) -> bool:
    """
    Check if data appears bounded to a fixed range (e.g., percentages, ratings).

    Bounded data often doesn't follow Benford's Law.
    """
    coerced, is_valid = coerce_to_numeric(series)
    if not is_valid:
        return False

    valid = coerced.dropna()
    if len(valid) == 0:
        return False

    min_val = valid.min()
    max_val = valid.max()

    # Check if values fall within expected bounds with some tolerance
    return (min_val >= lower - 0.1) and (max_val <= upper + 0.1)


def get_semantic_type_hint(col_name: str, series: pd.Series) -> str:
    """
    Get a generic semantic type hint based on column characteristics.

    Returns hints like 'identifier', 'binary', 'percentage', 'amount', etc.
    NO hardcoded dataset-specific logic.
    """
    col_lower = col_name.lower()

    # Check for identifier patterns
    if is_identifier_like(col_name, series):
        return 'identifier'

    # Check for binary
    if is_binary_column(series):
        if col_lower.startswith(('is_', 'has_', 'was_', 'can_')):
            return 'binary_flag'
        return 'binary'

    # Check for bounded percentage-like
    if is_bounded_data(series, 0, 100) or is_bounded_data(series, 0, 1):
        if 'pct' in col_lower or 'percent' in col_lower or 'rate' in col_lower:
            return 'percentage'
        return 'bounded'

    # Check for amount-like keywords
    amount_keywords = ['amount', 'price', 'cost', 'fee', 'balance', 'total', 'sum',
                       'value', 'payment', 'revenue', 'income', 'salary', 'wage']
    if any(kw in col_lower for kw in amount_keywords):
        return 'amount'

    # Check for count-like
    count_keywords = ['count', 'num', 'qty', 'quantity', 'number']
    if any(kw in col_lower for kw in count_keywords):
        return 'count'

    return 'general'
