"""
Statistical outlier detection using Z-score and IQR methods.

This module provides unified outlier detection functions that can be used
by both the profiler and validator to ensure consistent behavior.

Methods:
    - Z-score: Identifies values that deviate significantly from the mean
    - IQR (Interquartile Range): Identifies values outside Q1-1.5*IQR to Q3+1.5*IQR

Usage:
    from validation_framework.shared_analysis import detect_outliers

    # Detect outliers using default settings (combined Z-score and IQR)
    results = detect_outliers(df['column'])

    # Use specific method
    z_outliers = detect_outliers_zscore(df['column'], threshold=3.0)
    iqr_outliers = detect_outliers_iqr(df['column'], multiplier=1.5)
"""

import numpy as np
from typing import Union, List, Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


def detect_outliers_zscore(
    series: Any,
    threshold: float = 3.0,
    return_mask: bool = False
) -> Union[int, Tuple[int, Any]]:
    """
    Detect outliers using Z-score method.

    A Z-score measures how many standard deviations a value is from the mean.
    Values with |Z| > threshold are considered outliers.

    Args:
        series: Pandas Series, Polars Series, or numpy array of numeric values
        threshold: Z-score threshold (default 3.0, meaning 3 standard deviations)
        return_mask: If True, also return boolean mask of outliers

    Returns:
        If return_mask=False: Count of outliers
        If return_mask=True: Tuple of (count, boolean mask)

    Example:
        >>> outlier_count = detect_outliers_zscore(df['value'], threshold=3.0)
        >>> count, mask = detect_outliers_zscore(df['value'], return_mask=True)
    """
    try:
        # Convert to numpy array, handling different backends
        if hasattr(series, 'to_numpy'):
            # Pandas
            values = series.dropna().to_numpy().astype(float)
        elif hasattr(series, 'to_list'):
            # Polars
            values = np.array([v for v in series.to_list() if v is not None], dtype=float)
        else:
            # Assume numpy array or list
            values = np.array([v for v in series if v is not None], dtype=float)

        if len(values) < 3:
            if return_mask:
                return 0, np.zeros(len(values), dtype=bool)
            return 0

        # Calculate mean and std
        mean = np.mean(values)
        std = np.std(values)

        if std == 0:
            # No variance, no outliers
            if return_mask:
                return 0, np.zeros(len(values), dtype=bool)
            return 0

        # Calculate Z-scores
        z_scores = np.abs((values - mean) / std)

        # Identify outliers
        outlier_mask = z_scores > threshold
        outlier_count = int(np.sum(outlier_mask))

        if return_mask:
            return outlier_count, outlier_mask
        return outlier_count

    except Exception as e:
        logger.debug(f"Z-score outlier detection error: {e}")
        if return_mask:
            return 0, np.array([])
        return 0


def detect_outliers_iqr(
    series: Any,
    multiplier: float = 1.5,
    return_mask: bool = False,
    return_bounds: bool = False
) -> Union[int, Tuple[int, Any], Dict[str, Any]]:
    """
    Detect outliers using IQR (Interquartile Range) method.

    Values outside [Q1 - multiplier*IQR, Q3 + multiplier*IQR] are outliers.

    Args:
        series: Pandas Series, Polars Series, or numpy array of numeric values
        multiplier: IQR multiplier (default 1.5, use 3.0 for extreme outliers)
        return_mask: If True, also return boolean mask of outliers
        return_bounds: If True, return dict with bounds and statistics

    Returns:
        If return_bounds=True: Dict with count, lower_bound, upper_bound, Q1, Q3, IQR
        If return_mask=True: Tuple of (count, boolean mask)
        Otherwise: Count of outliers

    Example:
        >>> outlier_count = detect_outliers_iqr(df['value'])
        >>> stats = detect_outliers_iqr(df['value'], return_bounds=True)
    """
    try:
        # Convert to numpy array, handling different backends
        if hasattr(series, 'to_numpy'):
            # Pandas
            values = series.dropna().to_numpy().astype(float)
        elif hasattr(series, 'to_list'):
            # Polars
            values = np.array([v for v in series.to_list() if v is not None], dtype=float)
        else:
            # Assume numpy array or list
            values = np.array([v for v in series if v is not None], dtype=float)

        if len(values) < 4:
            if return_bounds:
                return {'count': 0, 'lower_bound': None, 'upper_bound': None}
            if return_mask:
                return 0, np.zeros(len(values), dtype=bool)
            return 0

        # Calculate quartiles
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1

        if iqr == 0:
            # No spread, no outliers by IQR definition
            if return_bounds:
                return {'count': 0, 'lower_bound': q1, 'upper_bound': q3, 'Q1': q1, 'Q3': q3, 'IQR': 0}
            if return_mask:
                return 0, np.zeros(len(values), dtype=bool)
            return 0

        # Calculate bounds
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr

        # Identify outliers
        outlier_mask = (values < lower_bound) | (values > upper_bound)
        outlier_count = int(np.sum(outlier_mask))

        if return_bounds:
            return {
                'count': outlier_count,
                'lower_bound': float(lower_bound),
                'upper_bound': float(upper_bound),
                'Q1': float(q1),
                'Q3': float(q3),
                'IQR': float(iqr),
                'lower_outliers': int(np.sum(values < lower_bound)),
                'upper_outliers': int(np.sum(values > upper_bound))
            }

        if return_mask:
            return outlier_count, outlier_mask
        return outlier_count

    except Exception as e:
        logger.debug(f"IQR outlier detection error: {e}")
        if return_bounds:
            return {'count': 0, 'lower_bound': None, 'upper_bound': None, 'error': str(e)}
        if return_mask:
            return 0, np.array([])
        return 0


def detect_outliers(
    series: Any,
    method: str = 'combined',
    z_threshold: float = 3.0,
    iqr_multiplier: float = 1.5,
    return_details: bool = False
) -> Union[int, Dict[str, Any]]:
    """
    Detect outliers using specified method or combined approach.

    Args:
        series: Pandas Series, Polars Series, or numpy array of numeric values
        method: Detection method - 'zscore', 'iqr', or 'combined' (default)
        z_threshold: Z-score threshold (default 3.0)
        iqr_multiplier: IQR multiplier (default 1.5)
        return_details: If True, return detailed statistics

    Returns:
        If return_details=False: Count of outliers
        If return_details=True: Dict with counts and method-specific statistics

    Example:
        >>> outliers = detect_outliers(df['value'])
        >>> details = detect_outliers(df['value'], return_details=True)
    """
    results = {
        'count': 0,
        'method': method,
        'zscore': None,
        'iqr': None
    }

    try:
        if method in ('zscore', 'combined'):
            z_count, z_mask = detect_outliers_zscore(series, threshold=z_threshold, return_mask=True)
            results['zscore'] = {
                'count': z_count,
                'threshold': z_threshold
            }

        if method in ('iqr', 'combined'):
            iqr_results = detect_outliers_iqr(series, multiplier=iqr_multiplier, return_bounds=True)
            results['iqr'] = iqr_results

        # Determine final count based on method
        if method == 'zscore':
            results['count'] = results['zscore']['count']
        elif method == 'iqr':
            results['count'] = results['iqr']['count']
        elif method == 'combined':
            # Combined: Use union of both methods (more conservative)
            # Or intersection (more strict) - we use intersection for fewer false positives
            z_count = results['zscore']['count'] if results['zscore'] else 0
            iqr_count = results['iqr']['count'] if results['iqr'] else 0
            # Use the smaller count (intersection heuristic)
            results['count'] = min(z_count, iqr_count)

        if return_details:
            return results
        return results['count']

    except Exception as e:
        logger.debug(f"Outlier detection error: {e}")
        if return_details:
            results['error'] = str(e)
            return results
        return 0


def get_outlier_values(
    series: Any,
    method: str = 'iqr',
    z_threshold: float = 3.0,
    iqr_multiplier: float = 1.5,
    max_samples: int = 100
) -> List[Any]:
    """
    Get sample outlier values from a series.

    Args:
        series: Pandas Series, Polars Series, or numpy array
        method: Detection method ('zscore' or 'iqr')
        z_threshold: Z-score threshold
        iqr_multiplier: IQR multiplier
        max_samples: Maximum number of outlier samples to return

    Returns:
        List of outlier values (up to max_samples)
    """
    try:
        # Get outlier mask
        if method == 'zscore':
            _, mask = detect_outliers_zscore(series, threshold=z_threshold, return_mask=True)
        else:
            _, mask = detect_outliers_iqr(series, multiplier=iqr_multiplier, return_mask=True)

        if len(mask) == 0:
            return []

        # Convert to numpy for indexing
        if hasattr(series, 'to_numpy'):
            values = series.dropna().to_numpy()
        elif hasattr(series, 'to_list'):
            values = np.array([v for v in series.to_list() if v is not None])
        else:
            values = np.array([v for v in series if v is not None])

        # Get outlier values
        outlier_values = values[mask]

        # Return up to max_samples
        if len(outlier_values) > max_samples:
            # Return diverse sample (evenly spaced)
            indices = np.linspace(0, len(outlier_values) - 1, max_samples, dtype=int)
            outlier_values = outlier_values[indices]

        return outlier_values.tolist()

    except Exception as e:
        logger.debug(f"Error getting outlier values: {e}")
        return []
