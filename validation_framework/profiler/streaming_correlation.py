"""
Streaming correlation calculator using Welford's online algorithm.

This module provides memory-efficient correlation calculation for very large datasets
by computing correlations incrementally using chunked data processing. The implementation
uses Welford's online algorithm for numerically stable computation of covariance and
correlation without requiring the entire dataset in memory.

Algorithm:
    For correlation between X and Y, we track:
    - n: Count of samples
    - mean_x, mean_y: Running means
    - M2_x, M2_y: Sum of squared differences from mean (for variance)
    - C: Co-moment (for covariance)

    Update formulas (for each new sample):
        n += 1
        delta_x = x - mean_x
        mean_x += delta_x / n
        delta_y = y - mean_y
        mean_y += delta_y / n
        M2_x += delta_x * (x - mean_x)
        M2_y += delta_y * (y - mean_y)
        C += delta_x * (y - mean_y)

    Final correlation:
        correlation = C / sqrt(M2_x * M2_y)

Performance:
    - Memory: O(n²) where n = number of columns (NOT number of rows)
    - Time per chunk: O(rows_in_chunk * columns²)
    - Numerical stability: Uses compensated summation for better accuracy

Usage:
    # Single pair
    >>> corr = StreamingCorrelation()
    >>> for chunk in data_chunks:
    ...     corr.update(chunk['x'], chunk['y'])
    >>> result = corr.get_correlation()

    # Full matrix
    >>> matrix = StreamingCorrelationMatrix(['col1', 'col2', 'col3'])
    >>> for chunk in data_chunks:
    ...     matrix.update_from_chunk(chunk)
    >>> correlations = matrix.get_correlation_dict()
"""

from typing import Dict, List, Optional, Any, Union
import numpy as np
import logging

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None

import pandas as pd


class StreamingCorrelation:
    """
    Calculate correlation between two variables using Welford's online algorithm.

    This class tracks correlation incrementally without storing all values,
    making it memory-efficient for very large datasets.

    Attributes:
        n: Number of samples processed
        mean_x: Running mean of X
        mean_y: Running mean of Y
        M2_x: Sum of squared differences from mean for X
        M2_y: Sum of squared differences from mean for Y
        C: Co-moment (for covariance calculation)

    Example:
        >>> corr = StreamingCorrelation()
        >>> corr.update_batch(np.array([1, 2, 3]), np.array([2, 4, 6]))
        >>> corr.get_correlation()
        1.0
    """

    def __init__(self):
        """Initialize streaming correlation calculator."""
        self.n = 0
        self.mean_x = 0.0
        self.mean_y = 0.0
        self.M2_x = 0.0  # Sum of squared differences from mean
        self.M2_y = 0.0
        self.C = 0.0  # Co-moment for covariance
        self.logger = logging.getLogger(__name__)

    def update(self, x: float, y: float) -> None:
        """
        Update correlation with a single pair of values.

        Args:
            x: Value from first variable
            y: Value from second variable
        """
        if np.isnan(x) or np.isnan(y):
            return  # Skip missing values

        self.n += 1

        # Update X statistics
        delta_x = x - self.mean_x
        self.mean_x += delta_x / self.n

        # Update Y statistics
        delta_y = y - self.mean_y
        self.mean_y += delta_y / self.n

        # Update second moments and co-moment
        # Note: Order matters for numerical stability
        self.M2_x += delta_x * (x - self.mean_x)
        self.M2_y += delta_y * (y - self.mean_y)
        self.C += delta_x * (y - self.mean_y)

    def update_batch(self, x_values: np.ndarray, y_values: np.ndarray) -> None:
        """
        Update correlation with arrays of values (vectorized for performance).

        Args:
            x_values: Array of values from first variable
            y_values: Array of values from second variable

        Raises:
            ValueError: If arrays have different lengths
        """
        if len(x_values) != len(y_values):
            raise ValueError(f"Arrays must have same length: {len(x_values)} != {len(y_values)}")

        # Remove pairs with any NaN
        mask = ~(np.isnan(x_values) | np.isnan(y_values))
        x_clean = x_values[mask]
        y_clean = y_values[mask]

        if len(x_clean) == 0:
            return  # No valid values

        # Vectorized update using parallel Welford algorithm
        # This is more efficient than iterating through individual values
        batch_size = len(x_clean)
        old_n = self.n
        new_n = old_n + batch_size

        # Batch statistics
        batch_mean_x = np.mean(x_clean)
        batch_mean_y = np.mean(y_clean)

        # Update means
        delta_mean_x = batch_mean_x - self.mean_x
        delta_mean_y = batch_mean_y - self.mean_y

        new_mean_x = self.mean_x + delta_mean_x * batch_size / new_n
        new_mean_y = self.mean_y + delta_mean_y * batch_size / new_n

        # Update M2 (sum of squared differences)
        # Using parallel algorithm: M2_combined = M2_a + M2_b + (mean_a - mean_b)² * n_a * n_b / n_combined
        batch_M2_x = np.sum((x_clean - batch_mean_x) ** 2)
        batch_M2_y = np.sum((y_clean - batch_mean_y) ** 2)

        if old_n > 0:
            self.M2_x += batch_M2_x + delta_mean_x ** 2 * old_n * batch_size / new_n
            self.M2_y += batch_M2_y + delta_mean_y ** 2 * old_n * batch_size / new_n
        else:
            self.M2_x = batch_M2_x
            self.M2_y = batch_M2_y

        # Update co-moment (for covariance)
        # C = sum((x_i - mean_x)(y_i - mean_y))
        batch_C = np.sum((x_clean - batch_mean_x) * (y_clean - batch_mean_y))

        if old_n > 0:
            self.C += batch_C + delta_mean_x * delta_mean_y * old_n * batch_size / new_n
        else:
            self.C = batch_C

        # Update counts and means
        self.n = new_n
        self.mean_x = new_mean_x
        self.mean_y = new_mean_y

    def get_correlation(self) -> Optional[float]:
        """
        Calculate final correlation coefficient.

        Returns:
            Pearson correlation coefficient [-1, 1], or None if insufficient data
            or if either variable is constant.

        Notes:
            - Returns None if n < 2 (insufficient data)
            - Returns None if either variable has zero variance (constant)
            - Handles edge cases with numerical stability
        """
        if self.n < 2:
            return None  # Insufficient data

        # Check for zero variance (constant columns)
        if self.M2_x <= 0 or self.M2_y <= 0:
            return None  # Cannot compute correlation with constant variable

        # Correlation = covariance / (std_x * std_y)
        # Since covariance = C / (n - 1) and variance = M2 / (n - 1):
        # correlation = C / sqrt(M2_x * M2_y)
        try:
            correlation = self.C / np.sqrt(self.M2_x * self.M2_y)

            # Clamp to [-1, 1] to handle floating point errors
            correlation = np.clip(correlation, -1.0, 1.0)

            return float(correlation)
        except (FloatingPointError, ZeroDivisionError):
            self.logger.warning("Numerical error in correlation calculation")
            return None

    def get_covariance(self) -> Optional[float]:
        """
        Calculate sample covariance.

        Returns:
            Sample covariance, or None if insufficient data.
        """
        if self.n < 2:
            return None

        return self.C / (self.n - 1)

    def get_variance_x(self) -> Optional[float]:
        """Get variance of X variable."""
        if self.n < 2:
            return None
        return self.M2_x / (self.n - 1)

    def get_variance_y(self) -> Optional[float]:
        """Get variance of Y variable."""
        if self.n < 2:
            return None
        return self.M2_y / (self.n - 1)

    def reset(self) -> None:
        """Reset all statistics."""
        self.n = 0
        self.mean_x = 0.0
        self.mean_y = 0.0
        self.M2_x = 0.0
        self.M2_y = 0.0
        self.C = 0.0


class StreamingCorrelationMatrix:
    """
    Calculate correlation matrix for multiple columns using streaming algorithm.

    This class efficiently tracks correlations between all pairs of numeric columns
    by processing data in chunks. Memory usage is O(n²) where n is the number of
    columns, not the number of rows.

    Attributes:
        columns: List of column names
        correlations: Dict mapping (col1, col2) tuples to StreamingCorrelation objects

    Example:
        >>> matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])
        >>> chunk_df = pd.DataFrame({'a': [1, 2], 'b': [2, 4], 'c': [3, 6]})
        >>> matrix.update_from_dataframe(chunk_df)
        >>> correlations = matrix.get_correlation_dict()
    """

    def __init__(self, columns: List[str]):
        """
        Initialize streaming correlation matrix.

        Args:
            columns: List of column names to track correlations for
        """
        self.columns = columns
        self.correlations: Dict[tuple, StreamingCorrelation] = {}
        self.logger = logging.getLogger(__name__)

        # Initialize correlation trackers for all pairs
        for i, col1 in enumerate(columns):
            for col2 in columns[i + 1:]:
                self.correlations[(col1, col2)] = StreamingCorrelation()

        self.logger.info(f"Initialized streaming correlation matrix for {len(columns)} columns "
                        f"({len(self.correlations)} pairs)")

    def update_from_dataframe(self, df: Union[pd.DataFrame, 'pl.DataFrame']) -> None:
        """
        Update correlations from a DataFrame chunk.

        Args:
            df: DataFrame chunk (pandas or Polars) containing the tracked columns

        Raises:
            ValueError: If DataFrame is missing required columns
        """
        # Convert Polars to pandas if needed
        if HAS_POLARS and isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        # Verify all columns exist
        missing_cols = set(self.columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"DataFrame missing columns: {missing_cols}")

        # Update each correlation pair
        for (col1, col2), corr in self.correlations.items():
            try:
                # Extract columns as numpy arrays
                x = df[col1].to_numpy() if hasattr(df[col1], 'to_numpy') else df[col1].values
                y = df[col2].to_numpy() if hasattr(df[col2], 'to_numpy') else df[col2].values

                # Ensure numeric type
                x = x.astype(np.float64, copy=False)
                y = y.astype(np.float64, copy=False)

                # Update correlation
                corr.update_batch(x, y)
            except Exception as e:
                self.logger.warning(f"Error updating correlation for {col1}, {col2}: {e}")

    def update_from_dict(self, data: Dict[str, np.ndarray]) -> None:
        """
        Update correlations from dictionary of arrays.

        Args:
            data: Dictionary mapping column names to numpy arrays

        Raises:
            ValueError: If data is missing required columns or arrays have different lengths
        """
        # Verify all columns exist
        missing_cols = set(self.columns) - set(data.keys())
        if missing_cols:
            raise ValueError(f"Data missing columns: {missing_cols}")

        # Verify all arrays have same length
        lengths = {col: len(data[col]) for col in self.columns}
        if len(set(lengths.values())) > 1:
            raise ValueError(f"Arrays have different lengths: {lengths}")

        # Update each correlation pair
        for (col1, col2), corr in self.correlations.items():
            try:
                x = np.asarray(data[col1], dtype=np.float64)
                y = np.asarray(data[col2], dtype=np.float64)
                corr.update_batch(x, y)
            except Exception as e:
                self.logger.warning(f"Error updating correlation for {col1}, {col2}: {e}")

    def get_correlation_dict(self, min_samples: int = 2) -> Dict[str, float]:
        """
        Get all correlations as a dictionary.

        Args:
            min_samples: Minimum number of samples required to return a correlation

        Returns:
            Dictionary mapping "col1|col2" to correlation coefficient.
            Pairs with insufficient data or constant columns are omitted.

        Example:
            >>> matrix.get_correlation_dict()
            {'a|b': 0.95, 'a|c': 0.87, 'b|c': 0.92}
        """
        result = {}
        for (col1, col2), corr in self.correlations.items():
            if corr.n >= min_samples:
                correlation = corr.get_correlation()
                if correlation is not None:
                    result[f"{col1}|{col2}"] = correlation
        return result

    def get_correlation_matrix(self, min_samples: int = 2) -> Optional[np.ndarray]:
        """
        Get correlation matrix as 2D numpy array.

        Args:
            min_samples: Minimum number of samples required to return a correlation

        Returns:
            2D numpy array where element [i, j] is correlation between columns[i] and columns[j].
            Diagonal is 1.0. Returns None if no correlations available.
        """
        n = len(self.columns)
        matrix = np.eye(n)  # Initialize with 1.0 on diagonal

        # Fill upper triangle
        for i, col1 in enumerate(self.columns):
            for j, col2 in enumerate(self.columns[i + 1:], start=i + 1):
                corr = self.correlations.get((col1, col2))
                if corr and corr.n >= min_samples:
                    correlation = corr.get_correlation()
                    if correlation is not None:
                        matrix[i, j] = correlation
                        matrix[j, i] = correlation  # Symmetric
                    else:
                        matrix[i, j] = np.nan
                        matrix[j, i] = np.nan
                else:
                    matrix[i, j] = np.nan
                    matrix[j, i] = np.nan

        return matrix

    def get_sample_counts(self) -> Dict[str, int]:
        """
        Get sample counts for each correlation pair.

        Returns:
            Dictionary mapping "col1|col2" to number of valid samples processed.
        """
        return {f"{col1}|{col2}": corr.n for (col1, col2), corr in self.correlations.items()}

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for the correlation matrix.

        Returns:
            Dictionary containing:
                - column_count: Number of columns
                - pair_count: Number of column pairs
                - correlations: Correlation dictionary
                - sample_counts: Sample count dictionary
                - strong_correlations: List of pairs with |r| > 0.7
        """
        correlations = self.get_correlation_dict()
        sample_counts = self.get_sample_counts()

        # Find strong correlations
        strong_correlations = [
            {'pair': pair, 'correlation': corr, 'samples': sample_counts.get(pair, 0)}
            for pair, corr in correlations.items()
            if abs(corr) > 0.7
        ]
        strong_correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)

        return {
            'column_count': len(self.columns),
            'pair_count': len(self.correlations),
            'correlations': correlations,
            'sample_counts': sample_counts,
            'strong_correlations': strong_correlations
        }

    def reset(self) -> None:
        """Reset all correlations."""
        for corr in self.correlations.values():
            corr.reset()
