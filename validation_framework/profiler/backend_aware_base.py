"""
Backend-Aware Base Class - Unified DataFrame Operations for Pandas and Polars.

This module provides a consistent API for DataFrame operations regardless of
whether the underlying data is stored in pandas or Polars. This abstraction
enables the profiler to leverage Polars' performance benefits when available
while maintaining pandas compatibility.

Architecture:
    BackendAwareProfiler serves as a mixin/base class providing:
    1. Backend detection (is_polars, is_pandas, get_backend_name)
    2. Column operations (get_columns, has_column, get_column_dtype)
    3. Null handling (get_null_mask, get_null_count, drop_nulls)
    4. Type checking (is_numeric_column, is_string_column)
    5. Value operations (get_value_counts, series_to_list)
    6. Statistical methods (min, max, mean, std, median, percentile)
    7. String operations (string_length, string_contains, string_match)

Design Decisions:
    - Polars is optional: Falls back gracefully to pandas if not installed
    - Memory safety: series_to_list() has a default_limit=100000 to prevent
      unbounded memory allocation (a common source of memory leaks)
    - Type consistency: All methods return Python native types where possible
    - Vectorized operations: Leverages each backend's vectorization for 5-100x
      performance over row-by-row iteration

Performance Notes:
    Polars provides significant advantages for:
    - Large datasets (>100K rows): 2-10x faster than pandas
    - String operations: Optimized UTF-8 handling
    - Memory usage: Column-oriented storage is more cache-friendly

Usage:
    class MyProfilerComponent(BackendAwareProfiler):
        def analyze(self, df):
            if self.is_polars(df):
                # Use Polars-specific optimizations
                pass

            # Or use backend-agnostic methods:
            null_count = self.get_null_count(df[column])
            values = self.series_to_list(df[column], limit=1000)
"""

import pandas as pd
from typing import Any, List, Dict, Optional, Union
import numpy as np

# Import Polars if available
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None


DataFrame = Union[pd.DataFrame, 'pl.DataFrame'] if HAS_POLARS else pd.DataFrame
Series = Union[pd.Series, 'pl.Series'] if HAS_POLARS else pd.Series


class BackendAwareProfiler:
    """
    Backend-agnostic base class for profiler components.

    Provides a unified API for DataFrame operations that works seamlessly with
    both pandas and Polars backends. Inherit from this class to write profiler
    components that automatically benefit from Polars' performance when available.

    All methods detect the backend at runtime and dispatch to the appropriate
    implementation. This design allows gradual migration to Polars without
    breaking existing pandas-based code.

    Example:
        >>> class MyAnalyzer(BackendAwareProfiler):
        ...     def analyze_column(self, df, column):
        ...         series = self.get_column_series(df, column)
        ...         return {
        ...             'null_count': self.get_null_count(series),
        ...             'unique_count': self.get_unique_count(series),
        ...             'values': self.series_to_list(series, limit=100)
        ...         }
    """

    def is_polars(self, df: Any) -> bool:
        """Check if DataFrame is Polars."""
        if not HAS_POLARS:
            return False
        return isinstance(df, (pl.DataFrame, pl.LazyFrame, pl.Series))

    def is_pandas(self, df: Any) -> bool:
        """Check if DataFrame is pandas."""
        return isinstance(df, (pd.DataFrame, pd.Series))

    def get_backend_name(self, df: Any) -> str:
        """Get backend name ('polars' or 'pandas')."""
        return 'polars' if self.is_polars(df) else 'pandas'

    def get_columns(self, df: DataFrame) -> List[str]:
        """Get column names (backend-agnostic)."""
        if self.is_polars(df):
            return df.columns
        else:
            return df.columns.tolist()

    def has_column(self, df: DataFrame, column: str) -> bool:
        """Check if column exists (backend-agnostic)."""
        return column in self.get_columns(df)

    def get_row_count(self, df: DataFrame) -> int:
        """Get row count (backend-agnostic)."""
        if self.is_polars(df):
            return df.height if hasattr(df, 'height') else len(df)
        else:
            return len(df)

    def get_column_dtype(self, df: DataFrame, column: str) -> str:
        """Get column data type as string (backend-agnostic)."""
        if self.is_polars(df):
            dtype = df.schema[column] if hasattr(df, 'schema') else df[column].dtype
            return str(dtype)
        else:
            return str(df[column].dtype)

    def is_numeric_column(self, df: DataFrame, column: str) -> bool:
        """Check if column is numeric (backend-agnostic)."""
        dtype_str = self.get_column_dtype(df, column).lower()

        if self.is_polars(df):
            return any(t in dtype_str for t in ['int', 'float', 'decimal'])
        else:
            return any(t in dtype_str for t in ['int', 'float', 'number'])

    def is_string_column(self, df: DataFrame, column: str) -> bool:
        """Check if column is string/text (backend-agnostic)."""
        dtype_str = self.get_column_dtype(df, column).lower()

        if self.is_polars(df):
            return 'utf8' in dtype_str or 'str' in dtype_str
        else:
            return 'object' in dtype_str or 'string' in dtype_str

    def get_null_mask(self, df: DataFrame, column: str):
        """Get boolean mask for null values (backend-agnostic)."""
        if self.is_polars(df):
            return df[column].is_null()
        else:
            return df[column].isna()

    def get_not_null_mask(self, df: DataFrame, column: str):
        """Get boolean mask for non-null values (backend-agnostic)."""
        if self.is_polars(df):
            return df[column].is_not_null()
        else:
            return df[column].notna()

    def filter_df(self, df: DataFrame, mask):
        """Filter DataFrame by boolean mask (backend-agnostic)."""
        if self.is_polars(df):
            return df.filter(mask)
        else:
            return df[mask]

    def get_column_series(self, df: DataFrame, column: str) -> Series:
        """Get column as Series (backend-agnostic)."""
        return df[column]

    def get_unique_count(self, series: Series) -> int:
        """Get count of unique values (backend-agnostic)."""
        if self.is_polars(series):
            return series.n_unique()
        else:
            return series.nunique()

    def get_null_count(self, series: Series) -> int:
        """Get count of null values (backend-agnostic)."""
        if self.is_polars(series):
            return series.null_count()
        else:
            return series.isna().sum()

    def get_value_counts(self, series: Series, limit: Optional[int] = None) -> Dict[Any, int]:
        """Get value counts (backend-agnostic)."""
        if self.is_polars(series):
            vc = series.value_counts()
            # Polars returns DataFrame with columns, convert to dict
            if limit:
                vc = vc.head(limit)
            return {row[0]: row[1] for row in vc.iter_rows()}
        else:
            vc = series.value_counts()
            if limit:
                vc = vc.head(limit)
            return vc.to_dict()

    def series_to_list(self, series: Series, limit: Optional[int] = None, default_limit: int = 100000) -> List[Any]:
        """Convert Series to list (backend-agnostic).

        Args:
            series: The series to convert
            limit: Optional explicit limit on items to return
            default_limit: Safety limit if no limit specified (prevents memory leaks).
                          Set to None to disable safety limit (use with caution).
        """
        # Apply safety limit if no explicit limit provided
        effective_limit = limit if limit is not None else default_limit

        if self.is_polars(series):
            if effective_limit:
                return series.head(effective_limit).to_list()
            return series.to_list()
        else:
            if effective_limit:
                return series.head(effective_limit).tolist()
            return series.tolist()

    def get_column_min(self, series: Series):
        """Get minimum value (backend-agnostic)."""
        return series.min()

    def get_column_max(self, series: Series):
        """Get maximum value (backend-agnostic)."""
        return series.max()

    def get_column_mean(self, series: Series) -> float:
        """Get mean value (backend-agnostic)."""
        result = series.mean()
        return float(result) if result is not None else None

    def get_column_std(self, series: Series) -> float:
        """Get standard deviation (backend-agnostic)."""
        result = series.std()
        return float(result) if result is not None else None

    def get_column_median(self, series: Series):
        """Get median value (backend-agnostic)."""
        if self.is_polars(series):
            return series.median()
        else:
            return series.median()

    def get_percentile(self, series: Series, percentile: float):
        """Get percentile value (backend-agnostic)."""
        if self.is_polars(series):
            return series.quantile(percentile / 100.0)
        else:
            return series.quantile(percentile / 100.0)

    def cast_to_numeric(self, series: Series, strict: bool = False):
        """Cast series to numeric (backend-agnostic)."""
        if self.is_polars(series):
            try:
                return series.cast(pl.Float64, strict=strict)
            except:
                return None
        else:
            return pd.to_numeric(series, errors='raise' if strict else 'coerce')

    def string_length(self, series: Series):
        """Get string lengths vectorized (backend-agnostic)."""
        if self.is_polars(series):
            return series.str.len_chars()
        else:
            return series.str.len()

    def string_contains(self, series: Series, pattern: str, regex: bool = True):
        """Check if strings contain pattern (backend-agnostic)."""
        if self.is_polars(series):
            return series.str.contains(pattern)
        else:
            return series.str.contains(pattern, regex=regex, na=False)

    def string_match(self, series: Series, pattern: str):
        """Check if strings match pattern (backend-agnostic)."""
        if self.is_polars(series):
            # Polars uses contains for regex matching
            return series.str.contains(f"^{pattern}$")
        else:
            return series.str.match(pattern, na=False)

    def to_pandas(self, df: DataFrame) -> pd.DataFrame:
        """Convert to pandas DataFrame if needed."""
        if self.is_polars(df):
            return df.to_pandas()
        return df

    def to_numpy(self, series: Series) -> np.ndarray:
        """Convert Series to numpy array (backend-agnostic)."""
        if self.is_polars(series):
            return series.to_numpy()
        else:
            return series.values

    def drop_nulls(self, series: Series):
        """Drop null values from series (backend-agnostic)."""
        if self.is_polars(series):
            return series.drop_nulls()
        else:
            return series.dropna()

    def sample(self, df: DataFrame, n: int, seed: Optional[int] = None):
        """Sample n rows (backend-agnostic)."""
        if self.is_polars(df):
            if seed is not None:
                return df.sample(n=n, seed=seed)
            return df.sample(n=n)
        else:
            if seed is not None:
                return df.sample(n=n, random_state=seed)
            return df.sample(n=n)
