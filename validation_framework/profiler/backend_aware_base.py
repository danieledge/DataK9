"""
Backend-aware base class for profiler components.

Provides helper methods for working with both pandas and Polars DataFrames,
enabling profiler components to work seamlessly with either backend.
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
    Base class providing backend-agnostic operations for profiler components.

    Supports both pandas and Polars DataFrames with consistent API.
    Vectorized operations provide 5-100x performance improvement over row iteration.
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

    def series_to_list(self, series: Series, limit: Optional[int] = None) -> List[Any]:
        """Convert Series to list (backend-agnostic)."""
        if self.is_polars(series):
            if limit:
                return series.head(limit).to_list()
            return series.to_list()
        else:
            if limit:
                return series.head(limit).tolist()
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
