"""
Polars compatibility utilities for seamless pandas/Polars interop.

This module provides helper functions and decorators to make validation rules
work transparently with both pandas and Polars backends without code duplication.

Key features:
- Automatic backend detection from DataFrame type
- Unified API for common operations
- Zero-copy conversions where possible
- Performance metrics tracking
"""

from typing import Any, Callable, Union, Optional, Iterator
import logging
from functools import wraps
from validation_framework.core.backend import (
    HAS_PANDAS,
    HAS_POLARS,
    DataFrameBackend,
    DataFrameAdapter,
    BackendManager
)

logger = logging.getLogger(__name__)

if HAS_PANDAS:
    import pandas as pd

if HAS_POLARS:
    import polars as pl


def detect_backend(df: Any) -> DataFrameBackend:
    """
    Detect which backend a DataFrame is using.

    Args:
        df: DataFrame object (pandas or Polars)

    Returns:
        DataFrameBackend enum value

    Raises:
        TypeError: If df is not a recognized DataFrame type
    """
    if HAS_POLARS and isinstance(df, pl.DataFrame):
        return DataFrameBackend.POLARS
    elif HAS_PANDAS and isinstance(df, pd.DataFrame):
        return DataFrameBackend.PANDAS
    else:
        raise TypeError(
            f"Unrecognized DataFrame type: {type(df)}. "
            "Expected pandas.DataFrame or polars.DataFrame"
        )


def to_polars(df: Any) -> 'pl.DataFrame':
    """
    Convert any DataFrame to Polars DataFrame.

    Args:
        df: DataFrame (pandas or Polars)

    Returns:
        Polars DataFrame

    Raises:
        RuntimeError: If Polars is not installed
    """
    if not HAS_POLARS:
        raise RuntimeError(
            "Polars not installed. Install with: pip install polars-lts-cpu"
        )

    if isinstance(df, pl.DataFrame):
        return df

    # Convert pandas to Polars
    return pl.from_pandas(df)


def to_pandas(df: Any) -> 'pd.DataFrame':
    """
    Convert any DataFrame to pandas DataFrame.

    Args:
        df: DataFrame (pandas or Polars)

    Returns:
        pandas DataFrame

    Raises:
        RuntimeError: If pandas is not installed
    """
    if not HAS_PANDAS:
        raise RuntimeError(
            "Pandas not installed. Install with: pip install pandas"
        )

    if isinstance(df, pd.DataFrame):
        return df

    # Convert Polars to pandas
    return df.to_pandas()


class UnifiedDataFrame:
    """
    Wrapper that provides a unified API for both pandas and Polars DataFrames.

    This class allows validation code to be written once and work with both backends.
    It delegates operations to the appropriate backend implementation.

    Usage:
        >>> udf = UnifiedDataFrame(df)
        >>> null_mask = udf.is_null('column_name')
        >>> filtered = udf.filter(null_mask)
        >>> count = udf.row_count()
    """

    def __init__(self, df: Any):
        """
        Initialize unified DataFrame wrapper.

        Args:
            df: pandas or Polars DataFrame
        """
        self.df = df
        self.backend = detect_backend(df)
        self.adapter = DataFrameAdapter(self.backend)

    def is_null(self, column: str) -> Any:
        """
        Get null/NA mask for a column.

        Args:
            column: Column name

        Returns:
            Boolean mask/series
        """
        return self.adapter.is_null(self.df, column)

    def filter(self, mask: Any) -> Any:
        """
        Filter rows by boolean mask.

        Args:
            mask: Boolean mask/series

        Returns:
            Filtered DataFrame
        """
        return self.adapter.filter_rows(self.df, mask)

    def columns(self) -> list:
        """
        Get list of column names.

        Returns:
            List of column names
        """
        return self.adapter.get_column_names(self.df)

    def row_count(self) -> int:
        """
        Get number of rows.

        Returns:
            Row count
        """
        return self.adapter.row_count(self.df)

    def column_count(self) -> int:
        """
        Get number of columns.

        Returns:
            Column count
        """
        return self.adapter.column_count(self.df)

    def select(self, columns: list) -> Any:
        """
        Select specific columns.

        Args:
            columns: List of column names to select

        Returns:
            DataFrame with selected columns
        """
        if self.backend == DataFrameBackend.POLARS:
            return self.df.select(columns)
        else:
            return self.df[columns]

    def group_by(self, columns: Union[str, list]) -> Any:
        """
        Group by columns.

        Args:
            columns: Column name or list of column names

        Returns:
            GroupBy object (backend-specific)
        """
        if isinstance(columns, str):
            columns = [columns]

        if self.backend == DataFrameBackend.POLARS:
            return self.df.group_by(columns)
        else:
            return self.df.groupby(columns)

    def sort(self, by: Union[str, list], descending: bool = False) -> Any:
        """
        Sort DataFrame by columns.

        Args:
            by: Column name or list of column names
            descending: Sort in descending order

        Returns:
            Sorted DataFrame
        """
        if self.backend == DataFrameBackend.POLARS:
            return self.df.sort(by, descending=descending)
        else:
            return self.df.sort_values(by, ascending=not descending)

    def head(self, n: int = 5) -> Any:
        """
        Get first n rows.

        Args:
            n: Number of rows

        Returns:
            DataFrame with first n rows
        """
        return self.df.head(n)

    def tail(self, n: int = 5) -> Any:
        """
        Get last n rows.

        Args:
            n: Number of rows

        Returns:
            DataFrame with last n rows
        """
        return self.df.tail(n)

    def describe(self) -> Any:
        """
        Get summary statistics.

        Returns:
            Summary statistics DataFrame
        """
        return self.df.describe()

    def unique(self, column: str) -> Any:
        """
        Get unique values in a column.

        Args:
            column: Column name

        Returns:
            Series or list of unique values
        """
        if self.backend == DataFrameBackend.POLARS:
            return self.df[column].unique().to_list()
        else:
            return self.df[column].unique()

    def value_counts(self, column: str) -> Any:
        """
        Count unique values in a column.

        Args:
            column: Column name

        Returns:
            Value counts (backend-specific format)
        """
        if self.backend == DataFrameBackend.POLARS:
            return self.df[column].value_counts()
        else:
            return self.df[column].value_counts()

    def get_column(self, column: str) -> Any:
        """
        Get a column as a series.

        Args:
            column: Column name

        Returns:
            Series/column (backend-specific)
        """
        return self.df[column]

    def to_dict(self, orient: str = 'records') -> list:
        """
        Convert to list of dictionaries.

        Args:
            orient: Orientation ('records' for list of dicts)

        Returns:
            List of dictionaries
        """
        if self.backend == DataFrameBackend.POLARS:
            return self.df.to_dicts()
        else:
            return self.df.to_dict(orient=orient)

    def unwrap(self) -> Any:
        """
        Get the underlying DataFrame object.

        Returns:
            Original pandas or Polars DataFrame
        """
        return self.df


def backend_agnostic(func: Callable) -> Callable:
    """
    Decorator to make validation functions backend-agnostic.

    This decorator wraps validation functions to automatically handle
    both pandas and Polars DataFrames, converting them to UnifiedDataFrame
    for processing.

    Usage:
        @backend_agnostic
        def check_nulls(df, column):
            udf = UnifiedDataFrame(df)
            return udf.is_null(column).sum()

    Args:
        func: Function that operates on DataFrames

    Returns:
        Wrapped function that handles both backends
    """
    @wraps(func)
    def wrapper(df, *args, **kwargs):
        # Detect backend and log
        backend = detect_backend(df)
        logger.debug(f"Processing with {backend.value} backend")

        # Call original function
        return func(df, *args, **kwargs)

    return wrapper


class BackendIterator:
    """
    Iterator wrapper that tracks backend across chunks.

    This ensures that all chunks from an iterator use the same backend,
    and provides utilities for backend-specific optimizations.
    """

    def __init__(self, data_iterator: Iterator[Any]):
        """
        Initialize backend-aware iterator.

        Args:
            data_iterator: Iterator yielding DataFrames
        """
        self.data_iterator = data_iterator
        self.backend: Optional[DataFrameBackend] = None
        self.chunk_count = 0
        self.total_rows = 0

    def __iter__(self):
        """Return iterator."""
        return self

    def __next__(self) -> UnifiedDataFrame:
        """
        Get next chunk as UnifiedDataFrame.

        Returns:
            UnifiedDataFrame wrapping the chunk

        Raises:
            StopIteration: When iterator is exhausted
        """
        chunk = next(self.data_iterator)

        # Detect and validate backend consistency
        chunk_backend = detect_backend(chunk)
        if self.backend is None:
            self.backend = chunk_backend
            logger.debug(f"Iterator using {self.backend.value} backend")
        elif self.backend != chunk_backend:
            logger.warning(
                f"Backend changed mid-iteration: {self.backend.value} -> {chunk_backend.value}"
            )
            self.backend = chunk_backend

        # Track statistics
        self.chunk_count += 1
        udf = UnifiedDataFrame(chunk)
        self.total_rows += udf.row_count()

        return udf

    def get_stats(self) -> dict:
        """
        Get iterator statistics.

        Returns:
            Dictionary with chunk count, total rows, and backend
        """
        return {
            'chunk_count': self.chunk_count,
            'total_rows': self.total_rows,
            'backend': self.backend.value if self.backend else None
        }
