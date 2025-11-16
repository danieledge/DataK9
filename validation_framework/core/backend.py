"""
DataFrame backend abstraction for supporting both pandas and Polars.

This module provides a unified interface for working with different DataFrame libraries,
allowing the framework to leverage Polars' performance while maintaining pandas compatibility.

Performance Characteristics:
    - Polars: 5-10x faster than pandas for most operations, lower memory usage
    - Pandas: More mature ecosystem, broader library compatibility
"""

from enum import Enum
from typing import Union, Any, Iterator
import logging

logger = logging.getLogger(__name__)

# Try to import both libraries
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    pd = None

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None


class DataFrameBackend(Enum):
    """
    Enum for supported DataFrame backends.

    POLARS is the default for best performance (5-10x faster than pandas).
    PANDAS is available for compatibility and when Polars is not installed.
    """
    POLARS = "polars"
    PANDAS = "pandas"


# Type alias for DataFrame objects from either library
if HAS_PANDAS and HAS_POLARS:
    DataFrame = Union[pd.DataFrame, pl.DataFrame]
elif HAS_PANDAS:
    DataFrame = pd.DataFrame
elif HAS_POLARS:
    DataFrame = pl.DataFrame
else:
    DataFrame = Any


class BackendManager:
    """
    Manages DataFrame backend selection and conversion.

    This class handles:
    - Backend availability checks
    - Default backend selection
    - Conversion between pandas and Polars DataFrames
    - Performance optimization recommendations
    """

    @staticmethod
    def get_default_backend() -> DataFrameBackend:
        """
        Get the default backend based on availability.

        Priority:
        1. Polars (if installed) - for best performance
        2. Pandas (if installed) - for compatibility

        Returns:
            DataFrameBackend: The default backend to use

        Raises:
            RuntimeError: If neither pandas nor Polars is installed
        """
        if HAS_POLARS:
            logger.info("Using Polars backend (default) for optimal performance")
            return DataFrameBackend.POLARS
        elif HAS_PANDAS:
            logger.warning(
                "Polars not installed, falling back to pandas. "
                "Install polars-lts-cpu for 5-10x performance improvement"
            )
            return DataFrameBackend.PANDAS
        else:
            raise RuntimeError(
                "Neither pandas nor Polars is installed. "
                "Install one with: pip install polars-lts-cpu (recommended) or pip install pandas"
            )

    @staticmethod
    def validate_backend(backend: DataFrameBackend) -> None:
        """
        Validate that the requested backend is available.

        Args:
            backend: The backend to validate

        Raises:
            RuntimeError: If the requested backend is not installed
        """
        if backend == DataFrameBackend.POLARS and not HAS_POLARS:
            raise RuntimeError(
                "Polars backend requested but not installed. "
                "Install with: pip install polars-lts-cpu"
            )
        elif backend == DataFrameBackend.PANDAS and not HAS_PANDAS:
            raise RuntimeError(
                "Pandas backend requested but not installed. "
                "Install with: pip install pandas"
            )

    @staticmethod
    def to_polars(df: Any) -> 'pl.DataFrame':
        """
        Convert a pandas DataFrame to Polars DataFrame.

        Args:
            df: pandas DataFrame to convert

        Returns:
            Polars DataFrame

        Raises:
            RuntimeError: If Polars is not installed
        """
        if not HAS_POLARS:
            raise RuntimeError("Polars not installed. Install with: pip install polars-lts-cpu")

        if isinstance(df, pl.DataFrame):
            return df

        # Convert pandas to Polars
        # Polars can directly read from pandas DataFrames efficiently
        return pl.from_pandas(df)

    @staticmethod
    def to_pandas(df: Any) -> 'pd.DataFrame':
        """
        Convert a Polars DataFrame to pandas DataFrame.

        Args:
            df: Polars DataFrame to convert

        Returns:
            pandas DataFrame

        Raises:
            RuntimeError: If pandas is not installed
        """
        if not HAS_PANDAS:
            raise RuntimeError("Pandas not installed. Install with: pip install pandas")

        if isinstance(df, pd.DataFrame):
            return df

        # Convert Polars to pandas
        # This is a zero-copy conversion when possible
        return df.to_pandas()

    @staticmethod
    def get_backend_info() -> dict:
        """
        Get information about available backends and their versions.

        Returns:
            Dictionary with backend availability and version information
        """
        info = {
            'pandas_available': HAS_PANDAS,
            'polars_available': HAS_POLARS,
            'default_backend': None,
            'pandas_version': None,
            'polars_version': None,
        }

        if HAS_PANDAS:
            info['pandas_version'] = pd.__version__

        if HAS_POLARS:
            info['polars_version'] = pl.__version__

        if HAS_POLARS or HAS_PANDAS:
            info['default_backend'] = BackendManager.get_default_backend().value

        return info


class DataFrameAdapter:
    """
    Adapter for working with DataFrames in a backend-agnostic way.

    This class provides common operations that work with both pandas and Polars,
    abstracting away the differences in their APIs.
    """

    def __init__(self, backend: DataFrameBackend):
        """
        Initialize the adapter with a specific backend.

        Args:
            backend: The DataFrame backend to use
        """
        self.backend = backend
        BackendManager.validate_backend(backend)

    def is_null(self, df: DataFrame, column: str) -> Any:
        """
        Get null/NA mask for a column.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Boolean mask/series indicating null values
        """
        if self.backend == DataFrameBackend.POLARS:
            return df[column].is_null()
        else:
            return df[column].isna()

    def filter_rows(self, df: DataFrame, mask: Any) -> DataFrame:
        """
        Filter DataFrame rows by a boolean mask.

        Args:
            df: DataFrame (pandas or Polars)
            mask: Boolean mask/series

        Returns:
            Filtered DataFrame
        """
        if self.backend == DataFrameBackend.POLARS:
            return df.filter(mask)
        else:
            return df[mask]

    def get_column_names(self, df: DataFrame) -> list:
        """
        Get list of column names.

        Args:
            df: DataFrame (pandas or Polars)

        Returns:
            List of column names
        """
        if self.backend == DataFrameBackend.POLARS:
            return df.columns
        else:
            return list(df.columns)

    def row_count(self, df: DataFrame) -> int:
        """
        Get number of rows in DataFrame.

        Args:
            df: DataFrame (pandas or Polars)

        Returns:
            Number of rows
        """
        if self.backend == DataFrameBackend.POLARS:
            return df.height
        else:
            return len(df)

    def column_count(self, df: DataFrame) -> int:
        """
        Get number of columns in DataFrame.

        Args:
            df: DataFrame (pandas or Polars)

        Returns:
            Number of columns
        """
        if self.backend == DataFrameBackend.POLARS:
            return df.width
        else:
            return len(df.columns)
