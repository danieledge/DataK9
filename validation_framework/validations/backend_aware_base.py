"""
Backend-aware validation base classes supporting both pandas and Polars.

This module provides base classes and helper methods that enable validations
to work seamlessly with both pandas and Polars DataFrames without code duplication.
"""

from typing import Iterator, Dict, Any, List, Optional, Union
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.backend import HAS_POLARS

if HAS_POLARS:
    import polars as pl
import pandas as pd


class BackendAwareValidationRule(DataValidationRule):
    """
    Base class for validations that support both pandas and Polars backends.

    Provides helper methods to detect and handle both DataFrame types,
    abstracting away backend-specific API differences.

    Example:
        class MyValidation(BackendAwareValidationRule):
            def validate(self, data_iterator, context):
                failures = []
                for chunk in data_iterator:
                    # Works with both pandas and Polars
                    null_mask = self.get_null_mask(chunk, 'my_column')
                    filtered = self.filter_df(chunk, null_mask)
                    failures.extend(self.df_to_dicts(filtered))

                return self._create_result(...)
    """

    def is_polars(self, df) -> bool:
        """
        Check if DataFrame is Polars.

        Args:
            df: DataFrame to check

        Returns:
            True if Polars DataFrame, False otherwise
        """
        return HAS_POLARS and isinstance(df, pl.DataFrame)

    def is_pandas(self, df) -> bool:
        """
        Check if DataFrame is pandas.

        Args:
            df: DataFrame to check

        Returns:
            True if pandas DataFrame, False otherwise
        """
        return isinstance(df, pd.DataFrame)

    def get_backend_name(self, df) -> str:
        """
        Get the name of the DataFrame backend.

        Args:
            df: DataFrame to check

        Returns:
            'polars', 'pandas', or 'unknown'
        """
        if self.is_polars(df):
            return 'polars'
        elif self.is_pandas(df):
            return 'pandas'
        else:
            return 'unknown'

    def get_columns(self, df) -> List[str]:
        """
        Get column names from DataFrame, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)

        Returns:
            List of column names
        """
        if self.is_polars(df):
            return df.columns
        else:
            return df.columns.tolist()

    def has_column(self, df, column: str) -> bool:
        """
        Check if DataFrame has a column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name to check

        Returns:
            True if column exists, False otherwise
        """
        return column in self.get_columns(df)

    def get_null_mask(self, df, column: str):
        """
        Get null mask for column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Boolean mask/series indicating null values
        """
        if self.is_polars(df):
            return df[column].is_null()
        else:
            return df[column].isna()

    def get_not_null_mask(self, df, column: str):
        """
        Get not-null mask for column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Boolean mask/series indicating non-null values
        """
        if self.is_polars(df):
            return df[column].is_not_null()
        else:
            return df[column].notna()

    def filter_df(self, df, mask):
        """
        Filter DataFrame by boolean mask, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            mask: Boolean mask/series

        Returns:
            Filtered DataFrame
        """
        if self.is_polars(df):
            return df.filter(mask)
        else:
            return df[mask]

    def get_row_count(self, df) -> int:
        """
        Get row count, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)

        Returns:
            Number of rows
        """
        if self.is_polars(df):
            return df.height
        else:
            return len(df)

    def get_column_count(self, df) -> int:
        """
        Get column count, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)

        Returns:
            Number of columns
        """
        if self.is_polars(df):
            return df.width
        else:
            return len(df.columns)

    def get_column_dtype(self, df, column: str) -> str:
        """
        Get column data type as string, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Data type as string
        """
        if self.is_polars(df):
            return str(df[column].dtype)
        else:
            return str(df[column].dtype)

    def select_columns(self, df, columns: List[str]):
        """
        Select specific columns from DataFrame, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            columns: List of column names to select

        Returns:
            DataFrame with selected columns
        """
        if self.is_polars(df):
            return df.select(columns)
        else:
            return df[columns]

    def drop_nulls(self, df, subset: Optional[List[str]] = None):
        """
        Drop rows with null values, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            subset: Columns to consider for null values (None = all columns)

        Returns:
            DataFrame with null rows removed
        """
        if self.is_polars(df):
            return df.drop_nulls(subset=subset)
        else:
            return df.dropna(subset=subset)

    def get_unique_values(self, df, column: str) -> List:
        """
        Get unique values from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            List of unique values
        """
        if self.is_polars(df):
            return df[column].unique().to_list()
        else:
            return df[column].unique().tolist()

    def get_value_counts(self, df, column: str) -> Dict[Any, int]:
        """
        Get value counts for column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Dictionary mapping values to counts
        """
        if self.is_polars(df):
            counts = df[column].value_counts()
            return dict(zip(counts[column].to_list(), counts["count"].to_list()))
        else:
            return df[column].value_counts().to_dict()

    def df_to_dicts(self, df, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Convert DataFrame rows to list of dictionaries, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            limit: Maximum number of rows to convert (None = all rows)

        Returns:
            List of dictionaries, one per row
        """
        if limit is not None and self.get_row_count(df) > limit:
            if self.is_polars(df):
                df = df.head(limit)
            else:
                df = df.head(limit)

        if self.is_polars(df):
            return df.to_dicts()
        else:
            return df.to_dict('records')

    def get_column_min(self, df, column: str):
        """
        Get minimum value from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Minimum value
        """
        if self.is_polars(df):
            return df[column].min()
        else:
            return df[column].min()

    def get_column_max(self, df, column: str):
        """
        Get maximum value from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Maximum value
        """
        if self.is_polars(df):
            return df[column].max()
        else:
            return df[column].max()

    def get_column_mean(self, df, column: str):
        """
        Get mean value from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Mean value
        """
        if self.is_polars(df):
            return df[column].mean()
        else:
            return df[column].mean()

    def get_column_std(self, df, column: str):
        """
        Get standard deviation from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Standard deviation
        """
        if self.is_polars(df):
            return df[column].std()
        else:
            return df[column].std()

    def group_by_count(self, df, columns: Union[str, List[str]]) -> Dict:
        """
        Group by column(s) and count, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            columns: Column name or list of column names to group by

        Returns:
            Dictionary with group counts
        """
        if isinstance(columns, str):
            columns = [columns]

        if self.is_polars(df):
            grouped = df.group_by(columns).agg(pl.len().alias("count"))
            return grouped.to_dicts()
        else:
            grouped = df.groupby(columns).size().reset_index(name='count')
            return grouped.to_dict('records')

    def concat_dataframes(self, dfs: List) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """
        Concatenate multiple DataFrames, backend-agnostic.

        Args:
            dfs: List of DataFrames (all must be same backend)

        Returns:
            Concatenated DataFrame or None if list is empty
        """
        if not dfs:
            return None

        if self.is_polars(dfs[0]):
            return pl.concat(dfs)
        else:
            return pd.concat(dfs, ignore_index=True)

    def get_column_median(self, df, column: str):
        """
        Get median value from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Median value
        """
        if self.is_polars(df):
            return df[column].median()
        else:
            return df[column].median()

    def get_column_quantile(self, df, column: str, q: float):
        """
        Get quantile value from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name
            q: Quantile to compute (0.0 to 1.0)

        Returns:
            Quantile value
        """
        if self.is_polars(df):
            return df[column].quantile(q)
        else:
            return df[column].quantile(q)

    def get_column_sum(self, df, column: str):
        """
        Get sum of values from column, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Sum value
        """
        if self.is_polars(df):
            return df[column].sum()
        else:
            return df[column].sum()

    def get_column_values_as_array(self, df, column: str):
        """
        Get column values as numpy array, backend-agnostic.

        Args:
            df: DataFrame (pandas or Polars)
            column: Column name

        Returns:
            Numpy array of column values
        """
        import numpy as np
        if self.is_polars(df):
            return df[column].to_numpy()
        else:
            return df[column].to_numpy()
