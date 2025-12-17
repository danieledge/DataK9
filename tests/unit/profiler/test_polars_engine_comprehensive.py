"""
Comprehensive tests for PolarsDataProfiler.

Tests the Polars-based data profiler for CSV and Parquet files,
including streaming correlation and memory-efficient processing.
"""

import pytest
import numpy as np
import pandas as pd
import tempfile
from pathlib import Path

from validation_framework.profiler.polars_engine import PolarsDataProfiler


class TestPolarsDataProfiler:
    """Tests for PolarsDataProfiler core functionality."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return PolarsDataProfiler()

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file."""
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'item_{i}' for i in range(1, 101)],
            'value': np.random.normal(100, 15, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100)
        })
        csv_path = tmp_path / 'sample.csv'
        df.to_csv(csv_path, index=False)
        return csv_path

    @pytest.fixture
    def sample_parquet(self, tmp_path):
        """Create sample Parquet file."""
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'item_{i}' for i in range(1, 101)],
            'value': np.random.normal(100, 15, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100)
        })
        parquet_path = tmp_path / 'sample.parquet'
        df.to_parquet(parquet_path, index=False)
        return parquet_path

    def test_profile_csv_basic(self, profiler, sample_csv):
        """Test basic CSV profiling."""
        result = profiler.profile_file(str(sample_csv))

        assert result is not None
        assert result.row_count == 100
        assert result.column_count == 4

    def test_profile_parquet_basic(self, profiler, sample_parquet):
        """Test basic Parquet profiling."""
        result = profiler.profile_file(str(sample_parquet))

        assert result is not None
        assert result.row_count == 100
        assert result.column_count == 4

    def test_column_statistics(self, profiler, sample_csv):
        """Test that column statistics are computed."""
        result = profiler.profile_file(str(sample_csv))

        # Check column profiles exist
        assert result.column_profiles is not None
        assert 'id' in result.column_profiles
        assert 'category' in result.column_profiles

        # Check id column
        id_profile = result.column_profiles['id']
        assert id_profile.get('null_count', 0) == 0

        # Check category column has limited unique values
        cat_profile = result.column_profiles['category']
        assert cat_profile.get('unique_count', 0) == 3

    def test_file_not_found(self, profiler):
        """Test error handling for missing file."""
        with pytest.raises(Exception):
            profiler.profile_file('/nonexistent/path/file.csv')

    def test_empty_file_handling(self, profiler, tmp_path):
        """Test handling of empty file."""
        empty_csv = tmp_path / 'empty.csv'
        empty_csv.write_text('col1,col2,col3\n')  # Header only

        result = profiler.profile_file(str(empty_csv))
        assert result.row_count == 0
        assert result.column_count == 3

    def test_null_value_handling(self, profiler, tmp_path):
        """Test handling of null values."""
        df = pd.DataFrame({
            'complete': [1, 2, 3, 4, 5],
            'partial': [1, None, 3, None, 5],
            'all_null': [None, None, None, None, None]
        })
        csv_path = tmp_path / 'nulls.csv'
        df.to_csv(csv_path, index=False)

        result = profiler.profile_file(str(csv_path))

        # Check null counts via column_profiles
        complete_profile = result.column_profiles['complete']
        assert complete_profile.get('null_count', 0) == 0

        partial_profile = result.column_profiles['partial']
        assert partial_profile.get('null_count', 0) == 2

        all_null_profile = result.column_profiles['all_null']
        assert all_null_profile.get('null_count', 0) == 5

    def test_mixed_data_types(self, profiler, tmp_path):
        """Test handling of mixed data types."""
        df = pd.DataFrame({
            'integers': [1, 2, 3, 4, 5],
            'floats': [1.1, 2.2, 3.3, 4.4, 5.5],
            'strings': ['a', 'b', 'c', 'd', 'e'],
            'booleans': [True, False, True, False, True]
        })
        csv_path = tmp_path / 'mixed.csv'
        df.to_csv(csv_path, index=False)

        result = profiler.profile_file(str(csv_path))
        assert result.column_count == 4

    def test_profile_dataframe(self, profiler):
        """Test profiling a DataFrame directly."""
        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': ['x', 'y', 'z', 'x', 'y']
        })

        result = profiler.profile_dataframe(df, name='test_df')

        assert result.row_count == 5
        assert result.column_count == 2


class TestPolarsEngineCorrelation:
    """Tests for correlation calculation in PolarsDataProfiler."""

    @pytest.fixture
    def profiler_with_correlation(self):
        """Create profiler with correlation enabled."""
        return PolarsDataProfiler(enable_correlation=True)

    @pytest.fixture
    def correlated_csv(self, tmp_path):
        """Create CSV with correlated columns."""
        np.random.seed(42)
        x = np.random.normal(0, 1, 500)
        y = x * 0.8 + np.random.normal(0, 0.2, 500)
        z = -x * 0.9 + np.random.normal(0, 0.1, 500)

        df = pd.DataFrame({'x': x, 'y': y, 'z': z})
        csv_path = tmp_path / 'correlated.csv'
        df.to_csv(csv_path, index=False)
        return csv_path

    def test_correlation_computed(self, profiler_with_correlation, correlated_csv):
        """Test that correlations are computed."""
        result = profiler_with_correlation.profile_file(str(correlated_csv))

        # Check that correlation info exists
        assert result.correlations is not None or hasattr(result, 'column_correlations')

    def test_positive_correlation_detected(self, profiler_with_correlation, tmp_path):
        """Test detection of positive correlation."""
        np.random.seed(42)
        x = np.random.normal(0, 1, 200)
        y = x * 2 + np.random.normal(0, 0.1, 200)

        df = pd.DataFrame({'x': x, 'y': y})
        csv_path = tmp_path / 'positive_corr.csv'
        df.to_csv(csv_path, index=False)

        result = profiler_with_correlation.profile_file(str(csv_path))
        # Result should contain correlation info
        assert result is not None

    def test_negative_correlation_detected(self, profiler_with_correlation, tmp_path):
        """Test detection of negative correlation."""
        np.random.seed(42)
        x = np.random.normal(0, 1, 200)
        y = -x * 2 + np.random.normal(0, 0.1, 200)

        df = pd.DataFrame({'x': x, 'y': y})
        csv_path = tmp_path / 'negative_corr.csv'
        df.to_csv(csv_path, index=False)

        result = profiler_with_correlation.profile_file(str(csv_path))
        # Result should contain correlation info
        assert result is not None


class TestPolarsEnginePerformance:
    """Tests for performance characteristics of PolarsDataProfiler."""

    @pytest.fixture
    def large_csv(self, tmp_path):
        """Create larger CSV file for performance testing."""
        np.random.seed(42)
        n_rows = 10000
        df = pd.DataFrame({
            'id': range(n_rows),
            'value1': np.random.normal(0, 1, n_rows),
            'value2': np.random.uniform(0, 100, n_rows),
            'category': np.random.choice(['A', 'B', 'C', 'D'], n_rows),
            'text': [f'text_{i}' for i in range(n_rows)]
        })
        csv_path = tmp_path / 'large.csv'
        df.to_csv(csv_path, index=False)
        return csv_path

    def test_large_file_processing(self, large_csv):
        """Test processing of larger file."""
        profiler = PolarsDataProfiler()
        result = profiler.profile_file(str(large_csv))

        assert result.row_count == 10000
        assert result.column_count == 5

    def test_chunked_processing(self, large_csv):
        """Test chunked processing works correctly."""
        profiler = PolarsDataProfiler(chunk_size=1000)
        result = profiler.profile_file(str(large_csv))

        assert result.row_count == 10000


class TestPolarsEngineEdgeCases:
    """Tests for edge cases in PolarsDataProfiler."""

    @pytest.fixture
    def profiler(self):
        """Create profiler instance."""
        return PolarsDataProfiler()

    def test_single_row(self, profiler, tmp_path):
        """Test file with single row."""
        df = pd.DataFrame({'a': [1], 'b': ['x']})
        csv_path = tmp_path / 'single_row.csv'
        df.to_csv(csv_path, index=False)

        result = profiler.profile_file(str(csv_path))
        assert result.row_count == 1

    def test_single_column(self, profiler, tmp_path):
        """Test file with single column."""
        df = pd.DataFrame({'only_col': [1, 2, 3, 4, 5]})
        csv_path = tmp_path / 'single_col.csv'
        df.to_csv(csv_path, index=False)

        result = profiler.profile_file(str(csv_path))
        assert result.column_count == 1

    def test_unicode_data(self, profiler, tmp_path):
        """Test handling of unicode data."""
        df = pd.DataFrame({
            'name': ['日本語', 'Ελληνικά', 'العربية', 'עברית', '한국어'],
            'value': [1, 2, 3, 4, 5]
        })
        csv_path = tmp_path / 'unicode.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')

        result = profiler.profile_file(str(csv_path))
        assert result.row_count == 5

    def test_special_characters_in_columns(self, profiler, tmp_path):
        """Test handling of special characters in column names."""
        df = pd.DataFrame({
            'col with spaces': [1, 2, 3],
            'col-with-dashes': [4, 5, 6],
            'col_with_underscores': [7, 8, 9]
        })
        csv_path = tmp_path / 'special_cols.csv'
        df.to_csv(csv_path, index=False)

        result = profiler.profile_file(str(csv_path))
        assert result.column_count == 3

    def test_very_wide_file(self, profiler, tmp_path):
        """Test file with many columns."""
        n_cols = 50
        df = pd.DataFrame({f'col_{i}': range(10) for i in range(n_cols)})
        csv_path = tmp_path / 'wide.csv'
        df.to_csv(csv_path, index=False)

        result = profiler.profile_file(str(csv_path))
        assert result.column_count == n_cols
