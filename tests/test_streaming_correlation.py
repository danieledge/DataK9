"""
Tests for streaming correlation implementation.

Validates the correctness and performance of the streaming correlation
algorithm against standard correlation methods.
"""

import pytest
import numpy as np
import pandas as pd
from validation_framework.profiler.streaming_correlation import (
    StreamingCorrelation,
    StreamingCorrelationMatrix
)

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


class TestStreamingCorrelation:
    """Test StreamingCorrelation class."""

    def test_perfect_positive_correlation(self):
        """Test perfect positive correlation (r = 1.0)."""
        corr = StreamingCorrelation()
        x = np.array([1, 2, 3, 4, 5])
        y = np.array([2, 4, 6, 8, 10])
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is not None
        assert abs(result - 1.0) < 1e-10

    def test_perfect_negative_correlation(self):
        """Test perfect negative correlation (r = -1.0)."""
        corr = StreamingCorrelation()
        x = np.array([1, 2, 3, 4, 5])
        y = np.array([10, 8, 6, 4, 2])
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is not None
        assert abs(result - (-1.0)) < 1e-10

    def test_no_correlation(self):
        """Test weak correlation."""
        corr = StreamingCorrelation()
        # Generate truly uncorrelated data
        np.random.seed(42)
        x = np.array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        y = np.random.permutation([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is not None
        # Should be close to 0 for uncorrelated data
        assert abs(result) < 0.7  # Relaxed threshold for small sample

    def test_matches_numpy_correlation(self):
        """Test that streaming correlation matches NumPy's correlation."""
        np.random.seed(42)
        x = np.random.randn(1000)
        y = 0.7 * x + 0.3 * np.random.randn(1000)

        # Streaming correlation
        corr = StreamingCorrelation()
        corr.update_batch(x, y)
        streaming_result = corr.get_correlation()

        # NumPy correlation
        numpy_result = np.corrcoef(x, y)[0, 1]

        assert streaming_result is not None
        assert abs(streaming_result - numpy_result) < 1e-10

    def test_incremental_updates(self):
        """Test that incremental updates produce same result as batch."""
        np.random.seed(42)
        x = np.random.randn(100)
        y = 0.5 * x + 0.5 * np.random.randn(100)

        # Batch update
        corr_batch = StreamingCorrelation()
        corr_batch.update_batch(x, y)
        batch_result = corr_batch.get_correlation()

        # Incremental updates
        corr_incremental = StreamingCorrelation()
        for i in range(len(x)):
            corr_incremental.update(x[i], y[i])
        incremental_result = corr_incremental.get_correlation()

        assert batch_result is not None
        assert incremental_result is not None
        assert abs(batch_result - incremental_result) < 1e-10

    def test_chunked_processing(self):
        """Test that processing in chunks gives same result as single batch."""
        np.random.seed(42)
        x = np.random.randn(1000)
        y = 0.8 * x + 0.2 * np.random.randn(1000)

        # Single batch
        corr_single = StreamingCorrelation()
        corr_single.update_batch(x, y)
        single_result = corr_single.get_correlation()

        # Process in chunks of 100
        corr_chunked = StreamingCorrelation()
        for i in range(0, len(x), 100):
            chunk_x = x[i:i+100]
            chunk_y = y[i:i+100]
            corr_chunked.update_batch(chunk_x, chunk_y)
        chunked_result = corr_chunked.get_correlation()

        assert single_result is not None
        assert chunked_result is not None
        assert abs(single_result - chunked_result) < 1e-10

    def test_handles_missing_values(self):
        """Test that missing values are handled correctly."""
        corr = StreamingCorrelation()
        x = np.array([1.0, 2.0, np.nan, 4.0, 5.0])
        y = np.array([2.0, 4.0, 6.0, np.nan, 10.0])
        corr.update_batch(x, y)

        # Should only use pairs (1, 2), (2, 4), and (5, 10) - pairs without any NaN
        assert corr.n == 3
        result = corr.get_correlation()
        assert result is not None

    def test_constant_variable_returns_none(self):
        """Test that constant variable returns None."""
        corr = StreamingCorrelation()
        x = np.array([5, 5, 5, 5, 5])  # Constant
        y = np.array([1, 2, 3, 4, 5])
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is None  # Cannot compute correlation with constant variable

    def test_insufficient_data_returns_none(self):
        """Test that insufficient data returns None."""
        corr = StreamingCorrelation()
        corr.update(1.0, 2.0)  # Only one sample

        result = corr.get_correlation()
        assert result is None  # Need at least 2 samples

    def test_get_covariance(self):
        """Test covariance calculation."""
        corr = StreamingCorrelation()
        x = np.array([1, 2, 3, 4, 5])
        y = np.array([2, 4, 6, 8, 10])
        corr.update_batch(x, y)

        cov = corr.get_covariance()
        expected_cov = np.cov(x, y, ddof=1)[0, 1]

        assert cov is not None
        assert abs(cov - expected_cov) < 1e-10

    def test_reset(self):
        """Test reset functionality."""
        corr = StreamingCorrelation()
        x = np.array([1, 2, 3])
        y = np.array([2, 4, 6])
        corr.update_batch(x, y)

        assert corr.n == 3

        corr.reset()

        assert corr.n == 0
        assert corr.mean_x == 0.0
        assert corr.mean_y == 0.0


class TestStreamingCorrelationMatrix:
    """Test StreamingCorrelationMatrix class."""

    def test_initialization(self):
        """Test matrix initialization."""
        columns = ['a', 'b', 'c']
        matrix = StreamingCorrelationMatrix(columns)

        assert len(matrix.columns) == 3
        # Should have 3 pairs: (a,b), (a,c), (b,c)
        assert len(matrix.correlations) == 3

    def test_update_from_dataframe_pandas(self):
        """Test updating from pandas DataFrame."""
        matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])

        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 4, 6, 8, 10],
            'c': [5, 4, 3, 2, 1]
        })

        matrix.update_from_dataframe(df)

        correlations = matrix.get_correlation_dict()

        # a and b should be perfectly correlated
        assert abs(correlations['a|b'] - 1.0) < 1e-10

        # a and c should be perfectly anti-correlated
        assert abs(correlations['a|c'] - (-1.0)) < 1e-10

    @pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
    def test_update_from_dataframe_polars(self):
        """Test updating from Polars DataFrame."""
        matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])

        df = pl.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 4, 6, 8, 10],
            'c': [5, 4, 3, 2, 1]
        })

        matrix.update_from_dataframe(df)

        correlations = matrix.get_correlation_dict()

        # a and b should be perfectly correlated
        assert abs(correlations['a|b'] - 1.0) < 1e-10

        # a and c should be perfectly anti-correlated
        assert abs(correlations['a|c'] - (-1.0)) < 1e-10

    def test_matches_pandas_correlation(self):
        """Test that results match pandas correlation."""
        np.random.seed(42)
        df = pd.DataFrame({
            'x': np.random.randn(500),
            'y': np.random.randn(500),
            'z': np.random.randn(500)
        })

        # Create correlated data
        df['y'] = 0.6 * df['x'] + 0.4 * df['y']
        df['z'] = -0.5 * df['x'] + 0.5 * df['z']

        # Streaming correlation
        matrix = StreamingCorrelationMatrix(['x', 'y', 'z'])
        matrix.update_from_dataframe(df)
        streaming_corr = matrix.get_correlation_dict()

        # Pandas correlation
        pandas_corr = df.corr()

        # Compare all pairs
        assert abs(streaming_corr['x|y'] - pandas_corr.loc['x', 'y']) < 1e-10
        assert abs(streaming_corr['x|z'] - pandas_corr.loc['x', 'z']) < 1e-10
        assert abs(streaming_corr['y|z'] - pandas_corr.loc['y', 'z']) < 1e-10

    def test_chunked_processing_matches(self):
        """Test that chunked processing gives same result as full data."""
        np.random.seed(42)
        df_full = pd.DataFrame({
            'a': np.random.randn(1000),
            'b': np.random.randn(1000),
            'c': np.random.randn(1000)
        })

        # Full data
        matrix_full = StreamingCorrelationMatrix(['a', 'b', 'c'])
        matrix_full.update_from_dataframe(df_full)
        full_corr = matrix_full.get_correlation_dict()

        # Chunked processing
        matrix_chunked = StreamingCorrelationMatrix(['a', 'b', 'c'])
        chunk_size = 100
        for i in range(0, len(df_full), chunk_size):
            chunk = df_full.iloc[i:i+chunk_size]
            matrix_chunked.update_from_dataframe(chunk)
        chunked_corr = matrix_chunked.get_correlation_dict()

        # Compare results
        for key in full_corr:
            assert abs(full_corr[key] - chunked_corr[key]) < 1e-10

    def test_get_correlation_matrix(self):
        """Test getting correlation as 2D matrix."""
        matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])

        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 4, 6, 8, 10],
            'c': [5, 4, 3, 2, 1]
        })

        matrix.update_from_dataframe(df)

        corr_matrix = matrix.get_correlation_matrix()

        # Diagonal should be 1.0
        assert abs(corr_matrix[0, 0] - 1.0) < 1e-10
        assert abs(corr_matrix[1, 1] - 1.0) < 1e-10
        assert abs(corr_matrix[2, 2] - 1.0) < 1e-10

        # Should be symmetric
        assert abs(corr_matrix[0, 1] - corr_matrix[1, 0]) < 1e-10
        assert abs(corr_matrix[0, 2] - corr_matrix[2, 0]) < 1e-10
        assert abs(corr_matrix[1, 2] - corr_matrix[2, 1]) < 1e-10

    def test_get_sample_counts(self):
        """Test sample count tracking."""
        matrix = StreamingCorrelationMatrix(['a', 'b'])

        df1 = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 4, 6]})
        df2 = pd.DataFrame({'a': [4, 5], 'b': [8, 10]})

        matrix.update_from_dataframe(df1)
        matrix.update_from_dataframe(df2)

        counts = matrix.get_sample_counts()
        assert counts['a|b'] == 5  # Total samples

    def test_get_summary(self):
        """Test summary statistics."""
        matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])

        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 4, 6, 8, 10],
            'c': [1, 1, 2, 2, 3]
        })

        matrix.update_from_dataframe(df)

        summary = matrix.get_summary()

        assert summary['column_count'] == 3
        assert summary['pair_count'] == 3
        assert 'correlations' in summary
        assert 'sample_counts' in summary
        assert 'strong_correlations' in summary

        # a and b should be in strong correlations (|r| > 0.7)
        strong_pairs = [item['pair'] for item in summary['strong_correlations']]
        assert 'a|b' in strong_pairs

    def test_missing_column_raises_error(self):
        """Test that missing columns raise an error."""
        matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])

        df = pd.DataFrame({'a': [1, 2], 'b': [2, 4]})  # Missing 'c'

        with pytest.raises(ValueError, match="missing columns"):
            matrix.update_from_dataframe(df)

    def test_update_from_dict(self):
        """Test updating from dictionary of arrays."""
        matrix = StreamingCorrelationMatrix(['x', 'y'])

        data = {
            'x': np.array([1, 2, 3, 4, 5]),
            'y': np.array([2, 4, 6, 8, 10])
        }

        matrix.update_from_dict(data)

        correlations = matrix.get_correlation_dict()
        assert abs(correlations['x|y'] - 1.0) < 1e-10

    def test_reset(self):
        """Test reset functionality."""
        matrix = StreamingCorrelationMatrix(['a', 'b'])

        df = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 4, 6]})
        matrix.update_from_dataframe(df)

        counts_before = matrix.get_sample_counts()
        assert counts_before['a|b'] == 3

        matrix.reset()

        counts_after = matrix.get_sample_counts()
        assert counts_after['a|b'] == 0


class TestNumericalStability:
    """Test numerical stability of streaming correlation."""

    def test_large_values(self):
        """Test with very large values."""
        corr = StreamingCorrelation()
        x = np.array([1e10, 2e10, 3e10, 4e10, 5e10])
        y = np.array([2e10, 4e10, 6e10, 8e10, 10e10])
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is not None
        assert abs(result - 1.0) < 1e-8  # Should still be close to 1.0

    def test_small_values(self):
        """Test with very small values."""
        corr = StreamingCorrelation()
        x = np.array([1e-10, 2e-10, 3e-10, 4e-10, 5e-10])
        y = np.array([2e-10, 4e-10, 6e-10, 8e-10, 10e-10])
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is not None
        assert abs(result - 1.0) < 1e-8

    def test_mixed_magnitude(self):
        """Test with values of very different magnitudes."""
        corr = StreamingCorrelation()
        x = np.array([1e-5, 1e0, 1e5, 1e10, 1e15])
        y = 2 * x
        corr.update_batch(x, y)

        result = corr.get_correlation()
        assert result is not None
        assert abs(result - 1.0) < 1e-6
