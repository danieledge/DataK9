"""
Tests for streaming quantile and correlation algorithms.

Tests the P² algorithm for quantile estimation and Welford's algorithm
for streaming correlation calculation.
"""

import pytest
import numpy as np
import pandas as pd
from typing import List

from validation_framework.profiler.sampling_utils import (
    P2Quantile,
    StreamingQuantiles,
    compute_streaming_quantiles,
)
from validation_framework.profiler.streaming_correlation import (
    StreamingCorrelation,
    StreamingCorrelationMatrix,
)


class TestP2Quantile:
    """Tests for P² quantile estimator."""

    def test_initialization_default(self):
        """Test default initialization with median."""
        p2 = P2Quantile(0.5)
        assert p2.p == 0.5
        assert p2.count == 0

    def test_initialization_custom_quantile(self):
        """Test initialization with custom quantile values."""
        for q in [0.01, 0.25, 0.5, 0.75, 0.95, 0.99]:
            p2 = P2Quantile(q)
            assert p2.p == q

    def test_invalid_quantile_too_low(self):
        """Test rejection of quantile < 0."""
        with pytest.raises(ValueError):
            P2Quantile(-0.1)

    def test_invalid_quantile_too_high(self):
        """Test rejection of quantile > 1."""
        with pytest.raises(ValueError):
            P2Quantile(1.5)

    def test_update_first_five_values(self):
        """Test that first five values are buffered."""
        p2 = P2Quantile(0.5)
        values = [5, 2, 8, 1, 9]

        for v in values:
            p2.update(v)

        assert p2.count == 5
        estimate = p2.quantile
        assert estimate is not None

    def test_quantile_before_five_values(self):
        """Test quantile property with < 5 values."""
        p2 = P2Quantile(0.5)
        assert p2.quantile is None  # No values yet

        p2.update(5)
        # After 1 value, should return that value for median
        assert p2.quantile is not None

    def test_median_sorted_values(self):
        """Test median estimation with sorted input."""
        p2 = P2Quantile(0.5)
        values = list(range(1, 101))  # 1 to 100

        for v in values:
            p2.update(v)

        estimate = p2.quantile
        actual_median = 50.5
        # Allow 5% error for P² algorithm
        assert abs(estimate - actual_median) / actual_median < 0.05

    def test_median_reverse_sorted(self):
        """Test median with reverse sorted input."""
        p2 = P2Quantile(0.5)
        values = list(range(100, 0, -1))  # 100 down to 1

        for v in values:
            p2.update(v)

        estimate = p2.quantile
        actual_median = 50.5
        assert abs(estimate - actual_median) / actual_median < 0.05

    def test_median_random_order(self):
        """Test median with random input order."""
        np.random.seed(42)
        p2 = P2Quantile(0.5)
        values = np.random.permutation(range(1, 101))

        for v in values:
            p2.update(float(v))

        estimate = p2.quantile
        actual_median = 50.5
        assert abs(estimate - actual_median) / actual_median < 0.05

    def test_95th_percentile(self):
        """Test 95th percentile estimation."""
        np.random.seed(42)
        p2 = P2Quantile(0.95)
        values = np.random.normal(100, 15, 10000)

        for v in values:
            p2.update(v)

        estimate = p2.quantile
        actual_95th = np.percentile(values, 95)
        # Allow 5% error
        assert abs(estimate - actual_95th) / actual_95th < 0.05

    def test_constant_values(self):
        """Test with constant values."""
        p2 = P2Quantile(0.5)
        for _ in range(100):
            p2.update(42.0)

        assert p2.quantile == 42.0

    def test_get_count(self):
        """Test get_count method."""
        p2 = P2Quantile(0.5)
        assert p2.get_count() == 0

        for i in range(10):
            p2.update(float(i))

        assert p2.get_count() == 10


class TestStreamingQuantiles:
    """Tests for StreamingQuantiles multi-quantile tracker."""

    def test_initialization_default(self):
        """Test default initialization."""
        sq = StreamingQuantiles()
        assert 0.25 in sq.quantile_values
        assert 0.50 in sq.quantile_values
        assert 0.75 in sq.quantile_values

    def test_initialization_custom_quantiles(self):
        """Test initialization with custom quantiles."""
        sq = StreamingQuantiles([0.1, 0.5, 0.9])
        assert sq.quantile_values == [0.1, 0.5, 0.9]

    def test_update_single_value(self):
        """Test updating with single values."""
        sq = StreamingQuantiles([0.5])
        sq.update(10.0)
        assert sq.get_count() == 1

    def test_update_batch(self):
        """Test updating with batch of values."""
        sq = StreamingQuantiles([0.5])
        sq.update_batch([1.0, 2.0, 3.0, 4.0, 5.0])
        assert sq.get_count() == 5

    def test_get_quantiles(self):
        """Test getting quantile estimates."""
        sq = StreamingQuantiles([0.25, 0.50, 0.75])
        values = list(range(1, 101))
        sq.update_batch(values)

        quantiles = sq.get_quantiles()
        # Keys should be 'p25', 'p50', 'p75'
        assert 'p25' in quantiles
        assert 'p50' in quantiles
        assert 'p75' in quantiles

    def test_accuracy_uniform_distribution(self):
        """Test accuracy on uniform distribution."""
        np.random.seed(42)
        sq = StreamingQuantiles([0.25, 0.50, 0.75])
        values = list(range(1, 1001))
        np.random.shuffle(values)
        sq.update_batch(values)

        quantiles = sq.get_quantiles()
        # Allow 5% error
        assert abs(quantiles['p50'] - 500) / 500 < 0.05


class TestComputeStreamingQuantiles:
    """Tests for compute_streaming_quantiles function."""

    def test_basic_usage(self):
        """Test basic quantile computation."""
        values = iter(range(1, 101))
        result = compute_streaming_quantiles(values, [0.5])
        assert 'p50' in result
        # Median of 1-100 is 50.5
        assert abs(result['p50'] - 50.5) / 50.5 < 0.1

    def test_with_none_values(self):
        """Test handling of None values."""
        values = [1, 2, None, 4, 5, None, 7, 8, 9, 10]
        result = compute_streaming_quantiles(iter(values), [0.5])
        assert 'p50' in result

    def test_with_nan_values(self):
        """Test handling of NaN values."""
        values = [1, 2, float('nan'), 4, 5, float('nan'), 7, 8, 9, 10]
        result = compute_streaming_quantiles(iter(values), [0.5])
        assert 'p50' in result

    def test_empty_iterator(self):
        """Test with empty iterator."""
        result = compute_streaming_quantiles(iter([]), [0.5])
        # Should return empty dict or dict without p50
        assert result == {} or 'p50' not in result


class TestStreamingCorrelation:
    """Tests for StreamingCorrelation pairwise correlation."""

    def test_initialization(self):
        """Test initialization."""
        corr = StreamingCorrelation()
        assert corr.n == 0

    def test_perfect_positive_correlation(self):
        """Test perfect positive correlation."""
        corr = StreamingCorrelation()
        x_values = list(range(100))
        y_values = [x * 2 + 5 for x in x_values]

        for x, y in zip(x_values, y_values):
            corr.update(x, y)

        r = corr.get_correlation()
        assert abs(r - 1.0) < 1e-10

    def test_perfect_negative_correlation(self):
        """Test perfect negative correlation."""
        corr = StreamingCorrelation()
        x_values = list(range(100))
        y_values = [-x * 2 + 100 for x in x_values]

        for x, y in zip(x_values, y_values):
            corr.update(x, y)

        r = corr.get_correlation()
        assert abs(r - (-1.0)) < 1e-10

    def test_no_correlation(self):
        """Test uncorrelated variables."""
        np.random.seed(42)
        corr = StreamingCorrelation()
        x_values = np.random.normal(0, 1, 1000)
        y_values = np.random.normal(0, 1, 1000)

        for x, y in zip(x_values, y_values):
            corr.update(x, y)

        r = corr.get_correlation()
        assert abs(r) < 0.1  # Should be close to 0

    def test_matches_numpy_correlation(self):
        """Test that streaming matches numpy correlation."""
        np.random.seed(42)
        x_values = np.random.normal(10, 2, 1000)
        y_values = 2 * x_values + np.random.normal(0, 1, 1000)

        corr = StreamingCorrelation()
        for x, y in zip(x_values, y_values):
            corr.update(x, y)

        streaming_r = corr.get_correlation()
        numpy_r = np.corrcoef(x_values, y_values)[0, 1]

        assert abs(streaming_r - numpy_r) < 1e-10

    def test_incremental_updates(self):
        """Test that incremental updates work correctly."""
        x_values = [1, 2, 3, 4, 5]
        y_values = [2, 4, 6, 8, 10]

        corr = StreamingCorrelation()
        for x, y in zip(x_values, y_values):
            corr.update(x, y)

        assert corr.n == 5
        assert abs(corr.get_correlation() - 1.0) < 1e-10

    def test_batch_update(self):
        """Test batch update method."""
        np.random.seed(42)
        x_values = np.random.normal(0, 1, 100)
        y_values = x_values * 0.8 + np.random.normal(0, 0.2, 100)

        corr = StreamingCorrelation()
        corr.update_batch(x_values, y_values)

        assert corr.n == 100
        r = corr.get_correlation()
        assert r is not None
        assert r > 0.9  # Should be highly correlated

    def test_reset(self):
        """Test reset method."""
        corr = StreamingCorrelation()
        corr.update(1, 2)
        corr.update(2, 4)

        assert corr.n == 2

        corr.reset()
        assert corr.n == 0
        assert corr.get_correlation() is None

    def test_insufficient_data(self):
        """Test behavior with insufficient data."""
        corr = StreamingCorrelation()
        assert corr.get_correlation() is None

        corr.update(1, 2)
        assert corr.get_correlation() is None  # Need at least 2


class TestStreamingCorrelationMatrix:
    """Tests for StreamingCorrelationMatrix multi-column correlation."""

    def test_initialization(self):
        """Test initialization."""
        matrix = StreamingCorrelationMatrix(['a', 'b', 'c'])
        assert matrix.columns == ['a', 'b', 'c']

    def test_update_from_dataframe_pandas(self):
        """Test updating from pandas DataFrame."""
        matrix = StreamingCorrelationMatrix(['x', 'y'])
        df = pd.DataFrame({
            'x': [1, 2, 3, 4, 5],
            'y': [2, 4, 6, 8, 10]
        })

        matrix.update_from_dataframe(df)

        corr_dict = matrix.get_correlation_dict()
        # Keys are in format 'col1|col2'
        assert 'x|y' in corr_dict or 'y|x' in corr_dict

    def test_correlation_matrix_as_numpy(self):
        """Test getting correlation matrix as numpy array."""
        matrix = StreamingCorrelationMatrix(['a', 'b'])
        df = pd.DataFrame({
            'a': [1, 2, 3, 4, 5],
            'b': [2, 4, 6, 8, 10]
        })
        matrix.update_from_dataframe(df)

        corr_matrix = matrix.get_correlation_matrix()
        assert isinstance(corr_matrix, np.ndarray)
        assert corr_matrix.shape == (2, 2)

    def test_positive_correlation_detected(self):
        """Test that positive correlation is detected."""
        matrix = StreamingCorrelationMatrix(['x', 'y'])
        df = pd.DataFrame({
            'x': list(range(100)),
            'y': list(range(100))
        })
        matrix.update_from_dataframe(df)

        corr_dict = matrix.get_correlation_dict()
        # Keys are in format 'col1|col2'
        key = 'x|y' if 'x|y' in corr_dict else 'y|x'
        assert corr_dict[key] > 0.99

    def test_negative_correlation_detected(self):
        """Test that negative correlation is detected."""
        matrix = StreamingCorrelationMatrix(['x', 'y'])
        df = pd.DataFrame({
            'x': list(range(100)),
            'y': list(range(99, -1, -1))
        })
        matrix.update_from_dataframe(df)

        corr_dict = matrix.get_correlation_dict()
        # Keys are in format 'col1|col2'
        key = 'x|y' if 'x|y' in corr_dict else 'y|x'
        assert corr_dict[key] < -0.99

    def test_chunked_processing(self):
        """Test that chunked processing gives same result."""
        np.random.seed(42)
        x_values = np.random.normal(0, 1, 1000)
        y_values = x_values * 0.7 + np.random.normal(0, 0.5, 1000)

        # Single batch
        matrix1 = StreamingCorrelationMatrix(['x', 'y'])
        matrix1.update_from_dataframe(pd.DataFrame({'x': x_values, 'y': y_values}))

        # Chunked
        matrix2 = StreamingCorrelationMatrix(['x', 'y'])
        for i in range(0, 1000, 100):
            chunk = pd.DataFrame({
                'x': x_values[i:i+100],
                'y': y_values[i:i+100]
            })
            matrix2.update_from_dataframe(chunk)

        corr1 = matrix1.get_correlation_dict()
        corr2 = matrix2.get_correlation_dict()

        # Keys are in format 'col1|col2'
        key = 'x|y' if 'x|y' in corr1 else 'y|x'
        assert abs(corr1[key] - corr2[key]) < 1e-10

    def test_reset(self):
        """Test reset method."""
        matrix = StreamingCorrelationMatrix(['a', 'b'])
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 4, 6]})
        matrix.update_from_dataframe(df)

        matrix.reset()
        corr_dict = matrix.get_correlation_dict()
        # After reset, correlations should be empty or None
        assert len(corr_dict) == 0 or all(v is None for v in corr_dict.values())


class TestIntegration:
    """Integration tests combining quantiles and correlation."""

    def test_quantiles_and_correlation_together(self):
        """Test using both streaming quantiles and correlation."""
        np.random.seed(42)
        data = np.random.normal(100, 15, 1000)

        # Compute quantiles
        sq = StreamingQuantiles([0.25, 0.50, 0.75])
        sq.update_batch(data.tolist())
        quantiles = sq.get_quantiles()

        assert 'p50' in quantiles
        assert abs(quantiles['p50'] - 100) < 5  # Should be near mean for normal dist

        # Compute correlation with related variable
        corr = StreamingCorrelation()
        related = data * 0.8 + np.random.normal(0, 5, 1000)
        for x, y in zip(data, related):
            corr.update(x, y)

        r = corr.get_correlation()
        assert r > 0.9  # Should be highly correlated
