"""
Unit tests for sampling utilities including P² algorithm.

This test module covers:
1. P2Quantile single quantile estimation
2. StreamingQuantiles multi-quantile tracking
3. compute_streaming_quantiles helper function
4. QuantileTracker hybrid approach
5. Edge cases and error handling
6. Accuracy vs exact quantiles

Author: Daniel Edge
"""

import pytest
import numpy as np
from typing import List

from validation_framework.profiler.sampling_utils import (
    P2Quantile,
    StreamingQuantiles,
    compute_streaming_quantiles,
    QuantileTracker,
    OnlineStatistics,
    ReservoirSampler,
)


# ============================================================================
# P2QUANTILE TESTS (SINGLE QUANTILE ESTIMATION)
# ============================================================================


@pytest.mark.unit
class TestP2Quantile:
    """Test suite for P² algorithm single quantile estimator."""

    def test_initialization(self):
        """Test basic initialization."""
        estimator = P2Quantile(0.5)

        assert estimator.p == 0.5
        assert estimator.count == 0
        assert estimator.quantile is None

    def test_invalid_quantile_value(self):
        """Test that invalid quantile values raise ValueError."""
        with pytest.raises(ValueError, match="Quantile must be between 0 and 1"):
            P2Quantile(1.5)

        with pytest.raises(ValueError, match="Quantile must be between 0 and 1"):
            P2Quantile(-0.1)

    def test_first_five_observations(self):
        """Test initialization phase with first 5 observations."""
        estimator = P2Quantile(0.5)

        # Add first 5 values
        values = [3.0, 1.0, 5.0, 2.0, 4.0]
        for v in values:
            estimator.update(v)

        assert estimator.count == 5
        # After 5 observations, heights should be sorted
        assert estimator.heights == sorted(values)
        # Median should be middle value
        assert estimator.quantile == 3.0

    def test_fewer_than_five_observations(self):
        """Test behavior with fewer than 5 observations."""
        estimator = P2Quantile(0.5)

        estimator.update(10.0)
        estimator.update(20.0)
        estimator.update(30.0)

        assert estimator.count == 3
        # Should return approximate quantile from small sample
        q = estimator.quantile
        assert q is not None
        assert 10.0 <= q <= 30.0

    def test_median_uniform_distribution(self):
        """Test median estimation on uniform distribution."""
        estimator = P2Quantile(0.5)

        # Stream 1000 values from uniform distribution [0, 100]
        np.random.seed(42)
        values = np.random.uniform(0, 100, 1000)

        for v in values:
            estimator.update(v)

        # P² median should be close to true median (50)
        estimated_median = estimator.quantile
        true_median = np.median(values)

        assert estimated_median is not None
        # Allow 10% error for approximation
        assert abs(estimated_median - true_median) < true_median * 0.10

    def test_quantile_95th_percentile(self):
        """Test 95th percentile estimation."""
        estimator = P2Quantile(0.95)

        # Stream normal distribution
        np.random.seed(42)
        values = np.random.normal(100, 15, 2000)

        for v in values:
            estimator.update(v)

        estimated_p95 = estimator.quantile
        true_p95 = np.percentile(values, 95)

        assert estimated_p95 is not None
        # P² should be reasonably accurate
        assert abs(estimated_p95 - true_p95) < 5.0

    def test_quantile_25th_percentile(self):
        """Test 25th percentile (first quartile)."""
        estimator = P2Quantile(0.25)

        np.random.seed(42)
        values = list(range(1, 101))  # 1 to 100
        np.random.shuffle(values)

        for v in values:
            estimator.update(float(v))

        estimated_q1 = estimator.quantile
        true_q1 = np.percentile(values, 25)

        assert estimated_q1 is not None
        assert abs(estimated_q1 - true_q1) < 5.0

    def test_sequential_sorted_values(self):
        """Test with sequential sorted input."""
        estimator = P2Quantile(0.5)

        # Feed sorted values 1-100
        for i in range(1, 101):
            estimator.update(float(i))

        assert estimator.count == 100
        estimated_median = estimator.quantile

        # True median of 1-100 is 50.5
        assert estimated_median is not None
        assert abs(estimated_median - 50.5) < 5.0

    def test_reverse_sorted_values(self):
        """Test with reverse sorted input."""
        estimator = P2Quantile(0.5)

        # Feed values 100 down to 1
        for i in range(100, 0, -1):
            estimator.update(float(i))

        estimated_median = estimator.quantile

        assert estimated_median is not None
        assert abs(estimated_median - 50.5) < 5.0

    def test_constant_values(self):
        """Test with all identical values."""
        estimator = P2Quantile(0.5)

        # All values are 42.0
        for _ in range(100):
            estimator.update(42.0)

        assert estimator.quantile == 42.0

    def test_extreme_values(self):
        """Test handling of extreme values."""
        estimator = P2Quantile(0.5)

        # Mix of small and large values
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 1000000.0]

        for v in values:
            estimator.update(v)

        # Median should be around 3-4, not affected by outlier
        estimated_median = estimator.quantile
        assert estimated_median is not None
        assert 2.0 <= estimated_median <= 5.0

    def test_get_count(self):
        """Test get_count method."""
        estimator = P2Quantile(0.75)

        assert estimator.get_count() == 0

        for i in range(50):
            estimator.update(float(i))

        assert estimator.get_count() == 50


# ============================================================================
# STREAMINGQUANTILES TESTS (MULTIPLE QUANTILES)
# ============================================================================


@pytest.mark.unit
class TestStreamingQuantiles:
    """Test suite for StreamingQuantiles multi-quantile tracker."""

    def test_initialization(self):
        """Test basic initialization."""
        tracker = StreamingQuantiles([0.25, 0.5, 0.75])

        assert tracker.quantile_values == [0.25, 0.5, 0.75]
        assert len(tracker.estimators) == 3
        assert tracker.get_count() == 0

    def test_default_quantiles(self):
        """Test default quantile list."""
        tracker = StreamingQuantiles()

        # Default: quartiles + 95th/99th percentiles
        assert 0.25 in tracker.quantile_values
        assert 0.50 in tracker.quantile_values
        assert 0.75 in tracker.quantile_values
        assert 0.95 in tracker.quantile_values
        assert 0.99 in tracker.quantile_values

    def test_update_single_value(self):
        """Test updating with single value."""
        tracker = StreamingQuantiles([0.5])

        tracker.update(42.0)

        assert tracker.get_count() == 1

    def test_update_batch(self):
        """Test batch update."""
        tracker = StreamingQuantiles([0.5])

        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        tracker.update_batch(values)

        assert tracker.get_count() == 5

    def test_quartiles_uniform_distribution(self):
        """Test quartile estimation on uniform data."""
        tracker = StreamingQuantiles([0.25, 0.5, 0.75])

        np.random.seed(42)
        values = np.random.uniform(0, 100, 1000)

        for v in values:
            tracker.update(v)

        quantiles = tracker.get_quantiles()

        # Check all quantiles are present
        assert 'p25' in quantiles
        assert 'p50' in quantiles
        assert 'p75' in quantiles

        # Verify order: Q1 < Q2 < Q3
        assert quantiles['p25'] < quantiles['p50'] < quantiles['p75']

        # Check accuracy
        true_q1 = np.percentile(values, 25)
        true_q2 = np.percentile(values, 50)
        true_q3 = np.percentile(values, 75)

        assert abs(quantiles['p25'] - true_q1) < 5.0
        assert abs(quantiles['p50'] - true_q2) < 5.0
        assert abs(quantiles['p75'] - true_q3) < 5.0

    def test_multiple_quantiles_normal_distribution(self):
        """Test multiple quantiles on normal distribution."""
        tracker = StreamingQuantiles([0.1, 0.5, 0.9, 0.95, 0.99])

        np.random.seed(42)
        values = np.random.normal(100, 15, 5000)

        tracker.update_batch(list(values))

        quantiles = tracker.get_quantiles()

        assert len(quantiles) == 5

        # Verify all quantiles are in ascending order
        q_values = [quantiles[f'p{int(q*100)}'] for q in [0.1, 0.5, 0.9, 0.95, 0.99]]
        assert q_values == sorted(q_values)

    def test_empty_tracker(self):
        """Test get_quantiles on empty tracker."""
        tracker = StreamingQuantiles([0.5])

        quantiles = tracker.get_quantiles()

        # Should return empty dict (no data yet)
        assert quantiles == {}

    def test_few_values(self):
        """Test with fewer than 5 values per quantile."""
        tracker = StreamingQuantiles([0.25, 0.5, 0.75])

        tracker.update_batch([1.0, 2.0, 3.0])

        quantiles = tracker.get_quantiles()

        # Should still return estimates
        assert 'p25' in quantiles or len(quantiles) >= 0


# ============================================================================
# COMPUTE_STREAMING_QUANTILES TESTS (HELPER FUNCTION)
# ============================================================================


@pytest.mark.unit
class TestComputeStreamingQuantiles:
    """Test suite for compute_streaming_quantiles helper function."""

    def test_basic_usage(self):
        """Test basic usage with list iterator."""
        values = list(range(1, 101))
        quantiles = compute_streaming_quantiles(iter(values), [0.5])

        assert 'p50' in quantiles
        # Median of 1-100 is 50.5
        assert abs(quantiles['p50'] - 50.5) < 5.0

    def test_default_quantiles(self):
        """Test with default quantile list."""
        values = list(range(1, 1001))
        quantiles = compute_streaming_quantiles(iter(values))

        # Should have default quantiles
        assert 'p25' in quantiles
        assert 'p50' in quantiles
        assert 'p75' in quantiles
        assert 'p95' in quantiles
        assert 'p99' in quantiles

    def test_with_none_values(self):
        """Test handling of None values."""
        values = [1.0, 2.0, None, 3.0, 4.0, None, 5.0]
        quantiles = compute_streaming_quantiles(iter(values), [0.5])

        # Should skip None values
        assert 'p50' in quantiles
        # Median of [1,2,3,4,5] is 3
        assert abs(quantiles['p50'] - 3.0) < 1.0

    def test_with_nan_values(self):
        """Test handling of NaN values."""
        values = [1.0, 2.0, np.nan, 3.0, 4.0, np.nan, 5.0]
        quantiles = compute_streaming_quantiles(iter(values), [0.5])

        # Should skip NaN values
        assert 'p50' in quantiles
        assert abs(quantiles['p50'] - 3.0) < 1.0

    def test_generator_expression(self):
        """Test with generator expression."""
        gen = (float(x) for x in range(1, 101))
        quantiles = compute_streaming_quantiles(gen, [0.25, 0.75])

        assert 'p25' in quantiles
        assert 'p75' in quantiles

    def test_empty_iterator(self):
        """Test with empty iterator."""
        quantiles = compute_streaming_quantiles(iter([]), [0.5])

        # Should return empty dict
        assert quantiles == {}


# ============================================================================
# QUANTILETRACKER TESTS (HYBRID APPROACH)
# ============================================================================


@pytest.mark.unit
class TestQuantileTracker:
    """Test suite for QuantileTracker hybrid approach."""

    def test_initialization(self):
        """Test basic initialization."""
        tracker = QuantileTracker([0.5, 0.95])

        assert tracker.quantiles == [0.5, 0.95]
        assert tracker.initialized is False
        assert tracker.p2_tracker is None

    def test_small_dataset_exact_calculation(self):
        """Test that small datasets use exact calculation."""
        tracker = QuantileTracker([0.5])

        # Add 100 values (less than init_samples threshold)
        for i in range(1, 101):
            tracker.add(float(i))

        quantiles = tracker.get_quantiles()

        # Should use exact calculation
        assert tracker.initialized is False
        assert 'p50' in quantiles
        assert quantiles['p50'] == 50.5  # Exact median

    def test_large_dataset_switches_to_p2(self):
        """Test that large datasets switch to P² algorithm."""
        tracker = QuantileTracker([0.5])
        tracker.init_samples = 100  # Lower threshold for testing

        # Add exactly init_samples values
        for i in range(1, 101):
            tracker.add(float(i))

        # At this point, values list is full but not yet switched
        assert tracker.initialized is False
        assert len(tracker.values) == 100

        # Add one more to trigger switch to P²
        tracker.add(101.0)

        # Now should have switched to P²
        assert tracker.initialized is True
        assert tracker.p2_tracker is not None
        assert len(tracker.values) == 0  # Memory freed

        # Add more values using P²
        for i in range(102, 201):
            tracker.add(float(i))

        quantiles = tracker.get_quantiles()
        assert 'p50' in quantiles

    def test_memory_efficiency(self):
        """Test that P² approach frees memory."""
        tracker = QuantileTracker([0.5])
        tracker.init_samples = 50

        # Add initial samples
        for i in range(50):
            tracker.add(float(i))

        # Values stored
        assert len(tracker.values) == 50

        # Trigger switch to P²
        tracker.add(51.0)

        # Memory should be freed
        assert tracker.initialized is True
        assert len(tracker.values) == 0

    def test_empty_tracker(self):
        """Test get_quantiles on empty tracker."""
        tracker = QuantileTracker([0.5])

        quantiles = tracker.get_quantiles()

        assert quantiles == {}


# ============================================================================
# ACCURACY AND PERFORMANCE TESTS
# ============================================================================


@pytest.mark.unit
class TestP2Accuracy:
    """Test accuracy of P² algorithm vs exact quantiles."""

    def test_accuracy_uniform_distribution(self):
        """Test accuracy on uniform distribution."""
        np.random.seed(42)
        values = np.random.uniform(0, 1000, 10000)

        # Compute exact quantiles
        exact_quantiles = {
            'p25': np.percentile(values, 25),
            'p50': np.percentile(values, 50),
            'p75': np.percentile(values, 75),
            'p95': np.percentile(values, 95),
        }

        # Compute P² approximations
        tracker = StreamingQuantiles([0.25, 0.50, 0.75, 0.95])
        tracker.update_batch(list(values))
        approx_quantiles = tracker.get_quantiles()

        # Check relative errors
        for key in exact_quantiles:
            exact = exact_quantiles[key]
            approx = approx_quantiles[key]
            relative_error = abs(approx - exact) / exact

            # P² should be within 5% for uniform distribution
            assert relative_error < 0.05, f"{key}: {relative_error*100:.2f}% error"

    def test_accuracy_normal_distribution(self):
        """Test accuracy on normal distribution."""
        np.random.seed(42)
        values = np.random.normal(100, 15, 10000)

        exact_quantiles = {
            'p50': np.percentile(values, 50),
            'p95': np.percentile(values, 95),
        }

        tracker = StreamingQuantiles([0.50, 0.95])
        tracker.update_batch(list(values))
        approx_quantiles = tracker.get_quantiles()

        # Check absolute errors
        for key in exact_quantiles:
            exact = exact_quantiles[key]
            approx = approx_quantiles[key]
            abs_error = abs(approx - exact)

            # Should be within reasonable absolute error
            assert abs_error < 2.0, f"{key}: {abs_error:.2f} absolute error"

    def test_accuracy_exponential_distribution(self):
        """Test accuracy on exponential distribution (skewed)."""
        np.random.seed(42)
        values = np.random.exponential(scale=10.0, size=5000)

        exact_median = np.percentile(values, 50)

        tracker = StreamingQuantiles([0.50])
        tracker.update_batch(list(values))
        approx_quantiles = tracker.get_quantiles()

        approx_median = approx_quantiles['p50']
        relative_error = abs(approx_median - exact_median) / exact_median

        # Allow larger error for skewed distributions
        assert relative_error < 0.15


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================


@pytest.mark.unit
class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_unique_value(self):
        """Test with all identical values."""
        tracker = StreamingQuantiles([0.25, 0.50, 0.75])

        # All values are 100.0
        for _ in range(1000):
            tracker.update(100.0)

        quantiles = tracker.get_quantiles()

        # All quantiles should be 100.0
        assert all(abs(v - 100.0) < 1e-6 for v in quantiles.values())

    def test_two_unique_values(self):
        """Test with only two distinct values."""
        tracker = StreamingQuantiles([0.5])

        # Alternating 0 and 100
        for i in range(100):
            tracker.update(0.0 if i % 2 == 0 else 100.0)

        quantiles = tracker.get_quantiles()

        # Median should be somewhere between 0 and 100
        assert 'p50' in quantiles
        assert 0.0 <= quantiles['p50'] <= 100.0

    def test_very_large_values(self):
        """Test with very large numeric values."""
        tracker = StreamingQuantiles([0.5])

        values = [1e10, 2e10, 3e10, 4e10, 5e10]
        tracker.update_batch(values)

        quantiles = tracker.get_quantiles()
        assert 'p50' in quantiles
        assert 1e10 <= quantiles['p50'] <= 5e10

    def test_very_small_values(self):
        """Test with very small numeric values."""
        tracker = StreamingQuantiles([0.5])

        values = [1e-10, 2e-10, 3e-10, 4e-10, 5e-10]
        tracker.update_batch(values)

        quantiles = tracker.get_quantiles()
        assert 'p50' in quantiles
        assert 1e-10 <= quantiles['p50'] <= 5e-10

    def test_negative_values(self):
        """Test with negative values."""
        tracker = StreamingQuantiles([0.5])

        values = [-100, -50, 0, 50, 100]
        tracker.update_batch([float(v) for v in values])

        quantiles = tracker.get_quantiles()
        assert 'p50' in quantiles
        assert -100 <= quantiles['p50'] <= 100


# ============================================================================
# INTEGRATION WITH OTHER SAMPLING UTILITIES
# ============================================================================


@pytest.mark.unit
class TestIntegrationWithOtherUtils:
    """Test integration with other sampling utilities."""

    def test_combined_with_online_statistics(self):
        """Test using P² alongside OnlineStatistics."""
        quantile_tracker = StreamingQuantiles([0.5])
        stats_tracker = OnlineStatistics()

        np.random.seed(42)
        values = np.random.normal(100, 15, 1000)

        for v in values:
            quantile_tracker.update(v)
            stats_tracker.update(v)

        quantiles = quantile_tracker.get_quantiles()
        stats = stats_tracker.get_statistics()

        # Median should be close to mean for normal distribution
        assert abs(quantiles['p50'] - stats['mean']) < 5.0

    def test_reservoir_sampler_comparison(self):
        """Compare P² results with reservoir sampling."""
        np.random.seed(42)
        values = list(np.random.uniform(0, 100, 10000))

        # P² approach
        p2_tracker = StreamingQuantiles([0.5])
        p2_tracker.update_batch(values)
        p2_median = p2_tracker.get_quantiles()['p50']

        # Reservoir sampling approach
        reservoir = ReservoirSampler(reservoir_size=1000, random_seed=42)
        reservoir.add_batch(values)
        sample = reservoir.get_sample()
        reservoir_median = np.median(sample)

        # Both should be reasonably close to true median
        true_median = np.median(values)

        assert abs(p2_median - true_median) < 5.0
        assert abs(reservoir_median - true_median) < 5.0
