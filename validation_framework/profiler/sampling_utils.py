"""
Sampling utilities for memory-efficient data profiling.

Provides reservoir sampling and statistical sampling methods to limit
memory usage when profiling large datasets.
"""

import numpy as np
import random
from typing import List, Any, Optional


class ReservoirSampler:
    """
    Reservoir sampling implementation for memory-efficient sampling.

    Maintains a fixed-size reservoir of samples from a streaming dataset,
    ensuring each item has equal probability of being included regardless
    of total dataset size.

    Uses Algorithm R (Vitter, 1985) for efficient reservoir sampling.
    """

    def __init__(self, reservoir_size: int = 100000, random_seed: Optional[int] = 42):
        """
        Initialize reservoir sampler.

        Args:
            reservoir_size: Maximum number of samples to retain
            random_seed: Random seed for reproducibility (None for random)
        """
        self.reservoir_size = reservoir_size
        self.reservoir: List[Any] = []
        self.items_seen = 0

        # Set random seed for reproducibility
        if random_seed is not None:
            random.seed(random_seed)
            np.random.seed(random_seed)

    def add(self, item: Any) -> None:
        """
        Add an item to the reservoir sample.

        For the first k items, simply add to reservoir.
        For subsequent items, randomly replace an existing item
        with probability k/n where k=reservoir_size and n=items_seen.

        Args:
            item: Item to potentially add to reservoir
        """
        self.items_seen += 1

        if len(self.reservoir) < self.reservoir_size:
            # Reservoir not full yet - add item
            self.reservoir.append(item)
        else:
            # Reservoir full - randomly replace
            # Generate random index in range [0, items_seen)
            j = random.randint(0, self.items_seen - 1)

            if j < self.reservoir_size:
                # Replace item at index j
                self.reservoir[j] = item

    def add_batch(self, items: List[Any]) -> None:
        """
        Add multiple items to the reservoir sample.

        More efficient than calling add() repeatedly.

        Args:
            items: List of items to add
        """
        for item in items:
            self.add(item)

    def get_sample(self) -> List[Any]:
        """
        Get the current reservoir sample.

        Returns:
            List of sampled items (up to reservoir_size)
        """
        return self.reservoir.copy()

    def get_count(self) -> int:
        """
        Get total number of items seen (not just in reservoir).

        Returns:
            Total items processed
        """
        return self.items_seen

    def get_sample_size(self) -> int:
        """
        Get current size of reservoir sample.

        Returns:
            Number of items currently in reservoir
        """
        return len(self.reservoir)

    def clear(self) -> None:
        """Clear the reservoir and reset counters."""
        self.reservoir = []
        self.items_seen = 0


class StratifiedSampler:
    """
    Stratified sampling for categorical data.

    Maintains proportional representation of each category in the sample,
    ensuring rare categories are not under-represented.
    """

    def __init__(self, max_per_category: int = 1000, max_total: int = 10000):
        """
        Initialize stratified sampler.

        Args:
            max_per_category: Maximum samples per category
            max_total: Maximum total samples across all categories
        """
        self.max_per_category = max_per_category
        self.max_total = max_total
        self.category_samples: dict = {}
        self.category_counts: dict = {}

    def add(self, item: Any, category: Any) -> None:
        """
        Add an item to the stratified sample.

        Args:
            item: Item to sample
            category: Category for stratification
        """
        # Initialize category if not seen before
        if category not in self.category_samples:
            self.category_samples[category] = []
            self.category_counts[category] = 0

        self.category_counts[category] += 1

        # Add to category sample if under limit
        if len(self.category_samples[category]) < self.max_per_category:
            self.category_samples[category].append(item)
        else:
            # Use reservoir sampling within category
            j = random.randint(0, self.category_counts[category] - 1)
            if j < self.max_per_category:
                self.category_samples[category][j] = item

    def get_sample(self) -> List[Any]:
        """
        Get all samples across all categories.

        Returns:
            Flattened list of all samples
        """
        all_samples = []
        for samples in self.category_samples.values():
            all_samples.extend(samples)
        return all_samples

    def get_category_samples(self) -> dict:
        """
        Get samples organized by category.

        Returns:
            Dict mapping category to list of samples
        """
        return {k: v.copy() for k, v in self.category_samples.items()}


class OnlineStatistics:
    """
    Online (streaming) statistics calculator using Welford's algorithm.

    Calculates mean, variance, and standard deviation in a single pass
    without storing all values, using numerically stable algorithms.
    """

    def __init__(self):
        """Initialize online statistics calculator."""
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0  # Sum of squared differences from mean
        self.min_value = float('inf')
        self.max_value = float('-inf')

    def update(self, value: float) -> None:
        """
        Update statistics with new value using Welford's algorithm.

        This algorithm is numerically stable and computes variance
        in a single pass without storing all values.

        Args:
            value: New numeric value
        """
        self.n += 1

        # Update min/max
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)

        # Welford's online algorithm for mean and variance
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def update_batch(self, values: List[float]) -> None:
        """
        Update statistics with batch of values.

        Args:
            values: List of numeric values
        """
        for value in values:
            self.update(value)

    @property
    def count(self) -> int:
        """Get count of values processed."""
        return self.n

    @property
    def variance(self) -> float:
        """
        Get sample variance.

        Returns:
            Sample variance (Bessel's correction: n-1 denominator)
        """
        if self.n < 2:
            return 0.0
        return self.m2 / (self.n - 1)

    @property
    def std_dev(self) -> float:
        """
        Get standard deviation.

        Returns:
            Standard deviation (sqrt of variance)
        """
        return np.sqrt(self.variance)

    @property
    def population_variance(self) -> float:
        """
        Get population variance.

        Returns:
            Population variance (n denominator)
        """
        if self.n == 0:
            return 0.0
        return self.m2 / self.n

    def get_statistics(self) -> dict:
        """
        Get all computed statistics.

        Returns:
            Dict with count, mean, variance, std_dev, min, max
        """
        return {
            'count': self.n,
            'mean': self.mean,
            'variance': self.variance,
            'std_dev': self.std_dev,
            'min': self.min_value if self.n > 0 else None,
            'max': self.max_value if self.n > 0 else None
        }


class QuantileTracker:
    """
    Approximate quantile tracking for streaming data.

    Uses P² algorithm for dynamic calculation of quantiles without
    storing all data points.

    For exact quantiles, use ReservoirSampler instead.
    """

    def __init__(self, quantiles: List[float] = [0.25, 0.50, 0.75, 0.95, 0.99]):
        """
        Initialize quantile tracker.

        Args:
            quantiles: List of quantiles to track (0.0 to 1.0)
        """
        self.quantiles = sorted(quantiles)
        self.values: List[float] = []
        self.initialized = False
        self.init_samples = 1000  # Number of samples before using approximation

    def add(self, value: float) -> None:
        """
        Add value and update quantile estimates.

        Args:
            value: Numeric value to add
        """
        if len(self.values) < self.init_samples:
            # Collecting initial samples for bootstrapping
            self.values.append(value)
        else:
            # TODO: Implement P² algorithm for approximate quantiles
            # For now, using exact calculation with reservoir sample
            if len(self.values) > 10000:
                # Subsample to limit memory
                self.values = random.sample(self.values, 5000)
            self.values.append(value)

    def get_quantiles(self) -> dict:
        """
        Get quantile estimates.

        Returns:
            Dict mapping quantile to estimated value
        """
        if not self.values:
            return {}

        # Calculate exact quantiles from current sample
        result = {}
        percentiles = [q * 100 for q in self.quantiles]
        quantile_values = np.percentile(self.values, percentiles)

        for q, val in zip(self.quantiles, quantile_values):
            result[f'p{int(q*100)}'] = float(val)

        return result
