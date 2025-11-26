"""
Sampling utilities for memory-efficient data profiling.

Provides reservoir sampling and statistical sampling methods to limit
memory usage when profiling large datasets.
"""

import numpy as np
import random
from typing import List, Any, Optional, Dict


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


class CardinalityEstimator:
    """
    Approximate cardinality estimation using HyperLogLog.

    Uses Polars' built-in approx_n_unique (HyperLogLog) when available,
    falls back to exact counting on samples for pandas.

    This solves the problem of unique value counts being capped at sample size
    (e.g., showing "10,000 unique" when actual is 2 million).
    """

    @staticmethod
    def estimate_cardinality_polars(
        file_path: str,
        columns: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Estimate cardinality for all columns using Polars HyperLogLog.

        This is memory-efficient and fast - scans data in chunks without
        loading the entire dataset into memory.

        Args:
            file_path: Path to parquet/csv file
            columns: Optional list of columns to analyze (default: all)

        Returns:
            Dict mapping column name to approximate unique count
        """
        try:
            import polars as pl

            # Use lazy scan for memory efficiency
            if file_path.endswith('.parquet'):
                lf = pl.scan_parquet(file_path)
            elif file_path.endswith('.csv'):
                lf = pl.scan_csv(file_path)
            else:
                # For other formats, try to load with pandas and convert
                return {}

            # Get column names if not specified
            if columns is None:
                columns = lf.columns

            # Compute approximate unique counts for all columns
            result = lf.select([
                pl.col(c).approx_n_unique().alias(f'{c}')
                for c in columns
            ]).collect()

            return {c: result[c][0] for c in columns}

        except ImportError:
            return {}
        except Exception:
            return {}

    @staticmethod
    def estimate_cardinality_chunked(
        data_iterator,
        columns: List[str],
        sample_rate: float = 0.01
    ) -> Dict[str, int]:
        """
        Estimate cardinality using chunked processing with sampling.

        Falls back method when Polars lazy scan is not available.
        Uses probabilistic counting with random sampling.

        Args:
            data_iterator: Iterator yielding data chunks (pandas DataFrames)
            columns: Columns to estimate cardinality for
            sample_rate: Fraction of rows to sample (default 1%)

        Returns:
            Dict mapping column name to estimated unique count
        """
        # Track unique values using sets with reservoir sampling
        unique_trackers: Dict[str, set] = {col: set() for col in columns}
        max_tracked = 100000  # Max unique values to track per column
        total_rows = 0

        for chunk in data_iterator:
            chunk_size = len(chunk)
            total_rows += chunk_size

            # Sample rows from this chunk
            if sample_rate < 1.0:
                sample_size = max(1, int(chunk_size * sample_rate))
                if hasattr(chunk, 'sample'):
                    chunk = chunk.sample(n=min(sample_size, len(chunk)), random_state=42)

            for col in columns:
                if col not in chunk.columns:
                    continue

                # Add values to tracker
                values = chunk[col].dropna().unique()

                if len(unique_trackers[col]) < max_tracked:
                    for v in values:
                        if len(unique_trackers[col]) < max_tracked:
                            unique_trackers[col].add(v)
                        else:
                            break

        # Estimate actual cardinality from sampled cardinality
        # If we hit the max, extrapolate based on sample rate
        result = {}
        for col, tracker in unique_trackers.items():
            tracked_count = len(tracker)
            if tracked_count >= max_tracked:
                # Extrapolate: if we found max_tracked in sample_rate of data,
                # actual unique count is likely much higher
                result[col] = int(tracked_count / sample_rate)
            else:
                result[col] = tracked_count

        return result


class AdaptiveSamplingStrategy:
    """
    Intelligent sampling strategy that adapts based on column characteristics.

    Combines multiple signals to determine optimal sampling:
    1. Column name semantics (id, date, amount, category)
    2. Approximate cardinality (from HyperLogLog)
    3. Data type (string, numeric, date)
    4. Dataset size

    Strategies:
    - LOW_CARDINALITY: Full value distribution (< 100 unique values)
    - MEDIUM_CARDINALITY: Reservoir sampling with stratification (100-10K unique)
    - HIGH_CARDINALITY: Random sampling for statistics only (> 10K unique)
    """

    # Cardinality thresholds
    LOW_CARDINALITY_THRESHOLD = 100
    MEDIUM_CARDINALITY_THRESHOLD = 10000
    HIGH_CARDINALITY_THRESHOLD = 100000

    @classmethod
    def determine_strategy(
        cls,
        column_name: str,
        approx_cardinality: int,
        total_rows: int,
        data_type: str
    ) -> Dict[str, Any]:
        """
        Determine optimal sampling strategy for a column.

        Args:
            column_name: Column name
            approx_cardinality: Approximate unique value count
            total_rows: Total rows in dataset
            data_type: Data type (string, integer, float, date, etc.)

        Returns:
            Dict with:
                - strategy: 'full', 'stratified', or 'random'
                - sample_size: Recommended sample size
                - reasoning: Human-readable explanation
                - full_scan_metrics: List of metrics to compute on full data
                - sample_metrics: List of metrics to compute on sample
        """
        cardinality_ratio = approx_cardinality / total_rows if total_rows > 0 else 0

        # Low cardinality: enumerate all values
        if approx_cardinality <= cls.LOW_CARDINALITY_THRESHOLD:
            return {
                'strategy': 'full',
                'sample_size': total_rows,
                'reasoning': f'Low cardinality ({approx_cardinality} unique values) - collecting full distribution',
                'full_scan_metrics': ['value_counts', 'null_count', 'row_count'],
                'sample_metrics': []
            }

        # Medium cardinality: stratified sampling to capture all values
        elif approx_cardinality <= cls.MEDIUM_CARDINALITY_THRESHOLD:
            # Sample enough to capture most unique values
            sample_size = min(approx_cardinality * 10, 50000, total_rows)
            return {
                'strategy': 'stratified',
                'sample_size': sample_size,
                'reasoning': f'Medium cardinality ({approx_cardinality:,} unique) - stratified sampling to capture value distribution',
                'full_scan_metrics': ['null_count', 'row_count', 'approx_unique'],
                'sample_metrics': ['value_counts', 'statistics', 'patterns']
            }

        # High cardinality: random sampling for statistics
        else:
            # For high cardinality, focus on statistics not value enumeration
            sample_size = min(10000, total_rows)
            return {
                'strategy': 'random',
                'sample_size': sample_size,
                'reasoning': f'High cardinality ({approx_cardinality:,} unique) - sampling for statistics only',
                'full_scan_metrics': ['null_count', 'row_count', 'approx_unique', 'min', 'max'],
                'sample_metrics': ['statistics', 'patterns', 'top_values']
            }

    @classmethod
    def get_full_scan_expressions_polars(cls, columns: List[str]) -> List:
        """
        Get Polars expressions for metrics that should be computed on full data.

        These are cheap O(n) operations that don't require sorting or grouping.

        Args:
            columns: List of column names

        Returns:
            List of Polars expressions
        """
        try:
            import polars as pl

            expressions = []
            for col in columns:
                expressions.extend([
                    pl.col(col).count().alias(f'{col}__count'),
                    pl.col(col).null_count().alias(f'{col}__null_count'),
                    pl.col(col).approx_n_unique().alias(f'{col}__approx_unique'),
                ])

            return expressions
        except ImportError:
            return []

    @classmethod
    def get_numeric_full_scan_expressions_polars(cls, columns: List[str]) -> List:
        """
        Get Polars expressions for numeric columns (min, max, mean, std).

        Args:
            columns: List of numeric column names

        Returns:
            List of Polars expressions
        """
        try:
            import polars as pl

            expressions = []
            for col in columns:
                expressions.extend([
                    pl.col(col).min().alias(f'{col}__min'),
                    pl.col(col).max().alias(f'{col}__max'),
                    pl.col(col).mean().alias(f'{col}__mean'),
                    pl.col(col).std().alias(f'{col}__std'),
                ])

            return expressions
        except ImportError:
            return []
