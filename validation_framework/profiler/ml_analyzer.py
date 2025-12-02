#!/usr/bin/env python3
"""
ML-based Data Quality Analyzer

Provides machine learning-based analysis for detecting:
- Outliers using Isolation Forest (univariate and multivariate)
- Clustering analysis using DBSCAN
- Format inconsistencies in string fields
- Cross-column consistency issues
- Rare/suspicious categorical values
- Temporal pattern anomalies
- Correlation-based anomaly detection

Enable with --beta-ml flag.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter
import re
import logging
import time
import warnings

# Import visualization fallback utilities for robust numeric detection
from validation_framework.profiler.visualization_fallbacks import (
    coerce_to_numeric,
    is_numeric_for_analysis,
    should_apply_benford_generic,
    extract_benford_digits,
    is_identifier_like,
    is_binary_column,
    is_bounded_data,
    get_semantic_type_hint
)

logger = logging.getLogger(__name__)

# Default sample size threshold - if file has more rows than this, sampling is applied
# No hard cap - user controls via --analysis-sample-size parameter
DEFAULT_SAMPLE_THRESHOLD = 100_000

# Minimum rows required for reliable ML analysis
MIN_ROWS_FOR_ML = 500


class ChunkedMLAccumulator:
    """
    Accumulates ML analysis statistics across chunks for memory-efficient processing.

    Instead of loading all data at once, this class collects statistics during
    the main chunk processing loop and finalizes analysis after all chunks.
    """

    def __init__(self, column_semantic_info: Optional[Dict[str, Dict[str, Any]]] = None):
        """Initialize accumulators for each ML analysis type."""
        self._column_semantic_info = column_semantic_info or {}
        self._fibo_taxonomy = self._load_fibo_taxonomy()

        # Accumulators
        self.total_rows = 0
        self.benford_digit_counts: Dict[str, Counter] = {}  # col -> Counter of first digits
        self.value_counts: Dict[str, Counter] = {}  # col -> Counter of values (for rare categories)
        self.format_pattern_counts: Dict[str, Counter] = {}  # col -> Counter of patterns
        self.numeric_stats: Dict[str, Dict] = {}  # col -> {sum, sum_sq, count, min, max}
        self.numeric_histograms: Dict[str, Dict] = {}  # col -> streaming histogram for percentiles
        self.column_types: Dict[str, str] = {}  # col -> 'numeric' or 'string'

        # Histogram settings for streaming percentiles
        self.histogram_bins = 1000  # Number of bins for percentile estimation

        # Sample reservoir for autoencoder (reservoir sampling)
        # Size scales with expected data - no arbitrary cap
        self.reservoir_size = 100000  # Initial size, can grow
        self.reservoir_samples: List[Dict] = []
        self.seen_count = 0
        self.max_reservoir_size = 1000000  # Allow up to 1M samples for autoencoder

        # Target/Feature Analysis accumulators (memory-efficient streaming)
        self.detected_targets: List[str] = []  # Detected target columns
        self.target_value_counts: Dict[str, Counter] = {}  # target_col -> Counter of target values
        # Streaming target-feature associations: {target_col: {feature_col: {target_val: Counter(feature_vals)}}}
        self.target_feature_counts: Dict[str, Dict[str, Dict[str, Counter]]] = {}
        # Missingness correlation with target: {target_col: {feature_col: {target_val: {'missing': n, 'present': n}}}}
        self.missingness_by_target: Dict[str, Dict[str, Dict[str, Dict[str, int]]]] = {}
        # Streaming numeric stats by target class: {target_col: {feature_col: {target_val: {sum, sum_sq, count}}}}
        self.numeric_by_target: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}
        # Mixed-type correlation accumulators (numeric-categorical, categorical-categorical)
        # For correlation ratio (numeric vs categorical): {cat_col: {cat_val: {num_col: {sum, sum_sq, count}}}}
        self.correlation_ratio_stats: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}

        # Reservoir samples for numeric columns (used for visualizations)
        self.numeric_samples: Dict[str, List[float]] = {}  # col -> sample values for scatter plots
        self.max_numeric_samples = 10000  # Max samples per column for memory efficiency

    def _load_fibo_taxonomy(self) -> Dict[str, Any]:
        """Load FIBO taxonomy for intelligent analysis."""
        import json
        from pathlib import Path
        taxonomy_path = Path(__file__).parent / "taxonomies" / "finance_taxonomy.json"
        try:
            with open(taxonomy_path, 'r') as f:
                taxonomy = json.load(f)
                flattened = {}
                for category, cat_data in taxonomy.get("taxonomy", {}).items():
                    for tag_name, tag_def in cat_data.get("tags", {}).items():
                        flattened[tag_name] = tag_def
                return flattened
        except Exception:
            return {}

    def _is_robust_numeric_column(self, series: pd.Series, col_name: str) -> bool:
        """
        Robust numeric column detection following generic rules.

        A column is considered numeric if:
        - It can be coerced to float (pd.to_numeric with errors="coerce")
        - After coercion, it has > 1 unique numeric value
        - It is not binary (≤ 3 unique numeric values)

        Works for:
        - float columns
        - integer columns
        - numeric strings (e.g. "100", "20.5", " ")
        - mixed dtype columns that become numeric after coercion

        Args:
            series: Column data
            col_name: Column name (for semantic hints)

        Returns:
            True if column should be treated as numeric for analysis
        """
        # First check native dtype
        if pd.api.types.is_numeric_dtype(series):
            # Check if it's binary/low-cardinality (likely categorical encoded as numeric)
            valid = series.dropna()
            if len(valid) > 0:
                unique_count = valid.nunique()
                if unique_count <= 3:
                    # Binary or very low cardinality - treat as categorical
                    return False
            return True

        # Try coercion for non-numeric dtypes
        coerced, is_valid = coerce_to_numeric(series)
        if not is_valid:
            return False

        # Additional check: if identifier-like, don't treat as numeric
        if is_identifier_like(col_name, series):
            return False

        return True

    def _detect_target_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect likely target/label columns using semantic types and patterns.

        Uses semantic-based detection (FIBO + Schema.org) plus generic patterns:
        1. Semantic types: schema:Boolean, schema:CategoryCode, category.*, status
        2. Pattern matching: 'is_*', 'has_*', '*_flag', '*_indicator'
        3. Generic keywords: 'target', 'label', 'class', 'outcome', 'y', 'status'
        4. Binary columns with low cardinality (2 unique values)

        Returns:
            List of detected target column names
        """
        # Generic target keywords (domain-agnostic, no hardcoded field names)
        generic_keywords = [
            'target', 'label', 'class', 'outcome', 'response', 'y',
            'status', 'result', 'decision', 'category', 'segment',
            'type', 'grade', 'rating', 'level', 'tier'
        ]
        prefix_patterns = ['is_', 'has_', 'was_', 'did_', 'will_']
        suffix_patterns = ['_flag', '_indicator', '_label', '_target', '_class', '_outcome', '_status', '_type', '_category']

        # Semantic types indicating targets (FIBO + Schema.org)
        target_semantic_types = {
            'schema:Boolean', 'schema:CategoryCode', 'schema:ItemList',
            'category.transaction_type', 'category.account_type', 'category.payment_method',
            'status'
        }

        def is_keyword_present(col_lower, keywords):
            """Check if keyword is present as a whole word (not substring)."""
            for kw in keywords:
                if col_lower == kw:
                    return True
                if f'_{kw}' in col_lower or f'{kw}_' in col_lower:
                    return True
                if col_lower.startswith(f'{kw}_') or col_lower.endswith(f'_{kw}'):
                    return True
            return False

        detected = []
        for col in df.columns:
            if col is None:
                continue
            col_lower = str(col).lower().strip()

            # Check semantic type match (from column_semantic_info)
            sem_info = self._column_semantic_info.get(col, {})
            resolved = sem_info.get('resolved', {})
            primary_type = resolved.get('primary_type', '') or sem_info.get('primary_tag', '')
            is_semantic_target = primary_type in target_semantic_types or (
                '.' in primary_type and primary_type.split('.')[0] == 'category'
            )

            # Check generic keyword match
            is_keyword_match = is_keyword_present(col_lower, generic_keywords)

            # Check prefix/suffix patterns
            is_prefix_match = any(col_lower.startswith(p) for p in prefix_patterns)
            is_suffix_match = any(col_lower.endswith(s) for s in suffix_patterns)

            # Check cardinality
            try:
                n_unique = df[col].nunique(dropna=True)
                is_binary = n_unique == 2
                is_low_cardinality = n_unique <= 5
            except Exception:
                is_binary = False
                is_low_cardinality = False

            # Detection logic (semantic-first):
            # - Semantic type match with low cardinality -> target
            # - Generic keyword match with low cardinality -> target
            # - Prefix/suffix pattern with binary -> target
            if is_semantic_target and is_low_cardinality:
                detected.append(col)
            elif is_keyword_match and is_low_cardinality:
                detected.append(col)
            elif (is_prefix_match or is_suffix_match) and is_binary:
                detected.append(col)

        return detected

    def process_chunk(self, chunk: pd.DataFrame, chunk_idx: int) -> None:
        """
        Process a chunk of data and accumulate statistics.

        Uses robust numeric detection that:
        - Coerces string columns to numeric where possible
        - Excludes binary/low-cardinality columns from numeric analysis
        - Works for float, int, numeric strings, and mixed dtype columns

        Args:
            chunk: DataFrame chunk to process
            chunk_idx: Index of current chunk (0-based)
        """
        self.total_rows += len(chunk)

        # Identify column types on first chunk using ROBUST detection
        if chunk_idx == 0:
            for col in chunk.columns:
                # Use robust numeric detection instead of simple dtype check
                is_numeric = self._is_robust_numeric_column(chunk[col], col)

                if is_numeric:
                    self.column_types[col] = 'numeric'
                    self.benford_digit_counts[col] = Counter()
                    self.numeric_stats[col] = {'sum': 0, 'sum_sq': 0, 'count': 0, 'min': float('inf'), 'max': float('-inf')}
                    # Streaming histogram for percentiles - initialized after first pass to know range
                    self.numeric_histograms[col] = {'bins': None, 'counts': None, 'initialized': False}
                    # Store Benford applicability for this column
                    self._benford_applicability = getattr(self, '_benford_applicability', {})
                    should_apply, reason = should_apply_benford_generic(chunk[col], col)
                    self._benford_applicability[col] = {'applicable': should_apply, 'reason': reason}
                else:
                    self.column_types[col] = 'string'
                    self.value_counts[col] = Counter()
                    self.format_pattern_counts[col] = Counter()

            # Detect target columns on first chunk
            self.detected_targets = self._detect_target_columns(chunk)
            # Initialize target analysis accumulators
            for target_col in self.detected_targets:
                self.target_value_counts[target_col] = Counter()
                self.target_feature_counts[target_col] = {}
                self.missingness_by_target[target_col] = {}
                self.numeric_by_target[target_col] = {}

        # Process each column
        for col in chunk.columns:
            if self.column_types.get(col) == 'numeric':
                self._accumulate_numeric(col, chunk[col])
            else:
                self._accumulate_string(col, chunk[col])

        # Accumulate target-feature statistics (streaming)
        self._accumulate_target_statistics(chunk)

        # Reservoir sampling for autoencoder training
        self._reservoir_sample(chunk)

    def _accumulate_numeric(self, col: str, series: pd.Series) -> None:
        """
        Accumulate statistics for a numeric column.

        Performs coercion BEFORE any analysis to handle numeric strings
        and mixed dtype columns.
        """
        # ROBUST: Always coerce to numeric first to handle numeric strings
        if not pd.api.types.is_numeric_dtype(series):
            series = pd.to_numeric(series, errors='coerce')

        valid = series.dropna()
        if len(valid) == 0:
            return

        # Running statistics for outlier detection
        stats = self.numeric_stats[col]
        stats['sum'] += valid.sum()
        stats['sum_sq'] += (valid ** 2).sum()
        stats['count'] += len(valid)
        stats['min'] = min(stats['min'], valid.min())
        stats['max'] = max(stats['max'], valid.max())

        # Reservoir sampling for numeric values (used in visualizations)
        if col not in self.numeric_samples:
            self.numeric_samples[col] = []
        current_samples = self.numeric_samples[col]
        if len(current_samples) < self.max_numeric_samples:
            # Still filling reservoir - take values directly
            space_left = self.max_numeric_samples - len(current_samples)
            sample_vals = valid.values[:space_left].tolist()
            current_samples.extend(sample_vals)
        else:
            # Reservoir full - use reservoir sampling algorithm
            import random
            for i, val in enumerate(valid.values[:1000]):  # Limit to prevent excessive iteration
                j = random.randint(0, stats['count'] - 1)
                if j < self.max_numeric_samples:
                    current_samples[j] = val

        # Update streaming histogram for percentile calculation
        hist_data = self.numeric_histograms[col]
        if not hist_data['initialized']:
            # Initialize histogram bins based on data range from this chunk
            # Will be refined as we see more data
            col_min, col_max = valid.min(), valid.max()
            if col_min != col_max:
                hist_data['range_min'] = col_min
                hist_data['range_max'] = col_max
                hist_data['counts'] = np.zeros(self.histogram_bins, dtype=np.int64)
                hist_data['initialized'] = True

        if hist_data['initialized']:
            # Expand range if needed
            col_min, col_max = valid.min(), valid.max()
            if col_min < hist_data['range_min'] or col_max > hist_data['range_max']:
                # Need to rebuild histogram with expanded range
                old_counts = hist_data['counts']
                old_min, old_max = hist_data['range_min'], hist_data['range_max']
                new_min = min(col_min, old_min)
                new_max = max(col_max, old_max)

                # Create new histogram with expanded range
                new_counts = np.zeros(self.histogram_bins, dtype=np.int64)

                # Redistribute old counts to new bins
                if old_max > old_min:
                    old_bin_width = (old_max - old_min) / self.histogram_bins
                    new_bin_width = (new_max - new_min) / self.histogram_bins
                    for i, count in enumerate(old_counts):
                        if count > 0:
                            # Map old bin center to new bin
                            old_center = old_min + (i + 0.5) * old_bin_width
                            new_bin = int((old_center - new_min) / new_bin_width)
                            new_bin = max(0, min(self.histogram_bins - 1, new_bin))
                            new_counts[new_bin] += count

                hist_data['counts'] = new_counts
                hist_data['range_min'] = new_min
                hist_data['range_max'] = new_max

            # Add current chunk values to histogram
            bin_width = (hist_data['range_max'] - hist_data['range_min']) / self.histogram_bins
            if bin_width > 0:
                bin_indices = ((valid - hist_data['range_min']) / bin_width).astype(int)
                bin_indices = np.clip(bin_indices, 0, self.histogram_bins - 1)
                # Efficient counting using numpy bincount
                chunk_counts = np.bincount(bin_indices, minlength=self.histogram_bins)
                hist_data['counts'] += chunk_counts[:self.histogram_bins]

        # Benford's law - count first digits (no sampling - just counting)
        # Check applicability first (stored during column type detection)
        benford_applicable = getattr(self, '_benford_applicability', {}).get(col, {}).get('applicable', True)
        if benford_applicable:
            positive_vals = valid[valid > 0]
            if len(positive_vals) > 0:
                # Extract first digit using SAFE regex method
                def safe_first_digit(x):
                    """Extract first significant digit using safe regex."""
                    try:
                        s = str(abs(float(x))).lstrip('0').lstrip('.')
                        if not s:
                            return 0
                        match = re.search(r'([1-9])', s)
                        return int(match.group(1)) if match else 0
                    except (ValueError, TypeError):
                        return 0

                first_digits = positive_vals.apply(safe_first_digit)
                first_digits = first_digits[first_digits > 0]  # Benford only applies to 1-9
                # Use value_counts() for memory efficiency (no list creation)
                digit_counts = first_digits.value_counts()
                for digit, count in digit_counts.items():
                    self.benford_digit_counts[col][digit] += count

    def _accumulate_string(self, col: str, series: pd.Series) -> None:
        """Accumulate statistics for a string column."""
        valid = series.dropna().astype(str)
        if len(valid) == 0:
            return

        # Value counts for rare category detection
        # Track up to 100K unique values - Counter is memory efficient
        max_unique_values = 100000
        if len(self.value_counts[col]) < max_unique_values:
            # Use pandas value_counts() - memory efficient
            val_counts = valid.value_counts()
            for val, count in val_counts.items():
                if len(self.value_counts[col]) >= max_unique_values:
                    break
                self.value_counts[col][val] += count

        # Format pattern counts - process all values (counting is memory efficient)
        patterns = valid.apply(self._extract_pattern)
        # Use pandas value_counts() for memory efficiency
        pattern_counts = patterns.value_counts()
        for pattern, count in pattern_counts.items():
            self.format_pattern_counts[col][pattern] += count

    def _extract_pattern(self, value: str) -> str:
        """Extract format pattern from string value."""
        if not value or pd.isna(value):
            return "EMPTY"
        pattern = re.sub(r'[A-Z]', 'A', str(value))
        pattern = re.sub(r'[a-z]', 'a', pattern)
        pattern = re.sub(r'[0-9]', '9', pattern)
        # Collapse repeated chars
        pattern = re.sub(r'A+', 'A+', pattern)
        pattern = re.sub(r'a+', 'a+', pattern)
        pattern = re.sub(r'9+', '9+', pattern)
        return pattern[:50]  # Limit length

    def _accumulate_target_statistics(self, chunk: pd.DataFrame) -> None:
        """
        Accumulate target-feature statistics for all detected targets.

        This method accumulates:
        1. Target value distributions
        2. Target-feature co-occurrence (categorical features)
        3. Missingness by target value
        4. Numeric feature stats by target class
        5. Correlation ratio stats (for mixed-type correlations)

        Memory-efficient: Uses Counter objects and streaming statistics.
        """
        if not self.detected_targets:
            return

        # Limit feature columns to analyze (avoid memory explosion)
        max_features = 20  # Analyze top N features by importance heuristic
        all_features = [c for c in chunk.columns if c not in self.detected_targets]

        # Prioritize low-cardinality categoricals and numeric features
        priority_features = []
        for col in all_features:
            if self.column_types.get(col) == 'string':
                # Low cardinality categorical features are more informative
                if col in self.value_counts and len(self.value_counts[col]) <= 50:
                    priority_features.append(col)
            else:
                # All numeric features
                priority_features.append(col)
        features_to_analyze = priority_features[:max_features]

        for target_col in self.detected_targets:
            if target_col not in chunk.columns:
                continue

            target_series = chunk[target_col].astype(str)  # Normalize to string for consistent counting

            # 1. Accumulate target value counts
            self.target_value_counts[target_col].update(target_series.dropna())

            # Only process if we have features to analyze
            if not features_to_analyze:
                continue

            for feature_col in features_to_analyze:
                if feature_col not in chunk.columns or feature_col == target_col:
                    continue

                # Initialize nested structures if needed
                if feature_col not in self.target_feature_counts[target_col]:
                    self.target_feature_counts[target_col][feature_col] = {}
                if feature_col not in self.missingness_by_target[target_col]:
                    self.missingness_by_target[target_col][feature_col] = {}
                if feature_col not in self.numeric_by_target[target_col]:
                    self.numeric_by_target[target_col][feature_col] = {}

                feature_series = chunk[feature_col]
                is_numeric = self.column_types.get(feature_col) == 'numeric'

                # Group by target value for analysis
                for target_val in target_series.unique():
                    if pd.isna(target_val):
                        continue
                    target_val_str = str(target_val)

                    mask = target_series == target_val
                    feature_subset = feature_series[mask]

                    # 2. Missingness by target
                    if target_val_str not in self.missingness_by_target[target_col][feature_col]:
                        self.missingness_by_target[target_col][feature_col][target_val_str] = {'missing': 0, 'present': 0}
                    n_missing = feature_subset.isna().sum()
                    n_present = len(feature_subset) - n_missing
                    self.missingness_by_target[target_col][feature_col][target_val_str]['missing'] += int(n_missing)
                    self.missingness_by_target[target_col][feature_col][target_val_str]['present'] += int(n_present)

                    if is_numeric:
                        # 3. Numeric stats by target class (for target-class distribution analysis)
                        # Coerce to numeric to handle edge cases
                        try:
                            numeric_subset = pd.to_numeric(feature_subset, errors='coerce')
                            valid_numeric = numeric_subset.dropna()
                            if len(valid_numeric) > 0:
                                if target_val_str not in self.numeric_by_target[target_col][feature_col]:
                                    self.numeric_by_target[target_col][feature_col][target_val_str] = {
                                        'sum': 0.0, 'sum_sq': 0.0, 'count': 0
                                    }
                                stats = self.numeric_by_target[target_col][feature_col][target_val_str]
                                stats['sum'] += float(valid_numeric.sum())
                                stats['sum_sq'] += float((valid_numeric ** 2).sum())
                                stats['count'] += len(valid_numeric)
                        except (ValueError, TypeError):
                            # Fall back to treating as categorical if numeric conversion fails
                            pass
                    else:
                        # 4. Categorical co-occurrence with target
                        if target_val_str not in self.target_feature_counts[target_col][feature_col]:
                            self.target_feature_counts[target_col][feature_col][target_val_str] = Counter()
                        # Limit to top values to avoid memory explosion
                        feature_vals = feature_subset.dropna().astype(str)
                        if len(feature_vals) > 0:
                            # Only track most common values
                            val_counts = Counter(feature_vals)
                            # Keep only top 20 per target value
                            for val, cnt in val_counts.most_common(20):
                                self.target_feature_counts[target_col][feature_col][target_val_str][val] += cnt

        # 5. Correlation ratio stats (categorical vs numeric for mixed correlation matrix)
        # Group numeric features by low-cardinality categorical features
        categorical_cols = [c for c in features_to_analyze if self.column_types.get(c) == 'string'
                          and c in self.value_counts and len(self.value_counts[c]) <= 20]
        numeric_cols = [c for c in features_to_analyze if self.column_types.get(c) == 'numeric']

        for cat_col in categorical_cols[:5]:  # Limit to 5 categorical columns
            if cat_col not in self.correlation_ratio_stats:
                self.correlation_ratio_stats[cat_col] = {}

            cat_series = chunk[cat_col].astype(str)

            for num_col in numeric_cols[:10]:  # Limit to 10 numeric columns
                if num_col not in self.correlation_ratio_stats[cat_col]:
                    self.correlation_ratio_stats[cat_col][num_col] = {}

                # Coerce to numeric to handle edge cases with mixed types
                try:
                    num_series = pd.to_numeric(chunk[num_col], errors='coerce')
                except (ValueError, TypeError):
                    continue

                for cat_val in cat_series.unique():
                    if pd.isna(cat_val):
                        continue
                    cat_val_str = str(cat_val)

                    if cat_val_str not in self.correlation_ratio_stats[cat_col][num_col]:
                        self.correlation_ratio_stats[cat_col][num_col][cat_val_str] = {
                            'sum': 0.0, 'sum_sq': 0.0, 'count': 0
                        }

                    mask = cat_series == cat_val
                    valid_numeric = num_series[mask].dropna()
                    if len(valid_numeric) > 0:
                        try:
                            stats = self.correlation_ratio_stats[cat_col][num_col][cat_val_str]
                            stats['sum'] += float(valid_numeric.sum())
                            stats['sum_sq'] += float((valid_numeric ** 2).sum())
                            stats['count'] += len(valid_numeric)
                        except (ValueError, TypeError):
                            pass

    def _reservoir_sample(self, chunk: pd.DataFrame) -> None:
        """Reservoir sampling for autoencoder training data.

        MEMORY FIX: Use vectorized sampling instead of row-by-row iteration.
        Row-by-row iteration with to_dict() is extremely slow and memory intensive.
        """
        import random

        chunk_size = len(chunk)

        # Dynamic reservoir size - grow up to max_reservoir_size
        target_size = min(self.max_reservoir_size, self.seen_count + chunk_size)

        # If reservoir can still grow, add samples directly
        if len(self.reservoir_samples) < target_size:
            spaces_available = target_size - len(self.reservoir_samples)
            samples_to_take = min(spaces_available, chunk_size)

            if samples_to_take > 0:
                # Take samples from beginning of chunk (faster than random)
                sample_chunk = chunk.head(samples_to_take)
                # Convert to list of dicts in one vectorized operation
                self.reservoir_samples.extend(sample_chunk.to_dict('records'))
                # Update reservoir_size to match actual size
                self.reservoir_size = len(self.reservoir_samples)
        elif len(self.reservoir_samples) >= self.max_reservoir_size:
            # Reservoir is at max capacity - use probabilistic replacement
            # Calculate how many replacements to make based on reservoir sampling theory
            replacement_prob = self.reservoir_size / (self.seen_count + chunk_size)
            expected_replacements = int(chunk_size * replacement_prob)

            if expected_replacements > 0:
                # Scale replacements with chunk size (proportional, not hard cap)
                sample_size = min(expected_replacements, chunk_size, max(100, chunk_size // 10))
                if sample_size > 0:
                    sample_indices = random.sample(range(chunk_size), sample_size)
                    replacement_positions = random.sample(range(self.reservoir_size), sample_size)

                    # Batch extract rows and replace
                    sample_rows = chunk.iloc[sample_indices].to_dict('records')
                    for i, row_dict in enumerate(sample_rows):
                        self.reservoir_samples[replacement_positions[i]] = row_dict

        self.seen_count += chunk_size

    def finalize(self, ml_analyzer: 'MLAnalyzer') -> Dict[str, Any]:
        """
        Finalize ML analysis after all chunks processed.

        Args:
            ml_analyzer: MLAnalyzer instance for running final analyses

        Returns:
            Dictionary containing ML findings
        """
        logger.info(f"ML Analysis: Finalizing analysis for {self.total_rows:,} rows (chunked processing)")

        findings = {
            "sample_info": {
                "original_rows": self.total_rows,
                "analyzed_rows": self.total_rows,
                "sampled": False,
                "sample_percentage": 100.0,
                "processing_mode": "chunked"
            },
            "benford_analysis": {},
            "numeric_outliers": {},
            "autoencoder_anomalies": {},
            "rare_categories": {},
            "format_anomalies": {},
            "cross_column_issues": [],
            "temporal_patterns": {},
            "correlation_anomalies": {},
            "clustering_analysis": {},
            # New target/feature analysis sections
            "target_feature_analysis": {},
            "missingness_impact": {},
            "target_class_distribution": {},
            "mixed_correlation_matrix": {},
            "summary": {
                "total_issues": 0,
                "severity": "low",
                "key_findings": []
            }
        }

        # 1. Benford's law analysis from accumulated digit counts
        # Include ALL results (both suspicious and natural) so users see full analysis
        # Also track ineligible columns with reasons (for fallback rendering)
        benford_ineligible = {}
        for col, digit_counts in self.benford_digit_counts.items():
            applicability = getattr(self, '_benford_applicability', {}).get(col, {})
            if not applicability.get('applicable', True):
                # Store reason for fallback display
                benford_ineligible[col] = applicability.get('reason', 'Not applicable')
                continue

            if sum(digit_counts.values()) >= MIN_ROWS_FOR_ML:
                benford_result = self._analyze_benford_from_counts(col, digit_counts)
                if benford_result:
                    findings["benford_analysis"][col] = benford_result
            else:
                # Store reason for insufficient data
                benford_ineligible[col] = f"Insufficient positive values ({sum(digit_counts.values())} < {MIN_ROWS_FOR_ML})"

        # Add ineligible columns info to findings for fallback rendering
        findings["benford_ineligible"] = benford_ineligible

        # 2. Numeric outliers from accumulated statistics using streaming histograms
        # Include ALL numeric fields for visualization, even those with 0% outliers
        all_numeric_outlier_stats = {}  # For IQR visualization (includes 0% outliers)
        for col, stats in self.numeric_stats.items():
            hist_data = self.numeric_histograms.get(col, {})
            if stats['count'] >= MIN_ROWS_FOR_ML and hist_data.get('initialized'):
                outlier_result = self._analyze_outliers_from_stats(col, stats, hist_data)
                if outlier_result:
                    # Always include in all_numeric_outlier_stats for visualization
                    all_numeric_outlier_stats[col] = outlier_result
                    # Only include in main numeric_outliers if actual outliers found
                    if outlier_result.get("anomaly_count", 0) > 0:
                        findings["numeric_outliers"][col] = outlier_result

        # Store all numeric stats for IQR visualization (even with 0 outliers)
        findings["all_numeric_outlier_stats"] = all_numeric_outlier_stats

        # 3. Rare category detection from accumulated value counts
        for col, counts in self.value_counts.items():
            if len(counts) > 1:
                rare_result = self._analyze_rare_categories(col, counts, self.total_rows)
                if rare_result and rare_result.get("anomaly_count", 0) > 0:
                    findings["rare_categories"][col] = rare_result

        # 4. Format anomalies from accumulated pattern counts
        for col, pattern_counts in self.format_pattern_counts.items():
            if len(pattern_counts) > 1:
                format_result = self._analyze_format_from_counts(col, pattern_counts)
                if format_result and format_result.get("anomaly_count", 0) > 0:
                    findings["format_anomalies"][col] = format_result

        # 5. Autoencoder on reservoir samples (if sklearn available)
        if self.reservoir_samples and ml_analyzer._sklearn_available:
            try:
                reservoir_df = pd.DataFrame(self.reservoir_samples)
                numeric_cols = [c for c in reservoir_df.columns if pd.api.types.is_numeric_dtype(reservoir_df[c])]
                if len(numeric_cols) >= 2:
                    ae_result = ml_analyzer._detect_autoencoder_anomalies(reservoir_df, numeric_cols)
                    if ae_result:
                        findings["autoencoder_anomalies"] = ae_result
            except Exception as e:
                logger.debug(f"Autoencoder analysis failed: {e}")

        # 6. Target/Feature Analysis (4 new sections)
        if self.detected_targets:
            findings["target_feature_analysis"] = self._finalize_target_feature_analysis()
            findings["missingness_impact"] = self._finalize_missingness_impact()
            findings["target_class_distribution"] = self._finalize_target_class_distribution()
            findings["mixed_correlation_matrix"] = self._finalize_mixed_correlation_matrix()

        # Calculate summary
        total_issues = (
            len(findings["benford_analysis"]) +
            sum(r.get("anomaly_count", 0) for r in findings["numeric_outliers"].values()) +
            sum(r.get("anomaly_count", 0) for r in findings["rare_categories"].values()) +
            sum(r.get("anomaly_count", 0) for r in findings["format_anomalies"].values())
        )
        findings["summary"]["total_issues"] = total_issues
        findings["summary"]["severity"] = "high" if total_issues > 10 else "medium" if total_issues > 3 else "low"

        # 6. Generate visualization data for advanced charts
        findings["visualizations"] = self._generate_visualization_data(findings, ml_analyzer)

        return findings

    def _generate_visualization_data(self, findings: Dict, ml_analyzer: 'MLAnalyzer') -> Dict[str, Any]:
        """
        Generate data for advanced visualizations.

        Returns dict with:
        - amount_distributions: Log-scaled histogram data for amount fields
        - amount_scatter: Paired samples for Amount Received vs Amount Paid scatter
        - class_imbalance: Class distribution for binary/categorical target fields
        - temporal_density: Event density over time for activity timeline
        - anomaly_scores: Distribution of isolation forest anomaly scores
        - reconstruction_errors: Distribution of autoencoder reconstruction errors
        """
        viz_data = {
            "amount_distributions": {},
            "amount_scatter": None,
            "class_imbalance": {},
            "temporal_density": {},
            "anomaly_scores": {},
            "reconstruction_errors": None,
            "sample_info": {
                "total_rows": self.total_rows,
                "sample_size": len(self.reservoir_samples) if self.reservoir_samples else self.total_rows,
                "is_sampled": len(self.reservoir_samples) < self.total_rows if self.reservoir_samples else False
            }
        }

        # 1. Amount field distributions (log-scaled histograms)
        amount_keywords = ['amount', 'price', 'cost', 'fee', 'total', 'balance', 'value', 'sum', 'payment', 'fare']
        for col, samples in self.numeric_samples.items():
            col_lower = str(col).lower() if col is not None else ''
            # Check if column looks like an amount field
            is_amount = any(kw in col_lower for kw in amount_keywords)
            if is_amount and len(samples) >= 100:
                viz_data["amount_distributions"][col] = self._compute_log_histogram(col, samples)

        # 2. Amount scatter plot (find Amount Received vs Amount Paid pairs)
        received_col = None
        paid_col = None
        for col in self.numeric_samples.keys():
            col_lower = str(col).lower() if col is not None else ''
            if 'received' in col_lower or 'recv' in col_lower or 'in' in col_lower.split('_'):
                received_col = col
            elif 'paid' in col_lower or 'sent' in col_lower or 'out' in col_lower.split('_'):
                paid_col = col

        if received_col and paid_col and self.reservoir_samples:
            try:
                scatter_data = []
                for row in self.reservoir_samples[:5000]:  # Limit scatter points
                    recv_val = row.get(received_col)
                    paid_val = row.get(paid_col)
                    if recv_val is not None and paid_val is not None:
                        try:
                            scatter_data.append({
                                "x": float(recv_val),
                                "y": float(paid_val)
                            })
                        except (ValueError, TypeError):
                            pass
                if scatter_data:
                    viz_data["amount_scatter"] = {
                        "x_column": received_col,
                        "y_column": paid_col,
                        "points": scatter_data[:2000],  # Limit for performance
                        "total_points": len(scatter_data)
                    }
            except Exception as e:
                logger.debug(f"Scatter plot data generation failed: {e}")

        # 3. Class imbalance data (for binary/low-cardinality columns)
        # Use detected_targets from proper target detection (not simple keyword matching)
        for col, counts in self.value_counts.items():
            is_target = col in self.detected_targets
            # Include all low-cardinality categorical columns (useful as ML features or targets)
            if len(counts) <= 10:
                total = sum(counts.values())
                viz_data["class_imbalance"][col] = {
                    "classes": [{"value": str(v), "count": c, "percentage": round(c/total*100, 2)}
                               for v, c in counts.most_common(10)],
                    "is_binary": len(counts) == 2,
                    "is_target_like": is_target,
                    "total": total
                }

        # 4. Anomaly score distributions from isolation forest results
        for col, outlier_data in findings.get("numeric_outliers", {}).items():
            if "anomaly_score_range" in outlier_data:
                score_range = outlier_data["anomaly_score_range"]
                viz_data["anomaly_scores"][col] = {
                    "min_score": score_range.get("min", 0),
                    "max_score": score_range.get("max", 0),
                    "anomaly_count": outlier_data.get("anomaly_count", 0),
                    "total_analyzed": outlier_data.get("total_analyzed", self.total_rows)
                }

        # 5. Reconstruction error distribution from autoencoder
        ae_data = findings.get("autoencoder_anomalies", {})
        if ae_data and "error_stats" in ae_data:
            error_stats = ae_data["error_stats"]
            viz_data["reconstruction_errors"] = {
                "mean": error_stats.get("mean", 0),
                "median": error_stats.get("median", 0),
                "q75": error_stats.get("q75", 0),
                "std": error_stats.get("std", 0),
                "max": error_stats.get("max", 0),
                "threshold": ae_data.get("threshold", 0),
                "anomaly_count": ae_data.get("anomaly_count", 0),
                "anomaly_percentage": ae_data.get("anomaly_percentage", 0),
                "anomaly_min_error": error_stats.get("anomaly_min_error", 0)
            }

        return viz_data

    def _finalize_target_feature_analysis(self) -> Dict[str, Any]:
        """
        Finalize Target-Feature Association Analysis.

        Computes association strength between detected targets and features using:
        - Cramér's V for categorical-categorical associations
        - Difference in means for numeric features across target classes

        Returns:
            Dictionary with association analysis per target column
        """
        results = {}

        for target_col in self.detected_targets:
            if target_col not in self.target_value_counts:
                continue

            target_counts = self.target_value_counts[target_col]
            total_target = sum(target_counts.values())
            if total_target < MIN_ROWS_FOR_ML:
                continue

            target_result = {
                "target_column": target_col,
                "target_distribution": {
                    str(v): {"count": c, "percentage": round(c / total_target * 100, 2)}
                    for v, c in target_counts.most_common(10)
                },
                "feature_associations": [],
                "total_rows_analyzed": total_target,
                "interpretation": ""
            }

            # Analyze categorical feature associations
            if target_col in self.target_feature_counts:
                for feature_col, target_val_dict in self.target_feature_counts[target_col].items():
                    if not target_val_dict:
                        continue

                    # Calculate simple association measure: difference in top category proportion
                    # More sophisticated: compute Cramér's V from contingency table
                    association = self._compute_categorical_association(
                        target_counts, target_val_dict, feature_col
                    )
                    if association:
                        target_result["feature_associations"].append(association)

            # Analyze numeric feature associations (difference in means)
            if target_col in self.numeric_by_target:
                for feature_col, target_val_dict in self.numeric_by_target[target_col].items():
                    if not target_val_dict:
                        continue

                    association = self._compute_numeric_association(
                        target_counts, target_val_dict, feature_col
                    )
                    if association:
                        target_result["feature_associations"].append(association)

            # Sort by association strength
            target_result["feature_associations"].sort(
                key=lambda x: x.get("association_strength", 0), reverse=True
            )
            # Keep top 10 associations
            target_result["feature_associations"] = target_result["feature_associations"][:10]

            # Generate interpretation
            if target_result["feature_associations"]:
                top_assoc = target_result["feature_associations"][0]
                target_result["interpretation"] = (
                    f"The feature '{top_assoc['feature']}' shows the strongest association "
                    f"with '{target_col}' (strength: {top_assoc['association_strength']:.2f}). "
                    f"This suggests {top_assoc['feature']} may be predictive of the target."
                )
            else:
                target_result["interpretation"] = (
                    f"No strong feature associations detected for '{target_col}'. "
                    "This could indicate the target is relatively independent of other features."
                )

            results[target_col] = target_result

        return results

    def _compute_categorical_association(
        self, target_counts: Counter, feature_dict: Dict, feature_col: str
    ) -> Optional[Dict]:
        """Compute association between categorical target and feature."""
        # Build contingency table and compute Cramér's V approximation
        # Using difference in proportions as a simpler measure
        target_values = list(target_counts.keys())
        if len(target_values) < 2:
            return None

        # Get all feature values across target classes
        all_feature_vals = set()
        for tv_dict in feature_dict.values():
            if isinstance(tv_dict, Counter):
                all_feature_vals.update(tv_dict.keys())

        if len(all_feature_vals) == 0:
            return None

        # Calculate proportion difference for top feature value
        max_diff = 0
        distinguishing_value = None
        for fv in list(all_feature_vals)[:10]:  # Check top values
            proportions = []
            for tv in target_values[:2]:  # Compare first two target values
                tv_counter = feature_dict.get(str(tv), Counter())
                tv_total = sum(tv_counter.values())
                if tv_total > 0:
                    proportions.append(tv_counter.get(fv, 0) / tv_total)
                else:
                    proportions.append(0)
            if len(proportions) >= 2:
                diff = abs(proportions[0] - proportions[1])
                if diff > max_diff:
                    max_diff = diff
                    distinguishing_value = fv

        if max_diff < 0.05:  # Minimum threshold for meaningful association
            return None

        return {
            "feature": feature_col,
            "feature_type": "categorical",
            "association_strength": round(max_diff, 3),
            "distinguishing_value": str(distinguishing_value) if distinguishing_value else None,
            "interpretation": f"'{feature_col}' shows different distributions across target classes"
        }

    def _compute_numeric_association(
        self, target_counts: Counter, feature_dict: Dict, feature_col: str
    ) -> Optional[Dict]:
        """Compute association between target and numeric feature using effect size."""
        target_values = list(target_counts.keys())
        if len(target_values) < 2:
            return None

        # Calculate mean and std for each target class
        class_stats = {}
        for tv in target_values[:2]:  # Compare first two target values
            tv_str = str(tv)
            if tv_str in feature_dict:
                stats = feature_dict[tv_str]
                if stats['count'] > 0:
                    mean = stats['sum'] / stats['count']
                    variance = (stats['sum_sq'] / stats['count']) - (mean ** 2)
                    std = np.sqrt(max(0, variance))
                    class_stats[tv_str] = {'mean': mean, 'std': std, 'count': stats['count']}

        if len(class_stats) < 2:
            return None

        # Calculate Cohen's d effect size
        stats_list = list(class_stats.values())
        mean_diff = abs(stats_list[0]['mean'] - stats_list[1]['mean'])
        pooled_std = np.sqrt(
            (stats_list[0]['std']**2 + stats_list[1]['std']**2) / 2
        )

        if pooled_std == 0:
            return None

        cohens_d = mean_diff / pooled_std

        if cohens_d < 0.2:  # Small effect threshold
            return None

        # Interpret effect size
        if cohens_d >= 0.8:
            effect_label = "large"
        elif cohens_d >= 0.5:
            effect_label = "medium"
        else:
            effect_label = "small"

        return {
            "feature": feature_col,
            "feature_type": "numeric",
            "association_strength": round(min(cohens_d, 3.0), 3),  # Cap at 3 for display
            "effect_size": effect_label,
            "class_means": {k: round(v['mean'], 2) for k, v in class_stats.items()},
            "interpretation": f"'{feature_col}' shows {effect_label} difference between target classes"
        }

    def _finalize_missingness_impact(self) -> Dict[str, Any]:
        """
        Finalize Missingness Impact Analysis.

        Analyzes how missing data correlates with target values.

        Returns:
            Dictionary with missingness analysis per target column
        """
        results = {}

        for target_col in self.detected_targets:
            if target_col not in self.missingness_by_target:
                continue

            target_result = {
                "target_column": target_col,
                "features_with_differential_missingness": [],
                "total_features_analyzed": 0,
                "interpretation": ""
            }

            feature_missingness = self.missingness_by_target[target_col]
            target_result["total_features_analyzed"] = len(feature_missingness)

            for feature_col, target_val_dict in feature_missingness.items():
                if not target_val_dict:
                    continue

                # Calculate missingness rate per target class
                missingness_rates = {}
                for tv, counts in target_val_dict.items():
                    total = counts.get('missing', 0) + counts.get('present', 0)
                    if total > 0:
                        miss_rate = counts.get('missing', 0) / total
                        missingness_rates[tv] = {
                            "rate": round(miss_rate * 100, 2),
                            "total": total
                        }

                if len(missingness_rates) < 2:
                    continue

                # Check for differential missingness
                rates = [v['rate'] for v in missingness_rates.values()]
                max_diff = max(rates) - min(rates)

                if max_diff >= 1.0:  # At least 1% difference in missingness rate (lowered from 5%)
                    target_result["features_with_differential_missingness"].append({
                        "feature": feature_col,
                        "missingness_by_target": missingness_rates,
                        "max_difference": round(max_diff, 2),
                        "interpretation": (
                            f"'{feature_col}' has {max_diff:.1f}% difference in missingness rate "
                            "across target classes, which may indicate non-random missing data."
                        )
                    })

            # Sort by impact
            target_result["features_with_differential_missingness"].sort(
                key=lambda x: x.get("max_difference", 0), reverse=True
            )

            # Generate interpretation
            n_diff = len(target_result["features_with_differential_missingness"])
            if n_diff > 0:
                top_feature = target_result["features_with_differential_missingness"][0]["feature"]
                target_result["interpretation"] = (
                    f"Found {n_diff} feature(s) with differential missingness across '{target_col}' values. "
                    f"'{top_feature}' shows the largest difference. This suggests missing data may not be "
                    "random and could affect model training."
                )
            else:
                target_result["interpretation"] = (
                    f"No significant differential missingness detected for '{target_col}'. "
                    "Missing data appears to be relatively uniform across target classes."
                )

            results[target_col] = target_result

        return results

    def _finalize_target_class_distribution(self) -> Dict[str, Any]:
        """
        Finalize Target-Class Distribution Analysis.

        Analyzes how numeric features are distributed within each target class.

        Returns:
            Dictionary with distribution analysis per target column
        """
        results = {}

        for target_col in self.detected_targets:
            if target_col not in self.numeric_by_target:
                continue

            target_counts = self.target_value_counts.get(target_col, Counter())
            total_target = sum(target_counts.values())

            target_result = {
                "target_column": target_col,
                "target_class_counts": {
                    str(v): c for v, c in target_counts.most_common(10)
                },
                "feature_distributions": {},
                "interpretation": ""
            }

            numeric_features = self.numeric_by_target[target_col]
            for feature_col, target_val_dict in numeric_features.items():
                if not target_val_dict:
                    continue

                feature_dist = {
                    "feature": feature_col,
                    "by_target_class": {}
                }

                for tv, stats in target_val_dict.items():
                    if stats['count'] > 0:
                        mean = stats['sum'] / stats['count']
                        variance = (stats['sum_sq'] / stats['count']) - (mean ** 2)
                        std = np.sqrt(max(0, variance))
                        feature_dist["by_target_class"][tv] = {
                            "mean": round(mean, 4),
                            "std": round(std, 4),
                            "count": stats['count']
                        }

                if len(feature_dist["by_target_class"]) >= 2:
                    target_result["feature_distributions"][feature_col] = feature_dist

            # Generate interpretation
            n_features = len(target_result["feature_distributions"])
            if n_features > 0:
                target_result["interpretation"] = (
                    f"Analyzed distribution of {n_features} numeric feature(s) across '{target_col}' classes. "
                    "Differences in means and standard deviations can help identify discriminative features."
                )
            else:
                target_result["interpretation"] = (
                    f"No numeric features available for distribution analysis across '{target_col}' classes."
                )

            results[target_col] = target_result

        return results

    def _finalize_mixed_correlation_matrix(self) -> Dict[str, Any]:
        """
        Finalize Mixed-Type Correlation Matrix.

        Computes correlations including:
        - Numeric-Categorical: Correlation Ratio (η)
        - Categorical-Categorical: Would need Cramér's V (simplified here)

        Returns:
            Dictionary with mixed correlation data
        """
        results = {
            "correlation_ratio": {},  # Categorical → Numeric associations
            "interpretation": "",
            "columns_analyzed": {
                "categorical": [],
                "numeric": []
            }
        }

        if not self.correlation_ratio_stats:
            results["interpretation"] = "No mixed correlations computed (insufficient categorical columns)."
            return results

        for cat_col, num_dict in self.correlation_ratio_stats.items():
            results["columns_analyzed"]["categorical"].append(cat_col)

            for num_col, cat_val_dict in num_dict.items():
                if num_col not in results["columns_analyzed"]["numeric"]:
                    results["columns_analyzed"]["numeric"].append(num_col)

                # Compute correlation ratio (η)
                # η² = SS_between / SS_total
                total_sum = 0
                total_sum_sq = 0
                total_count = 0
                group_means = []

                for cat_val, stats in cat_val_dict.items():
                    if stats['count'] > 0:
                        group_mean = stats['sum'] / stats['count']
                        group_means.append((group_mean, stats['count']))
                        total_sum += stats['sum']
                        total_sum_sq += stats['sum_sq']
                        total_count += stats['count']

                if total_count < MIN_ROWS_FOR_ML or len(group_means) < 2:
                    continue

                grand_mean = total_sum / total_count
                ss_total = total_sum_sq - (total_sum ** 2) / total_count

                if ss_total <= 0:
                    continue

                # SS_between = Σ n_i * (mean_i - grand_mean)²
                ss_between = sum(
                    n * (m - grand_mean) ** 2 for m, n in group_means
                )

                eta_squared = ss_between / ss_total if ss_total > 0 else 0
                eta = np.sqrt(max(0, min(1, eta_squared)))

                if eta >= 0.1:  # Minimum threshold for meaningful correlation
                    key = f"{cat_col}_vs_{num_col}"
                    results["correlation_ratio"][key] = {
                        "categorical_column": cat_col,
                        "numeric_column": num_col,
                        "eta": round(eta, 3),
                        "eta_squared": round(eta_squared, 3),
                        "interpretation": self._interpret_correlation_ratio(eta)
                    }

        # Sort by eta
        sorted_correlations = sorted(
            results["correlation_ratio"].items(),
            key=lambda x: x[1]["eta"],
            reverse=True
        )
        results["correlation_ratio"] = dict(sorted_correlations[:20])  # Top 20

        # Generate interpretation
        n_corr = len(results["correlation_ratio"])
        if n_corr > 0:
            top_key = list(results["correlation_ratio"].keys())[0]
            top_corr = results["correlation_ratio"][top_key]
            results["interpretation"] = (
                f"Found {n_corr} significant categorical-numeric associations. "
                f"Strongest: '{top_corr['categorical_column']}' explains {top_corr['eta_squared']*100:.1f}% "
                f"of variance in '{top_corr['numeric_column']}' (η={top_corr['eta']:.2f})."
            )
        else:
            results["interpretation"] = "No significant categorical-numeric correlations detected."

        return results

    def _interpret_correlation_ratio(self, eta: float) -> str:
        """Interpret correlation ratio value."""
        if eta >= 0.5:
            return "Strong association"
        elif eta >= 0.3:
            return "Moderate association"
        elif eta >= 0.1:
            return "Weak association"
        else:
            return "Negligible association"

    def _compute_log_histogram(self, col: str, samples: List, bins: int = 30) -> Dict[str, Any]:
        """Compute log-scaled histogram for amount field."""
        try:
            arr = np.array([s for s in samples if s is not None and s > 0], dtype=float)
            if len(arr) < 10:
                return {}

            # Handle zeros and negatives for log scale
            min_positive = arr[arr > 0].min() if np.any(arr > 0) else 1
            arr_shifted = np.where(arr > 0, arr, min_positive / 10)

            # Compute log-scaled bins
            log_arr = np.log10(arr_shifted)
            hist, bin_edges = np.histogram(log_arr, bins=bins)

            # Convert back to original scale for display
            return {
                "histogram": hist.tolist(),
                "bin_edges_log": bin_edges.tolist(),
                "bin_edges": (10 ** bin_edges).tolist(),
                "min_value": float(arr.min()),
                "max_value": float(arr.max()),
                "median": float(np.median(arr)),
                "mean": float(np.mean(arr)),
                "has_zeros": int(np.sum(samples == 0)) if hasattr(samples, '__len__') else 0,
                "total_samples": len(arr),
                "log_scale_explanation": "Values transformed using log10 scale to visualize skewed distributions"
            }
        except Exception as e:
            logger.debug(f"Log histogram computation failed for {col}: {e}")
            return {}

    def _analyze_benford_from_counts(self, col: str, digit_counts: Counter) -> Optional[Dict]:
        """Analyze Benford's law from accumulated digit counts."""
        from scipy import stats as scipy_stats

        total = sum(digit_counts.values())
        if total < MIN_ROWS_FOR_ML:
            return None

        # Expected Benford distribution (proportions)
        benford_expected = {d: np.log10(1 + 1/d) for d in range(1, 10)}

        # Calculate observed distribution (proportions)
        observed = {d: digit_counts.get(d, 0) / total for d in range(1, 10)}

        # Observed counts for chi-square test
        observed_counts = np.array([digit_counts.get(d, 0) for d in range(1, 10)])
        expected_counts = np.array([benford_expected[d] * total for d in range(1, 10)])

        # Chi-square test with actual p-value
        chi_sq, p_value = scipy_stats.chisquare(observed_counts, expected_counts)

        # Interpretation: low p-value = deviation from Benford (suspicious)
        # High p-value = conforms to Benford (natural data)
        is_suspicious = p_value < 0.05

        # Confidence in the DEVIATION finding (consistent with _detect_benford_anomalies)
        # Low p-value = high confidence that data deviates from Benford
        if p_value < 0.001:
            confidence = "Very High"  # Strong evidence of deviation
        elif p_value < 0.01:
            confidence = "High"
        elif p_value < 0.05:
            confidence = "Medium"
        else:
            confidence = "Low"  # Not suspicious / conforms to Benford

        return {
            "chi_square": round(chi_sq, 2),
            "p_value": round(p_value, 6),
            "alpha": 0.05,  # Significance level used for test
            "conforms": not is_suspicious,  # True if data follows Benford's Law
            "is_suspicious": is_suspicious,  # True if data deviates from Benford's Law
            "confidence": confidence,  # Confidence in the deviation finding
            "total_values": total,
            "observed_distribution": {str(d): round(observed.get(d, 0) * 100, 1) for d in range(1, 10)},
            "expected_distribution": {str(d): round(benford_expected[d] * 100, 1) for d in range(1, 10)}
        }

    def _percentile_from_histogram(self, hist_data: Dict, percentile: float) -> float:
        """Compute approximate percentile from streaming histogram."""
        if not hist_data['initialized'] or hist_data['counts'] is None:
            return 0.0

        counts = hist_data['counts']
        total = counts.sum()
        if total == 0:
            return 0.0

        target = total * (percentile / 100.0)
        cumsum = 0
        bin_width = (hist_data['range_max'] - hist_data['range_min']) / len(counts)

        for i, count in enumerate(counts):
            cumsum += count
            if cumsum >= target:
                # Linear interpolation within the bin
                bin_start = hist_data['range_min'] + i * bin_width
                if count > 0:
                    fraction = (target - (cumsum - count)) / count
                    return bin_start + fraction * bin_width
                return bin_start

        return hist_data['range_max']

    def _count_outliers_from_histogram(self, hist_data: Dict, lower_bound: float, upper_bound: float) -> int:
        """Count values outside bounds using histogram."""
        if not hist_data['initialized'] or hist_data['counts'] is None:
            return 0

        counts = hist_data['counts']
        bin_width = (hist_data['range_max'] - hist_data['range_min']) / len(counts)
        outlier_count = 0

        for i, count in enumerate(counts):
            if count > 0:
                bin_center = hist_data['range_min'] + (i + 0.5) * bin_width
                if bin_center < lower_bound or bin_center > upper_bound:
                    outlier_count += count

        return outlier_count

    def _analyze_outliers_from_stats(self, col: str, stats: Dict, hist_data: Dict) -> Optional[Dict]:
        """Analyze numeric outliers using streaming histogram percentiles."""
        if stats['count'] < MIN_ROWS_FOR_ML:
            return None

        # Calculate mean and std from running stats
        mean = stats['sum'] / stats['count']
        variance = (stats['sum_sq'] / stats['count']) - (mean ** 2)
        std = np.sqrt(max(0, variance))

        if std == 0:
            return None

        # Use IQR method with streaming histogram percentiles
        q1 = self._percentile_from_histogram(hist_data, 25)
        q3 = self._percentile_from_histogram(hist_data, 75)
        iqr = q3 - q1

        if iqr == 0:
            return None

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Count outliers directly from histogram
        outlier_count = self._count_outliers_from_histogram(hist_data, lower_bound, upper_bound)
        outlier_rate = outlier_count / stats['count']

        if outlier_count == 0:
            return None

        return {
            "method": "IQR (streaming histogram)",
            "anomaly_count": outlier_count,
            "anomaly_percentage": round(outlier_rate * 100, 2),
            "bounds": {"lower": round(lower_bound, 2), "upper": round(upper_bound, 2)},
            "statistics": {"mean": round(mean, 2), "std": round(std, 2), "min": round(stats['min'], 2), "max": round(stats['max'], 2)}
        }

    def _analyze_rare_categories(self, col: str, counts: Counter, total_rows: int) -> Optional[Dict]:
        """Analyze rare categories from accumulated value counts."""
        total = sum(counts.values())
        if total < MIN_ROWS_FOR_ML:
            return None

        # Find rare categories (< 0.1% of data)
        threshold = max(1, total * 0.001)
        rare = [(val, cnt) for val, cnt in counts.items() if cnt < threshold]

        if not rare:
            return None

        rare_count = sum(cnt for _, cnt in rare)

        return {
            "anomaly_count": len(rare),
            "rare_value_count": rare_count,
            "anomaly_percentage": round(rare_count / total * 100, 2),
            "total_categories": len(counts),
            "rare_examples": [{"value": str(v)[:50], "count": c} for v, c in sorted(rare, key=lambda x: x[1])[:10]]
        }

    def _analyze_format_from_counts(self, col: str, pattern_counts: Counter) -> Optional[Dict]:
        """Analyze format anomalies from accumulated pattern counts."""
        total = sum(pattern_counts.values())
        if total < MIN_ROWS_FOR_ML or len(pattern_counts) <= 1:
            return None

        # Find dominant pattern
        dominant_pattern, dominant_count = pattern_counts.most_common(1)[0]
        dominant_pct = dominant_count / total

        # If dominant pattern is > 90%, others are anomalies
        if dominant_pct < 0.9:
            return None

        anomaly_count = total - dominant_count
        anomaly_patterns = [(p, c) for p, c in pattern_counts.items() if p != dominant_pattern]

        return {
            "anomaly_count": anomaly_count,
            "anomaly_percentage": round((1 - dominant_pct) * 100, 2),
            "dominant_pattern": dominant_pattern,
            "dominant_percentage": round(dominant_pct * 100, 1),
            "anomaly_patterns": [{"pattern": p, "count": c} for p, c in sorted(anomaly_patterns, key=lambda x: -x[1])[:5]]
        }


class MLAnalyzer:
    """
    Machine learning-based data quality analyzer.

    Runs multiple ML techniques on sampled data to detect anomalies
    and data quality issues that traditional profiling might miss.

    Supports FIBO-based semantic intelligence for smart rare category detection.
    """

    def __init__(self):
        """
        Initialize ML analyzer.

        Note: No internal sample size limit - the caller (engine) handles sampling.
        This analyzer processes whatever data is passed to it.
        """
        self.results: Dict[str, Any] = {}
        self._sklearn_available = self._check_sklearn()
        self._fibo_taxonomy = self._load_fibo_taxonomy()
        self._column_semantic_info: Dict[str, Dict[str, Any]] = {}

    def _load_fibo_taxonomy(self) -> Dict[str, Any]:
        """Load FIBO taxonomy for intelligent rare category detection."""
        import json
        from pathlib import Path

        taxonomy_path = Path(__file__).parent / "taxonomies" / "finance_taxonomy.json"
        try:
            with open(taxonomy_path, 'r') as f:
                taxonomy = json.load(f)
                # Flatten tags for easy lookup
                flattened = {}
                for category, cat_data in taxonomy.get("taxonomy", {}).items():
                    for tag_name, tag_def in cat_data.get("tags", {}).items():
                        flattened[tag_name] = tag_def
                logger.debug(f"Loaded FIBO taxonomy with {len(flattened)} semantic tags")
                return flattened
        except Exception as e:
            logger.warning(f"Failed to load FIBO taxonomy: {e}")
            return {}

    def _check_sklearn(self) -> bool:
        """Check if scikit-learn is available."""
        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.cluster import DBSCAN
            from sklearn.preprocessing import StandardScaler
            return True
        except ImportError:
            logger.warning("scikit-learn not available - some ML features disabled")
            return False

    def _get_numeric_columns(self, df: pd.DataFrame, exclude_binary: bool = True) -> Tuple[List[str], Dict[str, str]]:
        """
        Get numeric columns including string columns that can be coerced to numeric.

        This improves on simple select_dtypes(include=[np.number]) by:
        1. Including string columns where >80% of non-null values are coercible to float
        2. Optionally excluding binary/near-binary columns (<=3 unique values)
        3. Returning info about which columns were coerced

        Args:
            df: DataFrame to analyze
            exclude_binary: If True, exclude columns with <=3 unique values

        Returns:
            Tuple of (list of numeric column names, dict of coerced columns with notes)
        """
        numeric_cols = []
        coerced_info = {}

        # Get native numeric columns
        native_numeric = df.select_dtypes(include=[np.number]).columns.tolist()

        for col in native_numeric:
            # Check for binary/near-binary exclusion
            if exclude_binary:
                unique_vals = df[col].dropna().nunique()
                if unique_vals <= 3:
                    logger.debug(f"Excluding {col}: near-binary ({unique_vals} unique values)")
                    coerced_info[col] = f"excluded: near-binary ({unique_vals} unique values)"
                    continue
            numeric_cols.append(col)

        # Try to coerce object columns to numeric
        object_cols = df.select_dtypes(include=['object']).columns.tolist()

        for col in object_cols:
            series = df[col]
            non_null = series.dropna()

            if len(non_null) == 0:
                continue

            # Try to convert to numeric
            try:
                converted = pd.to_numeric(non_null, errors='coerce')
                valid_count = converted.notna().sum()
                success_rate = valid_count / len(non_null) if len(non_null) > 0 else 0

                # Include if >80% of values convert successfully
                if success_rate >= 0.80:
                    # Check for binary exclusion
                    if exclude_binary:
                        unique_vals = converted.dropna().nunique()
                        if unique_vals <= 3:
                            logger.debug(f"Excluding coerced {col}: near-binary ({unique_vals} unique values)")
                            coerced_info[col] = f"excluded: near-binary ({unique_vals} unique values)"
                            continue

                    numeric_cols.append(col)
                    coerced_info[col] = f"coerced from string ({success_rate*100:.1f}% success rate)"
                    logger.info(f"Detected {col} as coercible numeric ({success_rate*100:.1f}% success)")
            except Exception as e:
                logger.debug(f"Could not coerce {col} to numeric: {e}")

        return numeric_cols, coerced_info

    def _detect_target_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Detect likely target/label columns using semantic types and patterns.

        Uses semantic-based detection (FIBO + Schema.org) plus generic patterns:
        1. Semantic types: schema:Boolean, schema:CategoryCode, category.*, status
        2. Pattern matching: 'is_*', 'has_*', '*_flag', '*_indicator'
        3. Generic keywords: 'target', 'label', 'class', 'outcome', 'y', 'status'
        4. Binary columns with low cardinality (2 unique values)

        Returns:
            List of detected target column names
        """
        # Generic target keywords (domain-agnostic, no hardcoded field names)
        generic_keywords = [
            'target', 'label', 'class', 'outcome', 'response', 'y',
            'status', 'result', 'decision', 'category', 'segment',
            'type', 'grade', 'rating', 'level', 'tier'
        ]
        prefix_patterns = ['is_', 'has_', 'was_', 'did_', 'will_']
        suffix_patterns = ['_flag', '_indicator', '_label', '_target', '_class', '_outcome', '_status', '_type', '_category']

        # Semantic types indicating targets (FIBO + Schema.org)
        target_semantic_types = {
            'schema:Boolean', 'schema:CategoryCode', 'schema:ItemList',
            'category.transaction_type', 'category.account_type', 'category.payment_method',
            'status'
        }

        def is_keyword_present(col_lower, keywords):
            """Check if keyword is present as a whole word (not substring)."""
            for kw in keywords:
                if col_lower == kw:
                    return True
                if f'_{kw}' in col_lower or f'{kw}_' in col_lower:
                    return True
                if col_lower.startswith(f'{kw}_') or col_lower.endswith(f'_{kw}'):
                    return True
            return False

        detected = []
        for col in df.columns:
            if col is None:
                continue
            col_lower = str(col).lower().strip()

            # Check semantic type match (from column_semantic_info)
            sem_info = self._column_semantic_info.get(col, {})
            resolved = sem_info.get('resolved', {})
            primary_type = resolved.get('primary_type', '') or sem_info.get('primary_tag', '')
            is_semantic_target = primary_type in target_semantic_types or (
                '.' in primary_type and primary_type.split('.')[0] == 'category'
            )

            # Check generic keyword match
            is_keyword_match = is_keyword_present(col_lower, generic_keywords)

            # Check prefix/suffix patterns
            is_prefix_match = any(col_lower.startswith(p) for p in prefix_patterns)
            is_suffix_match = any(col_lower.endswith(s) for s in suffix_patterns)

            # Check cardinality
            try:
                n_unique = df[col].nunique(dropna=True)
                is_binary = n_unique == 2
                is_low_cardinality = n_unique <= 5
            except Exception:
                is_binary = False
                is_low_cardinality = False

            # Detection logic (semantic-first):
            # - Semantic type match with low cardinality -> target
            # - Generic keyword match with low cardinality -> target
            # - Prefix/suffix pattern with binary -> target
            if is_semantic_target and is_low_cardinality:
                detected.append(col)
            elif is_keyword_match and is_low_cardinality:
                detected.append(col)
            elif (is_prefix_match or is_suffix_match) and is_binary:
                detected.append(col)

        return detected

    def _calculate_confidence(self, method: str, anomaly_count: int,
                              anomaly_pct: float, sample_size: int) -> str:
        """
        Calculate confidence level for anomaly detection.

        Factors:
        - Detection method reliability
        - Sample size adequacy
        - Anomaly percentage (too high = suspicious)

        Returns:
            Confidence level: 'Very High', 'High', 'Medium', 'Low'
        """
        # Base score by method
        method_scores = {
            'isolation_forest': 80,
            'multivariate_isolation_forest': 85,
            'iqr_statistical': 70,
            'dbscan': 75,
        }
        score = method_scores.get(method, 60)

        # Adjust for anomaly percentage
        if anomaly_pct < 0.5:
            score += 10  # Rare outliers = high confidence
        elif anomaly_pct < 2:
            score += 5
        elif anomaly_pct > 10:
            score -= 15  # Too many = suspicious
        elif anomaly_pct > 5:
            score -= 10

        # Adjust for sample size
        if sample_size < 1000:
            score -= 15  # Small sample = lower confidence
        elif sample_size < 5000:
            score -= 5
        elif sample_size > 50000:
            score += 5  # Large sample = higher confidence

        # Adjust for count (too few examples = lower confidence)
        if anomaly_count < 5:
            score -= 20
        elif anomaly_count < 20:
            score -= 10

        # Clamp and categorize
        score = max(0, min(100, score))

        if score >= 85:
            return 'Very High'
        elif score >= 70:
            return 'High'
        elif score >= 55:
            return 'Medium'
        else:
            return 'Low'

    def _compute_missingness_impact_direct(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute missingness impact analysis directly from DataFrame.

        For each detected target column, analyze how missing data in feature columns
        correlates with target values. This helps identify potential bias in missing data.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary with missingness analysis per target column
        """
        results = {}

        for target_col in self.detected_targets:
            if target_col not in df.columns:
                continue

            target_result = {
                "target_column": target_col,
                "features_with_differential_missingness": [],
                "total_features_analyzed": 0,
                "interpretation": ""
            }

            # Get target values (drop rows where target is null)
            target_series = df[target_col].dropna()
            if len(target_series) == 0:
                continue

            target_values = target_series.unique()
            if len(target_values) < 2 or len(target_values) > 10:
                # Skip if target is not binary/low-cardinality
                continue

            features_analyzed = 0
            for col in df.columns:
                if col == target_col:
                    continue

                # Check if column has any missing values
                missing_count = df[col].isna().sum()
                if missing_count == 0:
                    continue

                features_analyzed += 1

                # Calculate missingness rate per target class
                missingness_rates = {}
                for tv in target_values:
                    mask = df[target_col] == tv
                    subset = df.loc[mask, col]
                    total = len(subset)
                    if total > 0:
                        missing = subset.isna().sum()
                        miss_rate = (missing / total) * 100
                        missingness_rates[str(tv)] = {
                            "rate": round(miss_rate, 2),
                            "total": total
                        }

                if len(missingness_rates) < 2:
                    continue

                # Check for differential missingness
                rates = [v['rate'] for v in missingness_rates.values()]
                max_diff = max(rates) - min(rates)

                # Only report if difference is at least 1%
                if max_diff >= 1.0:
                    # Calculate overall missing rate
                    overall_missing_rate = round((missing_count / len(df)) * 100, 2)

                    target_result["features_with_differential_missingness"].append({
                        "feature": col,
                        "overall_missing_rate": overall_missing_rate,
                        "missingness_by_target": missingness_rates,
                        "max_difference": round(max_diff, 2),
                        "interpretation": (
                            f"'{col}' has {max_diff:.1f}% difference in missingness rate "
                            "across target classes, which may indicate non-random missing data."
                        )
                    })

            target_result["total_features_analyzed"] = features_analyzed

            # Sort by impact
            target_result["features_with_differential_missingness"].sort(
                key=lambda x: x.get("max_difference", 0), reverse=True
            )

            # Generate interpretation
            n_diff = len(target_result["features_with_differential_missingness"])
            if n_diff > 0:
                top_feature = target_result["features_with_differential_missingness"][0]["feature"]
                target_result["interpretation"] = (
                    f"Found {n_diff} feature(s) with differential missingness across '{target_col}' values. "
                    f"'{top_feature}' shows the largest difference. This suggests missing data may not be "
                    "random and could affect model training."
                )
            else:
                target_result["interpretation"] = (
                    f"No significant differential missingness detected for '{target_col}'. "
                    "Missing data appears to be relatively uniform across target classes."
                )

            results[target_col] = target_result

        return results

    def _recommend_targets_mi(self, df: pd.DataFrame, max_targets: int = 3, sample_size: int = 1000) -> List[Dict[str, Any]]:
        """
        Data-driven ML target recommendation using Cramér's V (fast).

        Uses a balanced scoring approach:
        - Predictability: Some signal exists (measured via Cramér's V)
        - Not too predictable: Penalizes derived/redundant columns
        - Binary bonus: Binary columns are common targets
        - Semantic type: Uses FIBO/Schema.org semantic classification
        - Completeness: Targets typically have no missing values

        Args:
            df: DataFrame to analyze
            max_targets: Maximum number of targets to recommend
            sample_size: Sample size for computation (for performance)

        Returns:
            List of recommended targets with scores and feature importance
        """
        # Only sample if dataset exceeds threshold
        if len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)

        # Semantic types that indicate likely ML targets (FIBO + Schema.org)
        # These are category/classification/outcome types, not domain-specific field names
        target_semantic_types = {
            # FIBO categories (high likelihood targets)
            'category.transaction_type': 0.30,
            'category.account_type': 0.30,
            'category.payment_method': 0.25,
            'status': 0.35,
            # Schema.org categories (resolved primary_type)
            'schema:Boolean': 0.35,      # Boolean flags are very common targets
            'schema:CategoryCode': 0.30,  # Category codes are common targets
            'schema:ItemList': 0.20,      # Enumerated lists can be targets
        }

        # Semantic types to SKIP as targets (identifiers, amounts, etc.)
        skip_semantic_types = {
            # FIBO identifiers
            'identifier', 'identifier.uuid', 'identifier.code',
            'banking.account', 'banking.transaction',
            'party.customer_id', 'loan.identifier', 'security.identifier',
            'money.amount', 'money.price', 'money.currency',
            # Schema.org identifiers and non-target types
            'schema:identifier', 'schema:URL', 'schema:email',
            'schema:telephone', 'schema:PostalAddress',
            'schema:MonetaryAmount', 'schema:QuantitativeValue',
            'schema:DateTime', 'schema:Date', 'schema:Time',
        }

        candidates = []
        n_rows = len(df)

        for col in df.columns:
            try:
                n_unique = df[col].nunique(dropna=True)
                completeness = df[col].notna().mean()

                # Get semantic info for this column (supports both old and new format)
                sem_info = self._column_semantic_info.get(col, {})
                # Try resolved format first (schema_org/fibo combined), then fallback to old primary_tag
                resolved = sem_info.get('resolved', {})
                primary_type = resolved.get('primary_type', '') or sem_info.get('primary_tag', '')

                # Skip identifier/amount columns based on semantic type
                if primary_type:
                    # Check if tag or parent matches skip list
                    should_skip = False
                    for skip_type in skip_semantic_types:
                        if primary_type == skip_type or primary_type.startswith(skip_type + '.'):
                            should_skip = True
                            break
                    if should_skip:
                        continue

                # Candidate filters
                is_low_cardinality = 2 <= n_unique <= 20
                is_complete = completeness >= 0.90
                is_not_id = (n_unique / n_rows) < 0.5

                if is_low_cardinality and is_complete and is_not_id:
                    candidates.append({
                        'column': col,
                        'n_classes': n_unique,
                        'completeness': completeness,
                        'is_binary': n_unique == 2,
                        'semantic_type': primary_type,
                        'semantic_source': resolved.get('primary_source', '')
                    })
            except Exception:
                continue

        if not candidates:
            return []

        # Limit candidates for performance
        candidates = candidates[:5]

        # Compute predictability for each candidate
        for cand in candidates:
            col = cand['column']
            max_mi, top_features = self._compute_mi_predictability(df, col)
            cand['raw_predictability'] = max_mi
            cand['top_features'] = top_features

            # === BALANCED SCORING ===
            score = 0.0
            reasons = []

            # 1. Base predictability (has SOME signal, 0.05-0.5 is sweet spot)
            if max_mi >= 0.05:
                signal_score = min(max_mi, 0.5) / 0.5  # Cap at 0.5
                score += signal_score * 0.35
                reasons.append(f"signal:{max_mi:.2f}")

            # 2. Penalize TOO predictable (likely derived column)
            if max_mi > 0.6:
                penalty = (max_mi - 0.6) * 0.5
                score -= penalty
                reasons.append("derived-penalty")

            # 3. Binary bonus (binary columns are common targets)
            if cand['is_binary']:
                score += 0.25
                reasons.append("binary")

            # 4. Semantic type scoring (FIBO + Schema.org combined)
            sem_type = cand.get('semantic_type', '')
            sem_source = cand.get('semantic_source', '')
            semantic_boost = 0.0

            # Check for exact match in target semantic types
            if sem_type in target_semantic_types:
                semantic_boost = target_semantic_types[sem_type]
            # Check for parent category match (e.g., 'category.foo' → 'category')
            elif '.' in sem_type:
                parent = sem_type.split('.')[0]
                if parent == 'category':
                    semantic_boost = 0.20  # Category fields often good targets
            # Check for Schema.org type prefix match
            elif sem_type.startswith('schema:') and 'Category' in sem_type:
                semantic_boost = 0.25  # Generic category boost for Schema.org

            if semantic_boost > 0:
                score += semantic_boost
                source_label = f"[{sem_source}]" if sem_source else ""
                reasons.append(f"semantic:{sem_type}{source_label}")

            # 5. High completeness bonus
            if cand['completeness'] >= 0.99:
                score += 0.10
                reasons.append("complete")

            cand['score'] = max(0, score)
            cand['reasons'] = reasons

        # Rank by balanced score
        candidates.sort(key=lambda x: x['score'], reverse=True)

        # Assign confidence and prepare output
        results = []
        for c in candidates[:max_targets]:
            if c['score'] > 0.5:
                confidence = 'high'
            elif c['score'] > 0.3:
                confidence = 'medium'
            else:
                confidence = 'low'

            results.append({
                'column': c['column'],
                'n_classes': c['n_classes'],
                'completeness': c['completeness'],
                'is_binary': c['is_binary'],
                'score': c['score'],
                'confidence': confidence,
                'raw_predictability': c['raw_predictability'],
                'reasons': c['reasons'],
                'top_features': c['top_features']
            })

        return results

    def _compute_mi_predictability(self, df: pd.DataFrame, target_col: str) -> Tuple[float, List[Dict]]:
        """
        Compute how predictable a column is using Cramér's V (fast scipy-based).

        Uses scipy.stats.chi2_contingency for vectorized chi-squared computation.

        Args:
            df: DataFrame
            target_col: Column to evaluate as potential target

        Returns:
            Tuple of (max_score, list of top features with importance)
        """
        try:
            from scipy.stats import chi2_contingency
        except ImportError:
            return 0.0, []

        # Skip high-cardinality and ID-like columns as features
        feature_cols = []
        for c in df.columns:
            if c == target_col:
                continue
            n_unique = df[c].nunique()
            if n_unique > 50 or n_unique == len(df):  # Skip high-cardinality
                continue
            feature_cols.append(c)
            if len(feature_cols) >= 8:  # Limit features for speed
                break

        if not feature_cols:
            return 0.0, []

        scores = []
        target_series = df[target_col].fillna('__NA__').astype(str)

        for col in feature_cols:
            try:
                feature_series = df[col].fillna('__NA__').astype(str)

                # Compute Cramér's V using scipy (vectorized)
                contingency = pd.crosstab(target_series, feature_series)
                if contingency.size < 4:
                    scores.append((col, 0.0))
                    continue

                n = contingency.sum().sum()
                chi2, _, _, _ = chi2_contingency(contingency.values)

                # Cramér's V
                min_dim = min(contingency.shape) - 1
                if min_dim > 0 and n > 0:
                    cramers_v = np.sqrt(chi2 / (n * min_dim))
                else:
                    cramers_v = 0.0

                scores.append((col, min(cramers_v, 1.0)))  # Cap at 1.0
            except Exception:
                scores.append((col, 0.0))

        # Sort by score
        scores.sort(key=lambda x: x[1], reverse=True)
        top_features = [{'feature': f, 'importance': float(s)} for f, s in scores[:5]]

        return float(max(s for _, s in scores)) if scores else 0.0, top_features

    def _compute_target_feature_analysis_direct(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Compute target-feature association analysis directly from DataFrame.

        For each detected target column, calculate association strength with all
        other features using correlation (numeric) and Cramér's V approximation (categorical).

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary with feature associations per target column
        """
        results = {}

        for target_col in self.detected_targets:
            if target_col not in df.columns:
                continue

            target_result = {
                "target_column": target_col,
                "target_distribution": {},
                "feature_associations": [],
                "total_rows_analyzed": 0,
                "interpretation": ""
            }

            # Get target values
            target_series = df[target_col].dropna()
            if len(target_series) < 50:  # Need minimum sample
                continue

            target_result["total_rows_analyzed"] = len(target_series)

            # Get target distribution
            target_counts = target_series.value_counts()
            total = len(target_series)
            for val, count in target_counts.head(10).items():
                target_result["target_distribution"][str(val)] = {
                    "count": int(count),
                    "percentage": round(count / total * 100, 2)
                }

            # Analyze each feature
            for col in df.columns:
                if col == target_col:
                    continue

                try:
                    # Get feature series (aligned with target)
                    feature_series = df.loc[target_series.index, col]

                    # Determine if numeric or categorical
                    is_numeric = pd.api.types.is_numeric_dtype(feature_series)

                    if is_numeric:
                        # Numeric: compute correlation with target
                        # Convert target to numeric if binary (0/1)
                        target_numeric = pd.to_numeric(target_series, errors='coerce')
                        feature_numeric = pd.to_numeric(feature_series, errors='coerce')

                        # Get valid pairs
                        valid_mask = target_numeric.notna() & feature_numeric.notna()
                        if valid_mask.sum() < 30:
                            continue

                        # Compute point-biserial correlation (or Pearson if both numeric)
                        correlation = target_numeric[valid_mask].corr(feature_numeric[valid_mask])
                        if pd.isna(correlation):
                            continue

                        association_strength = abs(correlation)
                        association_type = "numeric_correlation"

                    else:
                        # Categorical: compute Cramér's V approximation
                        # Create contingency table
                        contingency = pd.crosstab(target_series, feature_series)

                        if contingency.size < 4:  # Need at least 2x2
                            continue

                        # Chi-square approximation for Cramér's V
                        n = contingency.sum().sum()
                        chi2 = 0
                        row_sums = contingency.sum(axis=1)
                        col_sums = contingency.sum(axis=0)

                        for i, row in enumerate(contingency.index):
                            for j, col_name in enumerate(contingency.columns):
                                observed = contingency.iloc[i, j]
                                expected = (row_sums.iloc[i] * col_sums.iloc[j]) / n
                                if expected > 0:
                                    chi2 += ((observed - expected) ** 2) / expected

                        # Cramér's V
                        k = min(len(contingency.index), len(contingency.columns))
                        if k > 1 and n > 0:
                            cramers_v = (chi2 / (n * (k - 1))) ** 0.5
                            association_strength = min(1.0, cramers_v)  # Cap at 1.0
                        else:
                            continue

                        association_type = "categorical_cramers_v"

                    # Only include meaningful associations
                    if association_strength >= 0.05:
                        target_result["feature_associations"].append({
                            "feature": col,
                            "association_strength": round(association_strength, 3),
                            "type": association_type
                        })

                except Exception:
                    # Skip problematic columns
                    continue

            # Sort by strength and keep top 10
            target_result["feature_associations"].sort(
                key=lambda x: x.get("association_strength", 0), reverse=True
            )
            target_result["feature_associations"] = target_result["feature_associations"][:10]

            # Generate interpretation
            if target_result["feature_associations"]:
                top_feat = target_result["feature_associations"][0]
                strength = top_feat["association_strength"]
                strength_word = "strong" if strength >= 0.3 else "moderate" if strength >= 0.15 else "weak"
                target_result["interpretation"] = (
                    f"'{top_feat['feature']}' shows the {strength_word} association "
                    f"with '{target_col}' (strength: {strength:.2f}). "
                    f"Top {len(target_result['feature_associations'])} predictive features identified."
                )
            else:
                target_result["interpretation"] = (
                    f"No strong feature associations detected for '{target_col}'."
                )

            results[target_col] = target_result

        return results

    def analyze(self, df: pd.DataFrame, column_semantic_info: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Run ML analysis on dataframe.

        Args:
            df: DataFrame to analyze (will be sampled if too large)
            column_semantic_info: Dict mapping column names to their semantic info (from profiler)
                                  Each entry has 'semantic_tags', 'primary_tag', etc.

        Returns:
            Dictionary containing ML findings
        """
        start_time = time.time()

        # Store semantic info for use by detection methods
        self._column_semantic_info = column_semantic_info or {}

        # Detect target columns (needed for is_target_like in visualizations)
        self.detected_targets = self._detect_target_columns(df)

        # No internal sampling - engine handles all sampling decisions
        original_rows = len(df)
        logger.info(f"ML Analysis: Analyzing {len(df):,} rows")

        findings = {
            "sample_info": {
                "original_rows": original_rows,
                "analyzed_rows": len(df)
            },
            # Tier 1: Data Authenticity
            "benford_analysis": {},
            # Tier 2: Record Anomalies
            "numeric_outliers": {},
            "autoencoder_anomalies": {},
            # Tier 3: Data Quality Issues
            "rare_categories": {},
            "format_anomalies": {},
            "cross_column_issues": [],
            # Tier 4: Pattern Analysis
            "temporal_patterns": {},
            "correlation_anomalies": {},
            # Informational (not counted as issues)
            "clustering_analysis": {},
            "summary": {
                "total_issues": 0,
                "severity": "low",
                "key_findings": []
            }
        }

        # Get column types - use enhanced detection that coerces string columns to numeric
        # This catches columns like "TotalCharges" that contain numeric values but are stored as strings
        numeric_cols, coerced_info = self._get_numeric_columns(df, exclude_binary=True)
        string_cols = df.select_dtypes(include=['object']).columns.tolist()

        # Log coercion info for transparency
        if coerced_info:
            coerced_cols = [col for col, info in coerced_info.items() if 'coerced' in info]
            excluded_cols = [col for col, info in coerced_info.items() if 'excluded' in info]
            if coerced_cols:
                logger.info(f"Coerced {len(coerced_cols)} string columns to numeric: {coerced_cols}")
            if excluded_cols:
                logger.debug(f"Excluded {len(excluded_cols)} binary/near-binary columns: {excluded_cols}")
            findings["numeric_column_detection"] = coerced_info

        # Filter out numeric columns that are semantically categorical (e.g., bank IDs)
        # These shouldn't have Isolation Forest applied - a bank ID of 1099 isn't an "outlier"
        actual_numeric_cols = []
        skipped_semantic = []
        for col in numeric_cols:
            should_skip, reason = self._should_skip_numeric_outlier_detection(col)
            if should_skip:
                skipped_semantic.append((col, reason))
                logger.debug(f"Skipping numeric outlier detection for {col}: {reason}")
            else:
                actual_numeric_cols.append(col)

        if skipped_semantic:
            findings["skipped_numeric_semantic"] = [
                {"column": col, "reason": reason} for col, reason in skipped_semantic
            ]

        # 1. Univariate outlier detection with adaptive contamination (only on true numeric cols)
        # Store ALL results for visualization (even 0% outliers)
        all_numeric_outlier_stats = {}
        for col in actual_numeric_cols:
            outlier_result = self._detect_numeric_outliers(df, col)
            if outlier_result:
                # Always store for visualization
                all_numeric_outlier_stats[col] = outlier_result
                # Only include in main findings if actual outliers found
                if outlier_result.get("anomaly_count", 0) > 0:
                    findings["numeric_outliers"][col] = outlier_result

        findings["all_numeric_outlier_stats"] = all_numeric_outlier_stats

        # 2. Clustering analysis (INFORMATIONAL ONLY - shows data structure, not anomalies)
        # Note: Noise points are NOT counted as issues - they overlap with outlier detection
        if len(actual_numeric_cols) >= 2 and self._sklearn_available:
            clustering_result = self._analyze_clusters(df, actual_numeric_cols)
            if clustering_result:
                clustering_result["is_informational"] = True  # Flag as informational
                findings["clustering_analysis"] = clustering_result

        # 4. Format pattern analysis (skip low cardinality columns)
        for col in string_cols:
            unique_count = df[col].nunique()
            if unique_count <= 100:
                logger.debug(f"Skipping format analysis for categorical column {col}")
                continue
            format_result = self._analyze_format_patterns(df, col)
            if format_result and format_result.get("anomaly_count", 0) > 0:
                findings["format_anomalies"][col] = format_result

        # 5. Rare category detection with adaptive threshold
        for col in string_cols:
            if self._looks_like_datetime(df[col]):
                continue
            unique_count = df[col].nunique()
            if 2 <= unique_count <= 100:
                rare_result = self._detect_rare_categories(df, col)
                if rare_result and rare_result.get("rare_values"):
                    findings["rare_categories"][col] = rare_result

        # 6. Cross-column consistency
        cross_col_issues = self._check_cross_column_consistency(df)
        findings["cross_column_issues"] = cross_col_issues

        # 7. Correlation-based anomaly detection (only on true numeric columns)
        # Correlations between numeric IDs (bank IDs, account numbers) are meaningless
        if len(actual_numeric_cols) >= 2:
            corr_anomalies = self._detect_correlation_anomalies(df, actual_numeric_cols)
            if corr_anomalies:
                findings["correlation_anomalies"] = corr_anomalies

        # 8. Temporal pattern analysis
        for col in string_cols:
            if self._looks_like_datetime(df[col]):
                temporal_result = self._analyze_temporal_patterns(df, col)
                if temporal_result:
                    findings["temporal_patterns"][col] = temporal_result

        # Also check datetime64 columns
        datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        for col in datetime_cols:
            temporal_result = self._analyze_temporal_patterns(df, col)
            if temporal_result:
                findings["temporal_patterns"][col] = temporal_result

        # 9. Benford's Law analysis (based on numeric properties, not just FIBO tags)
        # Include ALL results (both suspicious and natural) so users see full analysis
        # Track ineligible columns with reasons for fallback display
        benford_ineligible = {}
        for col in numeric_cols:
            # Handle string columns that need coercion to numeric
            series = df[col]
            if series.dtype == 'object':
                series = pd.to_numeric(series, errors='coerce')
            values = series.dropna().values
            if self._should_apply_benford(col, values):
                benford_result = self._detect_benford_anomalies(df, col)
                if benford_result:
                    findings["benford_analysis"][col] = benford_result
            else:
                # Track why column was excluded
                benford_ineligible[col] = self._get_benford_ineligibility_reason(col, values)

        findings["benford_ineligible"] = benford_ineligible

        # 10. Autoencoder anomaly detection (on true numeric columns)
        if len(actual_numeric_cols) >= 2 and self._sklearn_available:
            autoencoder_result = self._detect_autoencoder_anomalies(df, actual_numeric_cols)
            if autoencoder_result:
                findings["autoencoder_anomalies"] = autoencoder_result

        # 11. NEW: Duplicate detection
        duplicate_result = self._detect_duplicates(df)
        if duplicate_result:
            findings["duplicate_analysis"] = duplicate_result

        # 12. NEW: Referential integrity checks
        ref_integrity = self._check_referential_integrity(df)
        if ref_integrity:
            findings["referential_integrity"] = ref_integrity

        # 13. NEW: Distribution fitting (on key numeric columns)
        findings["distribution_analysis"] = {}
        for col in actual_numeric_cols[:5]:  # Limit to first 5 numeric columns for performance
            # Handle string columns that need coercion to numeric
            series = df[col]
            if series.dtype == 'object':
                series = pd.to_numeric(series, errors='coerce')
            values = series.dropna().values
            if len(values) >= 100:
                dist_result = self._fit_distribution(values)
                if dist_result and dist_result.get("best_fit"):
                    findings["distribution_analysis"][col] = dist_result

        # 14. Missingness Impact Analysis (target-based differential missingness)
        if self.detected_targets:
            missingness_result = self._compute_missingness_impact_direct(df)
            if missingness_result:
                findings["missingness_impact"] = missingness_result

        # 15. Target-Feature Analysis (feature importance for detected targets)
        if self.detected_targets:
            target_feature_result = self._compute_target_feature_analysis_direct(df)
            if target_feature_result:
                findings["target_feature_analysis"] = target_feature_result

        # 16. MI-based Target Recommendation (data-driven target detection)
        try:
            target_recommendations = self._recommend_targets_mi(df, max_targets=3)
            if target_recommendations:
                findings["recommended_targets"] = target_recommendations
                logger.info(f"Recommended {len(target_recommendations)} ML targets: {[t['column'] for t in target_recommendations]}")
        except Exception as e:
            logger.debug(f"Target recommendation failed: {e}")

        # Generate summary
        findings["summary"] = self._generate_summary(findings)
        findings["analysis_time_seconds"] = round(time.time() - start_time, 2)

        # Generate visualization data
        findings["visualizations"] = self._generate_visualization_data_direct(df, findings, numeric_cols, string_cols)

        return findings

    def _generate_visualization_data_direct(self, df: pd.DataFrame, findings: Dict,
                                            numeric_cols: List[str], string_cols: List[str]) -> Dict[str, Any]:
        """
        Generate visualization data directly from DataFrame (for non-chunked processing).

        Args:
            df: DataFrame to analyze
            findings: Current findings dict
            numeric_cols: List of numeric column names
            string_cols: List of string column names

        Returns:
            Dictionary with visualization data
        """
        viz_data = {
            "amount_distributions": {},
            "amount_scatter": None,
            "class_imbalance": {},
            "temporal_density": {},
            "anomaly_scores": {},
            "reconstruction_errors": None,
            "sample_info": {
                "total_rows": len(df),
                "sample_size": len(df),
                "is_sampled": findings.get("sample_info", {}).get("sampled", False)
            }
        }

        # 1. Amount field distributions (log-scaled histograms)
        amount_keywords = ['amount', 'price', 'cost', 'fee', 'total', 'balance', 'value', 'sum', 'payment', 'fare']
        for col in numeric_cols:
            col_lower = str(col).lower() if col is not None else ''
            is_amount = any(kw in col_lower for kw in amount_keywords)
            if is_amount:
                values = df[col].dropna().values
                if len(values) >= 100:
                    viz_data["amount_distributions"][col] = self._compute_log_histogram_direct(values)

        # 2. Amount scatter plot (find Received vs Paid pairs)
        received_col = None
        paid_col = None
        for col in numeric_cols:
            col_lower = str(col).lower() if col is not None else ''
            if 'received' in col_lower or 'recv' in col_lower:
                received_col = col
            elif 'paid' in col_lower or 'sent' in col_lower:
                paid_col = col

        if received_col and paid_col:
            try:
                scatter_df = df[[received_col, paid_col]].dropna()
                if len(scatter_df) > 0:
                    scatter_data = [{"x": float(row[received_col]), "y": float(row[paid_col])}
                                   for _, row in scatter_df.sample(min(2000, len(scatter_df))).iterrows()]
                    viz_data["amount_scatter"] = {
                        "x_column": received_col,
                        "y_column": paid_col,
                        "points": scatter_data,
                        "total_points": len(scatter_df)
                    }
            except Exception as e:
                logger.debug(f"Scatter plot generation failed: {e}")

        # 3. Class imbalance data (for binary/low-cardinality columns)
        # Use detected_targets from proper target detection (not simple keyword matching)
        for col in df.columns:
            is_target = col in self.detected_targets

            try:
                unique_count = df[col].nunique()
                # Include all low-cardinality categorical columns (useful as ML features or targets)
                if unique_count <= 10:
                    value_counts = df[col].value_counts()
                    total = len(df[col].dropna())
                    if total > 0:
                        viz_data["class_imbalance"][col] = {
                            "classes": [{"value": str(v), "count": int(c), "percentage": round(c/total*100, 2)}
                                       for v, c in value_counts.items()],
                            "is_binary": unique_count == 2,
                            "is_target_like": is_target,
                            "total": total
                        }
            except Exception:
                pass

        # 4. Reconstruction error distribution from autoencoder findings
        ae_data = findings.get("autoencoder_anomalies", {})
        if ae_data and "error_stats" in ae_data:
            error_stats = ae_data["error_stats"]
            viz_data["reconstruction_errors"] = {
                "mean": error_stats.get("mean", 0),
                "median": error_stats.get("median", 0),
                "q75": error_stats.get("q75", 0),
                "std": error_stats.get("std", 0),
                "max": error_stats.get("max", 0),
                "threshold": ae_data.get("threshold", 0),
                "anomaly_count": ae_data.get("anomaly_count", 0),
                "anomaly_percentage": ae_data.get("anomaly_percentage", 0),
                "anomaly_min_error": error_stats.get("anomaly_min_error", 0)
            }

        # 5. Anomaly score info from isolation forest
        for col, outlier_data in findings.get("numeric_outliers", {}).items():
            if "anomaly_score_range" in outlier_data:
                score_range = outlier_data["anomaly_score_range"]
                viz_data["anomaly_scores"][col] = {
                    "min_score": score_range.get("min", 0),
                    "max_score": score_range.get("max", 0),
                    "anomaly_count": outlier_data.get("anomaly_count", 0),
                    "total_analyzed": len(df)
                }

        # 6. Temporal density (activity timeline) for datetime columns
        temporal_cols = df.select_dtypes(include=['datetime64']).columns.tolist()

        # Also check string columns that look like dates
        for col in string_cols:
            if self._looks_like_datetime(df[col]):
                temporal_cols.append(col)

        for col in temporal_cols[:2]:  # Limit to first 2 temporal columns
            try:
                temporal_data = self._compute_temporal_density(df, col)
                if temporal_data:
                    viz_data["temporal_density"][col] = temporal_data
            except Exception as e:
                logger.debug(f"Temporal density computation failed for {col}: {e}")

        return viz_data

    def _compute_temporal_density(self, df: pd.DataFrame, col: str, bins: int = 50) -> Dict[str, Any]:
        """
        Compute temporal density (activity timeline) for a datetime column.

        Args:
            df: DataFrame
            col: Column name
            bins: Number of time bins

        Returns:
            Dictionary with temporal density data for charting
        """
        try:
            # Convert to datetime if needed
            if df[col].dtype == 'object':
                dates = pd.to_datetime(df[col], errors='coerce')
            else:
                dates = df[col]

            dates = dates.dropna()
            if len(dates) < 10:
                return {}

            # Convert to numeric (timestamps)
            timestamps = dates.astype(np.int64) // 10**9  # Convert to seconds

            # Compute histogram
            hist, bin_edges = np.histogram(timestamps, bins=bins)

            # Convert bin edges back to datetime strings for display
            bin_dates = pd.to_datetime(bin_edges, unit='s')

            # Determine appropriate time format based on date range
            date_range = dates.max() - dates.min()
            if date_range.days > 365:
                date_format = '%Y-%m'
            elif date_range.days > 30:
                date_format = '%Y-%m-%d'
            elif date_range.days > 1:
                date_format = '%m-%d %H:%M'
            else:
                date_format = '%H:%M'

            labels = [d.strftime(date_format) for d in bin_dates[:-1]]

            # Detect gaps (bins with zero or very low activity)
            mean_activity = np.mean(hist)
            gap_threshold = mean_activity * 0.1  # Less than 10% of mean
            gaps = []
            for i, count in enumerate(hist):
                if count <= gap_threshold and i < len(labels):
                    gaps.append(labels[i])

            # Calculate peak activity
            peak_idx = np.argmax(hist)
            peak_label = labels[peak_idx] if peak_idx < len(labels) else labels[-1]

            return {
                "histogram": hist.tolist(),
                "labels": labels,
                "min_date": str(dates.min()),
                "max_date": str(dates.max()),
                "total_records": len(dates),
                "peak_activity": {
                    "label": peak_label,
                    "count": int(hist[peak_idx])
                },
                "gaps_detected": len(gaps),
                "gap_labels": gaps[:10],  # Limit to first 10 gaps
                "date_range_days": date_range.days,
                "mean_per_bin": round(float(mean_activity), 1)
            }
        except Exception as e:
            logger.debug(f"Temporal density computation failed: {e}")
            return {}

    def _compute_log_histogram_direct(self, values: np.ndarray, bins: int = 30) -> Dict[str, Any]:
        """Compute log-scaled histogram from numpy array."""
        try:
            arr = values[values > 0]  # Filter to positive values only
            if len(arr) < 10:
                return {}

            # Compute log-scaled bins
            log_arr = np.log10(arr)
            hist, bin_edges = np.histogram(log_arr, bins=bins)

            return {
                "histogram": hist.tolist(),
                "bin_edges_log": bin_edges.tolist(),
                "bin_edges": (10 ** bin_edges).tolist(),
                "min_value": float(arr.min()),
                "max_value": float(arr.max()),
                "median": float(np.median(arr)),
                "mean": float(np.mean(arr)),
                "total_samples": len(arr),
                "log_scale_explanation": "Values transformed using log10 scale to visualize skewed distributions"
            }
        except Exception as e:
            logger.debug(f"Log histogram computation failed: {e}")
            return {}

    def _estimate_contamination(self, values: np.ndarray) -> float:
        """
        Estimate appropriate contamination rate based on data characteristics.
        Uses IQR-based approach with dataset size adjustment.

        Args:
            values: Array of numeric values

        Returns:
            Estimated contamination rate (between 0.001 and 0.05)
        """
        n = len(values)
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1

        if iqr == 0:
            # No variance in IQR, use conservative size-adjusted default
            return max(0.001, min(5 / n, 0.005))

        # Use 3*IQR for extreme outliers (more conservative, per expert recommendation)
        extreme_lower = q1 - 3 * iqr
        extreme_upper = q3 + 3 * iqr
        extreme_count = np.sum((values < extreme_lower) | (values > extreme_upper))
        extreme_rate = extreme_count / n

        # Dataset size factor - larger datasets need lower contamination
        # This prevents over-detection in very large datasets
        if n > 1_000_000:
            size_factor = 0.5
        elif n > 100_000:
            size_factor = 0.7
        elif n > 10_000:
            size_factor = 0.85
        else:
            size_factor = 1.0

        # Calculate contamination with size adjustment
        if extreme_rate > 0.001:
            # Use extreme rate with modest buffer (1.2x instead of 1.5x)
            contamination = extreme_rate * size_factor * 1.2
        else:
            # Default for clean data (reduced from 0.01 to 0.005)
            contamination = 0.005 * size_factor

        # Dynamic minimum based on sample size
        min_contamination = max(0.001, min(5 / n, 0.005))

        # Clamp to valid range (max reduced from 0.1 to 0.05)
        return max(min_contamination, min(contamination, 0.05))

    def _detect_numeric_outliers(self, df: pd.DataFrame, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Detect outliers in numeric column using Isolation Forest with adaptive contamination.
        """
        if not self._sklearn_available:
            return self._detect_outliers_statistical(df, col_name)

        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            return self._detect_outliers_statistical(df, col_name)

        series = df[col_name]

        # Handle string columns that need coercion to numeric
        if series.dtype == 'object':
            series = pd.to_numeric(series, errors='coerce')

        values = series.dropna().values
        if len(values) < MIN_ROWS_FOR_ML:
            return None

        # Skip binary/boolean columns - rare class is not an anomaly
        unique_values = np.unique(values)
        if len(unique_values) <= 2:
            logger.debug(f"Skipping binary column {col_name} - outlier detection not meaningful")
            return None

        try:
            # Estimate appropriate contamination rate
            contamination = self._estimate_contamination(values)

            # Use Isolation Forest with adaptive contamination
            iso = IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100,
                max_samples='auto',
                n_jobs=-1
            )
            predictions = iso.fit_predict(values.reshape(-1, 1))

            # Get anomaly scores for ranking
            scores = iso.decision_function(values.reshape(-1, 1))

            anomaly_indices = np.where(predictions == -1)[0]
            anomaly_values = values[anomaly_indices]
            anomaly_scores = scores[anomaly_indices]

            if len(anomaly_values) == 0:
                return None

            # Sort by anomaly score (most anomalous first)
            sorted_idx = np.argsort(anomaly_scores)
            top_anomaly_indices = anomaly_indices[sorted_idx[:10]]

            # Get sample rows for top anomalies
            non_null_df = df[df[col_name].notna()].reset_index(drop=True)
            sample_rows = []
            for idx in top_anomaly_indices[:5]:
                if idx < len(non_null_df):
                    row = non_null_df.iloc[idx]
                    row_dict = {str(k)[:25]: str(v)[:50] for k, v in row.items()}
                    sample_rows.append(row_dict)

            anomaly_pct = len(anomaly_values) / len(values) * 100
            confidence = self._calculate_confidence(
                'isolation_forest', len(anomaly_values), anomaly_pct, len(values)
            )

            return {
                "method": "isolation_forest",
                "contamination_used": round(contamination, 4),
                "anomaly_count": len(anomaly_values),
                "anomaly_percentage": round(anomaly_pct, 4),
                "confidence": confidence,
                "normal_range": {
                    "min": float(np.percentile(values, 1)),
                    "max": float(np.percentile(values, 99)),
                    "median": float(np.median(values))
                },
                "anomaly_range": {
                    "min": float(anomaly_values.min()),
                    "max": float(anomaly_values.max()),
                    "mean": float(anomaly_values.mean())
                },
                "top_anomalies": sorted([float(v) for v in anomaly_values[sorted_idx[:5]]], reverse=True),
                "anomaly_score_range": {
                    "min": float(anomaly_scores.min()),
                    "max": float(anomaly_scores.max())
                },
                "sample_rows": sample_rows,
                "interpretation": self._interpret_numeric_outliers(col_name, anomaly_values, values, contamination)
            }
        except Exception as e:
            logger.debug(f"Error in outlier detection for {col_name}: {e}")
            return self._detect_outliers_statistical(df, col_name)

    def _detect_outliers_statistical(self, df: pd.DataFrame, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Fallback statistical outlier detection when sklearn unavailable.
        Uses IQR method.
        """
        series = df[col_name]

        # Handle string columns that need coercion to numeric
        if series.dtype == 'object':
            series = pd.to_numeric(series, errors='coerce')

        values = series.dropna().values
        if len(values) < MIN_ROWS_FOR_ML:
            return None

        # Skip binary/boolean columns - rare class is not an anomaly
        unique_values = np.unique(values)
        if len(unique_values) <= 2:
            return None

        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1

        if iqr == 0:
            return None

        lower_bound = q1 - 3 * iqr  # Use 3*IQR for extreme outliers
        upper_bound = q3 + 3 * iqr

        outlier_mask = (values < lower_bound) | (values > upper_bound)
        anomaly_values = values[outlier_mask]

        if len(anomaly_values) == 0:
            return None

        non_null_df = df[df[col_name].notna()].reset_index(drop=True)
        anomaly_indices = np.where(outlier_mask)[0][:5]
        sample_rows = []
        for idx in anomaly_indices:
            if idx < len(non_null_df):
                row = non_null_df.iloc[idx]
                row_dict = {str(k)[:25]: str(v)[:50] for k, v in row.items()}
                sample_rows.append(row_dict)

        return {
            "method": "iqr_statistical",
            "anomaly_count": len(anomaly_values),
            "anomaly_percentage": round(len(anomaly_values) / len(values) * 100, 4),
            "normal_range": {
                "min": float(lower_bound),
                "max": float(upper_bound),
                "median": float(np.median(values))
            },
            "anomaly_range": {
                "min": float(anomaly_values.min()),
                "max": float(anomaly_values.max())
            },
            "top_anomalies": sorted([float(v) for v in anomaly_values])[-5:],
            "sample_rows": sample_rows,
            "interpretation": self._interpret_numeric_outliers(col_name, anomaly_values, values, None)
        }

    def _detect_multivariate_outliers(self, df: pd.DataFrame, numeric_cols: List[str]) -> Optional[Dict[str, Any]]:
        """
        Detect multivariate outliers that may not be visible in univariate analysis.
        Uses Isolation Forest on multiple columns simultaneously.
        """
        try:
            from sklearn.ensemble import IsolationForest
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            return None

        # Select columns with sufficient non-null data
        valid_cols = []
        for col in numeric_cols:
            if df[col].notna().sum() >= MIN_ROWS_FOR_ML:
                valid_cols.append(col)

        if len(valid_cols) < 2:
            return None

        # Limit to top 10 columns by variance to avoid curse of dimensionality
        if len(valid_cols) > 10:
            variances = df[valid_cols].var()
            valid_cols = variances.nlargest(10).index.tolist()

        try:
            # Prepare data - drop rows with any nulls in selected columns
            subset_df = df[valid_cols].dropna()
            if len(subset_df) < MIN_ROWS_FOR_ML:
                return None

            # Standardize features
            scaler = StandardScaler()
            scaled_data = scaler.fit_transform(subset_df)

            # Estimate contamination from univariate analysis
            contamination_estimates = []
            for col in valid_cols:
                col_contam = self._estimate_contamination(subset_df[col].values)
                contamination_estimates.append(col_contam)

            # Use 75th percentile of univariate estimates (not mean * 1.5)
            # Per expert recommendation: multivariate outliers are often rarer, not more common
            contamination = min(np.percentile(contamination_estimates, 75), 0.05)
            contamination = max(contamination, 0.005)

            iso = IsolationForest(
                contamination=contamination,
                random_state=42,
                n_estimators=100,
                n_jobs=-1
            )
            predictions = iso.fit_predict(scaled_data)
            scores = iso.decision_function(scaled_data)

            anomaly_mask = predictions == -1
            anomaly_count = anomaly_mask.sum()

            if anomaly_count == 0:
                return None

            # Find which columns contribute most to anomalies
            anomaly_data = subset_df[anomaly_mask]
            normal_data = subset_df[~anomaly_mask]

            # Calculate z-scores for anomalous points
            contributing_columns = []
            for col in valid_cols:
                if len(normal_data) > 0:
                    mean_normal = normal_data[col].mean()
                    std_normal = normal_data[col].std()
                    if std_normal > 0:
                        mean_anomaly = anomaly_data[col].mean()
                        z_diff = abs(mean_anomaly - mean_normal) / std_normal
                        if z_diff > 1.5:
                            contributing_columns.append({
                                "column": col,
                                "z_score_diff": round(z_diff, 2),
                                "normal_mean": round(mean_normal, 2),
                                "anomaly_mean": round(mean_anomaly, 2)
                            })

            # Sort by z-score difference
            contributing_columns.sort(key=lambda x: x["z_score_diff"], reverse=True)

            # Get sample anomalous rows
            anomaly_indices = np.where(anomaly_mask)[0]
            sorted_by_score = np.argsort(scores[anomaly_mask])[:5]
            sample_rows = []
            for i in sorted_by_score:
                if i < len(anomaly_data):
                    row = subset_df.iloc[anomaly_indices[i]]
                    row_dict = {str(k)[:25]: f"{v:.2f}" if isinstance(v, float) else str(v)[:50]
                               for k, v in row.items()}
                    sample_rows.append(row_dict)

            return {
                "method": "multivariate_isolation_forest",
                "columns_analyzed": valid_cols,
                "contamination_used": round(contamination, 4),
                "rows_analyzed": len(subset_df),
                "anomaly_count": int(anomaly_count),
                "anomaly_percentage": round(anomaly_count / len(subset_df) * 100, 4),
                "contributing_columns": contributing_columns[:5],
                "sample_rows": sample_rows,
                "interpretation": self._interpret_multivariate_outliers(
                    anomaly_count, len(subset_df), contributing_columns
                )
            }

        except Exception as e:
            logger.debug(f"Error in multivariate outlier detection: {e}")
            return None

    def _interpret_multivariate_outliers(self, anomaly_count: int, total_rows: int,
                                         contributing_cols: List[Dict]) -> str:
        """Generate interpretation for multivariate outliers."""
        pct = anomaly_count / total_rows * 100

        if contributing_cols:
            top_cols = ", ".join([c["column"] for c in contributing_cols[:3]])
            return (f"Found {anomaly_count:,} records ({pct:.2f}%) with unusual combinations of values. "
                    f"Key contributing columns: {top_cols}")
        else:
            return f"Found {anomaly_count:,} records ({pct:.2f}%) with unusual multi-dimensional patterns"

    def _analyze_clusters(self, df: pd.DataFrame, numeric_cols: List[str]) -> Optional[Dict[str, Any]]:
        """
        Analyze data clusters using DBSCAN to identify natural groupings and noise points.
        Uses proportional sampling for memory efficiency on large datasets.
        """
        try:
            from sklearn.cluster import DBSCAN
            from sklearn.preprocessing import StandardScaler
            from sklearn.neighbors import NearestNeighbors
        except ImportError:
            return None

        # Select columns with sufficient data
        valid_cols = [col for col in numeric_cols if df[col].notna().sum() >= MIN_ROWS_FOR_ML]

        if len(valid_cols) < 2:
            return None

        # Limit columns to top 5 by variance
        if len(valid_cols) > 5:
            variances = df[valid_cols].var()
            valid_cols = variances.nlargest(5).index.tolist()

        try:
            subset_df = df[valid_cols].dropna()
            if len(subset_df) < MIN_ROWS_FOR_ML:
                return None

            # No sampling - process full dataset with memory-efficient batching
            logger.debug(f"Clustering: analyzing {len(subset_df):,} rows with batched kneighbors")

            # Standardize
            scaler = StandardScaler()
            scaled_data = scaler.fit_transform(subset_df)

            # Estimate eps using k-nearest neighbors with elbow method
            k = min(max(5, len(subset_df) // 50), 20)  # Adaptive k

            # Use ball_tree algorithm - more memory efficient for moderate dimensions
            nn = NearestNeighbors(n_neighbors=k, algorithm='ball_tree', leaf_size=40)
            nn.fit(scaled_data)

            # Batch kneighbors to manage memory - process in chunks
            n_rows = len(scaled_data)
            batch_size = min(50000, n_rows)  # Process 50K rows at a time
            k_distances_list = []

            for start_idx in range(0, n_rows, batch_size):
                end_idx = min(start_idx + batch_size, n_rows)
                batch_data = scaled_data[start_idx:end_idx]
                batch_distances, _ = nn.kneighbors(batch_data)
                k_distances_list.append(batch_distances[:, k-1])
                del batch_distances
                gc.collect()

            # Combine and sort k-distances
            k_distances = np.concatenate(k_distances_list)
            del k_distances_list
            k_distances = np.sort(k_distances)

            # Clean up NearestNeighbors immediately
            del nn
            gc.collect()

            # Use adaptive eps - aim for ~5-15% as noise maximum
            eps_95 = np.percentile(k_distances, 95)
            eps_99 = np.percentile(k_distances, 99)
            del k_distances  # Clean up
            gc.collect()

            # Adaptive min_samples based on dataset size
            min_samples = max(5, min(len(subset_df) // 200, 50))

            # Run DBSCAN
            dbscan = DBSCAN(eps=eps_95, min_samples=min_samples)
            labels = dbscan.fit_predict(scaled_data)

            # If too much noise (>50%), try with larger eps
            noise_ratio = (labels == -1).sum() / len(labels)
            if noise_ratio > 0.5:
                dbscan = DBSCAN(eps=eps_99, min_samples=min_samples)
                labels = dbscan.fit_predict(scaled_data)

            n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            noise_count = (labels == -1).sum()
            noise_pct = noise_count / len(labels) * 100

            if n_clusters == 0 and noise_count == 0:
                return None

            # Analyze clusters
            cluster_info = []
            for cluster_id in range(n_clusters):
                cluster_mask = labels == cluster_id
                cluster_size = cluster_mask.sum()
                cluster_data = subset_df[cluster_mask]

                cluster_summary = {
                    "cluster_id": cluster_id,
                    "size": int(cluster_size),
                    "percentage": round(cluster_size / len(labels) * 100, 2),
                    "characteristics": {}
                }

                # Get cluster characteristics
                for col in valid_cols:
                    cluster_summary["characteristics"][col] = {
                        "mean": round(cluster_data[col].mean(), 2),
                        "std": round(cluster_data[col].std(), 2)
                    }

                cluster_info.append(cluster_summary)

            # Get sample noise points
            sample_noise_rows = []
            if noise_count > 0:
                noise_indices = np.where(labels == -1)[0][:5]
                for idx in noise_indices:
                    row = subset_df.iloc[idx]
                    row_dict = {str(k)[:25]: f"{v:.2f}" if isinstance(v, float) else str(v)[:50]
                               for k, v in row.items()}
                    sample_noise_rows.append(row_dict)

            return {
                "method": "dbscan",
                "columns_analyzed": valid_cols,
                "rows_analyzed": len(subset_df),
                "n_clusters": n_clusters,
                "noise_points": int(noise_count),
                "noise_percentage": round(noise_pct, 2),
                "clusters": cluster_info[:5],  # Top 5 clusters
                "sample_noise_rows": sample_noise_rows,
                "interpretation": self._interpret_clusters(n_clusters, noise_count, len(subset_df), cluster_info)
            }

        except Exception as e:
            logger.debug(f"Error in cluster analysis: {e}")
            return None

    def _interpret_clusters(self, n_clusters: int, noise_count: int, total: int,
                           cluster_info: List[Dict]) -> str:
        """Generate interpretation for clustering results."""
        noise_pct = noise_count / total * 100

        if n_clusters == 0:
            if noise_count > 0:
                return f"No clear clusters found. {noise_count:,} records ({noise_pct:.1f}%) classified as noise/outliers"
            return "Data appears uniform with no distinct clusters or outliers"

        interpretation = f"Found {n_clusters} distinct data clusters"

        if noise_count > 0:
            interpretation += f" with {noise_count:,} noise points ({noise_pct:.1f}%)"

        # Note if one cluster dominates
        if cluster_info:
            largest = max(cluster_info, key=lambda x: x["size"])
            if largest["percentage"] > 80:
                interpretation += f". Dominant cluster contains {largest['percentage']:.0f}% of data"

        return interpretation

    def _interpret_numeric_outliers(self, col_name: str, anomalies: np.ndarray,
                                    all_values: np.ndarray, contamination: Optional[float]) -> str:
        """Generate human-readable interpretation of outliers."""
        max_anomaly = anomalies.max()
        min_anomaly = anomalies.min()
        median_val = np.median(all_values)
        pct = len(anomalies) / len(all_values) * 100

        # Handle edge cases
        if median_val == 0 or abs(median_val) < 1e-10:
            return f"Found {len(anomalies):,} outliers ({pct:.2f}%) - range: {min_anomaly:,.2f} to {max_anomaly:,.2f}"

        max_ratio = abs(max_anomaly / median_val)
        min_ratio = abs(min_anomaly / median_val) if min_anomaly != 0 else 0

        if max_ratio > 100 or min_ratio > 100:
            return f"Found {len(anomalies):,} extreme outliers ({pct:.2f}%) - values up to {max(max_ratio, min_ratio):.0f}x the median"
        elif max_ratio > 10 or min_ratio > 10:
            return f"Found {len(anomalies):,} significant outliers ({pct:.2f}%) - values deviate significantly from median"
        else:
            return f"Found {len(anomalies):,} potential outliers ({pct:.2f}%) outside the typical distribution"

    def _analyze_format_patterns(self, df: pd.DataFrame, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Analyze string column for format consistency.
        """
        series = df[col_name]
        values = series.dropna().astype(str)
        if len(values) < 50:
            return None

        # Extract format patterns
        patterns = values.apply(self._extract_format_pattern)
        pattern_counts = Counter(patterns)

        if not pattern_counts:
            return None

        dominant_pattern, dominant_count = pattern_counts.most_common(1)[0]
        dominant_pct = dominant_count / len(values) * 100

        # Only report if there's a clear dominant pattern with anomalies
        if dominant_pct < 90:
            return None

        anomaly_patterns = [(p, c) for p, c in pattern_counts.items() if p != dominant_pattern]
        if not anomaly_patterns:
            return None

        anomaly_count = sum(c for _, c in anomaly_patterns)

        # Get sample values
        anomaly_mask = patterns != dominant_pattern
        sample_anomalies = values[anomaly_mask].head(10).tolist()
        sample_dominant = values[~anomaly_mask].head(5).tolist()

        # Get sample rows
        non_null_df = df[df[col_name].notna()].reset_index(drop=True)
        anomaly_indices = anomaly_mask[anomaly_mask].index[:5]
        sample_rows = []
        for idx in anomaly_indices:
            if idx < len(non_null_df):
                row = non_null_df.iloc[idx]
                row_dict = {str(k)[:25]: str(v)[:50] for k, v in row.items()}
                sample_rows.append(row_dict)

        pattern_desc = self._describe_pattern(dominant_pattern)

        return {
            "dominant_pattern": dominant_pattern,
            "dominant_pattern_description": pattern_desc,
            "dominant_percentage": round(dominant_pct, 2),
            "sample_dominant_values": sample_dominant,
            "anomaly_count": anomaly_count,
            "anomaly_percentage": round(anomaly_count / len(values) * 100, 4),
            "anomaly_patterns": dict(anomaly_patterns[:5]),
            "sample_anomalies": sample_anomalies,
            "sample_rows": sample_rows,
            "interpretation": f"Format inconsistency: {anomaly_count:,} values ({anomaly_count/len(values)*100:.3f}%) don't match the expected format"
        }

    def _describe_pattern(self, pattern: str) -> str:
        """Convert pattern notation to human-readable description."""
        if not pattern or pattern == "NULL":
            return "Empty/null values"

        # Simple regex-based parsing
        parts = []
        i = 0

        while i < len(pattern):
            char = pattern[i]

            # Read any following digits as count
            count_str = ""
            j = i + 1
            while j < len(pattern) and pattern[j].isdigit():
                count_str += pattern[j]
                j += 1

            count = int(count_str) if count_str else 1

            if char == 'A':
                parts.append(f"{count} letter{'s' if count > 1 else ''}")
            elif char == '9':
                parts.append(f"{count} digit{'s' if count > 1 else ''}")
            elif char == '_':
                parts.append(f"{count} space{'s' if count > 1 else ''}")
            elif not char.isdigit():
                parts.append(f"'{char}'" if count == 1 else f"'{char}'×{count}")

            i = j if count_str else i + 1

        return " + ".join(parts) if parts else pattern

    def _extract_format_pattern(self, value: str) -> str:
        """Convert value to format pattern (A=alpha, 9=digit, _=space)."""
        if not value or value.lower() == 'nan':
            return "NULL"

        pattern = []
        for c in str(value):
            if c.isalpha():
                pattern.append("A")
            elif c.isdigit():
                pattern.append("9")
            elif c.isspace():
                pattern.append("_")
            else:
                pattern.append(c)

        # Compress repeating characters
        if not pattern:
            return "NULL"

        compressed = []
        current_char = pattern[0]
        count = 1

        for c in pattern[1:]:
            if c == current_char:
                count += 1
            else:
                compressed.append(current_char + (str(count) if count > 1 else ""))
                current_char = c
                count = 1

        compressed.append(current_char + (str(count) if count > 1 else ""))

        return "".join(compressed)

    # Known valid values for common domains - rare but valid values should not be flagged
    KNOWN_CURRENCIES = {
        'usd', 'us dollar', 'dollar', 'eur', 'euro', 'gbp', 'uk pound', 'pound', 'sterling',
        'jpy', 'yen', 'cny', 'yuan', 'renminbi', 'cad', 'canadian dollar', 'aud', 'australian dollar',
        'chf', 'swiss franc', 'franc', 'inr', 'rupee', 'rub', 'ruble', 'brl', 'brazil real', 'real',
        'mxn', 'mexican peso', 'peso', 'krw', 'won', 'sgd', 'singapore dollar', 'hkd', 'hong kong dollar',
        'nok', 'norwegian krone', 'krone', 'sek', 'swedish krona', 'krona', 'dkk', 'danish krone',
        'pln', 'zloty', 'thb', 'baht', 'idr', 'rupiah', 'myr', 'ringgit', 'php', 'philippine peso',
        'czk', 'czech koruna', 'ils', 'shekel', 'zar', 'rand', 'try', 'turkish lira', 'lira',
        'aed', 'dirham', 'sar', 'riyal', 'bitcoin', 'btc', 'eth', 'ethereum', 'crypto'
    }

    KNOWN_COUNTRIES = {
        'us', 'usa', 'united states', 'uk', 'united kingdom', 'gb', 'great britain', 'canada', 'ca',
        'australia', 'au', 'germany', 'de', 'france', 'fr', 'japan', 'jp', 'china', 'cn',
        'india', 'in', 'brazil', 'br', 'mexico', 'mx', 'russia', 'ru', 'italy', 'it',
        'spain', 'es', 'netherlands', 'nl', 'switzerland', 'ch', 'sweden', 'se', 'norway', 'no'
        # Add more as needed - this is just a sample
    }

    def _is_known_domain_column(self, col_name: str) -> Optional[set]:
        """Check if column represents a known domain with valid reference values."""
        col_lower = col_name.lower()

        # Check for currency columns
        if any(word in col_lower for word in ['currency', 'curr', 'ccy']):
            return self.KNOWN_CURRENCIES

        # Check for country columns
        if any(word in col_lower for word in ['country', 'nation', 'region']):
            return self.KNOWN_COUNTRIES

        return None

    def _get_semantic_ml_behavior(self, col_name: str) -> Tuple[str, Optional[List[str]], str]:
        """
        Get ML rare category detection behavior based on column's FIBO semantic tag.

        Returns:
            Tuple of (behavior, expected_values, reason)
            behavior: 'skip', 'strict_threshold', 'reference_validate', or 'default'
            expected_values: List of valid values for reference_validate mode
            reason: Explanation of why this behavior was chosen
        """
        # Get semantic info for this column
        sem_info = self._column_semantic_info.get(col_name, {})
        primary_tag = sem_info.get('primary_tag', '')

        if not primary_tag or primary_tag == 'unknown':
            return 'default', None, "No semantic tag detected"

        # Look up tag in FIBO taxonomy
        tag_def = self._fibo_taxonomy.get(primary_tag, {})

        if tag_def:
            behavior = tag_def.get('ml_rare_behavior', 'default')
            expected_values = tag_def.get('expected_values', [])
            reason = tag_def.get('ml_rare_reason', f"FIBO tag: {primary_tag}")
            return behavior, expected_values if expected_values else None, reason

        # Fallback: check parent category (e.g., 'identifier' from 'identifier.uuid')
        if '.' in primary_tag:
            parent_category = primary_tag.split('.')[0]
            # Check if any tag in this category has skip behavior
            for tag_name, tag_def in self._fibo_taxonomy.items():
                if tag_name.startswith(parent_category + '.'):
                    behavior = tag_def.get('ml_rare_behavior')
                    if behavior == 'skip':
                        return 'skip', None, f"Parent category {parent_category} skips rare detection"

        return 'default', None, f"FIBO tag {primary_tag} uses default behavior"

    def _should_skip_numeric_outlier_detection(self, col_name: str) -> Tuple[bool, str]:
        """
        Check if numeric outlier detection should be skipped for this column.

        Some columns are numeric but semantically categorical (e.g., bank IDs stored as integers).
        Running Isolation Forest on these is meaningless - Bank ID 1099 isn't an outlier
        just because most transactions use banks 1-100.

        Returns:
            (should_skip, reason)
        """
        # Get semantic info for this column
        sem_info = self._column_semantic_info.get(col_name, {})
        primary_tag = sem_info.get('primary_tag', '')

        if not primary_tag or primary_tag == 'unknown':
            return False, ""

        # Tags that indicate numeric values are actually identifiers/categories
        skip_tags = {
            'party.counterparty': "Counterparty IDs - numeric but categorical",
            'party.customer_id': "Customer IDs - numeric but categorical",
            'banking.account': "Account numbers - identifiers not measures",
            'banking.transaction': "Transaction IDs - identifiers not measures",
            'identifier.uuid': "UUIDs - identifiers not measures",
            'identifier.code': "Codes - identifiers not measures",
            'category': "Categorical data stored as numbers",
            'flag.binary': "Binary flags - not continuous data",
        }

        for tag_prefix, reason in skip_tags.items():
            if primary_tag == tag_prefix or primary_tag.startswith(tag_prefix + '.'):
                return True, reason

        return False, ""

    def _detect_rare_categories(self, df: pd.DataFrame, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Detect suspiciously rare categorical values with adaptive threshold.

        Uses FIBO-based semantic intelligence for smart behavior:
        - skip: Don't flag rare values for this column (identifiers, etc.)
        - strict_threshold: Use very strict threshold (counterparties)
        - reference_validate: Only flag values NOT in valid reference list
        - default: Normal rare category detection
        """
        series = df[col_name]
        freq = series.value_counts()
        total = len(series.dropna())

        if total < MIN_ROWS_FOR_ML:
            return None

        # Get semantic behavior from FIBO taxonomy
        behavior, expected_values, semantic_reason = self._get_semantic_ml_behavior(col_name)

        # Handle skip behavior - don't flag rare values for identifiers, etc.
        if behavior == 'skip':
            logger.debug(f"Skipping rare detection for {col_name}: {semantic_reason}")
            return None

        # Check if this is a known domain column (fallback for untagged columns)
        known_values = self._is_known_domain_column(col_name)
        is_known_domain = known_values is not None

        # Adaptive threshold based on dataset size and cardinality
        unique_count = len(freq)

        # Base threshold depends on behavior
        if behavior == 'strict_threshold':
            # Use much stricter threshold for counterparties, etc.
            if total > 100000:
                base_threshold = 0.00005  # 0.005% - 10x stricter
            elif total > 10000:
                base_threshold = 0.0001  # 0.01%
            else:
                base_threshold = 0.0005  # 0.05%
        else:
            # Default thresholds
            if total > 100000:
                base_threshold = 0.0005  # 0.05%
            elif total > 10000:
                base_threshold = 0.001  # 0.1%
            else:
                base_threshold = 0.005  # 0.5%

        # Adjust for cardinality
        if unique_count > 50:
            threshold = base_threshold * 2
        else:
            threshold = base_threshold

        rare_values = []
        skipped_known = 0
        skipped_reference = 0

        # Build reference set for reference_validate mode
        reference_set = None
        if behavior == 'reference_validate' and expected_values:
            reference_set = set(v.upper() for v in expected_values)

        for val, count in freq.items():
            pct = count / total
            if pct < threshold:
                val_str = str(val).strip()
                val_upper = val_str.upper()

                # Reference validate mode: only flag values NOT in valid reference list
                if behavior == 'reference_validate' and reference_set:
                    if val_upper in reference_set:
                        skipped_reference += 1
                        continue  # Valid reference value - don't flag

                # For known domains (fallback), skip values that match known valid entries
                if is_known_domain:
                    val_lower = val_str.lower()
                    if val_lower in known_values:
                        skipped_known += 1
                        continue  # Skip - this is a valid but rare value

                rare_values.append({
                    "value": val_str[:50],
                    "count": int(count),
                    "percentage": round(pct * 100, 4)
                })

        if not rare_values:
            return None

        # Build interpretation based on behavior
        if behavior == 'reference_validate' and skipped_reference > 0:
            interpretation = (
                f"Found {len(rare_values)} potentially invalid values "
                f"(excluded {skipped_reference} valid reference codes). "
                f"These values may be typos or non-standard codes."
            )
        elif behavior == 'strict_threshold':
            interpretation = (
                f"Found {len(rare_values)} extremely rare categories (strict threshold) "
                f"appearing in <{threshold*100:.3f}% of records. "
                f"Only flagging statistically significant outliers."
            )
        elif is_known_domain and skipped_known > 0:
            interpretation = (
                f"Found {len(rare_values)} potentially suspicious categories "
                f"(excluded {skipped_known} valid but rare values like regional currencies/codes)"
            )
        else:
            interpretation = (
                f"Found {len(rare_values)} rare categories appearing in <{threshold*100:.2f}% "
                f"of records - may indicate typos or data errors"
            )

        return {
            "threshold_percentage": round(threshold * 100, 3),
            "threshold_adaptive": True,
            "semantic_behavior": behavior,
            "semantic_reason": semantic_reason if behavior != 'default' else None,
            "known_domain_detected": is_known_domain,
            "valid_values_skipped": skipped_known if is_known_domain else 0,
            "reference_values_skipped": skipped_reference if behavior == 'reference_validate' else 0,
            "rare_values": rare_values[:10],
            "total_rare_count": sum(v["count"] for v in rare_values),
            "total_rare_categories": len(rare_values),
            "interpretation": interpretation
        }

    def _check_cross_column_consistency(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Check for cross-column consistency issues.
        """
        issues = []

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        amount_cols = [c for c in numeric_cols
                      if any(word in c.lower() for word in
                            ['amount', 'value', 'price', 'paid', 'received', 'total', 'balance'])]

        # Check ratios between related columns
        for i, col1 in enumerate(amount_cols):
            for col2 in amount_cols[i+1:]:
                if self._columns_might_be_related(col1, col2):
                    ratio_issue = self._check_amount_ratio(df, col1, col2)
                    if ratio_issue:
                        issues.append(ratio_issue)

        return issues

    def _columns_might_be_related(self, col1: str, col2: str) -> bool:
        """Check if two columns might be related."""
        related_pairs = [
            ('paid', 'received'),
            ('debit', 'credit'),
            ('in', 'out'),
            ('from', 'to'),
            ('sent', 'received'),
            ('buy', 'sell'),
            ('deposit', 'withdrawal')
        ]
        col1_lower = col1.lower()
        col2_lower = col2.lower()

        for word1, word2 in related_pairs:
            if (word1 in col1_lower and word2 in col2_lower) or \
               (word2 in col1_lower and word1 in col2_lower):
                return True
        return False

    def _check_amount_ratio(self, df: pd.DataFrame, col1: str, col2: str) -> Optional[Dict[str, Any]]:
        """Check for extreme ratios between two amount columns."""
        try:
            valid_mask = (df[col1] > 0) & (df[col2] > 0)
            if valid_mask.sum() < MIN_ROWS_FOR_ML:
                return None

            # Compute dynamic near-zero threshold from data distribution
            # Use 1st percentile of positive values as baseline for "near-zero"
            col1_positive = df.loc[valid_mask, col1]
            near_zero_threshold = col1_positive.quantile(0.01)
            # Fallback: if 1st percentile is 0, use a small fraction of median
            if near_zero_threshold <= 0:
                near_zero_threshold = col1_positive.median() * 0.001

            ratios = df.loc[valid_mask, col2] / df.loc[valid_mask, col1]

            # Flag near-zero denominators (will produce extremely large ratios)
            near_zero_mask = col1_positive < near_zero_threshold

            # Adaptive thresholds based on ratio distribution
            median_ratio = ratios.median()
            q1, q3 = ratios.quantile([0.25, 0.75])
            iqr = q3 - q1

            if iqr > 0:
                high_threshold = q3 + 3 * iqr
                low_threshold = max(q1 - 3 * iqr, 0.001)
            else:
                high_threshold = median_ratio * 10
                low_threshold = median_ratio * 0.1

            extreme_high_mask = ratios > high_threshold
            extreme_low_mask = ratios < low_threshold
            extreme_high = extreme_high_mask.sum()
            extreme_low = extreme_low_mask.sum()

            if extreme_high + extreme_low < 10:
                return None

            # Get sample rows - sorted by ratio extremeness (furthest from median first)
            extreme_mask = extreme_high_mask | extreme_low_mask
            valid_df = df[valid_mask].reset_index(drop=True)

            # Get indices and their ratio values for sorting
            extreme_indices = extreme_mask[extreme_mask].index.tolist()
            if not extreme_indices:
                return None

            # Calculate deviation from median for sorting (log scale for ratios)
            extreme_ratios = ratios.iloc[extreme_indices]
            log_median = np.log10(median_ratio) if median_ratio > 0 else 0
            log_ratios = np.log10(extreme_ratios.replace(0, 1e-10))
            deviations = np.abs(log_ratios - log_median)

            # Sort by deviation (most extreme first) and take top 5
            sorted_indices = np.argsort(deviations.values)[::-1][:5]
            top_extreme_indices = [extreme_indices[i] for i in sorted_indices]

            sample_rows = []
            for idx in top_extreme_indices:
                if idx < len(valid_df):
                    row = valid_df.iloc[idx]
                    ratio_val = float(ratios.iloc[idx])
                    denom_val = float(col1_positive.iloc[idx]) if idx < len(col1_positive) else 0
                    is_near_zero = denom_val < near_zero_threshold
                    row_dict = {
                        'row_index': int(idx),
                        'ratio': ratio_val,
                        'near_zero_baseline': is_near_zero,
                        col1: row[col1] if col1 in row else 'N/A',
                        col2: row[col2] if col2 in row else 'N/A'
                    }
                    sample_rows.append(row_dict)

            return {
                "columns": [col1, col2],
                "issue_type": "extreme_ratio",
                "extreme_high_count": int(extreme_high),
                "extreme_low_count": int(extreme_low),
                "total_issues": int(extreme_high + extreme_low),
                "median_ratio": float(median_ratio),
                "near_zero_threshold": float(near_zero_threshold),
                "expected_range": {
                    "low": float(low_threshold),
                    "high": float(high_threshold)
                },
                "sample_rows": sample_rows,
                "interpretation": f"Found {extreme_high + extreme_low:,} records with extreme ratios between {col1} and {col2}"
            }
        except Exception:
            return None

    def _detect_correlation_anomalies(self, df: pd.DataFrame, numeric_cols: List[str]) -> Optional[Dict[str, Any]]:
        """
        Detect records that break expected correlation patterns.
        """
        if len(numeric_cols) < 2:
            return None

        try:
            # Calculate correlation matrix
            subset = df[numeric_cols].dropna()
            if len(subset) < MIN_ROWS_FOR_ML:
                return None

            corr_matrix = subset.corr()

            # Find highly correlated pairs
            high_corr_pairs = []
            for i, col1 in enumerate(numeric_cols):
                for col2 in numeric_cols[i+1:]:
                    corr = corr_matrix.loc[col1, col2]
                    if abs(corr) > 0.7:  # Strong correlation threshold
                        high_corr_pairs.append({
                            "columns": [col1, col2],
                            "correlation": round(corr, 3)
                        })

            if not high_corr_pairs:
                return None

            # For each highly correlated pair, find records that deviate
            anomalies = []
            for pair in high_corr_pairs[:3]:  # Limit to top 3 pairs
                col1, col2 = pair["columns"]
                corr = pair["correlation"]

                # Use residuals from linear regression
                x = subset[col1].values
                y = subset[col2].values

                # Simple linear fit
                x_mean = np.mean(x)
                y_mean = np.mean(y)
                slope = np.sum((x - x_mean) * (y - y_mean)) / np.sum((x - x_mean) ** 2)
                intercept = y_mean - slope * x_mean

                predicted = slope * x + intercept
                residuals = np.abs(y - predicted)

                # Flag extreme residuals (configurable percentile)
                residual_percentile = 99  # Top 1% outliers
                residual_threshold = np.percentile(residuals, residual_percentile)
                anomaly_mask = residuals > residual_threshold
                anomaly_count = anomaly_mask.sum()
                outlier_percent = 100 - residual_percentile  # What percent are flagged

                if anomaly_count > 0:
                    # Get sample rows for anomalies
                    anomaly_indices = np.where(anomaly_mask)[0][:5]
                    sample_rows = []
                    for idx in anomaly_indices:
                        if idx < len(subset):
                            row = subset.iloc[idx]
                            row_dict = {str(k)[:25]: f"{v:.2f}" if isinstance(v, (int, float)) else str(v)[:50]
                                       for k, v in row.items()}
                            sample_rows.append(row_dict)

                    anomalies.append({
                        "columns": [col1, col2],
                        "expected_correlation": round(corr, 3),
                        "anomaly_count": int(anomaly_count),
                        "outlier_percent": outlier_percent,  # Dynamic: what % are flagged as outliers
                        "sample_rows": sample_rows,
                        "interpretation": f"{anomaly_count} records deviate from expected {col1}/{col2} relationship"
                    })

            if not anomalies:
                return None

            return {
                "high_correlation_pairs": high_corr_pairs,
                "correlation_breaks": anomalies,
                "interpretation": f"Found {sum(a['anomaly_count'] for a in anomalies)} records that break expected correlation patterns"
            }

        except Exception as e:
            logger.debug(f"Error in correlation anomaly detection: {e}")
            return None

    def _looks_like_datetime(self, series: pd.Series) -> bool:
        """Check if a string column looks like datetime values."""
        sample = series.dropna().head(100)
        if len(sample) < 10:
            return False

        datetime_patterns = [
            r'\d{4}[-/]\d{2}[-/]\d{2}',
            r'\d{2}[-/]\d{2}[-/]\d{4}',
            r'\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}',
        ]

        matches = 0
        for val in sample:
            val_str = str(val)
            for pattern in datetime_patterns:
                if re.search(pattern, val_str):
                    matches += 1
                    break

        return matches / len(sample) > 0.8

    def _analyze_temporal_patterns(self, df: pd.DataFrame, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Analyze temporal patterns with enhanced detection.
        """
        try:
            series = df[col_name]

            # Parse datetime
            if series.dtype == 'object':
                for fmt in ['%Y/%m/%d %H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y']:
                    try:
                        dt_series = pd.to_datetime(series, format=fmt)
                        break
                    except:
                        continue
                else:
                    dt_series = pd.to_datetime(series, errors='coerce')
            else:
                dt_series = series

            valid_dates = dt_series.dropna()
            if len(valid_dates) < MIN_ROWS_FOR_ML:
                return None

            # Basic temporal analysis
            hours = valid_dates.dt.hour
            minutes = valid_dates.dt.minute
            seconds = valid_dates.dt.second
            day_of_week = valid_dates.dt.dayofweek

            hour_dist = hours.value_counts().sort_index()
            dow_dist = day_of_week.value_counts().sort_index()

            findings = {
                "date_range": {
                    "min": str(valid_dates.min()),
                    "max": str(valid_dates.max()),
                    "span_days": (valid_dates.max() - valid_dates.min()).days
                },
                "hour_distribution": {str(h): int(c) for h, c in hour_dist.head(5).items()},
                "day_of_week_distribution": {
                    ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][int(d)]: int(c)
                    for d, c in dow_dist.items()
                }
            }

            # Check for true midnight concentration
            is_true_midnight = (hours == 0) & (minutes == 0) & (seconds == 0)
            midnight_pct = is_true_midnight.sum() / len(hours) * 100

            # Check for weekend gaps
            weekend_count = dow_dist.get(5, 0) + dow_dist.get(6, 0)
            weekday_avg = sum(dow_dist.get(i, 0) for i in range(5)) / 5
            has_weekend_gap = weekend_count < weekday_avg * 0.2 if weekday_avg > 0 else False

            # Check for business hours concentration
            business_hours = hours.between(9, 17).sum()
            business_hours_pct = business_hours / len(hours) * 100

            # Get sample rows
            sample_rows = []
            sample_df = df.head(5)
            for _, row in sample_df.iterrows():
                row_dict = {str(k)[:25]: str(v)[:50] for k, v in row.items()}
                sample_rows.append(row_dict)
            findings["sample_rows"] = sample_rows

            # Generate warnings
            warnings = []

            if midnight_pct > 90:
                warnings.append("suspicious_midnight_concentration")
                findings["midnight_percentage"] = round(midnight_pct, 2)

            if has_weekend_gap:
                warnings.append("weekend_gap")
                findings["weekend_gap"] = True

            if business_hours_pct > 90:
                warnings.append("business_hours_only")
                findings["business_hours_percentage"] = round(business_hours_pct, 2)

            # Detect gaps in time series
            sorted_dates = valid_dates.sort_values()
            diffs = sorted_dates.diff().dropna()
            if len(diffs) > 0:
                median_diff = diffs.median()
                large_gaps = diffs[diffs > median_diff * 10]
                if len(large_gaps) > 0:
                    warnings.append("large_temporal_gaps")
                    findings["large_gaps_count"] = len(large_gaps)
                    findings["largest_gap_days"] = large_gaps.max().days if hasattr(large_gaps.max(), 'days') else 0

            if warnings:
                findings["warnings"] = warnings
                findings["warning"] = warnings[0]  # Backwards compatibility
                findings["interpretation"] = self._interpret_temporal_findings(findings, warnings)
            else:
                findings["interpretation"] = "No suspicious temporal patterns detected"

            return findings

        except Exception as e:
            logger.debug(f"Error analyzing temporal patterns for {col_name}: {e}")
            return None

    def _interpret_temporal_findings(self, findings: Dict, warnings: List[str]) -> str:
        """Generate interpretation for temporal findings."""
        interpretations = []

        if "suspicious_midnight_concentration" in warnings:
            pct = findings.get("midnight_percentage", 0)
            interpretations.append(f"{pct:.1f}% of timestamps are at midnight (may indicate missing time component)")

        if "weekend_gap" in warnings:
            interpretations.append("Significant gap in weekend data (may be expected for business data)")

        if "business_hours_only" in warnings:
            pct = findings.get("business_hours_percentage", 0)
            interpretations.append(f"{pct:.1f}% of activity during business hours (9-5)")

        if "large_temporal_gaps" in warnings:
            count = findings.get("large_gaps_count", 0)
            interpretations.append(f"Found {count} large gaps in time series")

        return "; ".join(interpretations) if interpretations else "Temporal patterns detected"

    def _get_benford_ineligibility_reason(self, col_name: str, values: Optional[np.ndarray] = None) -> str:
        """
        Get the reason why Benford's Law should NOT be applied to this column.

        Used for fallback display in reports.

        Args:
            col_name: Column name
            values: Optional numpy array of column values

        Returns:
            Generic reason string explaining why Benford is not applicable
        """
        col_lower = col_name.lower()

        # Check for identifier patterns
        exclude_keywords = ['id', 'code', 'number', 'count', 'qty', 'quantity', 'bank', 'account',
                           'index', 'row', 'sequence', 'version', 'rank', 'position']
        if any(kw in col_lower for kw in exclude_keywords):
            return "Column name suggests identifier/code (not suitable for Benford)"

        # Bounded/demographic data (ages, ratings, percentages, etc.)
        # These don't follow Benford's Law because they don't grow multiplicatively
        # Note: uses generic patterns only, not domain-specific field names
        bounded_keywords = ['age', 'rating', 'score', 'grade', 'percent', 'pct', 'rate',
                           'level', 'class', 'tier', 'year', 'month', 'day', 'hour',
                           'duration', 'tenure', 'rank', 'priority']
        if any(kw in col_lower for kw in bounded_keywords):
            return "Column contains bounded/demographic data (not suitable for Benford)"

        # Binary/flag indicators
        if col_lower.startswith(('is_', 'has_', 'was_', 'can_')) or col_lower.endswith(('_flag', '_indicator')):
            return "Column appears to be a binary flag"

        # Check numeric properties
        if values is not None and len(values) > 0:
            positive_values = values[values > 0]

            if len(positive_values) < 100:
                return f"Insufficient positive values ({len(positive_values)} < 100)"

            unique_count = len(np.unique(positive_values))
            if unique_count < 20:
                return f"Insufficient unique values ({unique_count} < 20) - data may be categorical"

            min_val = np.min(positive_values)
            max_val = np.max(positive_values)
            if min_val > 0:
                magnitude_span = np.log10(max_val / min_val)
                if magnitude_span < 1.5:
                    return f"Value range too narrow ({magnitude_span:.1f} < 1.5 orders of magnitude)"

            # Check for structured pricing/tariff data
            if self._looks_structured_pricing(col_name, positive_values):
                return (
                    "Column appears to contain structured prices or tariffs: a small set of repeated "
                    "price points and/or highly concentrated first digits. Such pricing tables are not "
                    "well suited for Benford's Law."
                )

        return "Column does not meet Benford's Law criteria"

    def _looks_structured_pricing(self, col_name: str, positive_values: np.ndarray) -> bool:
        """
        Detect structured pricing/tariff data that should NOT use Benford's Law.

        Structured pricing (e.g., ticket fares, toll fees) often has:
        - A small set of repeated price points (tariff tables)
        - Highly concentrated first digits due to pricing tiers

        These violate Benford assumptions but are NOT indicators of data quality issues.

        Args:
            col_name: Column name
            positive_values: Array of positive numeric values

        Returns:
            True if column appears to contain structured pricing data
        """
        # Pricing-related keywords
        pricing_keywords = ['fare', 'price', 'fee', 'charge', 'tax', 'tariff', 'toll', 'rate']
        lower = col_name.lower()
        name_is_pricing = any(k in lower for k in pricing_keywords)

        if not name_is_pricing:
            return False  # Only apply this guard to pricing-named columns

        n = len(positive_values)
        if n < 100:
            return False  # Not enough data to assess

        # Check for low unique ratio (indicates tariff-like tables)
        unique_count = len(np.unique(positive_values))
        unique_ratio = unique_count / n
        low_unique = unique_ratio < 0.10  # Less than 10% unique values

        # Check for dominant first digit (≥75% concentration)
        dominant_first_digit = False
        first_digits = self._extract_first_digits_array(positive_values)
        if len(first_digits) > 0:
            counts = np.bincount(first_digits, minlength=10)
            total = counts[1:].sum()  # Only digits 1-9
            if total > 0:
                max_share = counts[1:].max() / float(total)
                dominant_first_digit = max_share >= 0.75

        # Structured pricing if: pricing name AND (low unique OR dominant first digit)
        return name_is_pricing and (low_unique or dominant_first_digit)

    def _extract_first_digits_array(self, values: np.ndarray) -> np.ndarray:
        """
        Extract first significant digits from an array of positive values.

        Args:
            values: Array of positive numeric values

        Returns:
            Array of first digits (1-9), filtering out zeros
        """
        result = []
        for x in values:
            try:
                if x <= 0:
                    continue
                s = str(abs(float(x))).lstrip('0').lstrip('.')
                if not s:
                    continue
                match = re.search(r'([1-9])', s)
                if match:
                    result.append(int(match.group(1)))
            except (ValueError, TypeError):
                continue
        return np.array(result, dtype=np.int32)

    def _should_apply_benford(self, col_name: str, values: Optional[np.ndarray] = None) -> bool:
        """
        Check if Benford's Law should be applied to this column.

        Benford's Law works well for data that:
        1. Has positive values (negative values don't have meaningful first digits)
        2. Spans multiple orders of magnitude (~2+ orders)
        3. Has sufficient observations (at least 100)
        4. Has enough unique values (not low-cardinality categorical data)

        FIBO tags and amount keywords are used as positive indicators,
        but numeric properties are the primary criteria.

        Args:
            col_name: Column name
            values: Optional numpy array of column values (for numeric property checks)

        Returns:
            True if Benford's Law analysis is appropriate for this column
        """
        # Get semantic info for this column
        sem_info = self._column_semantic_info.get(col_name, {})
        primary_tag = sem_info.get('primary_tag', '')

        # FIBO tags that should use Benford's Law (financial amounts)
        benford_tags = {
            'money.amount',
            'money.price',
            'money.interest_rate',
            'money.balance',
            'money.fee',
        }

        col_lower = col_name.lower()

        # Exclude keywords that strongly suggest identifiers (should NOT use Benford)
        exclude_keywords = ['id', 'code', 'number', 'count', 'qty', 'quantity', 'bank', 'account',
                           'index', 'row', 'sequence', 'version', 'rank', 'position']

        if any(kw in col_lower for kw in exclude_keywords):
            return False

        # Exclude bounded/demographic data (ages, ratings, percentages, etc.)
        # These don't follow Benford's Law because they don't grow multiplicatively
        # Note: uses generic patterns only, not domain-specific field names
        bounded_keywords = ['age', 'rating', 'score', 'grade', 'percent', 'pct', 'rate',
                           'level', 'class', 'tier', 'year', 'month', 'day', 'hour',
                           'duration', 'tenure', 'rank', 'priority']

        if any(kw in col_lower for kw in bounded_keywords):
            return False

        # Check if tagged as money-related (strong positive indicator)
        if primary_tag in benford_tags:
            return True

        # Amount-like keywords (positive indicators)
        amount_keywords = ['amount', 'price', 'value', 'total', 'sum', 'paid', 'received',
                          'balance', 'fee', 'cost', 'revenue', 'sales', 'income', 'fare',
                          'payment', 'salary', 'wage', 'tax', 'charge', 'bill', 'expense']

        has_amount_keyword = any(kw in col_lower for kw in amount_keywords)

        # If values provided, check numeric properties
        if values is not None and len(values) > 0:
            # Filter to positive non-zero values only (Benford requires positive numbers)
            positive_values = values[values > 0]

            # Criterion 1: Need sufficient observations (at least 100)
            if len(positive_values) < 100:
                return False

            # Criterion 2: Need sufficient unique values (not low-cardinality)
            unique_count = len(np.unique(positive_values))
            if unique_count < 20:
                return False

            # Criterion 3: Data should span at least ~2 orders of magnitude
            # This makes Benford's Law meaningful
            min_val = np.min(positive_values)
            max_val = np.max(positive_values)
            if min_val > 0:
                magnitude_span = np.log10(max_val / min_val)
                if magnitude_span < 1.5:  # At least ~1.5 orders of magnitude
                    return False

            # Criterion 4: Check for structured pricing/tariff data
            # Pricing tables with limited unique values or dominant first digits
            # are not suitable for Benford analysis (but not data quality issues)
            if self._looks_structured_pricing(col_name, positive_values):
                return False

            # If we pass all criteria, apply Benford analysis
            # FIBO tags or amount keywords boost confidence, but numeric properties are primary
            return True

        # Fallback if no values provided: rely on keywords/tags
        return has_amount_keyword

    def _get_benford_applicability(self, col_name: str, values: Optional[np.ndarray] = None) -> Dict[str, Any]:
        """
        Assess whether Benford's Law is applicable to this column.

        Benford's Law works well for:
        - Independent observations (not cumulative totals)
        - Data spanning multiple orders of magnitude
        - Financial amounts, population data, invoice amounts

        Benford's Law does NOT work well for:
        - Cumulative/running totals (e.g., total_cases, running_sum)
        - Sequential counts that grow from zero
        - Data with hard minimum/maximum bounds
        - Assigned numbers (IDs, zip codes)
        - Structured pricing/tariff tables

        Args:
            col_name: Column name
            values: Optional numpy array of column values

        Returns:
            Dict with applicability assessment and explanation
        """
        col_lower = col_name.lower()

        # Check for structured pricing/tariff data first (if values provided)
        if values is not None and len(values) > 0:
            positive_values = values[values > 0]
            if len(positive_values) >= 100 and self._looks_structured_pricing(col_name, positive_values):
                return {
                    "applicable": False,
                    "reason": "structured_pricing",
                    "explanation": (
                        "This column appears to contain structured prices/tariffs with limited unique "
                        "values or dominant leading digits, which violates Benford assumptions."
                    ),
                    "recommendation": "Use distribution or frequency analysis instead of Benford."
                }

        # Patterns indicating cumulative/running totals - Benford NOT applicable
        cumulative_patterns = [
            'total_', '_total', 'cumulative', 'running_', '_running',
            'sum_', '_sum', 'aggregate', 'accumulated', 'ytd', 'mtd',
            'to_date', 'todate', 'all_time', 'lifetime'
        ]

        # Patterns indicating per-unit/transactional data - Benford applicable
        transactional_patterns = [
            'new_', '_new', 'daily_', '_daily', 'weekly_', '_weekly',
            'per_', '_per', 'each', 'unit_', '_unit', 'transaction',
            'smoothed', '_avg', 'average'
        ]

        # Check for cumulative patterns
        is_cumulative = any(pattern in col_lower for pattern in cumulative_patterns)

        # Check for transactional patterns (overrides cumulative if present)
        is_transactional = any(pattern in col_lower for pattern in transactional_patterns)

        # Specific column name checks
        if col_lower.startswith('total') and not is_transactional:
            is_cumulative = True

        if is_cumulative and not is_transactional:
            return {
                "applicable": False,
                "reason": "cumulative_data",
                "explanation": (
                    "Benford's Law is NOT applicable to cumulative/running totals. "
                    "Cumulative data starts from zero and grows monotonically, which naturally "
                    "skews the first-digit distribution toward lower digits. This is expected "
                    "behavior, not an indicator of data quality issues."
                ),
                "recommendation": "For authenticity analysis, examine the underlying incremental values (e.g., new_cases instead of total_cases)."
            }

        # Check semantic info for additional context
        sem_info = self._column_semantic_info.get(col_name, {})
        primary_tag = sem_info.get('primary_tag', '')

        # Good candidates for Benford's Law
        benford_good_tags = {'money.amount', 'money.price', 'money.fee', 'money.balance'}
        if primary_tag in benford_good_tags:
            return {
                "applicable": True,
                "reason": "financial_amount",
                "explanation": "Financial amounts typically follow Benford's Law when representing independent transactions.",
                "recommendation": None
            }

        # Default: applicable but with standard caveats
        return {
            "applicable": True,
            "reason": "general_numeric",
            "explanation": "Benford's Law analysis performed. Results are most meaningful for data spanning multiple orders of magnitude.",
            "recommendation": None
        }

    def _detect_benford_anomalies(self, df: pd.DataFrame, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Detect anomalies using Benford's Law.

        Benford's Law states that in naturally occurring datasets, the first digit
        follows a specific distribution: 1 appears ~30.1%, 2 appears ~17.6%, etc.

        Violations often indicate:
        - Fabricated/synthetic data
        - Fraud or manipulation
        - Poor test data generation
        - Data transformation errors

        Args:
            df: DataFrame to analyze
            col_name: Column name to check

        Returns:
            Dictionary with Benford analysis results or None if not applicable
        """
        series = df[col_name]

        # Handle string columns that need coercion to numeric
        if series.dtype == 'object':
            series = pd.to_numeric(series, errors='coerce')

        series = series.dropna()

        # Need sufficient positive values for meaningful analysis
        positive_values = series[series > 0]
        if len(positive_values) < MIN_ROWS_FOR_ML:
            return None

        # Extract first digits
        first_digits = positive_values.apply(lambda x: int(str(abs(x)).lstrip('0').lstrip('.')[0])
                                              if str(abs(x)).lstrip('0').lstrip('.') and
                                              str(abs(x)).lstrip('0').lstrip('.')[0].isdigit() else 0)
        first_digits = first_digits[first_digits > 0]  # Only digits 1-9

        if len(first_digits) < MIN_ROWS_FOR_ML:
            return None

        # Expected Benford distribution
        benford_expected = {
            1: 0.301, 2: 0.176, 3: 0.125, 4: 0.097,
            5: 0.079, 6: 0.067, 7: 0.058, 8: 0.051, 9: 0.046
        }

        # Observed distribution
        observed_counts = first_digits.value_counts().sort_index()
        total = len(first_digits)

        observed_dist = {}
        for digit in range(1, 10):
            observed_dist[digit] = observed_counts.get(digit, 0) / total

        # Chi-square test for goodness of fit
        chi_square = 0
        deviations = {}
        for digit in range(1, 10):
            expected = benford_expected[digit]
            observed = observed_dist[digit]
            deviation = observed - expected
            deviations[digit] = {
                "expected": round(expected * 100, 2),
                "observed": round(observed * 100, 2),
                "deviation": round(deviation * 100, 2)
            }
            # Chi-square contribution
            expected_count = expected * total
            observed_count = observed * total
            if expected_count > 0:
                chi_square += (observed_count - expected_count) ** 2 / expected_count

        # Degrees of freedom = 8 (9 digits - 1)
        # Calculate actual p-value using chi-squared distribution
        from scipy import stats as scipy_stats
        df_chi = 8  # degrees of freedom
        p_value = 1 - scipy_stats.chi2.cdf(chi_square, df_chi)

        # Interpretation based on p-value (standard statistical significance)
        # Low p-value = deviation from Benford (suspicious)
        is_suspicious = p_value < 0.05

        # Confidence in suspicious finding (based on p-value)
        if p_value < 0.001:
            confidence = "Very High"  # Strong evidence of deviation
        elif p_value < 0.01:
            confidence = "High"
        elif p_value < 0.05:
            confidence = "Medium"
        else:
            confidence = "Low"  # Not suspicious

        # Find worst deviations
        worst_deviations = sorted(deviations.items(), key=lambda x: abs(x[1]["deviation"]), reverse=True)[:3]

        # Calculate Mean Absolute Deviation (MAD) - simpler metric
        mad = np.mean([abs(deviations[d]["deviation"]) for d in range(1, 10)])

        # Determine interpretation
        if not is_suspicious:
            interpretation = f"Distribution follows Benford's Law (χ²={chi_square:.1f}, MAD={mad:.2f}%). Data appears naturally occurring."
        elif mad > 10:
            interpretation = f"STRONG deviation from Benford's Law (χ²={chi_square:.1f}, MAD={mad:.2f}%). Data may be fabricated, manipulated, or synthetic."
        else:
            interpretation = f"Moderate deviation from Benford's Law (χ²={chi_square:.1f}, MAD={mad:.2f}%). Warrants investigation."

        # Get applicability assessment for this column
        applicability = self._get_benford_applicability(col_name)

        # Adjust interpretation if Benford's Law is not applicable
        if not applicability["applicable"]:
            interpretation = (
                f"⚠️ NOT APPLICABLE: {applicability['explanation']} "
                f"The deviation (χ²={chi_square:.1f}, MAD={mad:.2f}%) is expected for cumulative data."
            )
            # Don't mark as suspicious if not applicable
            is_suspicious = False
            confidence = "Not Applicable"

        return {
            "method": "benford_law",
            "sample_size": total,
            "chi_square": round(chi_square, 2),
            "p_value": round(p_value, 6),
            "alpha": 0.05,  # Significance level used for test
            "conforms": not is_suspicious,  # True if data follows Benford's Law
            "mean_absolute_deviation": round(mad, 2),
            "is_suspicious": is_suspicious,
            "confidence": confidence,
            "digit_distribution": deviations,
            "worst_deviations": [
                {"digit": d, **dev} for d, dev in worst_deviations
            ],
            "interpretation": interpretation,
            "applicability": applicability,
            "plain_english": (
                "Benford's Law says naturally occurring numbers (like financial transactions) "
                "follow a predictable pattern - '1' appears as the first digit about 30% of the time, "
                "'2' about 17%, and so on. When data doesn't follow this pattern, it often means "
                "the numbers were made up, manipulated, or generated incorrectly. "
                "NOTE: This law does NOT apply to cumulative totals, sequential counts, or assigned numbers."
            )
        }

    def _detect_autoencoder_anomalies(self, df: pd.DataFrame, numeric_cols: List[str]) -> Optional[Dict[str, Any]]:
        """
        Detect anomalies using an Autoencoder neural network.

        Autoencoders learn to compress and reconstruct data. Records that are
        hard to reconstruct (high reconstruction error) are likely anomalies.

        This catches complex, non-linear relationships that Isolation Forest misses.

        Args:
            df: DataFrame to analyze
            numeric_cols: List of numeric columns to include

        Returns:
            Dictionary with autoencoder anomaly results or None if not applicable
        """
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.neural_network import MLPRegressor
        except ImportError:
            logger.debug("sklearn not available for autoencoder")
            return None

        # Need at least 2 columns and sufficient rows
        if len(numeric_cols) < 2:
            return None

        # Prepare data - drop rows with any nulls
        subset_df = df[numeric_cols].dropna()
        if len(subset_df) < MIN_ROWS_FOR_ML:
            return None

        # Limit columns to avoid curse of dimensionality
        if len(numeric_cols) > 10:
            # Select columns with highest variance
            variances = subset_df.var()
            numeric_cols = variances.nlargest(10).index.tolist()
            subset_df = subset_df[numeric_cols]

        try:
            # Standardize features
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(subset_df)

            n_features = X_scaled.shape[1]

            # Autoencoder architecture: compress to bottleneck, then expand
            # For simplicity, use sklearn's MLPRegressor as autoencoder
            # Hidden layers: input -> compress -> bottleneck -> expand -> output
            bottleneck_size = max(2, n_features // 3)
            hidden_layers = (n_features, bottleneck_size, n_features)

            # Train autoencoder (predict X from X)
            autoencoder = MLPRegressor(
                hidden_layer_sizes=hidden_layers,
                activation='relu',
                solver='adam',
                max_iter=200,
                random_state=42,
                early_stopping=True,
                validation_fraction=0.1,
                n_iter_no_change=10,
                verbose=False
            )

            # Suppress ConvergenceWarning unless DEBUG logging is enabled
            # The autoencoder still produces valid results without full convergence
            if logger.isEnabledFor(logging.DEBUG):
                autoencoder.fit(X_scaled, X_scaled)
            else:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=UserWarning, module='sklearn')
                    from sklearn.exceptions import ConvergenceWarning
                    warnings.filterwarnings('ignore', category=ConvergenceWarning)
                    autoencoder.fit(X_scaled, X_scaled)

            # Get reconstructions
            X_reconstructed = autoencoder.predict(X_scaled)

            # Calculate reconstruction error (MSE per row)
            reconstruction_errors = np.mean((X_scaled - X_reconstructed) ** 2, axis=1)

            # Identify anomalies using adaptive threshold
            # Use IQR method on reconstruction errors
            q1, q3 = np.percentile(reconstruction_errors, [25, 75])
            iqr = q3 - q1
            threshold = q3 + 2.5 * iqr  # More conservative than 1.5*IQR

            anomaly_mask = reconstruction_errors > threshold
            anomaly_count = anomaly_mask.sum()

            if anomaly_count == 0:
                return None

            anomaly_pct = anomaly_count / len(reconstruction_errors) * 100

            # Get top anomalies by reconstruction error
            anomaly_indices = np.where(anomaly_mask)[0]
            sorted_by_error = np.argsort(reconstruction_errors[anomaly_mask])[::-1]
            top_anomaly_indices = anomaly_indices[sorted_by_error[:10]]

            # Calculate which features contribute most to anomalies
            anomaly_data = X_scaled[anomaly_mask]
            anomaly_reconstructed = X_reconstructed[anomaly_mask]
            feature_errors = np.mean((anomaly_data - anomaly_reconstructed) ** 2, axis=0)

            contributing_features = []
            for i, col in enumerate(numeric_cols):
                if feature_errors[i] > np.mean(feature_errors):
                    contributing_features.append({
                        "column": col,
                        "error_contribution": round(feature_errors[i], 4)
                    })
            contributing_features.sort(key=lambda x: x["error_contribution"], reverse=True)

            # Get sample anomalous rows
            sample_rows = []
            for idx in top_anomaly_indices[:5]:
                if idx < len(subset_df):
                    row = subset_df.iloc[idx]
                    row_dict = {str(k)[:25]: f"{v:.2f}" if isinstance(v, (int, float)) else str(v)[:50]
                               for k, v in row.items()}
                    row_dict["_reconstruction_error"] = f"{reconstruction_errors[idx]:.4f}"
                    sample_rows.append(row_dict)

            # Confidence based on anomaly percentage
            if anomaly_pct < 1:
                confidence = "Very High"
            elif anomaly_pct < 3:
                confidence = "High"
            elif anomaly_pct < 5:
                confidence = "Medium"
            else:
                confidence = "Low"

            # Helper for consistent precision (use scientific notation for very small values)
            def smart_round(val, decimals=4):
                if val == 0:
                    return 0.0
                elif abs(val) < 0.0001:
                    return float(f"{val:.2e}")  # Scientific notation for tiny values
                else:
                    return round(val, decimals)

            # Calculate percentiles for better interpretability (mean is skewed by outliers)
            median_error = np.median(reconstruction_errors)
            q75_error = np.percentile(reconstruction_errors, 75)

            return {
                "method": "autoencoder",
                "architecture": f"Input({n_features}) -> {hidden_layers} -> Output({n_features})",
                "rows_analyzed": len(subset_df),
                "columns_analyzed": numeric_cols,
                "anomaly_count": int(anomaly_count),
                "anomaly_percentage": round(anomaly_pct, 4),
                "confidence": confidence,
                "threshold": smart_round(threshold, 8),
                "error_stats": {
                    "mean": smart_round(np.mean(reconstruction_errors)),
                    "median": smart_round(median_error),
                    "q75": smart_round(q75_error),
                    "std": smart_round(np.std(reconstruction_errors)),
                    "max": round(np.max(reconstruction_errors), 4),  # Max is always large
                    "anomaly_min_error": smart_round(reconstruction_errors[anomaly_mask].min())
                },
                "contributing_features": contributing_features[:5],
                "sample_rows": sample_rows,
                "interpretation": (
                    f"Found {anomaly_count:,} records ({anomaly_pct:.2f}%) that are difficult to reconstruct. "
                    f"These have unusual combinations of values that don't fit the learned patterns."
                ),
                "plain_english": (
                    "An autoencoder is like a 'data compressor' that learns what normal data looks like. "
                    "When it can't compress and decompress a record accurately, it means that record "
                    "has unusual patterns - combinations of values that don't match the rest of your data."
                )
            }

        except Exception as e:
            logger.debug(f"Error in autoencoder analysis: {e}")
            return None

    # =========================================================================
    # NEW ML CAPABILITIES - Data Drift, Duplicates, Anomaly Explanation,
    # Referential Integrity, Distribution Fitting
    # =========================================================================

    def detect_data_drift(self, current_df: pd.DataFrame,
                          baseline_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect distribution drift between current data and baseline statistics.

        Uses Population Stability Index (PSI) and Kolmogorov-Smirnov tests.

        Args:
            current_df: Current DataFrame to check
            baseline_stats: Baseline statistics from previous profile

        Returns:
            Dictionary with drift metrics per column
        """
        drift_results = {
            "columns_analyzed": 0,
            "columns_with_drift": 0,
            "drift_details": {},
            "overall_drift_score": 0.0,
            "interpretation": "",
            "plain_english": (
                "Data drift means your new data looks different from what you had before. "
                "Like if your customer ages suddenly skewed younger, or transaction amounts "
                "changed dramatically. This can break ML models and indicate data quality issues."
            )
        }

        try:
            from scipy import stats
        except ImportError:
            drift_results["error"] = "scipy not available for drift detection"
            return drift_results

        numeric_cols = current_df.select_dtypes(include=[np.number]).columns.tolist()
        drift_scores = []

        for col in numeric_cols:
            if col not in baseline_stats:
                continue

            baseline = baseline_stats[col]
            current_values = current_df[col].dropna().values

            if len(current_values) < 100:
                continue

            drift_results["columns_analyzed"] += 1
            col_drift = {"column": col, "metrics": {}}

            # Calculate PSI (Population Stability Index)
            try:
                baseline_mean = baseline.get("mean", 0)
                baseline_std = baseline.get("std_dev", 1) or 1
                current_mean = np.mean(current_values)
                current_std = np.std(current_values) or 1

                # Simplified PSI using mean/std comparison
                mean_shift = abs(current_mean - baseline_mean) / baseline_std
                std_ratio = max(current_std / baseline_std, baseline_std / current_std)

                psi_approx = mean_shift + abs(np.log(std_ratio))
                col_drift["metrics"]["psi"] = round(psi_approx, 4)
                col_drift["metrics"]["mean_shift_sigma"] = round(mean_shift, 2)
                col_drift["metrics"]["std_ratio"] = round(std_ratio, 2)

                # PSI interpretation
                if psi_approx < 0.1:
                    col_drift["psi_status"] = "stable"
                elif psi_approx < 0.25:
                    col_drift["psi_status"] = "moderate_drift"
                else:
                    col_drift["psi_status"] = "significant_drift"
                    drift_results["columns_with_drift"] += 1

                drift_scores.append(psi_approx)

            except Exception as e:
                logger.debug(f"PSI calculation failed for {col}: {e}")

            # KS Test against expected distribution
            try:
                baseline_min = baseline.get("min_value")
                baseline_max = baseline.get("max_value")
                if baseline_min is not None and baseline_max is not None:
                    # Normalize to 0-1 range for comparison
                    baseline_range = float(baseline_max) - float(baseline_min)
                    if baseline_range > 0:
                        normalized = (current_values - float(baseline_min)) / baseline_range
                        # KS test against uniform (if data was uniform in baseline)
                        ks_stat, ks_pvalue = stats.kstest(normalized, 'uniform')
                        col_drift["metrics"]["ks_statistic"] = round(ks_stat, 4)
                        col_drift["metrics"]["ks_pvalue"] = round(ks_pvalue, 6)

                        if ks_pvalue < 0.01:
                            col_drift["ks_status"] = "significant_change"
                        elif ks_pvalue < 0.05:
                            col_drift["ks_status"] = "moderate_change"
                        else:
                            col_drift["ks_status"] = "no_significant_change"

            except Exception as e:
                logger.debug(f"KS test failed for {col}: {e}")

            drift_results["drift_details"][col] = col_drift

        # Overall drift score
        if drift_scores:
            drift_results["overall_drift_score"] = round(np.mean(drift_scores), 4)
            if drift_results["overall_drift_score"] < 0.1:
                drift_results["interpretation"] = "Data distribution is stable - no significant drift detected."
            elif drift_results["overall_drift_score"] < 0.25:
                drift_results["interpretation"] = f"Moderate drift in {drift_results['columns_with_drift']} column(s). Monitor for trends."
            else:
                drift_results["interpretation"] = f"Significant drift detected in {drift_results['columns_with_drift']} column(s). Investigate data source changes."

        return drift_results

    def _detect_duplicates(self, df: pd.DataFrame,
                           key_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Detect exact and near-duplicate records.

        Args:
            df: DataFrame to check
            key_columns: Optional list of columns to use as keys (default: all)

        Returns:
            Dictionary with duplicate detection results
        """
        results = {
            "exact_duplicates": {"count": 0, "percentage": 0.0, "sample_indices": []},
            "fuzzy_duplicates": {"count": 0, "percentage": 0.0, "sample_pairs": []},
            "potential_key_columns": [],
            "interpretation": "",
            "plain_english": (
                "Duplicate detection finds records that are identical or nearly identical. "
                "Exact duplicates are perfect copies. Fuzzy duplicates are almost the same "
                "(like 'John Smith' vs 'John  Smith' with extra space). Both can indicate "
                "data entry errors, failed deduplication, or merge issues."
            )
        }

        total_rows = len(df)
        if total_rows < 10:
            results["interpretation"] = "Dataset too small for duplicate analysis."
            return results

        # 1. Exact duplicate detection
        try:
            check_cols = key_columns if key_columns else df.columns.tolist()
            # Limit columns for performance
            check_cols = check_cols[:20]

            duplicate_mask = df.duplicated(subset=check_cols, keep=False)
            exact_dup_count = duplicate_mask.sum()

            if exact_dup_count > 0:
                # Get indices of duplicates
                dup_indices = df[duplicate_mask].index.tolist()
                results["exact_duplicates"] = {
                    "count": int(exact_dup_count),
                    "percentage": round(exact_dup_count / total_rows * 100, 2),
                    "sample_indices": dup_indices[:10]
                }
        except Exception as e:
            logger.debug(f"Exact duplicate detection failed: {e}")

        # 2. Fuzzy duplicate detection (on string columns)
        string_cols = df.select_dtypes(include=['object']).columns.tolist()[:5]
        if string_cols and total_rows < 50000:  # Limit for performance
            try:
                # Simple fuzzy matching using normalized strings
                fuzzy_pairs = []
                sample_size = min(5000, total_rows)
                sample_df = df.sample(n=sample_size, random_state=42) if total_rows > sample_size else df

                for col in string_cols[:3]:  # Check first 3 string columns
                    values = sample_df[col].dropna().astype(str)
                    # Normalize: lowercase, strip whitespace, remove extra spaces
                    normalized = values.str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)

                    # Find values that become identical after normalization
                    value_counts = normalized.value_counts()
                    duplicated_normalized = value_counts[value_counts > 1].index.tolist()

                    for norm_val in duplicated_normalized[:5]:
                        original_values = values[normalized == norm_val].unique().tolist()
                        if len(original_values) > 1:  # Different original values normalize to same
                            fuzzy_pairs.append({
                                "column": col,
                                "normalized_value": norm_val,
                                "original_variants": original_values[:5],
                                "count": int(value_counts[norm_val])
                            })

                if fuzzy_pairs:
                    fuzzy_count = sum(p["count"] for p in fuzzy_pairs)
                    results["fuzzy_duplicates"] = {
                        "count": fuzzy_count,
                        "percentage": round(fuzzy_count / sample_size * 100, 2),
                        "sample_pairs": fuzzy_pairs[:10]
                    }
            except Exception as e:
                logger.debug(f"Fuzzy duplicate detection failed: {e}")

        # 3. Identify potential key columns (high uniqueness)
        for col in df.columns[:20]:
            try:
                unique_ratio = df[col].nunique() / total_rows
                if unique_ratio > 0.95:
                    results["potential_key_columns"].append({
                        "column": col,
                        "uniqueness": round(unique_ratio * 100, 2)
                    })
            except Exception:
                pass

        # Generate interpretation
        exact_count = results["exact_duplicates"]["count"]
        fuzzy_count = results["fuzzy_duplicates"]["count"]

        if exact_count == 0 and fuzzy_count == 0:
            results["interpretation"] = "No duplicate records detected. Data appears to be properly deduplicated."
        elif exact_count > 0 and fuzzy_count > 0:
            results["interpretation"] = (
                f"Found {exact_count:,} exact duplicates and {fuzzy_count:,} fuzzy duplicates. "
                "Review for data entry issues or failed deduplication processes."
            )
        elif exact_count > 0:
            results["interpretation"] = (
                f"Found {exact_count:,} exact duplicate records ({results['exact_duplicates']['percentage']:.1f}%). "
                "Consider deduplication if these are unintended."
            )
        else:
            results["interpretation"] = (
                f"Found {fuzzy_count:,} near-duplicate values. "
                "May indicate data entry inconsistencies or normalization issues."
            )

        return results

    def _explain_anomaly(self, record_values: np.ndarray, column_names: List[str],
                         model_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Explain why a record is flagged as anomalous using feature contribution analysis.

        Uses a simplified SHAP-like approach based on deviation from expected values.

        Args:
            record_values: Array of values for the anomalous record
            column_names: Names of columns
            model_stats: Statistics from the baseline (mean, std per column)

        Returns:
            Dictionary with feature contributions
        """
        contributions = []

        for i, (col, val) in enumerate(zip(column_names, record_values)):
            if col not in model_stats:
                continue

            try:
                col_stats = model_stats[col]
                mean = col_stats.get("mean", 0)
                std = col_stats.get("std_dev", 1) or 1

                # Z-score as contribution measure
                z_score = (val - mean) / std
                abs_z = abs(z_score)

                contribution = {
                    "column": col,
                    "value": float(val) if not np.isnan(val) else None,
                    "expected_mean": round(mean, 2),
                    "z_score": round(z_score, 2),
                    "contribution_score": round(abs_z, 2),
                    "direction": "high" if z_score > 0 else "low"
                }

                # Plain English explanation
                if abs_z > 3:
                    contribution["explanation"] = f"Extremely {'high' if z_score > 0 else 'low'} - {abs_z:.1f} standard deviations from normal"
                elif abs_z > 2:
                    contribution["explanation"] = f"Unusually {'high' if z_score > 0 else 'low'} - well outside typical range"
                elif abs_z > 1:
                    contribution["explanation"] = f"Somewhat {'high' if z_score > 0 else 'low'} - borderline unusual"
                else:
                    contribution["explanation"] = "Within normal range"

                contributions.append(contribution)

            except Exception as e:
                logger.debug(f"Feature contribution failed for {col}: {e}")

        # Sort by contribution (most anomalous first)
        contributions.sort(key=lambda x: x["contribution_score"], reverse=True)

        # Top contributors
        top_contributors = contributions[:5]

        return {
            "feature_contributions": contributions,
            "top_contributors": top_contributors,
            "primary_driver": top_contributors[0] if top_contributors else None,
            "explanation_summary": self._summarize_anomaly_explanation(top_contributors),
            "plain_english": (
                "This shows WHY a record is flagged as unusual. Each feature gets a 'contribution score' "
                "based on how far it deviates from typical values. Higher scores mean that feature "
                "is more responsible for making this record look anomalous."
            )
        }

    def _summarize_anomaly_explanation(self, contributors: List[Dict]) -> str:
        """Generate a human-readable summary of anomaly contributors."""
        if not contributors:
            return "Unable to determine anomaly cause."

        top = contributors[0]
        summary = f"Primary anomaly driver: {top['column']} is {top['direction']} "
        summary += f"({top['z_score']:.1f} std devs from mean). "

        if len(contributors) > 1:
            others = [c['column'] for c in contributors[1:3]]
            summary += f"Also contributing: {', '.join(others)}."

        return summary

    def _check_referential_integrity(self, df: pd.DataFrame,
                                     reference_data: Optional[Dict[str, set]] = None) -> Dict[str, Any]:
        """
        Check for orphan records and broken references.

        Args:
            df: DataFrame to check
            reference_data: Optional dict mapping column names to sets of valid values

        Returns:
            Dictionary with referential integrity results
        """
        results = {
            "potential_foreign_keys": [],
            "orphan_analysis": [],
            "self_referential_issues": [],
            "interpretation": "",
            "plain_english": (
                "Referential integrity ensures that references between tables are valid. "
                "Like if you have an 'employee_id' in the orders table, that employee should "
                "actually exist in the employees table. Orphan records are those pointing to "
                "non-existent references - they often indicate data sync issues or deletions."
            )
        }

        # 1. Identify potential foreign key columns (naming patterns)
        fk_patterns = ['_id', 'id_', '_key', '_code', '_ref', 'fk_']
        potential_fks = []

        for col in df.columns:
            col_lower = str(col).lower() if col is not None else ''
            if any(pattern in col_lower for pattern in fk_patterns):
                unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0
                null_ratio = df[col].isna().sum() / len(df) if len(df) > 0 else 0

                potential_fks.append({
                    "column": col,
                    "unique_values": df[col].nunique(),
                    "unique_ratio": round(unique_ratio, 4),
                    "null_ratio": round(null_ratio, 4),
                    "likely_type": "foreign_key" if unique_ratio < 0.5 else "primary_key"
                })

        results["potential_foreign_keys"] = potential_fks

        # 2. Check against provided reference data
        if reference_data:
            for col, valid_values in reference_data.items():
                if col in df.columns:
                    col_values = set(df[col].dropna().unique())
                    orphans = col_values - valid_values
                    if orphans:
                        results["orphan_analysis"].append({
                            "column": col,
                            "orphan_count": len(orphans),
                            "sample_orphans": list(orphans)[:10],
                            "total_affected_rows": int(df[col].isin(orphans).sum())
                        })

        # 3. Self-referential checks (e.g., parent_id referencing same table's id)
        id_cols = [c for c in df.columns if c.lower().endswith('_id') or c.lower() == 'id']
        parent_cols = [c for c in df.columns if 'parent' in c.lower() or 'manager' in c.lower()]

        for parent_col in parent_cols:
            for id_col in id_cols:
                if parent_col != id_col:
                    try:
                        valid_ids = set(df[id_col].dropna().unique())
                        parent_values = set(df[parent_col].dropna().unique())
                        invalid_parents = parent_values - valid_ids - {0, None, '', 'NULL', 'null'}

                        if invalid_parents and len(invalid_parents) < len(parent_values) * 0.5:
                            results["self_referential_issues"].append({
                                "parent_column": parent_col,
                                "id_column": id_col,
                                "invalid_references": len(invalid_parents),
                                "sample_invalid": list(invalid_parents)[:5]
                            })
                    except Exception:
                        pass

        # Generate interpretation
        fk_count = len(potential_fks)
        orphan_count = len(results["orphan_analysis"])
        self_ref_issues = len(results["self_referential_issues"])

        if orphan_count > 0 or self_ref_issues > 0:
            results["interpretation"] = (
                f"Found {orphan_count} columns with orphan records and {self_ref_issues} self-referential issues. "
                "Review data synchronization and deletion cascades."
            )
        elif fk_count > 0:
            results["interpretation"] = (
                f"Identified {fk_count} potential foreign key columns. "
                "Provide reference data to check for orphan records."
            )
        else:
            results["interpretation"] = "No obvious foreign key columns detected."

        return results

    def _fit_distribution(self, values: np.ndarray) -> Dict[str, Any]:
        """
        Identify the best-fit statistical distribution for numeric data.

        Tests common distributions and returns goodness-of-fit metrics.

        Args:
            values: Array of numeric values

        Returns:
            Dictionary with distribution fitting results
        """
        results = {
            "best_fit": None,
            "distributions_tested": [],
            "interpretation": "",
            "plain_english": (
                "Distribution fitting tells you what 'shape' your data has. Normal (bell curve) "
                "is common for natural measurements. Exponential is common for waiting times. "
                "Log-normal is common for financial data. Knowing the distribution helps set "
                "appropriate thresholds and detect true anomalies."
            )
        }

        try:
            from scipy import stats
        except ImportError:
            results["error"] = "scipy not available for distribution fitting"
            return results

        # Clean data
        values = values[~np.isnan(values)]
        if len(values) < 100:
            results["interpretation"] = "Not enough data for reliable distribution fitting."
            return results

        # Distributions to test
        distributions = [
            ('normal', stats.norm),
            ('lognormal', stats.lognorm),
            ('exponential', stats.expon),
            ('uniform', stats.uniform),
            ('gamma', stats.gamma),
        ]

        fit_results = []

        for name, dist in distributions:
            try:
                # Fit the distribution
                if name == 'lognormal' and np.any(values <= 0):
                    continue  # Skip lognormal for non-positive data

                params = dist.fit(values)

                # KS test for goodness of fit
                ks_stat, ks_pvalue = stats.kstest(values, dist.cdf, args=params)

                # AIC approximation (lower is better)
                log_likelihood = np.sum(dist.logpdf(values, *params))
                n_params = len(params)
                aic = 2 * n_params - 2 * log_likelihood

                fit_results.append({
                    "distribution": name,
                    "parameters": {f"param_{i}": round(p, 4) for i, p in enumerate(params)},
                    "ks_statistic": round(ks_stat, 4),
                    "ks_pvalue": round(ks_pvalue, 6),
                    "aic": round(aic, 2),
                    "good_fit": ks_pvalue > 0.05
                })

            except Exception as e:
                logger.debug(f"Distribution fitting failed for {name}: {e}")

        if fit_results:
            # Sort by AIC (lower is better)
            fit_results.sort(key=lambda x: x["aic"])
            results["distributions_tested"] = fit_results
            results["best_fit"] = fit_results[0]

            best = fit_results[0]
            if best["good_fit"]:
                results["interpretation"] = (
                    f"Data best fits a {best['distribution']} distribution (p={best['ks_pvalue']:.4f}). "
                    f"This is a statistically good fit."
                )
            else:
                results["interpretation"] = (
                    f"Best match is {best['distribution']} distribution, but fit is not statistically strong. "
                    f"Data may have a mixed or non-standard distribution."
                )

            # Add practical implications
            dist_implications = {
                'normal': "Standard statistical tests and z-scores are appropriate.",
                'lognormal': "Consider log-transforming for analysis. Common in financial data.",
                'exponential': "Data represents waiting times or decay. Mean = standard deviation.",
                'uniform': "Values are evenly spread. May indicate synthetic or categorical data.",
                'gamma': "Right-skewed positive data. Common for income, insurance claims."
            }

            best_dist = best['distribution']
            if best_dist in dist_implications:
                results["practical_implication"] = dist_implications[best_dist]

        return results

    def _generate_summary(self, findings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate overall summary of ML findings.

        Organized into tiers:
        - Tier 1: Data Authenticity (Benford's Law)
        - Tier 2: Record Anomalies (Outliers, Autoencoder)
        - Tier 3: Data Quality Issues (Rare values, Format, Cross-column)
        - Tier 4: Pattern Analysis (Temporal, Correlation)
        - Informational: Clustering (not counted as issues)
        """
        key_findings = []
        total_issues = 0

        # === TIER 1: DATA AUTHENTICITY ===
        benford_violations = findings.get("benford_analysis", {})
        if benford_violations:
            suspicious_cols = [col for col, data in benford_violations.items() if data.get("is_suspicious")]
            if suspicious_cols:
                worst = max(benford_violations.items(), key=lambda x: x[1].get("chi_square", 0))
                key_findings.append(f"📊 Benford's Law: {len(suspicious_cols)} column(s) may contain fabricated/synthetic data")
                # Informational - doesn't add to total_issues

        # === TIER 2: RECORD ANOMALIES ===
        # Univariate outliers (extreme values per column)
        outlier_count = sum(
            f.get("anomaly_count", 0)
            for f in findings["numeric_outliers"].values()
        )
        if outlier_count > 0:
            worst_col = max(
                findings["numeric_outliers"].items(),
                key=lambda x: x[1].get("anomaly_count", 0)
            )
            key_findings.append(f"🎯 Outliers: {outlier_count:,} extreme values detected (worst: {worst_col[0]})")
            total_issues += outlier_count

        # Autoencoder (complex pattern anomalies)
        autoencoder = findings.get("autoencoder_anomalies", {})
        if autoencoder:
            ae_count = autoencoder.get("anomaly_count", 0)
            if ae_count > 0:
                ae_pct = autoencoder.get("anomaly_percentage", 0)
                key_findings.append(f"🧠 Deep Learning: {ae_count:,} records ({ae_pct:.2f}%) with unusual patterns")
                # Note: May overlap with outliers, but captures different anomalies
                # Only add non-overlapping portion (estimate 50% overlap)
                total_issues += ae_count // 2

        # === TIER 3: DATA QUALITY ISSUES ===
        # Rare categories (typos, invalid values)
        rare_count = sum(
            f.get("total_rare_count", 0)
            for f in findings["rare_categories"].values()
        )
        if rare_count > 0:
            total_issues += rare_count
            key_findings.append(f"📝 Rare Values: {rare_count:,} potentially invalid categorical values")

        # Format anomalies
        format_count = sum(
            f.get("anomaly_count", 0)
            for f in findings["format_anomalies"].values()
        )
        if format_count > 0:
            total_issues += format_count
            key_findings.append(f"📋 Format Issues: {format_count:,} values don't match expected patterns")

        # Cross-column issues
        cross_col_count = sum(
            issue.get("total_issues", 0)
            for issue in findings["cross_column_issues"]
        )
        if cross_col_count > 0:
            total_issues += cross_col_count
            key_findings.append(f"🔗 Cross-Column: {cross_col_count:,} records with unexpected ratios")

        # === TIER 4: PATTERN ANALYSIS ===
        # Temporal anomalies
        temporal_warnings = [
            f for f in findings["temporal_patterns"].values()
            if f.get("warning") or f.get("warnings")
        ]
        if temporal_warnings:
            key_findings.append(f"⏰ Temporal: {len(temporal_warnings)} column(s) with suspicious time patterns")

        # Correlation anomalies
        corr_anomalies = findings.get("correlation_anomalies", {})
        if corr_anomalies:
            corr_breaks = corr_anomalies.get("correlation_breaks", [])
            corr_count = sum(b.get("anomaly_count", 0) for b in corr_breaks)
            if corr_count > 0:
                total_issues += corr_count
                key_findings.append(f"📈 Correlations: {corr_count:,} records break expected relationships")

        # === NEW CAPABILITIES ===
        # Duplicate detection
        duplicates = findings.get("duplicate_analysis", {})
        if duplicates:
            exact_dups = duplicates.get("exact_duplicates", {}).get("count", 0)
            fuzzy_dups = duplicates.get("fuzzy_duplicates", {}).get("count", 0)
            if exact_dups > 0:
                total_issues += exact_dups
                key_findings.append(f"📋 Duplicates: {exact_dups:,} exact duplicate records found")
            if fuzzy_dups > 0:
                key_findings.append(f"📋 Near-Duplicates: {fuzzy_dups:,} fuzzy matches detected")

        # Referential integrity
        ref_integrity = findings.get("referential_integrity", {})
        if ref_integrity:
            orphans = ref_integrity.get("orphan_analysis", [])
            self_ref = ref_integrity.get("self_referential_issues", [])
            if orphans:
                orphan_count = sum(o.get("total_affected_rows", 0) for o in orphans)
                if orphan_count > 0:
                    total_issues += orphan_count
                    key_findings.append(f"🔗 Orphan Records: {orphan_count:,} records with broken references")
            if self_ref:
                key_findings.append(f"🔗 Self-Reference Issues: {len(self_ref)} circular reference problems")

        # Distribution analysis (informational)
        dist_analysis = findings.get("distribution_analysis", {})
        if dist_analysis:
            non_normal = [col for col, d in dist_analysis.items()
                         if d.get("best_fit", {}).get("distribution") != "normal"]
            if non_normal:
                key_findings.append(f"📈 Distributions: {len(non_normal)} column(s) are not normally distributed")

        # === INFORMATIONAL (not counted as issues) ===
        clustering = findings.get("clustering_analysis", {})
        if clustering:
            n_clusters = clustering.get("n_clusters", 0)
            if n_clusters > 0:
                key_findings.append(f"ℹ️ Data Structure: {n_clusters} natural clusters identified")

        # Determine severity based on percentage of issues
        sample_info = findings.get("sample_info", {})
        analyzed_rows = sample_info.get("analyzed_rows", 1)
        issue_rate = total_issues / analyzed_rows if analyzed_rows > 0 else 0

        if issue_rate > 0.05 or total_issues > 10000:
            severity = "high"
        elif issue_rate > 0.01 or total_issues > 1000:
            severity = "medium"
        elif total_issues > 0:
            severity = "low"
        else:
            severity = "none"
            key_findings.append("No significant anomalies detected")

        return {
            "total_issues": total_issues,
            "issue_rate": round(issue_rate * 100, 4),
            "severity": severity,
            "key_findings": key_findings
        }


def run_ml_analysis(
    df: pd.DataFrame,
    column_semantic_info: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Convenience function to run ML analysis on a DataFrame.

    Args:
        df: DataFrame to analyze (sampling should be done by caller if needed)
        column_semantic_info: Optional dict mapping column names to their semantic info.
                              Each entry should have 'semantic_tags', 'primary_tag', etc.
                              Used for intelligent rare category detection based on FIBO.

    Returns:
        Dictionary containing ML findings
    """
    analyzer = MLAnalyzer()
    return analyzer.analyze(df, column_semantic_info=column_semantic_info)
