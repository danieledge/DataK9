"""
Column Family Detector for DataK9 Profiler.

Detects groups of columns that share naming patterns and likely represent
related data (e.g., time series dates, numbered sequences, prefixed groups).

This enables intelligent profiling of wide datasets by grouping similar
columns together instead of profiling each individually.
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Thresholds
WIDE_DATASET_THRESHOLD = 50  # Columns above this trigger family detection
MIN_FAMILY_SIZE = 5  # Minimum columns to form a family
SAMPLE_SIZE = 10  # Number of representative columns to sample per family


@dataclass
class ColumnFamily:
    """Represents a group of related columns."""
    name: str  # Human-readable family name
    pattern_type: str  # 'date', 'numeric_sequence', 'prefix', 'suffix', 'similar_dtype'
    pattern_description: str  # e.g., "Date columns (M/D/YY format)"
    columns: List[str] = field(default_factory=list)
    sample_columns: List[str] = field(default_factory=list)  # Representative sample
    aggregate_stats: Dict[str, Any] = field(default_factory=dict)
    anomalous_columns: List[str] = field(default_factory=list)  # Columns that deviate

    @property
    def count(self) -> int:
        return len(self.columns)


class ColumnFamilyDetector:
    """
    Detects column families in wide datasets.

    Supports detection of:
    - Date-formatted column names (M/D/YY, YYYY-MM-DD, etc.)
    - Numeric sequences (col_1, col_2, ... or 1, 2, 3, ...)
    - Common prefixes/suffixes
    - Columns with identical dtypes and similar statistics
    """

    # Date patterns commonly found in column names
    DATE_PATTERNS = [
        (r'^\d{1,2}/\d{1,2}/\d{2,4}$', 'M/D/YY'),  # 1/22/20, 12/31/2020
        (r'^\d{4}-\d{2}-\d{2}$', 'YYYY-MM-DD'),  # 2020-01-22
        (r'^\d{2}-\d{2}-\d{4}$', 'DD-MM-YYYY'),  # 22-01-2020
        (r'^\d{8}$', 'YYYYMMDD'),  # 20200122
        (r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[- ]\d{2,4}$', 'Mon-YY'),
        (r'^\d{4}[- ]?Q[1-4]$', 'YYYY-Q#'),  # 2020-Q1, 2020Q1
        (r'^(Q[1-4])[- ]?\d{4}$', 'Q#-YYYY'),  # Q1-2020, Q12020
        (r'^\d{4}$', 'YYYY'),  # Year only: 2020, 2021
    ]

    # Numeric sequence patterns
    NUMERIC_PATTERNS = [
        (r'^(.+?)(\d+)$', 'prefix_number'),  # col_1, feature_2
        (r'^(\d+)(.+?)$', 'number_prefix'),  # 1_col, 2_feature
        (r'^\d+$', 'pure_number'),  # 1, 2, 3
    ]

    def __init__(
        self,
        wide_threshold: int = WIDE_DATASET_THRESHOLD,
        min_family_size: int = MIN_FAMILY_SIZE,
        sample_size: int = SAMPLE_SIZE
    ):
        self.wide_threshold = wide_threshold
        self.min_family_size = min_family_size
        self.sample_size = sample_size

    def is_wide_dataset(self, df: pd.DataFrame) -> bool:
        """Check if dataset qualifies as 'wide' for family detection."""
        return len(df.columns) > self.wide_threshold

    def detect_families(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect column families in the dataframe.

        Returns:
            {
                'is_wide': bool,
                'total_columns': int,
                'families': [ColumnFamily, ...],
                'standalone_columns': [col_names that don't belong to any family],
                'family_columns': [col_names that belong to families]
            }
        """
        columns = list(df.columns)
        total_cols = len(columns)

        if not self.is_wide_dataset(df):
            return {
                'is_wide': False,
                'total_columns': total_cols,
                'families': [],
                'standalone_columns': columns,
                'family_columns': []
            }

        logger.info(f"Wide dataset detected ({total_cols} columns). Detecting families...")

        # Track which columns have been assigned to families
        assigned = set()
        families = []

        # Priority 1: Detect date-formatted columns
        date_family = self._detect_date_columns(columns)
        if date_family and date_family.count >= self.min_family_size:
            families.append(date_family)
            assigned.update(date_family.columns)
            logger.info(f"Found date family: {date_family.count} columns ({date_family.pattern_description})")

        # Priority 2: Detect numeric sequences
        remaining = [c for c in columns if c not in assigned]
        numeric_families = self._detect_numeric_sequences(remaining)
        for family in numeric_families:
            if family.count >= self.min_family_size:
                families.append(family)
                assigned.update(family.columns)
                logger.info(f"Found numeric sequence: {family.count} columns ({family.pattern_description})")

        # Priority 3: Detect prefix/suffix groups
        remaining = [c for c in columns if c not in assigned]
        prefix_families = self._detect_prefix_suffix_groups(remaining)
        for family in prefix_families:
            if family.count >= self.min_family_size:
                families.append(family)
                assigned.update(family.columns)
                logger.info(f"Found prefix/suffix group: {family.count} columns ({family.pattern_description})")

        # Priority 4: Group remaining by dtype similarity (for very wide datasets)
        remaining = [c for c in columns if c not in assigned]
        if len(remaining) > self.wide_threshold:
            dtype_families = self._detect_dtype_families(df, remaining)
            for family in dtype_families:
                if family.count >= self.min_family_size:
                    families.append(family)
                    assigned.update(family.columns)
                    logger.info(f"Found dtype family: {family.count} columns ({family.pattern_description})")

        # Select sample columns for each family
        for family in families:
            family.sample_columns = self._select_sample_columns(family.columns, df)

        standalone = [c for c in columns if c not in assigned]

        return {
            'is_wide': True,
            'total_columns': total_cols,
            'families': families,
            'standalone_columns': standalone,
            'family_columns': list(assigned)
        }

    def _detect_date_columns(self, columns: List[str]) -> Optional[ColumnFamily]:
        """Detect columns with date-formatted names."""
        for pattern, format_name in self.DATE_PATTERNS:
            regex = re.compile(pattern, re.IGNORECASE)
            matching = [c for c in columns if regex.match(str(c))]

            if len(matching) >= self.min_family_size:
                return ColumnFamily(
                    name='Time Series Dates',
                    pattern_type='date',
                    pattern_description=f'Date columns ({format_name} format)',
                    columns=matching
                )

        return None

    def _detect_numeric_sequences(self, columns: List[str]) -> List[ColumnFamily]:
        """Detect columns that follow numeric sequence patterns."""
        families = []

        # Group by prefix + number pattern
        prefix_groups = defaultdict(list)

        for col in columns:
            col_str = str(col)
            match = re.match(r'^(.+?)[-_]?(\d+)$', col_str)
            if match:
                prefix = match.group(1).rstrip('-_')
                prefix_groups[prefix].append(col)

        for prefix, cols in prefix_groups.items():
            if len(cols) >= self.min_family_size:
                families.append(ColumnFamily(
                    name=f'{prefix} sequence',
                    pattern_type='numeric_sequence',
                    pattern_description=f'Numbered columns ({prefix}_N)',
                    columns=cols
                ))

        return families

    def _detect_prefix_suffix_groups(self, columns: List[str]) -> List[ColumnFamily]:
        """Detect columns with common prefixes or suffixes."""
        families = []

        if len(columns) < self.min_family_size:
            return families

        # Find common prefixes (minimum 3 characters)
        prefix_groups = defaultdict(list)
        for col in columns:
            col_str = str(col)
            if len(col_str) >= 4:
                # Try prefixes of length 3-10
                for length in range(3, min(11, len(col_str))):
                    prefix = col_str[:length]
                    if prefix.endswith('_') or prefix.endswith('-'):
                        prefix_groups[prefix].append(col)
                        break

        for prefix, cols in prefix_groups.items():
            if len(cols) >= self.min_family_size:
                families.append(ColumnFamily(
                    name=f'{prefix}* columns',
                    pattern_type='prefix',
                    pattern_description=f'Columns starting with "{prefix}"',
                    columns=cols
                ))

        return families

    def _detect_dtype_families(self, df: pd.DataFrame, columns: List[str]) -> List[ColumnFamily]:
        """Group remaining columns by dtype and statistical similarity."""
        families = []

        dtype_groups = defaultdict(list)
        for col in columns:
            dtype_str = str(df[col].dtype)
            dtype_groups[dtype_str].append(col)

        for dtype, cols in dtype_groups.items():
            if len(cols) >= self.min_family_size:
                families.append(ColumnFamily(
                    name=f'{dtype} columns',
                    pattern_type='similar_dtype',
                    pattern_description=f'Columns with {dtype} data type',
                    columns=cols
                ))

        return families

    def _select_sample_columns(self, columns: List[str], df: pd.DataFrame) -> List[str]:
        """Select representative sample of columns from a family."""
        if len(columns) <= self.sample_size:
            return columns

        # Always include first and last
        sample = [columns[0], columns[-1]]

        # Add evenly spaced middle samples
        n_middle = self.sample_size - 2
        step = len(columns) // (n_middle + 1)
        for i in range(1, n_middle + 1):
            idx = i * step
            if idx < len(columns) and columns[idx] not in sample:
                sample.append(columns[idx])

        # Sort by original order
        col_order = {c: i for i, c in enumerate(columns)}
        sample.sort(key=lambda x: col_order.get(x, 0))

        return sample[:self.sample_size]

    def compute_family_stats(
        self,
        family: ColumnFamily,
        df: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Compute aggregate statistics across all columns in a family.

        Returns summary stats that describe the family as a whole.
        """
        family_df = df[family.columns]

        stats = {
            'column_count': family.count,
            'pattern_type': family.pattern_type,
            'pattern_description': family.pattern_description,
            'sample_columns': family.sample_columns,
        }

        # Check if numeric
        numeric_cols = family_df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            # Aggregate numeric stats across all family columns
            all_values = family_df[numeric_cols].values.flatten()
            all_values = all_values[~np.isnan(all_values)]

            if len(all_values) > 0:
                stats['value_stats'] = {
                    'min': float(np.min(all_values)),
                    'max': float(np.max(all_values)),
                    'mean': float(np.mean(all_values)),
                    'median': float(np.median(all_values)),
                    'std': float(np.std(all_values)),
                    'total_values': len(all_values),
                }

                # Per-column stats for anomaly detection
                col_means = family_df[numeric_cols].mean()
                col_stds = family_df[numeric_cols].std()

                # Detect anomalous columns (mean > 2 std from family mean)
                family_mean = col_means.mean()
                family_std = col_means.std()
                if family_std > 0:
                    z_scores = (col_means - family_mean) / family_std
                    anomalous = z_scores[abs(z_scores) > 2].index.tolist()
                    stats['anomalous_columns'] = anomalous
                    family.anomalous_columns = anomalous

        # Null analysis across family
        null_counts = family_df.isnull().sum()
        total_nulls = null_counts.sum()
        stats['null_stats'] = {
            'total_nulls': int(total_nulls),
            'null_rate': float(total_nulls / family_df.size) if family_df.size > 0 else 0,
            'columns_with_nulls': int((null_counts > 0).sum()),
            'fully_null_columns': int((null_counts == len(df)).sum()),
        }

        # Detect columns that are all zeros or constant
        if len(numeric_cols) > 0:
            constant_cols = []
            zero_cols = []
            for col in numeric_cols:
                unique_vals = family_df[col].dropna().unique()
                if len(unique_vals) <= 1:
                    constant_cols.append(col)
                    if len(unique_vals) == 1 and unique_vals[0] == 0:
                        zero_cols.append(col)

            stats['constant_columns'] = constant_cols
            stats['zero_columns'] = zero_cols

        family.aggregate_stats = stats
        return stats


def detect_and_summarize_families(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Convenience function to detect families and compute their stats.

    Returns complete family analysis for a dataframe.
    """
    detector = ColumnFamilyDetector()
    result = detector.detect_families(df)

    if result['is_wide']:
        for family in result['families']:
            detector.compute_family_stats(family, df)

    return result
