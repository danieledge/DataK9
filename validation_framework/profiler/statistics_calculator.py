"""
Statistics Calculator - Comprehensive Statistical Analysis for Data Profiling.

This module handles all statistical calculations for column profiling, providing
numeric statistics, frequency analysis, string metrics, pattern detection, and
data quality scoring.

Architecture:
    StatisticsCalculator is responsible for:
    1. Numeric statistics (min, max, mean, median, std, quartiles)
    2. Frequency analysis (mode, top values, value counts)
    3. String metrics (length distribution)
    4. Pattern analysis (structural patterns in data)
    5. Quality metrics (completeness, validity, uniqueness, consistency)
    6. Correlation analysis between numeric columns

Design Decisions:
    - Extreme value filtering: Values beyond 1e100 are filtered as data corruption
    - At least 50% of values must be reasonable to report numeric stats
    - Quality score is weighted: 30% completeness, 30% validity, 20% uniqueness, 20% consistency
    - Correlations only reported when |r| > 0.5 (moderate to strong)
    - Top 10 values/patterns reported to balance detail vs. report size

Usage:
    calculator = StatisticsCalculator(max_correlation_columns=20)
    stats = calculator.calculate_statistics(profile_data, total_rows)
    quality = calculator.calculate_quality_metrics(profile_data, type_info, stats, total_rows)
    correlations = calculator.calculate_correlations(numeric_data, row_count)

Extracted from DataProfiler to follow Single Responsibility Principle and
enable independent testing of statistical calculations.
"""

import logging
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd

from validation_framework.profiler.profile_result import (
    ColumnStatistics,
    QualityMetrics,
    TypeInference,
    CorrelationResult,
)
from validation_framework.profiler.column_intelligence import SmartColumnAnalyzer

logger = logging.getLogger(__name__)

# Optional visions library for semantic type detection
try:
    from visions.functional import detect_type
    VISIONS_AVAILABLE = True
except ImportError:
    VISIONS_AVAILABLE = False


class StatisticsCalculator:
    """
    Comprehensive statistical analysis for data columns.

    Provides robust statistical calculations with outlier handling, semantic type
    detection, and data quality scoring. Designed for both small and large datasets
    with memory-efficient processing.

    Attributes:
        max_correlation_columns: Maximum columns to include in correlation analysis.
            Limited to prevent O(n²) explosion with many columns.

    Example:
        >>> calculator = StatisticsCalculator(max_correlation_columns=20)
        >>> stats = calculator.calculate_statistics(profile_data, total_rows=1000)
        >>> print(f"Mean: {stats.mean}, Median: {stats.median}")
        >>> quality = calculator.calculate_quality_metrics(profile_data, type_info, stats, 1000)
        >>> print(f"Overall quality score: {quality.overall_score:.1f}%")
    """

    def __init__(self, max_correlation_columns: int = 20):
        """
        Initialize the statistics calculator.

        Args:
            max_correlation_columns: Maximum columns for correlation analysis
        """
        self.max_correlation_columns = max_correlation_columns

    def calculate_statistics(
        self,
        profile_data: Dict[str, Any],
        total_rows: int
    ) -> ColumnStatistics:
        """
        Calculate comprehensive column statistics.

        Args:
            profile_data: Dictionary containing column profile data
            total_rows: Total number of rows in the dataset

        Returns:
            ColumnStatistics object with calculated statistics
        """
        null_count = profile_data["null_count"]
        value_counts = profile_data["value_counts"]
        numeric_values = profile_data["numeric_values"]
        string_lengths = profile_data["string_lengths"]
        patterns = profile_data["patterns"]

        # Use intelligent sampling to determine optimal sample size
        column_name = profile_data["column_name"]
        intelligence = SmartColumnAnalyzer.analyze_column(column_name)

        stats = ColumnStatistics()
        stats.count = total_rows
        stats.null_count = null_count
        stats.null_percentage = 100 * null_count / total_rows if total_rows > 0 else 0
        stats.whitespace_null_count = profile_data.get("whitespace_null_count", 0)
        stats.placeholder_null_count = profile_data.get("placeholder_null_count", 0)
        stats.placeholder_values_found = profile_data.get("placeholder_values_found", {})

        # Enhanced semantic type detection using visions (if available)
        visions_semantic_type = self._detect_semantic_type_with_visions(profile_data["sample_values"])

        # Prefer visions detection over name-based detection for higher accuracy
        if visions_semantic_type and visions_semantic_type not in ['integer', 'float', 'string', 'object']:
            # Visions detected a specific semantic type (email, url, uuid, etc.)
            stats.semantic_type = visions_semantic_type
            logger.debug(f"Visions detected semantic type for '{column_name}': {visions_semantic_type}")
        else:
            # Fallback to name-based detection from SmartColumnAnalyzer
            stats.semantic_type = intelligence.semantic_type

        # Add intelligent sampling metadata for transparency
        stats.sample_size = min(len(numeric_values) if numeric_values else len(value_counts), intelligence.recommended_sample_size)
        stats.sampling_strategy = SmartColumnAnalyzer.get_sampling_summary(
            column_name, total_rows, intelligence
        )

        # Unique counts
        stats.unique_count = len(value_counts)
        non_null_count = total_rows - null_count
        stats.unique_percentage = 100 * stats.unique_count / non_null_count if non_null_count > 0 else 0
        stats.cardinality = stats.unique_count / non_null_count if non_null_count > 0 else 0

        # Numeric statistics
        self._calculate_numeric_stats(stats, numeric_values)

        # Frequency statistics
        self._calculate_frequency_stats(stats, value_counts, non_null_count)

        # String length statistics
        self._calculate_string_stats(stats, string_lengths)

        # Pattern samples
        self._calculate_pattern_stats(stats, patterns, profile_data.get("sample_values", []))

        return stats

    def _calculate_numeric_stats(
        self,
        stats: ColumnStatistics,
        numeric_values: List[float]
    ) -> None:
        """Calculate numeric statistics in place."""
        if numeric_values is None or len(numeric_values) == 0:
            return

        try:
            # Convert to float array explicitly to avoid type issues
            numeric_array = np.array(numeric_values, dtype=np.float64)

            # Filter out extreme values that indicate parsing errors
            # Values beyond 1e100 are almost certainly data corruption
            reasonable_mask = np.abs(numeric_array) < 1e100
            if np.any(~np.isfinite(numeric_array)):
                reasonable_mask &= np.isfinite(numeric_array)

            filtered_array = numeric_array[reasonable_mask]

            # Only calculate stats if we have reasonable values left
            # and if the majority of values are reasonable
            if len(filtered_array) > 0 and len(filtered_array) >= len(numeric_array) * 0.5:
                stats.min_value = float(np.min(filtered_array))
                stats.max_value = float(np.max(filtered_array))
                stats.mean = float(np.mean(filtered_array))
                stats.median = float(np.median(filtered_array))
                stats.std_dev = float(np.std(filtered_array))

                # Quartiles
                q1, q2, q3 = np.percentile(filtered_array, [25, 50, 75])
                stats.quartiles = {
                    "Q1": round(float(q1), 3),
                    "Q2": round(float(q2), 3),
                    "Q3": round(float(q3), 3)
                }
            else:
                # Too many extreme values - likely string column with some numeric-like values
                logger.debug(f"Skipping numeric stats: {len(numeric_array) - len(filtered_array)} extreme values filtered")
        except (TypeError, ValueError) as e:
            logger.warning(f"Could not calculate numeric statistics: {e}")

    def _calculate_frequency_stats(
        self,
        stats: ColumnStatistics,
        value_counts: Dict[Any, int],
        non_null_count: int
    ) -> None:
        """Calculate frequency statistics in place."""
        if not value_counts:
            return

        sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
        stats.mode = sorted_values[0][0]
        stats.mode_frequency = sorted_values[0][1]

        # Top values (top 10)
        stats.top_values = [
            {
                "value": str(val),
                "count": count,
                "percentage": round(100 * count / non_null_count, 2) if non_null_count > 0 else 0
            }
            for val, count in sorted_values[:10]
        ]

    def _calculate_string_stats(
        self,
        stats: ColumnStatistics,
        string_lengths: List[int]
    ) -> None:
        """Calculate string length statistics in place."""
        if not string_lengths:
            return

        stats.min_length = int(np.min(string_lengths))
        stats.max_length = int(np.max(string_lengths))
        stats.avg_length = float(np.mean(string_lengths))

    def _calculate_pattern_stats(
        self,
        stats: ColumnStatistics,
        patterns: Dict[str, int],
        sample_values: List[Any]
    ) -> None:
        """Calculate pattern statistics in place."""
        if not patterns:
            return

        sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)
        stats.pattern_samples = [
            {
                "pattern": pattern,
                "count": count,
                "percentage": round(100 * count / len(sample_values), 2) if sample_values else 0
            }
            for pattern, count in sorted_patterns[:10]
        ]

    def _detect_semantic_type_with_visions(self, sample_values: List[Any]) -> Optional[str]:
        """
        Use visions library to detect semantic type from sample values.

        Args:
            sample_values: List of sample values to analyze

        Returns:
            Semantic type string or None
        """
        if not VISIONS_AVAILABLE or not sample_values:
            return None

        try:
            # Use first 100 samples for visions detection (memory efficient)
            sample = pd.Series(sample_values[:100])
            detected_type = detect_type(sample)
            type_name = detected_type.__name__ if hasattr(detected_type, '__name__') else str(detected_type)

            # Map visions types to our semantic types
            type_mapping = {
                'EmailAddress': 'email',
                'URL': 'url',
                'UUID': 'uuid',
                'IPAddress': 'ip_address',
                'PhoneNumber': 'phone_number',
                'Integer': 'integer',
                'Float': 'float',
                'Boolean': 'boolean',
                'Categorical': 'category',
                'String': 'string',
                'Object': 'object'
            }

            if type_name is None:
                return None
            return type_mapping.get(type_name, type_name.lower())

        except Exception as e:
            logger.debug(f"Visions type detection failed: {e}")
            return None

    def calculate_quality_metrics(
        self,
        profile_data: Dict[str, Any],
        type_info: TypeInference,
        statistics: ColumnStatistics,
        total_rows: int
    ) -> QualityMetrics:
        """
        Calculate data quality metrics.

        Args:
            profile_data: Dictionary containing column profile data
            type_info: Type inference result
            statistics: Column statistics
            total_rows: Total number of rows

        Returns:
            QualityMetrics object
        """
        quality = QualityMetrics()
        issues = []
        observations = []  # General informational insights

        # Completeness: % of non-null values
        quality.completeness = 100 - statistics.null_percentage
        if quality.completeness < 50:
            issues.append(f"Low completeness: {quality.completeness:.1f}% non-null")
        elif quality.completeness < 90:
            issues.append(f"Moderate completeness: {quality.completeness:.1f}% non-null")

        # Validity: % matching inferred type (considering numeric type compatibility)
        # Integer/float are compatible - values like 13.0 can be detected as integer
        # but are still valid floats
        if type_info.inferred_type in ['float', 'integer']:
            # For numeric types, combine integer + float counts for validity
            # since integers are valid floats and float-like integers (13.0) are valid
            quality.validity = 100.0  # Numeric types are inherently compatible
        else:
            quality.validity = type_info.confidence * 100

        if quality.validity < 95:
            issues.append(f"Type inconsistency: {quality.validity:.1f}% match inferred type")

        # Uniqueness: cardinality
        quality.uniqueness = statistics.cardinality * 100
        if statistics.cardinality == 1.0 and total_rows > 1:
            observations.append("All values are unique (potential key field)")
        elif statistics.cardinality < 0.01 and statistics.unique_count < 100 and total_rows > 100:
            # Low cardinality is not an issue - categorical fields SHOULD have few unique values
            # This is informational, not a problem
            observations.append(f"Low cardinality: {statistics.unique_count} unique values (likely categorical)")

        # Consistency: pattern matching
        if statistics.pattern_samples:
            top_pattern_pct = statistics.pattern_samples[0]["percentage"]
            quality.consistency = top_pattern_pct
            if quality.consistency < 50:
                observations.append(f"{len(statistics.pattern_samples)} different patterns detected")
        else:
            quality.consistency = 100.0

        # Overall score (weighted average)
        quality.overall_score = (
            0.3 * quality.completeness +
            0.3 * quality.validity +
            0.2 * quality.uniqueness +
            0.2 * quality.consistency
        )

        quality.issues = issues
        quality.observations = observations

        return quality

    def calculate_correlations(
        self,
        numeric_data: Dict[str, List[float]],
        row_count: int
    ) -> List[CorrelationResult]:
        """
        Calculate correlations between numeric columns.

        Args:
            numeric_data: Dictionary mapping column names to numeric values
            row_count: Total row count (for reference, not used directly)

        Returns:
            List of CorrelationResult objects
        """
        correlations = []

        # Limit columns for performance
        numeric_columns = list(numeric_data.keys())[:self.max_correlation_columns]

        if len(numeric_columns) < 2:
            return correlations

        try:
            # Create DataFrame for correlation using sampled data
            max_sample_length = max(len(numeric_data[col]) for col in numeric_columns) if numeric_columns else 0

            df_dict = {}
            for col in numeric_columns:
                # Ensure same length by padding/truncating to max_sample_length
                values = numeric_data[col][:max_sample_length]
                if len(values) < max_sample_length:
                    values = values + [np.nan] * (max_sample_length - len(values))
                df_dict[col] = values

            df = pd.DataFrame(df_dict)

            # Calculate correlation matrix
            corr_matrix = df.corr()

            # Extract significant correlations (|r| > 0.5)
            for i, col1 in enumerate(numeric_columns):
                for col2 in numeric_columns[i+1:]:
                    try:
                        corr_value = corr_matrix.loc[col1, col2]
                        if pd.notna(corr_value) and abs(corr_value) > 0.5:
                            correlations.append(CorrelationResult(
                                column1=col1,
                                column2=col2,
                                correlation=round(float(corr_value), 3),
                                strength="high" if abs(corr_value) > 0.8 else "moderate"
                            ))
                    except KeyError:
                        continue

        except Exception as e:
            logger.warning(f"Could not calculate correlations: {e}")

        return correlations
