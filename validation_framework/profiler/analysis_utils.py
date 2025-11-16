"""
Advanced analysis utilities for data profiling.

Provides specialized analysis methods for:
- Distribution analysis and outlier detection
- Anomaly detection using multiple methods
- Temporal pattern analysis
- Enhanced pattern and semantic type detection
- Functional dependency discovery
"""

import numpy as np
import pandas as pd
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from collections import Counter
import logging

from validation_framework.profiler.profile_result import (
    DistributionMetrics, AnomalyInfo, TemporalMetrics, PatternInfo, DependencyInfo
)

# Import advanced statistical analysis if available
try:
    from validation_framework.profiler.statistical_analysis import (
        StatisticalTests, DistributionFitter, EnhancedCorrelation, MLAnomalyDetection
    )
    ADVANCED_STATS_AVAILABLE = True
except ImportError:
    ADVANCED_STATS_AVAILABLE = False
    logging.warning("Advanced statistical analysis not available - install scipy, statsmodels, scikit-learn")

logger = logging.getLogger(__name__)


class DistributionAnalyzer:
    """Analyzes statistical distributions and detects outliers."""

    @staticmethod
    def analyze(numeric_values: List[float], total_count: int, enable_advanced_stats: bool = True) -> DistributionMetrics:
        """
        Perform comprehensive distribution analysis on numeric data.

        Args:
            numeric_values: List of numeric values
            total_count: Total count including nulls
            enable_advanced_stats: Whether to run advanced statistical tests (normality, distribution fitting)

        Returns:
            DistributionMetrics with distribution analysis
        """
        if not numeric_values or len(numeric_values) < 3:
            return DistributionMetrics()

        try:
            arr = np.array(numeric_values, dtype=np.float64)

            # Calculate distribution characteristics
            skewness = float(pd.Series(arr).skew())
            kurtosis = float(pd.Series(arr).kurtosis())

            # Determine distribution type based on skewness and kurtosis
            distribution_type = DistributionAnalyzer._classify_distribution(skewness, kurtosis)

            # Check if normal distribution (simplified test)
            is_normal = abs(skewness) < 0.5 and abs(kurtosis - 3) < 1.0

            # Calculate percentiles
            p1, p5, p95, p99 = np.percentile(arr, [1, 5, 95, 99])

            # Detect outliers using IQR method
            q1, q3 = np.percentile(arr, [25, 75])
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers_iqr = arr[(arr < lower_bound) | (arr > upper_bound)]

            # Detect outliers using Z-score method (|z| > 3)
            mean = np.mean(arr)
            std = np.std(arr)
            if std > 0:
                z_scores = np.abs((arr - mean) / std)
                outliers_zscore = arr[z_scores > 3]
            else:
                outliers_zscore = np.array([])

            # Combine outliers (union)
            all_outliers = np.unique(np.concatenate([outliers_iqr, outliers_zscore]))
            outlier_count = len(all_outliers)
            outlier_percentage = 100.0 * outlier_count / len(arr) if len(arr) > 0 else 0.0

            # Advanced statistical tests (if scipy available)
            normality_tests = None
            fitted_distributions = []
            best_fit_distribution = None
            theoretical_percentiles = None

            if enable_advanced_stats and ADVANCED_STATS_AVAILABLE and len(numeric_values) >= 10:
                try:
                    # Perform rigorous normality tests
                    normality_tests = StatisticalTests.test_normality(numeric_values)

                    # Override is_normal with consensus from statistical tests
                    if normality_tests and normality_tests.get('available'):
                        consensus = normality_tests.get('consensus', {})
                        if consensus:
                            is_normal = consensus.get('is_normal', is_normal)

                    # Fit distributions and rank by AIC
                    fitted_distributions = DistributionFitter.fit_distributions(numeric_values, top_n=3)

                    # Get best fit distribution
                    if fitted_distributions:
                        best_fit_distribution = fitted_distributions[0]

                        # Calculate theoretical percentiles from best fit
                        theoretical_percentiles = DistributionFitter.get_theoretical_percentiles(
                            best_fit_distribution,
                            percentiles=[1, 5, 25, 50, 75, 95, 99]
                        )

                except Exception as e:
                    logger.warning(f"Advanced statistical tests failed: {e}")

            return DistributionMetrics(
                distribution_type=distribution_type,
                skewness=skewness,
                kurtosis=kurtosis,
                is_normal=is_normal,
                outliers_iqr=outliers_iqr.tolist()[:100],  # Limit samples
                outliers_zscore=outliers_zscore.tolist()[:100],
                outlier_count=outlier_count,
                outlier_percentage=outlier_percentage,
                percentile_95=float(p95),
                percentile_99=float(p99),
                percentile_1=float(p1),
                percentile_5=float(p5),
                normality_tests=normality_tests,
                fitted_distributions=fitted_distributions,
                best_fit_distribution=best_fit_distribution,
                theoretical_percentiles=theoretical_percentiles
            )

        except Exception as e:
            logger.warning(f"Distribution analysis failed: {e}")
            return DistributionMetrics()

    @staticmethod
    def _classify_distribution(skewness: float, kurtosis: float) -> str:
        """
        Classify distribution type based on skewness and kurtosis.

        Args:
            skewness: Measure of asymmetry
            kurtosis: Measure of tailedness (excess kurtosis, normal = 3)

        Returns:
            Distribution type string
        """
        # Normal distribution: skew ≈ 0, kurtosis ≈ 3
        if abs(skewness) < 0.5 and abs(kurtosis - 3) < 1.0:
            return "normal"

        # Uniform distribution: kurtosis < 3
        if kurtosis < 1.8:
            return "uniform"

        # Right-skewed (positive skew)
        if skewness > 1.0:
            return "right_skewed"

        # Left-skewed (negative skew)
        if skewness < -1.0:
            return "left_skewed"

        # Heavy-tailed (leptokurtic)
        if kurtosis > 5:
            return "heavy_tailed"

        # Light-tailed (platykurtic)
        if kurtosis < 2:
            return "light_tailed"

        # Moderately skewed
        if abs(skewness) > 0.5:
            return "moderately_skewed"

        return "unknown"


class AnomalyDetector:
    """Detects anomalies using multiple statistical methods."""

    @staticmethod
    def detect(
        column_data: Dict[str, Any],
        inferred_type: str,
        statistics: Any
    ) -> AnomalyInfo:
        """
        Detect anomalies in column data using multiple methods.

        Args:
            column_data: Column profile data dict
            inferred_type: Inferred data type
            statistics: Column statistics object

        Returns:
            AnomalyInfo with anomaly detection results
        """
        anomalies = []
        methods = []
        details = []

        # Numeric anomalies (using distribution analysis from sampled data)
        if inferred_type in ["integer", "float"]:
            # Get sampled values from reservoir sampler
            numeric_sampler = column_data.get("numeric_sampler")
            numeric_values = numeric_sampler.get_sample() if numeric_sampler else []

            if len(numeric_values) > 10:
                numeric_anomalies, numeric_details = AnomalyDetector._detect_numeric_anomalies(
                    numeric_values
                )
                if numeric_anomalies:
                    anomalies.extend(numeric_anomalies)
                    details.extend(numeric_details)
                    methods.append("statistical_outlier")

        # String length anomalies from sampled data
        if inferred_type == "string":
            # Get sampled string lengths from reservoir sampler
            string_length_sampler = column_data.get("string_length_sampler")
            string_lengths = string_length_sampler.get_sample() if string_length_sampler else []

            if len(string_lengths) > 10:
                length_anomalies, length_details = AnomalyDetector._detect_length_anomalies(
                    string_lengths, column_data.get("sample_values", [])
                )
                if length_anomalies:
                    anomalies.extend(length_anomalies)
                    details.extend(length_details)
                    methods.append("length_outlier")

        # Type inconsistency anomalies
        type_conflicts = column_data.get("type_counts", {})
        if len(type_conflicts) > 1:
            conflict_anomalies = AnomalyDetector._detect_type_conflicts(
                type_conflicts, column_data.get("sample_values", [])
            )
            if conflict_anomalies:
                anomalies.extend(conflict_anomalies)
                methods.append("type_inconsistency")

        # Pattern violations
        patterns = column_data.get("patterns", {})
        if patterns and len(patterns) > 1:
            pattern_anomalies = AnomalyDetector._detect_pattern_violations(
                patterns, column_data.get("sample_values", [])
            )
            if pattern_anomalies:
                anomalies.extend(pattern_anomalies)
                methods.append("pattern_violation")

        # Build result
        anomaly_count = len(set(anomalies))  # Deduplicate
        total_values = column_data.get("total_processed", 1)
        anomaly_percentage = 100.0 * anomaly_count / total_values if total_values > 0 else 0.0

        return AnomalyInfo(
            has_anomalies=anomaly_count > 0,
            anomaly_count=anomaly_count,
            anomaly_percentage=anomaly_percentage,
            anomaly_methods=methods,
            anomaly_samples=list(set(anomalies))[:50],  # Sample
            anomaly_details=details[:50]
        )

    @staticmethod
    def _detect_numeric_anomalies(
        values: List[float]
    ) -> Tuple[List[float], List[Dict[str, Any]]]:
        """Detect numeric anomalies using modified Z-score."""
        if len(values) < 10:
            return [], []

        arr = np.array(values, dtype=np.float64)
        median = np.median(arr)
        mad = np.median(np.abs(arr - median))  # Median Absolute Deviation

        if mad == 0:
            return [], []

        # Modified Z-score
        modified_z_scores = 0.6745 * (arr - median) / mad
        anomaly_mask = np.abs(modified_z_scores) > 3.5

        anomalies = arr[anomaly_mask].tolist()
        details = [
            {
                "value": float(val),
                "modified_z_score": float(modified_z_scores[i]),
                "method": "modified_z_score"
            }
            for i, val in enumerate(arr[anomaly_mask][:20])
        ]

        return anomalies, details

    @staticmethod
    def _detect_length_anomalies(
        lengths: List[int],
        sample_values: List[Any]
    ) -> Tuple[List[Any], List[Dict[str, Any]]]:
        """Detect string length anomalies."""
        if len(lengths) < 10:
            return [], []

        arr = np.array(lengths, dtype=np.float64)
        q1, q3 = np.percentile(arr, [25, 75])
        iqr = q3 - q1
        lower = q1 - 3 * iqr  # More aggressive threshold
        upper = q3 + 3 * iqr

        # Find anomalous lengths
        anomaly_indices = np.where((arr < lower) | (arr > upper))[0]

        anomalies = []
        details = []

        for idx in anomaly_indices[:20]:
            if idx < len(sample_values):
                val = sample_values[idx]
                anomalies.append(val)
                details.append({
                    "value": str(val),
                    "length": int(lengths[idx]),
                    "expected_range": f"{int(lower)}-{int(upper)}",
                    "method": "length_outlier"
                })

        return anomalies, details

    @staticmethod
    def _detect_type_conflicts(
        type_counts: Dict[str, int],
        sample_values: List[Any]
    ) -> List[Any]:
        """Detect values with unexpected types."""
        if not type_counts:
            return []

        # Find minority types (< 5% of data)
        total = sum(type_counts.values())
        minority_types = {
            typ for typ, count in type_counts.items()
            if count < total * 0.05
        }

        # This is a simplified detection - in practice, we'd need access to which values
        # have which types, which would require refactoring the profiler
        # For now, just flag that conflicts exist
        return []

    @staticmethod
    def _detect_pattern_violations(
        patterns: Dict[str, int],
        sample_values: List[Any]
    ) -> List[Any]:
        """Detect values that violate dominant pattern."""
        if not patterns:
            return []

        # Find dominant pattern (>50% of data)
        total = sum(patterns.values())
        dominant_pattern = max(patterns.items(), key=lambda x: x[1])

        if dominant_pattern[1] < total * 0.5:
            return []  # No clearly dominant pattern

        # Values not matching dominant pattern are potential anomalies
        # This is simplified - would need better pattern matching
        return []


class TemporalAnalyzer:
    """Analyzes temporal patterns in date/time columns."""

    @staticmethod
    def analyze(
        column_data: Dict[str, Any],
        sample_values: List[Any]
    ) -> TemporalMetrics:
        """
        Analyze temporal patterns in date column.

        Args:
            column_data: Column profile data
            sample_values: Sample date values

        Returns:
            TemporalMetrics with temporal analysis
        """
        if not sample_values:
            return TemporalMetrics()

        try:
            # Try to parse dates from samples
            dates = TemporalAnalyzer._parse_dates(sample_values)

            if len(dates) < 2:
                return TemporalMetrics()

            dates_sorted = sorted(dates)
            earliest = dates_sorted[0]
            latest = dates_sorted[-1]

            # Calculate date range
            date_range_days = (latest - earliest).days

            # Detect gaps in sequence
            gap_info = TemporalAnalyzer._detect_gaps(dates_sorted)

            # Check freshness (within last 30 days)
            days_since_latest = (datetime.now() - latest).days
            is_fresh = days_since_latest <= 30

            # Detect future dates
            future_dates = [d for d in dates if d > datetime.now()]
            has_future_dates = len(future_dates) > 0

            # Detect temporal pattern
            pattern = TemporalAnalyzer._detect_pattern(dates_sorted)

            # Calculate average interval
            if len(dates_sorted) > 1:
                intervals = [
                    (dates_sorted[i+1] - dates_sorted[i]).days
                    for i in range(len(dates_sorted)-1)
                ]
                avg_interval = np.mean(intervals) if intervals else None
            else:
                avg_interval = None

            return TemporalMetrics(
                earliest_date=earliest.strftime("%Y-%m-%d"),
                latest_date=latest.strftime("%Y-%m-%d"),
                date_range_days=date_range_days,
                has_gaps=gap_info["has_gaps"],
                gap_count=gap_info["gap_count"],
                largest_gap_days=gap_info["largest_gap"],
                is_fresh=is_fresh,
                days_since_latest=days_since_latest,
                has_future_dates=has_future_dates,
                future_date_count=len(future_dates),
                temporal_pattern=pattern,
                avg_interval_days=avg_interval
            )

        except Exception as e:
            logger.warning(f"Temporal analysis failed: {e}")
            return TemporalMetrics()

    @staticmethod
    def _parse_dates(values: List[Any]) -> List[datetime]:
        """Parse date strings to datetime objects."""
        dates = []
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S"
        ]

        for val in values:
            if pd.isna(val):
                continue

            val_str = str(val).strip()

            # Try each format
            for fmt in formats:
                try:
                    dt = datetime.strptime(val_str, fmt)
                    dates.append(dt)
                    break
                except ValueError:
                    continue

        return dates

    @staticmethod
    def _detect_gaps(dates_sorted: List[datetime]) -> Dict[str, Any]:
        """Detect gaps in date sequence."""
        if len(dates_sorted) < 2:
            return {"has_gaps": False, "gap_count": 0, "largest_gap": None}

        # Calculate intervals
        intervals = [
            (dates_sorted[i+1] - dates_sorted[i]).days
            for i in range(len(dates_sorted)-1)
        ]

        # Estimate expected interval (median)
        expected_interval = np.median(intervals)

        # Gaps are intervals > 2x expected interval
        threshold = max(expected_interval * 2, 7)  # At least 7 days
        gaps = [interval for interval in intervals if interval > threshold]

        return {
            "has_gaps": len(gaps) > 0,
            "gap_count": len(gaps),
            "largest_gap": max(gaps) if gaps else None
        }

    @staticmethod
    def _detect_pattern(dates_sorted: List[datetime]) -> str:
        """Detect temporal pattern (daily, weekly, monthly, etc.)."""
        if len(dates_sorted) < 3:
            return "insufficient_data"

        # Calculate intervals
        intervals = [
            (dates_sorted[i+1] - dates_sorted[i]).days
            for i in range(len(dates_sorted)-1)
        ]

        avg_interval = np.mean(intervals)
        std_interval = np.std(intervals)

        # Classify pattern based on average interval
        if avg_interval < 2 and std_interval < 1:
            return "daily"
        elif 6 <= avg_interval <= 8 and std_interval < 2:
            return "weekly"
        elif 28 <= avg_interval <= 32 and std_interval < 5:
            return "monthly"
        elif 88 <= avg_interval <= 93 and std_interval < 10:
            return "quarterly"
        elif 360 <= avg_interval <= 370 and std_interval < 20:
            return "yearly"
        elif std_interval < avg_interval * 0.1:
            return "regular"
        else:
            return "irregular"


class PatternDetector:
    """Detects semantic types and PII in string data."""

    # Pre-compiled regex patterns for better performance
    # Note: Patterns are ordered from most specific to least specific
    _COMPILED_PATTERNS = {
        "email": re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', re.IGNORECASE),
        "ssn": re.compile(r'^\d{3}-\d{2}-\d{4}$'),
        "credit_card": re.compile(r'^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$'),
        "uuid": re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', re.IGNORECASE),
        "phone_us": re.compile(r'^\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})$'),
        "zipcode_us": re.compile(r'^\d{5}(-\d{4})?$'),
        "url": re.compile(r'^https?://[^\s]+$', re.IGNORECASE),
        "ipv4": re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
        "currency": re.compile(r'^\$?\d+(\.\d{2})?$'),
        "phone_intl": re.compile(r'^\+\d{1,3}[-.\s]\(?\d{1,4}\)?[-.\s]\d{1,4}[-.\s]\d{4,9}$'),
    }

    # PII patterns (subset of _COMPILED_PATTERNS)
    PII_TYPES = {"email", "phone_us", "phone_intl", "ssn", "credit_card"}

    # Column name patterns that are unlikely to contain PII (skip PII detection)
    UNLIKELY_PII_COLUMNS = ['id', 'amount', 'count', 'total', 'quantity', 'price', 'date',
                             'time', 'year', 'month', 'day', 'score', 'rating', 'value']

    @staticmethod
    def detect(sample_values: List[Any], patterns: Dict[str, int], column_name: str = "") -> PatternInfo:
        """
        Detect semantic types and PII in string data.

        Args:
            sample_values: Sample string values
            patterns: Pattern frequency dict
            column_name: Column name (used to avoid PII false positives)

        Returns:
            PatternInfo with detection results
        """
        if not sample_values:
            return PatternInfo()

        # Test each semantic type pattern using pre-compiled regex
        semantic_matches = {}
        for semantic_type, compiled_pattern in PatternDetector._COMPILED_PATTERNS.items():
            match_count = sum(
                1 for val in sample_values
                if compiled_pattern.match(str(val))
            )
            if match_count > 0:
                semantic_matches[semantic_type] = match_count

        # Find best match (highest percentage)
        if semantic_matches:
            total = len(sample_values)
            best_match = max(semantic_matches.items(), key=lambda x: x[1])
            semantic_type = best_match[0]
            confidence = 100.0 * best_match[1] / total

            # Require minimum 30% match to reduce false positives
            if confidence < 30.0:
                logger.debug(f"Pattern {semantic_type} confidence {confidence:.1f}% below threshold for {column_name}")
                semantic_type = None
                confidence = 0.0
        else:
            semantic_type = None
            confidence = 0.0

        # Check for PII - but skip for unlikely columns
        pii_detected = False
        pii_types = []

        if semantic_type in PatternDetector.PII_TYPES:
            # Check if column name suggests it's unlikely to be PII
            col_lower = column_name.lower()
            is_unlikely_pii = any(term in col_lower for term in PatternDetector.UNLIKELY_PII_COLUMNS)

            if is_unlikely_pii:
                logger.debug(f"Skipping PII detection for {column_name} - column name suggests non-PII data")
            else:
                pii_detected = True
                pii_types = [semantic_type]

        # Generate regex pattern from dominant pattern
        regex_pattern = None
        if patterns:
            dominant_pattern = max(patterns.items(), key=lambda x: x[1])[0]
            regex_pattern = PatternDetector._convert_pattern_to_regex(dominant_pattern)

        # Get format examples
        format_examples = list(patterns.keys())[:5] if patterns else []

        return PatternInfo(
            semantic_type=semantic_type,
            semantic_confidence=confidence,
            regex_pattern=regex_pattern,
            format_examples=format_examples,
            pii_detected=pii_detected,
            pii_types=pii_types
        )

    @staticmethod
    def _convert_pattern_to_regex(pattern: str) -> str:
        """
        Convert pattern template (AAA-999) to regex pattern.

        Args:
            pattern: Pattern string with A=letter, 9=digit

        Returns:
            Regex pattern string
        """
        regex_parts = []
        i = 0
        while i < len(pattern):
            char = pattern[i]
            if char == 'A':
                # Count consecutive A's
                count = 1
                while i + count < len(pattern) and pattern[i + count] == 'A':
                    count += 1
                regex_parts.append(f'[a-zA-Z]{{{count}}}')
                i += count
            elif char == '9':
                # Count consecutive 9's
                count = 1
                while i + count < len(pattern) and pattern[i + count] == '9':
                    count += 1
                regex_parts.append(f'\\d{{{count}}}')
                i += count
            else:
                # Literal character - escape if special
                if char in r'\.^$*+?{}[]()|\-':
                    regex_parts.append('\\' + char)
                else:
                    regex_parts.append(char)
                i += 1

        return '^' + ''.join(regex_parts) + '$'


class DependencyDiscoverer:
    """Discovers functional dependencies between columns."""

    @staticmethod
    def discover(
        columns: List[Any],
        data_sample: pd.DataFrame
    ) -> Dict[str, DependencyInfo]:
        """
        Discover functional dependencies between columns.

        Args:
            columns: List of column profiles
            data_sample: Sample DataFrame for dependency analysis

        Returns:
            Dict mapping column names to DependencyInfo
        """
        if data_sample is None or len(data_sample) < 10:
            return {}

        dependencies = {}

        try:
            # For each column, check if it's determined by other columns
            for col in data_sample.columns:
                dep_info = DependencyDiscoverer._analyze_column_dependencies(
                    col, data_sample
                )
                if dep_info.depends_on or dep_info.determines:
                    dependencies[col] = dep_info

        except Exception as e:
            logger.warning(f"Dependency discovery failed: {e}")

        return dependencies

    @staticmethod
    def _analyze_column_dependencies(
        target_col: str,
        df: pd.DataFrame
    ) -> DependencyInfo:
        """
        Analyze dependencies for a single column.

        Args:
            target_col: Target column name
            df: DataFrame with sample data

        Returns:
            DependencyInfo for the column
        """
        depends_on = []
        determines = []
        dependency_strength = {}

        other_cols = [c for c in df.columns if c != target_col]

        for source_col in other_cols:
            # Check if source_col -> target_col (source determines target)
            strength = DependencyDiscoverer._calculate_dependency_strength(
                df, source_col, target_col
            )

            if strength > 98:  # Strong dependency (increased from 95 to reduce false positives)
                depends_on.append(source_col)
                dependency_strength[source_col] = strength

            # Check reverse: target_col -> source_col
            reverse_strength = DependencyDiscoverer._calculate_dependency_strength(
                df, target_col, source_col
            )

            if reverse_strength > 98:  # Increased from 95 to reduce false positives
                determines.append(source_col)
                dependency_strength[f"{target_col}->{source_col}"] = reverse_strength

        return DependencyInfo(
            depends_on=depends_on,
            determines=determines,
            dependency_strength=dependency_strength
        )

    @staticmethod
    def _calculate_dependency_strength(
        df: pd.DataFrame,
        source_col: str,
        target_col: str
    ) -> float:
        """
        Calculate functional dependency strength: source -> target.

        Returns percentage (0-100) indicating how well source determines target.
        100 = perfect functional dependency (each source value maps to exactly one target value)

        Applies cardinality and sample size checks to avoid false positives from coincidental
        1:1 mappings in small samples.

        Args:
            df: DataFrame
            source_col: Source column name
            target_col: Target column name

        Returns:
            Dependency strength (0-100)
        """
        try:
            # Check minimum sample size
            if len(df) < 100:
                logger.debug(f"Small sample size ({len(df)}) may produce unreliable dependencies")

            # Cardinality check: if source is too unique, it's likely not a determining factor
            source_unique = df[source_col].nunique()
            target_unique = df[target_col].nunique()

            if source_unique > target_unique * 0.8:
                # Source is too unique to be a determining factor
                logger.debug(f"{source_col} too unique ({source_unique}/{target_unique}) for dependency")
                return 0.0

            # Minimum occurrence threshold: source values must repeat to establish dependency
            value_counts = df[source_col].value_counts()
            frequent_values = value_counts[value_counts >= 5].index

            if len(frequent_values) < 3:
                # Not enough repeating values to establish dependency
                logger.debug(f"{source_col} has insufficient repeating values for dependency")
                return 0.0

            # Filter to only rows with frequently occurring source values
            filtered_df = df[df[source_col].isin(frequent_values)]

            if len(filtered_df) == 0:
                return 0.0

            # Group by source, count unique target values
            grouped = filtered_df.groupby(source_col)[target_col].nunique()

            # Perfect dependency: all groups have exactly 1 unique target value
            perfect_groups = (grouped == 1).sum()
            total_groups = len(grouped)

            if total_groups == 0:
                return 0.0

            strength = 100.0 * perfect_groups / total_groups

            # Penalize small sample sizes
            if len(df) < 100:
                strength *= 0.7  # 30% confidence reduction for small samples

            return strength

        except Exception as e:
            logger.warning(f"Dependency calculation failed for {source_col}->{target_col}: {e}")
            return 0.0
