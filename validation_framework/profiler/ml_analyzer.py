#!/usr/bin/env python3
"""
ML-based Data Quality Analyzer (Beta)

Provides machine learning-based analysis for detecting:
- Outliers using Isolation Forest
- Format inconsistencies in string fields
- Cross-column consistency issues
- Rare/suspicious categorical values
- Temporal pattern anomalies

This is a beta feature - enable with --beta-ml flag.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter
import re
import logging
import time

logger = logging.getLogger(__name__)

# Max sample size for ML analysis (balance between accuracy and speed)
ML_SAMPLE_SIZE = 250_000


class MLAnalyzer:
    """
    Machine learning-based data quality analyzer.

    Runs multiple ML techniques on sampled data to detect anomalies
    and data quality issues that traditional profiling might miss.
    """

    def __init__(self, sample_size: int = ML_SAMPLE_SIZE):
        """
        Initialize ML analyzer.

        Args:
            sample_size: Maximum rows to sample for ML analysis
        """
        self.sample_size = sample_size
        self.results: Dict[str, Any] = {}

    def analyze(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Run ML analysis on dataframe.

        Args:
            df: DataFrame to analyze (will be sampled if too large)

        Returns:
            Dictionary containing ML findings
        """
        start_time = time.time()

        # Sample if needed
        original_rows = len(df)
        if len(df) > self.sample_size:
            df = df.sample(n=self.sample_size, random_state=42)
            sampled = True
        else:
            sampled = False

        logger.info(f"ML Analysis: Analyzing {len(df):,} rows (sampled: {sampled})")

        findings = {
            "sample_info": {
                "original_rows": original_rows,
                "analyzed_rows": len(df),
                "sampled": sampled,
                "sample_percentage": round(len(df) / original_rows * 100, 2) if original_rows > 0 else 100
            },
            "numeric_outliers": {},
            "format_anomalies": {},
            "rare_categories": {},
            "cross_column_issues": [],
            "temporal_patterns": {},
            "summary": {
                "total_issues": 0,
                "severity": "low",
                "key_findings": []
            }
        }

        # Analyze numeric columns for outliers
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        for col in numeric_cols:
            outlier_result = self._detect_numeric_outliers(df[col], col)
            if outlier_result and outlier_result.get("anomaly_count", 0) > 0:
                findings["numeric_outliers"][col] = outlier_result

        # Analyze string columns for format patterns
        string_cols = df.select_dtypes(include=['object']).columns.tolist()
        for col in string_cols:
            format_result = self._analyze_format_patterns(df[col], col)
            if format_result and format_result.get("anomaly_count", 0) > 0:
                findings["format_anomalies"][col] = format_result

        # Detect rare categories in low-cardinality columns
        for col in string_cols:
            unique_count = df[col].nunique()
            if 2 <= unique_count <= 100:  # Categorical range
                rare_result = self._detect_rare_categories(df[col], col)
                if rare_result and rare_result.get("rare_values"):
                    findings["rare_categories"][col] = rare_result

        # Cross-column consistency (for columns that look related)
        cross_col_issues = self._check_cross_column_consistency(df)
        findings["cross_column_issues"] = cross_col_issues

        # Temporal patterns (if datetime columns exist)
        datetime_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        # Also check object columns that might be datetime strings
        for col in string_cols:
            if self._looks_like_datetime(df[col]):
                temporal_result = self._analyze_temporal_patterns(df[col], col)
                if temporal_result:
                    findings["temporal_patterns"][col] = temporal_result

        # Generate summary
        findings["summary"] = self._generate_summary(findings)
        findings["analysis_time_seconds"] = round(time.time() - start_time, 2)

        return findings

    def _detect_numeric_outliers(self, series: pd.Series, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Detect outliers in numeric column using Isolation Forest.
        """
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            logger.warning("scikit-learn not available, skipping Isolation Forest analysis")
            return None

        values = series.dropna().values
        if len(values) < 100:  # Not enough data
            return None

        try:
            # Use Isolation Forest with low contamination
            iso = IsolationForest(
                contamination=0.001,  # Expect 0.1% outliers
                random_state=42,
                n_jobs=-1
            )
            predictions = iso.fit_predict(values.reshape(-1, 1))
            anomaly_indices = np.where(predictions == -1)[0]
            anomaly_values = values[anomaly_indices]

            if len(anomaly_values) == 0:
                return None

            # Calculate statistics
            return {
                "method": "isolation_forest",
                "anomaly_count": len(anomaly_values),
                "anomaly_percentage": round(len(anomaly_values) / len(values) * 100, 4),
                "normal_range": {
                    "min": float(np.percentile(values, 1)),
                    "max": float(np.percentile(values, 99))
                },
                "anomaly_range": {
                    "min": float(anomaly_values.min()),
                    "max": float(anomaly_values.max())
                },
                "top_anomalies": sorted([float(v) for v in anomaly_values])[-5:],
                "interpretation": self._interpret_numeric_outliers(col_name, anomaly_values, values)
            }
        except Exception as e:
            logger.debug(f"Error in outlier detection for {col_name}: {e}")
            return None

    def _interpret_numeric_outliers(self, col_name: str, anomalies: np.ndarray, all_values: np.ndarray) -> str:
        """Generate human-readable interpretation of outliers."""
        max_anomaly = anomalies.max()
        median_val = np.median(all_values)

        # Handle edge cases where median is 0 or very small
        if median_val == 0 or abs(median_val) < 1e-10:
            return f"Found {len(anomalies)} outliers - max value: {max_anomaly:,.2f}"

        ratio = max_anomaly / median_val
        if ratio > 100:
            return f"Found {len(anomalies)} extreme outliers - largest is {ratio:.0f}x the median value"
        elif ratio > 10:
            return f"Found {len(anomalies)} significant outliers - values up to {ratio:.0f}x the median"
        else:
            return f"Found {len(anomalies)} potential outliers outside the typical range"

    def _analyze_format_patterns(self, series: pd.Series, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Analyze string column for format consistency.
        """
        values = series.dropna().astype(str)
        if len(values) < 50:
            return None

        # Extract format patterns
        patterns = values.apply(self._extract_format_pattern)
        pattern_counts = Counter(patterns)

        # Get dominant pattern
        if not pattern_counts:
            return None

        dominant_pattern, dominant_count = pattern_counts.most_common(1)[0]
        dominant_pct = dominant_count / len(values) * 100

        # Only report if there's a clear dominant pattern with anomalies
        if dominant_pct < 90:  # No clear dominant pattern
            return None

        anomaly_patterns = [(p, c) for p, c in pattern_counts.items() if p != dominant_pattern]
        if not anomaly_patterns:
            return None

        anomaly_count = sum(c for _, c in anomaly_patterns)

        # Get sample anomaly values
        anomaly_mask = patterns != dominant_pattern
        sample_anomalies = values[anomaly_mask].head(10).tolist()

        return {
            "dominant_pattern": dominant_pattern,
            "dominant_percentage": round(dominant_pct, 2),
            "anomaly_count": anomaly_count,
            "anomaly_percentage": round(anomaly_count / len(values) * 100, 4),
            "anomaly_patterns": dict(anomaly_patterns[:5]),
            "sample_anomalies": sample_anomalies,
            "interpretation": f"Format inconsistency: {anomaly_count:,} values ({anomaly_count/len(values)*100:.3f}%) don't match the dominant '{dominant_pattern}' pattern"
        }

    def _extract_format_pattern(self, value: str) -> str:
        """Convert value to format pattern (A=alpha, 9=digit, _=space)."""
        if not value or value.lower() == 'nan':
            return "NULL"
        pattern = ""
        for c in str(value):
            if c.isalpha():
                pattern += "A"
            elif c.isdigit():
                pattern += "9"
            elif c.isspace():
                pattern += "_"
            else:
                pattern += c
        # Compress repeating characters
        compressed = re.sub(r'(.)\1+', lambda m: m.group(1) + str(len(m.group(0))), pattern)
        return compressed

    def _detect_rare_categories(self, series: pd.Series, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Detect suspiciously rare categorical values.
        """
        freq = series.value_counts()
        total = len(series.dropna())

        if total < 100:
            return None

        # Find values that appear less than 0.1% of the time
        threshold = 0.001
        rare_values = []
        for val, count in freq.items():
            pct = count / total
            if pct < threshold:
                rare_values.append({
                    "value": str(val)[:50],  # Truncate long values
                    "count": int(count),
                    "percentage": round(pct * 100, 4)
                })

        if not rare_values:
            return None

        return {
            "threshold_percentage": threshold * 100,
            "rare_values": rare_values[:10],  # Top 10 rare values
            "total_rare_count": sum(v["count"] for v in rare_values),
            "interpretation": f"Found {len(rare_values)} rare values appearing in <0.1% of records - may indicate typos or data errors"
        }

    def _check_cross_column_consistency(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Check for cross-column consistency issues.
        """
        issues = []

        # Look for amount/value column pairs
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        amount_cols = [c for c in numeric_cols if any(word in c.lower() for word in ['amount', 'value', 'price', 'paid', 'received', 'total'])]

        # Check ratios between related columns
        for i, col1 in enumerate(amount_cols):
            for col2 in amount_cols[i+1:]:
                # Check if columns might be related (e.g., paid/received)
                if self._columns_might_be_related(col1, col2):
                    ratio_issue = self._check_amount_ratio(df, col1, col2)
                    if ratio_issue:
                        issues.append(ratio_issue)

        return issues

    def _columns_might_be_related(self, col1: str, col2: str) -> bool:
        """Check if two columns might be related (e.g., amount paid/received)."""
        related_pairs = [
            ('paid', 'received'),
            ('debit', 'credit'),
            ('in', 'out'),
            ('from', 'to'),
            ('sent', 'received')
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
            if valid_mask.sum() < 100:
                return None

            ratios = df.loc[valid_mask, col2] / df.loc[valid_mask, col1]

            # Check for extreme ratios (>10x or <0.1x)
            extreme_high = (ratios > 10).sum()
            extreme_low = (ratios < 0.1).sum()

            if extreme_high + extreme_low < 10:
                return None

            return {
                "columns": [col1, col2],
                "issue_type": "extreme_ratio",
                "extreme_high_count": int(extreme_high),
                "extreme_low_count": int(extreme_low),
                "total_issues": int(extreme_high + extreme_low),
                "median_ratio": float(ratios.median()),
                "interpretation": f"Found {extreme_high + extreme_low:,} records with extreme ratios between {col1} and {col2}"
            }
        except Exception:
            return None

    def _looks_like_datetime(self, series: pd.Series) -> bool:
        """Check if a string column looks like datetime values."""
        sample = series.dropna().head(100)
        if len(sample) < 10:
            return False

        # Check for common datetime patterns
        datetime_patterns = [
            r'\d{4}[-/]\d{2}[-/]\d{2}',  # YYYY-MM-DD or YYYY/MM/DD
            r'\d{2}[-/]\d{2}[-/]\d{4}',  # DD-MM-YYYY or MM-DD-YYYY
            r'\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}',  # With time
        ]

        matches = 0
        for val in sample:
            val_str = str(val)
            for pattern in datetime_patterns:
                if re.search(pattern, val_str):
                    matches += 1
                    break

        return matches / len(sample) > 0.8

    def _analyze_temporal_patterns(self, series: pd.Series, col_name: str) -> Optional[Dict[str, Any]]:
        """
        Analyze temporal patterns in datetime-like column.
        """
        try:
            # Try to parse as datetime
            if series.dtype == 'object':
                # Try common formats
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
            if len(valid_dates) < 100:
                return None

            # Analyze hour distribution
            hours = valid_dates.dt.hour
            hour_dist = hours.value_counts().sort_index()

            # Check for suspicious patterns (e.g., all at midnight)
            midnight_pct = (hours == 0).sum() / len(hours) * 100

            findings = {
                "date_range": {
                    "min": str(valid_dates.min()),
                    "max": str(valid_dates.max())
                },
                "hour_distribution": {str(h): int(c) for h, c in hour_dist.head(5).items()}
            }

            # Flag if suspiciously concentrated at certain times
            if midnight_pct > 90:
                findings["warning"] = "suspicious_midnight_concentration"
                findings["midnight_percentage"] = round(midnight_pct, 2)
                findings["interpretation"] = f"{midnight_pct:.1f}% of timestamps are at midnight - may indicate missing time component or data generation artifact"
            else:
                most_common_hour = hour_dist.idxmax()
                most_common_pct = hour_dist.max() / len(hours) * 100
                if most_common_pct > 50:
                    findings["warning"] = "concentrated_time"
                    findings["interpretation"] = f"{most_common_pct:.1f}% of transactions at hour {most_common_hour}:00"

            return findings

        except Exception as e:
            logger.debug(f"Error analyzing temporal patterns for {col_name}: {e}")
            return None

    def _generate_summary(self, findings: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate overall summary of ML findings.
        """
        total_issues = 0
        key_findings = []

        # Count outliers
        outlier_count = sum(
            f.get("anomaly_count", 0)
            for f in findings["numeric_outliers"].values()
        )
        if outlier_count > 0:
            total_issues += outlier_count
            # Get the most significant outlier finding
            if findings["numeric_outliers"]:
                worst_col = max(
                    findings["numeric_outliers"].items(),
                    key=lambda x: x[1].get("anomaly_count", 0)
                )
                key_findings.append(f"Numeric outliers: {outlier_count:,} detected (worst: {worst_col[0]})")

        # Count format anomalies
        format_count = sum(
            f.get("anomaly_count", 0)
            for f in findings["format_anomalies"].values()
        )
        if format_count > 0:
            total_issues += format_count
            key_findings.append(f"Format inconsistencies: {format_count:,} values don't match expected patterns")

        # Count rare categories
        rare_count = sum(
            f.get("total_rare_count", 0)
            for f in findings["rare_categories"].values()
        )
        if rare_count > 0:
            total_issues += rare_count
            key_findings.append(f"Rare values: {rare_count:,} potentially suspicious categorical values")

        # Count cross-column issues
        cross_col_count = sum(
            issue.get("total_issues", 0)
            for issue in findings["cross_column_issues"]
        )
        if cross_col_count > 0:
            total_issues += cross_col_count
            key_findings.append(f"Cross-column issues: {cross_col_count:,} records with unexpected ratios")

        # Check temporal warnings
        temporal_warnings = [
            f for f in findings["temporal_patterns"].values()
            if f.get("warning")
        ]
        if temporal_warnings:
            key_findings.append(f"Temporal anomalies: {len(temporal_warnings)} columns with suspicious time patterns")

        # Determine severity
        if total_issues > 1000:
            severity = "high"
        elif total_issues > 100:
            severity = "medium"
        elif total_issues > 0:
            severity = "low"
        else:
            severity = "none"
            key_findings.append("No significant anomalies detected")

        return {
            "total_issues": total_issues,
            "severity": severity,
            "key_findings": key_findings
        }


def run_ml_analysis(df: pd.DataFrame, sample_size: int = ML_SAMPLE_SIZE) -> Dict[str, Any]:
    """
    Convenience function to run ML analysis on a DataFrame.

    Args:
        df: DataFrame to analyze
        sample_size: Maximum rows to sample

    Returns:
        Dictionary containing ML findings
    """
    analyzer = MLAnalyzer(sample_size=sample_size)
    return analyzer.analyze(df)
