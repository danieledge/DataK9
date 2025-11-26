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

logger = logging.getLogger(__name__)

# Max sample size for ML analysis (balance between accuracy and speed)
ML_SAMPLE_SIZE = 250_000

# Minimum rows required for reliable ML analysis
# Increased from 100 to 500 per data science best practices
MIN_ROWS_FOR_ML = 500


class MLAnalyzer:
    """
    Machine learning-based data quality analyzer.

    Runs multiple ML techniques on sampled data to detect anomalies
    and data quality issues that traditional profiling might miss.

    Supports FIBO-based semantic intelligence for smart rare category detection.
    """

    def __init__(self, sample_size: int = ML_SAMPLE_SIZE):
        """
        Initialize ML analyzer.

        Args:
            sample_size: Maximum rows to sample for ML analysis
        """
        self.sample_size = sample_size
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

        # Get column types
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        string_cols = df.select_dtypes(include=['object']).columns.tolist()

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
        for col in actual_numeric_cols:
            outlier_result = self._detect_numeric_outliers(df, col)
            if outlier_result and outlier_result.get("anomaly_count", 0) > 0:
                findings["numeric_outliers"][col] = outlier_result

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

        # 9. Benford's Law analysis (only on FIBO money-related columns)
        for col in numeric_cols:
            if self._should_apply_benford(col):
                benford_result = self._detect_benford_anomalies(df, col)
                if benford_result and benford_result.get("is_suspicious"):
                    findings["benford_analysis"][col] = benford_result

        # 10. Autoencoder anomaly detection (on true numeric columns)
        if len(actual_numeric_cols) >= 2 and self._sklearn_available:
            autoencoder_result = self._detect_autoencoder_anomalies(df, actual_numeric_cols)
            if autoencoder_result:
                findings["autoencoder_anomalies"] = autoencoder_result

        # Generate summary
        findings["summary"] = self._generate_summary(findings)
        findings["analysis_time_seconds"] = round(time.time() - start_time, 2)

        return findings

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

        # Limit columns
        if len(valid_cols) > 5:
            variances = df[valid_cols].var()
            valid_cols = variances.nlargest(5).index.tolist()

        try:
            subset_df = df[valid_cols].dropna()
            if len(subset_df) < MIN_ROWS_FOR_ML:
                return None

            # Standardize
            scaler = StandardScaler()
            scaled_data = scaler.fit_transform(subset_df)

            # Estimate eps using k-nearest neighbors with elbow method
            k = min(max(5, len(subset_df) // 50), 20)  # Adaptive k
            nn = NearestNeighbors(n_neighbors=k)
            nn.fit(scaled_data)
            distances, _ = nn.kneighbors(scaled_data)

            # Use k-th neighbor distance, sorted, and find elbow
            k_distances = np.sort(distances[:, k-1])

            # Use a more adaptive eps - aim for ~5-15% as noise maximum
            # Start with 95th percentile of k-distances
            eps = np.percentile(k_distances, 95)

            # Adaptive min_samples based on dataset size
            min_samples = max(5, min(len(subset_df) // 200, 50))

            # Run DBSCAN
            dbscan = DBSCAN(eps=eps, min_samples=min_samples)
            labels = dbscan.fit_predict(scaled_data)

            # If too much noise (>50%), try with larger eps
            noise_ratio = (labels == -1).sum() / len(labels)
            if noise_ratio > 0.5:
                eps = np.percentile(k_distances, 99)
                dbscan = DBSCAN(eps=eps, min_samples=min_samples)
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

            ratios = df.loc[valid_mask, col2] / df.loc[valid_mask, col1]

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

            # Get sample rows
            extreme_mask = extreme_high_mask | extreme_low_mask
            valid_df = df[valid_mask].reset_index(drop=True)
            extreme_indices = extreme_mask[extreme_mask].index[:5]
            sample_rows = []
            for idx in extreme_indices:
                if idx < len(valid_df):
                    row = valid_df.iloc[idx]
                    row_dict = {str(k)[:25]: str(v)[:50] for k, v in row.items()}
                    sample_rows.append(row_dict)

            return {
                "columns": [col1, col2],
                "issue_type": "extreme_ratio",
                "extreme_high_count": int(extreme_high),
                "extreme_low_count": int(extreme_low),
                "total_issues": int(extreme_high + extreme_low),
                "median_ratio": float(median_ratio),
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

                # Flag extreme residuals
                residual_threshold = np.percentile(residuals, 99)
                anomaly_mask = residuals > residual_threshold
                anomaly_count = anomaly_mask.sum()

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

    def _should_apply_benford(self, col_name: str) -> bool:
        """
        Check if Benford's Law should be applied to this column.

        Only applies to FIBO money-related columns (amounts, prices, values)
        NOT to identifiers, counts, or categorical data stored as numbers.

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

        # Check if tagged as money-related
        if primary_tag in benford_tags:
            return True

        # Fallback: check column name patterns for amount-like columns
        col_lower = col_name.lower()
        amount_keywords = ['amount', 'price', 'value', 'total', 'sum', 'paid', 'received',
                          'balance', 'fee', 'cost', 'revenue', 'sales', 'income']

        # Exclude keywords that suggest identifiers
        exclude_keywords = ['id', 'code', 'number', 'count', 'qty', 'quantity', 'bank', 'account']

        if any(kw in col_lower for kw in exclude_keywords):
            return False

        if any(kw in col_lower for kw in amount_keywords):
            return True

        return False

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
        series = df[col_name].dropna()

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
        # Critical values: 15.51 (p=0.05), 20.09 (p=0.01)
        is_suspicious = chi_square > 15.51
        confidence = "Very High" if chi_square > 26.12 else "High" if chi_square > 20.09 else "Medium" if chi_square > 15.51 else "Low"

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

        return {
            "method": "benford_law",
            "sample_size": total,
            "chi_square": round(chi_square, 2),
            "mean_absolute_deviation": round(mad, 2),
            "is_suspicious": is_suspicious,
            "confidence": confidence,
            "digit_distribution": deviations,
            "worst_deviations": [
                {"digit": d, **dev} for d, dev in worst_deviations
            ],
            "interpretation": interpretation,
            "plain_english": (
                "Benford's Law says naturally occurring numbers (like financial transactions) "
                "follow a predictable pattern - '1' appears as the first digit about 30% of the time, "
                "'2' about 17%, and so on. When data doesn't follow this pattern, it often means "
                "the numbers were made up, manipulated, or generated incorrectly."
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

            return {
                "method": "autoencoder",
                "architecture": f"Input({n_features}) -> {hidden_layers} -> Output({n_features})",
                "rows_analyzed": len(subset_df),
                "columns_analyzed": numeric_cols,
                "anomaly_count": int(anomaly_count),
                "anomaly_percentage": round(anomaly_pct, 4),
                "confidence": confidence,
                "threshold": round(threshold, 4),
                "error_stats": {
                    "mean": round(np.mean(reconstruction_errors), 4),
                    "std": round(np.std(reconstruction_errors), 4),
                    "max": round(np.max(reconstruction_errors), 4),
                    "anomaly_min_error": round(reconstruction_errors[anomaly_mask].min(), 4)
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
    sample_size: int = ML_SAMPLE_SIZE,
    column_semantic_info: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Convenience function to run ML analysis on a DataFrame.

    Args:
        df: DataFrame to analyze
        sample_size: Maximum rows to sample
        column_semantic_info: Optional dict mapping column names to their semantic info.
                              Each entry should have 'semantic_tags', 'primary_tag', etc.
                              Used for intelligent rare category detection based on FIBO.

    Returns:
        Dictionary containing ML findings
    """
    analyzer = MLAnalyzer(sample_size=sample_size)
    return analyzer.analyze(df, column_semantic_info=column_semantic_info)
