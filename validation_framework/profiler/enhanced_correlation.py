"""
Enhanced correlation analysis for data profiling.

Provides multi-method correlation analysis including:
- Pearson correlation (linear relationships)
- Spearman rank correlation (monotonic relationships)
- Kendall tau correlation (ordinal data)
- Mutual information (non-linear relationships)
- Correlation significance testing
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import logging

from validation_framework.profiler.backend_aware_base import BackendAwareProfiler

try:
    from scipy.stats import pearsonr, spearmanr, kendalltau
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("scipy not available - enhanced correlation analysis disabled")

try:
    from sklearn.feature_selection import mutual_info_regression
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("scikit-learn not available - mutual information disabled")

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None


logger = logging.getLogger(__name__)


class EnhancedCorrelationAnalyzer(BackendAwareProfiler):
    """
    Multi-method correlation analysis for numeric columns.

    Supports Pearson, Spearman, Kendall, and Mutual Information methods
    with significance testing and interpretation.
    """

    def __init__(
        self,
        max_correlation_columns: int = 20,
        significance_level: float = 0.05,
        min_correlation_threshold: float = 0.3
    ):
        """
        Initialize enhanced correlation analyzer.

        Args:
            max_correlation_columns: Maximum columns to analyze (performance limit)
            significance_level: P-value threshold for significance (default: 0.05)
            min_correlation_threshold: Minimum correlation to report (default: 0.3)
        """
        self.max_correlation_columns = max_correlation_columns
        self.significance_level = significance_level
        self.min_correlation_threshold = min_correlation_threshold

    def calculate_correlations_multi_method(
        self,
        numeric_data: Dict[str, List[float]],
        row_count: int,
        methods: List[str] = ['pearson', 'spearman']
    ) -> Dict[str, Any]:
        """
        Calculate correlations using multiple methods.

        Args:
            numeric_data: Dict of column_name -> list of numeric values
            row_count: Total number of rows
            methods: Correlation methods to use (pearson, spearman, kendall, mutual_info)

        Returns:
            Dict with correlation results including:
                - correlation_pairs: List of significant correlations
                - correlation_matrix: Full correlation matrices by method
                - method_comparison: Comparison of correlation methods
                - heatmap_data: Data for visualization
        """
        if not SCIPY_AVAILABLE:
            logger.warning("scipy not available - falling back to basic correlation")
            return self._fallback_correlation(numeric_data, row_count)

        # Limit columns for performance
        numeric_columns = list(numeric_data.keys())[:self.max_correlation_columns]

        if len(numeric_columns) < 2:
            return {
                "available": False,
                "reason": "Less than 2 numeric columns available"
            }

        logger.info(f"Calculating correlations for {len(numeric_columns)} columns using methods: {methods}")

        # Create DataFrame for correlation
        df_dict = {}
        for col in numeric_columns:
            # Ensure same length by padding/truncating
            values = numeric_data[col][:row_count]
            if len(values) < row_count:
                values.extend([np.nan] * (row_count - len(values)))
            df_dict[col] = values

        df = pd.DataFrame(df_dict)

        # Remove rows with any NaN values for correlation calculation
        df_clean = df.dropna()

        if len(df_clean) < 3:
            return {
                "available": False,
                "reason": "Insufficient non-null overlapping data points"
            }

        result = {
            "available": True,
            "methods_used": methods,
            "columns_analyzed": len(numeric_columns),
            "data_points": len(df_clean),
            "correlation_pairs": [],
            "correlation_matrix": {},
            "method_comparison": []
        }

        # Calculate correlations for each method
        for method in methods:
            if method == 'pearson':
                matrix, pairs = self._calculate_pearson(df_clean, numeric_columns)
            elif method == 'spearman':
                matrix, pairs = self._calculate_spearman(df_clean, numeric_columns)
            elif method == 'kendall':
                matrix, pairs = self._calculate_kendall(df_clean, numeric_columns)
            elif method == 'mutual_info' and SKLEARN_AVAILABLE:
                matrix, pairs = self._calculate_mutual_info(df_clean, numeric_columns)
            else:
                logger.warning(f"Unknown or unavailable method: {method}")
                continue

            result["correlation_matrix"][method] = matrix
            result["correlation_pairs"].extend(pairs)

        # Deduplicate and sort correlation pairs
        result["correlation_pairs"] = self._deduplicate_and_rank_pairs(
            result["correlation_pairs"]
        )

        # Compare methods if multiple used
        if len(methods) > 1:
            result["method_comparison"] = self._compare_methods(
                result["correlation_matrix"],
                numeric_columns
            )

        # Generate heatmap data
        result["heatmap_data"] = self._generate_heatmap_data(
            result["correlation_matrix"],
            numeric_columns
        )

        return result

    def _calculate_pearson(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Calculate Pearson correlation (linear relationships).

        Args:
            df: DataFrame with numeric data
            columns: List of column names

        Returns:
            Tuple of (correlation_matrix, significant_pairs)
        """
        # Calculate correlation matrix
        corr_matrix = df.corr(method='pearson')

        matrix_data = corr_matrix.to_dict()
        pairs = []

        # Extract significant pairs
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i < j:  # Upper triangle only
                    corr_value = corr_matrix.loc[col1, col2]

                    if abs(corr_value) >= self.min_correlation_threshold and not np.isnan(corr_value):
                        # Calculate p-value
                        _, p_value = pearsonr(df[col1], df[col2])

                        pairs.append({
                            "column1": col1,
                            "column2": col2,
                            "method": "pearson",
                            "correlation": float(corr_value),
                            "p_value": float(p_value),
                            "is_significant": p_value < self.significance_level,
                            "strength": self._classify_strength(abs(corr_value)),
                            "direction": "positive" if corr_value > 0 else "negative"
                        })

        return matrix_data, pairs

    def _calculate_spearman(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Calculate Spearman rank correlation (monotonic relationships).

        Args:
            df: DataFrame with numeric data
            columns: List of column names

        Returns:
            Tuple of (correlation_matrix, significant_pairs)
        """
        # Calculate correlation matrix
        corr_matrix = df.corr(method='spearman')

        matrix_data = corr_matrix.to_dict()
        pairs = []

        # Extract significant pairs
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i < j:  # Upper triangle only
                    corr_value = corr_matrix.loc[col1, col2]

                    if abs(corr_value) >= self.min_correlation_threshold and not np.isnan(corr_value):
                        # Calculate p-value
                        _, p_value = spearmanr(df[col1], df[col2])

                        pairs.append({
                            "column1": col1,
                            "column2": col2,
                            "method": "spearman",
                            "correlation": float(corr_value),
                            "p_value": float(p_value),
                            "is_significant": p_value < self.significance_level,
                            "strength": self._classify_strength(abs(corr_value)),
                            "direction": "positive" if corr_value > 0 else "negative"
                        })

        return matrix_data, pairs

    def _calculate_kendall(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Calculate Kendall tau correlation (ordinal data).

        Args:
            df: DataFrame with numeric data
            columns: List of column names

        Returns:
            Tuple of (correlation_matrix, significant_pairs)
        """
        # Kendall correlation matrix (slower, so limit to fewer columns)
        if len(columns) > 10:
            logger.info("Limiting Kendall correlation to first 10 columns (performance)")
            columns = columns[:10]

        corr_matrix = df[columns].corr(method='kendall')

        matrix_data = corr_matrix.to_dict()
        pairs = []

        # Extract significant pairs
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i < j:  # Upper triangle only
                    corr_value = corr_matrix.loc[col1, col2]

                    if abs(corr_value) >= self.min_correlation_threshold and not np.isnan(corr_value):
                        # Calculate p-value
                        _, p_value = kendalltau(df[col1], df[col2])

                        pairs.append({
                            "column1": col1,
                            "column2": col2,
                            "method": "kendall",
                            "correlation": float(corr_value),
                            "p_value": float(p_value),
                            "is_significant": p_value < self.significance_level,
                            "strength": self._classify_strength(abs(corr_value)),
                            "direction": "positive" if corr_value > 0 else "negative"
                        })

        return matrix_data, pairs

    def _calculate_mutual_info(
        self,
        df: pd.DataFrame,
        columns: List[str]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Calculate mutual information (non-linear relationships).

        Args:
            df: DataFrame with numeric data
            columns: List of column names

        Returns:
            Tuple of (correlation_matrix, significant_pairs)
        """
        if not SKLEARN_AVAILABLE:
            return {}, []

        # Limit to fewer columns for performance
        if len(columns) > 10:
            logger.info("Limiting mutual information to first 10 columns (performance)")
            columns = columns[:10]

        # Build mutual information matrix
        mi_matrix = {}
        pairs = []

        for i, col1 in enumerate(columns):
            mi_matrix[col1] = {}

            for j, col2 in enumerate(columns):
                if i == j:
                    mi_matrix[col1][col2] = 1.0  # Self-information
                elif i < j:
                    # Calculate mutual information
                    X = df[col1].values.reshape(-1, 1)
                    y = df[col2].values

                    # Mutual information
                    mi_score = mutual_info_regression(X, y, random_state=42)[0]

                    # Normalize to [0, 1] range (approximate)
                    # MI is unbounded, so we use a heuristic normalization
                    mi_normalized = min(mi_score / 2.0, 1.0)

                    mi_matrix[col1][col2] = float(mi_normalized)
                    mi_matrix[col2] = mi_matrix.get(col2, {})
                    mi_matrix[col2][col1] = float(mi_normalized)

                    if mi_normalized >= self.min_correlation_threshold:
                        pairs.append({
                            "column1": col1,
                            "column2": col2,
                            "method": "mutual_info",
                            "correlation": float(mi_normalized),
                            "p_value": None,  # MI doesn't have p-values
                            "is_significant": True,  # Assume significant if above threshold
                            "strength": self._classify_strength(mi_normalized),
                            "direction": "non-linear"
                        })

        return mi_matrix, pairs

    def _classify_strength(self, abs_correlation: float) -> str:
        """
        Classify correlation strength.

        Args:
            abs_correlation: Absolute value of correlation

        Returns:
            Strength classification (very_strong, strong, moderate, weak)
        """
        if abs_correlation >= 0.9:
            return "very_strong"
        elif abs_correlation >= 0.7:
            return "strong"
        elif abs_correlation >= 0.5:
            return "moderate"
        else:
            return "weak"

    def _deduplicate_and_rank_pairs(
        self,
        pairs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Deduplicate correlation pairs and rank by strength.

        Args:
            pairs: List of correlation pairs

        Returns:
            Deduplicated and sorted list
        """
        # Create unique key for each pair (sorted column names)
        unique_pairs = {}

        for pair in pairs:
            col1, col2 = sorted([pair["column1"], pair["column2"]])
            key = f"{col1}|{col2}|{pair['method']}"

            if key not in unique_pairs or abs(pair["correlation"]) > abs(unique_pairs[key]["correlation"]):
                unique_pairs[key] = pair

        # Sort by absolute correlation (descending)
        sorted_pairs = sorted(
            unique_pairs.values(),
            key=lambda x: abs(x["correlation"]),
            reverse=True
        )

        return sorted_pairs

    def _compare_methods(
        self,
        correlation_matrices: Dict[str, Dict[str, Any]],
        columns: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Compare correlation methods to identify best method for each pair.

        Args:
            correlation_matrices: Dict of method -> matrix
            columns: List of column names

        Returns:
            List of method comparisons
        """
        comparisons = []

        # Compare each column pair across methods
        for i, col1 in enumerate(columns):
            for j, col2 in enumerate(columns):
                if i < j:  # Upper triangle only
                    comparison = {
                        "column1": col1,
                        "column2": col2,
                        "correlations": {}
                    }

                    max_abs_corr = 0
                    recommended_method = None

                    for method, matrix in correlation_matrices.items():
                        if col1 in matrix and col2 in matrix[col1]:
                            corr_value = matrix[col1][col2]
                            comparison["correlations"][method] = float(corr_value)

                            if abs(corr_value) > max_abs_corr:
                                max_abs_corr = abs(corr_value)
                                recommended_method = method

                    # Only include if at least one method shows correlation
                    if max_abs_corr >= self.min_correlation_threshold:
                        comparison["recommended_method"] = recommended_method
                        comparison["max_correlation"] = max_abs_corr

                        # Interpret differences between methods
                        comparison["interpretation"] = self._interpret_method_differences(
                            comparison["correlations"]
                        )

                        comparisons.append(comparison)

        return comparisons

    def _interpret_method_differences(
        self,
        correlations: Dict[str, float]
    ) -> str:
        """
        Interpret differences between correlation methods.

        Args:
            correlations: Dict of method -> correlation value

        Returns:
            Plain-language interpretation
        """
        pearson = correlations.get("pearson", 0)
        spearman = correlations.get("spearman", 0)
        kendall = correlations.get("kendall", 0)

        # Compare Pearson vs Spearman
        if abs(pearson) > 0.1 and abs(spearman) > 0.1:
            diff = abs(abs(pearson) - abs(spearman))

            if diff > 0.2:
                if abs(spearman) > abs(pearson):
                    return "Strong monotonic relationship with possible non-linear components (Spearman > Pearson)"
                else:
                    return "Strong linear relationship (Pearson > Spearman)"
            else:
                return "Linear relationship (Pearson ≈ Spearman)"

        return "Weak or no correlation detected"

    def _generate_heatmap_data(
        self,
        correlation_matrices: Dict[str, Dict[str, Any]],
        columns: List[str]
    ) -> Dict[str, Any]:
        """
        Generate data for correlation heatmap visualization.

        Args:
            correlation_matrices: Dict of method -> matrix
            columns: List of column names

        Returns:
            Heatmap data structure
        """
        heatmap_data = {
            "columns": columns,
            "matrices": {}
        }

        for method, matrix in correlation_matrices.items():
            # Convert to 2D array for visualization
            heatmap_matrix = []
            for col1 in columns:
                row = []
                for col2 in columns:
                    if col1 in matrix and col2 in matrix[col1]:
                        row.append(float(matrix[col1][col2]))
                    else:
                        row.append(0.0)
                heatmap_matrix.append(row)

            heatmap_data["matrices"][method] = heatmap_matrix

        return heatmap_data

    def _fallback_correlation(
        self,
        numeric_data: Dict[str, List[float]],
        row_count: int
    ) -> Dict[str, Any]:
        """
        Fallback to basic numpy correlation if scipy unavailable.

        Args:
            numeric_data: Dict of column_name -> list of numeric values
            row_count: Total number of rows

        Returns:
            Basic correlation results
        """
        numeric_columns = list(numeric_data.keys())[:self.max_correlation_columns]

        if len(numeric_columns) < 2:
            return {
                "available": False,
                "reason": "Less than 2 numeric columns available"
            }

        # Create DataFrame
        df_dict = {}
        for col in numeric_columns:
            values = numeric_data[col][:row_count]
            if len(values) < row_count:
                values.extend([np.nan] * (row_count - len(values)))
            df_dict[col] = values

        df = pd.DataFrame(df_dict)

        # Basic Pearson correlation
        corr_matrix = df.corr()

        pairs = []
        for i, col1 in enumerate(numeric_columns):
            for j, col2 in enumerate(numeric_columns):
                if i < j:
                    corr_value = corr_matrix.loc[col1, col2]

                    if abs(corr_value) >= self.min_correlation_threshold and not np.isnan(corr_value):
                        pairs.append({
                            "column1": col1,
                            "column2": col2,
                            "method": "pearson",
                            "correlation": float(corr_value),
                            "p_value": None,
                            "is_significant": None,
                            "strength": self._classify_strength(abs(corr_value)),
                            "direction": "positive" if corr_value > 0 else "negative"
                        })

        return {
            "available": True,
            "methods_used": ["pearson"],
            "correlation_pairs": sorted(pairs, key=lambda x: abs(x["correlation"]), reverse=True),
            "note": "Limited to basic Pearson correlation (scipy not available)"
        }
