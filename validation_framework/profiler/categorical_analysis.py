"""
Categorical and mixed-type association analysis for data profiling.

Provides analysis methods for relationships involving categorical variables:
- Cramér's V for categorical×categorical associations
- Point-biserial correlation for binary×numeric associations
- Binary target column detection
- Missing data pattern analysis (MCAR vs MNAR detection)
"""

import re
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple, Set
import logging

try:
    from scipy.stats import chi2_contingency, pointbiserialr, spearmanr
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("scipy not available - categorical analysis limited")

logger = logging.getLogger(__name__)


class CategoricalAnalyzer:
    """
    Analyzer for categorical and mixed-type column associations.

    Computes association measures between categorical columns and between
    categorical and numeric columns to surface relationships that Pearson
    correlation cannot detect.
    """

    def __init__(
        self,
        min_association_threshold: float = 0.2,
        max_categories_for_analysis: int = 20,
        binary_detection_threshold: float = 0.95
    ):
        """
        Initialize categorical analyzer.

        Args:
            min_association_threshold: Minimum association strength to report (default: 0.2)
            max_categories_for_analysis: Max unique values to consider categorical (default: 20)
            binary_detection_threshold: Min proportion of binary values to detect as binary (default: 0.95)
        """
        self.min_association_threshold = min_association_threshold
        self.max_categories_for_analysis = max_categories_for_analysis
        self.binary_detection_threshold = binary_detection_threshold

    def analyze_categorical_associations(
        self,
        df: pd.DataFrame,
        column_types: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Analyze associations involving categorical columns.

        Args:
            df: DataFrame with the data
            column_types: Dict mapping column name to inferred type

        Returns:
            Dict with categorical analysis results including:
                - cramers_v_associations: Categorical×categorical associations
                - point_biserial_associations: Binary×numeric associations
                - target_columns: Detected likely target/label columns
                - missing_patterns: Missing data pattern analysis
        """
        result = {
            "available": True,
            "cramers_v_associations": [],
            "point_biserial_associations": [],
            "missing_patterns": [],
            "categorical_columns": [],
            "binary_columns": [],
            "potential_outcomes": []
        }

        if not SCIPY_AVAILABLE:
            result["available"] = False
            result["reason"] = "scipy not available"
            return result

        # Identify column categories
        categorical_cols = self._identify_categorical_columns(df, column_types)
        binary_cols = self._identify_binary_columns(df)
        numeric_cols = self._identify_numeric_columns(df, column_types)

        result["categorical_columns"] = categorical_cols
        result["binary_columns"] = binary_cols

        logger.debug(f"Found {len(categorical_cols)} categorical, {len(binary_cols)} binary, {len(numeric_cols)} numeric columns")

        # Calculate Cramér's V for categorical pairs
        if len(categorical_cols) >= 2:
            result["cramers_v_associations"] = self._calculate_cramers_v(
                df, categorical_cols
            )

        # Calculate point-biserial for binary×numeric pairs
        if binary_cols and numeric_cols:
            result["point_biserial_associations"] = self._calculate_point_biserial(
                df, binary_cols, numeric_cols
            )

        # Analyze missing data patterns
        result["missing_patterns"] = self._analyze_missing_patterns(df)

        # Detect potential outcome columns by name pattern (past-tense verbs)
        # Pass point-biserial associations to score by correlated features
        result["potential_outcomes"] = self._detect_potential_outcomes(
            df, binary_cols, result.get("point_biserial_associations", [])
        )

        return result

    def _identify_categorical_columns(
        self,
        df: pd.DataFrame,
        column_types: Dict[str, str]
    ) -> List[str]:
        """Identify columns that are categorical (low cardinality or string type)."""
        categorical = []

        for col in df.columns:
            col_type = column_types.get(col, "unknown")
            nunique = df[col].nunique()

            # Consider categorical if:
            # 1. Explicitly string type with low cardinality
            # 2. Numeric with very low cardinality (likely encoded categories)
            # 3. Object dtype
            if col_type in ("string", "object") and nunique <= self.max_categories_for_analysis:
                categorical.append(col)
            elif nunique <= 10 and nunique > 1:  # Low cardinality numeric = likely categorical
                categorical.append(col)
            elif df[col].dtype == 'object':
                categorical.append(col)

        return categorical

    def _identify_binary_columns(self, df: pd.DataFrame) -> List[str]:
        """Identify binary columns (exactly 2 unique non-null values)."""
        binary = []

        for col in df.columns:
            non_null = df[col].dropna()
            unique_vals = non_null.unique()

            if len(unique_vals) == 2:
                # Check if values are binary-like (0/1, yes/no, true/false)
                binary.append(col)
            elif len(unique_vals) <= 3:
                # Could be binary with one rare value
                # Check if dominant two values cover most data
                value_counts = non_null.value_counts()
                if len(value_counts) >= 2:
                    top_two_pct = value_counts.iloc[:2].sum() / len(non_null)
                    if top_two_pct >= self.binary_detection_threshold:
                        binary.append(col)

        return binary

    def _detect_potential_outcomes(
        self,
        df: pd.DataFrame,
        binary_cols: List[str],
        point_biserial_associations: List[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Detect potential outcome/target columns using linguistic patterns.

        Looks for past-tense verb patterns (-ed, -en endings) in column names,
        which often indicate outcome variables (e.g., Survived, Passed, Chosen).
        Only considers binary columns as candidates.

        Ranks by ML target potential based on:
        - Class balance (closer to 50/50 is better for ML)
        - Number of correlated features (more correlations = more predictive signal)
        - Completeness (fewer nulls = better)
        """
        # Pattern: word ending in -ed or -en (past tense/past participle)
        past_tense_pattern = re.compile(r'(?i)\b\w+(ed|en)\b')

        potential_outcomes = []

        for col in binary_cols:
            match = past_tense_pattern.search(col)
            if match:
                # Get column stats
                non_null = df[col].dropna()
                unique_vals = list(non_null.unique())
                value_counts = non_null.value_counts()

                # Calculate ML target score (0-100)
                score = 0
                score_factors = []

                # Factor 1: Class balance (max 40 points)
                # Perfect balance (50/50) = 40 points, highly imbalanced = 0
                if len(value_counts) >= 2:
                    minority_ratio = value_counts.iloc[-1] / len(non_null)
                    balance_score = min(minority_ratio, 1 - minority_ratio) * 2  # 0 to 1 scale
                    balance_points = balance_score * 40
                    score += balance_points
                    balance_pct = round(minority_ratio * 100, 1)
                    score_factors.append(f"Class balance: {balance_pct}% minority")

                # Factor 2: Correlated features (max 40 points)
                # More point-biserial correlations = more predictive features
                correlated_count = 0
                if point_biserial_associations:
                    for assoc in point_biserial_associations:
                        if assoc.get('binary_column') == col and assoc.get('is_significant'):
                            correlated_count += 1
                # Cap at 10 correlations for max points
                correlation_points = min(correlated_count / 10, 1) * 40
                score += correlation_points
                if correlated_count > 0:
                    score_factors.append(f"{correlated_count} correlated feature{'s' if correlated_count > 1 else ''}")

                # Factor 3: Completeness (max 20 points)
                completeness = len(non_null) / len(df)
                completeness_points = completeness * 20
                score += completeness_points
                if completeness < 1.0:
                    score_factors.append(f"{round(completeness * 100, 1)}% complete")

                potential_outcomes.append({
                    "column": col,
                    "reason": "past_tense_verb",
                    "pattern_matched": match.group(),
                    "unique_values": unique_vals[:5],
                    "value_counts": value_counts.head(5).to_dict(),
                    "ml_target_score": round(score, 1),
                    "score_factors": score_factors,
                    "correlated_features": correlated_count,
                    "class_balance": round(min(value_counts.iloc[-1] / len(non_null),
                                               value_counts.iloc[0] / len(non_null)) * 100, 1) if len(value_counts) >= 2 else 0,
                    "interpretation": f"'{col}' matches past-tense pattern '{match.group()}'"
                })

        # Sort by ML target score (highest first)
        potential_outcomes.sort(key=lambda x: x['ml_target_score'], reverse=True)

        return potential_outcomes

    def _identify_numeric_columns(
        self,
        df: pd.DataFrame,
        column_types: Dict[str, str]
    ) -> List[str]:
        """Identify numeric columns suitable for correlation."""
        numeric = []

        for col in df.columns:
            if df[col].dtype in ('int64', 'float64', 'int32', 'float32'):
                # Exclude very low cardinality (likely categorical codes)
                if df[col].nunique() > 10:
                    numeric.append(col)
            elif column_types.get(col) in ("integer", "float", "decimal"):
                numeric.append(col)

        return numeric

    def _calculate_cramers_v(
        self,
        df: pd.DataFrame,
        categorical_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Calculate Cramér's V for categorical column pairs.

        Cramér's V measures association between categorical variables,
        ranging from 0 (no association) to 1 (perfect association).
        """
        associations = []

        for i, col1 in enumerate(categorical_cols):
            for col2 in categorical_cols[i+1:]:
                try:
                    # Create contingency table
                    contingency = pd.crosstab(df[col1], df[col2])

                    if contingency.size < 4:  # Need at least 2x2
                        continue

                    # Calculate chi-squared
                    chi2, p_value, dof, expected = chi2_contingency(contingency)

                    # Calculate Cramér's V
                    n = contingency.sum().sum()
                    min_dim = min(contingency.shape[0] - 1, contingency.shape[1] - 1)

                    if min_dim == 0 or n == 0:
                        continue

                    cramers_v = np.sqrt(chi2 / (n * min_dim))

                    if cramers_v >= self.min_association_threshold:
                        associations.append({
                            "column1": col1,
                            "column2": col2,
                            "method": "cramers_v",
                            "association": round(float(cramers_v), 4),
                            "p_value": round(float(p_value), 6),
                            "is_significant": p_value < 0.05,
                            "strength": self._classify_association_strength(cramers_v),
                            "interpretation": self._interpret_cramers_v(col1, col2, cramers_v)
                        })

                except Exception as e:
                    logger.debug(f"Could not calculate Cramér's V for {col1}×{col2}: {e}")

        # Sort by association strength
        associations.sort(key=lambda x: x["association"], reverse=True)
        return associations

    def _calculate_point_biserial(
        self,
        df: pd.DataFrame,
        binary_cols: List[str],
        numeric_cols: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Calculate point-biserial correlation for binary×numeric pairs.

        Point-biserial is equivalent to Pearson correlation when one
        variable is dichotomous (binary).
        """
        associations = []

        for binary_col in binary_cols:
            for numeric_col in numeric_cols:
                if binary_col == numeric_col:
                    continue

                try:
                    # Get complete cases only
                    mask = df[binary_col].notna() & df[numeric_col].notna()

                    if mask.sum() < 10:  # Need sufficient data
                        continue

                    binary_vals = df.loc[mask, binary_col]
                    numeric_vals = df.loc[mask, numeric_col]

                    # Convert binary to numeric if needed
                    if binary_vals.dtype == 'object':
                        unique_vals = binary_vals.unique()
                        if len(unique_vals) == 2:
                            binary_vals = (binary_vals == unique_vals[0]).astype(int)
                        else:
                            continue

                    # Ensure binary is 0/1
                    unique_binary = set(binary_vals.unique())
                    if not unique_binary <= {0, 1, 0.0, 1.0}:
                        # Map to 0/1
                        min_val, max_val = binary_vals.min(), binary_vals.max()
                        binary_vals = ((binary_vals - min_val) / (max_val - min_val)).astype(int)

                    # Calculate point-biserial correlation
                    correlation, p_value = pointbiserialr(binary_vals, numeric_vals)

                    if abs(correlation) >= self.min_association_threshold:
                        associations.append({
                            "binary_column": binary_col,
                            "numeric_column": numeric_col,
                            "method": "point_biserial",
                            "correlation": round(float(correlation), 4),
                            "p_value": round(float(p_value), 6),
                            "is_significant": p_value < 0.05,
                            "strength": self._classify_association_strength(abs(correlation)),
                            "direction": "positive" if correlation > 0 else "negative",
                            "interpretation": self._interpret_point_biserial(
                                binary_col, numeric_col, correlation
                            ),
                            "n_observations": int(mask.sum())
                        })

                except Exception as e:
                    logger.debug(f"Could not calculate point-biserial for {binary_col}×{numeric_col}: {e}")

        # Sort by absolute correlation
        associations.sort(key=lambda x: abs(x["correlation"]), reverse=True)
        return associations

    def _analyze_missing_patterns(
        self,
        df: pd.DataFrame
    ) -> List[Dict[str, Any]]:
        """
        Analyze missing data patterns to detect systematic missingness.

        Identifies:
        - MCAR (Missing Completely At Random): No pattern
        - MAR (Missing At Random): Missingness related to other observed values
        - MNAR (Missing Not At Random): Missingness related to the missing value itself
        """
        patterns = []

        # Get columns with missing data
        missing_cols = [col for col in df.columns if df[col].isna().any()]

        if not missing_cols:
            return patterns

        for missing_col in missing_cols:
            missing_rate = df[missing_col].isna().mean()

            if missing_rate < 0.01 or missing_rate > 0.99:
                continue  # Too little or too much missing to analyze

            pattern_info = {
                "column": missing_col,
                "missing_rate": round(missing_rate * 100, 2),
                "correlations_with_missingness": [],
                "likely_mechanism": "MCAR",
                "confidence": 0.5
            }

            # Create missingness indicator
            missing_indicator = df[missing_col].isna().astype(int)

            # Check correlation of missingness with other columns
            for other_col in df.columns:
                if other_col == missing_col:
                    continue

                try:
                    other_vals = df[other_col]

                    # For numeric columns, use Spearman correlation
                    if other_vals.dtype in ('int64', 'float64'):
                        mask = other_vals.notna()
                        if mask.sum() < 10:
                            continue

                        corr, p_val = spearmanr(
                            missing_indicator[mask],
                            other_vals[mask]
                        )

                        if abs(corr) >= 0.15 and p_val < 0.05:
                            pattern_info["correlations_with_missingness"].append({
                                "column": other_col,
                                "correlation": round(float(corr), 4),
                                "p_value": round(float(p_val), 6)
                            })

                    # For categorical columns, check if missingness differs by group
                    elif other_vals.dtype == 'object' or other_vals.nunique() <= 10:
                        # Chi-squared test for independence
                        contingency = pd.crosstab(missing_indicator, other_vals)
                        if contingency.size >= 4:
                            chi2, p_val, _, _ = chi2_contingency(contingency)

                            if p_val < 0.05:
                                pattern_info["correlations_with_missingness"].append({
                                    "column": other_col,
                                    "chi_squared": round(float(chi2), 2),
                                    "p_value": round(float(p_val), 6),
                                    "association_type": "categorical"
                                })

                except Exception as e:
                    logger.debug(f"Could not analyze missingness pattern for {missing_col}×{other_col}: {e}")

            # Determine likely mechanism
            if pattern_info["correlations_with_missingness"]:
                # Missingness is related to other variables -> MAR
                pattern_info["likely_mechanism"] = "MAR"
                pattern_info["confidence"] = 0.7
                pattern_info["interpretation"] = (
                    f"Missingness in '{missing_col}' appears related to: "
                    f"{', '.join(c['column'] for c in pattern_info['correlations_with_missingness'][:3])}"
                )
            else:
                pattern_info["interpretation"] = (
                    f"No systematic pattern detected in '{missing_col}' missingness"
                )

            if pattern_info["correlations_with_missingness"]:
                patterns.append(pattern_info)

        # Sort by missing rate
        patterns.sort(key=lambda x: x["missing_rate"], reverse=True)
        return patterns

    def _classify_association_strength(self, value: float) -> str:
        """Classify association/correlation strength."""
        abs_val = abs(value)
        if abs_val >= 0.5:
            return "strong"
        elif abs_val >= 0.3:
            return "moderate"
        elif abs_val >= 0.1:
            return "weak"
        else:
            return "negligible"

    def _interpret_cramers_v(self, col1: str, col2: str, v: float) -> str:
        """Generate plain-language interpretation of Cramér's V."""
        strength = self._classify_association_strength(v)
        return f"{strength.capitalize()} association between '{col1}' and '{col2}'"

    def _interpret_point_biserial(
        self,
        binary_col: str,
        numeric_col: str,
        correlation: float
    ) -> str:
        """Generate plain-language interpretation of point-biserial correlation."""
        strength = self._classify_association_strength(abs(correlation))
        direction = "higher" if correlation > 0 else "lower"
        return (
            f"{strength.capitalize()} relationship: when '{binary_col}' is true/1, "
            f"'{numeric_col}' tends to be {direction}"
        )
