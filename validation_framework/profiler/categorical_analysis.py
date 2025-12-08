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

from .semantic_config import get_semantic_config

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
        binary_detection_threshold: float = 0.95,
        max_columns_for_pairwise: int = 50,
        column_profiles: List = None
    ):
        """
        Initialize categorical analyzer.

        Args:
            min_association_threshold: Minimum association strength to report (default: 0.2)
            max_categories_for_analysis: Max unique values to consider categorical (default: 20)
            binary_detection_threshold: Min proportion of binary values to detect as binary (default: 0.95)
            max_columns_for_pairwise: Max columns to include in pairwise analysis (default: 50)
                                      Prevents memory explosion with wide datasets (1000+ columns)
            column_profiles: Optional list of ColumnProfile objects with semantic_info for ID detection
        """
        self.min_association_threshold = min_association_threshold
        self.max_categories_for_analysis = max_categories_for_analysis
        self.binary_detection_threshold = binary_detection_threshold
        self.max_columns_for_pairwise = max_columns_for_pairwise

        # Build semantic cache for identifier detection
        self._semantic_cache = {}
        if column_profiles:
            for col_profile in column_profiles:
                if hasattr(col_profile, 'name') and hasattr(col_profile, 'semantic_info'):
                    self._semantic_cache[col_profile.name] = col_profile.semantic_info

    def _is_identifier_column(self, col: str, df: pd.DataFrame = None) -> bool:
        """
        Check if column is an identifier based on semantic metadata and config-driven patterns.

        Identifier columns should be excluded from association analysis as they
        represent structural relationships (join keys) not predictive signals.

        Uses the semantic detection system's classification first, then falls back
        to config-driven token matching from semantic_config.yaml.
        """
        # Check semantic cache first (most authoritative)
        if col in self._semantic_cache:
            semantic_info = self._semantic_cache.get(col)
            if semantic_info:
                resolved = semantic_info.get('resolved', {})
                primary_type = (resolved.get('primary_type') or '').lower()
                if 'identifier' in primary_type or 'id' in primary_type:
                    return True

                # Check FIBO tags
                fibo = semantic_info.get('fibo', {})
                if fibo:
                    fibo_type = (fibo.get('type') or '').lower()
                    if 'identifier' in fibo_type or 'account' in fibo_type:
                        # Use config to verify it's actually an identifier pattern
                        config = get_semantic_config()
                        token_match = config.check_name_tokens('identifier', col)
                        if token_match is True:
                            return True

                # Check semantic tags
                tags = semantic_info.get('semantic_tags', [])
                for tag in tags:
                    tag_lower = tag.lower() if isinstance(tag, str) else ''
                    if 'identifier' in tag_lower:
                        return True

        # Config-driven fallback: use semantic_config.yaml tokens and patterns
        config = get_semantic_config()

        # Check if column matches negative pattern (should NOT be treated as identifier)
        if config.matches_negative_pattern('identifier', col):
            return False

        # Check name tokens from config
        token_match = config.check_name_tokens('identifier', col)
        if token_match is False:
            # Explicitly excluded by negative token
            return False
        elif token_match is True:
            # Positive token match - verify with cardinality check
            if df is not None and col in df.columns:
                uniqueness = df[col].nunique() / max(len(df), 1)
                # Use config's value hints for threshold (default 0.80, but accept 0.5 for correlation exclusion)
                value_hints = config.get_value_hints('identifier')
                min_unique = value_hints.get('min_unique_ratio', 0.80)
                # Use 50% threshold for correlation exclusion (stricter analysis requires higher uniqueness)
                if uniqueness > 0.5:
                    return True
            else:
                # No data to verify - trust the token match
                return True

        return False

    def set_column_profiles(self, column_profiles: List) -> None:
        """
        Update the semantic cache with column profiles for identifier detection.

        Call this before analyze_categorical_associations when column profiles
        become available after initialization.
        """
        self._semantic_cache = {}
        if column_profiles:
            for col_profile in column_profiles:
                if hasattr(col_profile, 'name') and hasattr(col_profile, 'semantic_info'):
                    self._semantic_cache[col_profile.name] = col_profile.semantic_info

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
            "potential_outcomes": [],
            "limits_applied": []
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
            # Check if limit will be applied
            if len(categorical_cols) > self.max_columns_for_pairwise:
                result["limits_applied"].append({
                    "analysis": "cramers_v",
                    "reason": f"Dataset has {len(categorical_cols)} categorical columns, exceeding the {self.max_columns_for_pairwise} column limit for pairwise analysis",
                    "columns_analyzed": self.max_columns_for_pairwise,
                    "columns_skipped": len(categorical_cols) - self.max_columns_for_pairwise
                })
            result["cramers_v_associations"] = self._calculate_cramers_v(
                df, categorical_cols
            )

        # Calculate point-biserial for binary×numeric pairs
        if binary_cols and numeric_cols:
            result["point_biserial_associations"] = self._calculate_point_biserial(
                df, binary_cols, numeric_cols
            )

        # Analyze missing data patterns
        missing_cols_count = sum(1 for col in df.columns if df[col].isna().any())
        if missing_cols_count > self.max_columns_for_pairwise:
            result["limits_applied"].append({
                "analysis": "missing_patterns",
                "reason": f"Dataset has {missing_cols_count} columns with missing data, exceeding the {self.max_columns_for_pairwise} column limit for pattern analysis",
                "columns_analyzed": self.max_columns_for_pairwise,
                "columns_skipped": missing_cols_count - self.max_columns_for_pairwise
            })
        result["missing_patterns"] = self._analyze_missing_patterns(df)

        # Detect potential outcome columns using multiple strategies:
        # 1. Past-tense verb patterns (e.g., Survived)
        # 2. Keyword matching (e.g., species, diagnosis, class)
        # 3. Suffix patterns (e.g., _class, _target)
        result["potential_outcomes"] = self._detect_potential_outcomes(
            df, binary_cols, result.get("point_biserial_associations", []), categorical_cols
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
        point_biserial_associations: List[Dict] = None,
        categorical_cols: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Detect potential outcome/target columns using multiple detection strategies.

        Detection strategies:
        1. Past-tense verb patterns (-ed, -en endings) for binary columns
        2. Keyword matching for common ML target column names (works with any cardinality)
        3. Suffix patterns (_class, _target, _label, etc.)

        Ranks by ML target potential based on:
        - Class balance (closer to 50/50 is better for ML)
        - Number of correlated features (more correlations = more predictive signal)
        - Completeness (fewer nulls = better)
        - Low cardinality bonus (fewer classes = easier classification)
        """
        # Pattern: word ending in -ed or -en (past tense/past participle)
        past_tense_pattern = re.compile(r'(?i)\b\w+(ed|en)\b')

        # Common ML target keywords (from sklearn, UCI, Kaggle conventions)
        target_keywords = [
            'target', 'label', 'class', 'y', 'output', 'response',
            'outcome', 'result', 'decision', 'prediction', 'predicted',
            'survived', 'default', 'churn', 'churned', 'fraud', 'spam',
            'approved', 'accepted', 'rejected', 'converted',
            'species', 'diagnosis', 'quality', 'digit', 'category',
            'risk', 'creditrisk', 'credit_risk',
        ]

        # Suffix patterns that indicate target columns
        suffix_patterns = ['_class', '_target', '_label', '_outcome', '_category', '_type']

        # Regression target keywords (continuous numeric targets)
        regression_keywords = [
            'price', 'value', 'amount', 'cost', 'revenue', 'income', 'salary',
            'rate', 'score', 'rating', 'count', 'total', 'sum', 'avg', 'mean',
            'progression', 'change', 'growth', 'return', 'profit', 'loss',
            'duration', 'time', 'age', 'weight', 'height', 'distance', 'size',
            'medv', 'median_house_value', 'saleprice', 'sale_price',
        ]

        potential_outcomes = []
        seen_columns = set()

        def _calculate_target_score(col: str, reason: str, match_info: str) -> Dict[str, Any]:
            """Calculate ML target score for a column."""
            non_null = df[col].dropna()
            if len(non_null) == 0:
                return None

            unique_vals = list(non_null.unique())
            value_counts = non_null.value_counts()
            n_classes = len(unique_vals)

            # Calculate ML target score (0-100)
            score = 0
            score_factors = []

            # Factor 1: Class balance (max 30 points for binary, 25 for multi-class)
            if len(value_counts) >= 2:
                minority_ratio = value_counts.iloc[-1] / len(non_null)
                balance_score = min(minority_ratio, 1 - minority_ratio) * 2  # 0 to 1 scale
                max_balance_points = 30 if n_classes == 2 else 25
                balance_points = balance_score * max_balance_points
                score += balance_points
                balance_pct = round(minority_ratio * 100, 1)
                score_factors.append(f"Class balance: {balance_pct}% minority")

            # Factor 2: Correlated features (max 30 points)
            correlated_count = 0
            if point_biserial_associations and col in binary_cols:
                for assoc in point_biserial_associations:
                    if assoc.get('binary_column') == col and assoc.get('is_significant'):
                        correlated_count += 1
            correlation_points = min(correlated_count / 10, 1) * 30
            score += correlation_points
            if correlated_count > 0:
                score_factors.append(f"{correlated_count} correlated feature{'s' if correlated_count > 1 else ''}")

            # Factor 3: Completeness (max 20 points)
            completeness = len(non_null) / len(df)
            completeness_points = completeness * 20
            score += completeness_points
            if completeness < 1.0:
                score_factors.append(f"{round(completeness * 100, 1)}% complete")

            # Factor 4: Low cardinality bonus (max 20 points)
            # Binary = 20, 3-5 classes = 15, 6-10 = 10, 11-20 = 5, >20 = 0
            if n_classes == 2:
                cardinality_points = 20
                score_factors.append("Binary target")
            elif n_classes <= 5:
                cardinality_points = 15
                score_factors.append(f"{n_classes}-class target")
            elif n_classes <= 10:
                cardinality_points = 10
                score_factors.append(f"{n_classes}-class target")
            elif n_classes <= 20:
                cardinality_points = 5
                score_factors.append(f"{n_classes}-class target")
            else:
                cardinality_points = 0
                score_factors.append(f"High cardinality ({n_classes} classes)")
            score += cardinality_points

            return {
                "column": col,
                "reason": reason,
                "pattern_matched": match_info,
                "unique_values": unique_vals[:10],
                "value_counts": value_counts.head(10).to_dict(),
                "ml_target_score": round(score, 1),
                "score_factors": score_factors,
                "correlated_features": correlated_count,
                "n_classes": n_classes,
                "class_balance": round(min(value_counts.iloc[-1] / len(non_null),
                                           value_counts.iloc[0] / len(non_null)) * 100, 1) if len(value_counts) >= 2 else 0,
                "interpretation": f"'{col}' detected as potential target ({reason}: {match_info})"
            }

        # Strategy 1: Past-tense verb patterns (binary columns only)
        for col in binary_cols:
            if col in seen_columns:
                continue
            match = past_tense_pattern.search(col)
            if match:
                result = _calculate_target_score(col, "past_tense_verb", match.group())
                if result:
                    potential_outcomes.append(result)
                    seen_columns.add(col)

        # Strategy 2: Keyword matching (any categorical or low-cardinality column)
        all_candidate_cols = set(binary_cols)
        if categorical_cols:
            all_candidate_cols.update(categorical_cols)

        # Also consider columns with <= 20 unique values
        for col in df.columns:
            if col is None:
                continue
            try:
                if df[col].nunique() <= 20:
                    all_candidate_cols.add(col)
            except Exception:
                pass

        for col in all_candidate_cols:
            if col is None or col in seen_columns:
                continue
            col_lower = str(col).lower().strip()

            # Check keyword match
            for keyword in target_keywords:
                if col_lower == keyword or f'_{keyword}' in col_lower or f'{keyword}_' in col_lower:
                    result = _calculate_target_score(col, "keyword_match", keyword)
                    if result:
                        potential_outcomes.append(result)
                        seen_columns.add(col)
                    break

        # Strategy 3: Suffix pattern matching
        for col in all_candidate_cols:
            if col is None or col in seen_columns:
                continue
            col_lower = str(col).lower().strip()

            for suffix in suffix_patterns:
                if col_lower.endswith(suffix):
                    result = _calculate_target_score(col, "suffix_pattern", suffix)
                    if result:
                        potential_outcomes.append(result)
                        seen_columns.add(col)
                    break

        # Strategy 4: Regression target detection (continuous numeric columns)
        def _calculate_regression_score(col: str, reason: str, match_info: str) -> Dict[str, Any]:
            """Calculate ML regression target score for a numeric column."""
            if df[col].dtype not in ('int64', 'float64', 'int32', 'float32'):
                return None

            non_null = df[col].dropna()
            if len(non_null) == 0:
                return None

            n_unique = non_null.nunique()

            # Must have high cardinality (continuous) to be regression target
            if n_unique < 20:
                return None

            # Calculate regression target score (0-100)
            score = 0
            score_factors = []

            # Factor 1: High cardinality is good for regression (max 25 points)
            cardinality_ratio = n_unique / len(non_null)
            cardinality_points = min(cardinality_ratio * 50, 25)
            score += cardinality_points
            score_factors.append(f"{n_unique} unique values ({round(cardinality_ratio * 100, 1)}% unique)")

            # Factor 2: Distribution characteristics (max 25 points)
            # Check if roughly normal-ish (good for regression)
            try:
                col_std = non_null.std()
                col_mean = non_null.mean()
                if col_std > 0:
                    # Check coefficient of variation (reasonable spread)
                    cv = col_std / abs(col_mean) if col_mean != 0 else 0
                    if 0.1 <= cv <= 2.0:  # Reasonable spread
                        score += 20
                        score_factors.append(f"Good variance (CV={round(cv, 2)})")
                    elif cv > 0:
                        score += 10
                        score_factors.append(f"Some variance (CV={round(cv, 2)})")
            except Exception:
                pass

            # Factor 3: Completeness (max 25 points)
            completeness = len(non_null) / len(df)
            completeness_points = completeness * 25
            score += completeness_points
            if completeness < 1.0:
                score_factors.append(f"{round(completeness * 100, 1)}% complete")

            # Factor 4: Column name match bonus (max 25 points)
            score += 25  # Already matched keyword
            score_factors.append(f"Keyword match: '{match_info}'")

            return {
                "column": col,
                "reason": reason,
                "pattern_matched": match_info,
                "unique_values": [f"min={round(non_null.min(), 2)}", f"max={round(non_null.max(), 2)}", f"mean={round(non_null.mean(), 2)}"],
                "value_counts": {"min": float(non_null.min()), "max": float(non_null.max()), "mean": float(non_null.mean()), "std": float(non_null.std())},
                "ml_target_score": round(score, 1),
                "score_factors": score_factors,
                "correlated_features": 0,  # Not calculated for regression
                "n_classes": n_unique,
                "class_balance": 0,  # Not applicable for regression
                "target_type": "regression",
                "interpretation": f"'{col}' detected as potential regression target ({reason}: {match_info})"
            }

        for col in df.columns:
            if col is None or col in seen_columns:
                continue
            col_lower = str(col).lower().strip()

            # Check regression keyword match
            for keyword in regression_keywords:
                if col_lower == keyword or f'_{keyword}' in col_lower or f'{keyword}_' in col_lower or keyword in col_lower:
                    result = _calculate_regression_score(col, "regression_keyword", keyword)
                    if result:
                        potential_outcomes.append(result)
                        seen_columns.add(col)
                    break

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

        Note: Identifier columns are excluded as they represent structural
        relationships (join keys) not meaningful predictive associations.
        """
        associations = []

        # Filter out identifier columns before analysis
        non_id_cols = [col for col in categorical_cols if not self._is_identifier_column(col, df)]

        if len(non_id_cols) < len(categorical_cols):
            id_count = len(categorical_cols) - len(non_id_cols)
            logger.debug(f"Filtered {id_count} identifier columns from Cramér's V analysis")

        # Apply column limit to prevent memory explosion with wide datasets
        cols_to_analyze = non_id_cols
        if len(non_id_cols) > self.max_columns_for_pairwise:
            logger.warning(
                f"Too many categorical columns ({len(non_id_cols)}) for pairwise analysis. "
                f"Limiting to first {self.max_columns_for_pairwise} columns."
            )
            cols_to_analyze = non_id_cols[:self.max_columns_for_pairwise]

        for i, col1 in enumerate(cols_to_analyze):
            for col2 in cols_to_analyze[i+1:]:
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

        # Limit columns to analyze to prevent memory explosion with wide datasets
        max_missing_cols = min(len(missing_cols), self.max_columns_for_pairwise)
        max_other_cols = self.max_columns_for_pairwise
        other_cols_to_check = list(df.columns)[:max_other_cols]

        if len(missing_cols) > max_missing_cols:
            logger.warning(
                f"Too many columns with missing data ({len(missing_cols)}) for pattern analysis. "
                f"Limiting to first {max_missing_cols} columns."
            )

        for missing_col in missing_cols[:max_missing_cols]:
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
