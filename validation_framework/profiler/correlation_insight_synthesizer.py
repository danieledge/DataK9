"""
Correlation Insight Synthesizer for DataK9 Profiler.

Transforms statistical correlations into meaningful, data-driven insights
with actual values, ratios, and business-relevant comparisons.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import logging
import re

logger = logging.getLogger(__name__)


@dataclass
class InsightResult:
    """Container for a synthesized correlation insight."""
    column1: str
    column2: str
    relationship_type: str  # numeric_numeric, numeric_categorical, categorical_categorical
    headline: str  # Main insight statement
    comparison_data: List[Dict]  # For bar charts
    metrics: Dict[str, Any]  # Supporting metrics (ratio, difference, etc.)
    confidence: str  # high, medium, low
    edge_cases: List[str]  # Warnings/context
    priority_score: float  # 0-100
    statistical_details: Dict[str, Any]  # Technical stats


class CorrelationInsightSynthesizer:
    """
    Synthesizes correlation statistics into actionable, data-driven insights.

    Transforms raw correlation data (r values, p-values) into:
    - Plain English headlines with actual data values
    - Visual comparison data for bar charts
    - Supporting metrics (ratios, multipliers, percentages)
    - Confidence indicators and edge case warnings
    """

    def __init__(self, df: pd.DataFrame, field_descriptions: Dict = None):
        """
        Initialize synthesizer with data.

        Args:
            df: DataFrame with the actual data for extracting values
            field_descriptions: Optional mapping of column names to friendly names
        """
        self.df = df
        self.field_descriptions = field_descriptions or {}
        self.insights: List[InsightResult] = []

    def get_friendly_name(self, col: str) -> str:
        """Get friendly name for a column."""
        if col in self.field_descriptions:
            return self.field_descriptions[col].get('friendly_name', col)
        return col

    def get_value_label(self, col: str, value) -> str:
        """Get friendly label for a column value, if configured."""
        if col in self.field_descriptions:
            value_labels = self.field_descriptions[col].get('value_labels', {})
            # Try exact match first, then string conversion
            if value in value_labels:
                return value_labels[value]
            str_val = str(value)
            if str_val in value_labels:
                return value_labels[str_val]
        return str(value)

    def synthesize_all(
        self,
        correlation_results: List[Dict],
        subgroups: List[Dict] = None
    ) -> List[InsightResult]:
        """
        Generate insights for all correlations.

        Args:
            correlation_results: List of correlation dicts from enhanced_correlation
            subgroups: List of categorical grouping patterns from context_discovery

        Returns:
            Prioritized list of InsightResult objects
        """
        self.insights = []

        # Process numeric correlations
        for corr in correlation_results:
            try:
                insight = self._synthesize_numeric_correlation(corr)
                if insight:
                    self.insights.append(insight)
            except Exception as e:
                logger.warning(f"Failed to synthesize correlation {corr}: {e}")

        # Process categorical groupings (subgroups)
        if subgroups:
            for sg in subgroups:
                try:
                    insight = self._synthesize_categorical_grouping(sg)
                    if insight:
                        self.insights.append(insight)
                except Exception as e:
                    logger.warning(f"Failed to synthesize subgroup {sg}: {e}")

        # Sort by priority (highest first)
        self.insights.sort(key=lambda x: x.priority_score, reverse=True)

        return self.insights

    def _synthesize_numeric_correlation(self, corr: Dict) -> Optional[InsightResult]:
        """Synthesize insight for numeric-numeric correlation."""
        col1 = corr.get('column1', '')
        col2 = corr.get('column2', '')
        r = corr.get('correlation', 0)

        if not col1 or not col2 or col1 not in self.df.columns or col2 not in self.df.columns:
            return None

        friendly1 = self.get_friendly_name(col1)
        friendly2 = self.get_friendly_name(col2)

        # Get clean data for analysis
        mask = self.df[col1].notna() & self.df[col2].notna()
        clean_df = self.df.loc[mask, [col1, col2]]
        n_obs = len(clean_df)

        if n_obs < 10:
            return None

        # Calculate quartile statistics
        col1_q1 = clean_df[col1].quantile(0.25)
        col1_q3 = clean_df[col1].quantile(0.75)

        # Get col2 values for low vs high col1
        low_mask = clean_df[col1] <= col1_q1
        high_mask = clean_df[col1] >= col1_q3

        col2_when_low = clean_df.loc[low_mask, col2]
        col2_when_high = clean_df.loc[high_mask, col2]

        low_median = col2_when_low.median()
        high_median = col2_when_high.median()

        # Calculate ratio - which group has higher col2 (e.g., Fare)?
        # high_median = col2 value when col1 is high (top 25%)
        # low_median = col2 value when col1 is low (bottom 25%)
        r_squared = r ** 2

        if low_median > 0 and high_median > 0:
            if high_median >= low_median:
                # High col1 -> High col2 (positive correlation)
                ratio = high_median / low_median
                higher_col2_label = f"High {friendly1}"
                higher_col2_val = high_median
                lower_col2_label = f"Low {friendly1}"
                lower_col2_val = low_median
            else:
                # Low col1 -> High col2 (negative correlation)
                ratio = low_median / high_median
                higher_col2_label = f"Low {friendly1}"
                higher_col2_val = low_median
                lower_col2_label = f"High {friendly1}"
                lower_col2_val = high_median
        else:
            ratio = None
            higher_col2_label = lower_col2_label = ""
            higher_col2_val = lower_col2_val = 0

        # Generate headline - simple correlation direction
        direction = "increases" if r > 0 else "decreases"
        headline = f"As {friendly1} increases, {friendly2} {direction}"

        # Build comparison bars with actual quartile values
        comparison_data = []
        if ratio and ratio > 1:
            # Get actual quartile boundary values for clearer labels
            q1_val = clean_df[col1].quantile(0.25)
            q3_val = clean_df[col1].quantile(0.75)

            # Format quartile values
            q1_fmt = f"{q1_val:.0f}" if q1_val == int(q1_val) else f"{q1_val:.1f}"
            q3_fmt = f"{q3_val:.0f}" if q3_val == int(q3_val) else f"{q3_val:.1f}"

            comparison_data = [
                {
                    'label': f"{friendly1} ≤ {q1_fmt}",
                    'value': low_median,
                    'percentage': (low_median / max(low_median, high_median) * 100),
                    'formatted': self._format_value(low_median, col2)
                },
                {
                    'label': f"{friendly1} ≥ {q3_fmt}",
                    'value': high_median,
                    'percentage': (high_median / max(low_median, high_median) * 100),
                    'formatted': self._format_value(high_median, col2)
                }
            ]
            # Sort so higher value is on top
            comparison_data.sort(key=lambda x: x['value'], reverse=True)
            comparison_data[0]['percentage'] = 100
            comparison_data[1]['percentage'] = (comparison_data[1]['value'] / comparison_data[0]['value'] * 100) if comparison_data[0]['value'] > 0 else 0

        # Calculate metrics
        pct_diff = ((higher_col2_val - lower_col2_val) / lower_col2_val * 100) if lower_col2_val > 0 else 0
        metrics = {
            'ratio': ratio,
            'percentage_difference': pct_diff,
            'r_squared': r_squared,
            'n_observations': n_obs
        }

        # Determine confidence
        confidence = self._calculate_confidence(n_obs, r_squared)

        # Check for edge cases
        edge_cases = self._detect_edge_cases_numeric(clean_df, col1, col2, r)

        # Calculate priority
        priority = self._calculate_priority(
            abs(r), r_squared, ratio, n_obs, col1, col2
        )

        # Statistical details
        stat_details = {
            'correlation': r,
            'r_squared': r_squared,
            'method': corr.get('type', 'pearson'),
            'p_value': corr.get('p_value'),
            'n_observations': n_obs,
            'col1_range': f"{clean_df[col1].min():.2f} - {clean_df[col1].max():.2f}",
            'col2_range': f"{clean_df[col2].min():.2f} - {clean_df[col2].max():.2f}"
        }

        return InsightResult(
            column1=col1,
            column2=col2,
            relationship_type='numeric_numeric',
            headline=headline,
            comparison_data=comparison_data,
            metrics=metrics,
            confidence=confidence,
            edge_cases=edge_cases,
            priority_score=priority,
            statistical_details=stat_details
        )

    def _synthesize_categorical_grouping(self, sg: Dict) -> Optional[InsightResult]:
        """Synthesize insight for categorical-numeric grouping (from context discovery)."""
        seg_col = sg.get('segment_col', '')
        val_col = sg.get('value_col', '')
        var_explained = sg.get('variance_explained', 0)

        if not seg_col or not val_col:
            return None
        if seg_col not in self.df.columns or val_col not in self.df.columns:
            return None

        friendly_seg = self.get_friendly_name(seg_col)
        friendly_val = self.get_friendly_name(val_col)

        # Get group statistics
        group_stats = self.df.groupby(seg_col)[val_col].agg(['median', 'mean', 'count', 'std'])
        group_stats = group_stats.dropna()

        if len(group_stats) < 2:
            return None

        # Sort by median to find highest/lowest
        group_stats = group_stats.sort_values('median', ascending=False)

        highest_group = group_stats.index[0]
        lowest_group = group_stats.index[-1]
        highest_val = group_stats.loc[highest_group, 'median']
        lowest_val = group_stats.loc[lowest_group, 'median']

        # Calculate ratio
        ratio = highest_val / lowest_val if lowest_val > 0 else None

        # Format group labels - use value_labels if configured, otherwise generic
        highest_label = self.get_value_label(seg_col, highest_group)
        lowest_label = self.get_value_label(seg_col, lowest_group)

        # Generate headline
        if ratio and ratio > 1.5:
            headline = f"{highest_label} shows <span class=\"highlight-value\">{ratio:.1f}x higher</span> {friendly_val} than {lowest_label}"
        else:
            headline = f"{friendly_val} varies significantly by {friendly_seg}"

        # Build comparison data for all groups - use value_labels if configured
        comparison_data = []
        max_val = group_stats['median'].max()
        for group_name, row in group_stats.iterrows():
            comparison_data.append({
                'label': self.get_value_label(seg_col, group_name),
                'value': row['median'],
                'percentage': (row['median'] / max_val * 100) if max_val > 0 else 0,
                'formatted': self._format_value(row['median'], val_col),
                'count': int(row['count'])
            })

        # Calculate metrics
        pct_diff = ((highest_val - lowest_val) / lowest_val * 100) if lowest_val > 0 else 0
        n_obs = int(group_stats['count'].sum())

        metrics = {
            'ratio': ratio,
            'percentage_difference': pct_diff,
            'variance_explained': var_explained,
            'n_groups': len(group_stats),
            'n_observations': n_obs
        }

        # Determine confidence
        confidence = self._calculate_confidence(n_obs, var_explained)

        # Check for edge cases
        edge_cases = self._detect_edge_cases_categorical(group_stats, seg_col, val_col)

        # Calculate priority
        priority = self._calculate_priority_categorical(
            var_explained, ratio, n_obs, seg_col, val_col
        )

        # Statistical details
        stat_details = {
            'test_type': 'ANOVA / Kruskal-Wallis',
            'variance_explained': var_explained,
            'eta_squared': var_explained,
            'n_groups': len(group_stats),
            'n_observations': n_obs,
            'group_sizes': {str(k): int(v) for k, v in group_stats['count'].items()}
        }

        return InsightResult(
            column1=val_col,
            column2=seg_col,
            relationship_type='numeric_categorical',
            headline=headline,
            comparison_data=comparison_data,
            metrics=metrics,
            confidence=confidence,
            edge_cases=edge_cases,
            priority_score=priority,
            statistical_details=stat_details
        )

    def _format_value(self, value: float, col_name: str) -> str:
        """Format value with appropriate units/precision."""
        col_lower = col_name.lower()

        # Currency-like columns
        if any(kw in col_lower for kw in ['price', 'fare', 'cost', 'amount', 'revenue', 'salary']):
            if value >= 1000:
                return f"${value:,.0f}"
            return f"${value:.2f}"

        # Percentage-like columns
        if any(kw in col_lower for kw in ['rate', 'percent', 'pct', 'ratio']):
            return f"{value:.1f}%"

        # Age-like columns
        if 'age' in col_lower:
            return f"{value:.0f} years"

        # Default formatting
        if value >= 1000:
            return f"{value:,.0f}"
        elif value >= 10:
            return f"{value:.1f}"
        elif value >= 1:
            return f"{value:.2f}"
        else:
            return f"{value:.3f}"

    def _calculate_confidence(self, n_obs: int, effect_size: float) -> str:
        """Calculate confidence level based on sample size and effect size."""
        if n_obs >= 100 and effect_size >= 0.25:
            return 'high'
        elif n_obs >= 30 and effect_size >= 0.10:
            return 'medium'
        else:
            return 'low'

    def _detect_edge_cases_numeric(
        self,
        df: pd.DataFrame,
        col1: str,
        col2: str,
        r: float
    ) -> List[str]:
        """Detect edge cases for numeric correlations."""
        edge_cases = []

        # Check for outlier influence
        try:
            q1, q99 = df[col1].quantile([0.01, 0.99])
            trimmed = df[(df[col1] >= q1) & (df[col1] <= q99)]
            if len(trimmed) >= 10:
                trimmed_r = trimmed[[col1, col2]].corr().iloc[0, 1]
                if abs(r - trimmed_r) > 0.15:
                    edge_cases.append(
                        f"Correlation changes from {r:.2f} to {trimmed_r:.2f} when outliers removed"
                    )
        except Exception:
            pass

        # Check for non-linearity (compare Pearson vs Spearman)
        try:
            from scipy.stats import spearmanr
            spearman_r, _ = spearmanr(df[col1], df[col2])
            if abs(spearman_r) - abs(r) > 0.15:
                edge_cases.append(
                    f"Non-linear pattern detected (Spearman ρ={spearman_r:.2f} vs Pearson r={r:.2f})"
                )
        except Exception:
            pass

        return edge_cases

    def _detect_edge_cases_categorical(
        self,
        group_stats: pd.DataFrame,
        seg_col: str,
        val_col: str
    ) -> List[str]:
        """Detect edge cases for categorical groupings."""
        edge_cases = []

        # Check for unbalanced groups
        counts = group_stats['count']
        if counts.max() / counts.min() > 10:
            edge_cases.append(
                f"Unbalanced groups: {counts.idxmax()} has {counts.max():.0f}x more observations"
            )

        # Check for high variance within groups
        if 'std' in group_stats.columns:
            cv = group_stats['std'] / group_stats['mean']
            high_cv_groups = cv[cv > 1.0]
            if len(high_cv_groups) > 0:
                edge_cases.append(
                    f"High variance within {', '.join(str(g) for g in high_cv_groups.index[:2])}"
                )

        return edge_cases

    def _calculate_priority(
        self,
        abs_r: float,
        r_squared: float,
        ratio: Optional[float],
        n_obs: int,
        col1: str,
        col2: str
    ) -> float:
        """Calculate priority score (0-100) for a numeric correlation."""
        score = 0.0

        # Statistical strength (0-30)
        if abs_r >= 0.7:
            score += 30
        elif abs_r >= 0.5:
            score += 22
        elif abs_r >= 0.3:
            score += 15
        else:
            score += 8

        # Effect size / practical significance (0-25)
        if ratio:
            if ratio >= 5.0:
                score += 25
            elif ratio >= 3.0:
                score += 20
            elif ratio >= 2.0:
                score += 15
            elif ratio >= 1.5:
                score += 10
            else:
                score += 5
        else:
            score += r_squared * 25

        # Business relevance (0-20)
        relevance_keywords = {
            'high': ['revenue', 'profit', 'churn', 'survival', 'survived', 'conversion',
                    'fraud', 'risk', 'default', 'outcome', 'target', 'price', 'cost'],
            'medium': ['satisfaction', 'engagement', 'usage', 'age', 'income', 'salary']
        }

        col_text = f"{col1} {col2}".lower()
        if any(kw in col_text for kw in relevance_keywords['high']):
            score += 20
        elif any(kw in col_text for kw in relevance_keywords['medium']):
            score += 12
        else:
            score += 6

        # Sample size confidence (0-15)
        if n_obs >= 500:
            score += 15
        elif n_obs >= 100:
            score += 12
        elif n_obs >= 30:
            score += 8
        else:
            score += 4

        # Clarity bonus for round ratios (0-10)
        if ratio:
            if abs(ratio - round(ratio)) < 0.1:
                score += 10
            else:
                score += 5
        else:
            score += 5

        return min(score, 100)

    def _calculate_priority_categorical(
        self,
        var_explained: float,
        ratio: Optional[float],
        n_obs: int,
        seg_col: str,
        val_col: str
    ) -> float:
        """Calculate priority score for categorical grouping."""
        score = 0.0

        # Variance explained (0-30)
        if var_explained >= 0.50:
            score += 30
        elif var_explained >= 0.30:
            score += 24
        elif var_explained >= 0.15:
            score += 18
        else:
            score += 10

        # Effect size / ratio (0-25)
        if ratio:
            if ratio >= 5.0:
                score += 25
            elif ratio >= 3.0:
                score += 20
            elif ratio >= 2.0:
                score += 15
            elif ratio >= 1.5:
                score += 10
            else:
                score += 5
        else:
            score += 10

        # Business relevance (0-20)
        relevance_keywords = {
            'high': ['revenue', 'profit', 'churn', 'survival', 'survived', 'conversion',
                    'fraud', 'risk', 'default', 'outcome', 'target', 'price', 'fare'],
            'medium': ['satisfaction', 'engagement', 'usage', 'age', 'income']
        }

        col_text = f"{seg_col} {val_col}".lower()
        if any(kw in col_text for kw in relevance_keywords['high']):
            score += 20
        elif any(kw in col_text for kw in relevance_keywords['medium']):
            score += 12
        else:
            score += 6

        # Sample size (0-15)
        if n_obs >= 500:
            score += 15
        elif n_obs >= 100:
            score += 12
        elif n_obs >= 30:
            score += 8
        else:
            score += 4

        # Clarity bonus (0-10)
        if ratio and abs(ratio - round(ratio)) < 0.1:
            score += 10
        else:
            score += 5

        return min(score, 100)

    def get_top_insights(self, n: int = 5) -> List[InsightResult]:
        """Get top N insights by priority."""
        return self.insights[:n]

    def to_dict_list(self) -> List[Dict]:
        """Convert insights to list of dicts for JSON/HTML rendering."""
        return [
            {
                'column1': i.column1,
                'column2': i.column2,
                'relationship_type': i.relationship_type,
                'headline': i.headline,
                'comparison_data': i.comparison_data,
                'metrics': i.metrics,
                'confidence': i.confidence,
                'edge_cases': i.edge_cases,
                'priority_score': i.priority_score,
                'statistical_details': i.statistical_details
            }
            for i in self.insights
        ]
