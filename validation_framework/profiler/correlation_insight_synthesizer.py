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

# Threshold below which correlations are considered "weak"
WEAK_CORRELATION_THRESHOLD = 0.3


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

    def __init__(self, df: pd.DataFrame, field_descriptions: Dict = None,
                 column_profiles: List = None):
        """
        Initialize synthesizer with data.

        Args:
            df: DataFrame with the actual data for extracting values
            field_descriptions: Optional mapping of column names to friendly names
            column_profiles: Optional list of ColumnProfile objects with semantic_info
        """
        self.df = df
        self.field_descriptions = field_descriptions or {}
        self.column_profiles = column_profiles or []
        self.insights: List[InsightResult] = []

        # Build lookup for semantic info
        self._semantic_cache = {}
        for col_profile in self.column_profiles:
            if hasattr(col_profile, 'name') and hasattr(col_profile, 'semantic_info'):
                self._semantic_cache[col_profile.name] = col_profile.semantic_info

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

    def _is_identifier_column(self, col: str) -> bool:
        """
        Check if column is an identifier based on existing semantic metadata.

        Uses the semantic detection system's classification - no redundant
        pattern matching here.
        """
        if col not in self._semantic_cache:
            return False

        semantic_info = self._semantic_cache.get(col)
        if not semantic_info:
            return False

        # Check resolved semantic type from existing detection
        resolved = semantic_info.get('resolved', {})
        primary_type = (resolved.get('primary_type') or '').lower()

        # Check if semantic system classified this as an identifier
        if 'identifier' in primary_type or 'id' in primary_type:
            return True

        # Check semantic tags from existing detection
        tags = semantic_info.get('semantic_tags', [])
        for tag in tags:
            tag_lower = tag.lower() if isinstance(tag, str) else ''
            if 'identifier' in tag_lower:
                return True

        return False

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
            Prioritized list of InsightResult objects (deduplicated)
        """
        self.insights = []
        seen_pairs = set()  # Track column pairs to avoid duplicates

        # Process numeric correlations
        for corr in correlation_results:
            try:
                col1 = corr.get('column1', '')
                col2 = corr.get('column2', '')
                pair_key = tuple(sorted([col1, col2]))

                if pair_key in seen_pairs:
                    continue

                # Skip correlations involving identifier columns
                # (they are record locators, not meaningful predictive features)
                if self._is_identifier_column(col1) or self._is_identifier_column(col2):
                    logger.debug(f"Skipping identifier-based correlation: {col1} vs {col2}")
                    continue

                insight = self._synthesize_numeric_correlation(corr)
                if insight:
                    self.insights.append(insight)
                    seen_pairs.add(pair_key)
            except Exception as e:
                logger.warning(f"Failed to synthesize correlation {corr}: {e}")

        # Process categorical groupings (subgroups)
        if subgroups:
            for sg in subgroups:
                try:
                    seg_col = sg.get('segment_col', '')
                    val_col = sg.get('value_col', '')
                    pair_key = tuple(sorted([seg_col, val_col]))

                    if pair_key in seen_pairs:
                        continue

                    # Skip groupings involving identifier columns
                    if self._is_identifier_column(seg_col) or self._is_identifier_column(val_col):
                        logger.debug(f"Skipping identifier-based grouping: {seg_col} vs {val_col}")
                        continue

                    insight = self._synthesize_categorical_grouping(sg)
                    if insight:
                        self.insights.append(insight)
                        seen_pairs.add(pair_key)
                except Exception as e:
                    logger.warning(f"Failed to synthesize subgroup {sg}: {e}")

        # Sort by priority (highest first)
        self.insights.sort(key=lambda x: x.priority_score, reverse=True)

        return self.insights

    def _is_binary_column(self, col: str) -> bool:
        """Check if column is binary (0/1 or two unique values)."""
        if col not in self.df.columns:
            return False
        unique = self.df[col].dropna().unique()
        if len(unique) == 2:
            return True
        if set(unique).issubset({0, 1, 0.0, 1.0}):
            return True
        return False

    def _is_tautological_correlation(self, col1: str, col2: str) -> bool:
        """
        Check if correlation between columns is tautological (low insight value).

        Examples:
        - SibSp/Parch: Both count family members, correlation is obvious
        - Price/Total: If total includes price, correlation is expected
        - Count/Amount: Often measure same thing
        """
        col1_lower = col1.lower()
        col2_lower = col2.lower()

        # Define groups of semantically related column patterns
        tautological_groups = [
            # Family members
            {'sib', 'spouse', 'parch', 'parent', 'child', 'family', 'relative'},
            # Financial amounts
            {'price', 'total', 'amount', 'cost', 'sum', 'value'},
            # Counts
            {'count', 'num', 'qty', 'quantity'},
        ]

        for group in tautological_groups:
            col1_matches = any(pat in col1_lower for pat in group)
            col2_matches = any(pat in col2_lower for pat in group)
            if col1_matches and col2_matches:
                return True

        return False

    def _synthesize_numeric_correlation(self, corr: Dict) -> Optional[InsightResult]:
        """Synthesize insight for numeric-numeric correlation."""
        col1 = corr.get('column1', '')
        col2 = corr.get('column2', '')
        r = corr.get('correlation', 0)

        if not col1 or not col2 or col1 not in self.df.columns or col2 not in self.df.columns:
            return None

        # Skip tautological correlations (low insight value)
        if self._is_tautological_correlation(col1, col2):
            logger.debug(f"Skipping tautological correlation: {col1} vs {col2}")
            return None

        friendly1 = self.get_friendly_name(col1)
        friendly2 = self.get_friendly_name(col2)

        # Get clean data for analysis
        mask = self.df[col1].notna() & self.df[col2].notna()
        clean_df = self.df.loc[mask, [col1, col2]]
        n_obs = len(clean_df)

        if n_obs < 10:
            return None

        r_squared = r ** 2

        # Check if either column is binary - need special handling
        col1_binary = self._is_binary_column(col1)
        col2_binary = self._is_binary_column(col2)

        # If col1 is binary (like Survived), swap so binary is col2 (outcome)
        if col1_binary and not col2_binary:
            col1, col2 = col2, col1
            friendly1, friendly2 = friendly2, friendly1
            col1_binary, col2_binary = col2_binary, col1_binary

        # Handle binary outcome (e.g., Survived)
        if col2_binary:
            # Group by col1 quartiles and show rate of col2=1
            col1_q1 = clean_df[col1].quantile(0.25)
            col1_q3 = clean_df[col1].quantile(0.75)

            low_group = clean_df[clean_df[col1] <= col1_q1]
            high_group = clean_df[clean_df[col1] >= col1_q3]

            low_rate = low_group[col2].mean() * 100  # Percentage
            high_rate = high_group[col2].mean() * 100

            # Format quartile values
            q1_fmt = f"{col1_q1:.0f}" if col1_q1 == int(col1_q1) else f"{col1_q1:.1f}"
            q3_fmt = f"{col1_q3:.0f}" if col1_q3 == int(col1_q3) else f"{col1_q3:.1f}"

            # Determine which group has higher rate
            if high_rate >= low_rate and low_rate > 0:
                ratio = high_rate / low_rate
                higher_label = f"{friendly1} ≥ {q3_fmt}"
                higher_rate = high_rate
                lower_label = f"{friendly1} ≤ {q1_fmt}"
                lower_rate_val = low_rate
            elif low_rate > high_rate and high_rate > 0:
                ratio = low_rate / high_rate
                higher_label = f"{friendly1} ≤ {q1_fmt}"
                higher_rate = low_rate
                lower_label = f"{friendly1} ≥ {q3_fmt}"
                lower_rate_val = high_rate
            else:
                ratio = None
                higher_label = lower_label = ""
                higher_rate = lower_rate_val = 0

            # Generate headline for binary outcome
            # Use softer language for weak correlations (|r| < 0.3)
            is_weak = abs(r) < WEAK_CORRELATION_THRESHOLD
            if ratio and ratio > 1.2:
                if is_weak:
                    headline = f"Records with {higher_label} show a slightly higher {friendly2} rate ({ratio:.1f}x)"
                else:
                    headline = f"Records with {higher_label} have <span class=\"highlight-value\">{ratio:.1f}x</span> the {friendly2} rate"
            else:
                direction = "higher" if r > 0 else "lower"
                if is_weak:
                    headline = f"Weak relationship: {friendly1} shows slight tendency toward {direction} {friendly2} rate"
                else:
                    headline = f"Higher {friendly1} associated with {direction} {friendly2} rate"

            # Build comparison bars showing rates
            comparison_data = []
            if ratio and ratio > 1:
                comparison_data = [
                    {
                        'label': higher_label,
                        'value': higher_rate,
                        'percentage': 100,
                        'formatted': f"{higher_rate:.0f}%"
                    },
                    {
                        'label': lower_label,
                        'value': lower_rate_val,
                        'percentage': (lower_rate_val / higher_rate * 100) if higher_rate > 0 else 0,
                        'formatted': f"{lower_rate_val:.0f}%"
                    }
                ]

            metrics = {
                'ratio': ratio,
                'higher_rate': higher_rate,
                'lower_rate': lower_rate_val,
                'r_squared': r_squared,
                'n_observations': n_obs,
                'is_binary_outcome': True
            }

        else:
            # Both columns are continuous - compare medians
            col1_q1 = clean_df[col1].quantile(0.25)
            col1_q3 = clean_df[col1].quantile(0.75)

            # Skip if quartiles are the same (no variance)
            if col1_q1 == col1_q3:
                return None

            low_mask = clean_df[col1] <= col1_q1
            high_mask = clean_df[col1] >= col1_q3

            col2_when_low = clean_df.loc[low_mask, col2]
            col2_when_high = clean_df.loc[high_mask, col2]

            low_median = col2_when_low.median()
            high_median = col2_when_high.median()

            # Calculate ratio (handle zeros)
            if low_median > 0 and high_median > 0:
                ratio = max(high_median, low_median) / min(high_median, low_median)
            elif high_median > 0 and low_median == 0:
                ratio = None  # Can't compute ratio with zero, but still show bars
            elif low_median > 0 and high_median == 0:
                ratio = None
            else:
                ratio = None

            # Format quartile values
            q1_fmt = f"{col1_q1:.0f}" if col1_q1 == int(col1_q1) else f"{col1_q1:.1f}"
            q3_fmt = f"{col1_q3:.0f}" if col1_q3 == int(col1_q3) else f"{col1_q3:.1f}"

            # Generate headline - show actual values instead of confusing "higher/lower"
            # This avoids semantic confusion (e.g., Pclass 1 is "first class" but numerically lower)
            high_label = f"{friendly1} ≥ {q3_fmt}"
            low_label = f"{friendly1} ≤ {q1_fmt}"
            high_val_fmt = self._format_value(high_median, col2)
            low_val_fmt = self._format_value(low_median, col2)

            if r > 0:
                # Positive correlation: high col1 → high col2
                headline = f"{high_label}: {high_val_fmt} {friendly2} vs {low_label}: {low_val_fmt}"
            else:
                # Negative correlation: high col1 → low col2
                headline = f"{low_label}: {high_val_fmt if low_median > high_median else low_val_fmt} {friendly2} vs {high_label}: {low_val_fmt if low_median > high_median else high_val_fmt}"

            # Simplified headline showing the relationship direction
            # Use softer language for weak correlations (|r| < 0.3)
            is_weak = abs(r) < WEAK_CORRELATION_THRESHOLD
            if high_median > low_median:
                if is_weak:
                    headline = f"Weak relationship: records with {high_label} tend to have slightly higher {friendly2} ({high_val_fmt} vs {low_val_fmt})"
                else:
                    headline = f"Records with {high_label} have higher {friendly2} ({high_val_fmt} vs {low_val_fmt})"
            else:
                if is_weak:
                    headline = f"Weak relationship: records with {low_label} tend to have slightly higher {friendly2} ({low_val_fmt} vs {high_val_fmt})"
                else:
                    headline = f"Records with {low_label} have higher {friendly2} ({low_val_fmt} vs {high_val_fmt})"

            # Build comparison bars - show even when ratio can't be computed
            comparison_data = []
            max_val = max(high_median, low_median)
            if max_val > 0 and (high_median != low_median):
                bars = [
                    {
                        'label': low_label,
                        'value': low_median,
                        'formatted': self._format_value(low_median, col2)
                    },
                    {
                        'label': high_label,
                        'value': high_median,
                        'formatted': self._format_value(high_median, col2)
                    }
                ]
                # Sort by value descending
                bars.sort(key=lambda x: x['value'], reverse=True)
                bars[0]['percentage'] = 100
                bars[1]['percentage'] = (bars[1]['value'] / max_val * 100) if max_val > 0 else 0
                comparison_data = bars

            metrics = {
                'ratio': ratio,
                'r_squared': r_squared,
                'n_observations': n_obs,
                'is_binary_outcome': False
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

        # Skip tautological correlations (low insight value)
        if self._is_tautological_correlation(seg_col, val_col):
            logger.debug(f"Skipping tautological categorical grouping: {seg_col} vs {val_col}")
            return None

        friendly_seg = self.get_friendly_name(seg_col)
        friendly_val = self.get_friendly_name(val_col)

        # Check if value column is binary (like Survived) - use rates instead of medians
        val_is_binary = self._is_binary_column(val_col)

        # Get group statistics
        group_stats = self.df.groupby(seg_col)[val_col].agg(['median', 'mean', 'count', 'std'])
        group_stats = group_stats.dropna()

        if len(group_stats) < 2:
            return None

        # For binary outcomes, use mean (rate) instead of median
        sort_col = 'mean' if val_is_binary else 'median'
        group_stats = group_stats.sort_values(sort_col, ascending=False)

        highest_group = group_stats.index[0]
        lowest_group = group_stats.index[-1]

        if val_is_binary:
            # Use rates (means) as percentages
            highest_rate = group_stats.loc[highest_group, 'mean'] * 100
            lowest_rate = group_stats.loc[lowest_group, 'mean'] * 100
            ratio = highest_rate / lowest_rate if lowest_rate > 0 else None
        else:
            highest_val = group_stats.loc[highest_group, 'median']
            lowest_val = group_stats.loc[lowest_group, 'median']
            ratio = highest_val / lowest_val if lowest_val > 0 else None

        # Format group labels - use value_labels if configured, otherwise generic
        highest_label = self.get_value_label(seg_col, highest_group)
        lowest_label = self.get_value_label(seg_col, lowest_group)

        # Generate headline
        # Consider relationship weak if variance explained is low (< 10%)
        is_weak = var_explained < 0.10
        if val_is_binary:
            # Binary outcome - use rate language
            if ratio and ratio > 1.5:
                if is_weak:
                    headline = f"{highest_label} show a somewhat higher {friendly_val} rate ({ratio:.1f}x) vs {lowest_label}"
                else:
                    headline = f"{highest_label} have <span class=\"highlight-value\">{ratio:.1f}x</span> the {friendly_val} rate vs {lowest_label}"
            else:
                if is_weak:
                    headline = f"Small difference in {friendly_val} rate: {highest_label} ({highest_rate:.0f}%) vs {lowest_label} ({lowest_rate:.0f}%)"
                else:
                    headline = f"{friendly_val} rate: {highest_label} ({highest_rate:.0f}%) vs {lowest_label} ({lowest_rate:.0f}%)"
        else:
            if ratio and ratio > 1.5:
                if is_weak:
                    headline = f"{highest_label} shows somewhat higher {friendly_val} ({ratio:.1f}x) than {lowest_label}"
                else:
                    headline = f"{highest_label} shows <span class=\"highlight-value\">{ratio:.1f}x higher</span> {friendly_val} than {lowest_label}"
            else:
                if is_weak:
                    headline = f"{friendly_val} shows minor variation by {friendly_seg}"
                else:
                    headline = f"{friendly_val} varies by {friendly_seg}"

        # Build comparison data for all groups - use value_labels if configured
        comparison_data = []
        if val_is_binary:
            # Show rates as percentages
            max_rate = group_stats['mean'].max() * 100
            for group_name, row in group_stats.iterrows():
                rate = row['mean'] * 100
                comparison_data.append({
                    'label': self.get_value_label(seg_col, group_name),
                    'value': rate,
                    'percentage': (rate / max_rate * 100) if max_rate > 0 else 0,
                    'formatted': f"{rate:.0f}%",
                    'count': int(row['count'])
                })
        else:
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
        n_obs = int(group_stats['count'].sum())
        if val_is_binary:
            pct_diff = highest_rate - lowest_rate  # Percentage point difference
        else:
            pct_diff = ((highest_val - lowest_val) / lowest_val * 100) if lowest_val > 0 else 0

        metrics = {
            'ratio': ratio,
            'percentage_difference': pct_diff,
            'variance_explained': var_explained,
            'n_groups': len(group_stats),
            'n_observations': n_obs,
            'is_binary_outcome': val_is_binary
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

        # Default formatting - prefer whole numbers when value is integer
        if value == int(value):
            return f"{int(value):,}"
        elif value >= 1000:
            return f"{value:,.0f}"
        elif value >= 10:
            return f"{value:.1f}"
        elif value >= 1:
            return f"{value:.2f}"
        else:
            return f"{value:.2f}"

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
        # Edge cases are now included in technical details only
        # Removed from main display as they were too technical
        return []

    def _detect_edge_cases_categorical(
        self,
        group_stats: pd.DataFrame,
        seg_col: str,
        val_col: str
    ) -> List[str]:
        """Detect edge cases for categorical groupings."""
        edge_cases = []

        # Check for unbalanced groups (only if significantly different)
        counts = group_stats['count']
        if counts.max() / counts.min() > 10:
            larger_group = self.get_value_label(seg_col, counts.idxmax())
            smaller_group = self.get_value_label(seg_col, counts.idxmin())
            edge_cases.append(
                f"Unbalanced sample: {larger_group} has {int(counts.max() / counts.min())}x more records than {smaller_group}"
            )

        # Skip variance check for binary outcomes (doesn't make sense)
        # High CV for binary outcomes is expected and not meaningful

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
