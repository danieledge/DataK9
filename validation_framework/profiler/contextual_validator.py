"""
Contextual Validator Module for Self-Calibrating Anomaly Detection.

Validates outlier candidates against discovered context to filter out
false positives - values that appear unusual globally but are normal
within their context (subgroup, correlation pattern, etc.).

Key principle: Only flag anomalies that can't be explained by discovered patterns.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple

from validation_framework.profiler.context_discovery import (
    ContextStore,
    FieldDescription,
    SubgroupPattern,
    CorrelationPattern
)

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None

logger = logging.getLogger(__name__)


@dataclass
class OutlierExplanation:
    """Explanation for why an outlier was validated/invalidated."""
    row_index: int
    value: Any
    column: str
    is_explained: bool
    confidence: float  # 0-1, how confident we are this is/isn't an outlier
    explanations: List[str] = field(default_factory=list)
    patterns_checked: int = 0
    patterns_matched: int = 0


@dataclass
class ValidatedOutlierResult:
    """Result of contextual validation for a column's outliers."""
    column: str
    column_display_name: str
    original_count: int
    validated_count: int  # Confirmed as real outliers
    explained_count: int  # Explained by context (not real outliers)
    confidence: float  # Overall confidence in the validation
    sample_explanations: List[OutlierExplanation] = field(default_factory=list)
    context_summary: str = ""  # Human-readable summary of what explained the outliers

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'column': self.column,
            'column_display_name': self.column_display_name,
            'original_count': self.original_count,
            'validated_count': self.validated_count,
            'explained_count': self.explained_count,
            'confidence': self.confidence,
            'context_summary': self.context_summary,
            'sample_explanations': [
                {
                    'row_index': e.row_index,
                    'value': e.value,
                    'is_explained': e.is_explained,
                    'confidence': e.confidence,
                    'explanations': e.explanations
                }
                for e in self.sample_explanations[:10]
            ]
        }


@dataclass
class ContextValidationResult:
    """Complete result of context-aware anomaly validation."""
    columns: Dict[str, ValidatedOutlierResult] = field(default_factory=dict)
    total_original: int = 0
    total_validated: int = 0
    total_explained: int = 0
    context_patterns_used: List[str] = field(default_factory=list)
    discovery_stats: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'total_original': self.total_original,
            'total_validated': self.total_validated,
            'total_explained': self.total_explained,
            'reduction_percentage': (
                (self.total_original - self.total_validated) / self.total_original * 100
                if self.total_original > 0 else 0
            ),
            'context_patterns_used': self.context_patterns_used,
            'columns': {col: result.to_dict() for col, result in self.columns.items()},
            'discovery_stats': self.discovery_stats
        }


class ContextualValidator:
    """
    Validates outlier candidates against discovered context.

    Filters out false positives by checking if apparent outliers are
    actually normal within their context (subgroup, correlation, etc.).
    """

    def __init__(
        self,
        context: ContextStore,
        min_confidence_to_flag: float = 0.5,  # Minimum confidence to keep as outlier
        require_multiple_checks: bool = True  # Require failing multiple checks to flag
    ):
        """
        Initialize the contextual validator.

        Args:
            context: ContextStore with discovered patterns
            min_confidence_to_flag: Minimum suspicion score to flag as outlier (default 0.5)
            require_multiple_checks: If True, only flag if multiple pattern checks fail
        """
        self.context = context
        self.min_confidence_to_flag = min_confidence_to_flag
        self.require_multiple_checks = require_multiple_checks

    def validate(
        self,
        outliers: Dict[str, Dict[str, Any]],
        df,
        max_validate_per_column: int = 100
    ) -> ContextValidationResult:
        """
        Validate outlier candidates against discovered context.

        Args:
            outliers: Dict mapping column names to outlier detection results
                Expected format: {col: {'anomaly_count': N, 'sample_rows': [{...}], 'top_anomalies': [...]}}
                OR legacy format: {col: {'count': N, 'indices': [...], 'outlier_values': [...]}}
            df: Original DataFrame for context lookup (used for index-based lookups)
            max_validate_per_column: Max outliers to validate per column (for performance)

        Returns:
            ContextValidationResult with validated outliers and explanations
        """
        result = ContextValidationResult(
            discovery_stats=self.context.discovery_stats
        )

        patterns_used = set()

        for col, outlier_data in outliers.items():
            if not isinstance(outlier_data, dict):
                continue

            count = outlier_data.get('anomaly_count', outlier_data.get('count', 0))
            if count == 0:
                continue

            # Extract sample rows for validation
            # New format uses 'sample_rows' with full row dicts, 'top_anomalies' for values
            sample_rows = outlier_data.get('sample_rows', [])[:max_validate_per_column]
            top_anomalies = outlier_data.get('top_anomalies', [])[:max_validate_per_column]

            # Legacy format uses 'indices' and 'outlier_values'
            indices = outlier_data.get('indices', [])[:max_validate_per_column]
            values = outlier_data.get('outlier_values', [])[:max_validate_per_column]

            # Validate each outlier
            explanations = []
            validated_count = 0
            explained_count = 0
            explanation_reasons = []

            # Prefer sample_rows (new format from ML analyzer)
            if sample_rows:
                for i, row_dict in enumerate(sample_rows):
                    # Get the value for this column from the row
                    value = row_dict.get(col)
                    if value is not None:
                        try:
                            value = float(value)
                        except (ValueError, TypeError):
                            pass

                    # Also get value from top_anomalies if available
                    if i < len(top_anomalies):
                        value = top_anomalies[i]

                    explanation = self._validate_single(col, value, row_dict)
                    explanations.append(explanation)

                    if explanation.is_explained:
                        explained_count += 1
                        explanation_reasons.extend(explanation.explanations)
                        for pattern in explanation.explanations:
                            patterns_used.add(pattern.split('(')[0].strip())
                    else:
                        validated_count += 1
            # Fall back to index-based lookup (legacy format)
            elif indices and HAS_POLARS and df is not None:
                # Convert to Polars if needed
                if not isinstance(df, pl.DataFrame):
                    try:
                        df = pl.from_pandas(df)
                    except Exception as e:
                        logger.warning(f"Could not convert to Polars: {e}")
                        df = None

                if df is not None:
                    for i, idx in enumerate(indices):
                        value = values[i] if i < len(values) else None

                        try:
                            row = df.row(idx, named=True)
                            explanation = self._validate_single(col, value, row)
                            explanations.append(explanation)

                            if explanation.is_explained:
                                explained_count += 1
                                explanation_reasons.extend(explanation.explanations)
                                for pattern in explanation.explanations:
                                    patterns_used.add(pattern.split('(')[0].strip())
                            else:
                                validated_count += 1

                        except Exception as e:
                            logger.debug(f"Error validating row {idx} for {col}: {e}")
                            validated_count += 1
            else:
                # No sample rows or indices available - can't validate
                result.columns[col] = ValidatedOutlierResult(
                    column=col,
                    column_display_name=self.context.get_field_display_name(col),
                    original_count=count,
                    validated_count=count,
                    explained_count=0,
                    confidence=0.5,
                    context_summary="No sample rows available for context validation"
                )
                result.total_original += count
                result.total_validated += count
                continue

            # Scale to original count if we sampled
            sample_size = len(sample_rows) if sample_rows else len(indices)
            if sample_size > 0 and sample_size < count:
                scale_factor = count / sample_size
                validated_count = int(validated_count * scale_factor)
                explained_count = int(explained_count * scale_factor)

            # Build context summary
            context_summary = self._build_context_summary(col, explanation_reasons)

            result.columns[col] = ValidatedOutlierResult(
                column=col,
                column_display_name=self.context.get_field_display_name(col),
                original_count=count,
                validated_count=validated_count,
                explained_count=explained_count,
                confidence=validated_count / count if count > 0 else 0,
                sample_explanations=explanations[:10],
                context_summary=context_summary
            )

            result.total_original += count
            result.total_validated += validated_count
            result.total_explained += explained_count

        result.context_patterns_used = list(patterns_used)

        return result

    def _to_numeric(self, value: Any) -> Optional[float]:
        """Convert value to numeric, handling strings from sample_rows."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                # Handle 'nan' string
                if value.lower() == 'nan':
                    return None
                return float(value)
            except ValueError:
                return None
        return None

    def _validate_single(
        self,
        col: str,
        value: Any,
        row: Dict[str, Any]
    ) -> OutlierExplanation:
        """
        Validate a single outlier against context patterns.

        Returns OutlierExplanation with is_explained=True if the value
        is explained by discovered patterns (i.e., NOT a real outlier).
        """
        explanations = []
        patterns_checked = 0
        patterns_matched = 0
        suspicion = 1.0  # Start fully suspicious

        # Ensure value is numeric
        value = self._to_numeric(value)
        if value is None:
            return OutlierExplanation(
                row_index=-1,
                value=None,
                column=col,
                is_explained=True,  # Can't evaluate, don't flag
                confidence=0.0,
                explanations=["Value could not be converted to numeric"],
                patterns_checked=0,
                patterns_matched=0
            )

        # Check 1: Subgroup patterns
        for pattern in self.context.get_subgroups_for_column(col):
            patterns_checked += 1
            segment_value = row.get(pattern.segment_col)

            if segment_value is not None:
                is_explained, explanation, confidence = pattern.explain_value(
                    segment_value, value, self.context.field_descriptions
                )
                if is_explained:
                    patterns_matched += 1
                    explanations.append(explanation)
                    # Strongly reduce suspicion when value is within group bounds
                    # If variance_explained > 20% and value is within bounds, trust it
                    if pattern.variance_explained >= 0.2:
                        suspicion *= 0.2  # Strong reduction for significant patterns
                    else:
                        suspicion *= 0.5  # Moderate reduction for weaker patterns

        # Check 2: Correlation patterns
        for pattern in self.context.get_correlations_for_column(col):
            patterns_checked += 1

            # Determine which column is the predictor
            if pattern.col1 == col:
                other_col = pattern.col2
                other_value = self._to_numeric(row.get(other_col))
                if other_value is not None:
                    # Reverse prediction (predict col1 from col2)
                    # Use inverse of the linear relationship
                    if pattern.slope != 0:
                        predicted = (value - pattern.intercept) / pattern.slope
                        error = abs(other_value - predicted)
                        if pattern.tolerance > 0 and error <= 2 * pattern.tolerance:
                            patterns_matched += 1
                            col1_desc = self.context.field_descriptions.get(pattern.col1, FieldDescription(pattern.col1, pattern.col1))
                            col2_desc = self.context.field_descriptions.get(pattern.col2, FieldDescription(pattern.col2, pattern.col2))
                            explanation = f"{col1_desc.friendly_name} correlates with {col2_desc.friendly_name} (r={pattern.correlation:.2f})"
                            explanations.append(explanation)
                            suspicion *= 0.5
            else:
                other_col = pattern.col1
                other_value = self._to_numeric(row.get(other_col))
                if other_value is not None:
                    is_explained, explanation, confidence = pattern.explain_value(
                        other_value, value, self.context.field_descriptions
                    )
                    if is_explained:
                        patterns_matched += 1
                        explanations.append(explanation)
                        suspicion *= (1.0 - confidence * 0.5)

        # Determine if explained
        is_explained = False

        if self.require_multiple_checks:
            # Require either: low suspicion OR multiple pattern matches
            is_explained = suspicion < self.min_confidence_to_flag or patterns_matched >= 2
        else:
            is_explained = suspicion < self.min_confidence_to_flag

        return OutlierExplanation(
            row_index=row.get('index', -1),
            value=value,
            column=col,
            is_explained=is_explained,
            confidence=1.0 - suspicion,  # Confidence it's NOT an outlier
            explanations=explanations,
            patterns_checked=patterns_checked,
            patterns_matched=patterns_matched
        )

    def _build_context_summary(self, col: str, explanation_reasons: List[str]) -> str:
        """Build human-readable summary of what explained the outliers."""
        if not explanation_reasons:
            return ""

        # Count unique reason types
        reason_counts = {}
        for reason in explanation_reasons:
            # Extract the key part of the reason
            key = reason.split('(')[0].strip()
            reason_counts[key] = reason_counts.get(key, 0) + 1

        # Build summary
        col_display = self.context.get_field_display_name(col)
        parts = []

        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
            if count > 1:
                parts.append(f"{reason} ({count}×)")
            else:
                parts.append(reason)

        if parts:
            return f"{col_display}: " + "; ".join(parts[:3])

        return ""

    def _passthrough_result(self, outliers: Dict[str, Dict[str, Any]]) -> ContextValidationResult:
        """Create passthrough result when validation can't be performed."""
        result = ContextValidationResult()

        for col, outlier_data in outliers.items():
            if not isinstance(outlier_data, dict):
                continue

            count = outlier_data.get('anomaly_count', outlier_data.get('count', 0))
            result.columns[col] = ValidatedOutlierResult(
                column=col,
                column_display_name=col,
                original_count=count,
                validated_count=count,
                explained_count=0,
                confidence=1.0,
                context_summary=""
            )
            result.total_original += count
            result.total_validated += count

        return result


def validate_outliers_with_context(
    outliers: Dict[str, Dict[str, Any]],
    df,
    field_descriptions: Optional[Dict[str, Dict[str, str]]] = None,
    min_variance_explained: float = 0.20,
    min_correlation: float = 0.5
) -> Tuple[ContextValidationResult, ContextStore]:
    """
    Convenience function to discover context and validate outliers in one call.

    Args:
        outliers: Dict mapping column names to outlier detection results
        df: Original DataFrame
        field_descriptions: Optional dict of field friendly names/descriptions
        min_variance_explained: Threshold for subgroup pattern discovery
        min_correlation: Threshold for correlation pattern discovery

    Returns:
        Tuple of (ContextValidationResult, ContextStore)
    """
    from validation_framework.profiler.context_discovery import ContextDiscovery

    # Discover context
    discovery = ContextDiscovery(
        field_descriptions=field_descriptions,
        min_variance_explained=min_variance_explained,
        min_correlation=min_correlation
    )
    context = discovery.discover(df)

    # Validate outliers
    validator = ContextualValidator(context)
    result = validator.validate(outliers, df)

    return result, context
