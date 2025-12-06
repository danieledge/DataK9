"""
Validation Suggestion Generator - Intelligent Validation Rule Recommendations.

This module analyzes profiled column data to generate contextually relevant
validation suggestions. It uses pattern recognition, semantic analysis, and
statistical heuristics to recommend appropriate data quality checks.

Architecture:
    ValidationSuggestionGenerator employs multiple strategies:
    1. Type-based suggestions (numeric ranges, date formats)
    2. Cardinality analysis (valid values for enums, uniqueness for keys)
    3. Pattern detection (regex for consistent formats)
    4. Semantic awareness (FIBO for finance, Schema.org for web data)
    5. Statistical heuristics (outlier detection for high-variance columns)

Design Decisions:
    - ID vs Measurement distinction: Uses regex patterns to avoid suggesting
      range checks on identifier columns (e.g., customer_id, account_number)
    - Confidence scoring: Each suggestion has a confidence percentage based on
      how strongly the data supports the recommendation
    - Deduplication: When multiple strategies suggest the same validation,
      only the highest-confidence version is kept
    - Binary detection: Fields with exactly 2 values that look boolean get
      BooleanCheck instead of ValidValuesCheck

Suggestion Categories:
    - File-level: EmptyFileCheck, RowCountRangeCheck
    - Completeness: MandatoryFieldCheck (for >95% complete fields)
    - Type validation: RangeCheck, DateFormatCheck, NumericPrecisionCheck
    - Cardinality: ValidValuesCheck, UniqueKeyCheck, BooleanCheck
    - Format: FormatCheck, RegexCheck, StringLengthCheck
    - Statistical: StatisticalOutlierCheck

Usage:
    generator = ValidationSuggestionGenerator()
    suggestions = generator.generate_suggestions(columns, row_count)
    for s in suggestions:
        print(f"{s.validation_type}: {s.reason} (confidence: {s.confidence}%)")

Extracted from DataProfiler to follow Single Responsibility Principle.
"""

import logging
import re
from typing import List, Dict, Any, Optional, Tuple

from validation_framework.profiler.profile_result import (
    ColumnProfile,
    ValidationSuggestion,
)
from validation_framework.reference_data import ReferenceDataLoader

logger = logging.getLogger(__name__)


class ValidationSuggestionGenerator:
    """
    Intelligent validation rule recommendation engine.

    Analyzes column profiles to suggest appropriate data quality validations.
    Uses pattern matching, semantic analysis, and statistical heuristics to
    provide contextually relevant suggestions with confidence scores.

    Attributes:
        _id_patterns: Regex patterns identifying columns that are likely identifiers
            (should NOT receive range checks). Loaded from ReferenceDataLoader.
        _measurement_patterns: Regex patterns identifying measurement columns
            (SHOULD receive range checks). Loaded from ReferenceDataLoader.
        _id_regex: Pre-compiled regex combining all ID patterns.
        _measurement_regex: Pre-compiled regex combining all measurement patterns.

    Example:
        >>> generator = ValidationSuggestionGenerator()
        >>> suggestions = generator.generate_suggestions(column_profiles, row_count=1000)
        >>> for s in suggestions:
        ...     print(f"{s.validation_type}: {s.confidence}%")
        MandatoryFieldCheck: 95.0%
        RangeCheck: 90.0%
        ValidValuesCheck: 85.0%
    """

    # Default fallback patterns (used if external JSON not available)
    _DEFAULT_ID_PATTERNS = [
        r'_id$', r'^id_', r'^id$',
        r'_code$', r'^code_',
        r'account', r'customer',
    ]

    _DEFAULT_MEASUREMENT_PATTERNS = [
        r'amount', r'price', r'cost',
        r'quantity', r'qty', r'count',
    ]

    def __init__(self):
        """Initialize the validation suggestion generator."""
        # Load patterns from external JSON via ReferenceDataLoader
        self._id_patterns = ReferenceDataLoader.get_id_patterns()
        if not self._id_patterns:
            logger.warning("No ID patterns loaded from JSON, using defaults")
            self._id_patterns = self._DEFAULT_ID_PATTERNS

        self._measurement_patterns = ReferenceDataLoader.get_measurement_patterns()
        if not self._measurement_patterns:
            logger.warning("No measurement patterns loaded from JSON, using defaults")
            self._measurement_patterns = self._DEFAULT_MEASUREMENT_PATTERNS

        logger.debug(
            f"ValidationSuggestionGenerator initialized: "
            f"{len(self._id_patterns)} ID patterns, "
            f"{len(self._measurement_patterns)} measurement patterns"
        )

        # Compile regex patterns for performance
        self._id_regex = re.compile('|'.join(self._id_patterns), re.IGNORECASE)
        self._measurement_regex = re.compile('|'.join(self._measurement_patterns), re.IGNORECASE)

    def generate_suggestions(
        self,
        columns: List[ColumnProfile],
        row_count: int,
        enable_ml_suggestions: bool = False,
        ml_analyzer: Any = None
    ) -> List[ValidationSuggestion]:
        """
        Generate validation suggestions based on column profiles.

        Args:
            columns: List of column profiles
            row_count: Total row count
            enable_ml_suggestions: Whether to include ML-based suggestions
            ml_analyzer: Optional ML analyzer for advanced suggestions

        Returns:
            List of validation suggestions
        """
        suggestions = []

        # File-level suggestions
        suggestions.extend(self._generate_file_level_suggestions(row_count))

        # Column-level suggestions
        mandatory_fields = []
        for col in columns:
            # Mandatory field check for high completeness
            if col.quality.completeness > 95:
                mandatory_fields.append(col.name)

            # Type-specific suggestions
            suggestions.extend(self._generate_type_suggestions(col, row_count))

            # Cardinality-based suggestions
            suggestions.extend(self._generate_cardinality_suggestions(col))

            # Pattern-based suggestions
            suggestions.extend(self._generate_pattern_suggestions(col))

            # Semantic-based suggestions (FIBO)
            suggestions.extend(self._generate_fibo_semantic_validations(col))

            # Schema.org semantic suggestions
            suggestions.extend(self._generate_semantic_type_validations(col))

            # Temporal suggestions
            suggestions.extend(self._generate_temporal_validations(col))

            # Statistical suggestions
            suggestions.extend(self._generate_statistical_validations(col, row_count))

            # String pattern validations
            suggestions.extend(self._generate_string_pattern_validations(col))

        # Add mandatory field check if we have mandatory fields
        if mandatory_fields:
            suggestions.append(ValidationSuggestion(
                validation_type="MandatoryFieldCheck",
                severity="ERROR",
                params={"fields": mandatory_fields},
                reason=f"Fields with >95% completeness: {', '.join(mandatory_fields[:5])}{'...' if len(mandatory_fields) > 5 else ''}",
                confidence=95.0
            ))

        # Deduplicate suggestions
        suggestions = self._deduplicate_suggestions(suggestions)

        # ML-based suggestions (if enabled)
        if enable_ml_suggestions and ml_analyzer:
            ml_suggestions = self._generate_ml_based_validations(columns, row_count, ml_analyzer)
            suggestions.extend(ml_suggestions)

        return suggestions

    def _generate_file_level_suggestions(self, row_count: int) -> List[ValidationSuggestion]:
        """Generate file-level validation suggestions."""
        suggestions = []

        if row_count > 0:
            suggestions.append(ValidationSuggestion(
                validation_type="EmptyFileCheck",
                severity="ERROR",
                params={},
                reason="Prevent empty file loads",
                confidence=100.0
            ))

            suggestions.append(ValidationSuggestion(
                validation_type="RowCountRangeCheck",
                severity="ERROR",
                params={
                    "min_rows": max(1, int(row_count * 0.5)),
                    "max_rows": int(row_count * 2)
                },
                reason=f"Expect approximately {row_count} rows based on profile",
                confidence=80.0
            ))

        return suggestions

    def _generate_type_suggestions(
        self,
        col: ColumnProfile,
        row_count: int
    ) -> List[ValidationSuggestion]:
        """Generate type-specific validation suggestions."""
        suggestions = []

        # Range check for numeric fields (but not IDs)
        if col.type_info.inferred_type in ["integer", "float"]:
            should_suggest_range = self._should_suggest_range_check(col, row_count)

            if should_suggest_range:
                if col.statistics.min_value is not None and col.statistics.max_value is not None:
                    schema_org_type = None
                    if col.semantic_info:
                        schema_org_type = col.semantic_info.get('schema_org', {}).get('type', '')

                    min_val, max_val, reason = self._calculate_smart_range(
                        col.name, col.statistics.min_value, col.statistics.max_value, schema_org_type
                    )

                    suggestions.append(ValidationSuggestion(
                        validation_type="RangeCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "min_value": min_val,
                            "max_value": max_val
                        },
                        reason=reason,
                        confidence=90.0
                    ))

        return suggestions

    def _generate_cardinality_suggestions(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate suggestions based on cardinality analysis."""
        suggestions = []

        # Valid values for low cardinality
        if col.statistics.cardinality < 0.05 and col.statistics.unique_count < 20:
            valid_values = [item["value"] for item in col.statistics.top_values]

            if valid_values:
                schema_org_type = None
                if col.semantic_info:
                    schema_org_type = col.semantic_info.get('schema_org', {}).get('type', '')

                is_binary_flag = (
                    col.statistics.unique_count == 2 and
                    (schema_org_type == 'schema:Boolean' or
                     set(str(v).lower() for v in valid_values) <= {'0', '1', 'true', 'false', 'yes', 'no', 'y', 'n'})
                )

                if is_binary_flag:
                    suggestions.append(ValidationSuggestion(
                        validation_type="BooleanCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "true_values": [1, "1", "true", "yes", "y", "True", "Yes", "Y"],
                            "false_values": [0, "0", "false", "no", "n", "False", "No", "N"]
                        },
                        reason="Binary flag field (boolean values)",
                        confidence=90.0
                    ))
                else:
                    suggestions.append(ValidationSuggestion(
                        validation_type="ValidValuesCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "valid_values": valid_values
                        },
                        reason=f"Low cardinality ({col.statistics.unique_count} unique values)",
                        confidence=85.0
                    ))

        # Uniqueness check for high cardinality (but NOT for datetime/timestamp fields)
        # Timestamps are naturally unique but not good primary keys - exclude them
        if col.statistics.cardinality > 0.99 and col.quality.completeness > 99:
            # Exclude datetime columns - they're naturally unique but not identifiers
            inferred_type = col.type_info.inferred_type.lower() if col.type_info.inferred_type else ""
            col_name_lower = col.name.lower()

            is_datetime = inferred_type in ['datetime', 'date', 'timestamp']
            is_datetime_by_name = any(
                term in col_name_lower
                for term in ['timestamp', 'date', 'time', '_at', '_on', 'created', 'updated', 'modified']
            )

            if not (is_datetime or is_datetime_by_name):
                suggestions.append(ValidationSuggestion(
                    validation_type="UniqueKeyCheck",
                    severity="WARNING",
                    params={"fields": [col.name]},
                    reason="High cardinality suggests unique identifier",
                    confidence=80.0
                ))

        return suggestions

    def _generate_pattern_suggestions(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate pattern-based validation suggestions."""
        suggestions = []

        # Patterns are stored in col.statistics.pattern_samples (not type_info)
        detected_patterns = getattr(col.statistics, 'pattern_samples', None)

        if col.type_info.inferred_type == "string" and detected_patterns:
            dominant_pattern = detected_patterns[0] if detected_patterns else None

            if dominant_pattern and dominant_pattern.get("percentage", 0) > 90:
                pattern = dominant_pattern.get("pattern", "")

                # Convert pattern to regex if possible
                format_string = self._pattern_to_format_string(pattern)
                if format_string:
                    suggestions.append(ValidationSuggestion(
                        validation_type="FormatCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "pattern": format_string
                        },
                        reason=f"Consistent pattern detected ({dominant_pattern.get('percentage', 0):.0f}% match)",
                        confidence=85.0
                    ))

        return suggestions

    def _should_suggest_range_check(self, col: ColumnProfile, row_count: int) -> bool:
        """
        Determine if a range check should be suggested for a numeric column.

        Uses pattern matching to distinguish IDs from measurements.
        """
        col_name_lower = col.name.lower()

        # Check if it matches ID patterns (should NOT suggest range)
        if self._id_regex.search(col_name_lower):
            logger.debug(f"Skipping range check for ID-like column: {col.name}")
            return False

        # Check if it matches measurement patterns (SHOULD suggest range)
        if self._measurement_regex.search(col_name_lower):
            return True

        # For ambiguous cases, use statistical heuristics
        if col.statistics.min_value is not None and col.statistics.max_value is not None:
            value_range = col.statistics.max_value - col.statistics.min_value

            # IDs typically have sequential or near-sequential values
            # Measurements typically have more variance
            if col.statistics.unique_count and row_count > 0:
                uniqueness_ratio = col.statistics.unique_count / row_count

                # High uniqueness + sequential-ish values = likely ID
                if uniqueness_ratio > 0.9 and col.statistics.min_value >= 0:
                    expected_sequential_range = col.statistics.unique_count - 1
                    if abs(value_range - expected_sequential_range) < expected_sequential_range * 0.1:
                        logger.debug(f"Skipping range check for sequential ID-like column: {col.name}")
                        return False

        # Default: suggest range check for numeric fields
        return True

    def _calculate_smart_range(
        self,
        col_name: str,
        min_val: float,
        max_val: float,
        schema_org_type: Optional[str] = None
    ) -> Tuple[float, float, str]:
        """
        Calculate smart range bounds with semantic awareness.

        Returns:
            Tuple of (min_bound, max_bound, reason)
        """
        value_range = max_val - min_val
        col_lower = col_name.lower()

        # Semantic-aware adjustments
        if schema_org_type:
            if 'Price' in schema_org_type or 'MonetaryAmount' in schema_org_type:
                # Prices/amounts can't be negative, but NO upper bound (amounts can grow)
                return (0, None, f"Price/amount field: must be non-negative")

            if 'Percentage' in schema_org_type:
                return (0, 100, "Percentage field: 0-100%")

            if 'Rating' in schema_org_type:
                # Common rating scales
                if max_val <= 5:
                    return (0, 5, "Rating field: 0-5 scale")
                elif max_val <= 10:
                    return (0, 10, "Rating field: 0-10 scale")

        # Name-based detection for amounts (financial values are unbounded upward)
        # These fields should only have non-negative constraint, not upper bound
        amount_patterns = ['amount', 'price', 'cost', 'value', 'total', 'payment', 'balance']
        if any(p in col_lower for p in amount_patterns):
            return (0, None, f"Financial amount field: must be non-negative")

        # Default: expand range by 20%
        buffer = value_range * 0.2 if value_range > 0 else abs(max_val) * 0.2

        suggested_min = min_val - buffer
        suggested_max = max_val + buffer

        # Don't allow negative for likely-positive fields
        if any(p in col_lower for p in ['count', 'quantity', 'qty', 'age', 'size']):
            suggested_min = max(0, suggested_min)

        return (
            suggested_min,
            suggested_max,
            f"Based on observed range {min_val:.2f} to {max_val:.2f} (+20% buffer)"
        )

    def _generate_fibo_semantic_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate validations based on FIBO semantic tags."""
        suggestions = []

        if not col.semantic_info:
            return suggestions

        fibo_info = col.semantic_info.get('fibo', {})
        if not fibo_info or not fibo_info.get('tag'):
            return suggestions

        fibo_tag = fibo_info.get('tag', '')
        confidence = fibo_info.get('confidence', 0)

        # Only suggest validations for high-confidence semantic matches
        if confidence < 70:
            return suggestions

        # FIBO-specific validation mappings
        if 'AccountIdentifier' in fibo_tag:
            suggestions.append(ValidationSuggestion(
                validation_type="MandatoryFieldCheck",
                severity="ERROR",
                params={"fields": [col.name]},
                reason=f"FIBO: Account identifiers must not be null ({fibo_tag})",
                confidence=confidence
            ))

        if 'MonetaryAmount' in fibo_tag or 'CurrencyAmount' in fibo_tag:
            suggestions.append(ValidationSuggestion(
                validation_type="NumericPrecisionCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "max_decimal_places": 2
                },
                reason=f"FIBO: Monetary amounts typically have 2 decimal places ({fibo_tag})",
                confidence=confidence * 0.8
            ))

        if 'Date' in fibo_tag or 'DateTime' in fibo_tag:
            suggestions.append(ValidationSuggestion(
                validation_type="DateFormatCheck",
                severity="WARNING",
                params={"field": col.name},
                reason=f"FIBO: Date field should have consistent format ({fibo_tag})",
                confidence=confidence * 0.9
            ))

        return suggestions

    def _generate_semantic_type_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate validations based on Schema.org semantic types."""
        suggestions = []

        if not col.semantic_info:
            return suggestions

        schema_info = col.semantic_info.get('schema_org', {})
        if not schema_info or not schema_info.get('type'):
            return suggestions

        schema_type = schema_info.get('type', '')
        confidence = schema_info.get('confidence', 0)

        if confidence < 60:
            return suggestions

        # Schema.org type-specific validations
        if schema_type == 'schema:Email':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                },
                reason="Schema.org: Email format validation",
                confidence=confidence
            ))

        if schema_type == 'schema:URL':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r"^https?://[^\s/$.?#].[^\s]*$"
                },
                reason="Schema.org: URL format validation",
                confidence=confidence
            ))

        if schema_type == 'schema:PostalCode':
            suggestions.append(ValidationSuggestion(
                validation_type="StringLengthCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "min_length": 3,
                    "max_length": 10
                },
                reason="Schema.org: Postal code length validation",
                confidence=confidence * 0.8
            ))

        return suggestions

    def _generate_temporal_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate temporal-specific validations."""
        suggestions = []

        if col.type_info.inferred_type not in ['datetime', 'date']:
            return suggestions

        # Patterns are stored in col.statistics.pattern_samples
        detected_patterns = getattr(col.statistics, 'pattern_samples', None)

        # Suggest date format validation
        if detected_patterns:
            dominant = detected_patterns[0]
            if dominant.get('percentage', 0) > 80:
                date_format = self._infer_date_format(col.type_info.sample_values, detected_patterns)
                if date_format:
                    suggestions.append(ValidationSuggestion(
                        validation_type="DateFormatCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "format": date_format
                        },
                        reason=f"Consistent date format detected: {date_format}",
                        confidence=85.0
                    ))

        # Temporal analysis suggestions
        if hasattr(col, 'temporal_analysis') and col.temporal_analysis:
            temporal = col.temporal_analysis

            # Suggest business day check if weekday-heavy
            if temporal.get('weekday_distribution'):
                weekday_pct = sum(temporal['weekday_distribution'].get(d, 0) for d in range(5))
                if weekday_pct > 90:
                    suggestions.append(ValidationSuggestion(
                        validation_type="BusinessDayCheck",
                        severity="INFO",
                        params={"field": col.name},
                        reason="Data concentrated on weekdays (>90%)",
                        confidence=75.0
                    ))

        return suggestions

    def _generate_statistical_validations(
        self,
        col: ColumnProfile,
        row_count: int
    ) -> List[ValidationSuggestion]:
        """Generate statistics-based validations."""
        suggestions = []

        if col.type_info.inferred_type not in ['integer', 'float']:
            return suggestions

        stats = col.statistics

        # Outlier detection suggestion for skewed distributions
        if stats.std_dev and stats.mean:
            cv = abs(stats.std_dev / stats.mean) if stats.mean != 0 else 0

            if cv > 1.5:  # High coefficient of variation
                suggestions.append(ValidationSuggestion(
                    validation_type="StatisticalOutlierCheck",
                    severity="INFO",
                    params={
                        "field": col.name,
                        "method": "zscore",
                        "threshold": 3.0
                    },
                    reason=f"High variance detected (CV={cv:.2f}), outlier check recommended",
                    confidence=70.0
                ))

        return suggestions

    def _generate_string_pattern_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate string pattern validations."""
        suggestions = []

        if col.type_info.inferred_type != 'string':
            return suggestions

        # String length validation based on observed lengths
        if col.statistics.min_length is not None and col.statistics.max_length is not None:
            # Only suggest if there's a reasonable constraint
            if col.statistics.max_length < 1000 and col.statistics.min_length > 0:
                # Add some buffer
                min_len = max(1, col.statistics.min_length - 1)
                max_len = col.statistics.max_length + 10

                suggestions.append(ValidationSuggestion(
                    validation_type="StringLengthCheck",
                    severity="WARNING",
                    params={
                        "field": col.name,
                        "min_length": min_len,
                        "max_length": max_len
                    },
                    reason=f"String length range: {col.statistics.min_length}-{col.statistics.max_length} chars",
                    confidence=75.0
                ))

        return suggestions

    def _generate_ml_based_validations(
        self,
        columns: List[ColumnProfile],
        row_count: int,
        ml_analyzer: Any
    ) -> List[ValidationSuggestion]:
        """Generate ML-based validation suggestions."""
        suggestions = []

        # This would integrate with the ML analyzer for advanced suggestions
        # Currently a placeholder for future implementation

        return suggestions

    def _deduplicate_suggestions(
        self,
        suggestions: List[ValidationSuggestion]
    ) -> List[ValidationSuggestion]:
        """Remove duplicate suggestions, keeping highest confidence."""
        seen = {}

        for suggestion in suggestions:
            key = (suggestion.validation_type, str(suggestion.params))

            if key not in seen or suggestion.confidence > seen[key].confidence:
                seen[key] = suggestion

        return list(seen.values())

    def _pattern_to_format_string(self, pattern: str) -> Optional[str]:
        """Convert a detected pattern to a regex format string."""
        # Pattern mapping for common formats
        pattern_map = {
            'NNNN-NN-NN': r'^\d{4}-\d{2}-\d{2}$',  # Date
            'NN/NN/NNNN': r'^\d{2}/\d{2}/\d{4}$',  # Date
            'AAAA-NNNN': r'^[A-Za-z]{4}-\d{4}$',   # Code pattern
            'NNN-NN-NNNN': r'^\d{3}-\d{2}-\d{4}$', # SSN-like
        }

        return pattern_map.get(pattern)

    def _infer_date_format(
        self,
        sample_values: List[Any],
        detected_patterns: List[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Infer date format from sample values."""
        if not sample_values:
            return None

        # Common date format patterns
        format_tests = [
            ('%Y-%m-%d', r'^\d{4}-\d{2}-\d{2}$'),
            ('%d/%m/%Y', r'^\d{2}/\d{2}/\d{4}$'),
            ('%m/%d/%Y', r'^\d{2}/\d{2}/\d{4}$'),
            ('%Y/%m/%d', r'^\d{4}/\d{2}/\d{2}$'),
            ('%d-%m-%Y', r'^\d{2}-\d{2}-\d{4}$'),
        ]

        for sample in sample_values[:10]:
            sample_str = str(sample)
            for fmt, pattern in format_tests:
                if re.match(pattern, sample_str):
                    return fmt

        return None
