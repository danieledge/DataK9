"""
Type Inferrer - Intelligent Data Type Detection and Inference.

This module provides robust type detection and inference capabilities for data
profiling. It analyzes values to determine their semantic types (integer, float,
boolean, date, string, etc.) with confidence scoring.

Architecture:
    TypeInferrer follows a multi-strategy approach:
    1. Direct type checking (Python native types)
    2. Coercion-based detection (try parsing as float/date)
    3. Pattern matching (regex for dates, emails, etc.)
    4. Statistical inference (majority type across sampled values)

Design Decisions:
    - Array-like values checked FIRST to avoid pandas "ambiguous truth value" error
    - Boolean detection includes string representations ('true', 'yes', etc.)
    - Mixed numeric/string columns default to string (safer for identifiers like
      ticket numbers that may contain both "A/5 21171" and "21171")
    - Date patterns pre-compiled as regex for performance

Usage:
    inferrer = TypeInferrer()
    value_type = inferrer.detect_type("2024-01-15")  # Returns 'date'
    column_type = inferrer.infer_column_type(profile_data, row_count)

Extracted from DataProfiler to follow Single Responsibility Principle and
enable independent testing of type inference logic.
"""

import logging
import re
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np

from validation_framework.profiler.profile_result import TypeInference

logger = logging.getLogger(__name__)

# Optional visions library for enhanced semantic type detection (email, URL, UUID, etc.)
try:
    from visions.functional import detect_type
    from visions.types import Float, Integer, String, Boolean, Object
    VISIONS_AVAILABLE = True
except ImportError:
    VISIONS_AVAILABLE = False
    logger.debug("Visions library not available - using fallback type inference")


class TypeInferrer:
    """
    Intelligent type detection and inference for data columns.

    This class handles both individual value type detection and aggregate
    column type inference with confidence scoring. It uses a priority-based
    detection strategy that handles edge cases like mixed-type columns.

    Attributes:
        DATE_PATTERNS: Regex patterns for common date formats (ISO, US, EU).
        _date_regexes: Pre-compiled regex objects for performance.

    Example:
        >>> inferrer = TypeInferrer()
        >>> inferrer.detect_type(42)
        'integer'
        >>> inferrer.detect_type("2024-01-15")
        'date'
        >>> inferrer.detect_type([1, 2, 3])
        'array'
    """

    # Common date patterns for detection
    DATE_PATTERNS = [
        r'^\d{4}-\d{2}-\d{2}',  # ISO date (2024-01-15)
        r'^\d{2}/\d{2}/\d{4}',  # US date (01/15/2024)
        r'^\d{2}-\d{2}-\d{4}',  # EU date (15-01-2024)
        r'^\d{4}/\d{2}/\d{2}',  # Alternative ISO (2024/01/15)
    ]

    def __init__(self):
        """Initialize the type inferrer."""
        # Pre-compile date patterns for performance
        self._date_regexes = [re.compile(p) for p in self.DATE_PATTERNS]

    def detect_type(self, value: Any) -> str:
        """
        Detect the type of a single value.

        Args:
            value: The value to detect type for

        Returns:
            Type string: 'integer', 'float', 'boolean', 'date', 'string', 'array', 'null'
        """
        # Check if value is array-like (list, numpy array, etc.)
        # This must be checked BEFORE pd.isna() to avoid "ambiguous truth value" error
        if isinstance(value, (list, np.ndarray)) or hasattr(value, '__array__'):
            return 'array'

        # Check for null
        if pd.isna(value):
            return 'null'

        # Boolean
        if isinstance(value, bool) or str(value).lower() in ['true', 'false', 'yes', 'no']:
            return 'boolean'

        # Try numeric
        try:
            float_val = float(value)
            if float_val.is_integer():
                return 'integer'
            return 'float'
        except (ValueError, TypeError):
            pass

        # Try date
        value_str = str(value)
        if self._is_date_like(value_str):
            return 'date'

        # Default to string
        return 'string'

    def _is_date_like(self, value: str) -> bool:
        """Check if string looks like a date."""
        for regex in self._date_regexes:
            if regex.match(value):
                return True
        return False

    def infer_column_type(
        self,
        profile_data: Dict[str, Any],
        total_rows: int
    ) -> TypeInference:
        """
        Infer type for a column with confidence level.

        Confidence based on:
        - Consistency of detected types
        - Presence of declared schema
        - Percentage of values matching inferred type

        Args:
            profile_data: Dictionary containing column profile data
            total_rows: Total number of rows in the dataset

        Returns:
            TypeInference object with inferred type and confidence
        """
        declared_type = profile_data.get("declared_type")
        type_counts = profile_data.get("type_counts", {})
        null_count = profile_data.get("null_count", 0)

        # Handle empty column
        if not type_counts:
            return TypeInference(
                declared_type=declared_type,
                inferred_type="empty",
                confidence=1.0 if declared_type else 0.0,
                is_known=declared_type is not None,
                sample_values=[]
            )

        # Get most common type
        non_null_count = total_rows - null_count
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        primary_type, primary_count = sorted_types[0]

        # Calculate confidence
        if declared_type:
            # If schema declares type, it's known
            confidence = 1.0
            is_known = True
            inferred = declared_type
        else:
            # Confidence = percentage of SAMPLED values matching primary type
            # Use type_sampled_count instead of total rows for accurate confidence with sampling
            type_sampled_count = profile_data.get("type_sampled_count", non_null_count)
            confidence = primary_count / type_sampled_count if type_sampled_count > 0 else 0.0
            is_known = False
            inferred = primary_type

        # Detect type conflicts
        conflicts = []
        type_sampled_count_for_conflicts = profile_data.get("type_sampled_count", non_null_count)
        for typ, count in sorted_types[1:4]:  # Top 3 conflicts
            if count > type_sampled_count_for_conflicts * 0.01:  # At least 1% of sampled data
                conflicts.append({
                    "type": typ,
                    "count": count,
                    "percentage": round(100 * count / type_sampled_count_for_conflicts, 2)
                })

        # Handle mixed types: if numeric is primary but there's significant string content,
        # treat as string (e.g., Ticket column with "A/5 21171" and "21171" mixed)
        if inferred in ['integer', 'float'] and not is_known:
            string_count = type_counts.get('string', 0)
            string_pct = (string_count / type_sampled_count_for_conflicts * 100) if type_sampled_count_for_conflicts > 0 else 0
            # If at least 5% of values are strings, treat as string (mixed alphanumeric)
            if string_pct >= 5:
                inferred = 'string'
                confidence = (string_count + primary_count) / type_sampled_count_for_conflicts if type_sampled_count_for_conflicts > 0 else 0.0

        # Log type inference summary with conflicts (DEBUG level)
        col_name = profile_data.get("column_name", "unknown")
        if conflicts and confidence < 0.95:
            # Log when confidence is low due to type conflicts
            conflict_summary = ", ".join([f"{c['type']} ({c['percentage']}%)" for c in conflicts])
            logger.debug(
                f"Type inference for '{col_name}': "
                f"primary={inferred} ({confidence*100:.1f}% confidence), "
                f"conflicts=[{conflict_summary}], "
                f"sampled={type_sampled_count_for_conflicts:,} values"
            )

        return TypeInference(
            declared_type=declared_type,
            inferred_type=inferred,
            confidence=confidence,
            is_known=is_known,
            type_conflicts=conflicts,
            sample_values=profile_data.get("sample_values", [])[:10]
        )

    def detect_semantic_type_with_visions(self, sample_values: List[Any]) -> Optional[str]:
        """
        Use visions library to detect semantic type from sample values.

        Args:
            sample_values: List of sample values to analyze

        Returns:
            Semantic type string like 'email', 'url', 'uuid', 'integer', 'float', etc.
            Returns None if visions is not available or detection fails.
        """
        if not VISIONS_AVAILABLE or not sample_values:
            return None

        try:
            # Use first 100 samples for visions detection (memory efficient)
            sample = pd.Series(sample_values[:100])
            detected_type = detect_type(sample)
            type_name = detected_type.__name__ if hasattr(detected_type, '__name__') else str(detected_type)

            # Map visions types to our semantic types
            type_mapping = {
                'EmailAddress': 'email',
                'URL': 'url',
                'UUID': 'uuid',
                'IPAddress': 'ip_address',
                'PhoneNumber': 'phone_number',
                'Integer': 'integer',
                'Float': 'float',
                'Boolean': 'boolean',
                'Categorical': 'category',
                'String': 'string',
                'Object': 'object'
            }

            # Return mapped type or original type_name in lowercase
            if type_name is None:
                return None
            return type_mapping.get(type_name, type_name.lower())

        except Exception as e:
            logger.debug(f"Visions type detection failed: {e}")
            return None

    def extract_pattern(self, value: str) -> str:
        """
        Extract pattern from string value.

        Replaces:
        - Digits with '9'
        - Letters with 'A'
        - Special chars remain as-is

        Example: "ABC-123" -> "AAA-999"

        Args:
            value: String value to extract pattern from

        Returns:
            Pattern string
        """
        if len(value) > 50:
            value = value[:50]  # Limit length

        pattern = []
        for char in value:
            if char.isdigit():
                pattern.append('9')
            elif char.isalpha():
                pattern.append('A')
            else:
                pattern.append(char)

        return ''.join(pattern)
