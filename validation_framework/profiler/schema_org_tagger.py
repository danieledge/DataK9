"""
Schema.org Semantic Tagger for DataK9 Profiler.

Assigns general-purpose semantic types based on Schema.org vocabulary.
This provides a universal semantic layer that applies to any domain,
while FIBO provides specialized financial semantics that override
when confidence is higher.

Schema.org is a collaborative community activity with a mission to create,
maintain, and promote schemas for structured data on the Internet.
https://schema.org/

Enhanced Detection (v1.1):
- Ticket-like identifiers (booking refs, PNRs) → schema:identifier
- Cabin/seat/location codes → schema:PropertyValue
- Short category codes (ports, regions) → schema:CategoryCode
- Generic code-like fallback for alphanumeric patterns
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)


# Compiled patterns for code-like field detection (performance optimization)
TICKET_NAME_PATTERN = re.compile(
    r'(?i)(ticket|booking|reservation|pnr|booking_ref|ticket_no|confirmation|itinerary|voucher|receipt)',
    re.IGNORECASE
)
CABIN_NAME_PATTERN = re.compile(
    r'(?i)(cabin|berth|room|seat|compartment|bay|deck|section|zone|block|unit)',
    re.IGNORECASE
)
PORT_CODE_NAME_PATTERN = re.compile(
    r'(?i)(port|embark|station|origin|dest|destination|terminal|loc|location|airport|code|hub)',
    re.IGNORECASE
)

# Value pattern for ticket-like identifiers (alphanumeric with common separators)
TICKET_VALUE_PATTERN = re.compile(r'^[A-Z0-9]+[A-Z0-9/.\-_ ]+$', re.IGNORECASE)
# Value pattern for cabin/location codes (letter + digits pattern)
CABIN_VALUE_PATTERN = re.compile(r'^[A-Z]\d+[A-Z]?$', re.IGNORECASE)
# Extended cabin pattern (allows spaces)
CABIN_VALUE_EXTENDED = re.compile(r'^[A-Z]\d+\s?[A-Z]?$', re.IGNORECASE)
# Short uppercase code pattern
SHORT_CODE_PATTERN = re.compile(r'^[A-Z][A-Z0-9]{0,3}$')


class SchemaOrgTagger:
    """
    Assigns Schema.org semantic types to columns using name patterns,
    data types, and statistical properties.

    This tagger always runs and provides a baseline semantic classification
    for every column. FIBO semantics may override when they have higher
    confidence for financial/ID-like fields.
    """

    # Minimum confidence thresholds
    SCHEMA_MIN_CONF = 0.5

    def __init__(self, taxonomy_path: Optional[str] = None):
        """
        Initialize Schema.org tagger.

        Args:
            taxonomy_path: Path to schema_org_taxonomy.json (default: auto-detect)
        """
        self.taxonomy = self._load_taxonomy(taxonomy_path)
        self.tag_definitions = self._flatten_taxonomy()
        logger.info(f"Loaded {len(self.tag_definitions)} Schema.org semantic definitions")

    def _load_taxonomy(self, taxonomy_path: Optional[str] = None) -> Dict[str, Any]:
        """Load Schema.org taxonomy from JSON file."""
        if taxonomy_path is None:
            current_dir = Path(__file__).parent
            taxonomy_path = current_dir / "taxonomies" / "schema_org_taxonomy.json"

        try:
            with open(taxonomy_path, 'r') as f:
                taxonomy = json.load(f)
                logger.debug(f"Loaded Schema.org taxonomy from {taxonomy_path}")
                return taxonomy
        except Exception as e:
            logger.warning(f"Failed to load Schema.org taxonomy from {taxonomy_path}: {e}")
            return {"metadata": {}, "taxonomy": {}}

    def _flatten_taxonomy(self) -> Dict[str, Dict[str, Any]]:
        """
        Flatten hierarchical taxonomy into a lookup dict.

        Returns:
            Dict mapping schema types to their definitions
        """
        flattened = {}
        taxonomy = self.taxonomy.get("taxonomy", {})

        for category, category_data in taxonomy.items():
            tags = category_data.get("tags", {})
            for tag_name, tag_def in tags.items():
                flattened[tag_name] = {
                    **tag_def,
                    "category": category
                }

        return flattened

    def tag_column(
        self,
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any,
        sample_values: Optional[List[Any]] = None
    ) -> Dict[str, Any]:
        """
        Assign Schema.org semantic type to a column.

        Args:
            column_name: Name of the column
            inferred_type: DataK9 inferred type (integer, float, string, etc.)
            statistics: ColumnStatistics object
            quality: QualityMetrics object
            sample_values: Optional sample values for pattern detection

        Returns:
            Dict with schema_org semantic info:
            {
                "type": "schema:MonetaryAmount",
                "confidence": 0.85,
                "signals": ["name_pattern:amount", "dtype:float", "non_negative"],
                "display_label": "Monetary amount"
            }
        """
        candidates = []

        # Phase 1: Enhanced code-like field detection (tickets, cabins, short codes)
        # This runs FIRST to catch code-like patterns that taxonomy may miss
        code_like_result = self._detect_code_like_field(
            column_name, inferred_type, statistics, sample_values
        )

        # Phase 1b: Binary flag detection (0/1 integers or Y/N strings)
        binary_result = self._detect_binary_flag(
            column_name, inferred_type, statistics, sample_values
        )
        if binary_result:
            return binary_result

        # Score each schema type from taxonomy
        for schema_type, tag_def in self.tag_definitions.items():
            score, signals = self._score_candidate(
                schema_type,
                tag_def,
                column_name,
                inferred_type,
                statistics,
                quality,
                sample_values
            )

            if score > 0:
                candidates.append({
                    "type": schema_type,
                    "confidence": score,
                    "signals": signals,
                    "display_label": tag_def.get("display_label", schema_type.replace("schema:", "")),
                    "priority": tag_def.get("priority", 50)
                })

        # Sort by confidence * priority weight, then by priority
        candidates.sort(key=lambda c: (c["confidence"] * (c["priority"] / 100), c["priority"]), reverse=True)

        # Phase 2: Compare taxonomy result with enhanced code-like detection
        taxonomy_result = None
        if candidates and candidates[0]["confidence"] >= self.SCHEMA_MIN_CONF:
            best = candidates[0]
            taxonomy_result = {
                "type": best["type"],
                "confidence": round(best["confidence"], 3),
                "signals": best["signals"],
                "display_label": best["display_label"]
            }

        # Decision: Use code-like result if it's better than taxonomy
        # Code-like detection overrides generic schema:Text with higher confidence
        if code_like_result:
            code_conf = code_like_result.get("confidence", 0)

            # If no taxonomy result, use code-like result
            if not taxonomy_result:
                return code_like_result

            # If taxonomy gave generic Text and code-like is confident, prefer code-like
            if taxonomy_result["type"] == "schema:Text" and code_conf >= 0.6:
                logger.debug(f"Enhanced detection override for {column_name}: "
                           f"{code_like_result['type']} (conf={code_conf:.2f})")
                return code_like_result

            # If code-like confidence is significantly higher, prefer it
            if code_conf > taxonomy_result["confidence"] + 0.1:
                logger.debug(f"Enhanced detection override for {column_name}: "
                           f"{code_like_result['type']} (conf={code_conf:.2f}) "
                           f"over {taxonomy_result['type']} (conf={taxonomy_result['confidence']:.2f})")
                return code_like_result

        # Use taxonomy result if available
        if taxonomy_result:
            return taxonomy_result

        # Fallback based on dtype
        fallback_type, fallback_label = self._get_fallback_type(inferred_type)
        return {
            "type": fallback_type,
            "confidence": 0.3,
            "signals": [f"fallback_dtype:{inferred_type}"],
            "display_label": fallback_label
        }

    def _score_candidate(
        self,
        schema_type: str,
        tag_def: Dict[str, Any],
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any,
        sample_values: Optional[List[Any]]
    ) -> Tuple[float, List[str]]:
        """
        Score how well a column matches a schema type.

        Returns:
            (score, signals) tuple
        """
        score = 0.0
        signals = []

        # Pattern matching on column name (strongest signal)
        patterns = tag_def.get("patterns", [])
        for pattern in patterns:
            try:
                if re.search(pattern, column_name, re.IGNORECASE):
                    score += 0.5
                    signals.append(f"name_pattern:{pattern[:30]}")
                    break  # Only count one pattern match
            except re.error:
                continue

        # Data type matching
        data_props = tag_def.get("data_properties", {})
        expected_types = data_props.get("type", [])
        if expected_types and inferred_type in expected_types:
            score += 0.2
            signals.append(f"dtype:{inferred_type}")
        elif expected_types and inferred_type not in expected_types:
            # Type mismatch - reduce score
            score -= 0.3

        # Cardinality checks
        cardinality = getattr(statistics, 'cardinality', None)
        unique_count = getattr(statistics, 'unique_count', None)

        if 'cardinality_min' in data_props and cardinality is not None:
            if cardinality >= data_props['cardinality_min']:
                score += 0.15
                signals.append(f"high_cardinality:{cardinality:.2f}")
            else:
                score -= 0.1

        if 'cardinality_max' in data_props and cardinality is not None:
            if cardinality <= data_props['cardinality_max']:
                score += 0.15
                signals.append(f"low_cardinality:{cardinality:.2f}")
            else:
                score -= 0.1

        if 'unique_count_max' in data_props and unique_count is not None:
            if unique_count <= data_props['unique_count_max']:
                score += 0.2
                signals.append(f"unique_count:{unique_count}")

        # Value range checks
        min_val = getattr(statistics, 'min_value', None)
        max_val = getattr(statistics, 'max_value', None)

        if 'min_value' in data_props and min_val is not None:
            try:
                if float(min_val) >= data_props['min_value']:
                    score += 0.1
                    signals.append("non_negative")
            except (ValueError, TypeError):
                pass

        # String length checks
        avg_length = getattr(statistics, 'avg_length', None)
        if 'avg_length_min' in data_props and avg_length is not None:
            if avg_length >= data_props['avg_length_min']:
                score += 0.15
                signals.append(f"long_text:{avg_length:.0f}")

        # Value pattern checks (for sample values)
        value_patterns = tag_def.get("value_patterns", [])
        if value_patterns and sample_values:
            str_samples = [str(v) for v in sample_values[:100] if v is not None]
            for vp in value_patterns:
                try:
                    match_count = sum(1 for s in str_samples if re.search(vp, s))
                    if match_count > len(str_samples) * 0.5:
                        score += 0.3
                        signals.append(f"value_pattern:{vp[:20]}")
                        break
                except re.error:
                    continue

        return max(0, min(1, score)), signals

    def _get_fallback_type(self, inferred_type: str) -> Tuple[str, str]:
        """
        Get fallback Schema.org type based on dtype.

        Returns:
            (schema_type, display_label)
        """
        fallback_map = {
            "integer": ("schema:Integer", "Integer"),
            "float": ("schema:Number", "Numeric value"),
            "decimal": ("schema:Number", "Numeric value"),
            "string": ("schema:Text", "Text"),
            "boolean": ("schema:Boolean", "Boolean"),
            "datetime": ("schema:DateTime", "Date/Time"),
            "date": ("schema:Date", "Date"),
            "time": ("schema:Time", "Time"),
        }
        return fallback_map.get(inferred_type, ("schema:Text", "Text"))

    def _detect_binary_flag(
        self,
        column_name: str,
        inferred_type: str,
        statistics: Any,
        sample_values: Optional[List[Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect binary flag columns (0/1, Y/N, True/False, Yes/No).

        These are common patterns for boolean-like data stored as integers or strings.

        Returns:
            Dict with type, confidence, signals, display_label if detected
            None if not a binary flag pattern
        """
        signals = []

        # Get unique count and min/max
        unique_count = getattr(statistics, 'unique_count', None)
        min_val = getattr(statistics, 'min_value', None)
        max_val = getattr(statistics, 'max_value', None)

        # Must have exactly 2 unique values (or 1 if all same)
        if unique_count is None or unique_count > 2:
            return None

        # Check for integer 0/1 pattern
        if inferred_type == 'integer':
            try:
                min_int = int(min_val) if min_val is not None else None
                max_int = int(max_val) if max_val is not None else None

                if min_int == 0 and max_int == 1:
                    signals.append("binary_values:0/1")
                    signals.append("dtype:integer")

                    # Boost confidence if name suggests flag/indicator
                    confidence = 0.75
                    flag_patterns = [
                        r'(?i)(flag|is_|has_|can_|should_|active|enabled|disabled)',
                        r'(?i)(survived|success|failed|valid|approved|deleted)',
                        r'(?i)(status|indicator|bool|binary|yn|y_n)'
                    ]
                    for pattern in flag_patterns:
                        if re.search(pattern, column_name):
                            confidence = 0.85
                            signals.append(f"name_pattern:{pattern[:25]}")
                            break

                    return {
                        "type": "schema:Boolean",
                        "confidence": confidence,
                        "signals": signals,
                        "display_label": "Boolean flag"
                    }
            except (ValueError, TypeError):
                pass

        # Check for string Y/N, Yes/No, True/False patterns
        if inferred_type == 'string' and sample_values:
            str_vals = set(str(v).lower().strip() for v in sample_values if v is not None)

            binary_string_patterns = [
                {'y', 'n'}, {'yes', 'no'}, {'true', 'false'},
                {'t', 'f'}, {'1', '0'}, {'on', 'off'},
                {'active', 'inactive'}, {'enabled', 'disabled'}
            ]

            for pattern_set in binary_string_patterns:
                if str_vals.issubset(pattern_set) or str_vals == pattern_set:
                    signals.append(f"binary_values:{'/'.join(sorted(str_vals))}")
                    signals.append("dtype:string")

                    return {
                        "type": "schema:Boolean",
                        "confidence": 0.80,
                        "signals": signals,
                        "display_label": "Boolean flag"
                    }

        return None

    def _detect_code_like_field(
        self,
        column_name: str,
        inferred_type: str,
        statistics: Any,
        sample_values: Optional[List[Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Enhanced detection for code-like fields using generic heuristics.

        Detects:
        1. Ticket-like identifiers (high uniqueness, alphanumeric patterns)
        2. Cabin/location codes (letter+digit patterns)
        3. Short category codes (low cardinality, uppercase)

        Returns:
            Dict with type, confidence, signals, display_label if detected
            None if no code-like pattern detected
        """
        # Only process string-like types
        if inferred_type not in ('string', 'object', 'category'):
            return None

        # Extract statistics safely
        unique_count = getattr(statistics, 'unique_count', None)
        row_count = getattr(statistics, 'row_count', None) or getattr(statistics, 'count', None)
        avg_length = getattr(statistics, 'avg_length', None)
        median_length = getattr(statistics, 'median_length', None) or avg_length

        # Calculate uniqueness ratio
        uniqueness_ratio = 0.0
        if unique_count and row_count and row_count > 0:
            uniqueness_ratio = unique_count / row_count

        # Get sample values for pattern analysis
        str_samples = []
        if sample_values:
            str_samples = [str(v).strip() for v in sample_values[:100] if v is not None and str(v).strip()]

        # 1. TICKET-LIKE IDENTIFIER DETECTION
        ticket_result = self._detect_ticket_identifier(
            column_name, uniqueness_ratio, str_samples, median_length
        )
        if ticket_result:
            return ticket_result

        # 2. CABIN/LOCATION CODE DETECTION
        cabin_result = self._detect_cabin_code(
            column_name, uniqueness_ratio, str_samples, median_length, unique_count
        )
        if cabin_result:
            return cabin_result

        # 3. SHORT CATEGORY CODE DETECTION
        category_result = self._detect_short_category_code(
            column_name, uniqueness_ratio, str_samples, unique_count, median_length
        )
        if category_result:
            return category_result

        # 4. GENERIC CODE-LIKE FALLBACK
        generic_result = self._detect_generic_code(
            uniqueness_ratio, str_samples, unique_count, median_length
        )
        if generic_result:
            return generic_result

        return None

    def _detect_ticket_identifier(
        self,
        column_name: str,
        uniqueness_ratio: float,
        str_samples: List[str],
        median_length: Optional[float]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect ticket-like identifiers (booking refs, PNRs, etc.).

        Criteria:
        - Name matches ticket pattern OR strong ID-like value pattern
        - High uniqueness (>= 0.8)
        - Contains digits AND (letters OR /-._ separators)
        """
        signals = []

        # Check name pattern
        name_match = bool(TICKET_NAME_PATTERN.search(column_name))
        if name_match:
            signals.append(f"ticket_name_pattern:{column_name}")

        # Check value patterns
        if str_samples:
            # Count values that look like tickets (alphanumeric with common separators)
            ticket_like_count = 0
            has_digits_and_alpha = 0

            for val in str_samples:
                if not val:
                    continue
                has_digit = any(c.isdigit() for c in val)
                has_alpha = any(c.isalpha() for c in val)
                has_separator = any(c in '/.-_ ' for c in val)

                if has_digit and (has_alpha or has_separator):
                    has_digits_and_alpha += 1

                if TICKET_VALUE_PATTERN.match(val):
                    ticket_like_count += 1

            ticket_pattern_ratio = ticket_like_count / len(str_samples) if str_samples else 0
            mixed_pattern_ratio = has_digits_and_alpha / len(str_samples) if str_samples else 0

            if ticket_pattern_ratio > 0.5:
                signals.append(f"ticket_value_pattern:{ticket_pattern_ratio:.2f}")
            if mixed_pattern_ratio > 0.6:
                signals.append("alphanumeric_mixed")

        # Decision logic
        # Strong ticket-like: name match + high uniqueness
        if name_match and uniqueness_ratio >= 0.7:
            signals.append(f"high_uniqueness:{uniqueness_ratio:.2f}")
            return {
                "type": "schema:identifier",
                "confidence": min(0.92, 0.75 + uniqueness_ratio * 0.2),
                "signals": signals,
                "display_label": "Identifier"
            }

        # Moderate ticket-like: strong value pattern + very high uniqueness
        if len(signals) >= 2 and uniqueness_ratio >= 0.8:
            signals.append(f"high_uniqueness:{uniqueness_ratio:.2f}")
            return {
                "type": "schema:identifier",
                "confidence": min(0.88, 0.70 + uniqueness_ratio * 0.2),
                "signals": signals,
                "display_label": "Identifier"
            }

        return None

    def _detect_cabin_code(
        self,
        column_name: str,
        uniqueness_ratio: float,
        str_samples: List[str],
        median_length: Optional[float],
        unique_count: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect cabin/location/seat codes.

        Criteria:
        - Name matches cabin pattern OR strong cabin value pattern
        - Moderate uniqueness (0.1 - 0.8)
        - Short to medium length (2-8 chars)
        - Pattern: letter + digits (+ optional letter)
        """
        signals = []

        # Check name pattern
        name_match = bool(CABIN_NAME_PATTERN.search(column_name))
        if name_match:
            signals.append(f"cabin_name_pattern:{column_name}")

        # Check median length is in cabin range
        length_ok = median_length is not None and 1 <= median_length <= 10
        if length_ok:
            signals.append(f"short_length:{median_length:.1f}")

        # Check value patterns (letter + digits)
        cabin_pattern_count = 0
        if str_samples:
            for val in str_samples:
                if not val:
                    continue
                # Must have at least one letter (not purely numeric)
                has_letter = any(c.isalpha() for c in val)
                has_digit = any(c.isdigit() for c in val)

                if has_letter and has_digit:
                    if CABIN_VALUE_PATTERN.match(val) or CABIN_VALUE_EXTENDED.match(val):
                        cabin_pattern_count += 1

            cabin_pattern_ratio = cabin_pattern_count / len(str_samples) if str_samples else 0
            if cabin_pattern_ratio > 0.4:
                signals.append(f"cabin_value_pattern:{cabin_pattern_ratio:.2f}")

        # Decision logic
        # Moderate uniqueness for cabin codes (not too unique, not too repetitive)
        uniqueness_ok = 0.05 <= uniqueness_ratio <= 0.85

        if name_match and length_ok and uniqueness_ok:
            signals.append(f"moderate_uniqueness:{uniqueness_ratio:.2f}")
            return {
                "type": "schema:PropertyValue",
                "confidence": 0.78,
                "signals": signals,
                "display_label": "Property code"
            }

        # Strong cabin pattern without name match
        if "cabin_value_pattern" in str(signals) and length_ok and uniqueness_ok:
            signals.append(f"moderate_uniqueness:{uniqueness_ratio:.2f}")
            return {
                "type": "schema:PropertyValue",
                "confidence": 0.72,
                "signals": signals,
                "display_label": "Property code"
            }

        return None

    def _detect_short_category_code(
        self,
        column_name: str,
        uniqueness_ratio: float,
        str_samples: List[str],
        unique_count: Optional[int],
        median_length: Optional[float]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect short category codes (port codes, region codes, status codes).

        Criteria:
        - Very low cardinality (2-20 distinct values)
        - Short median length (1-4 chars)
        - Mostly uppercase letters or short alphanumerics
        - Not boolean (not just Y/N, 0/1, true/false)
        """
        signals = []

        # Check name pattern
        name_match = bool(PORT_CODE_NAME_PATTERN.search(column_name))
        if name_match:
            signals.append(f"port_code_name_pattern:{column_name}")

        # Check cardinality (low = category-like)
        low_cardinality = unique_count is not None and 2 <= unique_count <= 25
        if low_cardinality:
            signals.append(f"low_cardinality:{unique_count}")

        # Check length (short codes)
        short_length = median_length is not None and 1 <= median_length <= 5
        if short_length:
            signals.append(f"very_short_length:{median_length:.1f}")

        # Check if values are short uppercase codes
        uppercase_code_count = 0
        boolean_like = False
        if str_samples:
            # Check for boolean-like patterns
            unique_vals = set(v.upper() for v in str_samples if v)
            boolean_patterns = [
                {'Y', 'N'}, {'YES', 'NO'}, {'TRUE', 'FALSE'},
                {'0', '1'}, {'T', 'F'}, {'ACTIVE', 'INACTIVE'}
            ]
            boolean_like = unique_vals in boolean_patterns or len(unique_vals) <= 2

            for val in str_samples:
                if not val:
                    continue
                if SHORT_CODE_PATTERN.match(val):
                    uppercase_code_count += 1
                # Also count short alphanumeric codes
                elif len(val) <= 4 and val.replace('-', '').replace('_', '').isalnum():
                    uppercase_code_count += 1

            code_pattern_ratio = uppercase_code_count / len(str_samples) if str_samples else 0
            if code_pattern_ratio > 0.7:
                signals.append(f"short_code_pattern:{code_pattern_ratio:.2f}")

        # Decision logic
        if boolean_like:
            return None  # Let Boolean detection handle this

        # Strong category code: low cardinality + short length + pattern match
        if low_cardinality and short_length and "short_code_pattern" in str(signals):
            return {
                "type": "schema:CategoryCode",
                "confidence": 0.85,
                "signals": signals,
                "display_label": "Category code"
            }

        # Moderate category code: name match + low cardinality
        if name_match and low_cardinality and short_length:
            return {
                "type": "schema:CategoryCode",
                "confidence": 0.80,
                "signals": signals,
                "display_label": "Category code"
            }

        # Weak category code: just low cardinality and short codes
        if low_cardinality and short_length and len(signals) >= 2:
            return {
                "type": "schema:CategoryCode",
                "confidence": 0.72,
                "signals": signals,
                "display_label": "Category code"
            }

        return None

    def _detect_generic_code(
        self,
        uniqueness_ratio: float,
        str_samples: List[str],
        unique_count: Optional[int],
        median_length: Optional[float]
    ) -> Optional[Dict[str, Any]]:
        """
        Generic code-like fallback for fields that look like codes but don't
        match specific patterns.

        Criteria:
        - String type
        - Code-like (short length, limited character set, repeated patterns)
        - Moderate uniqueness (0.2 - 0.9)
        - Median length <= 8
        - Mostly alphanumeric with no spaces
        """
        signals = []

        # Check length
        if median_length is None or median_length > 10:
            return None

        signals.append(f"short_length:{median_length:.1f}")

        # Check that values are mostly alphanumeric without spaces
        if str_samples:
            alphanumeric_count = 0
            no_space_count = 0

            for val in str_samples:
                if not val:
                    continue
                # Check if alphanumeric (allowing common separators)
                cleaned = val.replace('-', '').replace('_', '').replace('.', '')
                if cleaned.isalnum():
                    alphanumeric_count += 1
                if ' ' not in val:
                    no_space_count += 1

            alphanumeric_ratio = alphanumeric_count / len(str_samples) if str_samples else 0
            no_space_ratio = no_space_count / len(str_samples) if str_samples else 0

            if alphanumeric_ratio < 0.7 or no_space_ratio < 0.8:
                return None  # Doesn't look code-like

            signals.append(f"alphanumeric:{alphanumeric_ratio:.2f}")

        # Decide based on uniqueness
        # High uniqueness = identifier-like
        if 0.7 <= uniqueness_ratio <= 0.98 and median_length <= 8:
            signals.append(f"high_uniqueness:{uniqueness_ratio:.2f}")
            return {
                "type": "schema:identifier",
                "confidence": 0.65,
                "signals": signals,
                "display_label": "Identifier"
            }

        # Low to moderate uniqueness = category-like
        if unique_count is not None and 2 <= unique_count <= 30:
            if 0.01 <= uniqueness_ratio <= 0.5:
                signals.append(f"low_uniqueness:{uniqueness_ratio:.2f}")
                return {
                    "type": "schema:CategoryCode",
                    "confidence": 0.62,
                    "signals": signals,
                    "display_label": "Category code"
                }

        return None

    def get_display_label(self, schema_type: str) -> str:
        """Get human-readable display label for a schema type."""
        if schema_type in self.tag_definitions:
            return self.tag_definitions[schema_type].get("display_label", schema_type)
        # Extract from type name
        return schema_type.replace("schema:", "").replace("_", " ").title()
