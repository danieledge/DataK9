"""
Semantic tagging for data profiler columns.

Assigns semantic tags to columns based on FIBO taxonomy, Visions detection,
column name patterns, and data properties.

ATTRIBUTION:
This module uses semantic concepts derived from FIBO (Financial Industry Business
Ontology), maintained by the EDM Council under the MIT License.
https://spec.edmcouncil.org/fibo/

FIBO taxonomy location: validation_framework/profiler/taxonomies/finance_taxonomy.json
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from validation_framework.profiler.semantic_info import SemanticInfo

logger = logging.getLogger(__name__)


class SemanticTagger:
    """
    Assigns semantic tags to columns using multiple evidence sources.

    Evidence sources (in priority order):
    1. FIBO financial taxonomy (pattern + data property matching)
    2. Visions type detection (email, URL, UUID, phone, IP)
    3. SmartColumnAnalyzer hints (column name patterns)
    4. Data properties (range, cardinality, distribution)
    """

    def __init__(self, taxonomy_path: Optional[str] = None):
        """
        Initialize semantic tagger.

        Args:
            taxonomy_path: Path to finance_taxonomy.json (default: auto-detect)
        """
        self.taxonomy = self._load_taxonomy(taxonomy_path)
        self.tag_definitions = self._flatten_taxonomy()
        logger.info(f"Loaded {len(self.tag_definitions)} semantic tag definitions from FIBO taxonomy")

    def _load_taxonomy(self, taxonomy_path: Optional[str] = None) -> Dict[str, Any]:
        """Load financial taxonomy from JSON file."""
        if taxonomy_path is None:
            # Auto-detect path
            current_dir = Path(__file__).parent
            taxonomy_path = current_dir / "taxonomies" / "finance_taxonomy.json"

        try:
            with open(taxonomy_path, 'r') as f:
                taxonomy = json.load(f)
                logger.debug(f"Loaded taxonomy from {taxonomy_path}")
                return taxonomy
        except Exception as e:
            logger.warning(f"Failed to load taxonomy from {taxonomy_path}: {e}")
            return {"metadata": {}, "taxonomy": {}}

    def _flatten_taxonomy(self) -> Dict[str, Dict[str, Any]]:
        """
        Flatten hierarchical taxonomy into a lookup dict.

        Returns:
            Dict mapping tag names to their definitions
        """
        flattened = {}
        taxonomy = self.taxonomy.get("taxonomy", {})

        for category, category_data in taxonomy.items():
            tags = category_data.get("tags", {})
            for tag_name, tag_def in tags.items():
                flattened[tag_name] = {
                    **tag_def,
                    "category": category,
                    "fibo_module": category_data.get("fibo_module")
                }

        return flattened

    def tag_column(
        self,
        column_name: str,
        inferred_type: str,
        visions_type: Optional[str],
        statistics: Any,
        quality: Any
    ) -> SemanticInfo:
        """
        Assign semantic tags to a column based on all available evidence.

        Args:
            column_name: Name of the column
            inferred_type: DataK9 inferred type (integer, float, string, etc.)
            visions_type: Visions detected semantic type (if any)
            statistics: ColumnStatistics object
            quality: QualityMetrics object

        Returns:
            SemanticInfo with assigned tags and explanation
        """
        evidence = {}
        tags = []
        confidence_scores = []

        # Stage 1: Visions detection (high confidence for specific patterns)
        if visions_type:
            visions_tags, visions_conf = self._map_visions_type(visions_type)
            if visions_tags:
                tags.extend(visions_tags)
                confidence_scores.append(visions_conf)
                evidence['visions_type'] = visions_type
                logger.debug(f"Column '{column_name}': Visions detected {visions_type} → tags {visions_tags}")

        # Stage 2: FIBO taxonomy pattern matching
        fibo_matches = self._match_fibo_patterns(
            column_name,
            inferred_type,
            statistics,
            quality
        )

        for match in fibo_matches:
            tags.append(match['tag'])
            confidence_scores.append(match['confidence'])
            evidence['fibo_match'] = match['tag']
            evidence['fibo_class'] = match.get('fibo_class')
            logger.debug(f"Column '{column_name}': FIBO matched {match['tag']} (confidence: {match['confidence']:.2f})")

        # Stage 3: Data property refinement
        refined_tags = self._refine_with_data_properties(
            tags,
            column_name,
            inferred_type,
            statistics,
            quality
        )

        # Combine all tags and deduplicate
        all_tags = list(dict.fromkeys(tags + refined_tags))

        # Calculate overall confidence
        overall_confidence = max(confidence_scores) if confidence_scores else 0.0

        # Determine primary tag (most specific)
        primary_tag = self._select_primary_tag(all_tags)

        # Generate human-readable explanation
        explanation = self._generate_explanation(
            column_name,
            primary_tag,
            all_tags,
            evidence
        )

        # Get FIBO source if available
        fibo_source = None
        if primary_tag in self.tag_definitions:
            fibo_source = self.tag_definitions[primary_tag].get('fibo_class')

        return SemanticInfo(
            semantic_tags=all_tags,
            primary_tag=primary_tag,
            confidence=overall_confidence,
            explanation=explanation,
            evidence=evidence,
            fibo_source=fibo_source
        )

    def _map_visions_type(self, visions_type: str) -> Tuple[List[str], float]:
        """
        Map Visions types to DataK9 semantic tags.

        Args:
            visions_type: Visions detected type

        Returns:
            (list of tags, confidence score)
        """
        # Visions is very accurate for specific patterns
        visions_mapping = {
            'email': (['contact.email', 'pii.email'], 0.95),
            'phone_number': (['contact.phone', 'pii.phone'], 0.95),
            'url': (['identifier.url'], 0.95),
            'uuid': (['identifier.uuid'], 0.95),
            'ip_address': (['identifier.ip'], 0.90),
            'boolean': (['flag.binary'], 0.90),
        }

        return visions_mapping.get(visions_type, ([], 0.0))

    def _match_fibo_patterns(
        self,
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any
    ) -> List[Dict[str, Any]]:
        """
        Match column against FIBO taxonomy patterns.

        Returns:
            List of matches with confidence scores
        """
        matches = []

        for tag_name, tag_def in self.tag_definitions.items():
            # Check if column name matches any patterns
            patterns = tag_def.get('patterns', [])
            pattern_match = False
            for pattern in patterns:
                try:
                    if re.search(pattern, column_name):
                        pattern_match = True
                        break
                except re.error as e:
                    # Log invalid regex patterns but continue
                    logger.debug(f"Invalid regex pattern '{pattern}' for tag {tag_name}: {e}")
                    continue

            if not pattern_match:
                continue

            # Check data properties match
            data_props = tag_def.get('data_properties', {})
            props_match, confidence = self._check_data_properties(
                data_props,
                inferred_type,
                statistics,
                quality
            )

            if props_match:
                matches.append({
                    'tag': tag_name,
                    'confidence': confidence,
                    'fibo_class': tag_def.get('fibo_class'),
                    'definition': tag_def.get('definition')
                })

        return matches

    def _check_data_properties(
        self,
        expected_props: Dict[str, Any],
        inferred_type: str,
        statistics: Any,
        quality: Any
    ) -> Tuple[bool, float]:
        """
        Check if column data properties match expected properties.

        Returns:
            (matches, confidence_score)
        """
        if not expected_props:
            # No data property constraints
            return True, 0.70

        confidence = 1.0
        matches = []

        # Check type
        expected_types = expected_props.get('type', [])
        if expected_types:
            type_match = inferred_type in expected_types
            matches.append(type_match)
            if not type_match:
                confidence *= 0.5

        # Check min_value
        if 'min_value' in expected_props:
            expected_min = expected_props['min_value']
            actual_min = getattr(statistics, 'min_value', None)
            if actual_min is not None:
                min_match = actual_min >= expected_min
                matches.append(min_match)
                if not min_match:
                    confidence *= 0.7

        # Check max_value
        if 'max_value' in expected_props:
            expected_max = expected_props['max_value']
            actual_max = getattr(statistics, 'max_value', None)
            if actual_max is not None:
                max_match = actual_max <= expected_max
                matches.append(max_match)
                if not max_match:
                    confidence *= 0.7

        # Check cardinality
        if 'cardinality_min' in expected_props:
            expected_card = expected_props['cardinality_min']
            actual_card = getattr(statistics, 'cardinality', 0)
            card_match = actual_card >= expected_card
            matches.append(card_match)
            if not card_match:
                confidence *= 0.8

        if 'cardinality_max' in expected_props:
            expected_card = expected_props['cardinality_max']
            actual_card = getattr(statistics, 'cardinality', 1)
            card_match = actual_card <= expected_card
            matches.append(card_match)
            if not card_match:
                confidence *= 0.8

        # Check string length
        if 'string_length' in expected_props:
            min_len, max_len = expected_props['string_length']
            actual_min = getattr(statistics, 'min_length', None)
            actual_max = getattr(statistics, 'max_length', None)
            if actual_min is not None and actual_max is not None:
                len_match = (actual_min >= min_len and actual_max <= max_len)
                matches.append(len_match)
                if not len_match:
                    confidence *= 0.6

        # Overall match requires most properties to match
        if not matches:
            return True, 0.70  # No constraints

        match_ratio = sum(matches) / len(matches)
        overall_match = match_ratio >= 0.6  # 60% of properties must match

        return overall_match, confidence if overall_match else 0.0

    def _refine_with_data_properties(
        self,
        current_tags: List[str],
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any
    ) -> List[str]:
        """
        Refine tags based on data properties and context.

        For example:
        - Integer + high uniqueness + name contains 'id' → add id.* tag
        - Float + non-negative + name contains 'balance' → add money.amount
        """
        refined_tags = []

        # Get data properties
        cardinality = getattr(statistics, 'cardinality', 0)
        unique_pct = getattr(statistics, 'unique_percentage', 0)
        min_val = getattr(statistics, 'min_value', None)
        max_val = getattr(statistics, 'max_value', None)
        unique_count = getattr(statistics, 'unique_count', 0)

        # Rule: High uniqueness + numeric → might be an ID
        if cardinality > 0.80 and inferred_type in ['integer', 'float']:
            name_lower = column_name.lower()
            if any(kw in name_lower for kw in ['id', 'num', 'number', 'code']):
                if not any(tag.startswith('id.') for tag in current_tags):
                    # Add generic identifier tag
                    refined_tags.append('identifier.code')

        # Rule: Low cardinality numeric → might be categorical
        # BUT: Exclude count/quantity fields which are numeric measures even with low cardinality
        if unique_count <= 20 and inferred_type in ['integer', 'float']:
            name_lower = column_name.lower()
            # Check if this looks like a count/quantity field (should be analyzed as measure)
            count_keywords = ['count', 'cnt', 'num', 'qty', 'quantity', 'total', 'sum',
                              'sib', 'par', 'child', 'spouse', 'depend', 'member']
            is_count_field = any(kw in name_lower for kw in count_keywords)

            # Also check if values are sequential from 0 or 1 (typical of counts)
            is_sequential_from_zero = (min_val is not None and max_val is not None and
                                       min_val >= 0 and max_val < 20 and
                                       unique_count > (max_val - min_val) * 0.7)  # Most values present

            # Only tag as category if it doesn't look like a count field
            if not is_count_field and not is_sequential_from_zero:
                if not any(tag.startswith('category.') for tag in current_tags):
                    refined_tags.append('category')

        # Rule: Binary values (0/1 or True/False) → flag
        if unique_count == 2 and inferred_type in ['integer', 'boolean']:
            if not any(tag.startswith('flag.') for tag in current_tags):
                refined_tags.append('flag.binary')

        # Rule: Percentage values (0-100 or 0-1) → add measure.percentage
        if inferred_type == 'float' and min_val is not None and max_val is not None:
            if (min_val >= 0 and max_val <= 100) or (min_val >= 0 and max_val <= 1):
                name_lower = column_name.lower()
                if any(kw in name_lower for kw in ['pct', 'percent', 'rate', 'ratio']):
                    refined_tags.append('measure.percentage')

        return refined_tags

    def _select_primary_tag(self, tags: List[str]) -> str:
        """
        Select the most specific tag as primary.

        Priority: specific subtypes > general types > unknown

        Args:
            tags: List of semantic tags

        Returns:
            Primary tag (most specific)
        """
        if not tags:
            return "unknown"

        # Sort by specificity (more dots = more specific)
        sorted_tags = sorted(tags, key=lambda t: t.count('.'), reverse=True)

        return sorted_tags[0]

    def _generate_explanation(
        self,
        column_name: str,
        primary_tag: str,
        all_tags: List[str],
        evidence: Dict[str, Any]
    ) -> str:
        """
        Generate human-readable explanation of semantic classification.

        Args:
            column_name: Column name
            primary_tag: Primary semantic tag
            all_tags: All assigned tags
            evidence: Evidence dictionary

        Returns:
            Explanation string
        """
        parts = []

        # Get definition from taxonomy
        if primary_tag in self.tag_definitions:
            definition = self.tag_definitions[primary_tag].get('definition', '')
            if definition:
                parts.append(f"Column '{column_name}' represents {definition.lower()}")

        # Add evidence
        evidence_parts = []

        if 'visions_type' in evidence:
            evidence_parts.append(f"Visions detected as {evidence['visions_type']}")

        if 'fibo_match' in evidence:
            evidence_parts.append(f"matched FIBO pattern for {evidence['fibo_match']}")

        if 'fibo_class' in evidence:
            evidence_parts.append(f"mapped to FIBO class {evidence['fibo_class']}")

        if evidence_parts:
            parts.append("Evidence: " + ", ".join(evidence_parts))

        # Fallback
        if not parts:
            parts.append(f"Column '{column_name}' classified as {primary_tag}")

        return ". ".join(parts) + "."

    def get_validation_rules(self, semantic_tag: str) -> List[str]:
        """
        Get recommended validation rules for a semantic tag.

        Args:
            semantic_tag: Semantic tag (e.g., 'money.amount')

        Returns:
            List of validation rule names
        """
        if semantic_tag in self.tag_definitions:
            return self.tag_definitions[semantic_tag].get('validation_rules', [])

        return []

    def get_skip_validations(self, semantic_tag: str) -> List[str]:
        """
        Get validations that should be skipped for a semantic tag.

        Args:
            semantic_tag: Semantic tag (e.g., 'money.amount')

        Returns:
            List of validation rule names to skip
        """
        if semantic_tag in self.tag_definitions:
            return self.tag_definitions[semantic_tag].get('skip_validations', [])

        return []
