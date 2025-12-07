"""
Science Semantic Tagger for DataK9 Profiler.

Assigns scientific measurement semantic types based on QUDT, ChEBI, and UO ontologies.
This provides semantic coverage for scientific and laboratory domains including:
- Chemical compounds (acids, alcohols, phenols, minerals)
- Physical properties (pH, density, temperature, pressure)
- Biological measurements (cell counts, enzymes, hormones)
- Morphological measurements (length, area, volume, mass)

DATA SOURCES:
- QUDT (qudt.org) - CC BY 4.0
- ChEBI (ebi.ac.uk/chebi) - CC BY 4.0
- UO (Units Ontology) - CC BY 3.0
"""

import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)


class ScienceTagger:
    """
    Assigns semantic types to columns using science-derived taxonomy.

    Covers scientific measurement domains:
    - Chemistry: alcohols, acids, phenols, minerals, sugars
    - Physical: pH, density, temperature, pressure, conductivity
    - Biology: cell counts, enzymes, hormones, glucose
    - Morphology: length, area, volume, mass
    - Ratios: indices, percentages, dimensionless ratios

    DATA SOURCES:
    - QUDT (qudt.org) - CC BY 4.0
    - ChEBI (ebi.ac.uk/chebi) - CC BY 4.0
    - UO (Units Ontology) - CC BY 3.0
    """

    MIN_CONFIDENCE_THRESHOLD = 0.55

    def __init__(self, taxonomy_path: Optional[str] = None, min_confidence: float = None):
        """
        Initialize Science tagger.

        Args:
            taxonomy_path: Path to science_taxonomy.json (default: auto-detect)
            min_confidence: Minimum confidence threshold (default: 0.55)
        """
        self.taxonomy = self._load_taxonomy(taxonomy_path)
        self.tag_definitions = self._flatten_taxonomy()
        self.min_confidence = min_confidence if min_confidence is not None else self.MIN_CONFIDENCE_THRESHOLD
        logger.info(f"Loaded {len(self.tag_definitions)} Science semantic definitions")

    def _load_taxonomy(self, taxonomy_path: Optional[str] = None) -> Dict[str, Any]:
        """Load Science taxonomy from JSON file."""
        if taxonomy_path is None:
            current_dir = Path(__file__).parent
            taxonomy_path = current_dir / "taxonomies" / "science_taxonomy.json"

        try:
            with open(taxonomy_path, 'r') as f:
                taxonomy = json.load(f)
                logger.debug(f"Loaded Science taxonomy from {taxonomy_path}")
                return taxonomy
        except Exception as e:
            logger.warning(f"Failed to load Science taxonomy from {taxonomy_path}: {e}")
            return {"metadata": {}, "taxonomy": {}}

    def _flatten_taxonomy(self) -> Dict[str, Dict[str, Any]]:
        """Flatten hierarchical taxonomy into a lookup dict."""
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
        quality: Any = None,
        sample_values: Optional[List[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Assign Science semantic type to a column.

        Args:
            column_name: Name of the column
            inferred_type: DataK9 inferred type (integer, float, string, etc.)
            statistics: ColumnStatistics object
            quality: QualityMetrics object
            sample_values: Optional sample values for pattern detection

        Returns:
            Dict with science semantic info if match found:
            {
                "type": "chebi:Alcohol",
                "confidence": 0.85,
                "signals": ["name_pattern:alcohol", "dtype:float"],
                "display_label": "Alcohol content",
                "category": "chemistry",
                "unit_hint": "% v/v or g/L",
                "source": "science"
            }
            None if no match found above threshold
        """
        candidates = []

        for tag_name, tag_def in self.tag_definitions.items():
            score, signals = self._score_candidate(
                tag_name,
                tag_def,
                column_name,
                inferred_type,
                statistics,
                quality,
                sample_values
            )

            if score >= self.min_confidence:
                candidates.append({
                    "type": tag_name,
                    "confidence": score,
                    "signals": signals,
                    "display_label": tag_def.get("display_label", tag_name),
                    "category": tag_def.get("category"),
                    "unit_hint": tag_def.get("unit_hint"),
                    "definition": tag_def.get("definition"),
                    "chebi_id": tag_def.get("chebi_id"),
                    "qudt_id": tag_def.get("qudt_id"),
                    "obi_id": tag_def.get("obi_id")
                })

        if not candidates:
            return None

        # Sort by confidence, then by priority
        candidates.sort(
            key=lambda c: (
                c["confidence"],
                self.tag_definitions.get(c["type"], {}).get("priority", 50)
            ),
            reverse=True
        )
        best = candidates[0]

        return {
            "type": best["type"],
            "confidence": round(best["confidence"], 3),
            "signals": best["signals"],
            "display_label": best["display_label"],
            "category": best["category"],
            "unit_hint": best["unit_hint"],
            "definition": best["definition"],
            "source": "science",
            "ontology_id": best.get("chebi_id") or best.get("qudt_id") or best.get("obi_id")
        }

    def _score_candidate(
        self,
        tag_name: str,
        tag_def: Dict[str, Any],
        column_name: str,
        inferred_type: str,
        statistics: Any,
        quality: Any,
        sample_values: Optional[List[Any]]
    ) -> Tuple[float, List[str]]:
        """
        Score how well a column matches a Science semantic type.

        Returns:
            (score, signals) tuple
        """
        score = 0.0
        signals = []

        # Pattern matching on column name (strongest signal)
        patterns = tag_def.get("patterns", [])
        pattern_matched = False
        matched_pattern = None

        for pattern in patterns:
            try:
                if re.search(pattern, column_name, re.IGNORECASE):
                    # Higher score for more specific patterns
                    pattern_specificity = len(pattern) / 50  # normalize
                    score += 0.45 + min(0.15, pattern_specificity * 0.1)
                    matched_pattern = pattern[:30]
                    signals.append(f"name:{matched_pattern}")
                    pattern_matched = True
                    break
            except re.error:
                continue

        # If no pattern match, unlikely to be this type
        if not pattern_matched:
            return 0.0, signals

        # Data type matching
        data_props = tag_def.get("data_properties", {})
        expected_types = data_props.get("type", [])
        if expected_types and inferred_type in expected_types:
            score += 0.2
            signals.append(f"dtype:{inferred_type}")
        elif expected_types and inferred_type not in expected_types:
            score -= 0.25

        # Value range checks
        min_val = getattr(statistics, 'min_value', None)
        max_val = getattr(statistics, 'max_value', None)

        if 'min_value' in data_props and min_val is not None:
            try:
                if float(min_val) >= data_props['min_value']:
                    score += 0.08
                    signals.append(f"min_ok")
                else:
                    score -= 0.15
            except (ValueError, TypeError):
                pass

        if 'max_value' in data_props and max_val is not None:
            try:
                if float(max_val) <= data_props['max_value']:
                    score += 0.08
                    signals.append(f"max_ok")
                else:
                    score -= 0.15
            except (ValueError, TypeError):
                pass

        # Numeric check - science measurements should be numeric
        if inferred_type in ['float', 'decimal', 'integer']:
            score += 0.1
            signals.append("numeric")

        # Check for non-negative values (most measurements are non-negative)
        if 'min_value' in data_props and data_props['min_value'] >= 0:
            if min_val is not None:
                try:
                    if float(min_val) >= 0:
                        score += 0.05
                except (ValueError, TypeError):
                    pass

        return max(0, min(1, score)), signals

    def get_display_label(self, science_tag: str) -> str:
        """Get human-readable display label for a tag."""
        if science_tag in self.tag_definitions:
            return self.tag_definitions[science_tag].get("display_label", science_tag)
        return science_tag

    def get_unit_hint(self, science_tag: str) -> Optional[str]:
        """Get unit hint for a science tag."""
        if science_tag in self.tag_definitions:
            return self.tag_definitions[science_tag].get("unit_hint")
        return None

    def get_category(self, science_tag: str) -> Optional[str]:
        """Get category for a science tag."""
        if science_tag in self.tag_definitions:
            return self.tag_definitions[science_tag].get("category")
        return None

    def get_ontology_url(self, science_tag: str) -> Optional[str]:
        """Get ontology URL for a science tag."""
        if science_tag not in self.tag_definitions:
            return None

        tag_def = self.tag_definitions[science_tag]

        if tag_def.get("chebi_id"):
            chebi_id = tag_def["chebi_id"].replace("CHEBI:", "")
            return f"https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:{chebi_id}"
        elif tag_def.get("qudt_id"):
            qudt_id = tag_def["qudt_id"].replace("qudt:", "")
            return f"https://qudt.org/vocab/quantitykind/{qudt_id}"
        elif tag_def.get("obi_id"):
            obi_id = tag_def["obi_id"].replace("OBI:", "")
            return f"http://purl.obolibrary.org/obo/OBI_{obi_id}"

        return None
