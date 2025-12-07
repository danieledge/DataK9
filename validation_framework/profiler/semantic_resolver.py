"""
Semantic Resolver for DataK9 Profiler.

Resolves the final semantic classification by combining Schema.org general
semantics with FIBO financial semantics, Science measurement semantics,
and Wikidata general knowledge.

Resolution Priority Order:
1. FIBO (highest) - Specialized financial/ID semantics
2. Science - Scientific measurements (QUDT, ChEBI, UO ontologies)
3. Wikidata - General knowledge types (geographic, reference, entity)
4. Schema.org (lowest) - Baseline web/general classification

Resolution Rules:
1. FIBO wins if fibo_conf >= FIBO_MIN_CONF and fibo_conf >= other confidences
2. Science wins if no strong FIBO and science_conf >= SCIENCE_MIN_CONF
3. Wikidata wins if no strong FIBO/Science and wikidata_conf >= WIKIDATA_MIN_CONF
4. Schema.org wins if no strong specialized match
5. Fallback to "none" if no layer has strong confidence

DATA SOURCES:
- FIBO (EDM Council) - Financial semantics
- QUDT (qudt.org) - Quantities, Units, Dimensions - CC BY 4.0
- ChEBI (ebi.ac.uk/chebi) - Chemical entities - CC BY 4.0
- UO (Units Ontology) - Units of measurement - CC BY 3.0
- Wikidata (wikidata.org) - General knowledge - CC0
- Schema.org - Web vocabulary - CC BY-SA 3.0
"""

import logging
from typing import Dict, Any, Optional

from validation_framework.profiler.semantic_config import get_semantic_config

logger = logging.getLogger(__name__)

# Resolution thresholds (defaults - actual values loaded from semantic_config.yaml)
FIBO_MIN_CONF = 0.7      # FIBO must have at least this confidence to override
SCIENCE_MIN_CONF = 0.6   # Science (QUDT/ChEBI) minimum confidence
WIKIDATA_MIN_CONF = 0.6  # Wikidata minimum confidence to override Schema.org
SCHEMA_MIN_CONF = 0.5    # Schema.org minimum confidence for valid classification


class SemanticResolver:
    """
    Resolves semantic classification from Schema.org, FIBO, Science, and Wikidata layers.

    Produces a unified semantic_info structure with:
    - structural_type: The pandas/numpy dtype
    - schema_org: Schema.org classification (always present as baseline)
    - fibo: FIBO classification (optional, for financial fields)
    - science: Science classification (optional, for scientific measurements)
    - wikidata: Wikidata classification (optional, for general knowledge types)
    - resolved: Final resolution with display_label and validation_driver
    """

    def __init__(
        self,
        fibo_min_conf: float = None,
        science_min_conf: float = None,
        wikidata_min_conf: float = None,
        schema_min_conf: float = None
    ):
        """
        Initialize the resolver.

        Resolution thresholds are loaded from semantic_config.yaml by default.
        Explicit parameters override config values for backward compatibility.

        Args:
            fibo_min_conf: Minimum FIBO confidence to override others
            science_min_conf: Minimum Science confidence to override Wikidata/Schema
            wikidata_min_conf: Minimum Wikidata confidence to override Schema.org
            schema_min_conf: Minimum Schema.org confidence for valid classification
        """
        # Load thresholds from config (or use defaults if config unavailable)
        config = get_semantic_config()
        resolution = config.resolution

        # Use config values, allow explicit overrides for backward compatibility
        self.fibo_min_conf = fibo_min_conf if fibo_min_conf is not None else resolution.fibo_min_conf
        self.science_min_conf = science_min_conf if science_min_conf is not None else resolution.science_min_conf
        self.wikidata_min_conf = wikidata_min_conf if wikidata_min_conf is not None else resolution.wikidata_min_conf
        self.schema_min_conf = schema_min_conf if schema_min_conf is not None else resolution.schema_min_conf

        logger.debug(
            f"SemanticResolver initialized with thresholds: "
            f"FIBO={self.fibo_min_conf}, Science={self.science_min_conf}, "
            f"Wikidata={self.wikidata_min_conf}, Schema={self.schema_min_conf}"
        )

    def resolve(
        self,
        structural_type: str,
        schema_org: Optional[Dict[str, Any]],
        fibo: Optional[Dict[str, Any]],
        wikidata: Optional[Dict[str, Any]] = None,
        science: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Resolve semantic classification from all layers.

        Args:
            structural_type: The pandas/numpy dtype (e.g., "float64", "object")
            schema_org: Schema.org classification dict with type, confidence, signals
            fibo: FIBO classification dict with type, confidence, signals (or None)
            wikidata: Wikidata classification dict with type, confidence, signals (or None)
            science: Science classification dict with type, confidence, signals (or None)

        Returns:
            Complete semantic_info structure:
            {
                "structural_type": "float64",
                "schema_org": {...},
                "fibo": {...} or None,
                "science": {...} or None,
                "wikidata": {...} or None,
                "resolved": {
                    "primary_source": "fibo" | "science" | "wikidata" | "schema_org" | "none",
                    "primary_type": "fibo:MoneyAmount" or "chebi:Alcohol" etc.,
                    "secondary_type": "schema:MonetaryAmount" or None,
                    "display_label": "Monetary amount (FIBO:MoneyAmount)",
                    "validation_driver": "fibo" | "science" | "wikidata" | "schema_org"
                }
            }
        """
        # Ensure schema_org has defaults
        if schema_org is None:
            schema_org = {
                "type": "schema:Text",
                "confidence": 0.3,
                "signals": ["fallback"],
                "display_label": "Text"
            }

        # Extract confidences
        schema_conf = schema_org.get("confidence", 0)
        schema_type = schema_org.get("type")
        schema_label = schema_org.get("display_label", "Unknown")

        fibo_conf = fibo.get("confidence", 0) if fibo else 0
        fibo_type = fibo.get("type") if fibo else None
        fibo_label = self._get_fibo_label(fibo_type) if fibo_type else None

        science_conf = science.get("confidence", 0) if science else 0
        science_type = science.get("type") if science else None
        science_label = science.get("display_label") if science else None

        wikidata_conf = wikidata.get("confidence", 0) if wikidata else 0
        wikidata_type = wikidata.get("type") if wikidata else None
        wikidata_label = wikidata.get("display_label") if wikidata else None

        # Resolution logic
        resolved = self._resolve_primary(
            schema_type, schema_conf, schema_label,
            fibo_type, fibo_conf, fibo_label,
            wikidata_type, wikidata_conf, wikidata_label,
            science_type, science_conf, science_label
        )

        return {
            "structural_type": structural_type,
            "schema_org": schema_org,
            "fibo": fibo,
            "science": science,
            "wikidata": wikidata,
            "resolved": resolved
        }

    def _resolve_primary(
        self,
        schema_type: str,
        schema_conf: float,
        schema_label: str,
        fibo_type: Optional[str],
        fibo_conf: float,
        fibo_label: Optional[str],
        wikidata_type: Optional[str] = None,
        wikidata_conf: float = 0,
        wikidata_label: Optional[str] = None,
        science_type: Optional[str] = None,
        science_conf: float = 0,
        science_label: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Determine which semantic layer is primary.

        Rules:
        1. FIBO wins if fibo_conf >= FIBO_MIN_CONF AND fibo_conf >= all others
        2. Science wins if no strong FIBO AND science_conf >= SCIENCE_MIN_CONF
        3. Wikidata wins if no strong FIBO/Science AND wikidata_conf >= WIKIDATA_MIN_CONF
        4. Schema.org wins otherwise (if confidence >= SCHEMA_MIN_CONF)
        5. Fallback to "none" if no layer is confident
        """
        # Rule 1: FIBO takes priority when strong (financial domain)
        if (fibo_type and
            fibo_conf >= self.fibo_min_conf and
            fibo_conf >= schema_conf and
            fibo_conf >= wikidata_conf and
            fibo_conf >= science_conf):

            # FIBO is primary, Schema.org is secondary
            display_label = self._build_display_label(
                primary_label=schema_label,
                primary_source="fibo",
                fibo_type=fibo_type
            )

            return {
                "primary_source": "fibo",
                "primary_type": fibo_type,
                "secondary_type": schema_type,
                "display_label": display_label,
                "validation_driver": "fibo"
            }

        # Rule 2: Science takes priority when FIBO is weak/absent (scientific measurements)
        if (science_type and
            science_conf >= self.science_min_conf and
            science_conf >= schema_conf and
            science_conf >= wikidata_conf):

            # Science is primary
            display_label = self._build_display_label(
                primary_label=science_label or schema_label,
                primary_source="science",
                science_type=science_type
            )

            return {
                "primary_source": "science",
                "primary_type": science_type,
                "secondary_type": schema_type,
                "display_label": display_label,
                "validation_driver": "science"
            }

        # Rule 3: Wikidata takes priority when FIBO/Science is weak/absent
        if (wikidata_type and
            wikidata_conf >= self.wikidata_min_conf and
            wikidata_conf >= schema_conf):

            # Wikidata is primary
            display_label = self._build_display_label(
                primary_label=wikidata_label or schema_label,
                primary_source="wikidata",
                wikidata_type=wikidata_type
            )

            return {
                "primary_source": "wikidata",
                "primary_type": wikidata_type,
                "secondary_type": schema_type,
                "display_label": display_label,
                "validation_driver": "wikidata"
            }

        # Rule 4: Schema.org is primary (all specialized layers weak or absent)
        if schema_conf >= self.schema_min_conf:
            # Determine best secondary type
            if fibo_type and fibo_conf > 0.3:
                secondary = fibo_type
            elif science_type and science_conf > 0.3:
                secondary = science_type
            elif wikidata_type and wikidata_conf > 0.3:
                secondary = wikidata_type
            else:
                secondary = None

            display_label = self._build_display_label(
                primary_label=schema_label,
                primary_source="schema_org",
                schema_type=schema_type
            )

            return {
                "primary_source": "schema_org",
                "primary_type": schema_type,
                "secondary_type": secondary,
                "display_label": display_label,
                "validation_driver": "schema_org"
            }

        # Rule 5: No layer is confident
        return {
            "primary_source": "none",
            "primary_type": None,
            "secondary_type": None,
            "display_label": "Unknown type",
            "validation_driver": "schema_org"  # Default to schema for validation
        }

    def _build_display_label(
        self,
        primary_label: str,
        primary_source: str,
        schema_type: Optional[str] = None,
        fibo_type: Optional[str] = None,
        wikidata_type: Optional[str] = None,
        science_type: Optional[str] = None
    ) -> str:
        """
        Build human-readable display label.

        Examples:
        - FIBO primary: "Monetary amount (FIBO:MoneyAmount)"
        - Science primary: "Alcohol content (chebi:Alcohol)"
        - Schema primary: "Identifier (schema:identifier)"
        - Wikidata primary: "Country (wikidata:geo.country)"
        """
        if primary_source == "science" and science_type:
            # Science types are like "chebi:Alcohol", "qudt:pH"
            return f"{primary_label} ({science_type})"

        if primary_source == "wikidata" and wikidata_type:
            # Wikidata types are like "geo.country", "ref.language"
            return f"{primary_label} (wikidata:{wikidata_type})"

        if primary_source == "fibo" and fibo_type:
            # Extract short FIBO type name
            fibo_short = fibo_type.replace("fibo-", "").replace("fibo:", "")
            if ":" in fibo_short:
                fibo_short = fibo_short.split(":")[-1]
            # Remove fibo class prefix like "fnd-acc-cur:"
            if "-" in fibo_short:
                parts = fibo_short.split("-")
                if len(parts) > 1:
                    fibo_short = parts[-1]

            return f"{primary_label} (FIBO:{fibo_short})"

        elif primary_source == "schema_org" and schema_type:
            # Clean schema type
            schema_short = schema_type.replace("schema:", "")
            return f"{primary_label} ({schema_type})"

        return primary_label

    def _get_fibo_label(self, fibo_type: str) -> str:
        """
        Extract human-readable label from FIBO type.

        Args:
            fibo_type: Full FIBO type string (e.g., "fibo-fnd-acc-cur:MonetaryAmount")

        Returns:
            Human-readable label (e.g., "Monetary Amount")
        """
        if not fibo_type:
            return "Unknown"

        # Extract the class name after the last colon
        if ":" in fibo_type:
            class_name = fibo_type.split(":")[-1]
        else:
            class_name = fibo_type

        # Convert CamelCase to Title Case with spaces
        import re
        label = re.sub(r'([A-Z])', r' \1', class_name).strip()
        return label.title()


def create_semantic_info(
    structural_type: str,
    schema_org_result: Optional[Dict[str, Any]],
    fibo_result: Optional[Dict[str, Any]],
    wikidata_result: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Convenience function to create complete semantic_info.

    Args:
        structural_type: The pandas dtype
        schema_org_result: Result from SchemaOrgTagger.tag_column()
        fibo_result: Result from existing FIBO SemanticTagger (converted to dict)
        wikidata_result: Result from WikidataTagger.tag_column()

    Returns:
        Complete semantic_info dictionary
    """
    resolver = SemanticResolver()
    return resolver.resolve(structural_type, schema_org_result, fibo_result, wikidata_result)
