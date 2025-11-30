"""
Semantic Resolver for DataK9 Profiler.

Resolves the final semantic classification by combining Schema.org general
semantics with FIBO financial semantics. FIBO takes priority when its
confidence is high enough (financial/ID-like fields), otherwise Schema.org
provides the baseline classification.

Resolution Rules:
1. FIBO wins if fibo_conf >= FIBO_MIN_CONF and fibo_conf >= schema_conf
2. Schema.org wins if no strong FIBO match
3. Fallback to "none" if neither layer has strong confidence
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Resolution thresholds
FIBO_MIN_CONF = 0.7    # FIBO must have at least this confidence to override
SCHEMA_MIN_CONF = 0.5  # Schema.org minimum confidence for valid classification


class SemanticResolver:
    """
    Resolves semantic classification from Schema.org and FIBO layers.

    Produces a unified semantic_info structure with:
    - structural_type: The pandas/numpy dtype
    - schema_org: Schema.org classification (always present)
    - fibo: FIBO classification (optional, only for financial fields)
    - resolved: Final resolution with display_label and validation_driver
    """

    def __init__(
        self,
        fibo_min_conf: float = FIBO_MIN_CONF,
        schema_min_conf: float = SCHEMA_MIN_CONF
    ):
        """
        Initialize the resolver.

        Args:
            fibo_min_conf: Minimum FIBO confidence to override Schema.org
            schema_min_conf: Minimum Schema.org confidence for valid classification
        """
        self.fibo_min_conf = fibo_min_conf
        self.schema_min_conf = schema_min_conf

    def resolve(
        self,
        structural_type: str,
        schema_org: Optional[Dict[str, Any]],
        fibo: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Resolve semantic classification from both layers.

        Args:
            structural_type: The pandas/numpy dtype (e.g., "float64", "object")
            schema_org: Schema.org classification dict with type, confidence, signals
            fibo: FIBO classification dict with type, confidence, signals (or None)

        Returns:
            Complete semantic_info structure:
            {
                "structural_type": "float64",
                "schema_org": {...},
                "fibo": {...} or None,
                "resolved": {
                    "primary_source": "fibo" | "schema_org" | "none",
                    "primary_type": "fibo:MoneyAmount" or "schema:MonetaryAmount",
                    "secondary_type": "schema:MonetaryAmount" or None,
                    "display_label": "Monetary amount (FIBO:MoneyAmount)",
                    "validation_driver": "fibo" | "schema_org"
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

        # Resolution logic
        resolved = self._resolve_primary(
            schema_type, schema_conf, schema_label,
            fibo_type, fibo_conf, fibo_label
        )

        return {
            "structural_type": structural_type,
            "schema_org": schema_org,
            "fibo": fibo,
            "resolved": resolved
        }

    def _resolve_primary(
        self,
        schema_type: str,
        schema_conf: float,
        schema_label: str,
        fibo_type: Optional[str],
        fibo_conf: float,
        fibo_label: Optional[str]
    ) -> Dict[str, Any]:
        """
        Determine which semantic layer is primary.

        Rules:
        1. FIBO wins if fibo_conf >= FIBO_MIN_CONF AND fibo_conf >= schema_conf
        2. Schema.org wins otherwise (if confidence >= SCHEMA_MIN_CONF)
        3. Fallback to "none" if neither is confident
        """
        # Rule 1: FIBO takes priority when strong
        if (fibo_type and
            fibo_conf >= self.fibo_min_conf and
            fibo_conf >= schema_conf):

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

        # Rule 2: Schema.org is primary (FIBO weak or absent)
        if schema_conf >= self.schema_min_conf:
            secondary = fibo_type if (fibo_type and fibo_conf > 0.3) else None

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

        # Rule 3: Neither is confident
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
        fibo_type: Optional[str] = None
    ) -> str:
        """
        Build human-readable display label.

        Examples:
        - FIBO primary: "Monetary amount (FIBO:MoneyAmount)"
        - Schema primary: "Identifier (schema:identifier)"
        """
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
    fibo_result: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Convenience function to create complete semantic_info.

    Args:
        structural_type: The pandas dtype
        schema_org_result: Result from SchemaOrgTagger.tag_column()
        fibo_result: Result from existing FIBO SemanticTagger (converted to dict)

    Returns:
        Complete semantic_info dictionary
    """
    resolver = SemanticResolver()
    return resolver.resolve(structural_type, schema_org_result, fibo_result)
