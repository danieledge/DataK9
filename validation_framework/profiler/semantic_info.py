"""
Semantic information data structures for column profiling.

Stores semantic understanding of columns including tags, explanations, and evidence.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class SemanticInfo:
    """
    Semantic understanding of a column.

    Attributes:
        semantic_tags: List of hierarchical semantic tags (e.g., ['money.amount', 'measure.continuous'])
        primary_tag: The primary/most specific semantic tag
        confidence: Overall confidence in the semantic classification (0.0 to 1.0)
        explanation: Human-readable explanation of why this semantic type was assigned
        evidence: Dictionary of evidence used to determine semantics
        fibo_source: FIBO class reference if derived from FIBO taxonomy
    """
    semantic_tags: List[str] = field(default_factory=list)
    primary_tag: str = "unknown"
    confidence: float = 0.0
    explanation: str = ""
    evidence: Dict[str, Any] = field(default_factory=dict)
    fibo_source: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "semantic_tags": self.semantic_tags,
            "primary_tag": self.primary_tag,
            "confidence": round(self.confidence, 3),
            "explanation": self.explanation,
            "evidence": self.evidence,
            "fibo_source": self.fibo_source
        }

    def has_tag(self, tag: str) -> bool:
        """
        Check if this column has a specific semantic tag.

        Args:
            tag: Tag to check (supports wildcards, e.g., 'money.*')

        Returns:
            True if tag matches
        """
        if '*' in tag:
            # Wildcard matching (e.g., 'money.*' matches 'money.amount')
            prefix = tag.replace('.*', '')
            return any(t.startswith(prefix) for t in self.semantic_tags)
        else:
            # Exact match
            return tag in self.semantic_tags

    def get_tags_by_category(self, category: str) -> List[str]:
        """
        Get all tags in a specific category.

        Args:
            category: Category name (e.g., 'money', 'banking')

        Returns:
            List of tags in that category
        """
        return [tag for tag in self.semantic_tags if tag.startswith(f"{category}.")]
