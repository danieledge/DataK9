"""
CDA Data Models

Defines data classes and enums for Critical Data Attribute tracking.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Dict, Optional
from datetime import datetime


class CDATier(Enum):
    """
    Classification tiers for Critical Data Attributes.

    TIER_1: Regulatory - Fields required for regulatory compliance (highest priority)
    TIER_2: Financial - Fields used in financial calculations and reporting
    TIER_3: Operational - Fields important for business operations
    """
    TIER_1 = "TIER_1"  # Regulatory
    TIER_2 = "TIER_2"  # Financial
    TIER_3 = "TIER_3"  # Operational

    @property
    def display_name(self) -> str:
        """Human-readable tier name."""
        names = {
            CDATier.TIER_1: "Regulatory",
            CDATier.TIER_2: "Financial",
            CDATier.TIER_3: "Operational"
        }
        return names.get(self, self.value)

    @property
    def priority(self) -> int:
        """Priority level (1 = highest)."""
        priorities = {
            CDATier.TIER_1: 1,
            CDATier.TIER_2: 2,
            CDATier.TIER_3: 3
        }
        return priorities.get(self, 99)


@dataclass
class CDADefinition:
    """
    Definition of a Critical Data Attribute.

    Attributes:
        field: Column/field name in the data source
        tier: Classification tier (TIER_1, TIER_2, TIER_3)
        description: Human-readable description of the field's importance
        owner: Optional business owner responsible for the field
        data_steward: Optional data steward contact
        regulatory_reference: Optional regulatory requirement reference
    """
    field: str
    tier: CDATier
    description: str = ""
    owner: str = ""
    data_steward: str = ""
    regulatory_reference: str = ""

    @classmethod
    def from_dict(cls, data: Dict) -> 'CDADefinition':
        """Create CDADefinition from dictionary (YAML parsing)."""
        tier_str = data.get('tier', 'TIER_3')
        try:
            tier = CDATier(tier_str)
        except ValueError:
            tier = CDATier.TIER_3

        return cls(
            field=data.get('field', ''),
            tier=tier,
            description=data.get('description', ''),
            owner=data.get('owner', ''),
            data_steward=data.get('data_steward', ''),
            regulatory_reference=data.get('regulatory_reference', '')
        )


@dataclass
class CDAFieldCoverage:
    """
    Validation coverage information for a single CDA field.

    Attributes:
        cda: The CDA definition
        is_covered: Whether any validation covers this field
        covering_validations: List of validation types that cover this field
        coverage_details: Details about how the field is validated
    """
    cda: CDADefinition
    is_covered: bool = False
    covering_validations: List[str] = field(default_factory=list)
    coverage_details: List[str] = field(default_factory=list)

    @property
    def coverage_count(self) -> int:
        """Number of validations covering this field."""
        return len(self.covering_validations)

    @property
    def status_icon(self) -> str:
        """Status icon for display."""
        return "✓" if self.is_covered else "✗"

    @property
    def status_class(self) -> str:
        """CSS class for styling."""
        return "covered" if self.is_covered else "gap"


@dataclass
class CDAGapResult:
    """
    Gap analysis result for a single file/source.

    Attributes:
        file_name: Name of the file/source analyzed
        total_cdas: Total number of CDAs defined
        covered_cdas: Number of CDAs with validation coverage
        gap_cdas: Number of CDAs without validation coverage
        field_coverage: Detailed coverage for each CDA field
        tier_coverage: Coverage breakdown by tier
        analysis_timestamp: When the analysis was performed
    """
    file_name: str
    total_cdas: int = 0
    covered_cdas: int = 0
    gap_cdas: int = 0
    field_coverage: List[CDAFieldCoverage] = field(default_factory=list)
    tier_coverage: Dict[CDATier, Dict[str, int]] = field(default_factory=dict)
    analysis_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def coverage_percentage(self) -> float:
        """Overall coverage percentage."""
        if self.total_cdas == 0:
            return 100.0
        return (self.covered_cdas / self.total_cdas) * 100

    @property
    def has_gaps(self) -> bool:
        """Whether any gaps exist."""
        return self.gap_cdas > 0

    @property
    def gaps(self) -> List[CDAFieldCoverage]:
        """List of uncovered CDAs."""
        return [fc for fc in self.field_coverage if not fc.is_covered]

    @property
    def covered(self) -> List[CDAFieldCoverage]:
        """List of covered CDAs."""
        return [fc for fc in self.field_coverage if fc.is_covered]

    def get_tier_coverage(self, tier: CDATier) -> Dict[str, int]:
        """Get coverage stats for a specific tier."""
        return self.tier_coverage.get(tier, {'total': 0, 'covered': 0, 'gaps': 0})

    def get_tier_percentage(self, tier: CDATier) -> float:
        """Get coverage percentage for a specific tier."""
        stats = self.get_tier_coverage(tier)
        if stats['total'] == 0:
            return 100.0
        return (stats['covered'] / stats['total']) * 100


@dataclass
class CDAAnalysisReport:
    """
    Complete CDA gap analysis report across all files.

    Attributes:
        job_name: Name of the validation job
        results: Gap analysis results per file
        analysis_timestamp: When the report was generated
    """
    job_name: str
    results: List[CDAGapResult] = field(default_factory=list)
    analysis_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def total_cdas(self) -> int:
        """Total CDAs across all files."""
        return sum(r.total_cdas for r in self.results)

    @property
    def total_covered(self) -> int:
        """Total covered CDAs across all files."""
        return sum(r.covered_cdas for r in self.results)

    @property
    def total_gaps(self) -> int:
        """Total gap CDAs across all files."""
        return sum(r.gap_cdas for r in self.results)

    @property
    def overall_coverage(self) -> float:
        """Overall coverage percentage."""
        if self.total_cdas == 0:
            return 100.0
        return (self.total_covered / self.total_cdas) * 100

    @property
    def has_gaps(self) -> bool:
        """Whether any gaps exist in any file."""
        return any(r.has_gaps for r in self.results)

    @property
    def tier1_at_risk(self) -> bool:
        """Whether any TIER_1 (regulatory) CDAs have gaps."""
        for result in self.results:
            stats = result.get_tier_coverage(CDATier.TIER_1)
            if stats['gaps'] > 0:
                return True
        return False
