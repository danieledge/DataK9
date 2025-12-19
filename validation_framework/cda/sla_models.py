"""
SLA Data Models for CDA Compliance Tracking.

Defines data classes for SLA (Service Level Agreement) evaluation:
- SLAStatus: Traffic light status (GREEN/AMBER/RED)
- SLADefinition: Tolerance configuration for a CDA field
- SLAResult: Evaluation result for a single field
- SLAReport: Complete SLA report for a file

Author: Daniel Edge
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
from datetime import datetime


class SLAStatus(Enum):
    """Traffic light status for SLA compliance."""
    GREEN = "green"              # Within tolerance
    AMBER = "amber"              # Approaching tolerance
    RED = "red"                  # Breached
    NOT_EVALUATED = "not_evaluated"  # No data or no validations


# Built-in tier definitions based on industry standards
DEFAULT_TIERS = {
    "critical": 0.0,     # 0% tolerance (100% accuracy) - PKs, regulated fields
    "high": 0.001,       # 0.1% tolerance (99.9% accuracy) - financial data
    "standard": 0.01,    # 1% tolerance (99% accuracy) - most business fields
    "low": 0.05,         # 5% tolerance (95% accuracy) - non-critical fields
}

DEFAULT_WARNING_AT = 0.8  # Amber at 80% of tolerance


@dataclass
class SLADefinition:
    """
    SLA tolerance definition for a CDA field.

    Attributes:
        tolerance: Maximum acceptable failure rate (0.0 to 1.0)
        warning_at: Fraction of tolerance that triggers amber status
        tier_name: Name of tier if using named tier (for display)
    """
    tolerance: float
    warning_at: float = 0.8
    tier_name: Optional[str] = None

    @property
    def warning_threshold(self) -> float:
        """Calculate amber threshold."""
        if self.tolerance == 0:
            return 0  # No amber zone for zero tolerance
        return self.tolerance * self.warning_at

    def __post_init__(self):
        if not 0.0 <= self.tolerance <= 1.0:
            raise ValueError(f"Tolerance must be 0.0-1.0, got {self.tolerance}")
        if not 0.0 <= self.warning_at <= 1.0:
            raise ValueError(f"warning_at must be 0.0-1.0, got {self.warning_at}")

    @classmethod
    def from_config(
        cls,
        field_config: Dict[str, Any],
        defaults: Dict[str, Any]
    ) -> 'SLADefinition':
        """
        Parse CDA config. Expects ONE of:
        - tier: <name>
        - tolerance: <float>
        - neither (uses default_tier)

        Args:
            field_config: CDA field configuration dict
            defaults: Global SLA defaults from sla_defaults

        Returns:
            SLADefinition instance
        """
        tiers = {**DEFAULT_TIERS, **defaults.get('tiers', {})}
        warning_at = defaults.get('warning_at', DEFAULT_WARNING_AT)
        default_tier = defaults.get('default_tier', 'standard')

        if 'tier' in field_config:
            tier_name = field_config['tier']
            if tier_name not in tiers:
                raise ValueError(f"Unknown SLA tier: {tier_name}. "
                               f"Available: {list(tiers.keys())}")
            return cls(
                tolerance=tiers[tier_name],
                warning_at=warning_at,
                tier_name=tier_name
            )

        if 'tolerance' in field_config:
            tolerance = float(field_config['tolerance'])
            return cls(
                tolerance=tolerance,
                warning_at=warning_at,
                tier_name="custom"
            )

        # Default tier
        return cls(
            tolerance=tiers[default_tier],
            warning_at=warning_at,
            tier_name=default_tier
        )


@dataclass
class SLAResult:
    """
    SLA evaluation result for a single CDA field.

    Attributes:
        field: CDA field name
        status: Traffic light status (GREEN/AMBER/RED)
        tier_name: Name of the tier or "custom"
        tolerance: Configured tolerance threshold
        failure_rate: Actual observed failure rate
        bad_records: Number of failed records
        evaluated_records: Total records evaluated
        aggregation_method: How failures were aggregated ("union" or "max_rate")
        contributing_validations: Validation rules that contributed to this result
    """
    field: str
    status: SLAStatus
    tier_name: str
    tolerance: float
    failure_rate: float
    bad_records: int
    evaluated_records: int
    aggregation_method: str
    contributing_validations: List[str] = field(default_factory=list)

    @property
    def accuracy(self) -> float:
        """Success rate as percentage."""
        return (1.0 - self.failure_rate) * 100

    @property
    def is_compliant(self) -> bool:
        """True if GREEN or AMBER (not breached)."""
        return self.status in (SLAStatus.GREEN, SLAStatus.AMBER)

    @property
    def is_healthy(self) -> bool:
        """True if GREEN only."""
        return self.status == SLAStatus.GREEN

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "field": self.field,
            "status": self.status.value,
            "tier": self.tier_name,
            "tolerance": f"{self.tolerance:.2%}",
            "failure_rate": f"{self.failure_rate:.4%}",
            "accuracy": f"{self.accuracy:.2f}%",
            "bad_records": self.bad_records,
            "evaluated_records": self.evaluated_records,
            "aggregation_method": self.aggregation_method,
            "contributing_validations": self.contributing_validations,
        }


@dataclass
class SLAReport:
    """
    Complete SLA evaluation report for a file.

    Attributes:
        file_name: Name of the file evaluated
        dataset_row_count: Total rows in the dataset
        results: SLA results per CDA field
        timestamp: When the evaluation was performed
    """
    file_name: str
    dataset_row_count: int
    results: List[SLAResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def total_cdas(self) -> int:
        """Total number of CDAs evaluated."""
        return len(self.results)

    @property
    def green_count(self) -> int:
        """Number of CDAs with GREEN status."""
        return sum(1 for r in self.results if r.status == SLAStatus.GREEN)

    @property
    def amber_count(self) -> int:
        """Number of CDAs with AMBER status."""
        return sum(1 for r in self.results if r.status == SLAStatus.AMBER)

    @property
    def red_count(self) -> int:
        """Number of CDAs with RED status."""
        return sum(1 for r in self.results if r.status == SLAStatus.RED)

    @property
    def not_evaluated_count(self) -> int:
        """Number of CDAs not evaluated (no validations)."""
        return sum(1 for r in self.results if r.status == SLAStatus.NOT_EVALUATED)

    @property
    def is_fully_green(self) -> bool:
        """True if all CDAs are GREEN."""
        evaluated = [r for r in self.results if r.status != SLAStatus.NOT_EVALUATED]
        return all(r.status == SLAStatus.GREEN for r in evaluated) if evaluated else True

    @property
    def has_breaches(self) -> bool:
        """True if any CDA is RED."""
        return any(r.status == SLAStatus.RED for r in self.results)

    @property
    def summary(self) -> str:
        """Human-readable summary string."""
        return f"{self.green_count} 🟢  {self.amber_count} 🟡  {self.red_count} 🔴"

    def get_breaches(self) -> List[SLAResult]:
        """Get all RED (breached) results."""
        return [r for r in self.results if r.status == SLAStatus.RED]

    def get_warnings(self) -> List[SLAResult]:
        """Get all AMBER (warning) results."""
        return [r for r in self.results if r.status == SLAStatus.AMBER]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "file_name": self.file_name,
            "dataset_row_count": self.dataset_row_count,
            "total_cdas": self.total_cdas,
            "green_count": self.green_count,
            "amber_count": self.amber_count,
            "red_count": self.red_count,
            "not_evaluated_count": self.not_evaluated_count,
            "is_fully_green": self.is_fully_green,
            "has_breaches": self.has_breaches,
            "summary": self.summary,
            "results": [r.to_dict() for r in self.results],
            "timestamp": self.timestamp.isoformat(),
        }
