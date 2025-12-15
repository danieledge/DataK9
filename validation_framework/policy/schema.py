"""
Policy Schema and Models

Defines data structures for validation policies including enforcement modes,
policy violations, and the main ValidationPolicy class.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum
import copy


class EnforcementMode(Enum):
    """Policy enforcement behavior."""
    WARN = "warn"       # Log warnings but continue execution
    ERROR = "error"     # Fail validation job if policy violated
    AUTO = "auto"       # Auto-inject missing checks with defaults


@dataclass
class PolicyViolation:
    """Represents a single policy violation."""
    file_name: str
    check_type: str
    reason: str
    severity: str  # "required" or "recommended"
    auto_fixable: bool = True
    cda_field: Optional[str] = None  # For CDA-related violations

    def __str__(self) -> str:
        if self.cda_field:
            return f"{self.file_name}: {self.check_type} for CDA '{self.cda_field}' - {self.reason}"
        return f"{self.file_name}: {self.check_type} - {self.reason}"


@dataclass
class ValidationPolicy:
    """
    Defines minimum required validations for data quality assurance.

    Policies can enforce:
    - Universal checks that apply to all files
    - Format-specific checks (e.g., CSVFormatCheck for CSV files)
    - CDA coverage requirements for critical data attributes
    - Recommended checks that generate warnings only

    Example:
        policy = ValidationPolicy(
            name="standard",
            universal_checks=["EmptyFileCheck"],
            format_checks={"csv": ["CSVFormatCheck"]},
            cda_require_one_of=["MandatoryFieldCheck", "RegexCheck"],
        )
    """
    name: str
    version: str = "1.0"
    description: str = ""
    enforcement: EnforcementMode = EnforcementMode.WARN

    # Universal checks - apply to ALL files regardless of format
    universal_checks: List[str] = field(default_factory=list)

    # Format-specific checks - keyed by format name (csv, parquet, excel, json)
    format_checks: Dict[str, List[str]] = field(default_factory=dict)

    # CDA (Critical Data Attribute) coverage requirements
    # cda_require_all: Every CDA field MUST have ALL of these checks
    cda_require_all: List[str] = field(default_factory=list)

    # cda_require_one_of: Every CDA field MUST have AT LEAST ONE of these checks
    cda_require_one_of: List[str] = field(default_factory=list)

    # Recommended checks - generate warnings only, never errors
    recommended_checks: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ValidationPolicy':
        """
        Create a ValidationPolicy from a dictionary (e.g., from YAML config).

        Args:
            data: Dictionary with policy configuration

        Returns:
            ValidationPolicy instance
        """
        enforcement_str = data.get('enforcement', 'warn')
        try:
            enforcement = EnforcementMode(enforcement_str.lower())
        except ValueError:
            enforcement = EnforcementMode.WARN

        return cls(
            name=data.get('name', 'custom'),
            version=data.get('version', '1.0'),
            description=data.get('description', ''),
            enforcement=enforcement,
            universal_checks=data.get('universal_checks', []),
            format_checks=data.get('format_checks', {}),
            cda_require_all=data.get('cda_require_all', []),
            cda_require_one_of=data.get('cda_require_one_of', []),
            recommended_checks=data.get('recommended_checks', []),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert policy to dictionary representation."""
        return {
            'name': self.name,
            'version': self.version,
            'description': self.description,
            'enforcement': self.enforcement.value,
            'universal_checks': self.universal_checks,
            'format_checks': self.format_checks,
            'cda_require_all': self.cda_require_all,
            'cda_require_one_of': self.cda_require_one_of,
            'recommended_checks': self.recommended_checks,
        }

    def copy_with_enforcement(self, enforcement: EnforcementMode) -> 'ValidationPolicy':
        """Create a copy of this policy with a different enforcement mode."""
        new_policy = copy.deepcopy(self)
        new_policy.enforcement = enforcement
        return new_policy

    def get_required_checks_for_format(self, file_format: str) -> List[str]:
        """
        Get all required checks for a specific file format.

        Args:
            file_format: File format (csv, parquet, excel, json, database)

        Returns:
            Combined list of universal and format-specific required checks
        """
        required = list(self.universal_checks)
        format_specific = self.format_checks.get(file_format.lower(), [])
        required.extend(format_specific)
        return required
