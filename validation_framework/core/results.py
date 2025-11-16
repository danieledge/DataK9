"""Classes for storing and managing validation results."""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum


class Severity(Enum):
    """Validation severity levels."""
    ERROR = "ERROR"
    WARNING = "WARNING"


class Status(Enum):
    """Overall validation status."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


@dataclass
class ValidationResult:
    """Result of a single validation rule execution."""

    rule_name: str
    severity: Severity
    passed: bool
    message: str
    failed_count: int = 0
    total_count: int = 0
    details: List[Dict[str, Any]] = field(default_factory=list)
    sample_failures: List[Dict[str, Any]] = field(default_factory=list)
    execution_time: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "rule_name": self.rule_name,
            "severity": self.severity.value,
            "passed": self.passed,
            "message": self.message,
            "failed_count": self.failed_count,
            "total_count": self.total_count,
            "success_rate": self._calculate_success_rate(),
            "sample_failures": self.sample_failures[:10],  # Limit to 10 samples
            "execution_time": round(self.execution_time, 3),
        }

    def _calculate_success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_count == 0:
            return 100.0 if self.passed else 0.0
        return round(((self.total_count - self.failed_count) / self.total_count) * 100, 2)


@dataclass
class FileValidationReport:
    """Validation report for a single file."""

    file_name: str
    file_path: str
    file_format: str
    status: Status
    validation_results: List[ValidationResult] = field(default_factory=list)
    execution_time: float = 0.0
    error_count: int = 0
    warning_count: int = 0
    total_validations: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_result(self, result: ValidationResult) -> None:
        """Add a validation result and update counts."""
        self.validation_results.append(result)
        self.total_validations += 1

        if not result.passed:
            if result.severity == Severity.ERROR:
                self.error_count += 1
            else:
                self.warning_count += 1

    def update_status(self) -> None:
        """Update overall status based on results."""
        if self.error_count > 0:
            self.status = Status.FAILED
        elif self.warning_count > 0:
            self.status = Status.WARNING
        else:
            self.status = Status.PASSED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "file_format": self.file_format,
            "status": self.status.value,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "total_validations": self.total_validations,
            "execution_time": round(self.execution_time, 3),
            "validation_results": [r.to_dict() for r in self.validation_results],
            "metadata": self.metadata,
        }


@dataclass
class ValidationReport:
    """Overall validation report for all files."""

    job_name: str
    execution_time: datetime
    duration_seconds: float
    overall_status: Status
    file_reports: List[FileValidationReport] = field(default_factory=list)
    total_errors: int = 0
    total_warnings: int = 0
    total_validations: int = 0
    config: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None

    def add_file_report(self, report: FileValidationReport) -> None:
        """Add a file validation report."""
        self.file_reports.append(report)
        self.total_errors += report.error_count
        self.total_warnings += report.warning_count
        self.total_validations += report.total_validations

    def update_overall_status(self) -> None:
        """Update overall status based on all file reports."""
        if self.total_errors > 0:
            self.overall_status = Status.FAILED
        elif self.total_warnings > 0:
            self.overall_status = Status.WARNING
        else:
            self.overall_status = Status.PASSED

    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return self.total_errors > 0

    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return self.total_warnings > 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_name": self.job_name,
            "execution_time": self.execution_time.isoformat(),
            "duration_seconds": round(self.duration_seconds, 3),
            "overall_status": self.overall_status.value,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "total_validations": self.total_validations,
            "files": [f.to_dict() for f in self.file_reports],
            "config": self.config,
        }
