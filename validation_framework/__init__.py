"""Data Validation Framework - A robust tool for pre-load data quality checks."""

__version__ = "0.1.0"

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.results import ValidationResult, ValidationReport
from validation_framework.validations.base import ValidationRule

__all__ = [
    "ValidationEngine",
    "ValidationResult",
    "ValidationReport",
    "ValidationRule",
]
