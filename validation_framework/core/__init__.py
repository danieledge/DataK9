"""Core framework components."""

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.async_engine import AsyncValidationEngine, run_async_validation, run_async_validation_concurrent
from validation_framework.core.config import ValidationConfig, ConfigError
from validation_framework.core.registry import ValidationRegistry
from validation_framework.core.results import ValidationResult, FileValidationReport, ValidationReport, Severity

__all__ = [
    # Sync engine
    "ValidationEngine",
    # Async engine
    "AsyncValidationEngine",
    "run_async_validation",
    "run_async_validation_concurrent",
    # Config
    "ValidationConfig",
    "ConfigError",
    # Registry
    "ValidationRegistry",
    # Results
    "ValidationResult",
    "FileValidationReport",
    "ValidationReport",
    "Severity",
]
