"""Core framework components."""

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.async_engine import AsyncValidationEngine, run_async_validation, run_async_validation_concurrent
from validation_framework.core.config import ValidationConfig, ConfigError
from validation_framework.core.registry import ValidationRegistry
from validation_framework.core.results import ValidationResult, FileValidationReport, ValidationReport, Severity
from validation_framework.core.expression_validator import (
    validate_expression,
    ExpressionValidationError,
    get_safe_error_message
)

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
    # Expression validation
    "validate_expression",
    "ExpressionValidationError",
    "get_safe_error_message",
]
