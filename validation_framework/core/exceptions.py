"""
DataK9 Exception Hierarchy.

This module defines a comprehensive exception hierarchy for the DataK9 framework,
providing clear categorization of errors and standardized error handling across
all components.

Exception Severity Levels:
    - FATAL: Stop all processing immediately
    - CRITICAL: Stop current file processing, continue with other files
    - RECOVERABLE: Log error, mark validation as failed, continue processing
    - WARNING: Log warning, validation continues

Author: Daniel Edge
"""

from typing import Optional, Dict, Any
from enum import Enum


class ErrorSeverity(Enum):
    """
    Classify error severity for handling decisions.

    Attributes:
        FATAL: Unrecoverable error, stop all processing
        CRITICAL: File-level error, stop processing this file
        RECOVERABLE: Validation-level error, continue with other validations
        WARNING: Non-critical issue, log and continue
    """
    FATAL = "fatal"
    CRITICAL = "critical"
    RECOVERABLE = "recoverable"
    WARNING = "warning"


class DataK9Exception(Exception):
    """
    Base exception for all DataK9 errors with enhanced context.

    All DataK9 exceptions inherit from this base class, providing:
    - Severity classification for handling decisions
    - Structured details dictionary for logging/reporting
    - Original exception preservation for debugging
    - Serialization support for JSON/dict output

    Attributes:
        message (str): Human-readable error message
        severity (ErrorSeverity): Error severity level
        details (Dict[str, Any]): Additional context (file path, validation name, etc.)
        original_exception (Optional[Exception]): Original exception if wrapping

    Example:
        >>> try:
        ...     result = process_data()
        ... except Exception as e:
        ...     raise DataK9Exception(
        ...         "Data processing failed",
        ...         severity=ErrorSeverity.RECOVERABLE,
        ...         details={'file': 'customers.csv'},
        ...         original_exception=e
        ...     )
    """

    def __init__(
        self,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.RECOVERABLE,
        details: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize DataK9 exception.

        Args:
            message: Human-readable error description
            severity: Error severity level (default: RECOVERABLE)
            details: Additional context dictionary
            original_exception: Original exception if this wraps another error
        """
        super().__init__(message)
        self.message = message
        self.severity = severity
        self.details = details or {}
        self.original_exception = original_exception

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize exception to dictionary for logging/reporting.

        Returns:
            Dictionary containing exception details suitable for JSON serialization

        Example:
            >>> exc = DataK9Exception("Test error", details={'field': 'email'})
            >>> exc.to_dict()
            {
                'type': 'DataK9Exception',
                'message': 'Test error',
                'severity': 'recoverable',
                'details': {'field': 'email'},
                'original_error': None
            }
        """
        return {
            'type': self.__class__.__name__,
            'message': self.message,
            'severity': self.severity.value,
            'details': self.details,
            'original_error': str(self.original_exception) if self.original_exception else None
        }


# ============================================================================
# Configuration Errors (Fatal)
# ============================================================================

class ConfigError(DataK9Exception):
    """
    Configuration file errors (fatal - stop all processing).

    Raised when:
    - Configuration file not found
    - Invalid YAML syntax
    - Required configuration fields missing
    - Invalid configuration structure

    These errors prevent the validation job from starting.

    Attributes:
        field (Optional[str]): Specific config field that caused error

    Example:
        >>> raise ConfigError(
        ...     "Required field 'validation_job' missing",
        ...     field="validation_job"
        ... )
    """

    def __init__(self, message: str, field: Optional[str] = None):
        """
        Initialize configuration error.

        Args:
            message: Error description
            field: Specific config field that failed (optional)
        """
        super().__init__(
            message,
            severity=ErrorSeverity.FATAL,
            details={'field': field} if field else {}
        )
        self.field = field


class YAMLSizeError(ConfigError):
    """
    YAML file too large (security protection).

    Raised when configuration file exceeds maximum allowed size.
    This is a security measure to prevent DoS attacks via huge YAML files.

    Example:
        >>> raise YAMLSizeError(
        ...     "Config file exceeds 10MB limit",
        ...     file_size=15000000,
        ...     max_size=10000000
        ... )
    """

    def __init__(self, message: str, file_size: Optional[int] = None, max_size: Optional[int] = None):
        """
        Initialize YAML size error.

        Args:
            message: Error description
            file_size: Actual file size in bytes
            max_size: Maximum allowed size in bytes
        """
        super().__init__(message)
        self.details.update({
            'file_size': file_size,
            'max_size': max_size
        })


class ConfigValidationError(ConfigError):
    """
    Configuration validation failed against schema.

    Raised when configuration structure is valid YAML but doesn't match
    the expected schema (missing required fields, wrong types, etc.).

    Example:
        >>> raise ConfigValidationError(
        ...     "Invalid severity value: 'CRITICAL'",
        ...     field="validations[0].severity",
        ...     expected="ERROR, WARNING, INFO",
        ...     actual="CRITICAL"
        ... )
    """

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        expected: Optional[str] = None,
        actual: Optional[str] = None
    ):
        """
        Initialize config validation error.

        Args:
            message: Error description
            field: Config field that failed validation
            expected: Expected value or type
            actual: Actual value found
        """
        super().__init__(message, field)
        self.details.update({
            'expected': expected,
            'actual': actual
        })


# ============================================================================
# Data Loading Errors (Critical)
# ============================================================================

class DataLoadError(DataK9Exception):
    """
    Data file loading errors (critical - stop processing this file).

    Raised when:
    - Data file not found
    - File format invalid or corrupted
    - File cannot be read (permissions, encoding issues)
    - Parsing errors (malformed CSV, invalid JSON, etc.)

    Processing stops for this file but continues with other files.

    Attributes:
        file_path (str): Path to file that failed to load
        line_number (Optional[int]): Line number where error occurred

    Example:
        >>> raise DataLoadError(
        ...     "Failed to parse CSV: invalid delimiter",
        ...     file_path="customers.csv",
        ...     line_number=42
        ... )
    """

    def __init__(
        self,
        message: str,
        file_path: str,
        line_number: Optional[int] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize data load error.

        Args:
            message: Error description
            file_path: Path to file being loaded
            line_number: Specific line number if applicable
            original_exception: Original exception from loader
        """
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            details={'file_path': file_path, 'line_number': line_number},
            original_exception=original_exception
        )
        self.file_path = file_path
        self.line_number = line_number


class FileNotFoundError(DataLoadError):
    """
    Data file not found at specified path.

    Example:
        >>> raise FileNotFoundError(
        ...     "customers.csv",
        ...     searched_paths=["/data/customers.csv", "./customers.csv"]
        ... )
    """

    def __init__(self, file_path: str, searched_paths: Optional[list] = None):
        """
        Initialize file not found error.

        Args:
            file_path: Expected file path
            searched_paths: All paths searched for the file
        """
        message = f"File not found: {file_path}"
        if searched_paths:
            message += f"\nSearched: {', '.join(searched_paths)}"

        super().__init__(message, file_path)
        self.details['searched_paths'] = searched_paths or []


class UnsupportedFormatError(DataLoadError):
    """
    File format not supported by DataK9.

    Example:
        >>> raise UnsupportedFormatError(
        ...     "customers.xml",
        ...     format="xml",
        ...     supported_formats=["csv", "excel", "json", "parquet"]
        ... )
    """

    def __init__(self, file_path: str, format: str, supported_formats: list):
        """
        Initialize unsupported format error.

        Args:
            file_path: Path to file with unsupported format
            format: Detected or specified format
            supported_formats: List of supported formats
        """
        super().__init__(
            f"Unsupported file format '{format}'. Supported: {', '.join(supported_formats)}",
            file_path
        )
        self.details.update({
            'format': format,
            'supported_formats': supported_formats
        })


# ============================================================================
# Validation Execution Errors (Recoverable/Critical)
# ============================================================================

class ValidationExecutionError(DataK9Exception):
    """
    Error during validation execution.

    Raised when a validation rule fails to execute properly (not when
    data fails validation - that's a normal ValidationResult).

    Can be either recoverable (log and continue) or critical (stop file)
    depending on the error type.

    Attributes:
        validation_name (str): Name of validation that failed
        recoverable (bool): Whether processing can continue

    Example:
        >>> raise ValidationExecutionError(
        ...     "Column 'email' not found in data",
        ...     validation_name="EmailFormatCheck",
        ...     recoverable=False  # Can't check email if column missing
        ... )
    """

    def __init__(
        self,
        message: str,
        validation_name: str,
        recoverable: bool = True,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize validation execution error.

        Args:
            message: Error description
            validation_name: Name of validation that failed
            recoverable: Whether to continue processing (default: True)
            original_exception: Original exception if wrapping
        """
        severity = ErrorSeverity.RECOVERABLE if recoverable else ErrorSeverity.CRITICAL

        super().__init__(
            message,
            severity=severity,
            details={'validation_name': validation_name},
            original_exception=original_exception
        )
        self.validation_name = validation_name
        self.recoverable = recoverable


class ParameterValidationError(ValidationExecutionError):
    """
    Invalid parameters for validation rule.

    Raised when validation parameters don't match expected schema or
    contain invalid values.

    Example:
        >>> raise ParameterValidationError(
        ...     "RangeCheck: min_value (100) must be <= max_value (50)",
        ...     validation_name="RangeCheck",
        ...     parameter="min_value",
        ...     value=100
        ... )
    """

    def __init__(
        self,
        message: str,
        validation_name: str,
        parameter: str,
        value: Any
    ):
        """
        Initialize parameter validation error.

        Args:
            message: Error description
            validation_name: Name of validation
            parameter: Parameter name that's invalid
            value: Invalid parameter value
        """
        super().__init__(message, validation_name, recoverable=False)
        self.details.update({
            'parameter': parameter,
            'value': value
        })


class ColumnNotFoundError(ValidationExecutionError):
    """
    Required column not found in dataset.

    Raised when validation requires a column that doesn't exist in the data.
    This is critical because the validation cannot proceed.

    Example:
        >>> raise ColumnNotFoundError(
        ...     validation_name="EmailCheck",
        ...     column="email",
        ...     available_columns=["customer_id", "name", "phone"]
        ... )
    """

    def __init__(
        self,
        validation_name: str,
        column: str,
        available_columns: list
    ):
        """
        Initialize column not found error.

        Args:
            validation_name: Name of validation
            column: Column that's missing
            available_columns: List of available columns
        """
        super().__init__(
            f"Column '{column}' not found in data. Available: {', '.join(available_columns)}",
            validation_name,
            recoverable=False
        )
        self.details.update({
            'column': column,
            'available_columns': available_columns
        })


# ============================================================================
# Database Errors
# ============================================================================

class DatabaseError(DataK9Exception):
    """
    Database connection or query errors.

    Raised when:
    - Cannot connect to database
    - Authentication fails
    - Query execution fails
    - Invalid SQL syntax

    Example:
        >>> raise DatabaseError(
        ...     "Failed to connect to PostgreSQL",
        ...     connection_string="postgresql://localhost:5432/mydb",
        ...     error_code="08001"
        ... )
    """

    def __init__(
        self,
        message: str,
        connection_string: Optional[str] = None,
        error_code: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize database error.

        Args:
            message: Error description
            connection_string: Database connection string (credentials masked)
            error_code: Database-specific error code
            original_exception: Original database exception
        """
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            details={
                'connection_string': connection_string,
                'error_code': error_code
            },
            original_exception=original_exception
        )


# ============================================================================
# Profiler Errors
# ============================================================================

class ProfilerError(DataK9Exception):
    """
    Data profiling errors.

    Raised when profiling operations fail (statistical calculations,
    pattern detection, anomaly detection, etc.).

    Example:
        >>> raise ProfilerError(
        ...     "Failed to calculate correlation matrix: insufficient data",
        ...     operation="correlation_analysis",
        ...     column="sales"
        ... )
    """

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        column: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize profiler error.

        Args:
            message: Error description
            operation: Profiling operation that failed
            column: Column being profiled
            original_exception: Original exception
        """
        super().__init__(
            message,
            severity=ErrorSeverity.RECOVERABLE,
            details={
                'operation': operation,
                'column': column
            },
            original_exception=original_exception
        )


# ============================================================================
# Reporter Errors
# ============================================================================

class ReporterError(DataK9Exception):
    """
    Report generation errors.

    Raised when HTML or JSON report generation fails.

    Example:
        >>> raise ReporterError(
        ...     "Failed to render HTML template",
        ...     report_type="html",
        ...     output_path="/tmp/report.html"
        ... )
    """

    def __init__(
        self,
        message: str,
        report_type: Optional[str] = None,
        output_path: Optional[str] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize reporter error.

        Args:
            message: Error description
            report_type: Type of report (html, json)
            output_path: Output file path
            original_exception: Original exception
        """
        super().__init__(
            message,
            severity=ErrorSeverity.RECOVERABLE,
            details={
                'report_type': report_type,
                'output_path': output_path
            },
            original_exception=original_exception
        )
