"""
Unit tests for exception hierarchy.

Tests the DataK9 exception classes and error handling mechanisms.

Author: Daniel Edge
"""

import pytest
from validation_framework.core.exceptions import (
    DataK9Exception,
    ErrorSeverity,
    ConfigError,
    YAMLSizeError,
    ConfigValidationError,
    DataLoadError,
    FileNotFoundError,
    UnsupportedFormatError,
    ValidationExecutionError,
    ParameterValidationError,
    ColumnNotFoundError,
    DatabaseError,
    ProfilerError,
    ReporterError
)


class TestErrorSeverity:
    """Test error severity enum."""

    def test_severity_values(self):
        """Test that all severity levels exist."""
        assert ErrorSeverity.FATAL.value == "fatal"
        assert ErrorSeverity.CRITICAL.value == "critical"
        assert ErrorSeverity.RECOVERABLE.value == "recoverable"
        assert ErrorSeverity.WARNING.value == "warning"


class TestDataK9Exception:
    """Test base exception class."""

    def test_basic_exception(self):
        """Test basic exception creation."""
        exc = DataK9Exception("Test error")

        assert str(exc) == "Test error"
        assert exc.message == "Test error"
        assert exc.severity == ErrorSeverity.RECOVERABLE  # Default
        assert exc.details == {}
        assert exc.original_exception is None

    def test_exception_with_details(self):
        """Test exception with details dictionary."""
        exc = DataK9Exception(
            "Test error",
            details={'field': 'email', 'value': 'invalid'}
        )

        assert exc.details['field'] == 'email'
        assert exc.details['value'] == 'invalid'

    def test_exception_with_original(self):
        """Test exception wrapping another exception."""
        original = ValueError("Original error")
        exc = DataK9Exception(
            "Wrapped error",
            original_exception=original
        )

        assert exc.original_exception == original
        assert isinstance(exc.original_exception, ValueError)

    def test_exception_serialization(self):
        """Test to_dict() serialization."""
        exc = DataK9Exception(
            "Test error",
            severity=ErrorSeverity.CRITICAL,
            details={'field': 'test'},
            original_exception=ValueError("Original")
        )

        result = exc.to_dict()

        assert result['type'] == 'DataK9Exception'
        assert result['message'] == 'Test error'
        assert result['severity'] == 'critical'
        assert result['details']['field'] == 'test'
        assert 'Original' in result['original_error']

    def test_exception_serialization_no_original(self):
        """Test serialization without original exception."""
        exc = DataK9Exception("Test")
        result = exc.to_dict()

        assert result['original_error'] is None


class TestConfigErrors:
    """Test configuration error classes."""

    def test_config_error_is_fatal(self):
        """Test that ConfigError has FATAL severity."""
        exc = ConfigError("Config error")

        assert exc.severity == ErrorSeverity.FATAL
        assert str(exc) == "Config error"

    def test_config_error_with_field(self):
        """Test ConfigError with field name."""
        exc = ConfigError("Missing field", field="validation_job")

        assert exc.field == "validation_job"
        assert exc.details['field'] == "validation_job"

    def test_yaml_size_error(self):
        """Test YAML size error."""
        exc = YAMLSizeError(
            "File too large",
            file_size=15000000,
            max_size=10000000
        )

        assert exc.severity == ErrorSeverity.FATAL
        assert exc.details['file_size'] == 15000000
        assert exc.details['max_size'] == 10000000

    def test_config_validation_error(self):
        """Test configuration validation error."""
        exc = ConfigValidationError(
            "Invalid severity",
            field="validations[0].severity",
            expected="ERROR, WARNING",
            actual="CRITICAL"
        )

        assert exc.field == "validations[0].severity"
        assert exc.details['expected'] == "ERROR, WARNING"
        assert exc.details['actual'] == "CRITICAL"


class TestDataLoadErrors:
    """Test data loading error classes."""

    def test_data_load_error_is_critical(self):
        """Test that DataLoadError has CRITICAL severity."""
        exc = DataLoadError("Load error", file_path="/data/file.csv")

        assert exc.severity == ErrorSeverity.CRITICAL
        assert exc.file_path == "/data/file.csv"
        assert exc.details['file_path'] == "/data/file.csv"

    def test_data_load_error_with_line_number(self):
        """Test DataLoadError with line number."""
        exc = DataLoadError(
            "Parse error",
            file_path="test.csv",
            line_number=42
        )

        assert exc.line_number == 42
        assert exc.details['line_number'] == 42

    def test_data_load_error_with_original(self):
        """Test DataLoadError wrapping original exception."""
        original = UnicodeDecodeError('utf-8', b'', 0, 1, 'invalid')
        exc = DataLoadError(
            "Encoding error",
            file_path="test.csv",
            original_exception=original
        )

        assert exc.original_exception == original

    def test_file_not_found_error(self):
        """Test file not found error."""
        exc = FileNotFoundError(
            "data.csv",
            searched_paths=["/data/data.csv", "./data.csv"]
        )

        assert exc.file_path == "data.csv"
        assert exc.details['searched_paths'] == ["/data/data.csv", "./data.csv"]
        assert "File not found" in str(exc)

    def test_unsupported_format_error(self):
        """Test unsupported format error."""
        exc = UnsupportedFormatError(
            "data.xml",
            format="xml",
            supported_formats=["csv", "excel", "json", "parquet"]
        )

        assert exc.details['format'] == "xml"
        assert "csv" in exc.details['supported_formats']
        assert "xml" in str(exc)


class TestValidationExecutionErrors:
    """Test validation execution error classes."""

    def test_validation_error_recoverable(self):
        """Test recoverable validation error."""
        exc = ValidationExecutionError(
            "Validation failed",
            validation_name="TestCheck",
            recoverable=True
        )

        assert exc.severity == ErrorSeverity.RECOVERABLE
        assert exc.validation_name == "TestCheck"
        assert exc.recoverable is True

    def test_validation_error_critical(self):
        """Test critical validation error."""
        exc = ValidationExecutionError(
            "Column missing",
            validation_name="EmailCheck",
            recoverable=False
        )

        assert exc.severity == ErrorSeverity.CRITICAL
        assert exc.recoverable is False

    def test_parameter_validation_error(self):
        """Test parameter validation error."""
        exc = ParameterValidationError(
            "Invalid range",
            validation_name="RangeCheck",
            parameter="min_value",
            value=100
        )

        assert exc.validation_name == "RangeCheck"
        assert exc.details['parameter'] == "min_value"
        assert exc.details['value'] == 100
        assert exc.severity == ErrorSeverity.CRITICAL  # Not recoverable

    def test_column_not_found_error(self):
        """Test column not found error."""
        exc = ColumnNotFoundError(
            validation_name="EmailCheck",
            column="email",
            available_columns=["id", "name", "phone"]
        )

        assert exc.validation_name == "EmailCheck"
        assert exc.details['column'] == "email"
        assert "id" in exc.details['available_columns']
        assert exc.severity == ErrorSeverity.CRITICAL


class TestOtherErrors:
    """Test database, profiler, and reporter errors."""

    def test_database_error(self):
        """Test database error."""
        exc = DatabaseError(
            "Connection failed",
            connection_string="postgresql://localhost:5432/db",
            error_code="08001"
        )

        assert exc.severity == ErrorSeverity.CRITICAL
        assert exc.details['connection_string'] == "postgresql://localhost:5432/db"
        assert exc.details['error_code'] == "08001"

    def test_profiler_error(self):
        """Test profiler error."""
        exc = ProfilerError(
            "Failed to calculate stats",
            operation="correlation_analysis",
            column="sales"
        )

        assert exc.severity == ErrorSeverity.RECOVERABLE
        assert exc.details['operation'] == "correlation_analysis"
        assert exc.details['column'] == "sales"

    def test_reporter_error(self):
        """Test reporter error."""
        exc = ReporterError(
            "Template rendering failed",
            report_type="html",
            output_path="/tmp/report.html"
        )

        assert exc.severity == ErrorSeverity.RECOVERABLE
        assert exc.details['report_type'] == "html"
        assert exc.details['output_path'] == "/tmp/report.html"


class TestExceptionInheritance:
    """Test exception inheritance hierarchy."""

    def test_all_inherit_from_base(self):
        """Test that all exceptions inherit from DataK9Exception."""
        exceptions = [
            ConfigError("test"),
            YAMLSizeError("test"),
            DataLoadError("test", "/path"),
            ValidationExecutionError("test", "check"),
            DatabaseError("test"),
            ProfilerError("test"),
            ReporterError("test")
        ]

        for exc in exceptions:
            assert isinstance(exc, DataK9Exception)
            assert isinstance(exc, Exception)

    def test_config_errors_inherit(self):
        """Test config error inheritance."""
        exc = YAMLSizeError("test")

        assert isinstance(exc, YAMLSizeError)
        assert isinstance(exc, ConfigError)
        assert isinstance(exc, DataK9Exception)

    def test_validation_errors_inherit(self):
        """Test validation error inheritance."""
        exc = ColumnNotFoundError("check", "col", [])

        assert isinstance(exc, ColumnNotFoundError)
        assert isinstance(exc, ValidationExecutionError)
        assert isinstance(exc, DataK9Exception)
