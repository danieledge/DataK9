"""
Observer Pattern for Engine Event Notifications.

This module implements the Observer pattern to decouple the validation engine
from output formatting and progress reporting. This allows the engine to remain
focused on validation logic while observers handle concerns like CLI output,
metrics collection, and logging.

Design Pattern: Observer (Behavioral)
Purpose: Decouple engine from presentation layer

Author: Daniel Edge
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime

from validation_framework.core.results import (
    ValidationResult,
    FileValidationReport,
    ValidationReport,
    Status
)


class EngineObserver(ABC):
    """
    Abstract base class for engine event observers.

    Observers receive notifications about validation lifecycle events
    and can react accordingly (print progress, collect metrics, log events, etc.).

    All methods are called synchronously by the engine, so observers should
    avoid blocking operations. For async operations, queue the event and
    process it in a background thread.

    Example:
        >>> class MyObserver(EngineObserver):
        ...     def on_job_start(self, job_name, file_count):
        ...         print(f"Starting job: {job_name}")
        ...
        >>> engine = ValidationEngine(config, observers=[MyObserver()])
        >>> report = engine.run()
    """

    @abstractmethod
    def on_job_start(self, job_name: str, file_count: int) -> None:
        """
        Called when validation job starts.

        Args:
            job_name: Name of the validation job
            file_count: Number of files to validate
        """
        pass

    @abstractmethod
    def on_file_start(
        self,
        file_name: str,
        file_path: str,
        validation_count: int
    ) -> None:
        """
        Called when file validation starts.

        Args:
            file_name: Logical name of the file
            file_path: Path to the file
            validation_count: Number of validations to run on this file
        """
        pass

    @abstractmethod
    def on_validation_start(
        self,
        validation_type: str,
        file_name: str
    ) -> None:
        """
        Called when individual validation starts.

        Args:
            validation_type: Type of validation (e.g., "MandatoryFieldCheck")
            file_name: Name of file being validated
        """
        pass

    @abstractmethod
    def on_validation_complete(
        self,
        validation_type: str,
        result: ValidationResult
    ) -> None:
        """
        Called when individual validation completes.

        Args:
            validation_type: Type of validation
            result: Validation result object
        """
        pass

    @abstractmethod
    def on_file_complete(self, report: FileValidationReport) -> None:
        """
        Called when file validation completes.

        Args:
            report: Complete file validation report
        """
        pass

    @abstractmethod
    def on_job_complete(self, report: ValidationReport) -> None:
        """
        Called when entire job completes.

        Args:
            report: Complete validation report for all files
        """
        pass

    @abstractmethod
    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """
        Called when error occurs during validation.

        Args:
            error: Exception that occurred
            context: Context dict with details (file_name, validation_type, etc.)
        """
        pass


class CLIProgressObserver(EngineObserver):
    """
    Observer for CLI pretty output and progress reporting.

    This observer handles all terminal output for the validation engine,
    including:
    - Logo and header display
    - File and validation progress
    - Success/failure indicators
    - Summary statistics

    The engine itself has no knowledge of terminal formatting - this
    observer handles all presentation logic.

    Attributes:
        verbose (bool): Whether to show detailed progress
        po (PrettyOutput): Pretty output utility class

    Example:
        >>> observer = CLIProgressObserver(verbose=True)
        >>> engine = ValidationEngine(config, observers=[observer])
        >>> report = engine.run()
        # Prints formatted progress to terminal
    """

    def __init__(self, verbose: bool = True):
        """
        Initialize CLI progress observer.

        Args:
            verbose: If True, show detailed progress. If False, minimal output.
        """
        self.verbose = verbose

        # Import here to avoid circular dependency
        from validation_framework.core.pretty_output import PrettyOutput
        self.po = PrettyOutput

    def on_job_start(self, job_name: str, file_count: int) -> None:
        """Display job start banner with logo and summary."""
        if self.verbose:
            self.po.logo()
            self.po.header("VALIDATION JOB")
            self.po.key_value("Job Name", job_name, indent=2)
            self.po.key_value("Files", file_count, indent=2)
            self.po.blank_line()

    def on_file_start(
        self,
        file_name: str,
        file_path: str,
        validation_count: int
    ) -> None:
        """Display file validation start."""
        if self.verbose:
            self.po.section(f"File: {file_name}")
            self.po.key_value("Path", file_path, indent=2)
            self.po.key_value("Validations", validation_count, indent=2)
            self.po.blank_line()

    def on_validation_start(
        self,
        validation_type: str,
        file_name: str
    ) -> None:
        """Display validation start (minimal - just spinner/progress)."""
        if self.verbose:
            # Could add a spinner here in future
            pass

    def on_validation_complete(
        self,
        validation_type: str,
        result: ValidationResult
    ) -> None:
        """Display validation result (pass/fail indicator)."""
        if self.verbose:
            if result.passed:
                print(f"  {self.po.SUCCESS}{self.po.CHECK} {validation_type}: PASS{self.po.RESET}")
            else:
                print(f"  {self.po.ERROR}{self.po.CROSS} {validation_type}: FAIL ({result.failed_count} failures){self.po.RESET}")

    def on_file_complete(self, report: FileValidationReport) -> None:
        """Display file summary."""
        if self.verbose:
            self.po.blank_line()
            self.po.info(f"File '{report.file_name}' completed: {report.status}")
            self.po.blank_line()

    def on_job_complete(self, report: ValidationReport) -> None:
        """Display final job summary."""
        if self.verbose:
            self.po.header("VALIDATION SUMMARY")

            # Create summary statistics
            total_validations = sum(
                len(file_report.validation_results)
                for file_report in report.file_reports
            )

            passed = sum(
                sum(1 for result in file_report.validation_results if result.passed)
                for file_report in report.file_reports
            )

            failed = total_validations - passed

            summary_items = [
                ("Files Processed", len(report.file_reports), self.po.INFO),
                ("Total Validations", total_validations, self.po.INFO),
                ("Passed", passed, self.po.SUCCESS),
                ("Failed", failed, self.po.ERROR),
                ("Status", report.overall_status.value, self.po.SUCCESS if report.overall_status == Status.PASSED else self.po.ERROR),
                ("Duration", f"{report.duration_seconds:.2f}s", self.po.DIM)
            ]

            self.po.summary_box("Results", summary_items)

    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Display error message."""
        if self.verbose:
            file_name = context.get('file_name', 'unknown')
            validation_type = context.get('validation_type', 'unknown')

            self.po.error(f"Error in {validation_type} for file '{file_name}': {str(error)}")


class MetricsCollectorObserver(EngineObserver):
    """
    Observer for collecting metrics and telemetry.

    This observer collects performance metrics, success/failure counts,
    and other statistics for monitoring and analysis. Metrics can be
    sent to monitoring systems like Prometheus, CloudWatch, or DataDog.

    Attributes:
        metrics (Dict[str, Any]): Collected metrics dictionary

    Example:
        >>> metrics_observer = MetricsCollectorObserver()
        >>> engine = ValidationEngine(config, observers=[metrics_observer])
        >>> report = engine.run()
        >>> print(metrics_observer.metrics)
        {
            'validations_run': 42,
            'validations_passed': 38,
            'validations_failed': 4,
            'total_duration': 12.5,
            'files_processed': 3
        }
    """

    def __init__(self):
        """Initialize metrics collector with empty metrics."""
        self.metrics: Dict[str, Any] = {
            'job_name': None,
            'start_time': None,
            'end_time': None,
            'validations_run': 0,
            'validations_passed': 0,
            'validations_failed': 0,
            'total_duration': 0,
            'files_processed': 0,
            'files_passed': 0,
            'files_failed': 0,
            'errors': []
        }

    def on_job_start(self, job_name: str, file_count: int) -> None:
        """Record job start time and name."""
        self.metrics['job_name'] = job_name
        self.metrics['start_time'] = datetime.now()

    def on_file_start(
        self,
        file_name: str,
        file_path: str,
        validation_count: int
    ) -> None:
        """No action needed for file start."""
        pass

    def on_validation_start(
        self,
        validation_type: str,
        file_name: str
    ) -> None:
        """No action needed for validation start."""
        pass

    def on_validation_complete(
        self,
        validation_type: str,
        result: ValidationResult
    ) -> None:
        """Increment validation counters."""
        self.metrics['validations_run'] += 1

        if result.passed:
            self.metrics['validations_passed'] += 1
        else:
            self.metrics['validations_failed'] += 1

    def on_file_complete(self, report: FileValidationReport) -> None:
        """Increment file counters."""
        self.metrics['files_processed'] += 1

        if report.status == Status.PASSED:
            self.metrics['files_passed'] += 1
        else:
            self.metrics['files_failed'] += 1

    def on_job_complete(self, report: ValidationReport) -> None:
        """Record job end time and calculate duration."""
        self.metrics['end_time'] = datetime.now()
        self.metrics['total_duration'] = report.duration_seconds

        # Could send metrics to monitoring system here
        self._send_metrics()

    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Record error details."""
        self.metrics['errors'].append({
            'error_type': type(error).__name__,
            'error_message': str(error),
            'context': context,
            'timestamp': datetime.now().isoformat()
        })

    def _send_metrics(self) -> None:
        """
        Send metrics to monitoring system.

        Override this method to integrate with your monitoring system:
        - Prometheus: Use prometheus_client
        - CloudWatch: Use boto3.cloudwatch
        - DataDog: Use datadog.statsd
        - Custom: POST to your metrics API
        """
        # Default: No-op (metrics available in self.metrics)
        pass


class LoggingObserver(EngineObserver):
    """
    Observer for structured logging of validation events.

    This observer logs all validation events using Python's logging module
    with structured context. Useful for debugging and audit trails.

    Attributes:
        logger (logging.Logger): Logger instance

    Example:
        >>> import logging
        >>> logging.basicConfig(level=logging.INFO)
        >>> observer = LoggingObserver()
        >>> engine = ValidationEngine(config, observers=[observer])
        >>> report = engine.run()
        # All events logged with structured context
    """

    def __init__(self):
        """Initialize logging observer."""
        import logging
        self.logger = logging.getLogger('validation_framework.engine')

    def on_job_start(self, job_name: str, file_count: int) -> None:
        """Log job start."""
        self.logger.info(
            "Validation job started",
            extra={'job_name': job_name, 'file_count': file_count}
        )

    def on_file_start(
        self,
        file_name: str,
        file_path: str,
        validation_count: int
    ) -> None:
        """Log file validation start."""
        self.logger.info(
            "File validation started",
            extra={
                'file_name': file_name,
                'file_path': file_path,
                'validation_count': validation_count
            }
        )

    def on_validation_start(
        self,
        validation_type: str,
        file_name: str
    ) -> None:
        """Log validation start."""
        self.logger.debug(
            f"Validation started: {validation_type}",
            extra={'validation_type': validation_type, 'file_name': file_name}
        )

    def on_validation_complete(
        self,
        validation_type: str,
        result: ValidationResult
    ) -> None:
        """Log validation completion."""
        level = logging.INFO if result.passed else logging.WARNING

        self.logger.log(
            level,
            f"Validation completed: {validation_type} - {'PASS' if result.passed else 'FAIL'}",
            extra={
                'validation_type': validation_type,
                'passed': result.passed,
                'failed_count': result.failed_count
            }
        )

    def on_file_complete(self, report: FileValidationReport) -> None:
        """Log file completion."""
        self.logger.info(
            f"File validation completed: {report.file_name} - {report.status.value}",
            extra={
                'file_name': report.file_name,
                'status': report.status.value,
                'error_count': report.error_count,
                'warning_count': report.warning_count
            }
        )

    def on_job_complete(self, report: ValidationReport) -> None:
        """Log job completion."""
        self.logger.info(
            f"Validation job completed - {report.overall_status.value}",
            extra={
                'status': report.overall_status.value,
                'total_errors': report.total_errors,
                'total_warnings': report.total_warnings,
                'duration_seconds': report.duration_seconds,
                'file_count': len(report.file_reports)
            }
        )

    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Log error with full context."""
        self.logger.error(
            f"Validation error: {str(error)}",
            extra=context,
            exc_info=True  # Include stack trace
        )


class QuietObserver(EngineObserver):
    """
    Minimal observer that produces no output.

    Useful for programmatic use cases where you want the engine
    to run silently and only inspect the returned report.

    Example:
        >>> observer = QuietObserver()
        >>> engine = ValidationEngine(config, observers=[observer])
        >>> report = engine.run()  # No output to terminal
        >>> print(report.passed)
        True
    """

    def on_job_start(self, job_name: str, file_count: int) -> None:
        """No output."""
        pass

    def on_file_start(
        self,
        file_name: str,
        file_path: str,
        validation_count: int
    ) -> None:
        """No output."""
        pass

    def on_validation_start(
        self,
        validation_type: str,
        file_name: str
    ) -> None:
        """No output."""
        pass

    def on_validation_complete(
        self,
        validation_type: str,
        result: ValidationResult
    ) -> None:
        """No output."""
        pass

    def on_file_complete(self, report: FileValidationReport) -> None:
        """No output."""
        pass

    def on_job_complete(self, report: ValidationReport) -> None:
        """No output."""
        pass

    def on_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """No output."""
        pass
