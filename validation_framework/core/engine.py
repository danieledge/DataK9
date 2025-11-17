"""
Validation engine - orchestrates the entire validation process.

The engine:
1. Loads configuration
2. Creates data loaders for each file
3. Executes validations in order
4. Collects and aggregates results
5. Generates reports
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

from validation_framework.core.config import ValidationConfig
from validation_framework.core.registry import get_registry, ValidationRegistry
from validation_framework.core.results import (
    ValidationReport,
    FileValidationReport,
    Status,
)
from validation_framework.loaders.factory import LoaderFactory
from validation_framework.core.logging_config import get_logger

# Import to trigger registration of built-in validations
import validation_framework.validations.builtin.registry  # noqa

logger = get_logger(__name__)

# Optional imports for colored output
try:
    import colorama
    from colorama import Fore, Style
    from validation_framework.core.pretty_output import PrettyOutput as po
    colorama.init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False
    # Create dummy color classes
    class Fore:
        CYAN = ''
        YELLOW = ''
        GREEN = ''
        RED = ''
    class Style:
        RESET_ALL = ''


class ValidationEngine:
    """
    Main validation engine that orchestrates the validation process.

    The engine loads configuration, creates appropriate data loaders,
    executes validations, and generates comprehensive reports.

    Example usage:
        # From config file
        engine = ValidationEngine.from_config('validation_config.yaml')
        report = engine.run()

        # Generate reports
        engine.generate_html_report(report, 'report.html')
        engine.generate_json_report(report, 'report.json')
    """

    def __init__(
        self,
        config: ValidationConfig,
        observers: Optional[List['EngineObserver']] = None
    ) -> None:
        """
        Initialize the validation engine.

        Args:
            config: Validation configuration object
            observers: Optional list of observers to receive engine events.
                      If None, creates a default CLIProgressObserver for
                      backwards compatibility.
        """
        self.config: ValidationConfig = config
        self.registry: ValidationRegistry = get_registry()
        self.observers: List['EngineObserver'] = observers if observers is not None else []

    @classmethod
    def from_config(cls, config_path: str) -> "ValidationEngine":
        """
        Create engine from YAML configuration file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            ValidationEngine instance

        Raises:
            ConfigError: If configuration is invalid
        """
        config = ValidationConfig.from_yaml(config_path)
        return cls(config)

    # Observer notification methods
    def _notify_job_start(self, job_name: str, file_count: int) -> None:
        """Notify observers that job is starting."""
        for observer in self.observers:
            try:
                observer.on_job_start(job_name, file_count)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_job_start: {e}")

    def _notify_file_start(self, file_name: str, file_path: str, validation_count: int) -> None:
        """Notify observers that file validation is starting."""
        for observer in self.observers:
            try:
                observer.on_file_start(file_name, file_path, validation_count)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_file_start: {e}")

    def _notify_validation_start(self, validation_type: str, file_name: str) -> None:
        """Notify observers that validation is starting."""
        for observer in self.observers:
            try:
                observer.on_validation_start(validation_type, file_name)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_validation_start: {e}")

    def _notify_validation_complete(self, validation_type: str, result: 'ValidationResult') -> None:
        """Notify observers that validation is complete."""
        for observer in self.observers:
            try:
                observer.on_validation_complete(validation_type, result)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_validation_complete: {e}")

    def _notify_file_complete(self, report: 'FileValidationReport') -> None:
        """Notify observers that file validation is complete."""
        for observer in self.observers:
            try:
                observer.on_file_complete(report)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_file_complete: {e}")

    def _notify_job_complete(self, report: 'ValidationReport') -> None:
        """Notify observers that job is complete."""
        for observer in self.observers:
            try:
                observer.on_job_complete(report)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_job_complete: {e}")

    def _notify_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """Notify observers of an error."""
        for observer in self.observers:
            try:
                observer.on_error(error, context)
            except Exception as e:
                logger.warning(f"Observer {observer.__class__.__name__} failed on_error: {e}")

    def run(self, verbose: bool = True) -> ValidationReport:
        """
        Execute all validations defined in the configuration.

        Args:
            verbose: If True, print progress information (auto-adds CLIProgressObserver
                    if no observers were provided)

        Returns:
            ValidationReport with complete validation results
        """
        logger.info(f"Starting validation job: {self.config.job_name}")
        logger.debug(f"Number of files to validate: {len(self.config.files)}")

        # For backwards compatibility: if verbose=True and no observers provided,
        # add a CLIProgressObserver
        if verbose and not self.observers:
            from validation_framework.core.observers import CLIProgressObserver
            self.observers = [CLIProgressObserver(verbose=True)]

        # Notify observers that job is starting
        self._notify_job_start(self.config.job_name, len(self.config.files))

        start_time = time.time()

        # Create overall report
        report = ValidationReport(
            job_name=self.config.job_name,
            execution_time=datetime.now(),
            duration_seconds=0,  # Will be updated at end
            overall_status=Status.PASSED,
            config=self.config.to_dict(),
            description=self.config.description,
        )
        logger.debug("Validation report initialized")

        # Process each file
        for file_idx, file_config in enumerate(self.config.files, 1):
            logger.info(f"Processing file {file_idx}/{len(self.config.files)}: {file_config['name']}")
            logger.debug(f"File path: {file_config['path']}, Format: {file_config['format']}")

            # Notify observers that file validation is starting
            self._notify_file_start(
                file_config['name'],
                file_config['path'],
                len(file_config['validations'])
            )

            # Validate the file
            file_report = self._validate_file(file_config, verbose)
            logger.info(f"File validation completed: {file_config['name']} - Status: {file_report.status.value}")

            # Add to overall report
            report.add_file_report(file_report)

            # Notify observers that file validation is complete
            self._notify_file_complete(file_report)

            # Legacy verbose output (will be removed once observers fully integrated)
            if verbose and not self.observers:
                error_color = po.ERROR if file_report.error_count > 0 else po.DIM
                warning_color = po.WARNING if file_report.warning_count > 0 else po.DIM
                po.key_value("Errors", file_report.error_count, indent=2, value_color=error_color)
                po.key_value("Warnings", file_report.warning_count, indent=2, value_color=warning_color)
                po.key_value("Duration", f"{file_report.execution_time:.2f}s", indent=2, value_color=po.DIM)
                po.blank_line()

        # Update overall status and duration
        report.update_overall_status()
        report.duration_seconds = time.time() - start_time

        logger.info(f"Validation job completed in {report.duration_seconds:.2f}s")
        logger.info(f"Overall status: {report.overall_status.value} (Errors: {report.total_errors}, Warnings: {report.total_warnings})")

        # Notify observers that job is complete
        self._notify_job_complete(report)

        return report

    def _validate_file(self, file_config: Dict[str, Any], verbose: bool) -> FileValidationReport:
        """
        Validate a single file.

        Args:
            file_config: File configuration dictionary
            verbose: Whether to print progress

        Returns:
            FileValidationReport with all validation results for this file
        """
        start_time = time.time()

        # Create file report
        file_report = FileValidationReport(
            file_name=file_config["name"],
            file_path=file_config["path"],
            file_format=file_config["format"],
            status=Status.PASSED,
        )

        try:
            # Create data loader
            loader = LoaderFactory.create_loader(
                file_path=file_config["path"],
                file_format=file_config["format"],
                chunk_size=self.config.chunk_size,
                delimiter=file_config.get("delimiter"),
                encoding=file_config.get("encoding"),
                header=file_config.get("header"),
                sheet_name=file_config.get("sheet_name"),
            )

            # Get file metadata
            metadata = loader.get_metadata()
            file_report.metadata = metadata

            # Build validation context
            context = {
                "file_path": file_config["path"],
                "file_name": file_config["name"],
                "file_format": file_config["format"],
                "file_config": file_config,  # Include full file config for validations that need it
                "max_sample_failures": self.config.max_sample_failures,
                **metadata,
            }

            # Execute each validation
            validations = file_config.get("validations", [])

            if verbose and validations:
                po.subsection("Executing Validations")

            for val_idx, validation_config in enumerate(validations, 1):
                if not validation_config.get("enabled", True):
                    continue

                validation_type = validation_config["type"]

                if verbose:
                    print(f"  {po.DIM}[{val_idx}/{len(validations)}]{po.RESET} {validation_type}...", end=" ", flush=True)

                try:
                    # Get validation class from registry
                    validation_class = self.registry.get(validation_type)

                    # Instantiate validation
                    validation = validation_class(
                        name=validation_type,
                        severity=validation_config["severity"],
                        params=validation_config.get("params", {}),
                        condition=validation_config.get("condition"),
                    )

                    # Execute validation
                    exec_start = time.time()

                    # Create fresh data iterator for this validation
                    data_iterator = loader.load()

                    result = validation.validate(data_iterator, context)
                    result.execution_time = time.time() - exec_start

                    # Add result to report
                    file_report.add_result(result)

                    if verbose:
                        if result.passed:
                            print(f"{po.SUCCESS}{po.CHECK} PASS{po.RESET}")
                        else:
                            print(f"{po.ERROR}{po.CROSS} FAIL{po.RESET} ({result.failed_count} failures)")

                except KeyError:
                    if verbose:
                        print(f"{po.ERROR}{po.CROSS} NOT FOUND{po.RESET}")
                    # Create error result for unknown validation
                    from validation_framework.core.results import ValidationResult, Severity
                    error_result = ValidationResult(
                        rule_name=validation_type,
                        severity=validation_config["severity"],
                        passed=False,
                        message=f"Validation type '{validation_type}' not found in registry",
                        failed_count=1,
                    )
                    file_report.add_result(error_result)

                except Exception as e:
                    if verbose:
                        print(f"{po.ERROR}{po.CROSS} ERROR{po.RESET}")
                    # Create error result
                    from validation_framework.core.results import ValidationResult
                    error_result = ValidationResult(
                        rule_name=validation_type,
                        severity=validation_config["severity"],
                        passed=False,
                        message=f"Error executing validation: {str(e)}",
                        failed_count=1,
                    )
                    file_report.add_result(error_result)

        except FileNotFoundError:
            if verbose:
                po.error("File not found!", indent=2)
            # Create error result
            from validation_framework.core.results import ValidationResult, Severity
            error_result = ValidationResult(
                rule_name="FileExistence",
                severity=Severity.ERROR,
                passed=False,
                message=f"File not found: {file_config['path']}",
                failed_count=1,
            )
            file_report.add_result(error_result)

        except Exception as e:
            if verbose:
                po.error(f"Error: {str(e)}", indent=2)
            # Create error result
            from validation_framework.core.results import ValidationResult, Severity
            error_result = ValidationResult(
                rule_name="FileProcessing",
                severity=Severity.ERROR,
                passed=False,
                message=f"Error processing file: {str(e)}",
                failed_count=1,
            )
            file_report.add_result(error_result)

        # Update file report status and duration
        file_report.update_status()
        file_report.execution_time = time.time() - start_time

        return file_report

    def _print_summary(self, report: ValidationReport) -> None:
        """
        Print a summary of the validation results.

        Args:
            report: ValidationReport to summarize
        """
        po.header("VALIDATION SUMMARY")

        # Overall status
        status_color = po.SUCCESS if report.overall_status == Status.PASSED else po.ERROR
        error_color = po.ERROR if report.total_errors > 0 else po.DIM
        warning_color = po.WARNING if report.total_warnings > 0 else po.DIM

        # Summary box with key metrics
        po.summary_box(
            title="Overall Results",
            items=[
                ("Status", report.overall_status.value, status_color),
                ("Total Errors", report.total_errors, error_color),
                ("Total Warnings", report.total_warnings, warning_color),
                ("Total Validations", report.total_validations, po.INFO),
                ("Files Processed", len(report.file_reports), po.PRIMARY),
                ("Duration", f"{report.duration_seconds:.2f}s", po.DIM),
            ]
        )

        # Per-file summary
        if len(report.file_reports) > 1:
            po.subsection("File Results")
            for file_report in report.file_reports:
                if file_report.status == Status.PASSED:
                    po.success(f"{file_report.file_name}: {file_report.error_count} errors, {file_report.warning_count} warnings", indent=2)
                else:
                    po.error(f"{file_report.file_name}: {file_report.error_count} errors, {file_report.warning_count} warnings", indent=2)
            po.blank_line()

    def generate_html_report(self, report: ValidationReport, output_path: str) -> None:
        """
        Generate HTML report.

        Args:
            report: ValidationReport to convert to HTML
            output_path: Path for output HTML file
        """
        from validation_framework.reporters.html_reporter import HTMLReporter
        reporter = HTMLReporter()
        reporter.generate(report, output_path)

    def generate_json_report(self, report: ValidationReport, output_path: str) -> None:
        """
        Generate JSON report.

        Args:
            report: ValidationReport to convert to JSON
            output_path: Path for output JSON file
        """
        from validation_framework.reporters.json_reporter import JSONReporter
        reporter = JSONReporter()
        reporter.generate(report, output_path)

    def list_available_validations(self) -> List[str]:
        """
        Get list of all available validation types.

        Returns:
            List of validation type names
        """
        return self.registry.list_available()
