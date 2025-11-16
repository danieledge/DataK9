"""
Async validation engine for concurrent file validation.

Enables non-blocking validation of multiple files using async/await patterns,
significantly improving throughput for I/O-bound validation workloads.
"""

import asyncio
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging
import time
from datetime import datetime

from validation_framework.core.config import ValidationConfig
from validation_framework.core.registry import get_registry
from validation_framework.core.results import ValidationReport, FileValidationReport, Severity, Status
from validation_framework.loaders.async_factory import AsyncLoaderFactory
from validation_framework.reporters.html_reporter import HTMLReporter
from validation_framework.reporters.json_reporter import JSONReporter

# Import builtin validations to populate registry
import validation_framework.validations.builtin.registry

logger = logging.getLogger(__name__)


class AsyncValidationEngine:
    """
    Async validation engine for concurrent file validation.

    This engine enables:
    - Concurrent validation of multiple files
    - Non-blocking I/O operations
    - Better resource utilization during I/O waits
    - Improved throughput for large validation jobs

    Performance improvements:
    - 2-5x faster for multiple files (depending on I/O wait times)
    - Better CPU utilization during file reads
    - Reduced wall-clock time for validation jobs

    Example:
        >>> config = ValidationConfig.from_yaml('config.yaml')
        >>> engine = AsyncValidationEngine(config)
        >>> report = await engine.run()
        >>> print(f"Validation completed: {report.overall_status}")
    """

    def __init__(self, config: ValidationConfig):
        """
        Initialize async validation engine.

        Args:
            config: ValidationConfig instance with job configuration
        """
        self.config = config
        self.registry = get_registry()

    async def run(self) -> ValidationReport:
        """
        Run validation job asynchronously.

        Validates all configured files concurrently, collecting results
        and generating reports.

        Returns:
            ValidationReport with overall results

        Example:
            >>> engine = AsyncValidationEngine(config)
            >>> report = await engine.run()
            >>> if report.overall_status == Status.FAILED:
            ...     print(f"Validation failed: {report.total_errors} errors")
        """
        logger.info(f"Starting async validation job: {self.config.job_name}")
        logger.info(f"Validating {len(self.config.files)} files concurrently")

        start_time = time.time()

        # Validate all files concurrently
        file_reports = await asyncio.gather(
            *[self._validate_file(file_config) for file_config in self.config.files],
            return_exceptions=True
        )

        # Handle any exceptions from file validations
        processed_reports = []
        for i, report in enumerate(file_reports):
            if isinstance(report, Exception):
                logger.error(f"Error validating file {self.config.files[i]['name']}: {str(report)}")
                # Create error report for failed file
                error_report = FileValidationReport(
                    file_name=self.config.files[i]['name'],
                    file_path=self.config.files[i]['path'],
                    file_format=self.config.files[i].get('format', 'csv'),
                    status=Status.FAILED,
                    error_count=1
                )
                error_report.metadata['error'] = f"Validation failed: {str(report)}"
                processed_reports.append(error_report)
            else:
                processed_reports.append(report)

        # Calculate duration
        duration = time.time() - start_time

        # Create overall validation report
        overall_report = ValidationReport(
            job_name=self.config.job_name,
            execution_time=datetime.now(),
            duration_seconds=duration,
            overall_status=Status.PASSED,  # Will be updated based on file reports
            file_reports=processed_reports
        )

        # Update overall status based on file reports
        overall_report.update_overall_status()

        # Generate reports
        await self._generate_reports(overall_report)

        logger.info(f"Async validation completed: {overall_report.overall_status}")
        logger.info(f"Files processed: {len(processed_reports)}, "
                   f"Errors: {overall_report.total_errors}, "
                   f"Warnings: {overall_report.total_warnings}")

        return overall_report

    async def _validate_file(self, file_config: Dict[str, Any]) -> FileValidationReport:
        """
        Validate a single file asynchronously.

        Args:
            file_config: File configuration dictionary

        Returns:
            FileValidationReport with validation results
        """
        file_name = file_config["name"]
        file_path = file_config["path"]
        file_format = file_config.get("format", "csv")

        logger.info(f"Async validating file: {file_name} ({file_path})")

        # Create file validation report
        file_report = FileValidationReport(
            file_name=file_name,
            file_path=file_path,
            file_format=file_format,
            status=Status.PASSED  # Will be updated based on validation results
        )

        try:
            # Create async loader
            loader_kwargs = {
                "delimiter": file_config.get("delimiter", ","),
                "encoding": file_config.get("encoding", "utf-8"),
                "header": file_config.get("header", 0),
            }

            loader = await AsyncLoaderFactory.create_loader(
                file_path=file_path,
                file_format=file_format,
                chunk_size=self.config.chunk_size,
                **loader_kwargs
            )

            # Get file metadata
            metadata = await loader.get_metadata()
            file_report.metadata = metadata

            # Run validations for this file
            validations = file_config.get("validations", [])
            logger.info(f"Running {len(validations)} validations on {file_name}")

            # Execute validations concurrently where possible
            # Note: Some validations may need to be sequential (e.g., cross-file)
            validation_results = await self._execute_validations(
                validations,
                loader,
                file_config
            )

            # Add results to report
            for result in validation_results:
                file_report.add_result(result)

        except Exception as e:
            logger.error(f"Error validating {file_name}: {str(e)}", exc_info=True)
            file_report.status = Status.FAILED
            file_report.error_count += 1
            file_report.metadata['error'] = f"Validation error: {str(e)}"

        # Update file report status based on validation results
        file_report.update_status()

        return file_report

    async def _execute_validations(
        self,
        validations: List[Dict[str, Any]],
        loader,
        file_config: Dict[str, Any]
    ) -> List:
        """
        Execute all validations for a file.

        Note: Currently runs validations sequentially to maintain data consistency.
        Future enhancement: Run independent validations concurrently.

        Args:
            validations: List of validation configurations
            loader: Async data loader
            file_config: File configuration

        Returns:
            List of ValidationResult objects
        """
        results = []

        for val_config in validations:
            if not val_config.get("enabled", True):
                continue

            validation_type = val_config["type"]
            severity = val_config["severity"]
            params = val_config.get("params", {})
            condition = val_config.get("condition")

            try:
                # Get validation class from registry
                validation_class = self.registry.get(validation_type)

                # Instantiate validation
                validation = validation_class(
                    name=validation_type,
                    severity=severity,
                    params=params,
                    condition=condition
                )

                # Execute validation
                # Convert async loader to sync iterator for compatibility with existing validators
                # TODO: Create async validators for full async support
                data_iterator = await self._async_loader_to_sync(loader)

                context = {
                    "file_name": file_config["name"],
                    "file_path": file_config["path"],
                    "max_sample_failures": self.config.max_sample_failures,
                }

                # Run validation in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None,
                    validation.validate,
                    data_iterator,
                    context
                )

                results.append(result)

                logger.debug(f"Validation {validation_type}: {'PASSED' if result.passed else 'FAILED'}")

            except Exception as e:
                logger.error(f"Error in validation {validation_type}: {str(e)}", exc_info=True)
                # Create error result directly without validation object
                from validation_framework.core.results import ValidationResult
                error_result = ValidationResult(
                    rule_name=validation_type,
                    severity=Severity[severity] if isinstance(severity, str) else severity,
                    passed=False,
                    message=f"Validation error: {str(e)}",
                    failed_count=1,
                    total_count=1
                )
                results.append(error_result)

        return results

    async def _async_loader_to_sync(self, async_loader):
        """
        Convert async loader to synchronous iterator.

        This is a compatibility layer until we implement fully async validators.
        Loads all chunks into memory then returns sync iterator.

        Args:
            async_loader: Async data loader

        Returns:
            Iterator of DataFrames

        Note:
            This temporarily defeats the memory efficiency of chunking.
            Future enhancement: Implement async validators that work with async iterators.
        """
        chunks = []
        async for chunk in async_loader.load():
            chunks.append(chunk)

        return iter(chunks)

    async def _generate_reports(self, report: ValidationReport) -> None:
        """
        Generate HTML and JSON reports asynchronously.

        Args:
            report: ValidationReport to generate reports from
        """
        logger.info("Generating validation reports")

        # Run report generation in thread pool (I/O bound)
        loop = asyncio.get_event_loop()

        async def _gen_html():
            html_reporter = HTMLReporter()
            await loop.run_in_executor(
                None,
                html_reporter.generate,
                report,
                self.config.html_report_path
            )
            logger.info(f"HTML report generated: {self.config.html_report_path}")

        async def _gen_json():
            json_reporter = JSONReporter()
            await loop.run_in_executor(
                None,
                json_reporter.generate,
                report,
                self.config.json_summary_path
            )
            logger.info(f"JSON report generated: {self.config.json_summary_path}")

        # Generate reports concurrently
        await asyncio.gather(_gen_html(), _gen_json())


async def run_async_validation(config_path: str) -> ValidationReport:
    """
    Convenience function to run async validation from a config file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        ValidationReport with results

    Example:
        >>> report = await run_async_validation('config.yaml')
        >>> print(f"Status: {report.overall_status}")
    """
    config = ValidationConfig.from_yaml(config_path)
    engine = AsyncValidationEngine(config)
    return await engine.run()


async def run_async_validation_concurrent(config_paths: List[str]) -> List[ValidationReport]:
    """
    Run multiple validation jobs concurrently.

    This allows parallel execution of multiple independent validation jobs,
    maximizing throughput for batch validation scenarios.

    Args:
        config_paths: List of paths to YAML configuration files

    Returns:
        List of ValidationReport objects

    Example:
        >>> config_files = ['job1.yaml', 'job2.yaml', 'job3.yaml']
        >>> reports = await run_async_validation_concurrent(config_files)
        >>> for report in reports:
        ...     print(f"{report.job_name}: {report.overall_status}")
    """
    # Load all configs
    configs = [ValidationConfig.from_yaml(path) for path in config_paths]

    # Create engines
    engines = [AsyncValidationEngine(config) for config in configs]

    # Run all jobs concurrently
    reports = await asyncio.gather(
        *[engine.run() for engine in engines],
        return_exceptions=True
    )

    # Handle exceptions
    processed_reports = []
    for i, report in enumerate(reports):
        if isinstance(report, Exception):
            logger.error(f"Error in validation job {config_paths[i]}: {str(report)}")
            # Create error report
            error_report = ValidationReport(
                job_name=f"Failed: {config_paths[i]}",
                execution_time=datetime.now(),
                duration_seconds=0.0,
                overall_status=Status.FAILED,
                file_reports=[]
            )
            processed_reports.append(error_report)
        else:
            processed_reports.append(report)

    return processed_reports
