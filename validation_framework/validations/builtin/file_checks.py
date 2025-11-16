"""
File-level validation rules.

These validations check file properties before data processing:
- File existence
- Empty file detection (0 bytes)
- Row count constraints
- File size limits
"""

from typing import Iterator, Dict, Any
import pandas as pd
from pathlib import Path
from validation_framework.validations.base import FileValidationRule, ValidationResult


class EmptyFileCheck(FileValidationRule):
    """
    Validates that a file is not empty and optionally has data rows.

    This is a critical check that should run before any data processing.
    An empty file often indicates an upstream failure in data generation.

    Checks for:
    1. File is not 0 bytes (literal empty file)
    2. File has data rows, not just a header (when check_data_rows=true)

    Configuration:
        severity: ERROR or WARNING (typically ERROR)
        params:
            check_data_rows (bool, optional): If true, also verify file has at least
                one data row (not just a header). Default: false

    Example YAML:
        # Check only for 0-byte files
        - type: "EmptyFileCheck"
          severity: "ERROR"

        # Check for both empty files and header-only files
        - type: "EmptyFileCheck"
          severity: "ERROR"
          params:
            check_data_rows: true
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        check_data_rows = self.params.get("check_data_rows", False)

        if check_data_rows:
            return "Checks that the file is not empty and contains data rows (not just headers)"
        return "Checks that the file is not empty (0 bytes)"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Check if file is empty.

        Args:
            context: Must contain 'file_path' key

        Returns:
            ValidationResult with passed=True if file has content
        """
        try:
            file_path = context.get("file_path")
            if not file_path:
                return self._create_result(
                    passed=False,
                    message="File path not provided in context",
                    failed_count=1,
                )

            # Check file size
            file_size = Path(file_path).stat().st_size

            if file_size == 0:
                return self._create_result(
                    passed=False,
                    message=f"File is empty (0 bytes): {file_path}",
                    failed_count=1,
                )

            # Check if we should also verify data rows exist
            check_data_rows = self.params.get("check_data_rows", False)

            if check_data_rows:
                # Need to peek at the file to check for data rows
                # since FileValidationRule runs before data processing
                file_format = context.get("file_format", "csv")

                try:
                    if file_format.lower() == "csv":
                        # Read first 2 lines to check if there's data beyond header
                        import csv
                        with open(file_path, 'r', encoding='utf-8') as f:
                            reader = csv.reader(f)
                            lines = []
                            for i, line in enumerate(reader):
                                lines.append(line)
                                if i >= 1:  # Read header + first data row
                                    break

                            # If we only got 1 line or less, it's header-only
                            if len(lines) <= 1:
                                return self._create_result(
                                    passed=False,
                                    message=f"File contains only headers with no data rows: {file_path}",
                                    failed_count=1,
                                )

                    elif file_format.lower() in ["excel", "xlsx", "xls"]:
                        # Check Excel file for data rows
                        df = pd.read_excel(file_path, nrows=1)
                        if len(df) == 0:
                            return self._create_result(
                                passed=False,
                                message=f"File contains only headers with no data rows: {file_path}",
                                failed_count=1,
                            )

                    elif file_format.lower() == "json":
                        # Check JSON file for data
                        import json
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            if isinstance(data, list) and len(data) == 0:
                                return self._create_result(
                                    passed=False,
                                    message=f"File contains no data records: {file_path}",
                                    failed_count=1,
                                )

                    elif file_format.lower() == "parquet":
                        # Check Parquet file for data rows
                        df = pd.read_parquet(file_path)
                        if len(df) == 0:
                            return self._create_result(
                                passed=False,
                                message=f"File contains only headers with no data rows: {file_path}",
                                failed_count=1,
                            )

                    return self._create_result(
                        passed=True,
                        message=f"File contains data rows ({file_size} bytes)",
                        total_count=1,
                    )

                except Exception as e:
                    # If we can't read the file, report error
                    return self._create_result(
                        passed=False,
                        message=f"Error checking for data rows: {str(e)}",
                        failed_count=1,
                    )

            return self._create_result(
                passed=True,
                message=f"File contains data ({file_size} bytes)",
                total_count=1,
            )

        except FileNotFoundError:
            return self._create_result(
                passed=False,
                message=f"File not found: {file_path}",
                failed_count=1,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error checking file: {str(e)}",
                failed_count=1,
            )


class RowCountRangeCheck(FileValidationRule):
    """
    Validates that the total row count falls within specified range.

    Useful for detecting data loading issues where too few or too many
    rows might indicate a problem.

    Configuration:
        params:
            min_rows (int, optional): Minimum expected rows
            max_rows (int, optional): Maximum expected rows

    Example YAML:
        - type: "RowCountRangeCheck"
          severity: "WARNING"
          params:
            min_rows: 100
            max_rows: 1000000
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        min_rows = self.params.get("min_rows")
        max_rows = self.params.get("max_rows")

        if min_rows and max_rows:
            return f"Checks that row count is between {min_rows} and {max_rows}"
        elif min_rows:
            return f"Checks that row count is at least {min_rows}"
        elif max_rows:
            return f"Checks that row count is at most {max_rows}"
        else:
            return "Checks row count (no limits specified)"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Check if row count is within specified range.

        Args:
            context: Must contain 'total_rows' or 'estimated_rows' key with row count

        Returns:
            ValidationResult indicating if row count is acceptable
        """
        try:
            # Get actual row count from context (populated by engine)
            # Try total_rows first (accurate), then estimated_rows (from metadata)
            actual_rows = context.get("total_rows") or context.get("estimated_rows", 0)

            min_rows = self.params.get("min_rows")
            max_rows = self.params.get("max_rows")

            # Check minimum
            if min_rows is not None and actual_rows < min_rows:
                return self._create_result(
                    passed=False,
                    message=f"Row count {actual_rows} is below minimum {min_rows}",
                    failed_count=1,
                    total_count=1,
                )

            # Check maximum
            if max_rows is not None and actual_rows > max_rows:
                return self._create_result(
                    passed=False,
                    message=f"Row count {actual_rows} exceeds maximum {max_rows}",
                    failed_count=1,
                    total_count=1,
                )

            return self._create_result(
                passed=True,
                message=f"Row count {actual_rows} is within acceptable range",
                total_count=1,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error checking row count: {str(e)}",
                failed_count=1,
            )


class FileSizeCheck(FileValidationRule):
    """
    Validates that file size is within acceptable limits.

    Useful for detecting unexpectedly large files that might cause
    processing issues or indicate data quality problems.

    Configuration:
        params:
            min_size_mb (float, optional): Minimum file size in MB
            max_size_mb (float, optional): Maximum file size in MB
            max_size_gb (float, optional): Maximum file size in GB (alternative)

    Example YAML:
        - type: "FileSizeCheck"
          severity: "WARNING"
          params:
            max_size_gb: 250
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        min_mb = self.params.get("min_size_mb")
        max_mb = self.params.get("max_size_mb")
        max_gb = self.params.get("max_size_gb")

        if max_gb:
            return f"Checks that file size does not exceed {max_gb} GB"
        elif min_mb and max_mb:
            return f"Checks that file size is between {min_mb} and {max_mb} MB"
        elif max_mb:
            return f"Checks that file size does not exceed {max_mb} MB"
        elif min_mb:
            return f"Checks that file size is at least {min_mb} MB"
        else:
            return "Checks file size (no limits specified)"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Check if file size is within acceptable range.

        Args:
            context: Must contain 'file_path' key

        Returns:
            ValidationResult indicating if file size is acceptable
        """
        try:
            file_path = context.get("file_path")
            if not file_path:
                return self._create_result(
                    passed=False,
                    message="File path not provided in context",
                    failed_count=1,
                )

            # Get file size in bytes
            file_size_bytes = Path(file_path).stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)
            file_size_gb = file_size_bytes / (1024 * 1024 * 1024)

            # Check against limits
            min_mb = self.params.get("min_size_mb")
            max_mb = self.params.get("max_size_mb")
            max_gb = self.params.get("max_size_gb")

            # Convert GB to MB if specified
            if max_gb:
                max_mb = max_gb * 1024

            # Check minimum
            if min_mb is not None and file_size_mb < min_mb:
                return self._create_result(
                    passed=False,
                    message=f"File size {file_size_mb:.2f} MB is below minimum {min_mb} MB",
                    failed_count=1,
                )

            # Check maximum
            if max_mb is not None and file_size_mb > max_mb:
                return self._create_result(
                    passed=False,
                    message=f"File size {file_size_gb:.2f} GB exceeds maximum {max_mb/1024:.2f} GB",
                    failed_count=1,
                )

            return self._create_result(
                passed=True,
                message=f"File size {file_size_gb:.2f} GB is within acceptable range",
                total_count=1,
            )

        except FileNotFoundError:
            return self._create_result(
                passed=False,
                message=f"File not found: {file_path}",
                failed_count=1,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error checking file size: {str(e)}",
                failed_count=1,
            )
