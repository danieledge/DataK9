"""
Cross-file validation rules.

These validations check relationships and consistency across multiple data files:
- Referential integrity (foreign key relationships)
- Cross-file comparisons and aggregations
- Duplicate detection across files
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
from pathlib import Path
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.exceptions import (
    ColumnNotFoundError,
    ParameterValidationError,
    DataLoadError
)
from validation_framework.core.constants import MAX_SAMPLE_FAILURES
import logging

logger = logging.getLogger(__name__)


class ReferentialIntegrityCheck(DataValidationRule):
    """
    Validates foreign key relationships between two files.

    Ensures that values in a column reference valid values in another file's column.
    This is critical for maintaining data integrity across related datasets.

    Configuration:
        params:
            foreign_key (str, required): Column name in the current file
            reference_file (str, required): Path to the reference file (relative or absolute)
            reference_key (str, required): Column name in the reference file to check against
            allow_null (bool, optional): Whether NULL values are allowed in foreign key. Default: false
            reference_file_format (str, optional): Format of reference file (csv, parquet, excel, json). Default: csv

    Example YAML:
        # Validate customer_id exists in customers file
        - type: "ReferentialIntegrityCheck"
          severity: "ERROR"
          params:
            foreign_key: "customer_id"
            reference_file: "customers.csv"
            reference_key: "id"
            allow_null: false

        # Validate with Parquet reference
        - type: "ReferentialIntegrityCheck"
          severity: "ERROR"
          params:
            foreign_key: "product_id"
            reference_file: "products.parquet"
            reference_key: "product_id"
            reference_file_format: "parquet"
            allow_null: true
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        foreign_key = self.params.get("foreign_key", "?")
        reference_file = self.params.get("reference_file", "?")
        reference_key = self.params.get("reference_key", "?")

        return f"Validates that {foreign_key} references valid values from {reference_file}:{reference_key}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check referential integrity against reference file.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Must contain 'file_path' or 'base_path' for resolving reference file

        Returns:
            ValidationResult indicating foreign key violations
        """
        try:
            # Get parameters
            foreign_key = self.params.get("foreign_key")
            reference_file = self.params.get("reference_file")
            reference_key = self.params.get("reference_key")
            allow_null = self.params.get("allow_null", False)
            reference_format = self.params.get("reference_file_format", "csv")

            # Validate required parameters
            if not foreign_key:
                raise ParameterValidationError(
                    "Parameter 'foreign_key' is required",
                    validation_name=self.name,
                    parameter="foreign_key",
                    value=None
                )

            if not reference_file:
                raise ParameterValidationError(
                    "Parameter 'reference_file' is required",
                    validation_name=self.name,
                    parameter="reference_file",
                    value=None
                )

            if not reference_key:
                raise ParameterValidationError(
                    "Parameter 'reference_key' is required",
                    validation_name=self.name,
                    parameter="reference_key",
                    value=None
                )

            # Resolve reference file path
            reference_path = self._resolve_reference_path(reference_file, context)
            if not Path(reference_path).exists():
                return self._create_result(
                    passed=False,
                    message=f"Reference file not found: {reference_path}",
                    failed_count=1,
                )

            # Load reference values
            reference_values = self._load_reference_values(
                reference_path,
                reference_key,
                reference_format
            )

            if reference_values is None:
                return self._create_result(
                    passed=False,
                    message=f"Failed to load reference values from {reference_path}",
                    failed_count=1,
                )

            # Convert to set for efficient lookups
            valid_values = set(reference_values)

            # Validate foreign keys across all chunks
            total_checked = 0
            total_violations = 0
            sample_violations = []
            max_samples = 10

            for chunk in data_iterator:
                # Check if foreign key column exists
                if foreign_key not in chunk.columns:
                    raise ColumnNotFoundError(
                        validation_name=self.name,
                        column=foreign_key,
                        available_columns=list(chunk.columns)
                    )

                # Apply conditional filter if specified
                if self.condition:
                    mask = self._evaluate_condition(chunk)
                    chunk_to_check = chunk[mask]
                else:
                    chunk_to_check = chunk

                # Get foreign key values
                fk_values = chunk_to_check[foreign_key]

                # Handle nulls
                null_mask = fk_values.isnull()
                if not allow_null and null_mask.any():
                    null_violations = chunk_to_check[null_mask]
                    total_violations += len(null_violations)

                    # Collect samples
                    if len(sample_violations) < max_samples:
                        for idx, row in null_violations.head(max_samples - len(sample_violations)).iterrows():
                            sample_violations.append({
                                "row_index": int(idx),
                                "foreign_key": foreign_key,
                                "value": None,
                                "reason": "NULL value in foreign key (not allowed)"
                            })

                # Always drop nulls before checking against reference
                # (nulls are either already reported as violations, or are allowed and should be skipped)
                fk_values = fk_values.dropna()

                # Check which values are not in reference
                invalid_mask = ~fk_values.isin(valid_values)
                if invalid_mask.any():
                    # Get rows with invalid foreign keys using .loc with the index
                    invalid_indices = fk_values.index[invalid_mask]
                    invalid_values = chunk_to_check.loc[invalid_indices]
                    total_violations += len(invalid_values)

                    # Collect samples
                    if len(sample_violations) < max_samples:
                        for idx, row in invalid_values.head(max_samples - len(sample_violations)).iterrows():
                            sample_violations.append({
                                "row_index": int(idx),
                                "foreign_key": foreign_key,
                                "value": str(row[foreign_key]),
                                "reason": f"Value not found in {reference_file}:{reference_key}"
                            })

                total_checked += len(chunk_to_check)

            # Build result
            if total_violations > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {total_violations} referential integrity violations in {foreign_key}",
                    failed_count=total_violations,
                    total_count=total_checked,
                    sample_failures=sample_violations,
                )

            return self._create_result(
                passed=True,
                message=f"All {total_checked} foreign key values in {foreign_key} are valid",
                total_count=total_checked,
            )

        except Exception as e:
            logger.error(f"Error in ReferentialIntegrityCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error checking referential integrity: {str(e)}",
                failed_count=1,
            )

    def _resolve_reference_path(self, reference_file: str, context: Dict[str, Any]) -> str:
        """
        Resolve reference file path (may be relative or absolute).

        Args:
            reference_file: Reference file path from config
            context: Context containing file_path or base_path

        Returns:
            Absolute path to reference file
        """
        ref_path = Path(reference_file)

        # If already absolute, return as-is
        if ref_path.is_absolute():
            return str(ref_path)

        # Try to resolve relative to current file's directory
        current_file = context.get("file_path")
        if current_file:
            base_dir = Path(current_file).parent
            resolved = base_dir / ref_path
            if resolved.exists():
                return str(resolved)

        # Try to resolve relative to base_path
        base_path = context.get("base_path")
        if base_path:
            resolved = Path(base_path) / ref_path
            if resolved.exists():
                return str(resolved)

        # Return as-is and let it fail later if not found
        return str(ref_path)

    def _load_reference_values(self, file_path: str, column: str, file_format: str) -> pd.Series:
        """
        Load reference values from file efficiently (only the required column).

        Args:
            file_path: Path to reference file
            column: Column name to load
            file_format: Format of the file

        Returns:
            Series of reference values, or None if error
        """
        try:
            # Load only the required column for memory efficiency
            if file_format.lower() == "csv":
                df = pd.read_csv(file_path, usecols=[column])
            elif file_format.lower() in ["parquet", "pq"]:
                df = pd.read_parquet(file_path, columns=[column])
            elif file_format.lower() in ["excel", "xlsx", "xls"]:
                df = pd.read_excel(file_path, usecols=[column])
            elif file_format.lower() == "json":
                df = pd.read_json(file_path)
                if column not in df.columns:
                    logger.error(f"Column '{column}' not found in reference file")
                    return None
                df = df[[column]]
            else:
                logger.error(f"Unsupported reference file format: {file_format}")
                return None

            # Get unique values (drop nulls for reference)
            values = df[column].dropna().unique()
            return pd.Series(values)

        except Exception as e:
            logger.error(f"Error loading reference values from {file_path}: {str(e)}")
            return None


class CrossFileComparisonCheck(DataValidationRule):
    """
    Compares aggregate values between two files.

    Useful for validating totals, counts, or other metrics match between related files.
    For example, ensuring total order amount in orders file matches sum in order_items file.

    Configuration:
        params:
            aggregation (str, required): Aggregation type: sum, count, mean, min, max
            column (str, optional): Column to aggregate (not needed for count)
            comparison (str, required): Comparison operator: ==, !=, >, <, >=, <=
            reference_file (str, required): Path to reference file
            reference_aggregation (str, required): Aggregation in reference file
            reference_column (str, optional): Column in reference file to aggregate
            reference_file_format (str, optional): Format of reference file. Default: csv
            tolerance (float, optional): Tolerance for numeric comparisons (absolute). Default: 0
            tolerance_pct (float, optional): Tolerance as percentage. Default: 0

    Example YAML:
        # Check total order amount matches sum of line items
        - type: "CrossFileComparisonCheck"
          severity: "ERROR"
          params:
            aggregation: "sum"
            column: "total_amount"
            comparison: "=="
            reference_file: "order_items.csv"
            reference_aggregation: "sum"
            reference_column: "item_amount"
            tolerance_pct: 0.01  # Allow 0.01% difference

        # Check customer count matches
        - type: "CrossFileComparisonCheck"
          severity: "WARNING"
          params:
            aggregation: "count"
            comparison: "=="
            reference_file: "customer_master.csv"
            reference_aggregation: "count"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        agg = self.params.get("aggregation", "?")
        col = self.params.get("column", "rows")
        comp = self.params.get("comparison", "?")
        ref_file = self.params.get("reference_file", "?")

        return f"Compares {agg}({col}) {comp} {agg} in {ref_file}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Compare aggregates between files.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Must contain 'file_path' or 'base_path' for resolving reference file

        Returns:
            ValidationResult indicating if comparison passed
        """
        try:
            # Get parameters
            aggregation = self.params.get("aggregation")
            column = self.params.get("column")
            comparison = self.params.get("comparison")
            reference_file = self.params.get("reference_file")
            reference_aggregation = self.params.get("reference_aggregation")
            reference_column = self.params.get("reference_column")
            reference_format = self.params.get("reference_file_format", "csv")
            tolerance = self.params.get("tolerance", 0)
            tolerance_pct = self.params.get("tolerance_pct", 0)

            # Validate required parameters
            if not aggregation:
                raise ParameterValidationError(
                    "Parameter 'aggregation' is required",
                    validation_name=self.name,
                    parameter="aggregation",
                    value=None
                )

            if aggregation != "count" and not column:
                raise ParameterValidationError(
                    "Parameter 'column' is required for non-count aggregations",
                    validation_name=self.name,
                    parameter="column",
                    value=None
                )

            if not comparison:
                raise ParameterValidationError(
                    "Parameter 'comparison' is required",
                    validation_name=self.name,
                    parameter="comparison",
                    value=None
                )

            if not reference_file:
                raise ParameterValidationError(
                    "Parameter 'reference_file' is required",
                    validation_name=self.name,
                    parameter="reference_file",
                    value=None
                )

            if not reference_aggregation:
                raise ParameterValidationError(
                    "Parameter 'reference_aggregation' is required",
                    validation_name=self.name,
                    parameter="reference_aggregation",
                    value=None
                )

            # Calculate current file aggregate
            current_value = self._calculate_aggregate(data_iterator, aggregation, column)

            if current_value is None:
                return self._create_result(
                    passed=False,
                    message=f"Failed to calculate {aggregation}({column})",
                    failed_count=1,
                )

            # Load and calculate reference aggregate
            reference_path = self._resolve_reference_path(reference_file, context)
            if not Path(reference_path).exists():
                return self._create_result(
                    passed=False,
                    message=f"Reference file not found: {reference_path}",
                    failed_count=1,
                )

            reference_value = self._calculate_reference_aggregate(
                reference_path,
                reference_aggregation,
                reference_column,
                reference_format
            )

            if reference_value is None:
                return self._create_result(
                    passed=False,
                    message=f"Failed to calculate reference {reference_aggregation}",
                    failed_count=1,
                )

            # Compare values
            passed, reason = self._compare_values(
                current_value,
                reference_value,
                comparison,
                tolerance,
                tolerance_pct
            )

            if passed:
                return self._create_result(
                    passed=True,
                    message=f"Comparison passed: {current_value} {comparison} {reference_value}",
                    total_count=1,
                )
            else:
                return self._create_result(
                    passed=False,
                    message=f"Comparison failed: {current_value} {comparison} {reference_value}. {reason}",
                    failed_count=1,
                    total_count=1,
                )

        except Exception as e:
            logger.error(f"Error in CrossFileComparisonCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error comparing files: {str(e)}",
                failed_count=1,
            )

    def _resolve_reference_path(self, reference_file: str, context: Dict[str, Any]) -> str:
        """Resolve reference file path (same as ReferentialIntegrityCheck)."""
        ref_path = Path(reference_file)

        if ref_path.is_absolute():
            return str(ref_path)

        current_file = context.get("file_path")
        if current_file:
            base_dir = Path(current_file).parent
            resolved = base_dir / ref_path
            if resolved.exists():
                return str(resolved)

        base_path = context.get("base_path")
        if base_path:
            resolved = Path(base_path) / ref_path
            if resolved.exists():
                return str(resolved)

        return str(ref_path)

    def _calculate_aggregate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        aggregation: str,
        column: str = None
    ) -> float:
        """
        Calculate aggregate across all chunks.

        Args:
            data_iterator: Data chunks
            aggregation: Type of aggregation
            column: Column name (not needed for count)

        Returns:
            Aggregated value
        """
        try:
            if aggregation == "count":
                total = 0
                for chunk in data_iterator:
                    total += len(chunk)
                return total

            elif aggregation == "sum":
                total = 0
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        raise ColumnNotFoundError(
                            validation_name="CrossFileComparisonCheck",
                            column=column,
                            available_columns=list(chunk.columns)
                        )
                    total += chunk[column].sum()
                return total

            elif aggregation == "mean":
                total_sum = 0
                total_count = 0
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        raise ColumnNotFoundError(
                            validation_name="CrossFileComparisonCheck",
                            column=column,
                            available_columns=list(chunk.columns)
                        )
                    valid_values = chunk[column].dropna()
                    total_sum += valid_values.sum()
                    total_count += len(valid_values)
                return total_sum / total_count if total_count > 0 else 0

            elif aggregation == "min":
                min_val = float('inf')
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        raise ColumnNotFoundError(
                            validation_name="CrossFileComparisonCheck",
                            column=column,
                            available_columns=list(chunk.columns)
                        )
                    chunk_min = chunk[column].min()
                    if pd.notna(chunk_min) and chunk_min < min_val:
                        min_val = chunk_min
                return min_val if min_val != float('inf') else None

            elif aggregation == "max":
                max_val = float('-inf')
                for chunk in data_iterator:
                    if column not in chunk.columns:
                        raise ColumnNotFoundError(
                            validation_name="CrossFileComparisonCheck",
                            column=column,
                            available_columns=list(chunk.columns)
                        )
                    chunk_max = chunk[column].max()
                    if pd.notna(chunk_max) and chunk_max > max_val:
                        max_val = chunk_max
                return max_val if max_val != float('-inf') else None

            else:
                logger.error(f"Unsupported aggregation: {aggregation}")
                return None

        except Exception as e:
            logger.error(f"Error calculating aggregate: {str(e)}")
            return None

    def _calculate_reference_aggregate(
        self,
        file_path: str,
        aggregation: str,
        column: str,
        file_format: str
    ) -> float:
        """Calculate aggregate from reference file."""
        try:
            # Load data based on format
            if file_format.lower() == "csv":
                if aggregation == "count":
                    # Optimize: just count lines
                    with open(file_path, 'r') as f:
                        return sum(1 for line in f) - 1  # Subtract header
                else:
                    df = pd.read_csv(file_path, usecols=[column])
            elif file_format.lower() in ["parquet", "pq"]:
                if aggregation == "count":
                    df = pd.read_parquet(file_path, columns=[])
                else:
                    df = pd.read_parquet(file_path, columns=[column])
            elif file_format.lower() in ["excel", "xlsx", "xls"]:
                if aggregation == "count":
                    df = pd.read_excel(file_path, usecols=[0])
                else:
                    df = pd.read_excel(file_path, usecols=[column])
            elif file_format.lower() == "json":
                df = pd.read_json(file_path)
                if aggregation != "count" and column not in df.columns:
                    logger.error(f"Column '{column}' not found in reference file")
                    return None
            else:
                logger.error(f"Unsupported reference file format: {file_format}")
                return None

            # Calculate aggregate
            if aggregation == "count":
                return len(df)
            elif aggregation == "sum":
                return df[column].sum()
            elif aggregation == "mean":
                return df[column].mean()
            elif aggregation == "min":
                return df[column].min()
            elif aggregation == "max":
                return df[column].max()
            else:
                logger.error(f"Unsupported aggregation: {aggregation}")
                return None

        except Exception as e:
            logger.error(f"Error calculating reference aggregate: {str(e)}")
            return None

    def _compare_values(
        self,
        value1: float,
        value2: float,
        comparison: str,
        tolerance: float,
        tolerance_pct: float
    ) -> tuple:
        """
        Compare two values with tolerance.

        Returns:
            (passed, reason) tuple
        """
        # Calculate effective tolerance
        if tolerance_pct > 0:
            effective_tolerance = abs(value2 * tolerance_pct / 100)
        else:
            effective_tolerance = tolerance

        # Apply comparison
        if comparison == "==":
            passed = abs(value1 - value2) <= effective_tolerance
            reason = f"Difference: {abs(value1 - value2)}, Tolerance: {effective_tolerance}"
        elif comparison == "!=":
            passed = abs(value1 - value2) > effective_tolerance
            reason = f"Difference: {abs(value1 - value2)}, Tolerance: {effective_tolerance}"
        elif comparison == ">":
            passed = value1 > value2 + effective_tolerance
            reason = f"{value1} not > {value2} (with tolerance)"
        elif comparison == "<":
            passed = value1 < value2 - effective_tolerance
            reason = f"{value1} not < {value2} (with tolerance)"
        elif comparison == ">=":
            passed = value1 >= value2 - effective_tolerance
            reason = f"{value1} not >= {value2} (with tolerance)"
        elif comparison == "<=":
            passed = value1 <= value2 + effective_tolerance
            reason = f"{value1} not <= {value2} (with tolerance)"
        else:
            passed = False
            reason = f"Invalid comparison operator: {comparison}"

        return passed, reason


class CrossFileDuplicateCheck(DataValidationRule):
    """
    Detects duplicate records across multiple files.

    Useful for ensuring no duplicate keys exist when merging datasets from multiple sources.

    Configuration:
        params:
            columns (list, required): Columns to check for duplicates
            reference_files (list, required): List of files to check against
            reference_file_format (str, optional): Format of reference files. Default: csv

    Example YAML:
        # Check for duplicate customer IDs across files
        - type: "CrossFileDuplicateCheck"
          severity: "ERROR"
          params:
            columns: ["customer_id"]
            reference_files:
              - "customers_archive.csv"
              - "customers_old.csv"

        # Check for composite key duplicates
        - type: "CrossFileDuplicateCheck"
          severity: "WARNING"
          params:
            columns: ["order_id", "line_number"]
            reference_files: ["orders_history.parquet"]
            reference_file_format: "parquet"
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        columns = self.params.get("columns", [])
        ref_files = self.params.get("reference_files", [])

        return f"Checks for duplicate {columns} across {len(ref_files)} reference files"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check for duplicates across files.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Must contain 'file_path' or 'base_path' for resolving reference files

        Returns:
            ValidationResult indicating duplicate violations
        """
        try:
            # Get parameters
            columns = self.params.get("columns", [])
            reference_files = self.params.get("reference_files", [])
            reference_format = self.params.get("reference_file_format", "csv")

            # Validate parameters
            if not columns:
                raise ParameterValidationError(
                    "Parameter 'columns' is required",
                    validation_name=self.name,
                    parameter="columns",
                    value=None
                )

            if not isinstance(columns, list):
                columns = [columns]

            if not reference_files:
                raise ParameterValidationError(
                    "Parameter 'reference_files' is required",
                    validation_name=self.name,
                    parameter="reference_files",
                    value=None
                )

            if not isinstance(reference_files, list):
                reference_files = [reference_files]

            # Load all reference values
            reference_values = set()
            for ref_file in reference_files:
                ref_path = self._resolve_reference_path(ref_file, context)
                if not Path(ref_path).exists():
                    logger.warning(f"Reference file not found: {ref_path}")
                    continue

                ref_vals = self._load_reference_key_values(ref_path, columns, reference_format)
                if ref_vals is not None:
                    reference_values.update(ref_vals)

            if not reference_values:
                return self._create_result(
                    passed=False,
                    message="No reference values loaded from reference files",
                    failed_count=1,
                )

            # Check for duplicates across chunks
            total_checked = 0
            total_duplicates = 0
            sample_duplicates = []
            max_samples = 10

            for chunk in data_iterator:
                # Check columns exist
                missing_cols = [col for col in columns if col not in chunk.columns]
                if missing_cols:
                    return self._create_result(
                        passed=False,
                        message=f"Columns not found in data: {missing_cols}",
                        failed_count=1,
                    )

                # Apply conditional filter if specified
                if self.condition:
                    mask = self._evaluate_condition(chunk)
                    chunk_to_check = chunk[mask]
                else:
                    chunk_to_check = chunk

                # Create composite keys
                if len(columns) == 1:
                    keys = chunk_to_check[columns[0]].astype(str)
                else:
                    keys = chunk_to_check[columns].astype(str).agg('|'.join, axis=1)

                # Check for duplicates
                duplicate_mask = keys.isin(reference_values)
                if duplicate_mask.any():
                    duplicates = chunk_to_check[duplicate_mask]
                    total_duplicates += len(duplicates)

                    # Collect samples
                    if len(sample_duplicates) < max_samples:
                        for idx, row in duplicates.head(max_samples - len(sample_duplicates)).iterrows():
                            key_values = {col: str(row[col]) for col in columns}
                            sample_duplicates.append({
                                "row_index": int(idx),
                                "key_columns": columns,
                                "key_values": key_values,
                                "reason": "Key found in reference files"
                            })

                total_checked += len(chunk_to_check)

            # Build result
            if total_duplicates > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {total_duplicates} duplicate keys across files",
                    failed_count=total_duplicates,
                    total_count=total_checked,
                    sample_failures=sample_duplicates,
                )

            return self._create_result(
                passed=True,
                message=f"No duplicates found across files ({total_checked} records checked)",
                total_count=total_checked,
            )

        except Exception as e:
            logger.error(f"Error in CrossFileDuplicateCheck: {str(e)}", exc_info=True)
            return self._create_result(
                passed=False,
                message=f"Error checking cross-file duplicates: {str(e)}",
                failed_count=1,
            )

    def _resolve_reference_path(self, reference_file: str, context: Dict[str, Any]) -> str:
        """Resolve reference file path."""
        ref_path = Path(reference_file)

        if ref_path.is_absolute():
            return str(ref_path)

        current_file = context.get("file_path")
        if current_file:
            base_dir = Path(current_file).parent
            resolved = base_dir / ref_path
            if resolved.exists():
                return str(resolved)

        base_path = context.get("base_path")
        if base_path:
            resolved = Path(base_path) / ref_path
            if resolved.exists():
                return str(resolved)

        return str(ref_path)

    def _load_reference_key_values(
        self,
        file_path: str,
        columns: List[str],
        file_format: str
    ) -> set:
        """Load composite key values from reference file."""
        try:
            # Load only required columns
            if file_format.lower() == "csv":
                df = pd.read_csv(file_path, usecols=columns)
            elif file_format.lower() in ["parquet", "pq"]:
                df = pd.read_parquet(file_path, columns=columns)
            elif file_format.lower() in ["excel", "xlsx", "xls"]:
                df = pd.read_excel(file_path, usecols=columns)
            elif file_format.lower() == "json":
                df = pd.read_json(file_path)
                missing_cols = [col for col in columns if col not in df.columns]
                if missing_cols:
                    logger.error(f"Columns not found in reference file: {missing_cols}")
                    return None
                df = df[columns]
            else:
                logger.error(f"Unsupported reference file format: {file_format}")
                return None

            # Create composite keys
            if len(columns) == 1:
                keys = df[columns[0]].astype(str)
            else:
                keys = df[columns].astype(str).agg('|'.join, axis=1)

            return set(keys.unique())

        except Exception as e:
            logger.error(f"Error loading reference keys from {file_path}: {str(e)}")
            return None
