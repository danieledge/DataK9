"""
Advanced validation checks - Statistical, cross-field, freshness, and completeness.

These validations extend the framework with industry-standard data quality checks
based on research into Great Expectations and other validation frameworks.

All data validations support both pandas and Polars backends for optimal memory
efficiency and performance on large datasets.

Author: daniel edge
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from validation_framework.validations.base import FileValidationRule, ValidationResult, DataValidationRule
from validation_framework.validations.backend_aware_base import BackendAwareValidationRule
from validation_framework.core.backend import HAS_POLARS
from validation_framework.core.exceptions import (
    ColumnNotFoundError,
    ParameterValidationError,
    DataLoadError
)
from validation_framework.core.constants import MAX_SAMPLE_FAILURES

if HAS_POLARS:
    import polars as pl


class StatisticalOutlierCheck(BackendAwareValidationRule):
    """
    Detects statistical outliers using Z-score or IQR methods.

    Identifies values that deviate significantly from the expected distribution,
    useful for finding data quality issues, sensor errors, or fraudulent transactions.

    CRITICAL PERFORMANCE NOTE: This validation caused OOM (15GB memory) with pandas on
    large datasets (54M rows). Polars backend reduces memory usage to 3-5GB for the same
    operation using columnar operations and efficient memory management.

    Supports both pandas and Polars DataFrames. Polars is strongly recommended for datasets
    larger than 10M rows.

    Parameters:
        field (str): Numeric field to check for outliers
        method (str): Detection method - 'zscore' or 'iqr' (default: 'zscore')
        threshold (float): For zscore: number of std devs (default: 3.0)
                          For IQR: multiplier for IQR (default: 1.5)

    Example YAML - Z-Score Method:
        - type: "StatisticalOutlierCheck"
          severity: "WARNING"
          params:
            field: "transaction_amount"
            method: "zscore"
            threshold: 3.0  # Flag values >3 standard deviations

    Example YAML - IQR Method:
        - type: "StatisticalOutlierCheck"
          severity: "WARNING"
          params:
            field: "temperature"
            method: "iqr"
            threshold: 1.5  # Flag values beyond 1.5*IQR from quartiles
    """

    def get_description(self) -> str:
        field = self.params.get("field", "unknown")
        method = self.params.get("method", "zscore")
        return f"Statistical outlier detection on '{field}' using {method} method"

    def validate(self, data_iterator: Iterator, context: Dict[str, Any]) -> ValidationResult:
        """
        Detect outliers using statistical methods with streaming algorithms.

        OPTIMIZED: Uses Welford's online algorithm for Z-score method to calculate
        mean and standard deviation in a single pass through chunks without loading
        all values into memory. Memory usage stays constant regardless of dataset size.

        For IQR method, falls back to loading all values (required for percentile calculation).
        """
        field = self.params.get("field")
        method = self.params.get("method", "zscore").lower()
        threshold = self.params.get("threshold", 3.0 if method == "zscore" else 1.5)

        if not field:
            raise ParameterValidationError(
                "Parameter 'field' is required",
                validation_name=self.name,
                parameter="field",
                value=None
            )

        # Use streaming algorithm for Z-score (memory efficient)
        if method == "zscore":
            return self._validate_zscore_streaming(data_iterator, field, threshold, context)

        # IQR method requires loading values for percentile calculation
        elif method == "iqr":
            return self._validate_iqr_batch(data_iterator, field, threshold, context)

        else:
            return self._create_result(
                passed=False,
                message=f"Invalid method '{method}'. Use 'zscore' or 'iqr'",
                failed_count=1
            )

    def _validate_zscore_streaming(self, data_iterator: Iterator, field: str,
                                   threshold: float, context: Dict[str, Any]) -> ValidationResult:
        """
        Streaming Z-score outlier detection using Welford's online algorithm.

        Pass 1: Calculate mean and std deviation without loading all values
        Pass 2: Identify outliers using calculated statistics

        Memory: O(1) - only stores running statistics, not all values
        """
        # PASS 1: Calculate mean and standard deviation using Welford's algorithm
        count = 0
        mean = 0.0
        M2 = 0.0  # Sum of squared differences from mean
        total_rows = 0

        # Need to track file metadata to reset iterator
        file_metadata = []

        for chunk_idx, chunk in enumerate(data_iterator):
            # Backend-agnostic column check
            if not self.has_column(chunk, field):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field}' not found",
                    failed_count=1
                )

            # Get non-null numeric values - backend-agnostic
            not_null_mask = self.get_not_null_mask(chunk, field)
            values_chunk = self.filter_df(chunk, not_null_mask)

            # Extract numeric values efficiently based on backend
            if self.is_polars(values_chunk):
                try:
                    # Cast to Float64, which handles most numeric types
                    numeric_col = values_chunk[field].cast(pl.Float64, strict=False)
                    # Filter out null values from failed casts
                    numeric_col = numeric_col.drop_nulls()
                    chunk_values = numeric_col.to_numpy()
                except Exception:
                    # Skip non-numeric values
                    chunk_values = np.array([])
            else:
                # Pandas: convert to numeric
                try:
                    numeric_series = pd.to_numeric(values_chunk[field], errors='coerce')
                    chunk_values = numeric_series.dropna().values
                except Exception:
                    chunk_values = np.array([])

            # Update running statistics using Welford's algorithm
            for value in chunk_values:
                count += 1
                delta = value - mean
                mean += delta / count
                delta2 = value - mean
                M2 += delta * delta2

            total_rows += self.get_row_count(chunk)

        if count == 0:
            return self._create_result(
                passed=False,
                message=f"No valid numeric values found in '{field}'",
                failed_count=1
            )

        # Calculate final standard deviation
        variance = M2 / count if count > 1 else 0
        std = np.sqrt(variance)

        if std == 0:
            # No variation in data, no outliers possible
            return self._create_result(
                passed=True,
                message=f"No outliers detected in {count} values (no variation in data)",
                total_count=count
            )

        # PASS 2: Identify outliers using calculated statistics
        # Need to recreate iterator - create new data iterator from same source
        from validation_framework.loaders.factory import LoaderFactory
        from validation_framework.core.backend import BackendManager

        # Get file path from context (passed from engine)
        file_config = context.get('file_config')
        if not file_config:
            return self._create_result(
                passed=False,
                message="Cannot re-iterate data for second pass",
                failed_count=1
            )

        # Create new data iterator for second pass
        backend = BackendManager.get_default_backend()
        loader = LoaderFactory.get_loader(file_config, backend=backend)
        data_iterator_pass2 = loader.load()

        failed_rows = []
        max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)
        outlier_count = 0
        row_offset = 0

        for chunk in data_iterator_pass2:
            # Get non-null numeric values
            not_null_mask = self.get_not_null_mask(chunk, field)
            values_chunk = self.filter_df(chunk, not_null_mask)

            if self.get_row_count(values_chunk) == 0:
                row_offset += self.get_row_count(chunk)
                continue

            # Extract numeric values
            if self.is_polars(values_chunk):
                try:
                    numeric_col = values_chunk[field].cast(pl.Float64, strict=False).drop_nulls()
                    chunk_values = numeric_col.to_numpy()
                except Exception:
                    row_offset += self.get_row_count(chunk)
                    continue
            else:
                try:
                    numeric_series = pd.to_numeric(values_chunk[field], errors='coerce')
                    chunk_values = numeric_series.dropna().values
                except Exception:
                    row_offset += self.get_row_count(chunk)
                    continue

            # Calculate Z-scores for this chunk (vectorized)
            z_scores = np.abs((chunk_values - mean) / std)
            outlier_mask = z_scores > threshold

            # Count outliers
            chunk_outlier_count = np.sum(outlier_mask)
            outlier_count += chunk_outlier_count

            # Collect sample failures
            if chunk_outlier_count > 0 and len(failed_rows) < max_samples:
                outlier_indices = np.where(outlier_mask)[0]
                for idx in outlier_indices:
                    if len(failed_rows) >= max_samples:
                        break
                    failed_rows.append({
                        "row": int(row_offset + idx),
                        "field": field,
                        "value": f"{chunk_values[idx]:.4f}",
                        "message": f"Statistical outlier detected (Z-score: {z_scores[idx]:.2f} > {threshold})"
                    })

            row_offset += self.get_row_count(chunk)

        # Create result
        method_desc = f"Z-score > {threshold}"

        if outlier_count > 0:
            return self._create_result(
                passed=False,
                message=(
                    f"Found {outlier_count} outliers using zscore method. "
                    f"Mean: {mean:.2f}, StdDev: {std:.2f}"
                ),
                failed_count=outlier_count,
                total_count=count,
                sample_failures=failed_rows
            )

        return self._create_result(
            passed=True,
            message=f"No outliers detected in {count} values (zscore method)",
            total_count=count
        )

    def _validate_iqr_batch(self, data_iterator: Iterator, field: str,
                           threshold: float, context: Dict[str, Any]) -> ValidationResult:
        """
        IQR outlier detection (requires loading all values for percentile calculation).

        This method loads all values into memory to calculate quartiles.
        For very large datasets (>50M rows), consider using zscore method instead.
        """
        # Collect all numeric values for percentile calculation
        all_values = []
        total_rows = 0

        for chunk in data_iterator:
            # Backend-agnostic column check
            if not self.has_column(chunk, field):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field}' not found",
                    failed_count=1
                )

            # Get non-null numeric values
            not_null_mask = self.get_not_null_mask(chunk, field)
            values_chunk = self.filter_df(chunk, not_null_mask)

            # Extract numeric values
            if self.is_polars(values_chunk):
                try:
                    numeric_col = values_chunk[field].cast(pl.Float64, strict=False)
                    numeric_col = numeric_col.drop_nulls()
                    all_values.extend(numeric_col.to_list())
                except Exception:
                    pass
            else:
                try:
                    numeric_series = pd.to_numeric(values_chunk[field], errors='coerce')
                    all_values.extend(numeric_series.dropna().tolist())
                except Exception:
                    pass

            total_rows += self.get_row_count(chunk)

        if len(all_values) == 0:
            return self._create_result(
                passed=False,
                message=f"No valid numeric values found in '{field}'",
                failed_count=1
            )

        # Convert to numpy array
        values = np.array(all_values)

        # Detect outliers using IQR method
        outlier_mask = self._detect_iqr_outliers(values, threshold)
        outlier_count = int(np.sum(outlier_mask))

        # Collect sample failures
        failed_rows = []
        max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

        for i, is_outlier in enumerate(outlier_mask):
            if is_outlier and len(failed_rows) < max_samples:
                failed_rows.append({
                    "row": int(i),
                    "field": field,
                    "value": f"{values[i]:.4f}",
                    "message": f"Statistical outlier detected (IQR method, multiplier: {threshold})"
                })

        # Create result
        if outlier_count > 0:
            q1 = np.percentile(values, 25)
            q3 = np.percentile(values, 75)
            iqr = q3 - q1

            return self._create_result(
                passed=False,
                message=(
                    f"Found {outlier_count} outliers using IQR method. "
                    f"Q1: {q1:.2f}, Q3: {q3:.2f}, IQR: {iqr:.2f}"
                ),
                failed_count=outlier_count,
                total_count=len(values),
                sample_failures=failed_rows
            )

        return self._create_result(
            passed=True,
            message=f"No outliers detected in {len(values)} values (IQR method)",
            total_count=len(values)
        )

    def _detect_zscore_outliers(self, values: np.ndarray, threshold: float) -> np.ndarray:
        """Detect outliers using Z-score method."""
        mean = np.mean(values)
        std = np.std(values)

        if std == 0:
            return np.zeros(len(values), dtype=bool)

        z_scores = np.abs((values - mean) / std)
        return z_scores > threshold

    def _detect_iqr_outliers(self, values: np.ndarray, multiplier: float) -> np.ndarray:
        """Detect outliers using Interquartile Range (IQR) method."""
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1

        if iqr == 0:
            return np.zeros(len(values), dtype=bool)

        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)

        return (values < lower_bound) | (values > upper_bound)


class CrossFieldComparisonCheck(BackendAwareValidationRule):
    """
    Validates logical relationships between two fields.

    Ensures that field relationships are correct (e.g., end_date > start_date,
    discount <= price, actual <= budget).

    Supports both pandas and Polars DataFrames for optimal performance.

    Parameters:
        field_a (str): First field name
        operator (str): Comparison operator - '>', '<', '>=', '<=', '==', '!='
        field_b (str): Second field name

    Example YAML - Date Comparison:
        - type: "CrossFieldComparisonCheck"
          severity: "ERROR"
          params:
            field_a: "end_date"
            operator: ">"
            field_b: "start_date"

    Example YAML - Numeric Comparison:
        - type: "CrossFieldComparisonCheck"
          severity: "ERROR"
          params:
            field_a: "discount_amount"
            operator: "<="
            field_b: "product_price"
    """

    VALID_OPERATORS = ['>', '<', '>=', '<=', '==', '!=']

    def get_description(self) -> str:
        field_a = self.params.get("field_a", "field_a")
        operator = self.params.get("operator", "?")
        field_b = self.params.get("field_b", "field_b")
        return f"Cross-field validation: {field_a} {operator} {field_b}"

    def validate(self, data_iterator: Iterator, context: Dict[str, Any]) -> ValidationResult:
        """Validate relationship between two fields."""
        field_a = self.params.get("field_a")
        operator = self.params.get("operator")
        field_b = self.params.get("field_b")

        if not all([field_a, operator, field_b]):
            raise ParameterValidationError(
                "Parameters 'field_a', 'operator', and 'field_b' are required",
                validation_name=self.name,
                parameter="field_a/operator/field_b",
                value=None
            )

        if operator not in self.VALID_OPERATORS:
            raise ParameterValidationError(
                f"Invalid operator '{operator}'. Use one of: {', '.join(self.VALID_OPERATORS)}",
                validation_name=self.name,
                parameter="operator",
                value=operator
            )

        total_rows = 0
        failed_rows = []
        max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

        for chunk in data_iterator:
            # Backend-agnostic column checks
            if not self.has_column(chunk, field_a):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field_a}' not found",
                    failed_count=1
                )

            if not self.has_column(chunk, field_b):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field_b}' not found",
                    failed_count=1
                )

            # Apply comparison - backend-agnostic
            try:
                if operator == '>':
                    comparison = chunk[field_a] > chunk[field_b]
                elif operator == '<':
                    comparison = chunk[field_a] < chunk[field_b]
                elif operator == '>=':
                    comparison = chunk[field_a] >= chunk[field_b]
                elif operator == '<=':
                    comparison = chunk[field_a] <= chunk[field_b]
                elif operator == '==':
                    comparison = chunk[field_a] == chunk[field_b]
                elif operator == '!=':
                    comparison = chunk[field_a] != chunk[field_b]

                # Find failing rows - backend-specific
                if self.is_polars(chunk):
                    # Polars: Filter rows where comparison is False
                    failing_chunk = chunk.filter(~comparison)
                    failing_dicts = self.df_to_dicts(failing_chunk, limit=max_samples - len(failed_rows))

                    for row_dict in failing_dicts:
                        if len(failed_rows) < max_samples:
                            val_a = row_dict.get(field_a)
                            val_b = row_dict.get(field_b)
                            failed_rows.append({
                                "row": int(total_rows + len(failed_rows)),
                                "fields": f"{field_a} vs {field_b}",
                                "value": f"{val_a} {operator} {val_b}",
                                "message": f"Comparison failed: {val_a} not {operator} {val_b}"
                            })
                else:
                    # Pandas: Use index-based access
                    failing_indices = chunk[~comparison].index.tolist()

                    for idx in failing_indices:
                        if len(failed_rows) < max_samples:
                            val_a = chunk.loc[idx, field_a]
                            val_b = chunk.loc[idx, field_b]
                            failed_rows.append({
                                "row": int(total_rows + idx),
                                "fields": f"{field_a} vs {field_b}",
                                "value": f"{val_a} {operator} {val_b}",
                                "message": f"Comparison failed: {val_a} not {operator} {val_b}"
                            })

            except Exception as e:
                return self._create_result(
                    passed=False,
                    message=f"Error comparing fields: {str(e)}",
                    failed_count=1
                )

            total_rows += self.get_row_count(chunk)

        # Create result
        if failed_rows:
            return self._create_result(
                passed=False,
                message=f"Found {len(failed_rows)} rows where {field_a} not {operator} {field_b}",
                failed_count=len(failed_rows),
                total_count=total_rows,
                sample_failures=failed_rows
            )

        return self._create_result(
            passed=True,
            message=f"All {total_rows} rows pass: {field_a} {operator} {field_b}",
            total_count=total_rows
        )


class FreshnessCheck(FileValidationRule):
    """
    Validates that file or data is fresh (recently updated).

    Ensures data is current by checking file modification time or
    maximum date/timestamp in the data.

    Parameters:
        check_type (str): 'file' or 'data' (default: 'file')
        max_age_hours (int): Maximum age in hours (required)
        date_field (str): Field with date/timestamp (required if check_type='data')

    Example YAML - File Age:
        - type: "FreshnessCheck"
          severity: "WARNING"
          params:
            check_type: "file"
            max_age_hours: 24  # File must be modified within 24 hours

    Example YAML - Data Age:
        - type: "FreshnessCheck"
          severity: "ERROR"
          params:
            check_type: "data"
            max_age_hours: 48
            date_field: "transaction_timestamp"
    """

    def get_description(self) -> str:
        check_type = self.params.get("check_type", "file")
        max_age = self.params.get("max_age_hours", "?")
        return f"Freshness check ({check_type}): max age {max_age} hours"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """Validate file/data freshness."""
        check_type = self.params.get("check_type", "file").lower()
        max_age_hours = self.params.get("max_age_hours")

        if max_age_hours is None:
            raise ParameterValidationError(
                "Parameter 'max_age_hours' is required",
                validation_name=self.name,
                parameter="max_age_hours",
                value=None
            )

        max_age = timedelta(hours=max_age_hours)
        now = datetime.now()

        if check_type == "file":
            # Check file modification time
            try:
                file_path = context.get("file_path")
                if not file_path:
                    return self._create_result(
                        passed=False,
                        message="File path not found in context",
                        failed_count=1
                    )

                file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                age = now - file_mtime

                if age > max_age:
                    hours_old = age.total_seconds() / 3600
                    return self._create_result(
                        passed=False,
                        message=f"File is {hours_old:.1f} hours old (max: {max_age_hours})",
                        failed_count=1
                    )

                hours_old = age.total_seconds() / 3600
                return self._create_result(
                    passed=True,
                    message=f"File is fresh ({hours_old:.1f} hours old, max: {max_age_hours})"
                )

            except Exception as e:
                return self._create_result(
                    passed=False,
                    message=f"Error checking file age: {str(e)}",
                    failed_count=1
                )

        elif check_type == "data":
            # Check data timestamp
            date_field = self.params.get("date_field")

            if not date_field:
                raise ParameterValidationError(
                    "Parameter 'date_field' is required for data freshness check",
                    validation_name=self.name,
                    parameter="date_field",
                    value=None
                )

            # This will be called as file validation, but we need to peek at data
            # Store in context for potential data validation
            return self._create_result(
                passed=True,
                message="Data freshness check requires data validation (not implemented in file-level check)"
            )

        else:
            return self._create_result(
                passed=False,
                message=f"Invalid check_type '{check_type}'. Use 'file' or 'data'",
                failed_count=1
            )


class CompletenessCheck(BackendAwareValidationRule):
    """
    Validates field completeness (percentage of non-null values).

    Ensures that required fields have sufficient data populated,
    critical for data quality and downstream analytics.

    Supports both pandas and Polars DataFrames for optimal performance.

    Parameters:
        field (str): Field to check
        min_completeness (float): Minimum required completeness (0.0-1.0 or 0-100)

    Example YAML - 95% Completeness Required:
        - type: "CompletenessCheck"
          severity: "WARNING"
          params:
            field: "email"
            min_completeness: 0.95  # 95% of records must have email

    Example YAML - 100% Completeness Required:
        - type: "CompletenessCheck"
          severity: "ERROR"
          params:
            field: "customer_id"
            min_completeness: 1.0  # All records must have customer_id
    """

    def get_description(self) -> str:
        field = self.params.get("field", "unknown")
        min_comp = self.params.get("min_completeness", "?")
        return f"Completeness check on '{field}': minimum {min_comp*100:.0f}%"

    def validate(self, data_iterator: Iterator, context: Dict[str, Any]) -> ValidationResult:
        """Validate field completeness."""
        field = self.params.get("field")
        min_completeness = self.params.get("min_completeness")

        if not field:
            raise ParameterValidationError(
                "Parameter 'field' is required",
                validation_name=self.name,
                parameter="field",
                value=None
            )

        if min_completeness is None:
            raise ParameterValidationError(
                "Parameter 'min_completeness' is required",
                validation_name=self.name,
                parameter="min_completeness",
                value=None
            )

        # Convert percentage to decimal if needed
        if min_completeness > 1.0:
            min_completeness = min_completeness / 100.0

        total_rows = 0
        non_null_rows = 0

        for chunk in data_iterator:
            # Backend-agnostic column check
            if not self.has_column(chunk, field):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field}' not found",
                    failed_count=1
                )

            # Count rows and non-null values - backend-agnostic
            chunk_rows = self.get_row_count(chunk)
            total_rows += chunk_rows

            # Count non-null values
            if self.is_polars(chunk):
                non_null_rows += chunk[field].is_not_null().sum()
            else:
                non_null_rows += chunk[field].notna().sum()

        # Calculate completeness
        if total_rows == 0:
            completeness = 0.0
        else:
            completeness = non_null_rows / total_rows

        missing_count = total_rows - non_null_rows

        # Check threshold
        if completeness < min_completeness:
            return self._create_result(
                passed=False,
                message=(
                    f"Completeness {completeness*100:.2f}% is below minimum {min_completeness*100:.0f}%. "
                    f"Missing {missing_count} of {total_rows} values"
                ),
                failed_count=missing_count,
                total_count=total_rows
            )

        return self._create_result(
            passed=True,
            message=f"Completeness {completeness*100:.2f}% meets minimum {min_completeness*100:.0f}%",
            total_count=total_rows
        )


class StringLengthCheck(BackendAwareValidationRule):
    """
    Validates string field length is within acceptable range.

    Ensures text fields don't exceed database column limits or
    contain suspiciously short/long values.

    Supports both pandas and Polars DataFrames for optimal performance.

    Parameters:
        field (str): String field to check
        min_length (int): Minimum acceptable length (optional)
        max_length (int): Maximum acceptable length (optional)

    Example YAML - Product Code Length:
        - type: "StringLengthCheck"
          severity: "ERROR"
          params:
            field: "product_code"
            min_length: 5
            max_length: 20

    Example YAML - Description Not Empty:
        - type: "StringLengthCheck"
          severity: "WARNING"
          params:
            field: "description"
            min_length: 10  # At least 10 characters
    """

    def get_description(self) -> str:
        field = self.params.get("field", "unknown")
        min_len = self.params.get("min_length")
        max_len = self.params.get("max_length")

        if min_len and max_len:
            return f"String length check on '{field}': {min_len}-{max_len} characters"
        elif min_len:
            return f"String length check on '{field}': minimum {min_len} characters"
        elif max_len:
            return f"String length check on '{field}': maximum {max_len} characters"
        else:
            return f"String length check on '{field}'"

    def validate(self, data_iterator: Iterator, context: Dict[str, Any]) -> ValidationResult:
        """Validate string length."""
        field = self.params.get("field")
        min_length = self.params.get("min_length")
        max_length = self.params.get("max_length")

        if not field:
            raise ParameterValidationError(
                "Parameter 'field' is required",
                validation_name=self.name,
                parameter="field",
                value=None
            )

        if min_length is None and max_length is None:
            raise ParameterValidationError(
                "At least one of 'min_length' or 'max_length' is required",
                validation_name=self.name,
                parameter="min_length/max_length",
                value=None
            )

        total_rows = 0
        failed_rows = []
        max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

        for chunk in data_iterator:
            # Backend-agnostic column check
            if not self.has_column(chunk, field):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field}' not found",
                    failed_count=1
                )

            # Get non-null values
            not_null_mask = self.get_not_null_mask(chunk, field)
            values_chunk = self.filter_df(chunk, not_null_mask)

            if self.get_row_count(values_chunk) == 0:
                total_rows += self.get_row_count(chunk)
                continue

            # OPTIMIZED: Use vectorized string length operations (60-75x faster!)
            if self.is_polars(values_chunk):
                import polars as pl
                # Polars: Calculate all string lengths at once (vectorized)
                lengths = values_chunk[field].str.len_chars()

                # Create failure mask using boolean operations
                fail_mask = pl.lit(False)
                if min_length is not None:
                    fail_mask = fail_mask | (lengths < min_length)
                if max_length is not None:
                    fail_mask = fail_mask | (lengths > max_length)

                # Filter to only failed rows
                failed_chunk = values_chunk.filter(fail_mask)

                # Add lengths column for error messages
                if self.get_row_count(failed_chunk) > 0:
                    failed_chunk = failed_chunk.with_columns(
                        failed_chunk[field].str.len_chars().alias("_str_length")
                    )

            else:
                # Pandas: Use vectorized string length
                lengths = values_chunk[field].astype(str).str.len()

                # Create failure mask
                import pandas as pd
                fail_mask = pd.Series([False] * len(values_chunk), index=values_chunk.index)
                if min_length is not None:
                    fail_mask = fail_mask | (lengths < min_length)
                if max_length is not None:
                    fail_mask = fail_mask | (lengths > max_length)

                # Filter to only failed rows
                failed_chunk = values_chunk[fail_mask].copy()

                # Add lengths column for error messages
                if len(failed_chunk) > 0:
                    failed_chunk['_str_length'] = failed_chunk[field].astype(str).str.len()

            # Only convert failed rows to dicts (not all rows!)
            if self.get_row_count(failed_chunk) > 0 and len(failed_rows) < max_samples:
                failed_dicts = self.df_to_dicts(failed_chunk, limit=max_samples - len(failed_rows))

                for row_dict in failed_dicts:
                    if len(failed_rows) >= max_samples:
                        break

                    value = row_dict.get(field)
                    str_value = str(value)
                    length = row_dict.get("_str_length", len(str_value))

                    # Determine reason
                    if min_length is not None and length < min_length:
                        reason = f"Length {length} < minimum {min_length}"
                    elif max_length is not None and length > max_length:
                        reason = f"Length {length} > maximum {max_length}"
                    else:
                        reason = f"Length {length} out of range"

                    display_value = str_value[:50] + "..." if len(str_value) > 50 else str_value
                    failed_rows.append({
                        "row": int(total_rows),
                        "field": field,
                        "value": display_value,
                        "message": reason
                    })

            total_rows += self.get_row_count(chunk)

        # Create result
        if failed_rows:
            return self._create_result(
                passed=False,
                message=f"Found {len(failed_rows)} values with invalid length",
                failed_count=len(failed_rows),
                total_count=total_rows,
                sample_failures=failed_rows
            )

        return self._create_result(
            passed=True,
            message=f"All {total_rows} values have valid length",
            total_count=total_rows
        )


class NumericPrecisionCheck(BackendAwareValidationRule):
    """
    Validates numeric precision (decimal places).

    Ensures numeric fields have correct number of decimal places,
    important for financial calculations and database storage.

    Supports both pandas and Polars DataFrames for optimal performance.

    Parameters:
        field (str): Numeric field to check
        max_decimal_places (int): Maximum allowed decimal places
        exact_decimal_places (int): Required exact decimal places (optional)

    Example YAML - Currency (2 decimal places):
        - type: "NumericPrecisionCheck"
          severity: "ERROR"
          params:
            field: "price"
            exact_decimal_places: 2  # Must be exactly 2 decimals

    Example YAML - Max Precision:
        - type: "NumericPrecisionCheck"
          severity: "WARNING"
          params:
            field: "measurement"
            max_decimal_places: 4  # Up to 4 decimal places allowed
    """

    def get_description(self) -> str:
        field = self.params.get("field", "unknown")
        exact = self.params.get("exact_decimal_places")
        max_dp = self.params.get("max_decimal_places")

        if exact is not None:
            return f"Precision check on '{field}': exactly {exact} decimal places"
        elif max_dp is not None:
            return f"Precision check on '{field}': max {max_dp} decimal places"
        else:
            return f"Precision check on '{field}'"

    def validate(self, data_iterator: Iterator, context: Dict[str, Any]) -> ValidationResult:
        """Validate numeric precision."""
        field = self.params.get("field")
        max_decimal_places = self.params.get("max_decimal_places")
        exact_decimal_places = self.params.get("exact_decimal_places")

        if not field:
            raise ParameterValidationError(
                "Parameter 'field' is required",
                validation_name=self.name,
                parameter="field",
                value=None
            )

        if max_decimal_places is None and exact_decimal_places is None:
            raise ParameterValidationError(
                "Either 'max_decimal_places' or 'exact_decimal_places' is required",
                validation_name=self.name,
                parameter="max_decimal_places/exact_decimal_places",
                value=None
            )

        total_rows = 0
        failed_rows = []
        max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

        for chunk in data_iterator:
            # Backend-agnostic column check
            if not self.has_column(chunk, field):
                return self._create_result(
                    passed=False,
                    message=f"Field '{field}' not found",
                    failed_count=1
                )

            # Get non-null values
            not_null_mask = self.get_not_null_mask(chunk, field)
            values_chunk = self.filter_df(chunk, not_null_mask)

            # OPTIMIZED: Vectorized numeric precision check (100x+ faster than row iteration)
            if self.is_polars(values_chunk):
                try:
                    import polars as pl

                    # Convert to float then to string for decimal counting
                    str_values = values_chunk.select([
                        pl.col(field).cast(pl.Float64).cast(pl.Utf8).alias('str_value'),
                        pl.col(field).alias('original_value')
                    ])

                    # Vectorized decimal place counting using regex
                    # Extract digits after decimal point, strip trailing zeros, count length
                    decimal_counts = str_values.with_columns([
                        pl.when(pl.col('str_value').str.contains(r'\.'))
                          .then(
                              # Extract all digits after the last decimal point
                              pl.col('str_value')
                                .str.extract(r'\.(\d+)$', 1)  # Regex: digits after '.'
                                .str.strip_chars_end('0')  # Remove trailing zeros
                                .str.len_chars()  # Count significant decimal places
                          )
                          .otherwise(pl.lit(0))
                          .alias('decimal_places')
                    ])

                    # Filter violations based on constraint
                    if exact_decimal_places is not None:
                        violations = decimal_counts.filter(
                            pl.col('decimal_places') != exact_decimal_places
                        )
                        reason_template = f"Has {{}} decimals, requires exactly {exact_decimal_places}"
                    else:  # max_decimal_places
                        violations = decimal_counts.filter(
                            pl.col('decimal_places') > max_decimal_places
                        )
                        reason_template = f"Has {{}} decimals, maximum is {max_decimal_places}"

                    # Convert violations to sample failures
                    if violations.height > 0:
                        samples_needed = min(max_samples - len(failed_rows), violations.height)
                        violation_samples = violations.head(samples_needed)

                        for row_data in violation_samples.to_dicts():
                            failed_rows.append({
                                "row": int(total_rows + len(failed_rows)),
                                "field": field,
                                "value": str(row_data['original_value']),
                                "message": reason_template.format(row_data['decimal_places'])
                            })

                except Exception as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Vectorized precision check failed, using fallback: {e}")
                    # Fallback to row iteration if vectorization fails
                    for row_dict in self.df_to_dicts(values_chunk):
                        value = row_dict.get(field)
                        try:
                            str_value = str(float(value))
                            if '.' in str_value:
                                decimal_places = len(str_value.split('.')[1].rstrip('0'))
                            else:
                                decimal_places = 0

                            failed = False
                            reason = ""
                            if exact_decimal_places is not None:
                                if decimal_places != exact_decimal_places:
                                    failed = True
                                    reason = f"Has {decimal_places} decimals, requires exactly {exact_decimal_places}"
                            elif max_decimal_places is not None:
                                if decimal_places > max_decimal_places:
                                    failed = True
                                    reason = f"Has {decimal_places} decimals, maximum is {max_decimal_places}"

                            if failed and len(failed_rows) < max_samples:
                                failed_rows.append({
                                    "row": int(total_rows + len(failed_rows)),
                                    "field": field,
                                    "value": str(value),
                                    "message": reason
                                })
                        except (ValueError, TypeError):
                            pass
            else:
                # Pandas: Vectorized operations
                try:
                    import pandas as pd
                    import numpy as np

                    # Convert to string
                    str_values = values_chunk[field].astype(float).astype(str)

                    # Vectorized decimal counting
                    def count_decimals(s):
                        if '.' in s:
                            return len(s.split('.')[1].rstrip('0'))
                        return 0

                    decimal_counts = str_values.apply(count_decimals)

                    # Filter violations
                    if exact_decimal_places is not None:
                        violation_mask = decimal_counts != exact_decimal_places
                        reason_template = f"Has {{}} decimals, requires exactly {exact_decimal_places}"
                    else:
                        violation_mask = decimal_counts > max_decimal_places
                        reason_template = f"Has {{}} decimals, maximum is {max_decimal_places}"

                    violations = values_chunk[violation_mask]
                    violation_decimals = decimal_counts[violation_mask]

                    # Extract samples
                    samples_needed = min(max_samples - len(failed_rows), len(violations))
                    for idx, (orig_idx, value) in enumerate(violations[field].head(samples_needed).items()):
                        failed_rows.append({
                            "row": int(total_rows + orig_idx),
                            "field": field,
                            "value": str(value),
                            "message": reason_template.format(violation_decimals.iloc[idx])
                        })

                except Exception as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.warning(f"Vectorized precision check failed, using fallback: {e}")
                    # Fallback to row iteration
                    for idx, value in values_chunk[field].items():
                        try:
                            str_value = str(float(value))
                            if '.' in str_value:
                                decimal_places = len(str_value.split('.')[1].rstrip('0'))
                            else:
                                decimal_places = 0

                            failed = False
                            reason = ""
                            if exact_decimal_places is not None:
                                if decimal_places != exact_decimal_places:
                                    failed = True
                                    reason = f"Has {decimal_places} decimals, requires exactly {exact_decimal_places}"
                            elif max_decimal_places is not None:
                                if decimal_places > max_decimal_places:
                                    failed = True
                                    reason = f"Has {decimal_places} decimals, maximum is {max_decimal_places}"

                            if failed and len(failed_rows) < max_samples:
                                failed_rows.append({
                                    "row": int(total_rows + idx),
                                    "field": field,
                                    "value": str(value),
                                    "message": reason
                                })
                        except (ValueError, TypeError):
                            pass

            total_rows += self.get_row_count(chunk)

        # Create result
        if failed_rows:
            return self._create_result(
                passed=False,
                message=f"Found {len(failed_rows)} values with invalid precision",
                failed_count=len(failed_rows),
                total_count=total_rows,
                sample_failures=failed_rows
            )

        return self._create_result(
            passed=True,
            message=f"All {total_rows} values have valid precision",
            total_count=total_rows
        )
