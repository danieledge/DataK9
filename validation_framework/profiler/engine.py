"""
Data profiler engine for comprehensive data analysis.

Analyzes data files to understand structure, quality, patterns, and characteristics.
Generates detailed profiles with type inference, statistics, and validation suggestions.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Iterator
import re
import time
import logging

from validation_framework.profiler.profile_result import (
    ProfileResult, ColumnProfile, TypeInference, ColumnStatistics,
    QualityMetrics, CorrelationResult, ValidationSuggestion
)
from validation_framework.loaders.factory import LoaderFactory
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

logger = logging.getLogger(__name__)


class DataProfiler:
    """
    Comprehensive data profiler with type inference and quality analysis.

    Profiles data files to understand:
    - Schema and data types (inferred vs known)
    - Statistical distributions
    - Data quality metrics
    - Correlations between fields
    - Suggested validations
    - Auto-generated validation configuration
    """

    def __init__(self, chunk_size: int = 50000, max_correlation_columns: int = 20):
        """
        Initialize data profiler.

        Args:
            chunk_size: Number of rows to process per chunk
            max_correlation_columns: Maximum columns for correlation analysis
        """
        self.chunk_size = chunk_size
        self.max_correlation_columns = max_correlation_columns

    def profile_dataframe(
        self,
        df: pd.DataFrame,
        name: str = "dataframe",
        declared_schema: Optional[Dict[str, str]] = None
    ) -> ProfileResult:
        """
        Profile an in-memory DataFrame (useful for database sources).

        Args:
            df: pandas DataFrame to profile
            name: Name for the profile (e.g., table name)
            declared_schema: Optional declared schema {column: type}

        Returns:
            ProfileResult with comprehensive profile information
        """
        start_time = time.time()
        logger.info(f"Starting profile of DataFrame: {name}")

        row_count = len(df)

        # Initialize column profiles
        column_profiles: Dict[str, Dict[str, Any]] = {}
        numeric_data: Dict[str, List[float]] = {}

        for col in df.columns:
            column_profiles[col] = self._initialize_column_profile(col, declared_schema)

        # Process entire dataframe (already in memory)
        for col in df.columns:
            self._update_column_profile(column_profiles[col], df[col], 0)

            # Collect numeric data for correlations
            if column_profiles[col]["inferred_type"] in ["integer", "float"]:
                numeric_values = pd.to_numeric(df[col], errors='coerce').dropna()
                numeric_data[col] = numeric_values.tolist()

        # Finalize column profiles
        columns = []
        for col_name, profile_data in column_profiles.items():
            column_profile = self._finalize_column_profile(col_name, profile_data, row_count)
            columns.append(column_profile)

        # Calculate correlations
        correlations = self._calculate_correlations(numeric_data, row_count)

        # Generate validation suggestions
        suggested_validations = self._generate_validation_suggestions(columns, row_count)

        # Calculate overall quality score
        overall_quality = self._calculate_overall_quality(columns)

        # Generate validation configuration (for database source)
        config_yaml, config_command = self._generate_validation_config(
            name, "", "database", 0, row_count, columns, suggested_validations
        )

        processing_time = time.time() - start_time
        logger.info(f"Profile completed in {processing_time:.2f} seconds")

        return ProfileResult(
            file_name=f"{name} (DataFrame)",
            file_path="",
            file_size_bytes=0,
            format="database",
            row_count=row_count,
            column_count=len(columns),
            profiled_at=datetime.now(),
            processing_time_seconds=processing_time,
            columns=columns,
            correlations=correlations,
            suggested_validations=suggested_validations,
            overall_quality_score=overall_quality,
            generated_config_yaml=config_yaml,
            generated_config_command=config_command
        )

    def profile_file(
        self,
        file_path: str,
        file_format: str = "csv",
        declared_schema: Optional[Dict[str, str]] = None,
        **loader_kwargs
    ) -> ProfileResult:
        """
        Profile a data file comprehensively.

        Args:
            file_path: Path to file to profile
            file_format: Format (csv, excel, json, parquet)
            declared_schema: Optional declared schema {column: type}
            **loader_kwargs: Additional arguments for data loader

        Returns:
            ProfileResult with comprehensive profile information
        """
        start_time = time.time()
        logger.info(f"Starting profile of {file_path}")

        # Get file metadata
        file_path_obj = Path(file_path)
        file_size = file_path_obj.stat().st_size
        file_name = file_path_obj.name

        # Load data iterator
        loader = LoaderFactory.create_loader(
            file_format=file_format,
            file_path=file_path,
            chunk_size=self.chunk_size,
            **loader_kwargs
        )

        # Initialize accumulators
        row_count = 0
        column_profiles: Dict[str, Dict[str, Any]] = {}
        numeric_data: Dict[str, List[float]] = {}  # For correlation analysis

        # Process data in chunks
        for chunk_idx, chunk in enumerate(loader.load()):
            logger.debug(f"Processing chunk {chunk_idx}, rows: {len(chunk)}")
            row_count += len(chunk)

            # Initialize column profiles on first chunk
            if chunk_idx == 0:
                for col in chunk.columns:
                    column_profiles[col] = self._initialize_column_profile(
                        col, declared_schema
                    )

            # Update profiles with chunk data
            for col in chunk.columns:
                self._update_column_profile(
                    column_profiles[col], chunk[col], chunk_idx
                )

                # Collect numeric data for correlations
                if column_profiles[col]["inferred_type"] in ["integer", "float"]:
                    if col not in numeric_data:
                        numeric_data[col] = []
                    # Convert to numeric, handling errors
                    numeric_values = pd.to_numeric(chunk[col], errors='coerce').dropna()
                    numeric_data[col].extend(numeric_values.tolist())

        # Finalize column profiles
        columns = []
        for col_name, profile_data in column_profiles.items():
            column_profile = self._finalize_column_profile(col_name, profile_data, row_count)
            columns.append(column_profile)

        # Calculate correlations
        correlations = self._calculate_correlations(numeric_data, row_count)

        # Generate validation suggestions
        suggested_validations = self._generate_validation_suggestions(columns, row_count)

        # Calculate overall quality score
        overall_quality = self._calculate_overall_quality(columns)

        # Generate validation configuration
        config_yaml, config_command = self._generate_validation_config(
            file_name, file_path, file_format, file_size, row_count, columns, suggested_validations
        )

        processing_time = time.time() - start_time
        logger.info(f"Profile completed in {processing_time:.2f} seconds")

        return ProfileResult(
            file_name=file_name,
            file_path=file_path,
            file_size_bytes=file_size,
            format=file_format,
            row_count=row_count,
            column_count=len(columns),
            profiled_at=datetime.now(),
            processing_time_seconds=processing_time,
            columns=columns,
            correlations=correlations,
            suggested_validations=suggested_validations,
            overall_quality_score=overall_quality,
            generated_config_yaml=config_yaml,
            generated_config_command=config_command
        )

    def _initialize_column_profile(
        self,
        col_name: str,
        declared_schema: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Initialize accumulator for column profile."""
        declared_type = declared_schema.get(col_name) if declared_schema else None

        return {
            "column_name": col_name,
            "declared_type": declared_type,
            "sample_values": [],
            "type_counts": {},  # Count of each detected type
            "null_count": 0,
            "value_counts": {},  # Frequency distribution
            "numeric_values": [],  # For statistics
            "string_lengths": [],  # For string analysis
            "patterns": {},  # Pattern frequency
            "inferred_type": "unknown",
            "total_processed": 0
        }

    def _update_column_profile(
        self,
        profile: Dict[str, Any],
        series: pd.Series,
        chunk_idx: int
    ) -> None:
        """Update column profile with chunk data."""
        profile["total_processed"] += len(series)

        # Count nulls
        null_mask = series.isna()
        profile["null_count"] += null_mask.sum()

        # Process non-null values
        non_null_series = series[~null_mask]

        # Sample values (from first chunk only, limit to 100)
        if chunk_idx == 0 and len(profile["sample_values"]) < 100:
            samples = non_null_series.head(100 - len(profile["sample_values"])).tolist()
            profile["sample_values"].extend(samples)

        # Type detection
        for value in non_null_series:
            detected_type = self._detect_type(value)
            profile["type_counts"][detected_type] = profile["type_counts"].get(detected_type, 0) + 1

        # Value frequency (limit to prevent memory issues)
        if len(profile["value_counts"]) < 10000:
            value_freq = non_null_series.value_counts()
            for val, count in value_freq.items():
                profile["value_counts"][val] = profile["value_counts"].get(val, 0) + count

        # Numeric analysis
        numeric_series = pd.to_numeric(non_null_series, errors='coerce').dropna()
        if len(numeric_series) > 0:
            profile["numeric_values"].extend(numeric_series.tolist())

        # String analysis
        string_series = non_null_series.astype(str)
        lengths = string_series.str.len()
        profile["string_lengths"].extend(lengths.tolist())

        # Pattern detection (sample only)
        if chunk_idx == 0:
            for val in non_null_series.head(100):
                pattern = self._extract_pattern(str(val))
                profile["patterns"][pattern] = profile["patterns"].get(pattern, 0) + 1

    def _detect_type(self, value: Any) -> str:
        """
        Detect the type of a value.

        Returns:
            Type string: 'integer', 'float', 'boolean', 'date', 'string'
        """
        # Check for null
        if pd.isna(value):
            return 'null'

        # Boolean
        if isinstance(value, bool) or str(value).lower() in ['true', 'false', 'yes', 'no']:
            return 'boolean'

        # Try numeric
        try:
            float_val = float(value)
            if float_val.is_integer():
                return 'integer'
            return 'float'
        except (ValueError, TypeError):
            pass

        # Try date
        value_str = str(value)
        if self._is_date_like(value_str):
            return 'date'

        # Default to string
        return 'string'

    def _is_date_like(self, value: str) -> bool:
        """Check if string looks like a date."""
        # Common date patterns
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}',  # ISO date
            r'^\d{2}/\d{2}/\d{4}',  # US date
            r'^\d{2}-\d{2}-\d{4}',  # EU date
            r'^\d{4}/\d{2}/\d{2}',  # Alternative ISO
        ]

        for pattern in date_patterns:
            if re.match(pattern, value):
                return True

        return False

    def _extract_pattern(self, value: str) -> str:
        """
        Extract pattern from string value.

        Replaces:
        - Digits with '9'
        - Letters with 'A'
        - Special chars remain as-is

        Example: "ABC-123" -> "AAA-999"
        """
        if len(value) > 50:
            value = value[:50]  # Limit length

        pattern = []
        for char in value:
            if char.isdigit():
                pattern.append('9')
            elif char.isalpha():
                pattern.append('A')
            else:
                pattern.append(char)

        return ''.join(pattern)

    def _finalize_column_profile(
        self,
        col_name: str,
        profile_data: Dict[str, Any],
        total_rows: int
    ) -> ColumnProfile:
        """Finalize column profile after processing all chunks."""

        # Determine inferred type and confidence
        type_info = self._infer_type(profile_data, total_rows)

        # Calculate statistics
        statistics = self._calculate_statistics(profile_data, total_rows)

        # Calculate quality metrics
        quality = self._calculate_quality_metrics(profile_data, type_info, statistics, total_rows)

        return ColumnProfile(
            name=col_name,
            type_info=type_info,
            statistics=statistics,
            quality=quality
        )

    def _infer_type(
        self,
        profile_data: Dict[str, Any],
        total_rows: int
    ) -> TypeInference:
        """
        Infer type with confidence level.

        Confidence based on:
        - Consistency of detected types
        - Presence of declared schema
        - Percentage of values matching inferred type
        """
        declared_type = profile_data["declared_type"]
        type_counts = profile_data["type_counts"]
        null_count = profile_data["null_count"]

        # Handle empty column
        if not type_counts:
            return TypeInference(
                declared_type=declared_type,
                inferred_type="empty",
                confidence=1.0 if declared_type else 0.0,
                is_known=declared_type is not None,
                sample_values=[]
            )

        # Get most common type
        non_null_count = total_rows - null_count
        sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        primary_type, primary_count = sorted_types[0]

        # Calculate confidence
        if declared_type:
            # If schema declares type, it's known
            confidence = 1.0
            is_known = True
            inferred = declared_type
        else:
            # Confidence = percentage of values matching primary type
            confidence = primary_count / non_null_count if non_null_count > 0 else 0.0
            is_known = False
            inferred = primary_type

        # Detect type conflicts
        conflicts = []
        for typ, count in sorted_types[1:4]:  # Top 3 conflicts
            if count > non_null_count * 0.01:  # At least 1% of data
                conflicts.append({
                    "type": typ,
                    "count": count,
                    "percentage": round(100 * count / non_null_count, 2)
                })

        return TypeInference(
            declared_type=declared_type,
            inferred_type=inferred,
            confidence=confidence,
            is_known=is_known,
            type_conflicts=conflicts,
            sample_values=profile_data["sample_values"][:10]
        )

    def _calculate_statistics(
        self,
        profile_data: Dict[str, Any],
        total_rows: int
    ) -> ColumnStatistics:
        """Calculate comprehensive column statistics."""
        null_count = profile_data["null_count"]
        value_counts = profile_data["value_counts"]
        numeric_values = profile_data["numeric_values"]
        string_lengths = profile_data["string_lengths"]
        patterns = profile_data["patterns"]

        stats = ColumnStatistics()
        stats.count = total_rows
        stats.null_count = null_count
        stats.null_percentage = 100 * null_count / total_rows if total_rows > 0 else 0

        # Unique counts
        stats.unique_count = len(value_counts)
        non_null_count = total_rows - null_count
        stats.unique_percentage = 100 * stats.unique_count / non_null_count if non_null_count > 0 else 0
        stats.cardinality = stats.unique_count / non_null_count if non_null_count > 0 else 0

        # Numeric statistics
        if numeric_values:
            try:
                # Convert to float array explicitly to avoid type issues
                numeric_array = np.array(numeric_values, dtype=np.float64)
                stats.min_value = float(np.min(numeric_array))
                stats.max_value = float(np.max(numeric_array))
                stats.mean = float(np.mean(numeric_array))
                stats.median = float(np.median(numeric_array))
                stats.std_dev = float(np.std(numeric_array))

                # Quartiles
                q1, q2, q3 = np.percentile(numeric_array, [25, 50, 75])
                stats.quartiles = {
                    "Q1": round(float(q1), 3),
                    "Q2": round(float(q2), 3),
                    "Q3": round(float(q3), 3)
                }
            except (TypeError, ValueError) as e:
                logger.warning(f"Could not calculate numeric statistics: {e}")
                # Skip numeric stats if conversion fails
                pass

        # Frequency statistics
        if value_counts:
            sorted_values = sorted(value_counts.items(), key=lambda x: x[1], reverse=True)
            stats.mode = sorted_values[0][0]
            stats.mode_frequency = sorted_values[0][1]

            # Top values (top 10)
            stats.top_values = [
                {
                    "value": str(val),
                    "count": count,
                    "percentage": round(100 * count / non_null_count, 2) if non_null_count > 0 else 0
                }
                for val, count in sorted_values[:10]
            ]

        # String length statistics
        if string_lengths:
            stats.min_length = int(np.min(string_lengths))
            stats.max_length = int(np.max(string_lengths))
            stats.avg_length = float(np.mean(string_lengths))

        # Pattern samples
        if patterns:
            sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)
            stats.pattern_samples = [
                {
                    "pattern": pattern,
                    "count": count,
                    "percentage": round(100 * count / len(profile_data["sample_values"]), 2) if profile_data["sample_values"] else 0
                }
                for pattern, count in sorted_patterns[:10]
            ]

        return stats

    def _calculate_quality_metrics(
        self,
        profile_data: Dict[str, Any],
        type_info: TypeInference,
        statistics: ColumnStatistics,
        total_rows: int
    ) -> QualityMetrics:
        """Calculate data quality metrics."""
        quality = QualityMetrics()
        issues = []

        # Completeness: % of non-null values
        quality.completeness = 100 - statistics.null_percentage

        if quality.completeness < 50:
            issues.append(f"Low completeness: {quality.completeness:.1f}% non-null")
        elif quality.completeness < 90:
            issues.append(f"Moderate completeness: {quality.completeness:.1f}% non-null")

        # Validity: % matching inferred type
        quality.validity = type_info.confidence * 100

        if quality.validity < 95:
            issues.append(f"Type inconsistency: {quality.validity:.1f}% match inferred type")

        # Uniqueness: cardinality
        quality.uniqueness = statistics.cardinality * 100

        if statistics.cardinality == 1.0 and total_rows > 1:
            issues.append("All values are unique (potential key field)")
        elif statistics.cardinality < 0.01 and total_rows > 100:
            issues.append(f"Very low cardinality: {statistics.unique_count} unique values")

        # Consistency: pattern matching
        if statistics.pattern_samples:
            # If top pattern covers >80% of data, high consistency
            top_pattern_pct = statistics.pattern_samples[0]["percentage"]
            quality.consistency = top_pattern_pct

            if quality.consistency < 50:
                issues.append(f"Low consistency: {len(statistics.pattern_samples)} different patterns")
        else:
            quality.consistency = 100.0

        # Overall score (weighted average)
        quality.overall_score = (
            0.3 * quality.completeness +
            0.3 * quality.validity +
            0.2 * quality.uniqueness +
            0.2 * quality.consistency
        )

        quality.issues = issues

        return quality

    def _calculate_correlations(
        self,
        numeric_data: Dict[str, List[float]],
        row_count: int
    ) -> List[CorrelationResult]:
        """Calculate correlations between numeric columns."""
        correlations = []

        # Limit columns for performance
        numeric_columns = list(numeric_data.keys())[:self.max_correlation_columns]

        if len(numeric_columns) < 2:
            return correlations

        try:
            # Create DataFrame for correlation
            df_dict = {}
            for col in numeric_columns:
                # Ensure same length by padding/truncating
                values = numeric_data[col][:row_count]
                if len(values) < row_count:
                    values.extend([np.nan] * (row_count - len(values)))
                df_dict[col] = values

            df = pd.DataFrame(df_dict)

            # Calculate correlation matrix
            corr_matrix = df.corr()

            # Extract significant correlations
            for i, col1 in enumerate(numeric_columns):
                for j, col2 in enumerate(numeric_columns):
                    if i < j:  # Upper triangle only
                        corr_value = corr_matrix.loc[col1, col2]
                        # Include if correlation is significant (>0.5 or <-0.5)
                        if abs(corr_value) > 0.5 and not np.isnan(corr_value):
                            correlations.append(
                                CorrelationResult(
                                    column1=col1,
                                    column2=col2,
                                    correlation=float(corr_value),
                                    type="pearson"
                                )
                            )

        except Exception as e:
            logger.warning(f"Correlation calculation failed: {e}")

        return sorted(correlations, key=lambda x: abs(x.correlation), reverse=True)

    def _generate_validation_suggestions(
        self,
        columns: List[ColumnProfile],
        row_count: int
    ) -> List[ValidationSuggestion]:
        """Generate validation suggestions based on profile."""
        suggestions = []

        # File-level suggestions
        if row_count > 0:
            suggestions.append(ValidationSuggestion(
                validation_type="EmptyFileCheck",
                severity="ERROR",
                params={},
                reason="Prevent empty file loads",
                confidence=100.0
            ))

            suggestions.append(ValidationSuggestion(
                validation_type="RowCountRangeCheck",
                severity="WARNING",
                params={
                    "min_rows": max(1, int(row_count * 0.5)),
                    "max_rows": int(row_count * 2)
                },
                reason=f"Expect approximately {row_count} rows based on profile",
                confidence=80.0
            ))

        # Column-level suggestions
        mandatory_fields = []
        for col in columns:
            # Mandatory field check for high completeness
            if col.quality.completeness > 95:
                mandatory_fields.append(col.name)

            # Range check for numeric fields
            if col.type_info.inferred_type in ["integer", "float"]:
                if col.statistics.min_value is not None and col.statistics.max_value is not None:
                    suggestions.append(ValidationSuggestion(
                        validation_type="RangeCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "min_value": col.statistics.min_value,
                            "max_value": col.statistics.max_value
                        },
                        reason=f"Values range from {col.statistics.min_value} to {col.statistics.max_value}",
                        confidence=90.0
                    ))

            # Valid values for low cardinality
            if col.statistics.cardinality < 0.05 and col.statistics.unique_count < 20:
                valid_values = [item["value"] for item in col.statistics.top_values]
                suggestions.append(ValidationSuggestion(
                    validation_type="ValidValuesCheck",
                    severity="ERROR",
                    params={
                        "field": col.name,
                        "valid_values": valid_values
                    },
                    reason=f"Low cardinality field with {col.statistics.unique_count} unique values",
                    confidence=85.0
                ))

            # Unique key check for high cardinality
            if col.statistics.cardinality > 0.99 and row_count > 100:
                suggestions.append(ValidationSuggestion(
                    validation_type="UniqueKeyCheck",
                    severity="ERROR",
                    params={
                        "fields": [col.name]
                    },
                    reason="Field appears to be a unique identifier",
                    confidence=95.0
                ))

            # Date format check
            if col.type_info.inferred_type == "date":
                # Try to infer date format from samples
                date_format = self._infer_date_format(col.type_info.sample_values)
                if date_format:
                    suggestions.append(ValidationSuggestion(
                        validation_type="DateFormatCheck",
                        severity="ERROR",
                        params={
                            "field": col.name,
                            "format": date_format
                        },
                        reason=f"Detected date format: {date_format}",
                        confidence=80.0
                    ))

        # Add mandatory field check if any mandatory fields found
        if mandatory_fields:
            suggestions.append(ValidationSuggestion(
                validation_type="MandatoryFieldCheck",
                severity="ERROR",
                params={
                    "fields": mandatory_fields
                },
                reason=f"{len(mandatory_fields)} fields have >95% completeness",
                confidence=95.0
            ))

        return sorted(suggestions, key=lambda x: x.confidence, reverse=True)

    def _infer_date_format(self, sample_values: List[Any]) -> Optional[str]:
        """Infer date format from sample values."""
        if not sample_values:
            return None

        # Common date formats
        formats = [
            ("%Y-%m-%d", r'^\d{4}-\d{2}-\d{2}$'),
            ("%d/%m/%Y", r'^\d{2}/\d{2}/\d{4}$'),
            ("%m/%d/%Y", r'^\d{2}/\d{2}/\d{4}$'),
            ("%Y/%m/%d", r'^\d{4}/\d{2}/\d{2}$'),
            ("%d-%m-%Y", r'^\d{2}-\d{2}-\d{4}$'),
        ]

        for date_format, pattern in formats:
            matches = sum(1 for val in sample_values if re.match(pattern, str(val)))
            if matches > len(sample_values) * 0.8:  # 80% match
                return date_format

        return None

    def _calculate_overall_quality(self, columns: List[ColumnProfile]) -> float:
        """Calculate overall data quality score."""
        if not columns:
            return 0.0

        # Average of all column quality scores
        total_score = sum(col.quality.overall_score for col in columns)
        return total_score / len(columns)

    def _generate_validation_config(
        self,
        file_name: str,
        file_path: str,
        file_format: str,
        file_size: int,
        row_count: int,
        columns: List[ColumnProfile],
        suggestions: List[ValidationSuggestion]
    ) -> tuple[str, str]:
        """Generate validation configuration YAML and CLI command."""

        # Calculate intelligent chunk size using ChunkSizeCalculator
        calculator = ChunkSizeCalculator()
        num_validations = len(suggestions[:15])  # Count suggested validations

        # Determine validation complexity based on suggestion types
        complexity = 'simple'
        for suggestion in suggestions:
            if 'Distribution' in suggestion.validation_type or 'Correlation' in suggestion.validation_type:
                complexity = 'heavy'
                break
            elif 'Duplicate' in suggestion.validation_type or 'Unique' in suggestion.validation_type:
                complexity = 'complex'
            elif complexity == 'simple' and ('Outlier' in suggestion.validation_type or 'Pattern' in suggestion.validation_type):
                complexity = 'moderate'

        # Calculate optimal chunk size
        result = calculator.calculate_optimal_chunk_size(
            file_path=file_path,
            file_format=file_format,
            num_validations=num_validations,
            validation_complexity=complexity
        )

        optimal_chunk_size = result['recommended_chunk_size']

        # Build YAML configuration
        yaml_lines = [
            "# Auto-generated validation configuration",
            f"# Generated from profile of: {file_name}",
            f"# Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"# File size: {file_size / (1024*1024):.1f} MB, Rows: {row_count:,}",
            f"# Recommended chunk size: {optimal_chunk_size:,} rows ({result['rationale']})",
            "",
            "validation_job:",
            f'  name: "Validation for {file_name}"',
            '  description: "Auto-generated from data profile"',
            "",
            "settings:",
            f"  chunk_size: {optimal_chunk_size}  # Optimized for {file_format} format, {num_validations} validations ({complexity} complexity)",
            "  max_sample_failures: 100",
            "",
            "files:",
            f'  - name: "{Path(file_name).stem}"',
            f'    path: "{file_path}"',
            f'    format: "{file_format}"',
            "",
            "    validations:"
        ]

        # Add suggested validations
        for suggestion in suggestions[:15]:  # Limit to top 15
            yaml_lines.append(f'      - type: "{suggestion.validation_type}"')
            yaml_lines.append(f'        severity: "{suggestion.severity}"')

            if suggestion.params:
                yaml_lines.append('        params:')
                for key, value in suggestion.params.items():
                    if isinstance(value, list):
                        yaml_lines.append(f'          {key}:')
                        for item in value:
                            if isinstance(item, str):
                                yaml_lines.append(f'            - "{item}"')
                            else:
                                yaml_lines.append(f'            - {item}')
                    elif isinstance(value, str):
                        yaml_lines.append(f'          {key}: "{value}"')
                    else:
                        yaml_lines.append(f'          {key}: {value}')

            yaml_lines.append(f'        # {suggestion.reason}')
            yaml_lines.append("")

        config_yaml = "\n".join(yaml_lines)

        # Generate CLI command
        config_filename = f"{Path(file_name).stem}_validation.yaml"
        command = f"python3 -m validation_framework.cli validate {config_filename} --html report.html"

        return config_yaml, command
