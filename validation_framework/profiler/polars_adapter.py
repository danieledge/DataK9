"""
Adapter to convert Polars ProfileResult to HTML-compatible ProfileResult.

This adapter bridges the gap between the optimized Polars profiler
(which uses a simple dict-based ProfileResult) and the HTML reporter
(which expects rich dataclass-based ProfileResult objects).

The adapter preserves the 5-10x performance advantage of Polars while
enabling full HTML report generation.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Import Polars ProfileResult (simple structure)
# We'll reference this by module to avoid naming conflicts
from validation_framework.profiler import polars_engine

# Import expected ProfileResult structure (rich dataclasses)
from validation_framework.profiler.profile_result import (
    ProfileResult,
    ColumnProfile,
    TypeInference,
    ColumnStatistics,
    QualityMetrics,
    DistributionMetrics,
    AnomalyInfo,
    PatternInfo,
    CorrelationResult,
    ValidationSuggestion,
    TemporalMetrics
)

logger = logging.getLogger(__name__)


class PolarsProfileAdapter:
    """
    Adapter to convert Polars ProfileResult to HTML-compatible ProfileResult.

    Converts the simple dict-based structure from PolarsDataProfiler to the
    rich dataclass-based structure expected by ProfileHTMLReporter.

    Features:
    - Zero-copy where possible for performance
    - Comprehensive error handling
    - Logging for debugging
    - Maintains all data from Polars profiler
    """

    @staticmethod
    def convert(polars_result: polars_engine.ProfileResult) -> ProfileResult:
        """
        Convert Polars ProfileResult to HTML-compatible ProfileResult.

        Args:
            polars_result: Simple ProfileResult from PolarsDataProfiler

        Returns:
            Rich ProfileResult compatible with ProfileHTMLReporter

        Raises:
            ValueError: If required fields are missing
            Exception: For conversion errors (logged and re-raised)
        """
        try:
            logger.info(f"Converting Polars ProfileResult for: {polars_result.file_path}")

            # Extract basic file information
            file_path = Path(polars_result.file_path)
            file_name = file_path.name
            file_size_bytes = file_path.stat().st_size if file_path.exists() else 0
            file_format = file_path.suffix.lstrip('.') or 'csv'

            # Convert column profiles
            columns = []
            for col_name, col_data in polars_result.column_profiles.items():
                try:
                    column_profile = PolarsProfileAdapter._convert_column_profile(
                        col_name, col_data
                    )
                    columns.append(column_profile)
                except Exception as e:
                    logger.warning(f"Error converting column '{col_name}': {e}")
                    # Create minimal column profile on error
                    columns.append(PolarsProfileAdapter._create_minimal_column_profile(col_name))

            # Convert correlations
            correlations = PolarsProfileAdapter._convert_correlations(
                polars_result.correlations
            )

            # Calculate overall quality score from column quality metrics
            overall_quality_score = PolarsProfileAdapter._calculate_overall_quality(columns)

            # Generate suggested validations based on profiled data
            suggested_validations = PolarsProfileAdapter._generate_suggestions(columns)

            # Generate validation config YAML
            generated_config_yaml, generated_config_command = (
                PolarsProfileAdapter._generate_validation_config(
                    file_name, str(polars_result.file_path), file_format,
                    columns, suggested_validations,
                    polars_result.row_count, file_size_bytes
                )
            )

            # Extract processing time from metadata
            processing_time = polars_result.metadata.get('profiling_time_seconds', 0.0)

            # Create the rich ProfileResult
            result = ProfileResult(
                file_name=file_name,
                file_path=str(polars_result.file_path),
                file_size_bytes=file_size_bytes,
                format=file_format,
                row_count=polars_result.row_count,
                column_count=polars_result.column_count,
                profiled_at=datetime.now(),
                processing_time_seconds=processing_time,
                columns=columns,
                correlations=correlations,
                suggested_validations=suggested_validations,
                overall_quality_score=overall_quality_score,
                generated_config_yaml=generated_config_yaml,
                generated_config_command=generated_config_command
            )

            logger.info(
                f"Conversion complete: {len(columns)} columns, "
                f"quality score {overall_quality_score:.1f}%"
            )

            return result

        except Exception as e:
            logger.error(f"Error converting Polars ProfileResult: {e}", exc_info=True)
            raise

    @staticmethod
    def _convert_column_profile(col_name: str, col_data: Dict[str, Any]) -> ColumnProfile:
        """Convert a single column profile from Polars format to rich format."""

        # Extract type information
        type_info = PolarsProfileAdapter._convert_type_info(col_data)

        # Extract statistics
        statistics = PolarsProfileAdapter._convert_statistics(col_data)

        # Calculate quality metrics
        quality = PolarsProfileAdapter._calculate_quality_metrics(col_data, statistics)

        # Extract distribution metrics (for numeric columns)
        distribution = PolarsProfileAdapter._convert_distribution(col_data)

        # Extract anomaly information
        anomalies = PolarsProfileAdapter._convert_anomalies(col_data)

        # Extract pattern information (for string columns)
        patterns = PolarsProfileAdapter._convert_patterns(col_data)

        # Create column profile
        return ColumnProfile(
            name=col_name,
            type_info=type_info,
            statistics=statistics,
            quality=quality,
            distribution=distribution,
            anomalies=anomalies,
            temporal=None,  # Polars profiler doesn't do temporal analysis yet
            patterns=patterns,
            dependencies=None  # Polars profiler doesn't track dependencies yet
        )

    @staticmethod
    def _convert_type_info(col_data: Dict[str, Any]) -> TypeInference:
        """Convert type information."""
        dtype = col_data.get('dtype', 'unknown')

        # Map Polars dtypes to standardized types
        type_map = {
            'Int64': 'integer',
            'Int32': 'integer',
            'Int16': 'integer',
            'Int8': 'integer',
            'UInt64': 'integer',
            'UInt32': 'integer',
            'UInt16': 'integer',
            'UInt8': 'integer',
            'Float64': 'float',
            'Float32': 'float',
            'Utf8': 'string',
            'String': 'string',
            'Boolean': 'boolean',
            'Date': 'date',
            'Datetime': 'datetime',
        }

        # Get base type (handle Polars type strings like "Int64" or pandas "int64")
        inferred_type = type_map.get(dtype, dtype.lower() if dtype else 'unknown')

        # Check if suggested type is available (from pattern detection)
        suggested_type = col_data.get('suggested_type')
        if suggested_type:
            inferred_type = suggested_type
            confidence = 0.9  # High confidence from pattern detection
        else:
            confidence = 1.0 if 'Int' in dtype or 'Float' in dtype else 0.8

        return TypeInference(
            declared_type=None,  # Polars profiler doesn't track declared types
            inferred_type=inferred_type,
            confidence=confidence,
            is_known=False,  # All types are inferred in Polars profiler
            type_conflicts=[],
            sample_values=[]
        )

    @staticmethod
    def _convert_statistics(col_data: Dict[str, Any]) -> ColumnStatistics:
        """Convert statistics information."""
        count = col_data.get('count', 0)
        null_count = col_data.get('null_count', 0)
        null_percentage = col_data.get('null_percentage', 0.0)
        unique_count = col_data.get('unique_count', 0)
        unique_percentage = col_data.get('unique_percentage', 0.0)

        # Cardinality (ratio of unique to total)
        cardinality = unique_count / count if count > 0 else 0.0

        # Numeric statistics
        min_value = col_data.get('min')
        max_value = col_data.get('max')
        mean = col_data.get('mean')
        median = col_data.get('median')
        std_dev = col_data.get('std')

        # Quartiles
        quartiles = None
        if 'q25' in col_data and 'q75' in col_data:
            quartiles = {
                'q1': col_data.get('q25'),
                'q2': median,
                'q3': col_data.get('q75')
            }

        # Top values
        top_values = []
        value_counts = col_data.get('value_counts', {})
        if value_counts:
            total = count - null_count
            for value, value_count in value_counts.items():
                percentage = (value_count / total * 100) if total > 0 else 0
                top_values.append({
                    'value': value,
                    'count': value_count,
                    'percentage': percentage
                })

        # String statistics
        min_length = col_data.get('min_length')
        max_length = col_data.get('max_length')
        avg_length = col_data.get('avg_length')

        return ColumnStatistics(
            count=count,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            unique_percentage=unique_percentage,
            cardinality=cardinality,
            min_value=min_value,
            max_value=max_value,
            mean=mean,
            median=median,
            std_dev=std_dev,
            quartiles=quartiles,
            mode=None,  # Not calculated by Polars profiler
            mode_frequency=None,
            top_values=top_values,
            min_length=min_length,
            max_length=max_length,
            avg_length=avg_length,
            pattern_samples=[]
        )

    @staticmethod
    def _calculate_quality_metrics(
        col_data: Dict[str, Any],
        statistics: ColumnStatistics
    ) -> QualityMetrics:
        """Calculate quality metrics for a column."""

        # Completeness: percentage of non-null values
        completeness = 100.0 - statistics.null_percentage

        # Validity: assume 100% for now (Polars profiler doesn't track invalid values)
        validity = 100.0

        # Uniqueness: based on cardinality
        uniqueness = statistics.unique_percentage

        # Consistency: based on pattern detection if available
        patterns = col_data.get('patterns', {})
        consistency = 100.0
        if patterns:
            # If we have pattern info, use pattern consistency
            pattern_matches = patterns.get('pattern_match_percentage', 100.0)
            consistency = pattern_matches

        # Overall score: weighted average
        overall_score = (
            completeness * 0.4 +  # Completeness is most important
            validity * 0.3 +
            consistency * 0.2 +
            min(uniqueness, 50) * 0.1  # Cap uniqueness contribution
        )

        # Identify issues
        issues = []
        if completeness < 95:
            issues.append(f"Missing data: {statistics.null_percentage:.1f}% null values")
        if uniqueness > 95:
            issues.append(f"High uniqueness: {uniqueness:.1f}% - possible ID field")
        if uniqueness < 5 and statistics.count > 100:
            issues.append(f"Low uniqueness: {uniqueness:.1f}% - limited distinct values")

        # Check for PII
        has_pii = col_data.get('has_pii', False)
        if has_pii:
            issues.append("Contains PII - requires special handling")

        return QualityMetrics(
            completeness=completeness,
            validity=validity,
            uniqueness=uniqueness,
            consistency=consistency,
            overall_score=overall_score,
            issues=issues
        )

    @staticmethod
    def _convert_distribution(col_data: Dict[str, Any]) -> Optional[DistributionMetrics]:
        """Convert distribution metrics for numeric columns."""

        # Only create distribution metrics for numeric columns
        if not col_data.get('min') or not col_data.get('max'):
            return None

        anomaly_data = col_data.get('anomalies', {})

        # Extract outlier information
        outlier_count = anomaly_data.get('outlier_count', 0)
        outlier_percentage = anomaly_data.get('outlier_percentage', 0.0)
        outliers_iqr = anomaly_data.get('outliers_iqr', [])[:10]  # Limit to 10 samples
        outliers_zscore = anomaly_data.get('outliers_zscore', [])[:10]

        # Distribution type from anomaly detection
        distribution_type = anomaly_data.get('distribution_type', 'unknown')

        return DistributionMetrics(
            distribution_type=distribution_type,
            skewness=None,  # Not calculated by Polars profiler yet
            kurtosis=None,
            is_normal=None,
            outliers_iqr=outliers_iqr,
            outliers_zscore=outliers_zscore,
            outlier_count=outlier_count,
            outlier_percentage=outlier_percentage,
            percentile_95=None,
            percentile_99=None,
            percentile_1=None,
            percentile_5=None,
            normality_tests=None,
            fitted_distributions=[],
            best_fit_distribution=None,
            theoretical_percentiles=None
        )

    @staticmethod
    def _convert_anomalies(col_data: Dict[str, Any]) -> Optional[AnomalyInfo]:
        """Convert anomaly detection information."""

        anomaly_data = col_data.get('anomalies', {})
        if not anomaly_data:
            return None

        has_anomalies = anomaly_data.get('has_anomalies', False)
        anomaly_count = anomaly_data.get('outlier_count', 0)

        # Calculate anomaly percentage
        count = col_data.get('count', 0)
        null_count = col_data.get('null_count', 0)
        non_null_count = count - null_count
        anomaly_percentage = (anomaly_count / non_null_count * 100) if non_null_count > 0 else 0.0

        # Methods used
        methods = []
        if 'outliers_iqr' in anomaly_data:
            methods.append('IQR')
        if 'outliers_zscore' in anomaly_data:
            methods.append('Z-Score')

        # Sample anomalies
        samples = []
        if 'outliers_iqr' in anomaly_data:
            samples.extend(anomaly_data['outliers_iqr'][:5])
        if 'outliers_zscore' in anomaly_data:
            samples.extend(anomaly_data['outliers_zscore'][:5])

        return AnomalyInfo(
            has_anomalies=has_anomalies,
            anomaly_count=anomaly_count,
            anomaly_percentage=anomaly_percentage,
            anomaly_methods=methods,
            anomaly_samples=samples[:10],  # Limit to 10
            anomaly_details=[]
        )

    @staticmethod
    def _convert_patterns(col_data: Dict[str, Any]) -> Optional[PatternInfo]:
        """Convert pattern detection information."""

        patterns = col_data.get('patterns', {})
        if not patterns:
            return None

        # Extract semantic type
        semantic_type = patterns.get('semantic_type')
        semantic_confidence = patterns.get('semantic_confidence', 0.0)

        # Extract regex pattern
        regex_pattern = patterns.get('common_pattern')

        # Format examples
        format_examples = patterns.get('format_examples', [])

        # PII detection
        pii_detected = col_data.get('has_pii', False)
        pii_types = patterns.get('pii_types', [])

        return PatternInfo(
            semantic_type=semantic_type,
            semantic_confidence=semantic_confidence,
            regex_pattern=regex_pattern,
            format_examples=format_examples,
            pii_detected=pii_detected,
            pii_types=pii_types
        )

    @staticmethod
    def _convert_correlations(corr_data: Dict[str, Any]) -> List[CorrelationResult]:
        """Convert correlation information."""

        correlations = []

        # Handle different correlation formats
        if 'correlations' in corr_data:
            corr_dict = corr_data['correlations']

            # Format: "col1|col2": value
            for key, value in corr_dict.items():
                if '|' in key:
                    col1, col2 = key.split('|')
                elif '__' in key:
                    col1, col2 = key.split('__')
                else:
                    continue

                # Only include significant correlations
                if abs(value) > 0.3:
                    correlations.append(
                        CorrelationResult(
                            column1=col1,
                            column2=col2,
                            correlation=float(value),
                            type='pearson'
                        )
                    )

        # Sort by absolute correlation value (strongest first)
        correlations.sort(key=lambda x: abs(x.correlation), reverse=True)

        return correlations

    @staticmethod
    def _calculate_overall_quality(columns: List[ColumnProfile]) -> float:
        """Calculate overall quality score from column quality metrics."""

        if not columns:
            return 0.0

        # Average of all column quality scores
        total_score = sum(col.quality.overall_score for col in columns)
        return total_score / len(columns)

    @staticmethod
    def _generate_suggestions(columns: List[ColumnProfile]) -> List[ValidationSuggestion]:
        """Generate validation suggestions based on column profiles."""

        suggestions = []

        for col in columns:
            # Suggest mandatory field check for high completeness
            if col.quality.completeness > 95:
                suggestions.append(
                    ValidationSuggestion(
                        validation_type="MandatoryFieldCheck",
                        severity="ERROR",
                        params={"fields": [col.name]},
                        reason=f"Column '{col.name}' has {col.quality.completeness:.1f}% completeness",
                        confidence=col.quality.completeness
                    )
                )

            # Suggest range check for numeric columns
            if col.statistics.min_value is not None and col.statistics.max_value is not None:
                suggestions.append(
                    ValidationSuggestion(
                        validation_type="RangeCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "min_value": col.statistics.min_value,
                            "max_value": col.statistics.max_value
                        },
                        reason=f"Observed range: {col.statistics.min_value} to {col.statistics.max_value}",
                        confidence=80.0
                    )
                )

            # Suggest regex check for patterns
            if col.patterns and col.patterns.semantic_type:
                suggestions.append(
                    ValidationSuggestion(
                        validation_type="RegexCheck",
                        severity="ERROR",
                        params={
                            "field": col.name,
                            "pattern": col.patterns.regex_pattern or ".*",
                            "message": f"Invalid {col.patterns.semantic_type} format"
                        },
                        reason=f"Detected {col.patterns.semantic_type} pattern",
                        confidence=col.patterns.semantic_confidence
                    )
                )

            # Suggest unique check for high uniqueness
            if col.quality.uniqueness > 95:
                suggestions.append(
                    ValidationSuggestion(
                        validation_type="UniqueCheck",
                        severity="ERROR",
                        params={"fields": [col.name]},
                        reason=f"Column '{col.name}' appears to be a unique identifier ({col.quality.uniqueness:.1f}% unique)",
                        confidence=col.quality.uniqueness
                    )
                )

        return suggestions

    @staticmethod
    def _generate_validation_config(
        file_name: str,
        file_path: str,
        file_format: str,
        columns: List[ColumnProfile],
        suggestions: List[ValidationSuggestion],
        row_count: int,
        file_size: int
    ) -> tuple[str, str]:
        """
        Generate validation configuration YAML and CLI command with intelligent settings recommendations.

        Analyzes dataset characteristics to recommend optimal processing settings including:
        - Backend selection (Polars vs pandas)
        - Chunk size optimization
        - Sampling strategy
        - Memory management
        - Performance tuning
        """

        # Use the same logic from pandas profiler for consistency
        # Import to avoid duplication
        from validation_framework.profiler.engine import DataProfiler
        from datetime import datetime

        # Calculate intelligent settings based on dataset characteristics
        file_size_mb = file_size / (1024 * 1024)
        file_size_gb = file_size_mb / 1024
        num_columns = len(columns)

        # Determine optimal backend
        if file_format == "excel":
            recommended_backend = "pandas"
            backend_reason = "Excel files require pandas backend"
        elif file_size_mb > 100 or row_count > 1000000:
            recommended_backend = "polars"
            backend_reason = f"Large dataset ({row_count:,} rows, {file_size_mb:.1f}MB) benefits from Polars (5-10x faster)"
        elif file_format == "parquet":
            recommended_backend = "polars"
            backend_reason = "Parquet format has native Polars support (zero-copy, very fast)"
        else:
            recommended_backend = "polars"
            backend_reason = "Polars recommended for best performance (fallback to pandas if unavailable)"

        # Determine optimal chunk size
        if recommended_backend == "polars":
            # Polars handles larger chunks efficiently
            if row_count > 10000000:
                chunk_size = 200000
                chunk_reason = "Large dataset (>10M rows): 200K chunks for optimal Polars streaming"
            elif row_count > 1000000:
                chunk_size = 100000
                chunk_reason = "Medium-large dataset (>1M rows): 100K chunks balances memory and performance"
            else:
                chunk_size = 50000
                chunk_reason = "Standard 50K chunk size suitable for dataset size"
        else:
            # pandas requires smaller chunks for memory efficiency
            if row_count > 1000000:
                chunk_size = 50000
                chunk_reason = "Large dataset with pandas: 50K chunks to prevent OOM"
            else:
                chunk_size = 50000
                chunk_reason = "Standard pandas chunk size"

        # Determine sampling strategy
        if row_count > 50000000:
            use_sampling = True
            sampling_reason = "Very large dataset (>50M rows): sampling recommended for memory efficiency"
            sampling_flag = "--sampling"
        elif file_size_gb > 10:
            use_sampling = True
            sampling_reason = f"Large file ({file_size_gb:.1f}GB): sampling prevents memory issues"
            sampling_flag = "--sampling"
        elif num_columns > 100:
            use_sampling = True
            sampling_reason = f"Wide dataset ({num_columns} columns): sampling reduces memory footprint"
            sampling_flag = "--sampling"
        else:
            use_sampling = False
            sampling_reason = "Dataset size suitable for full validation (no sampling needed)"
            sampling_flag = "--no-sampling"

        # Determine max sample failures
        if row_count > 10000000:
            max_failures = 1000
            failures_reason = "Large dataset: capture up to 1000 sample failures for analysis"
        elif row_count > 1000000:
            max_failures = 500
            failures_reason = "Medium dataset: 500 sample failures provides good coverage"
        else:
            max_failures = 100
            failures_reason = "Standard 100 sample failures limit"

        # Build YAML configuration with intelligent recommendations
        yaml_lines = [
            "# ============================================================================",
            "# Auto-generated DataK9 Validation Configuration",
            "# ============================================================================",
            f"# Generated from profile of: {file_name}",
            f"# Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "#",
            "# Dataset Characteristics:",
            f"#   • Rows:       {row_count:,}",
            f"#   • Columns:    {num_columns}",
            f"#   • File Size:  {file_size_mb:.1f} MB" + (f" ({file_size_gb:.2f} GB)" if file_size_gb >= 1 else ""),
            f"#   • Format:     {file_format.upper()}",
            "#",
            "# Recommended Settings (see 'processing' section below for explanations):",
            f"#   ✓ Backend:    {recommended_backend.upper()} - {backend_reason}",
            f"#   ✓ Chunk Size: {chunk_size:,} rows - {chunk_reason}",
            f"#   ✓ Sampling:   {'ENABLED' if use_sampling else 'DISABLED'} - {sampling_reason}",
            "#",
            "# ============================================================================",
            "",
            "validation_job:",
            f'  name: "Validation for {file_name}"',
            '  description: "Auto-generated from data profile with intelligent settings recommendations"',
            "",
            "  files:",
            f'    - name: "{Path(file_name).stem}"',
            f'      path: "{file_path}"',
            f'      format: "{file_format}"',
            "",
            "      validations:"
        ]

        # Add suggested validations (with proper indentation for nesting under files[0].validations)
        for suggestion in suggestions[:15]:  # Limit to top 15
            yaml_lines.append(f'        - type: "{suggestion.validation_type}"')
            yaml_lines.append(f'          severity: "{suggestion.severity}"')

            if suggestion.params:
                yaml_lines.append('          params:')
                for key, value in suggestion.params.items():
                    if isinstance(value, list):
                        yaml_lines.append(f'            {key}:')
                        for item in value:
                            if isinstance(item, str):
                                yaml_lines.append(f'              - "{item}"')
                            else:
                                yaml_lines.append(f'              - {item}')
                    elif isinstance(value, str):
                        yaml_lines.append(f'            {key}: "{value}"')
                    else:
                        yaml_lines.append(f'            {key}: {value}')

            yaml_lines.append(f'          # Recommendation: {suggestion.reason}')
            yaml_lines.append("")

        # Add comprehensive processing settings with explanations
        yaml_lines.extend([
            "  # ===========================================================================",
            "  # Processing Settings - Optimized for Your Dataset",
            "  # ===========================================================================",
            "  # These settings have been intelligently selected based on your data profile.",
            "  # Adjust if needed for your specific environment and requirements.",
            "  #",
            "  processing:",
            "",
            f"    # Chunk Size: {chunk_size:,} rows per chunk",
            f"    # Why: {chunk_reason}",
            "    # • Larger chunks = faster processing but more memory",
            "    # • Smaller chunks = slower but memory-safe",
            f"    # • Your data: {row_count:,} rows, {file_size_mb:.1f}MB",
            f"    chunk_size: {chunk_size}",
            "",
            f"    # Max Sample Failures: {max_failures} failures captured per validation",
            f"    # Why: {failures_reason}",
            "    # • Captures representative sample of failures for debugging",
            "    # • Prevents memory overflow from storing millions of failures",
            "    # • Use JSON report for detailed failure analysis",
            f"    max_sample_failures: {max_failures}",
            "",
            "    # Parallel Processing: Validate multiple files concurrently",
            "    # Why: Disabled by default for predictable resource usage",
            "    # • Enable if validating multiple independent files",
            "    # • Requires sufficient CPU cores and memory",
            "    parallel_files: false",
            "",
            "  # ===========================================================================",
            "  # Output Configuration",
            "  # ===========================================================================",
            "  #",
            "  output:",
            '    html_report: "validation_report.html"',
            '    json_summary: "validation_summary.json"',
            "",
            "    # Fail on Error: Stop execution if ERROR-severity validations fail",
            "    # Why: Recommended for data quality gates in CI/CD pipelines",
            "    fail_on_error: false",
            "",
            "    # Fail on Warning: Stop execution if WARNING-severity validations fail",
            "    # Why: Disabled by default (warnings are informational)",
            "    fail_on_warning: false"
        ])

        config_yaml = "\n".join(yaml_lines)

        # Generate optimized CLI command with recommended flags
        config_filename = f"{Path(file_name).stem}_validation.yaml"
        command = (
            f"python3 -m validation_framework.cli validate {config_filename} "
            f"--backend {recommended_backend} "
            f"{sampling_flag} "
            f"-o validation_report.html -j validation_summary.json"
        )

        return config_yaml, command

    @staticmethod
    def _create_minimal_column_profile(col_name: str) -> ColumnProfile:
        """Create a minimal column profile for error recovery."""

        return ColumnProfile(
            name=col_name,
            type_info=TypeInference(
                inferred_type='unknown',
                confidence=0.0,
                is_known=False
            ),
            statistics=ColumnStatistics(
                count=0,
                null_count=0,
                null_percentage=0.0,
                unique_count=0,
                unique_percentage=0.0,
                cardinality=0.0
            ),
            quality=QualityMetrics(
                completeness=0.0,
                validity=0.0,
                uniqueness=0.0,
                consistency=0.0,
                overall_score=0.0,
                issues=["Error converting column profile"]
            )
        )
