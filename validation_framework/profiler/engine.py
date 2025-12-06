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
import psutil
import os
import gc  # Explicit garbage collection for memory management

# Initialize logger FIRST before any conditional imports that may use it
logger = logging.getLogger(__name__)

from validation_framework.profiler.profile_result import (
    ProfileResult, ColumnProfile, TypeInference, ColumnStatistics,
    QualityMetrics, CorrelationResult, ValidationSuggestion, DataLineage
)
import hashlib
import platform
import socket
from validation_framework.profiler.column_intelligence import SmartColumnAnalyzer
from validation_framework.loaders.factory import LoaderFactory
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

# Phase 1 Profiler Enhancements
try:
    from validation_framework.profiler.temporal_analysis import TemporalAnalyzer
    TEMPORAL_ANALYSIS_AVAILABLE = True
except ImportError:
    TEMPORAL_ANALYSIS_AVAILABLE = False
    logger.debug("Temporal analysis not available - statsmodels may be missing")

try:
    from validation_framework.profiler.pii_detector import PIIDetector
    PII_DETECTION_AVAILABLE = True
except ImportError:
    PII_DETECTION_AVAILABLE = False
    logger.debug("PII detection not available")

try:
    from validation_framework.profiler.enhanced_correlation import EnhancedCorrelationAnalyzer
    ENHANCED_CORRELATION_AVAILABLE = True
except ImportError:
    ENHANCED_CORRELATION_AVAILABLE = False
    logger.debug("Enhanced correlation not available")

# Phase 2: Semantic Tagging with FIBO
try:
    from validation_framework.profiler.semantic_tagger import SemanticTagger
    SEMANTIC_TAGGING_AVAILABLE = True
except ImportError:
    SEMANTIC_TAGGING_AVAILABLE = False
    # Semantic tagging is optional

# Phase 2b: Schema.org General Semantics
try:
    from validation_framework.profiler.schema_org_tagger import SchemaOrgTagger
    from validation_framework.profiler.semantic_resolver import SemanticResolver
    SCHEMA_ORG_AVAILABLE = True
except ImportError:
    SCHEMA_ORG_AVAILABLE = False
    SchemaOrgTagger = None
    SemanticResolver = None

# Phase 3: ML-based Anomaly Detection (Beta)
try:
    from validation_framework.profiler.ml_analyzer import MLAnalyzer, ChunkedMLAccumulator
    ML_ANALYSIS_AVAILABLE = True
except ImportError:
    ML_ANALYSIS_AVAILABLE = False
    # ML analysis is optional

# Phase 4: Categorical Association Analysis
try:
    from validation_framework.profiler.categorical_analysis import CategoricalAnalyzer
    CATEGORICAL_ANALYSIS_AVAILABLE = True
except ImportError:
    CATEGORICAL_ANALYSIS_AVAILABLE = False
    logger.debug("Categorical analysis not available")

# Validation Suggestion Generator (extracted from god class)
from validation_framework.profiler.validation_suggester import ValidationSuggestionGenerator

# Type Inferrer (extracted from god class)
from validation_framework.profiler.type_inferrer import TypeInferrer

# Statistics Calculator (extracted from god class)
from validation_framework.profiler.statistics_calculator import StatisticsCalculator

# Correlation Insight Synthesizer
try:
    from validation_framework.profiler.correlation_insight_synthesizer import CorrelationInsightSynthesizer
    CORRELATION_INSIGHT_AVAILABLE = True
except ImportError:
    CORRELATION_INSIGHT_AVAILABLE = False
    logger.debug("Correlation insight synthesizer not available")

try:
    from visions.functional import detect_type
    from visions.types import Float, Integer, String, Boolean, Object
    VISIONS_AVAILABLE = True
except ImportError:
    VISIONS_AVAILABLE = False
    logger.debug("Visions library not available - using fallback type inference")


def check_csv_format(file_path: str, sample_rows: int = 1000) -> Dict[str, Any]:
    """
    Check CSV file for structural issues before profiling.

    Args:
        file_path: Path to CSV file
        sample_rows: Number of rows to check

    Returns:
        Dict with 'valid', 'issues', 'delimiter', 'encoding', 'column_count'
    """
    import csv

    result = {
        'valid': True,
        'issues': [],
        'delimiter': ',',
        'encoding': 'utf-8',
        'column_count': 0,
        'rows_checked': 0,
        'inconsistent_rows': []
    }

    # Auto-detect delimiter
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']
    detected_encoding = 'utf-8'

    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as f:
                sample = f.read(8192)
            detected_encoding = encoding
            break
        except UnicodeDecodeError:
            continue

    result['encoding'] = detected_encoding

    # Detect delimiter
    try:
        with open(file_path, 'r', newline='', encoding=detected_encoding) as f:
            sample = f.read(8192)
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample, delimiters=',\t|;:')
        result['delimiter'] = dialect.delimiter
    except (csv.Error, UnicodeDecodeError, IOError) as e:
        logger.debug(f"Delimiter auto-detection failed, defaulting to comma: {e}")
        result['delimiter'] = ','

    # Check for structural issues
    try:
        with open(file_path, 'r', newline='', encoding=detected_encoding) as f:
            reader = csv.reader(f, delimiter=result['delimiter'])

            expected_columns = None
            for i, row in enumerate(reader):
                result['rows_checked'] = i + 1

                if expected_columns is None:
                    expected_columns = len(row)
                    result['column_count'] = expected_columns
                    continue

                if len(row) != expected_columns:
                    result['valid'] = False
                    result['inconsistent_rows'].append({
                        'row': i + 1,
                        'expected': expected_columns,
                        'actual': len(row)
                    })

                if i >= sample_rows:
                    break

    except Exception as e:
        result['valid'] = False
        result['issues'].append(f"Error reading file: {str(e)}")

    # Summarize issues
    if result['inconsistent_rows']:
        count = len(result['inconsistent_rows'])
        result['issues'].append(
            f"{count} row(s) have inconsistent column counts "
            f"(expected {result['column_count']}, delimiter={repr(result['delimiter'])})"
        )

    return result


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

    def __init__(
        self,
        chunk_size: Optional[int] = None,
        max_correlation_columns: int = 20,
        enable_temporal_analysis: bool = True,
        enable_pii_detection: bool = True,
        enable_enhanced_correlation: bool = True,
        enable_semantic_tagging: bool = True,
        enable_ml_analysis: bool = True,
        disable_memory_safety: bool = False,
        full_analysis: bool = False,
        analysis_sample_size: int = 100000,
        field_descriptions: Optional[Dict[str, Dict[str, str]]] = None
    ):
        """
        Initialize data profiler.

        Args:
            chunk_size: Number of rows to process per chunk (None = auto-calculate based on available memory)
            max_correlation_columns: Maximum columns for correlation analysis
            enable_temporal_analysis: Enable Phase 1 temporal analysis (default: True)
            enable_pii_detection: Enable Phase 1 PII detection (default: True)
            enable_enhanced_correlation: Enable Phase 1 enhanced correlation (default: True)
            enable_semantic_tagging: Enable Phase 2 FIBO-based semantic tagging (default: True)
            enable_ml_analysis: Enable Phase 3 ML-based anomaly detection (default: True, Beta)
            disable_memory_safety: Disable memory safety termination (default: False, USE WITH CAUTION)
            full_analysis: Disable internal sampling for ML analysis (default: False, slower but more accurate)
            analysis_sample_size: Sample size for analysis when file exceeds this many rows (default: 100000)
            field_descriptions: Dict of friendly field names/descriptions for context-aware anomaly detection
        """
        self.chunk_size = chunk_size  # None means auto-calculate
        self.analysis_sample_size = analysis_sample_size  # Configurable sample size
        self.max_correlation_columns = max_correlation_columns

        # Phase 1 enhancement flags
        self.enable_temporal_analysis = enable_temporal_analysis and TEMPORAL_ANALYSIS_AVAILABLE
        self.enable_pii_detection = enable_pii_detection and PII_DETECTION_AVAILABLE
        self.enable_enhanced_correlation = enable_enhanced_correlation and ENHANCED_CORRELATION_AVAILABLE

        # Phase 2: Semantic tagging
        self.enable_semantic_tagging = enable_semantic_tagging and SEMANTIC_TAGGING_AVAILABLE

        # Initialize Phase 1 analyzers if enabled
        self.temporal_analyzer = TemporalAnalyzer() if self.enable_temporal_analysis else None
        self.pii_detector = PIIDetector() if self.enable_pii_detection else None
        self.enhanced_correlation_analyzer = EnhancedCorrelationAnalyzer() if self.enable_enhanced_correlation else None

        # Initialize Phase 2: Semantic tagger (FIBO)
        self.semantic_tagger = SemanticTagger() if self.enable_semantic_tagging else None

        # Initialize Phase 2b: Schema.org general semantics + resolver
        self.schema_org_tagger = SchemaOrgTagger() if SCHEMA_ORG_AVAILABLE else None
        self.semantic_resolver = SemanticResolver() if SCHEMA_ORG_AVAILABLE else None

        # Initialize Phase 3: ML analyzer (Beta)
        self.enable_ml_analysis = enable_ml_analysis and ML_ANALYSIS_AVAILABLE
        self.ml_analyzer = MLAnalyzer() if self.enable_ml_analysis else None

        # Initialize Phase 4: Categorical analysis
        self.enable_categorical_analysis = CATEGORICAL_ANALYSIS_AVAILABLE
        self.categorical_analyzer = CategoricalAnalyzer() if self.enable_categorical_analysis else None

        # Initialize validation suggestion generator (extracted from god class)
        self.validation_suggester = ValidationSuggestionGenerator()

        # Initialize type inferrer (extracted from god class)
        self.type_inferrer = TypeInferrer()

        # Initialize statistics calculator (extracted from god class)
        self.stats_calculator = StatisticsCalculator(max_correlation_columns=max_correlation_columns)

        # Memory safety configuration
        self.disable_memory_safety = disable_memory_safety  # WARNING: Only for development/testing
        self.memory_check_interval = 5  # Check memory every N chunks (reduced from 10)
        self.memory_warning_threshold = 70  # Warn at 70% memory usage
        self.memory_critical_threshold = 80  # Terminate at 80% memory usage (failsafe)

        # Full analysis mode - disable internal sampling for ML
        self.full_analysis = full_analysis

        # Field descriptions for context-aware anomaly detection
        self.field_descriptions = field_descriptions

    def _create_data_lineage(
        self,
        file_path: str,
        file_size_bytes: int,
        row_count: int,
        analysis_applied: List[str],
        sampling_info: Optional[Dict[str, Any]] = None
    ) -> DataLineage:
        """
        Create data lineage and provenance tracking for the profile.

        Args:
            file_path: Path to the source file
            file_size_bytes: Size of source file in bytes
            row_count: Total rows in dataset
            analysis_applied: List of analysis types that were applied
            sampling_info: Details about sampling if applied

        Returns:
            DataLineage object with full provenance information
        """
        path = Path(file_path)

        # Get file timestamps
        source_modified_at = None
        source_created_at = None
        source_hash = None

        if path.exists():
            stat = path.stat()
            source_modified_at = datetime.fromtimestamp(stat.st_mtime).isoformat()
            # Note: st_ctime is creation time on Windows, metadata change time on Unix
            source_created_at = datetime.fromtimestamp(stat.st_ctime).isoformat()

            # Calculate SHA-256 hash for smaller files (< 100MB for performance)
            if file_size_bytes < 100 * 1024 * 1024:
                try:
                    sha256 = hashlib.sha256()
                    with open(file_path, 'rb') as f:
                        for chunk in iter(lambda: f.read(8192), b''):
                            sha256.update(chunk)
                    source_hash = sha256.hexdigest()
                except Exception as e:
                    logger.debug(f"Could not calculate file hash: {e}")

        # Determine source type from file extension
        suffix = path.suffix.lower()
        source_type = "file"
        if suffix in ['.csv', '.tsv']:
            source_type = "csv_file"
        elif suffix in ['.parquet', '.pq']:
            source_type = "parquet_file"
        elif suffix in ['.xlsx', '.xls']:
            source_type = "excel_file"
        elif suffix in ['.json', '.jsonl']:
            source_type = "json_file"

        # Build environment info
        environment = {
            "hostname": socket.gethostname(),
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
            "processor": platform.processor() or "unknown"
        }

        # Build transformations list (what was done to the data)
        transformations = []
        if sampling_info and sampling_info.get('was_sampled'):
            transformations.append({
                "type": "sampling",
                "method": sampling_info.get('method', 'random'),
                "sample_size": sampling_info.get('sample_size'),
                "original_size": sampling_info.get('original_size'),
                "reason": "Large dataset optimized for memory efficiency"
            })

        return DataLineage(
            source_type=source_type,
            source_path=str(path.absolute()) if path.exists() else file_path,
            source_hash=source_hash,
            source_size_bytes=file_size_bytes,
            source_modified_at=source_modified_at,
            source_created_at=source_created_at,
            profiled_at=datetime.now().isoformat(),
            profiled_by="DataK9 Profiler",
            profiler_version="1.55",
            environment=environment,
            analysis_applied=analysis_applied,
            sampling_info=sampling_info,
            transformations=transformations
        )

    def _check_memory_safety(self, chunk_idx: int, row_count: int, force_check: bool = False) -> bool:
        """
        Check system memory usage and terminate if critical threshold exceeded.

        Args:
            chunk_idx: Current chunk index
            row_count: Total rows processed so far
            force_check: If True, bypass the interval check and always check memory

        Returns:
            True if safe to continue, False if critical threshold exceeded

        Raises:
            MemoryError: If memory usage exceeds critical threshold
        """
        # Check every N chunks to minimize overhead, or always if force_check=True
        if not force_check and chunk_idx % self.memory_check_interval != 0:
            return True

        try:
            # Get current process memory info
            process = psutil.Process(os.getpid())
            process_memory_mb = process.memory_info().rss / 1024 / 1024

            # Get system memory info
            system_memory = psutil.virtual_memory()
            memory_percent = system_memory.percent

            # Log memory usage
            logger.debug(f"💾 Memory check at chunk {chunk_idx + 1}: Process={process_memory_mb:.1f}MB, System={memory_percent:.1f}%")

            # Check warning threshold
            if memory_percent >= self.memory_warning_threshold and memory_percent < self.memory_critical_threshold:
                logger.warning(f"⚠️  High memory usage: {memory_percent:.1f}% (threshold: {self.memory_warning_threshold}%)")
                logger.warning(f"⚠️  Process using {process_memory_mb:.1f}MB, {row_count:,} rows processed")

            # Check critical threshold
            if memory_percent >= self.memory_critical_threshold:
                available_mb = system_memory.available / 1024 / 1024

                if self.disable_memory_safety:
                    # Memory safety disabled - log warning but continue processing
                    logger.warning(f"🚨 CRITICAL: Memory usage {memory_percent:.1f}% exceeds threshold {self.memory_critical_threshold}%")
                    logger.warning(f"🚨 Process: {process_memory_mb:.1f}MB, Available: {available_mb:.1f}MB")
                    logger.warning(f"🚨 Memory safety disabled (--no-memory-check) - continuing at {row_count:,} rows despite high memory")
                    logger.warning(f"⚠️  System instability possible - monitor memory usage carefully")
                else:
                    # Normal operation - terminate to protect system
                    logger.error(f"🚨 CRITICAL: Memory usage {memory_percent:.1f}% exceeds threshold {self.memory_critical_threshold}%")
                    logger.error(f"🚨 Process: {process_memory_mb:.1f}MB, Available: {available_mb:.1f}MB")
                    logger.error(f"🚨 Terminating profiler to prevent system instability at {row_count:,} rows")
                    raise MemoryError(
                        f"Profiler terminated: Memory usage {memory_percent:.1f}% exceeded critical threshold {self.memory_critical_threshold}%. "
                        f"Processed {row_count:,} rows before termination. "
                        f"Consider using --sample flag to profile a subset of data, or increase available system memory."
                    )

            return True

        except psutil.Error as e:
            logger.warning(f"Could not check memory usage: {e}")
            return True  # Continue if we can't check memory

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
        logger.debug(f"Starting profile of DataFrame: {name}")

        row_count = len(df)

        # Track phase timings for performance analysis
        phase_timings = {}

        # Initialize column profiles
        column_profiles: Dict[str, Dict[str, Any]] = {}
        numeric_data: Dict[str, List[float]] = {}

        # Phase 1 enhancement accumulators
        datetime_data: Dict[str, List] = {}
        all_column_data: Dict[str, List] = {}

        for col in df.columns:
            column_profiles[col] = self._initialize_column_profile(col, declared_schema)

        # Process entire dataframe (already in memory)
        for col in df.columns:
            self._update_column_profile(column_profiles[col], df[col], 0)

            # Collect sample data for PII detection (Phase 1) - limit to 1000 samples
            if self.enable_pii_detection:
                all_column_data[col] = df[col].dropna().head(1000).tolist()

        # Finalize column profiles
        columns = []
        for col_name, profile_data in column_profiles.items():
            column_profile = self._finalize_column_profile(col_name, profile_data, row_count)
            columns.append(column_profile)

        # Collect numeric and datetime data AFTER type finalization (Phase 1)
        # Bound all collections to prevent memory leaks on large dataframes
        MAX_CORRELATION_SAMPLES = 50000  # Sufficient for statistical correlation analysis
        for column in columns:
            # Collect numeric data for correlations
            if column.type_info.inferred_type in ["integer", "float"]:
                try:
                    numeric_values = pd.to_numeric(df[column.name], errors='coerce').dropna()
                    # Bound to MAX_CORRELATION_SAMPLES to prevent memory leaks
                    bounded_values = numeric_values.head(MAX_CORRELATION_SAMPLES)
                    numeric_data[column.name] = bounded_values.tolist()
                    logger.debug(f"Collected {len(bounded_values)} numeric values for {column.name}")
                except Exception as e:
                    logger.warning(f"Failed to collect numeric data for {column.name}: {e}")

            # Collect datetime data for temporal analysis
            if self.enable_temporal_analysis and column.type_info.inferred_type in ["datetime", "date"]:
                try:
                    dt_values = pd.to_datetime(df[column.name], errors='coerce').dropna()
                    # Bound to MAX_CORRELATION_SAMPLES to prevent memory leaks
                    bounded_dt_values = dt_values.head(MAX_CORRELATION_SAMPLES)
                    datetime_data[column.name] = bounded_dt_values.tolist()
                    logger.debug(f"Collected {len(bounded_dt_values)} datetime values for {column.name}")
                except Exception as e:
                    logger.warning(f"Failed to collect datetime data for {column.name}: {e}")

        # Phase 1: Apply temporal analysis to datetime columns
        if self.enable_temporal_analysis:
            logger.debug("Running temporal analysis on datetime columns...")
            for column in columns:
                if column.name in datetime_data and len(datetime_data[column.name]) > 0:
                    try:
                        temporal_result = self.temporal_analyzer.analyze_temporal_column(
                            pd.Series(datetime_data[column.name]),
                            column_name=column.name
                        )
                        column.temporal_analysis = temporal_result
                        logger.debug(f"Temporal analysis completed for column: {column.name}")
                    except Exception as e:
                        logger.warning(f"Temporal analysis failed for column {column.name}: {e}")

        # Phase 1: Apply PII detection to all columns
        pii_columns = []
        pii_start = time.time()
        if self.enable_pii_detection:
            logger.debug("Running PII detection on all columns...")
            for column in columns:
                if column.name in all_column_data and len(all_column_data[column.name]) > 0:
                    try:
                        pii_result = self.pii_detector.detect_pii_in_column(
                            column.name,
                            all_column_data[column.name],
                            total_rows=row_count
                        )
                        column.pii_info = pii_result
                        if pii_result.get("detected"):
                            pii_columns.append(pii_result)
                        logger.debug(f"PII detection completed for column: {column.name}")
                    except Exception as e:
                        logger.warning(f"PII detection failed for column {column.name}: {e}")
            phase_timings['pii_detection'] = time.time() - pii_start
            logger.debug(f"⏱  PII detection completed in {phase_timings['pii_detection']:.2f}s")

        # Phase 2: Apply dual semantic tagging (Schema.org + FIBO) to all columns
        semantic_start = time.time()
        logger.debug("🧠 Running dual semantic tagging (Schema.org + FIBO) on all columns...")
        for column in columns:
            try:
                # Get structural type from pandas dtype
                structural_type = str(column.type_info.declared_type or column.type_info.inferred_type)

                # Step 1: Always compute Schema.org general semantics
                schema_org_result = None
                if self.schema_org_tagger:
                    schema_org_result = self.schema_org_tagger.tag_column(
                        column_name=column.name,
                        inferred_type=column.type_info.inferred_type,
                        statistics=column.statistics,
                        quality=column.quality,
                        sample_values=column.type_info.sample_values
                    )

                # Step 2: Compute FIBO semantics (optional, for financial/ID fields)
                fibo_result = None
                if self.enable_semantic_tagging and self.semantic_tagger:
                    visions_type = getattr(column.statistics, 'semantic_type', None)
                    fibo_info = self.semantic_tagger.tag_column(
                        column_name=column.name,
                        inferred_type=column.type_info.inferred_type,
                        visions_type=visions_type,
                        statistics=column.statistics,
                        quality=column.quality
                    )
                    # Convert SemanticInfo to dict format for resolver
                    if fibo_info and fibo_info.primary_tag != "unknown":
                        fibo_result = {
                            "type": fibo_info.fibo_source or fibo_info.primary_tag,
                            "confidence": fibo_info.confidence,
                            "signals": list(fibo_info.evidence.keys()) if fibo_info.evidence else []
                        }

                # Step 3: Resolve which layer is primary
                if self.semantic_resolver:
                    column.semantic_info = self.semantic_resolver.resolve(
                        structural_type=structural_type,
                        schema_org=schema_org_result,
                        fibo=fibo_result
                    )
                    resolved = column.semantic_info.get("resolved", {})
                    logger.debug(f"Semantic tagging for '{column.name}': {resolved.get('display_label')} (source: {resolved.get('primary_source')})")
                elif schema_org_result:
                    # Fallback if resolver not available
                    column.semantic_info = {
                        "structural_type": structural_type,
                        "schema_org": schema_org_result,
                        "fibo": fibo_result,
                        "resolved": {
                            "primary_source": "schema_org",
                            "primary_type": schema_org_result.get("type"),
                            "secondary_type": None,
                            "display_label": schema_org_result.get("display_label", "Unknown"),
                            "validation_driver": "schema_org"
                        }
                    }
                else:
                    # Legacy FIBO-only mode
                    if fibo_result:
                        column.semantic_info = {
                            "structural_type": structural_type,
                            "schema_org": None,
                            "fibo": fibo_result,
                            "resolved": {
                                "primary_source": "fibo",
                                "primary_type": fibo_result.get("type"),
                                "secondary_type": None,
                                "display_label": fibo_result.get("type", "Unknown"),
                                "validation_driver": "fibo"
                            }
                        }

            except Exception as e:
                logger.warning(f"Semantic tagging failed for column {column.name}: {e}")

        phase_timings['semantic_tagging'] = time.time() - semantic_start
        logger.debug(f"⏱  Semantic tagging completed in {phase_timings['semantic_tagging']:.2f}s")

        # Calculate correlations (using extracted StatisticsCalculator)
        correlation_start = time.time()
        correlations = self.stats_calculator.calculate_correlations(numeric_data, row_count)
        phase_timings['basic_correlation'] = time.time() - correlation_start

        # Phase 1: Calculate enhanced correlations
        enhanced_correlations = None
        if self.enable_enhanced_correlation and len(numeric_data) >= 2:
            enhanced_corr_start = time.time()
            logger.debug("Running enhanced correlation analysis...")
            try:
                enhanced_correlations = self.enhanced_correlation_analyzer.calculate_correlations_multi_method(
                    numeric_data,
                    row_count=row_count,
                    methods=['pearson', 'spearman']
                )
                # Update correlations list with enhanced correlation info
                for pair in enhanced_correlations.get('correlation_pairs', []):
                    correlations.append(CorrelationResult(
                        column1=pair['column1'],
                        column2=pair['column2'],
                        correlation=pair['correlation'],
                        type=pair['method'],
                        strength=pair.get('strength'),
                        direction=pair.get('direction'),
                        p_value=pair.get('p_value'),
                        is_significant=pair.get('is_significant')
                    ))
                logger.debug(f"Enhanced correlation analysis found {len(enhanced_correlations.get('correlation_pairs', []))} significant correlations")
            except Exception as e:
                logger.warning(f"Enhanced correlation analysis failed: {e}")

        # Phase 1: Calculate dataset-level privacy risk
        dataset_privacy_risk = None
        if self.enable_pii_detection and pii_columns:
            logger.debug("Calculating dataset-level privacy risk...")
            try:
                dataset_privacy_risk = self.pii_detector.calculate_dataset_privacy_risk(
                    pii_columns=pii_columns,
                    total_columns=len(columns),
                    total_rows=row_count
                )
                logger.debug(f"Dataset privacy risk: {dataset_privacy_risk.get('risk_level', 'unknown').upper()} ({dataset_privacy_risk.get('risk_score', 0)}/100)")
            except Exception as e:
                logger.warning(f"Dataset privacy risk calculation failed: {e}")

        # Generate validation suggestions (using extracted ValidationSuggestionGenerator)
        suggested_validations = self.validation_suggester.generate_suggestions(
            columns, row_count,
            enable_ml_suggestions=self.enable_ml_analysis,
            ml_analyzer=self.ml_analyzer
        )

        # Calculate overall quality score
        overall_quality = self._calculate_overall_quality(columns)

        # Generate validation configuration (for database source)
        config_yaml, config_command = self._generate_validation_config(
            name, "", "database", 0, row_count, columns, suggested_validations
        )

        processing_time = time.time() - start_time
        logger.debug(f"Profile completed in {processing_time:.2f} seconds")

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
            generated_config_command=config_command,
            enhanced_correlations=enhanced_correlations,
            dataset_privacy_risk=dataset_privacy_risk
        )

    def _extract_parquet_column_stats(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Extract column statistics from parquet metadata without reading data.

        This is a CRITICAL optimization for large parquet files:
        - Gets accurate null counts, min/max from metadata in <1 second
        - vs 55+ minutes reading all data for a 180M row file

        Returns dict with:
            - total_rows: int
            - num_row_groups: int
            - columns: {col_name: {null_count, min, max, has_stats}}
        """
        try:
            import pyarrow.parquet as pq

            pf = pq.ParquetFile(file_path)
            total_rows = pf.metadata.num_rows
            num_row_groups = pf.metadata.num_row_groups
            column_names = pf.schema_arrow.names

            # Initialize stats aggregators
            stats = {
                'total_rows': total_rows,
                'num_row_groups': num_row_groups,
                'columns': {}
            }

            for col in column_names:
                stats['columns'][col] = {
                    'null_count': 0,
                    'min': None,
                    'max': None,
                    'has_stats': False
                }

            # Aggregate statistics across all row groups
            for rg_idx in range(num_row_groups):
                rg_meta = pf.metadata.row_group(rg_idx)
                for col_idx in range(rg_meta.num_columns):
                    col_meta = rg_meta.column(col_idx)
                    col_name = col_meta.path_in_schema

                    if col_meta.statistics:
                        stats['columns'][col_name]['has_stats'] = True
                        stats['columns'][col_name]['null_count'] += col_meta.statistics.null_count or 0

                        # Track global min/max
                        if col_meta.statistics.min is not None:
                            current_min = stats['columns'][col_name]['min']
                            if current_min is None or col_meta.statistics.min < current_min:
                                stats['columns'][col_name]['min'] = col_meta.statistics.min

                        if col_meta.statistics.max is not None:
                            current_max = stats['columns'][col_name]['max']
                            if current_max is None or col_meta.statistics.max > current_max:
                                stats['columns'][col_name]['max'] = col_meta.statistics.max

            logger.debug(f"📊 Extracted parquet stats from {num_row_groups:,} row groups (no data read)")
            return stats

        except Exception as e:
            logger.debug(f"Could not extract parquet stats: {e}")
            return None

    def _create_stratified_parquet_chunks(
        self,
        file_path: str,
        sample_size: int = 50000,
        chunk_size: int = 50000,
        num_strata: int = 10
    ):
        """
        Generator that yields chunks from stratified row groups (MEMORY-EFFICIENT).

        Instead of loading all sample data at once, this yields chunks one at a time,
        keeping memory usage bounded to chunk_size rows.

        Strategy:
        1. Divide row groups into strata across the file
        2. Yield chunks from each stratum, sampling if row group is larger than chunk_size
        3. Stop when sample_size total rows have been yielded

        Args:
            file_path: Path to parquet file
            sample_size: Total rows to sample across all chunks
            chunk_size: Maximum rows per chunk (memory bound)
            num_strata: Number of strata to divide file into

        Yields:
            DataFrame chunks, each with at most chunk_size rows
        """
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(file_path)
        total_rows = pf.metadata.num_rows
        num_row_groups = pf.metadata.num_row_groups

        # Calculate rows per row group
        avg_rows_per_group = total_rows / num_row_groups

        # Calculate how many row groups we need
        groups_needed = int(np.ceil(sample_size / avg_rows_per_group))
        actual_strata = max(num_strata, min(groups_needed, num_row_groups))

        # Calculate rows to collect from each stratum
        rows_per_stratum = sample_size // actual_strata
        extra_rows = sample_size % actual_strata

        # Select one row group from each stratum (spread across file)
        stratum_size = num_row_groups / actual_strata
        selected_groups = []
        for stratum_idx in range(actual_strata):
            stratum_start = int(stratum_idx * stratum_size)
            stratum_end = int((stratum_idx + 1) * stratum_size)
            middle_group = (stratum_start + stratum_end) // 2
            selected_groups.append((stratum_idx, middle_group))

        logger.debug(f"📊 Stratified chunking: {actual_strata} strata, ~{rows_per_stratum:,} rows each, chunk_size={chunk_size:,}")

        rows_yielded = 0

        for stratum_idx, rg_idx in selected_groups:
            if rows_yielded >= sample_size:
                break

            # Calculate rows to take from this stratum
            rows_to_take = rows_per_stratum + (1 if stratum_idx < extra_rows else 0)
            rows_to_take = min(rows_to_take, sample_size - rows_yielded)

            # Load single row group
            rg_table = pf.read_row_group(rg_idx)
            rg_df = rg_table.to_pandas()

            # Sample if row group is larger than needed
            if len(rg_df) > rows_to_take:
                rg_df = rg_df.sample(n=rows_to_take, random_state=42 + stratum_idx)

            # Yield in chunk_size pieces if needed
            for start_idx in range(0, len(rg_df), chunk_size):
                if rows_yielded >= sample_size:
                    break

                end_idx = min(start_idx + chunk_size, len(rg_df))
                chunk = rg_df.iloc[start_idx:end_idx].copy()

                rows_yielded += len(chunk)
                logger.debug(f"📊 Yielding chunk from stratum {stratum_idx + 1}/{actual_strata}: {len(chunk):,} rows (total: {rows_yielded:,}/{sample_size:,})")

                yield chunk

                del chunk
                gc.collect()

            # Clean up row group
            del rg_table, rg_df
            gc.collect()

        logger.debug(f"📊 Stratified sampling complete: {rows_yielded:,} rows from {len(selected_groups)} row groups")

    def profile_file(
        self,
        file_path: str,
        file_format: Optional[str] = None,
        declared_schema: Optional[Dict[str, str]] = None,
        sample_rows: Optional[int] = None,
        **loader_kwargs
    ) -> ProfileResult:
        """
        Profile a data file comprehensively.

        Args:
            file_path: Path to file to profile
            file_format: Format (csv, excel, json, parquet). Auto-detected from extension if not specified.
            declared_schema: Optional declared schema {column: type}
            sample_rows: Optional limit - profile only the first N rows (useful for large files)
            **loader_kwargs: Additional arguments for data loader

        Returns:
            ProfileResult with comprehensive profile information
        """
        start_time = time.time()
        logger.debug(f"Starting profile of {file_path}")

        # Auto-detect format from file extension if not specified
        if file_format is None:
            suffix = Path(file_path).suffix.lower()
            format_map = {
                '.csv': 'csv',
                '.tsv': 'csv',
                '.parquet': 'parquet',
                '.pq': 'parquet',
                '.xlsx': 'excel',
                '.xls': 'excel',
                '.json': 'json',
                '.jsonl': 'json',
            }
            file_format = format_map.get(suffix, 'csv')
            logger.debug(f"Auto-detected format '{file_format}' from extension '{suffix}'")

        # Track timing for each phase
        phase_timings = {}

        # CSV format check for CSV files
        csv_format_check = None
        if file_format.lower() == 'csv':
            csv_format_check = check_csv_format(file_path)
            if not csv_format_check['valid']:
                logger.warning(f"CSV format issues detected: {csv_format_check['issues']}")

        # 50k Sampling Policy:
        # Datasets >= 50k rows use a 50k sample for statistical/ML analysis.
        # This provides strong statistical accuracy (±0.5-1%) while keeping processing fast.
        # Row counts, null counts, and metadata always use the full dataset.
        ANALYSIS_SAMPLE_SIZE = self.analysis_sample_size  # Configurable sample size for ML and statistical analysis
        MAX_CORRELATION_SAMPLES = max(100_000, ANALYSIS_SAMPLE_SIZE * 2)  # Scale with sample size
        MAX_TEMPORAL_SAMPLES = ANALYSIS_SAMPLE_SIZE  # Limit datetime samples for temporal analysis
        sampling_triggered = {}  # Track which columns hit sampling limit

        # Get file metadata
        file_path_obj = Path(file_path)
        file_size = file_path_obj.stat().st_size
        file_name = file_path_obj.name

        # Auto-calculate chunk size if not specified
        chunk_size = self.chunk_size
        if chunk_size is None:
            calculator = ChunkSizeCalculator()
            calc_result = calculator.calculate_optimal_chunk_size(
                file_path=file_path,
                file_format=file_format,
                num_validations=0,  # Profiling only
                validation_complexity="simple"
            )
            chunk_size = calc_result['recommended_chunk_size']
            logger.debug(f"🎯 Auto-calculated chunk size: {chunk_size:,} rows (based on {calc_result['available_memory_mb']:,}MB available memory)")
            logger.debug(f"   Estimated chunks: {calc_result['estimated_chunks']:,} | Peak memory: ~{calc_result['estimated_memory_mb']:,}MB")
        else:
            logger.debug(f"📊 Using specified chunk size: {chunk_size:,} rows")

        # Load data iterator
        loader = LoaderFactory.create_loader(
            file_format=file_format,
            file_path=file_path,
            chunk_size=chunk_size,
            **loader_kwargs
        )

        # Initialize accumulators
        row_count = 0
        column_profiles: Dict[str, Dict[str, Any]] = {}
        numeric_data: Dict[str, List[float]] = {}  # For correlation analysis

        # Phase 1 enhancement accumulators
        datetime_data: Dict[str, List] = {}  # For temporal analysis
        all_column_data: Dict[str, List] = {}  # For PII detection (sample-based)

        # Phase 3: Chunked ML accumulator for full analysis mode
        # When full_analysis=True, we accumulate ML stats during chunk processing
        # instead of loading all data at once (which would cause OOM on large datasets)
        ml_accumulator = None
        if self.full_analysis and self.enable_ml_analysis and ML_ANALYSIS_AVAILABLE:
            ml_accumulator = ChunkedMLAccumulator()
            logger.info("📊 Full analysis mode: ML stats will be accumulated during chunk processing")

        # Try to get metadata for enhanced profiling (works for Parquet and other formats)
        total_chunks_str = "?"
        file_metadata = {}
        try:
            if hasattr(loader, 'get_metadata'):
                file_metadata = loader.get_metadata()

                # Log Parquet-specific metadata if available
                if file_metadata.get('total_rows'):
                    total_rows = file_metadata['total_rows']
                    import math
                    total_chunks = math.ceil(total_rows / self.chunk_size)
                    total_chunks_str = str(total_chunks)

                    # Build info message with metadata
                    info_parts = [f"{total_rows:,} rows"]
                    if file_metadata.get('compression'):
                        info_parts.append(f"{file_metadata['compression']} compression")
                    if file_metadata.get('num_row_groups'):
                        info_parts.append(f"{file_metadata['num_row_groups']} row groups")

                    logger.debug(f"📋 File: {', '.join(info_parts)} ({total_chunks} chunks of {self.chunk_size:,})")
            elif hasattr(loader, 'get_row_count'):
                # Fallback to just row count
                total_rows = loader.get_row_count()
                import math
                total_chunks = math.ceil(total_rows / self.chunk_size)
                total_chunks_str = str(total_chunks)
                logger.debug(f"📋 File contains {total_rows:,} rows ({total_chunks} chunks of {self.chunk_size:,})")
        except Exception as e:
            # If we can't get metadata, just show "?" - not a critical failure
            logger.debug(f"Could not read file metadata: {e}")
            pass

        # PARQUET OPTIMIZATION: For large parquet files, use stratified sampling
        # and metadata for accurate stats. This reduces profiling from 55+ min to ~60s.
        # CSV files use full scan (necessary for accurate placeholder null detection).
        parquet_column_stats = None
        actual_total_rows = None  # Track actual file rows vs sampled rows
        use_stratified_chunks = False  # Flag to use stratified chunk iterator

        if file_format == 'parquet' and file_metadata.get('total_rows', 0) > ANALYSIS_SAMPLE_SIZE:
            actual_total_rows = file_metadata['total_rows']
            logger.info(f"📊 Large parquet file ({actual_total_rows:,} rows) - using stratified chunked sampling")

            # Extract accurate column statistics from parquet metadata
            parquet_column_stats = self._extract_parquet_column_stats(file_path)
            use_stratified_chunks = True

        # Process data in chunks
        # For parquet: use stratified chunk iterator (memory-efficient)
        # For CSV and other formats: iterate through all chunks (full scan)
        chunk_processing_start = time.time()

        if use_stratified_chunks:
            # PARQUET PATH: Use stratified chunk iterator (memory-efficient)
            # Each chunk is bounded by chunk_size, data comes from across the file
            chunk_iterator = self._create_stratified_parquet_chunks(
                file_path,
                sample_size=ANALYSIS_SAMPLE_SIZE,
                chunk_size=self.chunk_size or 50000,  # Default chunk size
                num_strata=10
            )
            total_chunks_str = "?"
        else:
            # CSV/OTHER PATH: Use regular loader iterator (full scan for accurate counts)
            chunk_iterator = loader.load()

        # Process chunks from either iterator (unified processing for both paths)
        for chunk_idx, chunk in enumerate(chunk_iterator):
            # Handle sampling: if we've already reached sample_rows, don't process more
            if sample_rows and row_count >= sample_rows:
                logger.debug(f"📊 Sample limit reached ({sample_rows:,} rows) - stopping chunk processing")
                break

            # If sampling and this chunk would exceed limit, truncate it
            if sample_rows and (row_count + len(chunk)) > sample_rows:
                rows_to_take = sample_rows - row_count
                chunk = chunk.iloc[:rows_to_take]
                logger.debug(f"📊 Final chunk truncated to {rows_to_take:,} rows to meet sample limit")

            row_count += len(chunk)
            logger.debug(f"📊 Processing chunk {chunk_idx + 1}/{total_chunks_str} ({len(chunk):,} rows) - Total: {row_count:,} rows")

            # Memory safety check - will raise MemoryError if critical threshold exceeded
            self._check_memory_safety(chunk_idx, row_count)

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

                # Collect numeric data for correlations with memory-efficient sampling
                # Limit to MAX_CORRELATION_SAMPLES per column to prevent memory exhaustion with very large datasets
                # Note: Check pandas dtype directly since inferred_type is not set until after chunk processing
                if pd.api.types.is_numeric_dtype(chunk[col]):
                    if col not in numeric_data:
                        numeric_data[col] = []

                    # Only collect if we haven't reached the limit
                    current_count = len(numeric_data[col])
                    if current_count < MAX_CORRELATION_SAMPLES:
                        # Convert to numeric, handling errors
                        numeric_values = pd.to_numeric(chunk[col], errors='coerce').dropna()

                        # Calculate how many samples to take from this chunk
                        samples_needed = MAX_CORRELATION_SAMPLES - current_count

                        # Use simple random sampling if chunk is larger than samples needed
                        if len(numeric_values) > samples_needed:
                            # Use pandas .sample() instead of tolist() to avoid memory spike
                            sampled = numeric_values.sample(n=samples_needed, random_state=42)
                            numeric_data[col].extend(sampled.tolist())
                            # Log when we hit the limit
                            if col not in sampling_triggered:
                                sampling_triggered[col] = row_count
                                logger.debug(f"💾 Memory optimization: Column '{col}' sampling limit reached at {row_count:,} rows (using {MAX_CORRELATION_SAMPLES:,} samples for correlation)")
                        else:
                            # Size is under limit, but still bound to samples_needed to prevent unbounded growth
                            bounded_values = numeric_values.head(samples_needed)
                            numeric_data[col].extend(bounded_values.tolist())

                # Collect datetime data for temporal analysis with memory-efficient sampling
                # CRITICAL: Only attempt datetime conversion on likely datetime columns to avoid memory bloat
                if self.enable_temporal_analysis:
                    # Skip datetime conversion for obviously non-datetime columns
                    col_lower = col.lower()
                    name_suggests_datetime = any(keyword in col_lower for keyword in ['date', 'time', 'timestamp', 'datetime', 'created', 'updated', 'modified'])

                    is_likely_datetime = (
                        # Column is already datetime type - always try to analyze
                        pd.api.types.is_datetime64_any_dtype(chunk[col]) or
                        # Column name suggests datetime - try conversion even if string type
                        name_suggests_datetime
                    )

                    if is_likely_datetime:
                        try:
                            # Try to convert to datetime
                            dt_values = pd.to_datetime(chunk[col], errors='coerce')
                            # Only keep if at least 50% of non-null values converted successfully
                            non_null_count = chunk[col].notna().sum()
                            if non_null_count > 0:
                                converted_count = dt_values.notna().sum()
                                if converted_count / non_null_count >= 0.5:
                                    if col not in datetime_data:
                                        datetime_data[col] = []

                                    # Only collect if we haven't reached the limit
                                    current_count = len(datetime_data[col])
                                    if current_count < MAX_TEMPORAL_SAMPLES:
                                        # MEMORY EFFICIENT: Sample from Series before tolist()
                                        dt_series = dt_values.dropna()
                                        samples_needed = MAX_TEMPORAL_SAMPLES - current_count

                                        # Sample from Series first to avoid memory spike
                                        if len(dt_series) > samples_needed:
                                            sampled = dt_series.sample(n=samples_needed, random_state=42 + chunk_idx)
                                            datetime_data[col].extend(sampled.tolist())
                                            if col not in sampling_triggered:
                                                sampling_triggered[col] = row_count
                                                logger.debug(f"💾 Memory optimization: Column '{col}' temporal sampling limit reached at {row_count:,} rows (using {MAX_TEMPORAL_SAMPLES:,} samples)")
                                        else:
                                            # Size is under limit - use head() to bound
                                            bounded_dt = dt_series.head(samples_needed)
                                            datetime_data[col].extend(bounded_dt.tolist())
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Could not convert column '{col}' to datetime: {e}")

                # Collect sample data for PII detection (Phase 1) - limit to 1000 samples per column
                if self.enable_pii_detection:
                    if col not in all_column_data:
                        all_column_data[col] = []
                    if len(all_column_data[col]) < 1000:
                        samples_needed = 1000 - len(all_column_data[col])
                        all_column_data[col].extend(chunk[col].dropna().head(samples_needed).tolist())

            # Process chunk for ML analysis (full_analysis mode)
            # This accumulates ML stats without loading all data at once
            if ml_accumulator is not None:
                # Force memory check before ML processing (heavy operation)
                self._check_memory_safety(chunk_idx, row_count, force_check=True)
                ml_accumulator.process_chunk(chunk, chunk_idx)

            # Clean up chunk immediately after processing to free memory
            del chunk
            gc.collect()

        # Record chunk processing time
        phase_timings['chunk_processing'] = time.time() - chunk_processing_start
        logger.debug(f"⏱  Chunk processing completed in {phase_timings['chunk_processing']:.2f}s")

        # Log memory optimization summary
        if sampling_triggered:
            logger.debug(f"💾 Memory optimization: Sampled {len(sampling_triggered)} numeric columns (max {MAX_CORRELATION_SAMPLES:,} values each)")
            total_samples = sum(len(values) for values in numeric_data.values())
            logger.debug(f"💾 Total correlation samples in memory: {total_samples:,} values (vs {row_count:,} total rows)")

        # Finalize column profiles
        finalize_start = time.time()
        columns = []
        for col_name, profile_data in column_profiles.items():
            column_profile = self._finalize_column_profile(col_name, profile_data, row_count)
            columns.append(column_profile)
        phase_timings['finalize_profiles'] = time.time() - finalize_start
        logger.debug(f"⏱  Profile finalization completed in {phase_timings['finalize_profiles']:.2f}s")

        # Phase 1: Apply temporal analysis to datetime columns
        if self.enable_temporal_analysis:
            temporal_start = time.time()
            logger.debug("Running temporal analysis on datetime columns...")
            for column in columns:
                if column.name in datetime_data and len(datetime_data[column.name]) > 0:
                    try:
                        temporal_result = self.temporal_analyzer.analyze_temporal_column(
                            pd.Series(datetime_data[column.name]),
                            column_name=column.name
                        )
                        column.temporal_analysis = temporal_result
                        logger.debug(f"Temporal analysis completed for column: {column.name}")
                    except Exception as e:
                        logger.warning(f"Temporal analysis failed for column {column.name}: {e}")
            phase_timings['temporal_analysis'] = time.time() - temporal_start
            logger.debug(f"⏱  Temporal analysis completed in {phase_timings['temporal_analysis']:.2f}s")

        # Phase 1: Apply PII detection to all columns
        pii_columns = []
        if self.enable_pii_detection:
            pii_start = time.time()
            logger.debug("Running PII detection on all columns...")
            for column in columns:
                if column.name in all_column_data and len(all_column_data[column.name]) > 0:
                    try:
                        pii_result = self.pii_detector.detect_pii_in_column(
                            column.name,
                            all_column_data[column.name],
                            total_rows=row_count
                        )
                        column.pii_info = pii_result
                        if pii_result.get("detected"):
                            pii_columns.append(pii_result)
                        logger.debug(f"PII detection completed for column: {column.name}")
                    except Exception as e:
                        logger.warning(f"PII detection failed for column {column.name}: {e}")
            phase_timings['pii_detection'] = time.time() - pii_start
            logger.debug(f"⏱  PII detection completed in {phase_timings['pii_detection']:.2f}s")

        # Phase 2: Apply dual semantic tagging (Schema.org + FIBO) to all columns
        semantic_start = time.time()
        logger.debug("🧠 Running dual semantic tagging (Schema.org + FIBO) on all columns...")
        for column in columns:
            try:
                # Get structural type from pandas dtype
                structural_type = str(column.type_info.declared_type or column.type_info.inferred_type)

                # Step 1: Always compute Schema.org general semantics
                schema_org_result = None
                if self.schema_org_tagger:
                    schema_org_result = self.schema_org_tagger.tag_column(
                        column_name=column.name,
                        inferred_type=column.type_info.inferred_type,
                        statistics=column.statistics,
                        quality=column.quality,
                        sample_values=column.type_info.sample_values
                    )

                # Step 2: Compute FIBO semantics (optional, for financial/ID fields)
                fibo_result = None
                if self.enable_semantic_tagging and self.semantic_tagger:
                    visions_type = getattr(column.statistics, 'semantic_type', None)
                    fibo_info = self.semantic_tagger.tag_column(
                        column_name=column.name,
                        inferred_type=column.type_info.inferred_type,
                        visions_type=visions_type,
                        statistics=column.statistics,
                        quality=column.quality
                    )
                    # Convert SemanticInfo to dict format for resolver
                    if fibo_info and fibo_info.primary_tag != "unknown":
                        fibo_result = {
                            "type": fibo_info.fibo_source or fibo_info.primary_tag,
                            "confidence": fibo_info.confidence,
                            "signals": list(fibo_info.evidence.keys()) if fibo_info.evidence else []
                        }

                # Step 3: Resolve which layer is primary
                if self.semantic_resolver:
                    column.semantic_info = self.semantic_resolver.resolve(
                        structural_type=structural_type,
                        schema_org=schema_org_result,
                        fibo=fibo_result
                    )
                    resolved = column.semantic_info.get("resolved", {})
                    logger.debug(f"Semantic tagging for '{column.name}': {resolved.get('display_label')} (source: {resolved.get('primary_source')})")
                elif schema_org_result:
                    # Fallback if resolver not available
                    column.semantic_info = {
                        "structural_type": structural_type,
                        "schema_org": schema_org_result,
                        "fibo": fibo_result,
                        "resolved": {
                            "primary_source": "schema_org",
                            "primary_type": schema_org_result.get("type"),
                            "secondary_type": None,
                            "display_label": schema_org_result.get("display_label", "Unknown"),
                            "validation_driver": "schema_org"
                        }
                    }
                else:
                    # Legacy FIBO-only mode
                    if fibo_result:
                        column.semantic_info = {
                            "structural_type": structural_type,
                            "schema_org": None,
                            "fibo": fibo_result,
                            "resolved": {
                                "primary_source": "fibo",
                                "primary_type": fibo_result.get("type"),
                                "secondary_type": None,
                                "display_label": fibo_result.get("type", "Unknown"),
                                "validation_driver": "fibo"
                            }
                        }

            except Exception as e:
                logger.warning(f"Semantic tagging failed for column {column.name}: {e}")

        phase_timings['semantic_tagging'] = time.time() - semantic_start
        logger.debug(f"⏱  Semantic tagging completed in {phase_timings['semantic_tagging']:.2f}s")

        # Calculate correlations (using extracted StatisticsCalculator)
        correlation_start = time.time()
        correlations = self.stats_calculator.calculate_correlations(numeric_data, row_count)
        phase_timings['basic_correlation'] = time.time() - correlation_start

        # Phase 1: Calculate enhanced correlations
        enhanced_correlations = None
        if self.enable_enhanced_correlation and len(numeric_data) >= 2:
            enhanced_corr_start = time.time()
            logger.debug("Running enhanced correlation analysis...")
            try:
                enhanced_correlations = self.enhanced_correlation_analyzer.calculate_correlations_multi_method(
                    numeric_data,
                    row_count=row_count,
                    methods=['pearson', 'spearman']
                )
                # Update correlations list with enhanced correlation info
                for pair in enhanced_correlations.get('correlation_pairs', []):
                    correlations.append(CorrelationResult(
                        column1=pair['column1'],
                        column2=pair['column2'],
                        correlation=pair['correlation'],
                        type=pair['method'],
                        strength=pair.get('strength'),
                        direction=pair.get('direction'),
                        p_value=pair.get('p_value'),
                        is_significant=pair.get('is_significant')
                    ))
                logger.debug(f"Enhanced correlation analysis found {len(enhanced_correlations.get('correlation_pairs', []))} significant correlations")
            except Exception as e:
                logger.warning(f"Enhanced correlation analysis failed: {e}")

        # Phase 1: Calculate dataset-level privacy risk
        dataset_privacy_risk = None
        if self.enable_pii_detection and pii_columns:
            logger.debug("Calculating dataset-level privacy risk...")
            try:
                dataset_privacy_risk = self.pii_detector.calculate_dataset_privacy_risk(
                    pii_columns=pii_columns,
                    total_columns=len(columns),
                    total_rows=row_count
                )
                logger.debug(f"Dataset privacy risk: {dataset_privacy_risk.get('risk_level', 'unknown').upper()} ({dataset_privacy_risk.get('risk_score', 0)}/100)")
            except Exception as e:
                logger.warning(f"Dataset privacy risk calculation failed: {e}")

        # Generate validation suggestions (using extracted ValidationSuggestionGenerator)
        # Use actual_total_rows for suggestions to get correct row count range (not sampled count)
        suggestions_start = time.time()
        actual_rows_for_suggestions = actual_total_rows if actual_total_rows else row_count
        suggested_validations = self.validation_suggester.generate_suggestions(
            columns, actual_rows_for_suggestions,
            enable_ml_suggestions=self.enable_ml_analysis,
            ml_analyzer=self.ml_analyzer
        )
        phase_timings['generate_suggestions'] = time.time() - suggestions_start

        # Calculate overall quality score
        quality_start = time.time()
        overall_quality = self._calculate_overall_quality(columns)
        phase_timings['quality_score'] = time.time() - quality_start

        # Generate validation configuration
        # Use actual_total_rows for config to get correct row count reference (not sampled count)
        config_start = time.time()
        config_yaml, config_command = self._generate_validation_config(
            file_name, file_path, file_format, file_size, actual_rows_for_suggestions, columns, suggested_validations
        )
        phase_timings['generate_config'] = time.time() - config_start

        # Phase 3: ML-based Anomaly Detection (Beta)
        ml_findings = None
        categorical_analysis = None  # Phase 4: Categorical analysis
        skip_ml_analysis = False
        if self.enable_ml_analysis and self.ml_analyzer:
            # Memory check before ML analysis
            try:
                system_memory = psutil.virtual_memory()
                if system_memory.percent >= self.memory_critical_threshold:
                    logger.warning(f"⚠️  Skipping ML analysis: Memory usage {system_memory.percent:.1f}% exceeds threshold {self.memory_critical_threshold}%")
                    logger.warning(f"⚠️  Profiling will continue without ML anomaly detection")
                    skip_ml_analysis = True
                else:
                    logger.debug(f"💾 Memory check before ML: {system_memory.percent:.1f}% (threshold: {self.memory_critical_threshold}%)")
            except (psutil.Error, OSError) as e:
                logger.debug(f"Could not check system memory: {e}")

        if self.enable_ml_analysis and self.ml_analyzer and not skip_ml_analysis:
            ml_start = time.time()
            logger.debug("🧠 Running ML-based anomaly detection (Beta)...")
            try:
                # Check if we used chunked ML accumulation (full_analysis mode)
                # Use actual_total_rows for correct original count display (not sampled row_count)
                true_original_rows = actual_total_rows if actual_total_rows else row_count
                if ml_accumulator is not None:
                    # Full analysis: finalize accumulated stats from all chunks
                    logger.info(f"📊 Finalizing ML analysis from {true_original_rows:,} rows (chunked processing)")
                    ml_findings = ml_accumulator.finalize(self.ml_analyzer)
                    # Update sample_info with actual original row count (accumulator only sees sampled rows)
                    if 'sample_info' in ml_findings:
                        ml_findings['sample_info']['original_rows'] = true_original_rows
                        ml_findings['sample_info']['sampled'] = ml_findings['sample_info']['analyzed_rows'] < true_original_rows
                        if true_original_rows > 0:
                            ml_findings['sample_info']['sample_percentage'] = round(
                                ml_findings['sample_info']['analyzed_rows'] / true_original_rows * 100, 2
                            )
                else:
                    # Standard mode: sample data and run ML analysis
                    # Note: Individual ML algorithms (clustering, etc.) have their own internal limits
                    # to prevent memory issues, so we can allow larger samples here
                    ml_sample_size = min(ANALYSIS_SAMPLE_SIZE, row_count)

                    if file_format == 'parquet':
                        # Parquet: MEMORY-EFFICIENT sampling using row groups
                        # CRITICAL: Do NOT load entire file - use row group sampling
                        import pyarrow.parquet as pq
                        parquet_file = pq.ParquetFile(file_path)
                        total_rows = parquet_file.metadata.num_rows
                        num_row_groups = parquet_file.metadata.num_row_groups

                        if total_rows <= ml_sample_size:
                            # Small file - safe to load entirely
                            ml_df = parquet_file.read().to_pandas()
                        else:
                            # Large file - sample from random row groups
                            # Calculate how many row groups to read to get ~ml_sample_size rows
                            avg_rows_per_group = total_rows / num_row_groups
                            groups_needed = max(1, int(ml_sample_size / avg_rows_per_group) + 1)
                            groups_to_read = min(groups_needed, num_row_groups)

                            # Randomly select row groups
                            selected_groups = np.random.choice(num_row_groups, groups_to_read, replace=False)
                            selected_groups = np.sort(selected_groups)

                            # Read only selected row groups
                            tables = [parquet_file.read_row_group(i) for i in selected_groups]
                            import pyarrow as pa
                            combined_table = pa.concat_tables(tables)

                            # Sample from the loaded data if still too large
                            if combined_table.num_rows > ml_sample_size:
                                indices = np.random.choice(combined_table.num_rows, ml_sample_size, replace=False)
                                indices = np.sort(indices)
                                ml_df = combined_table.take(indices).to_pandas()
                            else:
                                ml_df = combined_table.to_pandas()

                            # Clean up intermediate tables
                            del tables, combined_table
                            gc.collect()

                        logger.debug(f"💾 ML sampling: loaded {len(ml_df):,} rows from {groups_to_read if total_rows > ml_sample_size else num_row_groups} row groups (vs {total_rows:,} total)")
                    else:
                        # CSV/other: read with nrows limit
                        # Use delimiter from loader_kwargs if provided (for pipe-delimited, tab-delimited, etc.)
                        csv_kwargs = {'nrows': ml_sample_size}
                        if 'delimiter' in loader_kwargs:
                            csv_kwargs['delimiter'] = loader_kwargs['delimiter']
                        if 'encoding' in loader_kwargs:
                            csv_kwargs['encoding'] = loader_kwargs['encoding']
                        ml_df = pd.read_csv(file_path, **csv_kwargs)

                    # Build semantic info from columns for intelligent analysis
                    column_semantic_info = {}
                    for col in columns:
                        if hasattr(col, 'semantic_info') and col.semantic_info:
                            column_semantic_info[col.name] = col.semantic_info

                    # Run ML analysis with semantic context
                    ml_findings = self.ml_analyzer.analyze(ml_df, column_semantic_info=column_semantic_info)

                    # Update sample_info with actual original row count (ML only sees sampled rows)
                    if 'sample_info' in ml_findings:
                        ml_findings['sample_info']['original_rows'] = true_original_rows
                        ml_findings['sample_info']['sampled'] = ml_findings['sample_info'].get('analyzed_rows', len(ml_df)) < true_original_rows
                        if true_original_rows > 0:
                            analyzed = ml_findings['sample_info'].get('analyzed_rows', len(ml_df))
                            ml_findings['sample_info']['sample_percentage'] = round(
                                analyzed / true_original_rows * 100, 2
                            )

                    # ═══════════════════════════════════════════════════════════════
                    # CONTEXT-AWARE ANOMALY VALIDATION
                    # Validate outliers against discovered patterns to reduce false positives
                    # Must run before ml_df cleanup!
                    # ═══════════════════════════════════════════════════════════════
                    if ml_findings.get('numeric_outliers') or ml_findings.get('outliers'):
                        context_start = time.time()
                        try:
                            from validation_framework.profiler.contextual_validator import validate_outliers_with_context

                            # Get outliers from ml_findings
                            outliers = ml_findings.get('numeric_outliers', {}) or ml_findings.get('outliers', {})

                            # Run context-aware validation using ml_df (still in memory)
                            context_result, context_store = validate_outliers_with_context(
                                outliers, ml_df,
                                field_descriptions=self.field_descriptions
                            )

                            # Add context validation results to ml_findings
                            ml_findings['context_validation'] = context_result.to_dict()
                            ml_findings['context_store'] = context_store.to_dict()

                            # Log results
                            if context_result.total_explained > 0:
                                reduction = (context_result.total_original - context_result.total_validated) / context_result.total_original * 100
                                logger.debug(f"🎯 Context validation: {context_result.total_original} → {context_result.total_validated} outliers ({reduction:.0f}% reduction)")

                            phase_timings['context_validation'] = time.time() - context_start
                        except Exception as e:
                            logger.debug(f"Context validation failed: {e}")

                    # Synthesize correlation insights (while ml_df is still available)
                    if CORRELATION_INSIGHT_AVAILABLE and correlations:
                        try:
                            insight_start = time.time()
                            synthesizer = CorrelationInsightSynthesizer(
                                ml_df,
                                field_descriptions=self.field_descriptions
                            )

                            # Convert CorrelationResult objects to dicts
                            corr_dicts = []
                            for c in correlations:
                                if isinstance(c, CorrelationResult):
                                    corr_dicts.append({
                                        'column1': c.column1,
                                        'column2': c.column2,
                                        'correlation': c.correlation,
                                        'type': c.type,
                                        'p_value': c.p_value,
                                        'strength': c.strength,
                                        'direction': c.direction
                                    })
                                else:
                                    corr_dicts.append(c)

                            # Get subgroups from context_store if available
                            subgroups = []
                            if ml_findings and 'context_store' in ml_findings:
                                subgroups = ml_findings['context_store'].get('subgroups', [])

                            # Synthesize insights
                            insights = synthesizer.synthesize_all(corr_dicts, subgroups)

                            # Store in ml_findings for the reporter
                            ml_findings['correlation_insights'] = synthesizer.to_dict_list()
                            logger.debug(f"💡 Synthesized {len(insights)} correlation insights")

                            phase_timings['correlation_insights'] = time.time() - insight_start
                        except Exception as e:
                            logger.debug(f"Correlation insight synthesis failed: {e}")

                    # ═══════════════════════════════════════════════════════════════
                    # PHASE 4: CATEGORICAL ASSOCIATION ANALYSIS
                    # Analyze relationships involving categorical columns
                    # (Cramér's V, point-biserial, target detection, missing patterns)
                    # Must run before ml_df cleanup!
                    # ═══════════════════════════════════════════════════════════════
                    if self.enable_categorical_analysis and self.categorical_analyzer:
                        cat_start = time.time()
                        try:
                            # Build column types dict from profiles
                            column_types = {}
                            for col in columns:
                                if hasattr(col, 'type_info') and col.type_info:
                                    column_types[col.name] = col.type_info.inferred_type

                            categorical_analysis = self.categorical_analyzer.analyze_categorical_associations(
                                ml_df,
                                column_types
                            )

                            # Log results
                            cramers_v_count = len(categorical_analysis.get('cramers_v_associations', []))
                            point_biserial_count = len(categorical_analysis.get('point_biserial_associations', []))
                            target_count = len(categorical_analysis.get('target_columns', []))
                            missing_patterns = len(categorical_analysis.get('missing_patterns', []))

                            if cramers_v_count + point_biserial_count + target_count > 0:
                                logger.debug(f"📊 Categorical analysis: {cramers_v_count} categorical associations, "
                                           f"{point_biserial_count} binary-numeric correlations, "
                                           f"{target_count} target candidates, "
                                           f"{missing_patterns} missing patterns")

                            phase_timings['categorical_analysis'] = time.time() - cat_start
                        except Exception as e:
                            logger.debug(f"Categorical analysis failed: {e}")
                            categorical_analysis = None
                    else:
                        categorical_analysis = None

                    # Clean up ml_df after context validation
                    del ml_df
                    gc.collect()

                phase_timings['ml_analysis'] = time.time() - ml_start
                logger.debug(f"🧠 ML analysis complete: {ml_findings.get('summary', {}).get('total_issues', 0)} potential issues found")

            except Exception as e:
                logger.warning(f"ML analysis failed: {e}")
                import traceback
                logger.debug(traceback.format_exc())

        # ═══════════════════════════════════════════════════════════════
        # SMART VALIDATION RECOMMENDATIONS FROM ML FINDINGS
        # Now that ML analysis is complete, generate ML-based suggestions
        # and merge them into the validation recommendations
        # ═══════════════════════════════════════════════════════════════
        if ml_findings:
            ml_suggestions_start = time.time()
            try:
                ml_based_suggestions = self._generate_ml_based_validations(ml_findings, columns)
                if ml_based_suggestions:
                    logger.debug(f"💡 Generated {len(ml_based_suggestions)} ML-based validation suggestions")

                    # Merge ML suggestions with existing suggestions
                    suggested_validations.extend(ml_based_suggestions)

                    # Re-apply deduplication after adding ML suggestions
                    suggested_validations = self._deduplicate_validations_by_field(suggested_validations)

                    # Re-sort by confidence
                    suggested_validations = sorted(suggested_validations, key=lambda x: x.confidence, reverse=True)

                    # Regenerate YAML config with ML-enhanced suggestions
                    config_yaml, config_command = self._generate_validation_config(
                        file_name, file_path, file_format, file_size, row_count, columns, suggested_validations
                    )
                    logger.debug(f"📝 Updated validation config with ML-based suggestions")

                phase_timings['ml_suggestions'] = time.time() - ml_suggestions_start
            except Exception as e:
                logger.warning(f"ML-based suggestion generation failed: {e}")

        processing_time = time.time() - start_time
        logger.debug(f"⏱  Profile completed in {processing_time:.2f} seconds")

        # Log timing breakdown
        logger.debug("⏱  Timing breakdown:")
        for phase, duration in phase_timings.items():
            percentage = (duration / processing_time * 100) if processing_time > 0 else 0
            logger.debug(f"   {phase}: {duration:.2f}s ({percentage:.1f}%)")

        # Use actual total rows from parquet metadata if available (vs sampled row_count)
        final_row_count = actual_total_rows if actual_total_rows else row_count

        # Merge parquet metadata stats into column profiles for accurate counts
        if parquet_column_stats:
            for col in columns:
                pq_stats = parquet_column_stats['columns'].get(col.name)
                if pq_stats and pq_stats['has_stats']:
                    # Update with accurate null count from metadata
                    col.statistics.null_count = pq_stats['null_count']
                    col.statistics.null_percentage = (pq_stats['null_count'] / final_row_count * 100) if final_row_count > 0 else 0.0
                    col.statistics.count = final_row_count

                    # Update min/max from parquet metadata - this is ALWAYS more accurate than sampled values
                    # because parquet metadata is aggregated from ALL row groups, not just the sample
                    # For large files with extreme values in sparse row groups, this is critical
                    # Note: Only compare if types are compatible (both numeric or both string)
                    pq_min = pq_stats['min']
                    pq_max = pq_stats['max']

                    if pq_min is not None:
                        if col.statistics.min_value is None:
                            col.statistics.min_value = pq_min
                        elif isinstance(pq_min, (int, float)) and isinstance(col.statistics.min_value, (int, float)):
                            # For numeric columns, use the smaller value (true min)
                            if pq_min < col.statistics.min_value:
                                col.statistics.min_value = pq_min
                        elif isinstance(pq_min, str) and isinstance(col.statistics.min_value, str):
                            # For string columns, use lexicographically smaller value
                            if pq_min < col.statistics.min_value:
                                col.statistics.min_value = pq_min

                    if pq_max is not None:
                        if col.statistics.max_value is None:
                            col.statistics.max_value = pq_max
                        elif isinstance(pq_max, (int, float)) and isinstance(col.statistics.max_value, (int, float)):
                            # For numeric columns, use the larger value (true max)
                            if pq_max > col.statistics.max_value:
                                col.statistics.max_value = pq_max
                        elif isinstance(pq_max, str) and isinstance(col.statistics.max_value, str):
                            # For string columns, use lexicographically larger value
                            if pq_max > col.statistics.max_value:
                                col.statistics.max_value = pq_max

            # Add sampling info to metadata
            file_metadata['sampled_rows'] = row_count
            file_metadata['sampling_note'] = f"Statistics from metadata ({final_row_count:,} rows), detailed analysis from {row_count:,} row sample"

        # Build list of analysis types applied for lineage tracking
        analysis_applied = ["basic_statistics", "type_inference", "pattern_detection"]
        if self.enable_temporal_analysis:
            analysis_applied.append("temporal_analysis")
        if self.enable_pii_detection:
            analysis_applied.append("pii_detection")
        if self.enable_ml_analysis:
            analysis_applied.append("ml_analysis")
        if self.enable_semantic_tagging:
            analysis_applied.append("semantic_classification")
        if correlations:
            analysis_applied.append("correlation_analysis")
        if self.enable_enhanced_correlation and enhanced_correlations:
            analysis_applied.append("enhanced_correlation_analysis")
        if self.enable_categorical_analysis and categorical_analysis:
            analysis_applied.append("categorical_association_analysis")

        # Build sampling info for lineage
        sampling_info = None
        if actual_total_rows and row_count < actual_total_rows:
            sampling_info = {
                "sampling_applied": True,
                "total_rows": actual_total_rows,
                "sampled_rows": row_count,
                "sampling_percentage": round((row_count / actual_total_rows) * 100, 2),
                "sampling_strategy": "intelligent",
                "analysis_sample_size": self.analysis_sample_size
            }

        # Create data lineage for provenance tracking
        data_lineage = self._create_data_lineage(
            file_path=file_path,
            file_size_bytes=file_size,
            row_count=final_row_count,
            analysis_applied=analysis_applied,
            sampling_info=sampling_info
        )

        return ProfileResult(
            file_name=file_name,
            file_path=file_path,
            file_size_bytes=file_size,
            format=file_format,
            row_count=final_row_count,
            column_count=len(columns),
            profiled_at=datetime.now(),
            processing_time_seconds=processing_time,
            columns=columns,
            correlations=correlations,
            suggested_validations=suggested_validations,
            overall_quality_score=overall_quality,
            generated_config_yaml=config_yaml,
            generated_config_command=config_command,
            enhanced_correlations=enhanced_correlations,
            dataset_privacy_risk=dataset_privacy_risk,
            file_metadata=file_metadata if file_metadata else None,
            ml_findings=ml_findings,
            data_lineage=data_lineage,
            csv_format_issues=csv_format_check if csv_format_check and not csv_format_check['valid'] else None,
            categorical_analysis=categorical_analysis
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
            "type_sampled_count": 0,  # Track how many rows we sampled for type detection
            "null_count": 0,
            "whitespace_null_count": 0,  # Count of whitespace-only values treated as null
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

        # Treat whitespace-only strings as null
        # Check for string columns and replace whitespace-only values with NaN
        if series.dtype == 'object':  # String columns are typically 'object' dtype
            # First, identify values that are NOT already null
            not_null_mask = series.notna()

            # For non-null values, check if they're whitespace-only
            # This avoids converting NaN to string 'nan' and incorrectly counting it
            whitespace_mask = not_null_mask & (series.str.strip() == '')
            whitespace_count = whitespace_mask.sum()

            # Track whitespace nulls for informational reporting
            if "whitespace_null_count" not in profile:
                profile["whitespace_null_count"] = 0
            profile["whitespace_null_count"] += whitespace_count

            # Replace whitespace-only values with NaN
            if whitespace_count > 0:
                series = series.copy()  # Avoid modifying original
                series[whitespace_mask] = np.nan

            # Detect placeholder values that represent missing data
            # Common placeholders: ?, N/A, NA, null, NULL, none, None, -, n/a, unknown, Unknown, NaN, nan
            placeholder_patterns = {'?', 'n/a', 'na', 'null', 'none', '-', 'unknown', 'nan', 'missing', 'undefined', '.', '..', '...', 'n.a.', 'n.a', '#n/a', '#na', 'not available', 'not applicable'}

            # Initialize placeholder tracking
            if "placeholder_null_count" not in profile:
                profile["placeholder_null_count"] = 0
                profile["placeholder_values_found"] = {}

            # Check for placeholders (case-insensitive, stripped)
            still_not_null = series.notna()
            if still_not_null.any():
                stripped_lower = series[still_not_null].str.strip().str.lower()
                placeholder_mask_values = stripped_lower.isin(placeholder_patterns)

                if placeholder_mask_values.any():
                    # Count placeholders
                    placeholder_count = placeholder_mask_values.sum()
                    profile["placeholder_null_count"] += placeholder_count

                    # Track which placeholder values were found
                    found_placeholders = stripped_lower[placeholder_mask_values].value_counts()
                    for val, count in found_placeholders.items():
                        profile["placeholder_values_found"][val] = profile["placeholder_values_found"].get(val, 0) + count

                    # Create full mask for replacement
                    full_placeholder_mask = pd.Series(False, index=series.index)
                    full_placeholder_mask[still_not_null] = placeholder_mask_values.values

                    # Replace placeholders with NaN
                    if not series._is_copy:
                        series = series.copy()
                    series[full_placeholder_mask] = np.nan

        # Count nulls (now includes whitespace-only values)
        null_mask = series.isna()
        profile["null_count"] += null_mask.sum()

        # Process non-null values
        non_null_series = series[~null_mask]

        # Sample values (from first chunk only, limit to 100)
        if chunk_idx == 0 and len(profile["sample_values"]) < 100:
            samples = non_null_series.head(100 - len(profile["sample_values"])).tolist()
            profile["sample_values"].extend(samples)

        # Type detection (sample-based for performance)
        # CRITICAL: Sample for type detection even on first chunk to avoid O(n) iteration
        # A 10K sample provides >99.9% confidence for type inference
        TYPE_DETECTION_SAMPLE_SIZE = 10000

        if chunk_idx == 0:
            # First chunk: sample for type detection (not all values - that's O(n)!)
            sample_size = min(TYPE_DETECTION_SAMPLE_SIZE, len(non_null_series))
            if len(non_null_series) > sample_size:
                type_sample = non_null_series.sample(n=sample_size, random_state=42)
            else:
                type_sample = non_null_series

            # Track unexpected types for debugging (limit to first 10 occurrences)
            unexpected_types_logged = 0
            for value in type_sample:
                detected_type = self.type_inferrer.detect_type(value)
                profile["type_counts"][detected_type] = profile["type_counts"].get(detected_type, 0) + 1

                # Debug logging for type mismatches (first chunk only, limit output)
                # Look for common patterns that might indicate issues
                if unexpected_types_logged < 10:
                    # If column name suggests it should be string but we detect numeric/other
                    col_lower = profile["column_name"].lower()
                    is_account_like = any(keyword in col_lower for keyword in ['account', 'id', 'code', 'reference', 'number'])
                    if is_account_like and detected_type in ['integer', 'float', 'boolean', 'date']:
                        logger.debug(f"Type mismatch in '{profile['column_name']}': value='{value}' -> detected as '{detected_type}' (expected string)")
                        unexpected_types_logged += 1

            profile["type_sampled_count"] += len(type_sample)
        elif chunk_idx % 10 == 0:
            # Every 10th chunk: sample 1000 values for type refinement
            sample_size = min(1000, len(non_null_series))
            # Use pandas .sample() instead of converting to list (memory efficient)
            if len(non_null_series) > sample_size:
                sampled_series = non_null_series.sample(n=sample_size, random_state=42)
            else:
                sampled_series = non_null_series

            for value in sampled_series:
                detected_type = self.type_inferrer.detect_type(value)
                profile["type_counts"][detected_type] = profile["type_counts"].get(detected_type, 0) + 1
            profile["type_sampled_count"] += len(sampled_series)

        # Value frequency (limit to prevent memory issues)
        # Only compute value_counts if we haven't reached the limit
        if len(profile["value_counts"]) < 10000:
            # CRITICAL MEMORY FIX: Always sample before computing value_counts
            # Computing value_counts on 2M rows creates large temporary structures
            # Sample to max 10K rows to prevent memory spikes
            max_sample_size = 10000
            if len(non_null_series) > max_sample_size:
                sample_for_freq = non_null_series.sample(n=max_sample_size, random_state=42)
            else:
                sample_for_freq = non_null_series

            value_freq = sample_for_freq.value_counts()

            for val, count in value_freq.items():
                if len(profile["value_counts"]) >= 10000:
                    break  # Stop if we hit the limit mid-iteration
                # Note: Counts are from sampled data, not exact counts
                # This is acceptable as value_counts is for cardinality estimation
                profile["value_counts"][val] = profile["value_counts"].get(val, 0) + count

        # Numeric analysis (memory-efficient sampling for statistics)
        # Use intelligent sampling based on column semantics
        intelligence = SmartColumnAnalyzer.analyze_column(profile["column_name"])
        MAX_NUMERIC_SAMPLES = intelligence.recommended_sample_size

        # Log intelligent sampling decision on first chunk
        if chunk_idx == 0 and intelligence.semantic_type != 'unknown':
            logger.debug(f"🧠 Intelligent sampling for '{profile['column_name']}': {intelligence.semantic_type} → {MAX_NUMERIC_SAMPLES:,} samples ({intelligence.reasoning})")

        numeric_series = pd.to_numeric(non_null_series, errors='coerce').dropna()
        if len(numeric_series) > 0:
            current_count = len(profile["numeric_values"])
            if current_count < MAX_NUMERIC_SAMPLES:
                samples_needed = MAX_NUMERIC_SAMPLES - current_count
                if len(numeric_series) > samples_needed:
                    # Use pandas .sample() instead of random.sample(tolist()) for memory efficiency
                    sampled = numeric_series.sample(n=samples_needed, random_state=42)
                    profile["numeric_values"].extend(sampled.tolist())
                else:
                    profile["numeric_values"].extend(numeric_series.tolist())

        # String analysis (memory-efficient sampling for length statistics)
        # Use intelligent sampling based on column semantics (reuse intelligence from above)
        MAX_STRING_LENGTH_SAMPLES = intelligence.recommended_sample_size

        string_series = non_null_series.astype(str)
        lengths = string_series.str.len()
        current_count = len(profile["string_lengths"])
        if current_count < MAX_STRING_LENGTH_SAMPLES:
            samples_needed = MAX_STRING_LENGTH_SAMPLES - current_count
            # Use pandas .sample() instead of tolist() for memory efficiency
            if len(lengths) > samples_needed:
                sampled_lengths = lengths.sample(n=samples_needed, random_state=42)
                profile["string_lengths"].extend(sampled_lengths.tolist())
            else:
                profile["string_lengths"].extend(lengths.tolist())

        # Pattern detection (sample only)
        if chunk_idx == 0:
            for val in non_null_series.head(100):
                pattern = self.type_inferrer.extract_pattern(str(val))
                profile["patterns"][pattern] = profile["patterns"].get(pattern, 0) + 1

    # =========================================================================
    # COLUMN PROFILE FINALIZATION
    # Note: Type inference, statistics, and quality metrics are now delegated
    # to extracted classes (TypeInferrer, StatisticsCalculator) for better
    # maintainability and testability.
    # =========================================================================

    def _finalize_column_profile(
        self,
        col_name: str,
        profile_data: Dict[str, Any],
        total_rows: int
    ) -> ColumnProfile:
        """Finalize column profile after processing all chunks."""

        # Determine inferred type and confidence (using extracted TypeInferrer)
        type_info = self.type_inferrer.infer_column_type(profile_data, total_rows)

        # Calculate statistics (using extracted StatisticsCalculator)
        statistics = self.stats_calculator.calculate_statistics(profile_data, total_rows)

        # Calculate quality metrics (using extracted StatisticsCalculator)
        quality = self.stats_calculator.calculate_quality_metrics(profile_data, type_info, statistics, total_rows)

        return ColumnProfile(
            name=col_name,
            type_info=type_info,
            statistics=statistics,
            quality=quality
        )

    # =========================================================================
    # VALIDATION SUGGESTION HELPERS
    # These methods support ML-based validation suggestions and are still
    # actively used by the profiling workflow.
    # =========================================================================

    def _should_suggest_range_check(self, col: ColumnProfile, row_count: int) -> bool:
        """
        Smart pattern-based detection: Should this field have a RangeCheck validation?

        Uses semantic type detection (from visions or name-based) combined with
        data characteristics and pattern analysis.

        Returns False for:
        - Identifiers (IDs, keys, codes)
        - Categories (low cardinality enums)
        - Amounts/prices/money (unbounded by nature)
        - Boolean flags
        - Semantic types: URLs, Emails, UUIDs, Phone numbers, IP addresses

        Returns True for:
        - Measurements with natural bounds (age, percentage, count)
        - Metrics with expected ranges
        """
        col_name_lower = col.name.lower()

        # 1. Check semantic type (already set by visions or SmartColumnAnalyzer)
        semantic_type = getattr(col.statistics, 'semantic_type', None)

        # Semantic types that should NOT have range checks
        non_range_semantic_types = [
            'id', 'identifier', 'key', 'category',
            'email', 'url', 'uuid', 'ip_address', 'phone_number'
        ]

        if semantic_type in non_range_semantic_types:
            logger.debug(f"Semantic type '{semantic_type}' detected for {col.name} - excluding from RangeCheck")
            return False

        # 2. Detect amount/money/price fields - these are UNBOUNDED and should NOT have range checks
        # Financial amounts can grow infinitely and historical max is not a valid upper bound
        amount_keywords = ['amount', 'price', 'cost', 'fee', 'payment', 'balance',
                          'total', 'sum', 'value', 'paid', 'received', 'revenue',
                          'income', 'expense', 'transaction', 'salary', 'wage']

        is_amount_field = any(kw in col_name_lower for kw in amount_keywords)

        if is_amount_field and col.type_info.inferred_type in ['float', 'integer']:
            # Amount fields should use non-negative check, not range check
            return False

        # 3. Boolean/flag fields (only 2 unique values) - use ValidValuesCheck instead
        if col.statistics.unique_count == 2:
            return False

        # 4. High cardinality (>80% unique) = likely identifier, not measurement
        if col.statistics.cardinality > 0.8:
            return False

        # 5. Low cardinality (<5%, <20 values) = categorical, will get ValidValuesCheck
        if col.statistics.cardinality < 0.05 and col.statistics.unique_count < 20:
            return False

        # 6. Check if numeric values look like IDs (large sparse range)
        if col.statistics.min_value is not None and col.statistics.max_value is not None:
            value_range = col.statistics.max_value - col.statistics.min_value

            # If range is >> row count, likely sequential/sparse IDs
            if row_count > 0 and value_range > row_count * 10:
                return False

            # If min is very large (like timestamps or large IDs), probably not a measurement
            if col.type_info.inferred_type == "integer" and col.statistics.min_value > 100000:
                # Exception: if it looks like a timestamp or very large ID
                if col.statistics.min_value > 1000000000:  # Unix timestamp range or large ID
                    return False

        # 7. Column name hint for identifiers
        # Strong indicators - no cardinality check needed (these ARE IDs)
        strong_id_indicators = ['_id', ' id', 'from bank', 'to bank', 'account', 'acct',
                                'reference', 'ref_', 'ref ', 'transaction id', 'txn_id']
        is_strong_id = any(kw in col_name_lower for kw in strong_id_indicators)

        if is_strong_id:
            logger.debug(f"Strong ID indicator detected for {col.name} - excluding from RangeCheck")
            return False

        # Weak indicators - require cardinality check to confirm
        weak_id_indicators = ['id', 'key', 'code', 'number', 'bank']
        name_suggests_id = any(kw in col_name_lower for kw in weak_id_indicators)

        # If name suggests ID AND cardinality is moderate-high, probably an identifier
        if name_suggests_id and col.statistics.cardinality > 0.3:  # Lowered from 0.5 to 0.3
            return False

        # Default: suggest range check for numeric measurements with natural bounds
        return True

    def _calculate_smart_range(
        self,
        col_name: str,
        observed_min: float,
        observed_max: float,
        schema_org_type: Optional[str]
    ) -> tuple:
        """
        Calculate semantically-aware range boundaries for validation.

        Instead of using exact observed min/max (which is too restrictive for new data),
        this method applies domain knowledge to suggest sensible ranges that will work
        across datasets.

        Args:
            col_name: Column name for pattern matching
            observed_min: Minimum value seen in data
            observed_max: Maximum value seen in data
            schema_org_type: Schema.org semantic type (e.g., 'schema:Integer', 'schema:Number')

        Returns:
            Tuple of (min_value, max_value, reason_string)
        """
        col_name_lower = col_name.lower()

        # Age-related fields: use human-sensible bounds
        age_keywords = ['age', 'years_old', 'yearsold', 'person_age']
        if any(kw in col_name_lower for kw in age_keywords):
            # Human age: 0-120 is sensible regardless of observed range
            return (0, 120, "Age field with human-sensible bounds (0-120 years)")

        # Percentage fields: 0-100 bounds
        pct_keywords = ['percent', 'pct', 'percentage', 'rate', 'ratio']
        if any(kw in col_name_lower for kw in pct_keywords):
            # Check if observed values suggest 0-1 scale vs 0-100 scale
            if observed_max <= 1.0:
                return (0.0, 1.0, "Percentage field (0-1 scale)")
            else:
                return (0, 100, "Percentage field (0-100 scale)")

        # Count fields: non-negative with generous upper bound
        count_keywords = ['count', 'num_', 'number_of', 'qty', 'quantity']
        if any(kw in col_name_lower for kw in count_keywords):
            # Use 0 as min, but expand max by 50% to allow for growth
            expanded_max = int(observed_max * 1.5) if observed_max > 0 else 100
            return (0, expanded_max, f"Count field (non-negative, max expanded to {expanded_max})")

        # Fare/price/monetary fields: non-negative only (no upper bound)
        monetary_keywords = ['fare', 'price', 'cost', 'fee', 'amount', 'salary', 'payment']
        if any(kw in col_name_lower for kw in monetary_keywords):
            return (0, None, "Monetary field (must be non-negative)")

        # Score/rating fields: common scales
        score_keywords = ['score', 'rating', 'grade', 'rank']
        if any(kw in col_name_lower for kw in score_keywords):
            # Detect common scales
            if observed_max <= 5:
                return (0, 5, "Score field (0-5 scale)")
            elif observed_max <= 10:
                return (0, 10, "Score field (0-10 scale)")
            elif observed_max <= 100:
                return (0, 100, "Score field (0-100 scale)")

        # Class/category numeric fields (like Pclass in Titanic): use observed as valid values
        class_keywords = ['class', 'category', 'type', 'level', 'tier']
        if any(kw in col_name_lower for kw in class_keywords):
            # For categorical numerics, use exact observed range
            return (int(observed_min), int(observed_max),
                    f"Categorical field (observed values {int(observed_min)}-{int(observed_max)})")

        # Default behavior: expand observed range by 20% on each side
        # This allows for natural variation without being overly restrictive
        range_span = observed_max - observed_min
        if range_span > 0:
            expansion = range_span * 0.2
            suggested_min = observed_min - expansion
            suggested_max = observed_max + expansion

            # Don't allow negative min if observed min was non-negative
            if observed_min >= 0:
                suggested_min = max(0, suggested_min)

            # Round for cleaner values
            if isinstance(observed_min, int) and isinstance(observed_max, int):
                suggested_min = int(suggested_min)
                suggested_max = int(suggested_max)
            else:
                suggested_min = round(suggested_min, 2)
                suggested_max = round(suggested_max, 2)

            return (suggested_min, suggested_max,
                    f"Range based on observed data ({observed_min}-{observed_max}) with 20% margin")
        else:
            # Single value case - use exact value
            return (observed_min, observed_max, f"Single observed value: {observed_min}")

    def _generate_fibo_semantic_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """
        Generate validation suggestions based on FIBO semantic tags.

        Phase 2: Uses semantic_tagger to get recommended validations from taxonomy.

        Args:
            col: ColumnProfile with semantic_info

        Returns:
            List of validation suggestions from FIBO taxonomy
        """
        suggestions = []

        if not col.semantic_info or not self.semantic_tagger:
            return suggestions

        # Get primary semantic tag
        primary_tag = col.semantic_info.get('primary_tag', 'unknown')
        if primary_tag == 'unknown':
            return suggestions

        # Get recommended validation rules from taxonomy
        validation_rules = self.semantic_tagger.get_validation_rules(primary_tag)
        skip_validations = self.semantic_tagger.get_skip_validations(primary_tag)

        logger.debug(f"FIBO validations for '{col.name}' ({primary_tag}): {validation_rules}")

        # Generate validation suggestions based on taxonomy rules
        for rule_type in validation_rules:
            # Skip validations that taxonomy says to skip
            if rule_type in skip_validations:
                continue

            # Generate appropriate parameters based on rule type
            if rule_type == "NonNegativeCheck":
                suggestions.append(ValidationSuggestion(
                    validation_type="RangeCheck",
                    severity="WARNING",
                    params={
                        "field": col.name,
                        "min_value": 0
                    },
                    reason=f"FIBO: {primary_tag} must be non-negative",
                    confidence=95.0
                ))

            elif rule_type == "MandatoryFieldCheck":
                # Only suggest if completeness is high
                if col.quality.completeness > 95:
                    suggestions.append(ValidationSuggestion(
                        validation_type="MandatoryFieldCheck",
                        severity="WARNING",
                        params={
                            "fields": [col.name]
                        },
                        reason=f"FIBO: {primary_tag} is typically mandatory",
                        confidence=90.0
                    ))

            elif rule_type == "UniqueKeyCheck":
                # Only suggest if cardinality is high
                if col.statistics.cardinality > 0.90:
                    suggestions.append(ValidationSuggestion(
                        validation_type="UniqueKeyCheck",
                        severity="WARNING",
                        params={
                            "fields": [col.name]
                        },
                        reason=f"FIBO: {primary_tag} should be unique identifier",
                        confidence=95.0
                    ))

            elif rule_type == "StringLengthCheck":
                # Use detected string lengths
                if hasattr(col.statistics, 'min_length') and hasattr(col.statistics, 'max_length'):
                    min_len = col.statistics.min_length
                    max_len = col.statistics.max_length
                    if min_len is not None and max_len is not None:
                        suggestions.append(ValidationSuggestion(
                            validation_type="StringLengthCheck",
                            severity="WARNING",
                            params={
                                "field": col.name,
                                "min_length": min_len,
                                "max_length": max_len
                            },
                            reason=f"FIBO: {primary_tag} has expected length range",
                            confidence=85.0
                        ))

            elif rule_type == "ValidValuesCheck":
                # For low cardinality fields, suggest valid values
                if col.statistics.cardinality < 0.05 and col.statistics.unique_count < 20:
                    valid_values = [item["value"] for item in col.statistics.top_values]
                    # Only suggest if we actually have valid values to check
                    if valid_values:
                        suggestions.append(ValidationSuggestion(
                            validation_type="ValidValuesCheck",
                            severity="WARNING",
                            params={
                                "field": col.name,
                                "valid_values": valid_values
                            },
                            reason=f"FIBO: {primary_tag} has limited valid values",
                            confidence=90.0
                        ))

            elif rule_type == "OutlierDetectionCheck":
                # Suggest outlier detection for monetary amounts, etc.
                suggestions.append(ValidationSuggestion(
                    validation_type="StatisticalOutlierCheck",
                    severity="WARNING",
                    params={
                        "field": col.name,
                        "method": "zscore",
                        "threshold": 3.0
                    },
                    reason=f"FIBO: {primary_tag} should be monitored for outliers",
                    confidence=75.0
                ))

            elif rule_type == "DateFormatCheck":
                # For temporal fields
                suggestions.append(ValidationSuggestion(
                    validation_type="DateFormatCheck",
                    severity="WARNING",
                    params={
                        "field": col.name
                    },
                    reason=f"FIBO: {primary_tag} must be valid date format",
                    confidence=90.0
                ))

            elif rule_type == "RangeCheck":
                # General range check (use detected min/max)
                if col.statistics.min_value is not None and col.statistics.max_value is not None:
                    suggestions.append(ValidationSuggestion(
                        validation_type="RangeCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "min_value": col.statistics.min_value,
                            "max_value": col.statistics.max_value
                        },
                        reason=f"FIBO: {primary_tag} observed range",
                        confidence=80.0
                    ))

        return suggestions

    def _generate_semantic_type_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """
        Generate validation suggestions based on detected semantic types.

        Phase 2: Enhanced with FIBO-based semantic tagging.
        - Checks FIBO semantic tags first (if available)
        - Falls back to old semantic_type detection

        Returns appropriate validations for each semantic type:
        - email → RegexCheck
        - url → RegexCheck
        - uuid → RegexCheck
        - ip_address → RegexCheck
        - phone_number → StringLengthCheck
        - amount → Non-negative check (min_value=0)
        - count → Non-negative check (min_value=0)
        - date/datetime → DateFormatCheck
        """
        suggestions = []

        # Phase 2: Check FIBO semantic tags first
        if col.semantic_info and self.enable_semantic_tagging and self.semantic_tagger:
            fibo_suggestions = self._generate_fibo_semantic_validations(col)
            suggestions.extend(fibo_suggestions)
            # If FIBO provided suggestions, we can return early
            if fibo_suggestions:
                return suggestions

        # Fallback to old semantic_type detection
        semantic_type = getattr(col.statistics, 'semantic_type', None)

        if not semantic_type or semantic_type == 'unknown':
            return suggestions

        # Email address validation
        if semantic_type == 'email':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                },
                reason="Email address format validation",
                confidence=90.0
            ))

        # URL validation
        elif semantic_type == 'url':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r'^https?://[^\s/$.?#].[^\s]*$'
                },
                reason="URL format validation",
                confidence=90.0
            ))

        # UUID validation
        elif semantic_type == 'uuid':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                },
                reason="UUID format validation (standard 8-4-4-4-12 format)",
                confidence=95.0
            ))

        # IP Address validation
        elif semantic_type == 'ip_address':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$'
                },
                reason="IPv4 address format validation",
                confidence=90.0
            ))

        # Phone number validation
        elif semantic_type == 'phone_number':
            suggestions.append(ValidationSuggestion(
                validation_type="StringLengthCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "min_length": 10,
                    "max_length": 20
                },
                reason="Phone number length validation",
                confidence=85.0
            ))

        # Amount fields - suggest non-negative check (NOT range check)
        elif semantic_type == 'amount':
            if col.statistics.min_value is not None:
                suggestions.append(ValidationSuggestion(
                    validation_type="RangeCheck",
                    severity="WARNING",
                    params={
                        "field": col.name,
                        "min_value": 0  # Amounts should be non-negative
                        # NO max_value - amounts are unbounded!
                    },
                    reason="Amount fields must be non-negative (no upper bound)",
                    confidence=95.0
                ))

        # Count fields - non-negative integers
        elif semantic_type == 'count':
            suggestions.append(ValidationSuggestion(
                validation_type="RangeCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "min_value": 0  # Counts must be non-negative
                    # NO max_value - counts are unbounded!
                },
                reason="Count fields must be non-negative integers",
                confidence=95.0
            ))

        # File path validation
        elif semantic_type == 'path':
            suggestions.append(ValidationSuggestion(
                validation_type="RegexCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "pattern": r'^[/\\]?[\w\s\-./\\]+$'
                },
                reason="File path format validation",
                confidence=80.0
            ))

        return suggestions

    def _generate_temporal_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate temporal validation suggestions based on temporal_analysis data."""
        suggestions = []

        # Only proceed if temporal analysis exists
        temporal_analysis = getattr(col.statistics, 'temporal_analysis', None)
        if not temporal_analysis:
            return suggestions

        # DateRangeCheck - based on detected date range
        date_range = temporal_analysis.get('date_range', {})
        if date_range and date_range.get('start') and date_range.get('end'):
            suggestions.append(ValidationSuggestion(
                validation_type="DateRangeCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "min_date": date_range['start'],
                    "max_date": date_range['end']
                },
                reason=f"Detected date range: {date_range['start']} to {date_range['end']}",
                confidence=90.0
            ))

        # DateSequenceCheck - based on regular frequency detection
        frequency = temporal_analysis.get('frequency', {})
        if frequency and frequency.get('is_regular'):
            suggestions.append(ValidationSuggestion(
                validation_type="DateSequenceCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "expected_frequency": frequency.get('inferred', 'daily')
                },
                reason=f"Detected regular {frequency.get('inferred', 'daily')} frequency",
                confidence=85.0
            ))

        # DateGapCheck - based on gaps detection
        gaps = temporal_analysis.get('gaps', {})
        if gaps and gaps.get('gaps_detected'):
            suggestions.append(ValidationSuggestion(
                validation_type="DateGapCheck",
                severity="WARNING",
                params={
                    "field": col.name,
                    "max_gap_days": 7  # Default threshold
                },
                reason="Temporal gaps detected in date sequence",
                confidence=80.0
            ))

        return suggestions

    def _generate_statistical_validations(self, col: ColumnProfile, row_count: int) -> List[ValidationSuggestion]:
        """Generate statistical validation suggestions based on distribution analysis."""
        suggestions = []

        # Only proceed for numeric fields
        if col.type_info.inferred_type not in ['integer', 'float', 'decimal']:
            return suggestions

        # CRITICAL FIX: Skip ID/code fields - CV analysis is meaningless for identifiers
        # Addresses ChatGPT review: Bank IDs (1-99), Account numbers shouldn't trigger outlier warnings
        field_name_lower = col.name.lower().replace(' ', '_').replace('-', '_')
        id_keywords = ['id', '_id', 'code', 'from_bank', 'to_bank', 'account', 'acct',
                       'customer', 'user', 'client', 'member', 'number', 'num', 'no', 'bank']
        is_id_or_code_field = any(keyword in field_name_lower for keyword in id_keywords)

        if is_id_or_code_field:
            logger.debug(f"Skipping statistical analysis for '{col.name}' - detected as ID/code field")
            return suggestions

        # OutlierDetectionCheck - based on std_dev relative to mean
        # FIX: Skip binary and low-cardinality categorical fields - CV is meaningless for these
        # Binary fields (e.g., 0/1 flags) always have high CV but this is expected, not anomalous
        if hasattr(col.statistics, 'mean') and hasattr(col.statistics, 'std_dev'):
            mean = col.statistics.mean
            std_dev = col.statistics.std_dev

            # Check if this is a binary or low-cardinality categorical field
            unique_count = col.statistics.unique_count if hasattr(col.statistics, 'unique_count') else None
            cardinality = col.statistics.cardinality if hasattr(col.statistics, 'cardinality') else None

            # Skip if binary field (2 unique values) or low-cardinality categorical (<=10 unique values or <10% cardinality)
            is_binary = unique_count == 2
            is_low_cardinality_categorical = (
                unique_count and unique_count <= 10 or
                cardinality and cardinality < 0.10
            )

            # Only suggest StatisticalOutlierCheck for continuous numeric fields with high variability
            if mean and std_dev and mean != 0 and not is_binary and not is_low_cardinality_categorical:
                coefficient_of_variation = (std_dev / abs(mean)) * 100

                if coefficient_of_variation > 50:
                    suggestions.append(ValidationSuggestion(
                        validation_type="StatisticalOutlierCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "method": "zscore",
                            "threshold": 3.0
                        },
                        reason=f"High variability detected (CV={coefficient_of_variation:.1f}%)",
                        confidence=75.0
                    ))
            elif is_binary or is_low_cardinality_categorical:
                card_str = f"{cardinality:.2f}" if cardinality is not None else "N/A"
                logger.debug(f"Skipping StatisticalOutlierCheck for '{col.name}' - detected as binary/categorical field (unique_count={unique_count}, cardinality={card_str})")

        return suggestions

    def _generate_string_pattern_validations(self, col: ColumnProfile) -> List[ValidationSuggestion]:
        """Generate string/pattern validation suggestions."""
        suggestions = []

        # Only proceed for string fields
        if col.type_info.inferred_type != 'string':
            return suggestions

        # StringLengthCheck - for fields with consistent length
        if hasattr(col.statistics, 'min_length') and hasattr(col.statistics, 'max_length'):
            min_len = col.statistics.min_length
            max_len = col.statistics.max_length

            # If min == max, all values have same length
            if min_len == max_len and min_len > 0:
                suggestions.append(ValidationSuggestion(
                    validation_type="StringLengthCheck",
                    severity="WARNING",
                    params={
                        "field": col.name,
                        "min_length": min_len,
                        "max_length": max_len
                    },
                    reason=f"All values have consistent length ({min_len} characters)",
                    confidence=95.0
                ))
            # If length varies within small range, suggest range
            elif min_len and max_len and (max_len - min_len) <= 3:
                suggestions.append(ValidationSuggestion(
                    validation_type="StringLengthCheck",
                    severity="WARNING",
                    params={
                        "field": col.name,
                        "min_length": min_len,
                        "max_length": max_len
                    },
                    reason=f"Length varies within small range ({min_len}-{max_len} characters)",
                    confidence=80.0
                ))

        return suggestions

    def _is_benford_semantic_confident(
        self,
        col_name: str,
        columns: List[ColumnProfile]
    ) -> tuple:
        """
        Determine if a column has high semantic confidence for Benford analysis.

        Benford analysis should only emit BenfordLawCheck when BOTH:
        - The Benford applicability flag is true (statistical criteria met)
        - Semantic confidence is HIGH (field is truly numeric/monetary)

        Returns:
            tuple: (is_high_confidence: bool, reason: str)
                - is_high_confidence: True if field is well-suited for Benford
                - reason: Explanation or caution text for YAML/UI
        """
        if not columns:
            return (False, "unknown_semantics")

        # Find the column
        col = None
        for c in columns:
            if c.name == col_name:
                col = c
                break

        if not col or not col.semantic_info:
            return (False, "unknown_semantics")

        # Get primary type from resolved semantics
        resolved = col.semantic_info.get('resolved', {})
        primary_type = resolved.get('primary_type', '').lower() if resolved else ''

        # If no resolved type, try schema_org or fibo directly
        if not primary_type:
            schema_org = col.semantic_info.get('schema_org', {})
            primary_type = schema_org.get('type', '').lower() if schema_org else ''
        if not primary_type:
            fibo = col.semantic_info.get('fibo', {})
            primary_type = fibo.get('type', '').lower() if fibo else ''

        # HIGH confidence types for Benford (truly numeric/monetary measures)
        high_conf_numeric = {
            'schema:number', 'schema:integer', 'schema:quantitativevalue',
            'schema:monetaryamount', 'schema:pricespecification',
            'fibo:moneyamount', 'fibo:transactionamount', 'fibo:amount'
        }

        # Identifier/code types - not suitable
        identifier_types = {
            'schema:identifier', 'fibo:accountidentifier', 'fibo:identifier'
        }

        # Check for structured pricing pattern in semantic info
        is_structured_pricing = False
        if col.semantic_info:
            # Check if the profiler detected structured pricing
            pricing_info = col.semantic_info.get('pricing_pattern', {})
            if pricing_info.get('is_structured', False):
                is_structured_pricing = True

        if primary_type in high_conf_numeric:
            if is_structured_pricing:
                return (False, "structured_pricing")
            return (True, "numeric_monetary")
        elif primary_type in identifier_types:
            return (False, "identifier")
        elif not primary_type:
            return (False, "unknown_semantics")
        else:
            return (False, "other_semantic_type")

    def _generate_ml_based_validations(
        self,
        ml_findings: Dict[str, Any],
        columns: List[ColumnProfile] = None
    ) -> List[ValidationSuggestion]:
        """
        Generate validation suggestions based on ML analysis findings.

        This function translates ML insights into actionable validation rules:
        - Outlier detection → OutlierCheck with identified columns and thresholds
        - Benford's Law violations → BenfordLawCheck for semantic-confident columns only
        - Cross-field anomalies → CrossFieldValidation suggestions
        - Multivariate anomalies → Relationship-based validation hints

        Args:
            ml_findings: Results from MLAnalyzer.analyze()
            columns: List of ColumnProfile objects for semantic confidence checks

        Returns:
            List of ValidationSuggestion objects based on ML findings
        """
        suggestions = []

        if not ml_findings:
            return suggestions

        # ═══════════════════════════════════════════════════════════════
        # 1. OUTLIER-BASED VALIDATIONS
        # If Isolation Forest found outliers, suggest OutlierCheck or RangeCheck
        # ═══════════════════════════════════════════════════════════════
        outlier_summary = ml_findings.get('outlier_summary', {})
        outlier_columns = outlier_summary.get('outlier_columns', {})

        for col_name, outlier_data in outlier_columns.items():
            outlier_count = outlier_data.get('count', 0)
            outlier_pct = outlier_data.get('percentage', 0)

            # Only suggest if meaningful outlier percentage found
            if outlier_count > 0 and outlier_pct >= 0.1:
                # Get threshold info if available
                typical_range = outlier_data.get('typical_range', {})
                min_typical = typical_range.get('min')
                max_typical = typical_range.get('max')

                if min_typical is not None and max_typical is not None:
                    # Suggest StatisticalOutlierCheck with learned bounds
                    suggestions.append(ValidationSuggestion(
                        validation_type="StatisticalOutlierCheck",
                        severity="WARNING",
                        params={
                            "field": col_name,
                            "method": "iqr",
                            "threshold": 3.0  # More lenient for real data
                        },
                        reason=f"ML detected {outlier_count:,} outliers ({outlier_pct:.2f}%) in {col_name}. Typical range: {min_typical:,.2f} to {max_typical:,.2f}",
                        confidence=80.0
                    ))

        # ═══════════════════════════════════════════════════════════════
        # 2. BENFORD'S LAW VALIDATIONS
        # Only emit BenfordLawCheck when BOTH applicability AND semantic confidence are high
        # ═══════════════════════════════════════════════════════════════
        benford_analysis = ml_findings.get('benford_analysis', {})

        for col_name, benford_data in benford_analysis.items():
            confidence = benford_data.get('confidence', 'Unknown')
            is_suspicious = benford_data.get('is_suspicious', False)
            chi_square = benford_data.get('chi_square', 0)
            p_value = benford_data.get('p_value', 1)

            # Check semantic confidence for this column
            is_semantic_confident, semantic_reason = self._is_benford_semantic_confident(col_name, columns)

            # Only emit BenfordLawCheck if semantic confidence is HIGH
            # Low semantic confidence = informational only (shown in HTML but not as YAML validation)
            if not is_semantic_confident:
                logger.debug(f"Skipping BenfordLawCheck for '{col_name}' - semantic confidence is low ({semantic_reason})")
                continue

            # Generate nuanced reason text based on context
            if semantic_reason == "structured_pricing":
                reason_text = (
                    f"First-digit frequencies deviate from Benford's Law (χ²={chi_square:.2f}, p={p_value:.4f}). "
                    "This field appears to have structured pricing/tariffs; deviations may reflect pricing structure "
                    "rather than data corruption. Treat as a weak signal and confirm with domain experts."
                )
            else:
                reason_text = (
                    f"First-digit frequencies deviate from Benford's Law (χ²={chi_square:.2f}, p={p_value:.4f}). "
                    "For naturally occurring transactional datasets this can indicate anomalies, but for structured "
                    "pricing or tariff-like fields it may simply reflect business rules. "
                    "Treat this as a weak signal and confirm with domain experts."
                )

            # NOTE: BenfordLawCheck validation suggestions are intentionally disabled.
            # Benford's Law analysis is performed and shown in reports for informational purposes,
            # but automatic validation suggestions are not generated because:
            # 1. Benford's Law has limited applicability (only for naturally occurring numeric data)
            # 2. Many datasets legitimately don't follow Benford's Law (prices, IDs, measurements)
            # 3. False positives are common and can cause confusion
            # The ML findings section will still show Benford analysis results for review.
            pass

        # ═══════════════════════════════════════════════════════════════
        # 3. CROSS-FIELD RELATIONSHIP VALIDATIONS
        # If correlation breaks or cross-field issues found, suggest checks
        # ═══════════════════════════════════════════════════════════════
        cross_issues = ml_findings.get('cross_column_issues', [])
        correlation_breaks = ml_findings.get('correlation_breaks', [])

        # Track which column pairs we've suggested for
        suggested_pairs = set()

        for issue in cross_issues:
            columns = issue.get('columns', [])
            issue_count = issue.get('total_issues', 0)

            if len(columns) >= 2 and issue_count > 10:
                pair_key = tuple(sorted(columns[:2]))
                if pair_key not in suggested_pairs:
                    suggested_pairs.add(pair_key)
                    suggestions.append(ValidationSuggestion(
                        validation_type="CrossFieldValidation",
                        severity="WARNING",
                        params={
                            "fields": columns[:2],
                            "rule": "relationship_check"
                        },
                        reason=f"ML found {issue_count:,} records where {columns[0]} and {columns[1]} show unusual relationships",
                        confidence=70.0
                    ))

        for cb in correlation_breaks:
            columns = cb.get('columns', [])
            anomaly_count = cb.get('anomaly_count', 0)

            if len(columns) >= 2 and anomaly_count > 10:
                pair_key = tuple(sorted(columns[:2]))
                if pair_key not in suggested_pairs:
                    suggested_pairs.add(pair_key)
                    correlation = cb.get('expected_correlation', 'unknown')
                    suggestions.append(ValidationSuggestion(
                        validation_type="CorrelationCheck",
                        severity="WARNING",
                        params={
                            "field1": columns[0],
                            "field2": columns[1],
                            "min_correlation": max(0.3, correlation - 0.2) if isinstance(correlation, (int, float)) else 0.3
                        },
                        reason=f"ML found {anomaly_count:,} correlation breaks between {columns[0]} and {columns[1]} (expected r={correlation})",
                        confidence=72.0
                    ))

        # ═══════════════════════════════════════════════════════════════
        # 4. MULTIVARIATE ANOMALY SUGGESTIONS
        # If autoencoder found unusual combinations, highlight
        # ═══════════════════════════════════════════════════════════════
        multivariate = ml_findings.get('multivariate_anomalies', {})
        autoencoder = multivariate.get('autoencoder_anomalies', {})

        if autoencoder:
            anomaly_count = autoencoder.get('total_anomalies', 0)
            columns_analyzed = autoencoder.get('columns_analyzed', [])

            if anomaly_count > 0 and isinstance(columns_analyzed, list) and len(columns_analyzed) >= 2:
                # Suggest a multi-field consistency check
                suggestions.append(ValidationSuggestion(
                    validation_type="RecordLevelValidation",
                    severity="INFO",
                    params={
                        "fields": columns_analyzed[:5],  # Limit to first 5
                        "rule": "multivariate_consistency"
                    },
                    reason=f"ML found {anomaly_count:,} records with unusual multi-field combinations across {len(columns_analyzed)} numeric fields. Consider custom validation.",
                    confidence=65.0
                ))

        # ═══════════════════════════════════════════════════════════════
        # 5. RARE VALUE SUGGESTIONS
        # If rare categories found, suggest ValidValuesCheck
        # ═══════════════════════════════════════════════════════════════
        rare_categories = ml_findings.get('rare_categories', {})

        for col_name, rare_data in rare_categories.items():
            rare_values = rare_data.get('rare_values', [])
            total_rare_count = rare_data.get('total_rare_count', 0)

            if total_rare_count > 0 and rare_values:
                # Get the rare values (potential typos/errors)
                rare_value_list = [rv.get('value') for rv in rare_values[:5]]

                suggestions.append(ValidationSuggestion(
                    validation_type="ValidValuesCheck",
                    severity="INFO",
                    params={
                        "field": col_name,
                        "exclude_values": rare_value_list,
                        "note": "These rare values may be valid edge cases or data entry errors"
                    },
                    reason=f"ML found {len(rare_values)} rare values in {col_name} ({total_rare_count:,} total instances). Review if these should be excluded.",
                    confidence=55.0
                ))

        return suggestions

    def _deduplicate_mandatory_field_checks(
        self,
        suggestions: List[ValidationSuggestion]
    ) -> List[ValidationSuggestion]:
        """
        Consolidate MandatoryFieldCheck suggestions to avoid duplicates.

        Fields can be flagged as mandatory from multiple sources:
        1. Completeness analysis (>95% completeness) - generates combined check
        2. FIBO semantic suggestions - generates individual checks per field

        This method:
        - Collects all unique fields from MandatoryFieldCheck suggestions
        - Creates a single MandatoryFieldCheck with all unique fields
        - Preserves all non-MandatoryFieldCheck suggestions unchanged
        """
        mandatory_fields = set()
        mandatory_reasons = []
        other_suggestions = []

        for sugg in suggestions:
            if sugg.validation_type == "MandatoryFieldCheck":
                # Extract fields from this suggestion
                fields = sugg.params.get('fields', [])
                field = sugg.params.get('field')

                if fields:
                    mandatory_fields.update(fields)
                if field:
                    mandatory_fields.add(field)

                # Collect reasons for documentation
                if sugg.reason and sugg.reason not in mandatory_reasons:
                    mandatory_reasons.append(sugg.reason)
            else:
                other_suggestions.append(sugg)

        # Create single consolidated MandatoryFieldCheck if any fields found
        if mandatory_fields:
            # Build combined reason
            if len(mandatory_reasons) == 1:
                combined_reason = mandatory_reasons[0]
            else:
                combined_reason = f"{len(mandatory_fields)} fields have >95% completeness"

            consolidated = ValidationSuggestion(
                validation_type="MandatoryFieldCheck",
                severity="WARNING",
                params={
                    "fields": sorted(list(mandatory_fields))  # Sort for consistent output
                },
                reason=combined_reason,
                confidence=95.0
            )
            other_suggestions.append(consolidated)

        return other_suggestions

    def _deduplicate_validations_by_field(
        self,
        suggestions: List[ValidationSuggestion]
    ) -> List[ValidationSuggestion]:
        """
        Deduplicate validations that target the same field.

        For certain validation types (ValidValuesCheck, RangeCheck, StringLengthCheck),
        multiple sources can generate identical suggestions for the same field:
        1. FIBO semantic analysis
        2. Statistical analysis (low cardinality, detected ranges, etc.)

        This method keeps only one validation per (validation_type, field) pair,
        preferring the one with higher confidence.
        """
        # Track validations by (type, field) - keep highest confidence
        field_validations: Dict[tuple, ValidationSuggestion] = {}
        other_suggestions = []

        # Validation types that should be deduplicated by field
        dedupe_types = {'ValidValuesCheck', 'RangeCheck', 'StringLengthCheck',
                        'DateFormatCheck', 'RegexCheck', 'UniqueKeyCheck'}

        for sugg in suggestions:
            if sugg.validation_type in dedupe_types:
                # Get the field this validation targets
                field = sugg.params.get('field') or sugg.params.get('fields', [None])[0]
                if field:
                    key = (sugg.validation_type, field)
                    existing = field_validations.get(key)

                    # Keep the one with higher confidence, or the existing if equal
                    if existing is None or sugg.confidence > existing.confidence:
                        field_validations[key] = sugg
                else:
                    # No field specified, keep as-is
                    other_suggestions.append(sugg)
            else:
                other_suggestions.append(sugg)

        # Add deduplicated validations back
        other_suggestions.extend(field_validations.values())

        return other_suggestions

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
                severity="ERROR",
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
            # ONLY suggest range checks for actual measurements/amounts, NOT identifiers
            if col.type_info.inferred_type in ["integer", "float"]:
                # Smart pattern-based detection instead of hardcoded keywords
                should_suggest_range = self._should_suggest_range_check(col, row_count)

                # Only suggest range check for actual numeric measurements
                if should_suggest_range:
                    if col.statistics.min_value is not None and col.statistics.max_value is not None:
                        # Get semantic type for smarter range suggestions
                        schema_org_type = None
                        if col.semantic_info:
                            schema_org_type = col.semantic_info.get('schema_org', {}).get('type', '')

                        # Apply semantic-aware range expansion
                        min_val, max_val, reason = self._calculate_smart_range(
                            col.name, col.statistics.min_value, col.statistics.max_value, schema_org_type
                        )

                        suggestions.append(ValidationSuggestion(
                            validation_type="RangeCheck",
                            severity="WARNING",
                            params={
                                "field": col.name,
                                "min_value": min_val,
                                "max_value": max_val
                            },
                            reason=reason,
                            confidence=90.0
                        ))

            # Valid values for low cardinality
            if col.statistics.cardinality < 0.05 and col.statistics.unique_count < 20:
                valid_values = [item["value"] for item in col.statistics.top_values]
                # Only suggest if we actually have valid values to check
                if valid_values:
                    # Check if this is a binary boolean field (0/1 or true/false)
                    schema_org_type = None
                    if col.semantic_info:
                        schema_org_type = col.semantic_info.get('schema_org', {}).get('type', '')

                    is_binary_flag = (
                        col.statistics.unique_count == 2 and
                        (schema_org_type == 'schema:Boolean' or
                         set(str(v).lower() for v in valid_values) <= {'0', '1', 'true', 'false', 'yes', 'no', 'y', 'n'})
                    )

                    if is_binary_flag:
                        # Use BooleanCheck for binary flags instead of ValidValuesCheck
                        suggestions.append(ValidationSuggestion(
                            validation_type="BooleanCheck",
                            severity="WARNING",
                            params={
                                "field": col.name,
                                "true_values": [1, "1", "true", "yes", "y", "True", "Yes", "Y"],
                                "false_values": [0, "0", "false", "no", "n", "False", "No", "N"]
                            },
                            reason="Binary flag field (boolean values)",
                            confidence=90.0
                        ))
                    else:
                        suggestions.append(ValidationSuggestion(
                            validation_type="ValidValuesCheck",
                            severity="WARNING",
                            params={
                                "field": col.name,
                                "valid_values": valid_values
                            },
                            reason=f"Low cardinality field with {col.statistics.unique_count} unique values",
                            confidence=85.0
                        ))

            # Unique key check for high cardinality
            # CRITICAL: Only suggest UniqueKeyCheck for fields that are actually identifiers
            # Use semantic classification to make smarter decisions
            if col.statistics.cardinality > 0.99 and row_count > 100:
                # Get semantic info from Schema.org/FIBO classification
                schema_org_type = None
                if col.semantic_info:
                    schema_org_type = col.semantic_info.get('schema_org', {}).get('type', '')
                    resolved_type = col.semantic_info.get('resolved', {}).get('primary_type', '')

                # Schema.org types that ARE likely unique identifiers
                identifier_types = {'schema:identifier', 'schema:productID', 'schema:sku',
                                   'schema:serialNumber', 'schema:accountId'}

                # Schema.org types that are NOT unique identifiers (even with high cardinality)
                exclude_schema_types = {'schema:name', 'schema:Text', 'schema:description',
                                       'schema:MonetaryAmount', 'schema:QuantitativeValue',
                                       'schema:DateTime', 'schema:Date', 'schema:Time',
                                       'schema:PropertyValue', 'schema:Number'}

                # Legacy exclusion types for backwards compatibility
                semantic_type = getattr(col.statistics, 'semantic_type', None)
                exclude_types = {'amount', 'count', 'measurement', 'float', 'decimal',
                                'datetime', 'timestamp', 'date'}

                # Check inferred_type for temporal fields
                inferred_type = col.type_info.inferred_type
                is_temporal = inferred_type == 'date' or getattr(col.type_info, 'is_temporal', False)

                # Measurement keyword check
                measurement_keywords = ['amount', 'price', 'cost', 'value', 'balance', 'total',
                                       'sum', 'count', 'quantity', 'measure', 'metric',
                                       'timestamp', 'datetime', 'time', 'date']
                field_name_lower = col.name.lower()
                is_measurement_field = any(keyword in field_name_lower for keyword in measurement_keywords)

                # Decision logic using semantic classification
                is_identifier = schema_org_type in identifier_types
                is_excluded_type = (schema_org_type in exclude_schema_types or
                                   semantic_type in exclude_types or
                                   is_measurement_field or is_temporal)

                # Only suggest UniqueKeyCheck if semantically an identifier OR high cardinality + not excluded
                if is_identifier or (not is_excluded_type):
                    confidence = 95.0 if is_identifier else 85.0
                    reason = ("Field classified as identifier type" if is_identifier
                             else "Field appears to be a unique identifier (high cardinality)")
                    suggestions.append(ValidationSuggestion(
                        validation_type="UniqueKeyCheck",
                        severity="WARNING",
                        params={
                            "fields": [col.name]
                        },
                        reason=reason,
                        confidence=confidence
                    ))
                else:
                    logger.debug(f"Skipping UniqueKeyCheck for '{col.name}' - schema_org={schema_org_type}, is_temporal={is_temporal}")

            # Date format check
            if col.type_info.inferred_type == "date":
                # Try to infer date format from samples and detected patterns
                detected_patterns = getattr(col.statistics, 'pattern_samples', None)
                date_format = self._infer_date_format(col.type_info.sample_values, detected_patterns)
                if date_format:
                    suggestions.append(ValidationSuggestion(
                        validation_type="DateFormatCheck",
                        severity="WARNING",
                        params={
                            "field": col.name,
                            "format": date_format
                        },
                        reason=f"Detected date/time format: {date_format}",
                        confidence=95.0  # High confidence when using detected patterns
                    ))

            # Semantic pattern-based suggestions
            # CRITICAL FIX: Raise confidence threshold from 30% to 80% for ERROR severity
            # Lower thresholds cause false positives in production validations
            from .semantic_patterns import SemanticPatternDetector

            if col.type_info.sample_values:
                # Detect semantic patterns in the data with tiered thresholds
                # - 80%+ confidence → ERROR severity (strict enforcement)
                # - 50-80% confidence → WARNING severity (advisory)
                # - Below 50% → Informational only (not suggested as validation)
                patterns = SemanticPatternDetector.detect_patterns(
                    col.type_info.sample_values,
                    min_confidence=0.50,  # Raised from 30% to 50% minimum
                    column_name=col.name  # Pass column name for context-based filtering
                )

                # Add suggestions for detected patterns
                for pattern_type, pattern_match in patterns.items():
                    pattern_suggestion = SemanticPatternDetector.suggest_validation(pattern_type)
                    if pattern_suggestion:
                        # Adjust confidence based on match percentage
                        adjusted_confidence = pattern_match.confidence * 100

                        # All field-level semantic pattern checks use WARNING severity
                        if adjusted_confidence >= 50:
                            severity = "WARNING"
                        else:
                            continue  # Skip suggestions below 50% confidence

                        suggestions.append(ValidationSuggestion(
                            validation_type=pattern_suggestion['validation_type'],
                            severity=severity,
                            params={
                                **pattern_suggestion['params'],
                                'field': col.name
                            },
                            reason=f"{pattern_suggestion['reason']} ({pattern_match.confidence*100:.1f}% of samples match)",
                            confidence=adjusted_confidence
                        ))

            # Semantic type-aware validation suggestions
            # Generate appropriate validations based on detected semantic types
            semantic_suggestions = self._generate_semantic_type_validations(col)
            suggestions.extend(semantic_suggestions)

        # ENHANCED VALIDATION SUGGESTIONS
        # Add temporal, statistical, and string/pattern validation suggestions
        # These were previously missing despite rich analysis data being available

        # Temporal validation suggestions (leverages existing temporal_analysis)
        for col in columns:
            temporal_suggestions = self._generate_temporal_validations(col)
            suggestions.extend(temporal_suggestions)

        # Statistical validation suggestions (leverages distribution/anomaly analysis)
        for col in columns:
            statistical_suggestions = self._generate_statistical_validations(col, row_count)
            suggestions.extend(statistical_suggestions)

        # String/pattern validation suggestions (StringLengthCheck, consistent patterns)
        for col in columns:
            string_suggestions = self._generate_string_pattern_validations(col)
            suggestions.extend(string_suggestions)

        # Add mandatory field check if any mandatory fields found
        if mandatory_fields:
            suggestions.append(ValidationSuggestion(
                validation_type="MandatoryFieldCheck",
                severity="WARNING",
                params={
                    "fields": mandatory_fields
                },
                reason=f"{len(mandatory_fields)} fields have >95% completeness",
                confidence=95.0
            ))

        # DEDUPLICATION: Consolidate MandatoryFieldCheck suggestions to avoid duplicates
        # Fields can be flagged as mandatory from multiple sources:
        # 1. Completeness analysis (>95% completeness)
        # 2. FIBO semantic suggestions (banking.account, party.customer_id, etc.)
        # Merge all into a single MandatoryFieldCheck per unique field set
        suggestions = self._deduplicate_mandatory_field_checks(suggestions)

        # DEDUPLICATION: Remove duplicate validations targeting the same field
        # Multiple sources can generate identical ValidValuesCheck, RangeCheck, etc:
        # 1. FIBO semantic analysis (e.g., money.currency → ValidValuesCheck)
        # 2. Statistical analysis (low cardinality → ValidValuesCheck)
        # Keep only the highest confidence validation per (type, field) pair
        suggestions = self._deduplicate_validations_by_field(suggestions)

        return sorted(suggestions, key=lambda x: x.confidence, reverse=True)

    def _pattern_to_format_string(self, pattern: str) -> Optional[str]:
        """
        Convert detected pattern (e.g., '9999/99/99 99:99') to Python format string (e.g., '%Y/%m/%d %H:%M').

        Pattern notation:
        - 9 = digit
        - A = letter
        - / - : space = literal characters
        """
        if not pattern:
            return None

        # Pattern mapping: detected pattern notation → Python strftime format
        pattern_mappings = {
            # DateTime patterns (date + time)
            r'^9999/99/99 99:99:99$': '%Y/%m/%d %H:%M:%S',
            r'^9999/99/99 99:99$': '%Y/%m/%d %H:%M',
            r'^9999-99-99 99:99:99$': '%Y-%m-%d %H:%M:%S',
            r'^9999-99-99 99:99$': '%Y-%m-%d %H:%M',
            r'^99/99/9999 99:99:99$': '%m/%d/%Y %H:%M:%S',
            r'^99/99/9999 99:99$': '%m/%d/%Y %H:%M',
            r'^99-99-9999 99:99:99$': '%m-%d-%Y %H:%M:%S',
            r'^99-99-9999 99:99$': '%m-%d-%Y %H:%M',

            # Date-only patterns
            r'^9999-99-99$': '%Y-%m-%d',
            r'^9999/99/99$': '%Y/%m/%d',
            r'^99/99/9999$': '%m/%d/%Y',
            r'^99-99-9999$': '%m-%d-%Y',
            r'^99\.99\.9999$': '%d.%m.%Y',

            # Time-only patterns
            r'^99:99:99$': '%H:%M:%S',
            r'^99:99$': '%H:%M',
        }

        # Check if pattern matches any known format
        for pattern_regex, format_string in pattern_mappings.items():
            if re.match(pattern_regex, pattern):
                return format_string

        return None

    def _infer_date_format(self, sample_values: List[Any], detected_patterns: List[Dict[str, Any]] = None) -> Optional[str]:
        """
        Infer date format from sample values and detected patterns.

        Args:
            sample_values: Sample values to analyze
            detected_patterns: Pre-detected patterns from pattern analysis

        Returns:
            Python strftime format string (e.g., '%Y/%m/%d %H:%M')
        """
        if not sample_values:
            return None

        # First try to use detected patterns if available (more accurate)
        if detected_patterns:
            for pattern_info in detected_patterns:
                pattern = pattern_info.get('pattern', '')
                percentage = pattern_info.get('percentage', 0)

                # If pattern has high confidence (>80%), convert it to format string
                if percentage > 80:
                    format_string = self._pattern_to_format_string(pattern)
                    if format_string:
                        logger.debug(f"Inferred date format from pattern '{pattern}': {format_string}")
                        return format_string

        # Fallback: Try common date/datetime formats with regex matching
        formats = [
            # DateTime formats (date + time)
            ("%Y/%m/%d %H:%M:%S", r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$'),
            ("%Y/%m/%d %H:%M", r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$'),
            ("%Y-%m-%d %H:%M:%S", r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'),
            ("%Y-%m-%d %H:%M", r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$'),
            ("%m/%d/%Y %H:%M:%S", r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'),
            ("%m/%d/%Y %H:%M", r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'),

            # Date-only formats
            ("%Y-%m-%d", r'^\d{4}-\d{2}-\d{2}$'),
            ("%Y/%m/%d", r'^\d{4}/\d{2}/\d{2}$'),
            ("%d/%m/%Y", r'^\d{2}/\d{2}/\d{4}$'),
            ("%m/%d/%Y", r'^\d{2}/\d{2}/\d{4}$'),
            ("%d-%m-%Y", r'^\d{2}-\d{2}-\d{4}$'),
        ]

        for date_format, pattern in formats:
            matches = sum(1 for val in sample_values if re.match(pattern, str(val)))
            if matches > len(sample_values) * 0.8:  # 80% match
                logger.debug(f"Inferred date format from samples: {date_format}")
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
