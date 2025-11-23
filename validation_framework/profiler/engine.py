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

from validation_framework.profiler.profile_result import (
    ProfileResult, ColumnProfile, TypeInference, ColumnStatistics,
    QualityMetrics, CorrelationResult, ValidationSuggestion
)
from validation_framework.profiler.column_intelligence import SmartColumnAnalyzer
from validation_framework.loaders.factory import LoaderFactory
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

# Phase 1 Profiler Enhancements
try:
    from validation_framework.profiler.temporal_analysis import TemporalAnalyzer
    TEMPORAL_ANALYSIS_AVAILABLE = True
except ImportError:
    TEMPORAL_ANALYSIS_AVAILABLE = False
    logger.warning("Temporal analysis not available - statsmodels may be missing")

try:
    from validation_framework.profiler.pii_detector import PIIDetector
    PII_DETECTION_AVAILABLE = True
except ImportError:
    PII_DETECTION_AVAILABLE = False
    logger.warning("PII detection not available")

try:
    from validation_framework.profiler.enhanced_correlation import EnhancedCorrelationAnalyzer
    ENHANCED_CORRELATION_AVAILABLE = True
except ImportError:
    ENHANCED_CORRELATION_AVAILABLE = False
    logger.warning("Enhanced correlation not available")

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

    def __init__(
        self,
        chunk_size: Optional[int] = None,
        max_correlation_columns: int = 20,
        enable_temporal_analysis: bool = True,
        enable_pii_detection: bool = True,
        enable_enhanced_correlation: bool = True
    ):
        """
        Initialize data profiler.

        Args:
            chunk_size: Number of rows to process per chunk (None = auto-calculate based on available memory)
            max_correlation_columns: Maximum columns for correlation analysis
            enable_temporal_analysis: Enable Phase 1 temporal analysis (default: True)
            enable_pii_detection: Enable Phase 1 PII detection (default: True)
            enable_enhanced_correlation: Enable Phase 1 enhanced correlation (default: True)
        """
        self.chunk_size = chunk_size  # None means auto-calculate
        self.max_correlation_columns = max_correlation_columns

        # Phase 1 enhancement flags
        self.enable_temporal_analysis = enable_temporal_analysis and TEMPORAL_ANALYSIS_AVAILABLE
        self.enable_pii_detection = enable_pii_detection and PII_DETECTION_AVAILABLE
        self.enable_enhanced_correlation = enable_enhanced_correlation and ENHANCED_CORRELATION_AVAILABLE

        # Initialize Phase 1 analyzers if enabled
        self.temporal_analyzer = TemporalAnalyzer() if self.enable_temporal_analysis else None
        self.pii_detector = PIIDetector() if self.enable_pii_detection else None
        self.enhanced_correlation_analyzer = EnhancedCorrelationAnalyzer() if self.enable_enhanced_correlation else None

        # Memory safety configuration
        self.memory_check_interval = 10  # Check memory every N chunks
        self.memory_warning_threshold = 75  # Warn at 75% memory usage
        self.memory_critical_threshold = 85  # Terminate at 85% memory usage

    def _check_memory_safety(self, chunk_idx: int, row_count: int) -> bool:
        """
        Check system memory usage and terminate if critical threshold exceeded.

        Args:
            chunk_idx: Current chunk index
            row_count: Total rows processed so far

        Returns:
            True if safe to continue, False if critical threshold exceeded

        Raises:
            MemoryError: If memory usage exceeds critical threshold
        """
        # Only check every N chunks to minimize overhead
        if chunk_idx % self.memory_check_interval != 0:
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
        logger.info(f"Starting profile of DataFrame: {name}")

        row_count = len(df)

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
        for column in columns:
            # Collect numeric data for correlations
            if column.type_info.inferred_type in ["integer", "float"]:
                try:
                    numeric_values = pd.to_numeric(df[column.name], errors='coerce').dropna()
                    numeric_data[column.name] = numeric_values.tolist()
                    logger.debug(f"Collected {len(numeric_values)} numeric values for {column.name}")
                except Exception as e:
                    logger.warning(f"Failed to collect numeric data for {column.name}: {e}")

            # Collect datetime data for temporal analysis
            if self.enable_temporal_analysis and column.type_info.inferred_type in ["datetime", "date"]:
                try:
                    dt_values = pd.to_datetime(df[column.name], errors='coerce').dropna()
                    datetime_data[column.name] = dt_values.tolist()
                    logger.debug(f"Collected {len(dt_values)} datetime values for {column.name}")
                except Exception as e:
                    logger.warning(f"Failed to collect datetime data for {column.name}: {e}")

        # Phase 1: Apply temporal analysis to datetime columns
        if self.enable_temporal_analysis:
            logger.info("Running temporal analysis on datetime columns...")
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
        if self.enable_pii_detection:
            logger.info("Running PII detection on all columns...")
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
            logger.info(f"⏱  PII detection completed in {phase_timings['pii_detection']:.2f}s")

        # Calculate correlations
        correlation_start = time.time()
        correlations = self._calculate_correlations(numeric_data, row_count)
        phase_timings['basic_correlation'] = time.time() - correlation_start

        # Phase 1: Calculate enhanced correlations
        enhanced_correlations = None
        if self.enable_enhanced_correlation and len(numeric_data) >= 2:
            enhanced_corr_start = time.time()
            logger.info("Running enhanced correlation analysis...")
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
                logger.info(f"Enhanced correlation analysis found {len(enhanced_correlations.get('correlation_pairs', []))} significant correlations")
            except Exception as e:
                logger.warning(f"Enhanced correlation analysis failed: {e}")

        # Phase 1: Calculate dataset-level privacy risk
        dataset_privacy_risk = None
        if self.enable_pii_detection and pii_columns:
            logger.info("Calculating dataset-level privacy risk...")
            try:
                dataset_privacy_risk = self.pii_detector.calculate_dataset_privacy_risk(
                    pii_columns=pii_columns,
                    total_columns=len(columns),
                    total_rows=row_count
                )
                logger.info(f"Dataset privacy risk: {dataset_privacy_risk.get('risk_level', 'unknown').upper()} ({dataset_privacy_risk.get('risk_score', 0)}/100)")
            except Exception as e:
                logger.warning(f"Dataset privacy risk calculation failed: {e}")

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
            generated_config_command=config_command,
            enhanced_correlations=enhanced_correlations,
            dataset_privacy_risk=dataset_privacy_risk
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

        # Track timing for each phase
        phase_timings = {}

        # Track sampling for memory efficiency
        MAX_CORRELATION_SAMPLES = 100_000  # Limit numeric samples for correlation analysis
        MAX_TEMPORAL_SAMPLES = 50_000      # Limit datetime samples for temporal analysis
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
            logger.info(f"🎯 Auto-calculated chunk size: {chunk_size:,} rows (based on {calc_result['available_memory_mb']:,}MB available memory)")
            logger.info(f"   Estimated chunks: {calc_result['estimated_chunks']:,} | Peak memory: ~{calc_result['estimated_memory_mb']:,}MB")
        else:
            logger.info(f"📊 Using specified chunk size: {chunk_size:,} rows")

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

                    logger.info(f"📋 File: {', '.join(info_parts)} ({total_chunks} chunks of {self.chunk_size:,})")
            elif hasattr(loader, 'get_row_count'):
                # Fallback to just row count
                total_rows = loader.get_row_count()
                import math
                total_chunks = math.ceil(total_rows / self.chunk_size)
                total_chunks_str = str(total_chunks)
                logger.info(f"📋 File contains {total_rows:,} rows ({total_chunks} chunks of {self.chunk_size:,})")
        except Exception as e:
            # If we can't get metadata, just show "?" - not a critical failure
            logger.debug(f"Could not read file metadata: {e}")
            pass

        # Process data in chunks
        chunk_processing_start = time.time()
        for chunk_idx, chunk in enumerate(loader.load()):
            row_count += len(chunk)
            logger.info(f"📊 Processing chunk {chunk_idx + 1}/{total_chunks_str} ({len(chunk):,} rows) - Total: {row_count:,} rows")
            logger.debug(f"Processing chunk {chunk_idx}, rows: {len(chunk)}")

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
                if column_profiles[col]["inferred_type"] in ["integer", "float"]:
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
                                logger.info(f"💾 Memory optimization: Column '{col}' sampling limit reached at {row_count:,} rows (using {MAX_CORRELATION_SAMPLES:,} samples for correlation)")
                        else:
                            # Only convert to list when size is small (under limit)
                            numeric_data[col].extend(numeric_values.tolist())

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
                                        dt_list = dt_values.dropna().tolist()
                                        samples_needed = MAX_TEMPORAL_SAMPLES - current_count

                                        # Use sampling if chunk has more than needed
                                        if len(dt_list) > samples_needed:
                                            import random
                                            datetime_data[col].extend(random.sample(dt_list, samples_needed))
                                            if col not in sampling_triggered:
                                                sampling_triggered[col] = row_count
                                                logger.info(f"💾 Memory optimization: Column '{col}' temporal sampling limit reached at {row_count:,} rows (using {MAX_TEMPORAL_SAMPLES:,} samples)")
                                        else:
                                            datetime_data[col].extend(dt_list)
                        except Exception:
                            pass

                # Collect sample data for PII detection (Phase 1) - limit to 1000 samples per column
                if self.enable_pii_detection:
                    if col not in all_column_data:
                        all_column_data[col] = []
                    if len(all_column_data[col]) < 1000:
                        samples_needed = 1000 - len(all_column_data[col])
                        all_column_data[col].extend(chunk[col].dropna().head(samples_needed).tolist())

        # Record chunk processing time
        phase_timings['chunk_processing'] = time.time() - chunk_processing_start
        logger.info(f"⏱  Chunk processing completed in {phase_timings['chunk_processing']:.2f}s")

        # Explicit garbage collection after each chunk to prevent memory buildup
        # This is critical for large files with many chunks
        del chunk  # Explicitly delete chunk DataFrame
        gc.collect()  # Force garbage collection

        # Log memory optimization summary
        if sampling_triggered:
            logger.info(f"💾 Memory optimization: Sampled {len(sampling_triggered)} numeric columns (max {MAX_CORRELATION_SAMPLES:,} values each)")
            total_samples = sum(len(values) for values in numeric_data.values())
            logger.info(f"💾 Total correlation samples in memory: {total_samples:,} values (vs {row_count:,} total rows)")

        # Finalize column profiles
        finalize_start = time.time()
        columns = []
        for col_name, profile_data in column_profiles.items():
            column_profile = self._finalize_column_profile(col_name, profile_data, row_count)
            columns.append(column_profile)
        phase_timings['finalize_profiles'] = time.time() - finalize_start
        logger.info(f"⏱  Profile finalization completed in {phase_timings['finalize_profiles']:.2f}s")

        # Phase 1: Apply temporal analysis to datetime columns
        if self.enable_temporal_analysis:
            temporal_start = time.time()
            logger.info("Running temporal analysis on datetime columns...")
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
            logger.info(f"⏱  Temporal analysis completed in {phase_timings['temporal_analysis']:.2f}s")

        # Phase 1: Apply PII detection to all columns
        pii_columns = []
        if self.enable_pii_detection:
            pii_start = time.time()
            logger.info("Running PII detection on all columns...")
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
            logger.info(f"⏱  PII detection completed in {phase_timings['pii_detection']:.2f}s")

        # Calculate correlations
        correlation_start = time.time()
        correlations = self._calculate_correlations(numeric_data, row_count)
        phase_timings['basic_correlation'] = time.time() - correlation_start

        # Phase 1: Calculate enhanced correlations
        enhanced_correlations = None
        if self.enable_enhanced_correlation and len(numeric_data) >= 2:
            enhanced_corr_start = time.time()
            logger.info("Running enhanced correlation analysis...")
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
                logger.info(f"Enhanced correlation analysis found {len(enhanced_correlations.get('correlation_pairs', []))} significant correlations")
            except Exception as e:
                logger.warning(f"Enhanced correlation analysis failed: {e}")

        # Phase 1: Calculate dataset-level privacy risk
        dataset_privacy_risk = None
        if self.enable_pii_detection and pii_columns:
            logger.info("Calculating dataset-level privacy risk...")
            try:
                dataset_privacy_risk = self.pii_detector.calculate_dataset_privacy_risk(
                    pii_columns=pii_columns,
                    total_columns=len(columns),
                    total_rows=row_count
                )
                logger.info(f"Dataset privacy risk: {dataset_privacy_risk.get('risk_level', 'unknown').upper()} ({dataset_privacy_risk.get('risk_score', 0)}/100)")
            except Exception as e:
                logger.warning(f"Dataset privacy risk calculation failed: {e}")

        # Generate validation suggestions
        suggestions_start = time.time()
        suggested_validations = self._generate_validation_suggestions(columns, row_count)
        phase_timings['generate_suggestions'] = time.time() - suggestions_start

        # Calculate overall quality score
        quality_start = time.time()
        overall_quality = self._calculate_overall_quality(columns)
        phase_timings['quality_score'] = time.time() - quality_start

        # Generate validation configuration
        config_start = time.time()
        config_yaml, config_command = self._generate_validation_config(
            file_name, file_path, file_format, file_size, row_count, columns, suggested_validations
        )
        phase_timings['generate_config'] = time.time() - config_start

        processing_time = time.time() - start_time
        logger.info(f"⏱  Profile completed in {processing_time:.2f} seconds")

        # Log timing breakdown
        logger.info("⏱  Timing breakdown:")
        for phase, duration in phase_timings.items():
            percentage = (duration / processing_time * 100) if processing_time > 0 else 0
            logger.info(f"   {phase}: {duration:.2f}s ({percentage:.1f}%)")

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
            generated_config_command=config_command,
            enhanced_correlations=enhanced_correlations,
            dataset_privacy_risk=dataset_privacy_risk,
            file_metadata=file_metadata if file_metadata else None
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

        # Type detection (sample-based for performance)
        # Only detect types on first chunk and sample of subsequent chunks
        if chunk_idx == 0:
            # First chunk: detect all types
            # Track unexpected types for debugging (limit to first 10 occurrences)
            unexpected_types_logged = 0
            for value in non_null_series:
                detected_type = self._detect_type(value)
                profile["type_counts"][detected_type] = profile["type_counts"].get(detected_type, 0) + 1

                # Debug logging for type mismatches (first chunk only, limit output)
                # Look for common patterns that might indicate issues
                if unexpected_types_logged < 10:
                    # If column name suggests it should be string but we detect numeric/other
                    col_lower = profile["column_name"].lower()
                    is_account_like = any(keyword in col_lower for keyword in ['account', 'id', 'code', 'reference', 'number'])
                    if is_account_like and detected_type in ['integer', 'float', 'boolean', 'date']:
                        logger.debug(f"🔍 Type mismatch in '{profile['column_name']}': value='{value}' → detected as '{detected_type}' (expected string)")
                        unexpected_types_logged += 1

            profile["type_sampled_count"] += len(non_null_series)
        elif chunk_idx % 10 == 0:
            # Every 10th chunk: sample 1000 values for type refinement
            sample_size = min(1000, len(non_null_series))
            # Use pandas .sample() instead of converting to list (memory efficient)
            if len(non_null_series) > sample_size:
                sampled_series = non_null_series.sample(n=sample_size, random_state=42)
            else:
                sampled_series = non_null_series

            for value in sampled_series:
                detected_type = self._detect_type(value)
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
            # Confidence = percentage of SAMPLED values matching primary type
            # Use type_sampled_count instead of total rows for accurate confidence with sampling
            type_sampled_count = profile_data.get("type_sampled_count", non_null_count)
            confidence = primary_count / type_sampled_count if type_sampled_count > 0 else 0.0
            is_known = False
            inferred = primary_type

        # Detect type conflicts
        conflicts = []
        type_sampled_count_for_conflicts = profile_data.get("type_sampled_count", non_null_count)
        for typ, count in sorted_types[1:4]:  # Top 3 conflicts
            if count > type_sampled_count_for_conflicts * 0.01:  # At least 1% of sampled data
                conflicts.append({
                    "type": typ,
                    "count": count,
                    "percentage": round(100 * count / type_sampled_count_for_conflicts, 2)
                })

        # Log type inference summary with conflicts (DEBUG level)
        col_name = profile_data.get("column_name", "unknown")
        if conflicts and confidence < 0.95:
            # Log when confidence is low due to type conflicts
            conflict_summary = ", ".join([f"{c['type']} ({c['percentage']}%)" for c in conflicts])
            logger.debug(
                f"🔍 Type inference for '{col_name}': "
                f"primary={inferred} ({confidence*100:.1f}% confidence), "
                f"conflicts=[{conflict_summary}], "
                f"sampled={type_sampled_count_for_conflicts:,} values"
            )

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

        # Use intelligent sampling to determine optimal sample size
        column_name = profile_data["column_name"]
        intelligence = SmartColumnAnalyzer.analyze_column(column_name)

        stats = ColumnStatistics()
        stats.count = total_rows
        stats.null_count = null_count
        stats.null_percentage = 100 * null_count / total_rows if total_rows > 0 else 0

        # Add intelligent sampling metadata for transparency
        stats.semantic_type = intelligence.semantic_type
        stats.sample_size = min(len(numeric_values) if numeric_values else len(value_counts), intelligence.recommended_sample_size)
        stats.sampling_strategy = SmartColumnAnalyzer.get_sampling_summary(
            column_name, total_rows, intelligence
        )

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
        observations = []  # General informational insights

        # Completeness: % of non-null values
        quality.completeness = 100 - statistics.null_percentage
        completeness_note = None
        if quality.completeness < 50:
            completeness_note = f"Low completeness: {quality.completeness:.1f}% non-null"
            issues.append(completeness_note)
        elif quality.completeness < 90:
            completeness_note = f"Moderate completeness: {quality.completeness:.1f}% non-null"
            issues.append(completeness_note)

        # Validity: % matching inferred type
        quality.validity = type_info.confidence * 100
        validity_note = None
        if quality.validity < 95:
            validity_note = f"Type inconsistency: {quality.validity:.1f}% match inferred type"
            issues.append(validity_note)

        # Uniqueness: cardinality
        quality.uniqueness = statistics.cardinality * 100
        uniqueness_note = None
        if statistics.cardinality == 1.0 and total_rows > 1:
            # This is an observation about uniqueness - informational
            uniqueness_note = "All values are unique (potential key field)"
            observations.append(uniqueness_note)
        elif statistics.cardinality < 0.01 and statistics.unique_count < 100 and total_rows > 100:
            # Very low cardinality is only an issue when there are actually few unique values
            # Example: 100 rows with 5 unique values = problem
            # NOT a problem: 1M rows with 5K unique values (0.5% cardinality but not "very low")
            uniqueness_note = f"Very low cardinality: {statistics.unique_count} unique values"
            issues.append(uniqueness_note)

        # Consistency: pattern matching
        consistency_note = None
        if statistics.pattern_samples:
            # If top pattern covers >80% of data, high consistency
            top_pattern_pct = statistics.pattern_samples[0]["percentage"]
            quality.consistency = top_pattern_pct

            if quality.consistency < 50:
                # Pattern diversity is informational
                consistency_note = f"{len(statistics.pattern_samples)} different patterns detected"
                observations.append(consistency_note)
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
        quality.observations = observations

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
            # Create DataFrame for correlation using sampled data (NOT full row_count!)
            # Find the maximum sample length across all columns
            max_sample_length = max(len(numeric_data[col]) for col in numeric_columns) if numeric_columns else 0

            df_dict = {}
            for col in numeric_columns:
                # Ensure same length by padding/truncating to max_sample_length (NOT row_count!)
                values = numeric_data[col][:max_sample_length]
                if len(values) < max_sample_length:
                    # Pad with NaN to match longest sample (typically 100K, not 179M!)
                    values = values + [np.nan] * (max_sample_length - len(values))
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

    def _should_suggest_range_check(self, col: ColumnProfile, row_count: int) -> bool:
        """
        Smart pattern-based detection: Is this field an identifier or a measurement?

        Uses data characteristics instead of hardcoded keywords.
        Returns True if range validation makes sense, False for identifiers/categories.
        """
        # 1. Check semantic type (if profiler already determined it)
        semantic_type = getattr(col.statistics, 'semantic_type', None)
        if semantic_type in ['id', 'identifier', 'key', 'category']:
            return False
        if semantic_type in ['amount', 'measurement', 'metric']:
            return True

        # 2. Boolean/flag fields (only 2 unique values) - use ValidValuesCheck instead
        if col.statistics.unique_count == 2:
            return False

        # 3. High cardinality (>80% unique) = likely identifier, not measurement
        if col.statistics.cardinality > 0.8:
            return False

        # 4. Low cardinality (<5%, <20 values) = categorical, will get ValidValuesCheck
        if col.statistics.cardinality < 0.05 and col.statistics.unique_count < 20:
            return False

        # 5. Check if numeric values look like IDs (large sparse range)
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

        # 6. Column name hint (weak signal) + data validation (strong signal)
        col_name_lower = col.name.lower()
        name_suggests_id = any(kw in col_name_lower for kw in
                              ['id', 'key', 'code', 'number', 'account', 'bank', 'reference', 'ref'])

        # If name suggests ID AND cardinality is moderate-high, probably an identifier
        if name_suggests_id and col.statistics.cardinality > 0.5:
            return False

        # Default: suggest range check for numeric measurements
        return True

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
            # ONLY suggest range checks for actual measurements/amounts, NOT identifiers
            if col.type_info.inferred_type in ["integer", "float"]:
                # Smart pattern-based detection instead of hardcoded keywords
                should_suggest_range = self._should_suggest_range_check(col, row_count)

                # Only suggest range check for actual numeric measurements
                if should_suggest_range:
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

            # Semantic pattern-based suggestions
            from .semantic_patterns import SemanticPatternDetector

            if col.type_info.sample_values:
                # Detect semantic patterns in the data
                patterns = SemanticPatternDetector.detect_patterns(
                    col.type_info.sample_values,
                    min_confidence=0.30  # 30% threshold for suggestions
                )

                # Add suggestions for detected patterns
                for pattern_type, pattern_match in patterns.items():
                    pattern_suggestion = SemanticPatternDetector.suggest_validation(pattern_type)
                    if pattern_suggestion:
                        # Adjust confidence based on match percentage
                        adjusted_confidence = pattern_match.confidence * 100

                        suggestions.append(ValidationSuggestion(
                            validation_type=pattern_suggestion['validation_type'],
                            severity=pattern_suggestion['severity'],
                            params={
                                **pattern_suggestion['params'],
                                'field': col.name
                            },
                            reason=f"{pattern_suggestion['reason']} ({pattern_match.confidence*100:.1f}% of samples match)",
                            confidence=adjusted_confidence
                        ))

            # Semantic type-aware suggestions (based on column semantics)
            semantic_type = getattr(col.statistics, 'semantic_type', None)

            if semantic_type == 'amount' or 'amount' in col.name.lower() or 'price' in col.name.lower():
                # Amount fields should be non-negative
                # NOTE: Only add this if we didn't already add a generic RangeCheck above
                if col.type_info.inferred_type in ['integer', 'float']:
                    # Check if we already added a RangeCheck for this field
                    already_has_range_check = any(
                        s.validation_type == "RangeCheck" and s.params.get("field") == col.name
                        for s in suggestions
                    )

                    if not already_has_range_check:
                        suggestions.append(ValidationSuggestion(
                            validation_type="RangeCheck",
                            severity="ERROR",
                            params={
                                "field": col.name,
                                "min_value": 0,
                                "max_value": col.statistics.max_value if col.statistics.max_value else 999999999
                            },
                            reason="Amount fields should be non-negative",
                            confidence=85.0
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
