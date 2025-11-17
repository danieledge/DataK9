"""
Unit tests for constants module.

Tests that all constants are properly defined and have sensible values.

Author: Daniel Edge
"""

import pytest
from validation_framework.core.constants import (
    # File processing
    DEFAULT_CHUNK_SIZE,
    MIN_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
    # Security
    MAX_YAML_FILE_SIZE,
    MAX_YAML_NESTING_DEPTH,
    MAX_YAML_KEY_COUNT,
    # Validation results
    MAX_SAMPLE_FAILURES,
    MAX_FAILURES_PER_CHUNK,
    # String processing
    MAX_SQL_IDENTIFIER_LENGTH,
    MAX_STRING_LENGTH,
    # Performance
    POLARS_THRESHOLD_BYTES,
    TYPE_INFERENCE_SAMPLE_SIZE,
    MAX_UNIQUE_VALUES_FOR_CATEGORICAL,
    # Profiler
    HISTOGRAM_BINS,
    PERCENTILES,
    OUTLIER_Z_SCORE_THRESHOLD,
    OUTLIER_IQR_MULTIPLIER,
    MIN_SAMPLE_SIZE_FOR_STATS,
    MAX_PATTERN_EXAMPLES,
    # File formats
    SUPPORTED_FILE_FORMATS,
    DEFAULT_FILE_FORMAT,
    FILE_EXTENSION_MAP,
    # Severity
    VALID_SEVERITIES,
    DEFAULT_SEVERITY,
    # Regex patterns
    EMAIL_PATTERN,
    PHONE_US_PATTERN,
    ZIP_US_PATTERN
)


class TestFileProcessingConstants:
    """Test file processing constants."""

    def test_chunk_size_bounds(self):
        """Test that chunk sizes are ordered correctly."""
        assert MIN_CHUNK_SIZE < DEFAULT_CHUNK_SIZE < MAX_CHUNK_SIZE

    def test_chunk_sizes_are_positive(self):
        """Test that all chunk sizes are positive."""
        assert MIN_CHUNK_SIZE > 0
        assert DEFAULT_CHUNK_SIZE > 0
        assert MAX_CHUNK_SIZE > 0

    def test_default_chunk_size_reasonable(self):
        """Test that default chunk size is reasonable for memory."""
        # 50K rows × 50 columns × 8 bytes ≈ 20MB per chunk
        assert DEFAULT_CHUNK_SIZE == 50_000
        estimated_memory_mb = (DEFAULT_CHUNK_SIZE * 50 * 8) / (1024 * 1024)
        assert 10 < estimated_memory_mb < 100  # Between 10MB and 100MB


class TestSecurityConstants:
    """Test security-related constants."""

    def test_yaml_size_limit(self):
        """Test YAML file size limit is reasonable."""
        # 10MB limit
        assert MAX_YAML_FILE_SIZE == 10 * 1024 * 1024
        assert MAX_YAML_FILE_SIZE > 0

    def test_yaml_nesting_depth(self):
        """Test YAML nesting depth prevents stack overflow."""
        # Should be deep enough for normal use but prevent attacks
        assert 10 < MAX_YAML_NESTING_DEPTH < 100

    def test_yaml_key_count(self):
        """Test YAML key count prevents memory exhaustion."""
        # Should allow reasonable configs but prevent attacks
        assert 1000 < MAX_YAML_KEY_COUNT < 100_000


class TestValidationResultConstants:
    """Test validation result constants."""

    def test_max_sample_failures(self):
        """Test that sample failure limit is reasonable."""
        assert MAX_SAMPLE_FAILURES == 100
        assert MAX_SAMPLE_FAILURES > 0

    def test_max_failures_per_chunk(self):
        """Test that per-chunk limit is higher than sample limit."""
        assert MAX_FAILURES_PER_CHUNK >= MAX_SAMPLE_FAILURES
        assert MAX_FAILURES_PER_CHUNK == 1_000


class TestStringProcessingConstants:
    """Test string processing constants."""

    def test_sql_identifier_length(self):
        """Test SQL identifier length matches PostgreSQL standard."""
        assert MAX_SQL_IDENTIFIER_LENGTH == 63  # PostgreSQL standard

    def test_max_string_length(self):
        """Test max string length is reasonable."""
        # 10MB limit
        assert MAX_STRING_LENGTH == 10 * 1024 * 1024


class TestPerformanceConstants:
    """Test performance tuning constants."""

    def test_polars_threshold(self):
        """Test Polars backend threshold is reasonable."""
        # 100MB threshold
        assert POLARS_THRESHOLD_BYTES == 100 * 1024 * 1024

    def test_type_inference_sample_size(self):
        """Test type inference sample is reasonable."""
        assert TYPE_INFERENCE_SAMPLE_SIZE == 10_000
        assert TYPE_INFERENCE_SAMPLE_SIZE > 100  # Enough for good inference

    def test_categorical_threshold(self):
        """Test categorical uniqueness threshold."""
        assert MAX_UNIQUE_VALUES_FOR_CATEGORICAL == 100
        assert MAX_UNIQUE_VALUES_FOR_CATEGORICAL > 0


class TestProfilerConstants:
    """Test profiler constants."""

    def test_histogram_bins(self):
        """Test histogram bin count is reasonable."""
        assert HISTOGRAM_BINS == 50
        assert 10 < HISTOGRAM_BINS < 200  # Good balance

    def test_percentiles(self):
        """Test percentiles list is valid."""
        assert isinstance(PERCENTILES, list)
        assert len(PERCENTILES) > 0

        # All values between 0 and 1
        for p in PERCENTILES:
            assert 0 <= p <= 1

        # Should be sorted
        assert PERCENTILES == sorted(PERCENTILES)

        # Should include common percentiles
        assert 0.25 in PERCENTILES  # Q1
        assert 0.50 in PERCENTILES  # Median
        assert 0.75 in PERCENTILES  # Q3

    def test_outlier_z_score(self):
        """Test outlier Z-score threshold."""
        # 3 standard deviations (99.7% of normal distribution)
        assert OUTLIER_Z_SCORE_THRESHOLD == 3.0
        assert OUTLIER_Z_SCORE_THRESHOLD > 0

    def test_outlier_iqr_multiplier(self):
        """Test outlier IQR multiplier (Tukey's fence)."""
        assert OUTLIER_IQR_MULTIPLIER == 1.5  # Standard practice
        assert OUTLIER_IQR_MULTIPLIER > 0

    def test_min_sample_size_for_stats(self):
        """Test minimum sample size for statistical tests."""
        assert MIN_SAMPLE_SIZE_FOR_STATS == 30  # Standard statistical practice
        assert MIN_SAMPLE_SIZE_FOR_STATS > 0

    def test_max_pattern_examples(self):
        """Test maximum pattern examples to collect."""
        assert MAX_PATTERN_EXAMPLES == 10
        assert MAX_PATTERN_EXAMPLES > 0


class TestFileFormatConstants:
    """Test file format constants."""

    def test_supported_formats(self):
        """Test supported file formats list."""
        assert isinstance(SUPPORTED_FILE_FORMATS, list)
        assert len(SUPPORTED_FILE_FORMATS) > 0

        # Should include common formats
        assert "csv" in SUPPORTED_FILE_FORMATS
        assert "excel" in SUPPORTED_FILE_FORMATS
        assert "json" in SUPPORTED_FILE_FORMATS
        assert "parquet" in SUPPORTED_FILE_FORMATS

    def test_default_format(self):
        """Test default file format."""
        assert DEFAULT_FILE_FORMAT == "csv"
        assert DEFAULT_FILE_FORMAT in SUPPORTED_FILE_FORMATS

    def test_file_extension_map(self):
        """Test file extension to format mapping."""
        assert isinstance(FILE_EXTENSION_MAP, dict)
        assert len(FILE_EXTENSION_MAP) > 0

        # Test common extensions
        assert FILE_EXTENSION_MAP[".csv"] == "csv"
        assert FILE_EXTENSION_MAP[".xlsx"] == "excel"
        assert FILE_EXTENSION_MAP[".json"] == "json"
        assert FILE_EXTENSION_MAP[".parquet"] == "parquet"

        # All extensions should start with dot
        for ext in FILE_EXTENSION_MAP.keys():
            assert ext.startswith(".")

        # All formats should be in supported list
        for format_name in FILE_EXTENSION_MAP.values():
            assert format_name in SUPPORTED_FILE_FORMATS


class TestSeverityConstants:
    """Test severity constants."""

    def test_valid_severities(self):
        """Test valid severity levels."""
        assert isinstance(VALID_SEVERITIES, list)
        assert len(VALID_SEVERITIES) > 0

        # Should include common severities
        assert "ERROR" in VALID_SEVERITIES
        assert "WARNING" in VALID_SEVERITIES

    def test_default_severity(self):
        """Test default severity."""
        assert DEFAULT_SEVERITY == "ERROR"
        assert DEFAULT_SEVERITY in VALID_SEVERITIES


class TestRegexPatterns:
    """Test regex pattern constants."""

    def test_email_pattern(self):
        """Test email regex pattern."""
        import re
        pattern = re.compile(EMAIL_PATTERN)

        # Valid emails
        assert pattern.match("user@example.com")
        assert pattern.match("test.user+tag@domain.co.uk")

        # Invalid emails
        assert not pattern.match("invalid")
        assert not pattern.match("@example.com")
        assert not pattern.match("user@")

    def test_phone_us_pattern(self):
        """Test US phone regex pattern."""
        import re
        pattern = re.compile(PHONE_US_PATTERN)

        # Valid US phones
        assert pattern.match("+12025551234")

        # Invalid
        assert not pattern.match("2025551234")  # Missing +1
        assert not pattern.match("+1202555123")  # Too short

    def test_zip_us_pattern(self):
        """Test US ZIP code regex pattern."""
        import re
        pattern = re.compile(ZIP_US_PATTERN)

        # Valid ZIP codes
        assert pattern.match("12345")
        assert pattern.match("12345-6789")

        # Invalid
        assert not pattern.match("1234")  # Too short
        assert not pattern.match("ABCDE")  # Not numeric


class TestConstantRelationships:
    """Test relationships between related constants."""

    def test_sample_failure_limits(self):
        """Test that sample failure limits are ordered correctly."""
        # Per-chunk limit should be >= global limit
        assert MAX_FAILURES_PER_CHUNK >= MAX_SAMPLE_FAILURES

    def test_memory_constants_reasonable(self):
        """Test that memory-related constants make sense together."""
        # Default chunk should fit comfortably in typical RAM
        default_chunk_memory_mb = (DEFAULT_CHUNK_SIZE * 100 * 8) / (1024 * 1024)

        # Should be less than 500MB for safety
        assert default_chunk_memory_mb < 500

    def test_sample_sizes_reasonable(self):
        """Test that sample sizes are ordered appropriately."""
        # Type inference sample should be less than default chunk
        assert TYPE_INFERENCE_SAMPLE_SIZE < DEFAULT_CHUNK_SIZE

        # Min sample for stats should be much smaller
        assert MIN_SAMPLE_SIZE_FOR_STATS < TYPE_INFERENCE_SAMPLE_SIZE
