"""
Comprehensive tests for the data profiling functionality.

Tests cover:
- Type detection and inference
- Statistical calculations
- Quality metrics
- Pattern detection
- Correlation analysis
- Validation suggestions
- HTML report generation
- End-to-end profiling workflow
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import tempfile
import os

from validation_framework.profiler.engine import DataProfiler
from validation_framework.profiler.profile_result import (
    ProfileResult, ColumnProfile, TypeInference, ColumnStatistics,
    QualityMetrics, CorrelationResult, ValidationSuggestion
)
from validation_framework.profiler.html_reporter import ProfileHTMLReporter


class TestTypeDetection:
    """Test type detection functionality via the extracted TypeInferrer class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()
        # Use the extracted type inferrer
        self.type_inferrer = self.profiler.type_inferrer

    def test_detect_integer(self):
        """Test integer type detection."""
        assert self.type_inferrer.detect_type(42) == "integer"
        assert self.type_inferrer.detect_type("42") == "integer"
        assert self.type_inferrer.detect_type(100) == "integer"

    def test_detect_float(self):
        """Test float type detection."""
        assert self.type_inferrer.detect_type(3.14) == "float"
        assert self.type_inferrer.detect_type("3.14") == "float"
        assert self.type_inferrer.detect_type(0.5) == "float"

    def test_detect_boolean(self):
        """Test boolean type detection."""
        assert self.type_inferrer.detect_type(True) == "boolean"
        assert self.type_inferrer.detect_type(False) == "boolean"
        assert self.type_inferrer.detect_type("true") == "boolean"
        assert self.type_inferrer.detect_type("yes") == "boolean"
        assert self.type_inferrer.detect_type("no") == "boolean"

    def test_detect_date(self):
        """Test date type detection."""
        assert self.type_inferrer.detect_type("2025-01-13") == "date"
        assert self.type_inferrer.detect_type("13/01/2025") == "date"
        assert self.type_inferrer.detect_type("01-13-2025") == "date"
        assert self.type_inferrer.detect_type("2025/01/13") == "date"

    def test_detect_string(self):
        """Test string type detection."""
        assert self.type_inferrer.detect_type("hello world") == "string"
        assert self.type_inferrer.detect_type("abc123xyz") == "string"
        assert self.type_inferrer.detect_type("test@example.com") == "string"

    def test_detect_null(self):
        """Test null type detection."""
        assert self.type_inferrer.detect_type(None) == "null"
        assert self.type_inferrer.detect_type(np.nan) == "null"
        assert self.type_inferrer.detect_type(pd.NA) == "null"


class TestPatternExtraction:
    """Test pattern extraction functionality via the extracted TypeInferrer class."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()
        # Use the extracted type inferrer
        self.type_inferrer = self.profiler.type_inferrer

    def test_pattern_with_numbers(self):
        """Test pattern extraction with numbers."""
        assert self.type_inferrer.extract_pattern("ABC-123") == "AAA-999"
        assert self.type_inferrer.extract_pattern("ID-001") == "AA-999"

    def test_pattern_with_letters(self):
        """Test pattern extraction with letters."""
        assert self.type_inferrer.extract_pattern("HELLO") == "AAAAA"
        assert self.type_inferrer.extract_pattern("Test") == "AAAA"  # All letters become 'A'

    def test_pattern_with_special_chars(self):
        """Test pattern extraction with special characters."""
        assert self.type_inferrer.extract_pattern("email@domain.com") == "AAAAA@AAAAAA.AAA"  # All letters become 'A'
        assert self.type_inferrer.extract_pattern("$100.50") == "$999.99"

    def test_pattern_length_limit(self):
        """Test pattern extraction length limit."""
        long_string = "A" * 100
        pattern = self.type_inferrer.extract_pattern(long_string)
        assert len(pattern) == 50  # Should be truncated to 50 chars


class TestDateFormatInference:
    """Test date format inference."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()

    def test_infer_iso_format(self):
        """Test ISO date format inference."""
        samples = ["2025-01-13", "2025-02-14", "2025-03-15"]
        assert self.profiler._infer_date_format(samples) == "%Y-%m-%d"

    def test_infer_us_format(self):
        """Test US date format inference."""
        samples = ["01/13/2025", "02/14/2025", "03/15/2025"]
        # Note: DD/MM/YYYY and MM/DD/YYYY have same regex, first match returned
        result = self.profiler._infer_date_format(samples)
        assert result in ["%d/%m/%Y", "%m/%d/%Y"]  # Both are valid matches

    def test_no_format_match(self):
        """Test when no date format matches."""
        samples = ["not a date", "also not a date"]
        assert self.profiler._infer_date_format(samples) is None

    def test_empty_samples(self):
        """Test with empty samples."""
        assert self.profiler._infer_date_format([]) is None


class TestStatisticsCalculation:
    """Test statistical calculations."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()

    def test_numeric_statistics(self):
        """Test calculation of numeric statistics."""
        profile_data = {
            "column_name": "test_col",
            "declared_type": None,
            "sample_values": [1, 2, 3, 4, 5],
            "type_counts": {"integer": 5},
            "null_count": 0,
            "value_counts": {1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
            "numeric_values": [1.0, 2.0, 3.0, 4.0, 5.0],
            "string_lengths": [1, 1, 1, 1, 1],
            "patterns": {},
            "inferred_type": "integer",
            "total_processed": 5
        }

        stats = self.profiler.stats_calculator.calculate_statistics(profile_data, total_rows=5)

        assert stats.count == 5
        assert stats.null_count == 0
        assert stats.unique_count == 5
        assert stats.min_value == 1.0
        assert stats.max_value == 5.0
        assert stats.mean == 3.0
        assert stats.median == 3.0
        assert stats.quartiles is not None

    def test_null_percentage(self):
        """Test null percentage calculation."""
        profile_data = {
            "column_name": "test_col",
            "declared_type": None,
            "sample_values": [],
            "type_counts": {"string": 5},
            "null_count": 5,
            "value_counts": {},
            "numeric_values": [],
            "string_lengths": [],
            "patterns": {},
            "inferred_type": "string",
            "total_processed": 5
        }

        stats = self.profiler.stats_calculator.calculate_statistics(profile_data, total_rows=10)

        assert stats.count == 10
        assert stats.null_count == 5
        assert stats.null_percentage == 50.0

    def test_cardinality_calculation(self):
        """Test cardinality calculation."""
        profile_data = {
            "column_name": "test_col",
            "declared_type": None,
            "sample_values": [],
            "type_counts": {"string": 10},
            "null_count": 0,
            "value_counts": {"A": 5, "B": 5},  # 2 unique values out of 10
            "numeric_values": [],
            "string_lengths": [],
            "patterns": {},
            "inferred_type": "string",
            "total_processed": 10
        }

        stats = self.profiler.stats_calculator.calculate_statistics(profile_data, total_rows=10)

        assert stats.unique_count == 2
        assert stats.cardinality == 0.2  # 2/10

    def test_top_values(self):
        """Test top values calculation."""
        profile_data = {
            "column_name": "test_col",
            "declared_type": None,
            "sample_values": [],
            "type_counts": {"string": 10},
            "null_count": 0,
            "value_counts": {"A": 7, "B": 2, "C": 1},
            "numeric_values": [],
            "string_lengths": [],
            "patterns": {},
            "inferred_type": "string",
            "total_processed": 10
        }

        stats = self.profiler.stats_calculator.calculate_statistics(profile_data, total_rows=10)

        assert len(stats.top_values) == 3
        assert stats.top_values[0]["value"] == "A"
        assert stats.top_values[0]["count"] == 7
        assert stats.top_values[0]["percentage"] == 70.0
        assert stats.mode == "A"
        assert stats.mode_frequency == 7


class TestQualityMetrics:
    """Test quality metrics calculation."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()

    def test_completeness_high(self):
        """Test completeness metric with high completeness."""
        stats = ColumnStatistics(
            count=100,
            null_count=5,
            null_percentage=5.0,
            unique_count=50,
            cardinality=0.5
        )
        type_info = TypeInference(
            inferred_type="integer",
            confidence=1.0
        )
        profile_data = {"type_counts": {}}

        quality = self.profiler.stats_calculator.calculate_quality_metrics(
            profile_data, type_info, stats, total_rows=100
        )

        assert quality.completeness == 95.0
        assert len(quality.issues) == 0  # No issues for >90% completeness

    def test_completeness_low(self):
        """Test completeness metric with low completeness."""
        stats = ColumnStatistics(
            count=100,
            null_count=60,
            null_percentage=60.0,
            unique_count=20,
            cardinality=0.5
        )
        type_info = TypeInference(
            inferred_type="string",
            confidence=1.0
        )
        profile_data = {"type_counts": {}}

        quality = self.profiler.stats_calculator.calculate_quality_metrics(
            profile_data, type_info, stats, total_rows=100
        )

        assert quality.completeness == 40.0
        assert any("Low completeness" in issue for issue in quality.issues)

    def test_validity_score(self):
        """Test validity score calculation.

        For numeric types (integer/float), validity is always 100% because
        integer and float are considered compatible types. Use string type
        to test confidence-based validity.
        """
        stats = ColumnStatistics(count=100, null_count=0, cardinality=0.5)
        type_info = TypeInference(
            inferred_type="string",  # Use string to test confidence-based validity
            confidence=0.85  # 85% of values match type
        )
        profile_data = {"type_counts": {}}

        quality = self.profiler.stats_calculator.calculate_quality_metrics(
            profile_data, type_info, stats, total_rows=100
        )

        assert quality.validity == 85.0

    def test_validity_score_numeric_types(self):
        """Test that numeric types (integer/float) always have 100% validity.

        Integer and float are considered compatible types - values like 13.0
        detected as integer are still valid floats.
        """
        stats = ColumnStatistics(count=100, null_count=0, cardinality=0.5)
        type_info = TypeInference(
            inferred_type="integer",
            confidence=0.85  # Even with 85% confidence, numeric types get 100% validity
        )
        profile_data = {"type_counts": {}}

        quality = self.profiler.stats_calculator.calculate_quality_metrics(
            profile_data, type_info, stats, total_rows=100
        )

        # Numeric types always have 100% validity (integer/float compatibility)
        assert quality.validity == 100.0

    def test_uniqueness_all_unique(self):
        """Test uniqueness when all values are unique."""
        stats = ColumnStatistics(
            count=100,
            null_count=0,
            unique_count=100,
            cardinality=1.0
        )
        type_info = TypeInference(inferred_type="integer", confidence=1.0)
        profile_data = {"type_counts": {}}

        quality = self.profiler.stats_calculator.calculate_quality_metrics(
            profile_data, type_info, stats, total_rows=100
        )

        assert quality.uniqueness == 100.0
        # "All values are unique" is an observation, not an issue (potential key field)
        assert any("All values are unique" in obs for obs in quality.observations)

    def test_overall_quality_score(self):
        """Test overall quality score calculation."""
        stats = ColumnStatistics(
            count=100,
            null_count=10,
            null_percentage=10.0,
            unique_count=50,
            cardinality=0.5
        )
        type_info = TypeInference(inferred_type="integer", confidence=0.95)
        profile_data = {"type_counts": {}}

        quality = self.profiler.stats_calculator.calculate_quality_metrics(
            profile_data, type_info, stats, total_rows=100
        )

        # Overall = 0.3*completeness + 0.3*validity + 0.2*uniqueness + 0.2*consistency
        # For numeric types, validity is always 100% (integer/float compatibility)
        # = 0.3*90 + 0.3*100 + 0.2*50 + 0.2*100
        # = 27 + 30 + 10 + 20 = 87.0
        assert 86.0 <= quality.overall_score <= 88.0


class TestTypeInference:
    """Test type inference functionality."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()

    def test_infer_type_with_schema(self):
        """Test type inference when schema is declared."""
        profile_data = {
            "declared_type": "integer",
            "type_counts": {"integer": 90, "string": 10},
            "null_count": 0,
            "sample_values": [1, 2, 3]
        }

        type_info = self.profiler.type_inferrer.infer_column_type(profile_data, total_rows=100)

        assert type_info.inferred_type == "integer"
        assert type_info.is_known is True
        assert type_info.confidence == 1.0
        assert type_info.declared_type == "integer"

    def test_infer_type_without_schema(self):
        """Test type inference without declared schema."""
        # Mixed integer/string columns (5% string) now default to string
        # This is correct behavior for mixed alphanumeric columns like ticket IDs
        profile_data = {
            "declared_type": None,
            "type_counts": {"integer": 95, "string": 5},
            "null_count": 0,
            "sample_values": [1, 2, 3]
        }

        type_info = self.profiler.type_inferrer.infer_column_type(profile_data, total_rows=100)

        # With 5% string values, column is treated as string (mixed alphanumeric)
        assert type_info.inferred_type == "string"
        assert type_info.is_known is False
        assert type_info.confidence == 1.0  # (95 + 5) / 100 = 100%

    def test_infer_type_with_conflicts(self):
        """Test type inference with type conflicts."""
        # Mixed integer/string columns (15% string) now default to string
        # This correctly handles mixed alphanumeric data like ticket IDs
        profile_data = {
            "declared_type": None,
            "type_counts": {"integer": 80, "string": 15, "float": 5},
            "null_count": 0,
            "sample_values": [1, 2, "abc", 3.14]
        }

        type_info = self.profiler.type_inferrer.infer_column_type(profile_data, total_rows=100)

        # With 15% string values, column is treated as string (mixed alphanumeric)
        assert type_info.inferred_type == "string"
        # Confidence is (integer + string) / total = (80 + 15) / 100 = 95%
        assert type_info.confidence == 0.95
        # Conflicts detected before type conversion: sorted by count (string=15, float=5)
        assert len(type_info.type_conflicts) >= 2
        assert type_info.type_conflicts[0]["type"] == "string"  # Largest non-primary
        assert type_info.type_conflicts[1]["type"] == "float"

    def test_infer_empty_column(self):
        """Test type inference for empty column."""
        profile_data = {
            "declared_type": None,
            "type_counts": {},
            "null_count": 100,
            "sample_values": []
        }

        type_info = self.profiler.type_inferrer.infer_column_type(profile_data, total_rows=100)

        assert type_info.inferred_type == "empty"
        assert type_info.confidence == 0.0


class TestCorrelationCalculation:
    """Test correlation calculation."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()

    def test_calculate_positive_correlation(self):
        """Test calculation of positive correlation."""
        # Create strongly correlated data
        numeric_data = {
            "col1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "col2": [2.0, 4.0, 6.0, 8.0, 10.0]  # col2 = 2 * col1
        }

        correlations = self.profiler.stats_calculator.calculate_correlations(numeric_data, row_count=5)

        assert len(correlations) == 1
        assert correlations[0].column1 == "col1"
        assert correlations[0].column2 == "col2"
        assert correlations[0].correlation > 0.99  # Strong positive correlation

    def test_calculate_negative_correlation(self):
        """Test calculation of negative correlation."""
        numeric_data = {
            "col1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "col2": [10.0, 8.0, 6.0, 4.0, 2.0]  # col2 decreases as col1 increases
        }

        correlations = self.profiler.stats_calculator.calculate_correlations(numeric_data, row_count=5)

        assert len(correlations) == 1
        assert correlations[0].correlation < -0.99  # Strong negative correlation

    def test_no_correlation(self):
        """Test when correlation is not significant."""
        numeric_data = {
            "col1": [1.0, 2.0, 3.0, 4.0, 5.0],
            "col2": [1.0, 1.0, 1.0, 1.0, 1.0]  # Constant, no correlation
        }

        correlations = self.profiler.stats_calculator.calculate_correlations(numeric_data, row_count=5)

        assert len(correlations) == 0  # No significant correlations (threshold is 0.5)

    def test_single_column(self):
        """Test correlation with only one numeric column."""
        numeric_data = {
            "col1": [1.0, 2.0, 3.0, 4.0, 5.0]
        }

        correlations = self.profiler.stats_calculator.calculate_correlations(numeric_data, row_count=5)

        assert len(correlations) == 0  # Need at least 2 columns


class TestValidationSuggestions:
    """Test validation suggestion generation."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler()

    def test_suggest_mandatory_field_check(self):
        """Test suggestion for mandatory field check."""
        columns = [
            ColumnProfile(
                name="id",
                type_info=TypeInference(inferred_type="integer", confidence=1.0),
                statistics=ColumnStatistics(count=100, null_count=0, cardinality=1.0),
                quality=QualityMetrics(completeness=100.0, overall_score=95.0)
            )
        ]

        suggestions = self.profiler.validation_suggester.generate_suggestions(columns, row_count=100)

        # Should suggest MandatoryFieldCheck for field with >95% completeness
        mandatory_suggestions = [s for s in suggestions if s.validation_type == "MandatoryFieldCheck"]
        assert len(mandatory_suggestions) > 0
        assert "id" in mandatory_suggestions[0].params["fields"]

    def test_suggest_unique_key_check(self):
        """Test suggestion for unique key check."""
        columns = [
            ColumnProfile(
                name="id",
                type_info=TypeInference(inferred_type="integer", confidence=1.0),
                statistics=ColumnStatistics(
                    count=101,
                    null_count=0,
                    unique_count=101,
                    cardinality=1.0
                ),
                quality=QualityMetrics(completeness=100.0, uniqueness=100.0)
            )
        ]

        suggestions = self.profiler.validation_suggester.generate_suggestions(columns, row_count=101)

        # Should suggest UniqueKeyCheck for field with cardinality > 0.99 and row_count > 100
        unique_suggestions = [s for s in suggestions if s.validation_type == "UniqueKeyCheck"]
        assert len(unique_suggestions) > 0
        assert "id" in unique_suggestions[0].params["fields"]

    def test_suggest_range_check(self):
        """Test suggestion for range check."""
        columns = [
            ColumnProfile(
                name="age",
                type_info=TypeInference(inferred_type="integer", confidence=1.0),
                statistics=ColumnStatistics(
                    count=100,
                    null_count=0,
                    min_value=18,
                    max_value=65
                ),
                quality=QualityMetrics(completeness=100.0)
            )
        ]

        suggestions = self.profiler.validation_suggester.generate_suggestions(columns, row_count=100)

        # Should suggest RangeCheck for numeric field
        range_suggestions = [s for s in suggestions if s.validation_type == "RangeCheck"]
        assert len(range_suggestions) > 0
        assert range_suggestions[0].params["field"] == "age"
        # Smart range adds 20% buffer: range=47, buffer=9.4
        # "age" field is likely-positive so min is clamped to 0 or above
        # min = max(0, 18 - 9.4) = 8.6, max = 65 + 9.4 = 74.4
        assert range_suggestions[0].params["min_value"] == 8.6
        assert range_suggestions[0].params["max_value"] == 74.4

    def test_suggest_valid_values_check(self):
        """Test suggestion for valid values check."""
        columns = [
            ColumnProfile(
                name="status",
                type_info=TypeInference(inferred_type="string", confidence=1.0),
                statistics=ColumnStatistics(
                    count=100,
                    null_count=0,
                    unique_count=3,
                    cardinality=0.03,
                    top_values=[
                        {"value": "active", "count": 50, "percentage": 50.0},
                        {"value": "inactive", "count": 30, "percentage": 30.0},
                        {"value": "pending", "count": 20, "percentage": 20.0}
                    ]
                ),
                quality=QualityMetrics(completeness=100.0)
            )
        ]

        suggestions = self.profiler.validation_suggester.generate_suggestions(columns, row_count=100)

        # Should suggest ValidValuesCheck for low cardinality field
        valid_values_suggestions = [s for s in suggestions if s.validation_type == "ValidValuesCheck"]
        assert len(valid_values_suggestions) > 0
        assert valid_values_suggestions[0].params["field"] == "status"
        assert set(valid_values_suggestions[0].params["valid_values"]) == {"active", "inactive", "pending"}

    def test_suggest_file_level_checks(self):
        """Test file-level validation suggestions."""
        columns = []
        suggestions = self.profiler.validation_suggester.generate_suggestions(columns, row_count=1000)

        # Should always suggest EmptyFileCheck
        empty_file_suggestions = [s for s in suggestions if s.validation_type == "EmptyFileCheck"]
        assert len(empty_file_suggestions) > 0

        # Should suggest RowCountRangeCheck
        row_count_suggestions = [s for s in suggestions if s.validation_type == "RowCountRangeCheck"]
        assert len(row_count_suggestions) > 0


class TestEndToEndProfiling:
    """Test end-to-end profiling workflow."""

    def setup_method(self):
        """Setup test fixtures."""
        self.profiler = DataProfiler(chunk_size=1000)
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Cleanup test files."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_profile_simple_csv(self):
        """Test profiling a simple CSV file."""
        # Create test CSV
        test_file = os.path.join(self.test_dir, "test_data.csv")
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "active": [True, True, False, True, False]
        })
        df.to_csv(test_file, index=False)

        # Profile the file
        result = self.profiler.profile_file(test_file, file_format="csv")

        # Verify result structure
        assert isinstance(result, ProfileResult)
        assert result.row_count == 5
        assert result.column_count == 4
        assert result.file_name == "test_data.csv"
        assert result.format == "csv"

        # Verify columns profiled
        assert len(result.columns) == 4
        column_names = [col.name for col in result.columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "age" in column_names
        assert "active" in column_names

    def test_profile_with_nulls(self):
        """Test profiling file with null values."""
        test_file = os.path.join(self.test_dir, "test_nulls.csv")
        df = pd.DataFrame({
            "col1": [1, 2, None, 4, None],
            "col2": ["a", "b", "c", None, None]
        })
        df.to_csv(test_file, index=False)

        result = self.profiler.profile_file(test_file, file_format="csv")

        # Check null handling
        col1_profile = next(col for col in result.columns if col.name == "col1")
        assert col1_profile.statistics.null_count == 2
        assert col1_profile.statistics.null_percentage == 40.0
        assert col1_profile.quality.completeness == 60.0

    def test_profile_with_declared_schema(self):
        """Test profiling with declared schema."""
        test_file = os.path.join(self.test_dir, "test_schema.csv")
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10.5, 20.3, 30.1]
        })
        df.to_csv(test_file, index=False)

        declared_schema = {
            "id": "integer",
            "value": "float"
        }

        result = self.profiler.profile_file(
            test_file,
            file_format="csv",
            declared_schema=declared_schema
        )

        # Verify schema was used
        id_col = next(col for col in result.columns if col.name == "id")
        assert id_col.type_info.declared_type == "integer"
        assert id_col.type_info.is_known is True
        assert id_col.type_info.confidence == 1.0

    def test_profile_generates_config(self):
        """Test that profiling generates validation config."""
        test_file = os.path.join(self.test_dir, "test_config.csv")
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "status": ["active", "active", "inactive", "active", "pending"]
        })
        df.to_csv(test_file, index=False)

        result = self.profiler.profile_file(test_file, file_format="csv")

        # Verify config was generated
        assert result.generated_config_yaml is not None
        assert "validation_job:" in result.generated_config_yaml
        assert "EmptyFileCheck" in result.generated_config_yaml

        assert result.generated_config_command is not None
        assert "validate" in result.generated_config_command

    def test_profile_chunked_processing(self):
        """Test profiling with chunked processing."""
        test_file = os.path.join(self.test_dir, "test_large.csv")
        # Create dataset larger than chunk size
        df = pd.DataFrame({
            "id": range(1, 2001),
            "value": range(1, 2001)
        })
        df.to_csv(test_file, index=False)

        # Profile with small chunk size
        profiler = DataProfiler(chunk_size=500)
        result = profiler.profile_file(test_file, file_format="csv")

        # Should still get correct row count
        assert result.row_count == 2000

        # Should still calculate correct statistics
        value_col = next(col for col in result.columns if col.name == "value")
        assert value_col.statistics.min_value == 1.0
        assert value_col.statistics.max_value == 2000.0


class TestHTMLReporter:
    """Test HTML report generation."""

    def setup_method(self):
        """Setup test fixtures."""
        self.reporter = ProfileHTMLReporter()
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Cleanup test files."""
        import shutil
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_generate_html_report(self):
        """Test HTML report generation."""
        # Create minimal profile result
        profile = ProfileResult(
            file_name="test.csv",
            file_path="/tmp/test.csv",
            file_size_bytes=1024,
            format="csv",
            row_count=100,
            column_count=2,
            profiled_at=datetime.now(),
            processing_time_seconds=1.5,
            columns=[
                ColumnProfile(
                    name="id",
                    type_info=TypeInference(
                        inferred_type="integer",
                        confidence=1.0,
                        is_known=False
                    ),
                    statistics=ColumnStatistics(
                        count=100,
                        null_count=0,
                        unique_count=100,
                        cardinality=1.0,
                        min_value=1,
                        max_value=100,
                        mean=50.5
                    ),
                    quality=QualityMetrics(
                        completeness=100.0,
                        validity=100.0,
                        uniqueness=100.0,
                        overall_score=100.0
                    )
                )
            ],
            overall_quality_score=95.0,
            suggested_validations=[
                ValidationSuggestion(
                    validation_type="EmptyFileCheck",
                    severity="ERROR",
                    params={},
                    reason="Prevent empty files",
                    confidence=100.0
                )
            ]
        )

        # Generate report
        output_path = os.path.join(self.test_dir, "report.html")
        self.reporter.generate_report(profile, output_path)

        # Verify file was created
        assert os.path.exists(output_path)

        # Verify content
        with open(output_path, 'r') as f:
            html_content = f.read()

        # Check for key sections
        assert "<!DOCTYPE html>" in html_content
        assert "Data Profile Report" in html_content
        assert "test.csv" in html_content
        assert "Overall Quality" in html_content
        assert "Column Profiles" in html_content
        assert "Suggested Validations" in html_content

    def test_html_escaping(self):
        """Test HTML special character escaping."""
        test_string = '<script>alert("XSS")</script>'
        escaped = self.reporter._escape_html(test_string)

        assert "&lt;" in escaped
        assert "&gt;" in escaped
        assert "<script>" not in escaped

    def test_format_file_size(self):
        """Test file size formatting."""
        assert self.reporter._format_file_size(500) == "500 B"
        assert "KB" in self.reporter._format_file_size(2048)
        assert "MB" in self.reporter._format_file_size(2 * 1024 * 1024)
        assert "GB" in self.reporter._format_file_size(2 * 1024 * 1024 * 1024)

    def test_quality_class_assignment(self):
        """Test quality class assignment."""
        assert self.reporter._get_quality_class(95.0) == "quality-excellent"
        assert self.reporter._get_quality_class(80.0) == "quality-good"
        assert self.reporter._get_quality_class(60.0) == "quality-fair"
        assert self.reporter._get_quality_class(40.0) == "quality-poor"


class TestDataStructures:
    """Test data structure conversions."""

    def test_type_inference_to_dict(self):
        """Test TypeInference to_dict conversion."""
        type_info = TypeInference(
            declared_type="integer",
            inferred_type="integer",
            confidence=0.95,
            is_known=True,
            sample_values=[1, 2, 3]
        )

        result = type_info.to_dict()

        assert result["declared_type"] == "integer"
        assert result["inferred_type"] == "integer"
        assert result["confidence"] == 0.95
        assert result["is_known"] is True

    def test_column_statistics_to_dict(self):
        """Test ColumnStatistics to_dict conversion."""
        stats = ColumnStatistics(
            count=100,
            null_count=10,
            null_percentage=10.0,
            unique_count=50,
            min_value=1.0,
            max_value=100.0,
            mean=50.5
        )

        result = stats.to_dict()

        assert result["count"] == 100
        assert result["null_count"] == 10
        assert result["unique_count"] == 50
        assert result["min_value"] == "1.0"
        assert "mean" in result

    def test_quality_metrics_to_dict(self):
        """Test QualityMetrics to_dict conversion."""
        quality = QualityMetrics(
            completeness=95.0,
            validity=98.0,
            uniqueness=75.0,
            consistency=90.0,
            overall_score=92.5,
            issues=["Issue 1", "Issue 2"]
        )

        result = quality.to_dict()

        assert result["completeness"] == 95.0
        assert result["validity"] == 98.0
        assert len(result["issues"]) == 2

    def test_profile_result_to_dict(self):
        """Test ProfileResult to_dict conversion."""
        profile = ProfileResult(
            file_name="test.csv",
            file_path="/tmp/test.csv",
            file_size_bytes=1024 * 1024,  # 1 MB
            format="csv",
            row_count=100,
            column_count=2,
            profiled_at=datetime(2025, 1, 13, 12, 0, 0),
            processing_time_seconds=2.5,
            columns=[],
            overall_quality_score=90.0
        )

        result = profile.to_dict()

        assert result["file_name"] == "test.csv"
        assert result["row_count"] == 100
        assert result["file_size_mb"] == 1.0
        assert "profiled_at" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
