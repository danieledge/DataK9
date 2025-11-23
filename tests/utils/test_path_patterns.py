"""
Unit tests for path pattern expansion utilities.

Tests pattern substitution for date/time and context-based filename generation.
"""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import shutil

from validation_framework.utils.path_patterns import PathPatternExpander, expand_path_patterns


class TestPathPatternExpander:
    """Test PathPatternExpander class functionality."""

    def test_date_pattern(self):
        """Test {date} pattern expansion."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        result = expander.expand("report_{date}.html", {})
        assert result == "report_2025-11-22.html"

    def test_time_pattern(self):
        """Test {time} pattern expansion."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        result = expander.expand("report_{time}.html", {})
        assert result == "report_14-30-45.html"

    def test_timestamp_pattern(self):
        """Test {timestamp} pattern expansion."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        result = expander.expand("report_{timestamp}.html", {})
        assert result == "report_20251122_143045.html"

    def test_datetime_pattern(self):
        """Test {datetime} pattern expansion."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        result = expander.expand("report_{datetime}.html", {})
        assert result == "report_2025-11-22_14-30-45.html"

    def test_job_name_pattern(self):
        """Test {job_name} pattern expansion with sanitization."""
        expander = PathPatternExpander()
        context = {'job_name': 'Customer Data Validation'}

        result = expander.expand("reports/{job_name}/report.html", context)
        assert "Customer_Data_Validation" in result

    def test_file_name_pattern(self):
        """Test {file_name} pattern expansion."""
        expander = PathPatternExpander()
        context = {'file_name': 'customers'}

        result = expander.expand("{file_name}_profile.html", context)
        assert result == "customers_profile.html"

    def test_table_name_pattern(self):
        """Test {table_name} pattern expansion."""
        expander = PathPatternExpander()
        context = {'table_name': 'orders'}

        result = expander.expand("{table_name}_validation.yaml", context)
        assert result == "orders_validation.yaml"

    def test_multiple_patterns(self):
        """Test expansion of multiple patterns in one path."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)
        context = {'job_name': 'My Job', 'file_name': 'data'}

        result = expander.expand(
            "reports/{date}/{job_name}/{file_name}_{time}.html",
            context
        )

        assert "2025-11-22" in result
        assert "My_Job" in result
        assert "data" in result
        assert "14-30-45" in result

    def test_timestamp_consistency(self):
        """Test that timestamp stays consistent across multiple expand calls."""
        expander = PathPatternExpander()

        result1 = expander.expand("report1_{timestamp}.html", {})
        result2 = expander.expand("report2_{timestamp}.html", {})

        # Extract timestamps
        ts1 = result1.replace("report1_", "").replace(".html", "")
        ts2 = result2.replace("report2_", "").replace(".html", "")

        assert ts1 == ts2

    def test_sanitize_filename_special_chars(self):
        """Test filename sanitization removes invalid characters."""
        expander = PathPatternExpander()

        # Test various invalid characters
        sanitized = expander._sanitize_filename('My/Data\\File:2024')
        assert '/' not in sanitized
        assert '\\' not in sanitized
        assert ':' not in sanitized
        assert sanitized == "My_Data_File_2024"

    def test_sanitize_filename_spaces(self):
        """Test filename sanitization converts spaces to underscores."""
        expander = PathPatternExpander()

        sanitized = expander._sanitize_filename('My File Name')
        assert sanitized == "My_File_Name"

    def test_sanitize_filename_repeated_underscores(self):
        """Test filename sanitization removes repeated underscores."""
        expander = PathPatternExpander()

        sanitized = expander._sanitize_filename('My___File___Name')
        assert sanitized == "My_File_Name"

    def test_sanitize_filename_length_limit(self):
        """Test filename sanitization respects length limit."""
        expander = PathPatternExpander()

        long_name = 'A' * 250  # Longer than MAX_FILENAME_LENGTH (200)
        sanitized = expander._sanitize_filename(long_name)

        assert len(sanitized) == 200

    def test_no_patterns(self):
        """Test path without patterns remains unchanged."""
        expander = PathPatternExpander()

        result = expander.expand("simple_report.html", {})
        assert result == "simple_report.html"

    def test_unknown_pattern_left_unchanged(self):
        """Test that unknown patterns are left unchanged."""
        expander = PathPatternExpander()

        result = expander.expand("report_{unknown_pattern}.html", {})
        assert result == "report_{unknown_pattern}.html"

    def test_empty_path(self):
        """Test empty path returns empty string."""
        expander = PathPatternExpander()

        result = expander.expand("", {})
        assert result == ""

    def test_none_path(self):
        """Test None path returns None."""
        expander = PathPatternExpander()

        result = expander.expand(None, {})
        assert result is None

    def test_directory_creation(self):
        """Test that parent directories are created."""
        expander = PathPatternExpander()

        # Create temp directory for testing
        temp_dir = tempfile.mkdtemp()
        try:
            nested_path = f"{temp_dir}/level1/level2/report.html"
            expander.expand(nested_path, {})

            # Check that parent directories were created
            assert Path(temp_dir, "level1", "level2").exists()

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_has_patterns_true(self):
        """Test has_patterns returns True for paths with patterns."""
        assert PathPatternExpander.has_patterns("report_{date}.html")
        assert PathPatternExpander.has_patterns("{timestamp}.json")
        assert PathPatternExpander.has_patterns("dir/{job_name}/file.txt")

    def test_has_patterns_false(self):
        """Test has_patterns returns False for paths without patterns."""
        assert not PathPatternExpander.has_patterns("report.html")
        assert not PathPatternExpander.has_patterns("simple/path/file.txt")
        assert not PathPatternExpander.has_patterns("")

    def test_context_none(self):
        """Test that None context is handled gracefully."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        result = expander.expand("report_{date}.html", None)
        assert result == "report_2025-11-22.html"

    def test_missing_context_value(self):
        """Test that missing context values result in pattern not being expanded."""
        expander = PathPatternExpander()
        context = {}  # Empty context, no job_name

        result = expander.expand("report_{job_name}.html", context)
        assert result == "report_{job_name}.html"


class TestConvenienceFunction:
    """Test expand_path_patterns convenience function."""

    def test_convenience_function_basic(self):
        """Test basic usage of convenience function."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)

        result = expand_path_patterns(
            "report_{date}.html",
            run_timestamp=test_timestamp
        )

        assert result == "report_2025-11-22.html"

    def test_convenience_function_with_context(self):
        """Test convenience function with context."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        context = {'job_name': 'My Job'}

        result = expand_path_patterns(
            "{job_name}_{timestamp}.html",
            context=context,
            run_timestamp=test_timestamp
        )

        assert "My_Job" in result
        assert "20251122_143045" in result


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_very_long_path(self):
        """Test handling of very long paths."""
        expander = PathPatternExpander()
        context = {'job_name': 'A' * 300}  # Very long job name

        # Should not crash, filename component will be truncated
        result = expander.expand("reports/{job_name}/file.html", context)
        assert result is not None

    def test_special_characters_in_context(self):
        """Test context values with special characters."""
        expander = PathPatternExpander()
        context = {'job_name': 'Data/Validation (2024)'}

        result = expander.expand("reports/{job_name}/file.html", context)

        # Special chars should be sanitized
        assert '/' not in Path(result).parts[-2]  # job_name part
        assert '(' not in result
        assert ')' not in result

    def test_numeric_patterns(self):
        """Test patterns that look like numbers."""
        expander = PathPatternExpander()

        # Should not crash on numeric-looking patterns
        result = expander.expand("report_{123}.html", {})
        assert result == "report_{123}.html"

    def test_unicode_in_job_name(self):
        """Test Unicode characters in job name."""
        expander = PathPatternExpander()
        context = {'job_name': 'Données Validation'}

        result = expander.expand("{job_name}_report.html", context)
        assert "Donn" in result or "Données" in result

    def test_nested_braces(self):
        """Test handling of nested braces."""
        expander = PathPatternExpander()

        # Should not crash on malformed patterns
        result = expander.expand("report_{{date}}.html", {})
        # Implementation note: This will try to expand {date} from {{date}}
        # Depending on regex, may or may not expand

    def test_mixed_pattern_and_literal(self):
        """Test mixing pattern placeholders with literal braces."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        result = expander.expand("report_{date}_version{1}.html", {})

        # {date} should expand, {1} should remain
        assert "2025-11-22" in result
        assert "version{1}" in result or "version1" in result
