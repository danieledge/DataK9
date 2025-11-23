"""
Integration tests for CLI pattern expansion.

Tests that CLI commands properly expand date/time patterns in output paths.
"""

import pytest
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
import yaml

from validation_framework.utils.path_patterns import PathPatternExpander
from validation_framework.core.config import ValidationConfig


class TestValidateCLIPatterns:
    """Test pattern expansion in validate CLI command."""

    def test_config_pattern_expansion(self, tmp_path):
        """Test that YAML config patterns are expanded."""
        # Create test config with patterns
        config_dict = {
            'validation_job': {
                'name': 'Test Job',
                'version': '1.0',
                'files': [{
                    'name': 'test_file',
                    'path': str(tmp_path / 'test.csv'),
                    'format': 'csv',
                    'validations': []
                }],
                'output': {
                    'html_report': 'reports/{job_name}_{date}.html',
                    'json_summary': 'results/{job_name}_{timestamp}.json'
                }
            }
        }

        # Parse config (which should expand patterns)
        config = ValidationConfig(config_dict)

        # Verify patterns were expanded
        assert config.html_report_path != 'reports/{job_name}_{date}.html'
        assert 'Test_Job' in config.html_report_path
        assert '.html' in config.html_report_path

        assert config.json_summary_path != 'results/{job_name}_{timestamp}.json'
        assert 'Test_Job' in config.json_summary_path
        assert '.json' in config.json_summary_path

    def test_config_timestamp_consistency(self, tmp_path):
        """Test that timestamp is consistent in HTML and JSON paths."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)

        config_dict = {
            'validation_job': {
                'name': 'Test Job',
                'files': [{
                    'name': 'test',
                    'path': str(tmp_path / 'test.csv'),
                    'format': 'csv',
                    'validations': []
                }],
                'output': {
                    'html_report': '{timestamp}.html',
                    'json_summary': '{timestamp}.json'
                }
            }
        }

        config = ValidationConfig(config_dict, run_timestamp=test_timestamp)

        # Extract timestamps from both paths
        html_ts = config.html_report_path.replace('.html', '')
        json_ts = config.json_summary_path.replace('.json', '')

        assert html_ts == json_ts
        assert html_ts == '20251122_143045'


class TestProfileCLIPatterns:
    """Test pattern expansion in profile CLI command."""

    def test_profile_default_patterns(self):
        """Test that profile command generates correct default patterns."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        # Simulate default pattern expansion for file profiling
        context = {'file_name': 'customers'}
        html_template = "{file_name}_profile_report_{date}.html"
        config_template = "{file_name}_validation_{timestamp}.yaml"

        html_output = expander.expand(html_template, context)
        config_output = expander.expand(config_template, context)

        assert html_output == "customers_profile_report_2025-11-22.html"
        assert config_output == "customers_validation_20251122_143045.yaml"

    def test_profile_database_patterns(self):
        """Test pattern expansion for database profiling."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        # Simulate database profiling pattern expansion
        context = {'table_name': 'orders'}
        html_template = "{table_name}_profile_report_{date}.html"

        html_output = expander.expand(html_template, context)

        assert html_output == "orders_profile_report_2025-11-22.html"


class TestCDAAnalysisCLIPatterns:
    """Test pattern expansion in cda-analysis CLI command."""

    def test_cda_default_pattern(self):
        """Test CDA analysis default pattern expansion."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        # Simulate default CDA analysis pattern
        default_template = "cda_gap_analysis_{timestamp}.html"
        output = expander.expand(default_template, {})

        assert output == "cda_gap_analysis_20251122_143045.html"

    def test_cda_custom_pattern_with_job_name(self):
        """Test CDA analysis with custom pattern including job name."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        context = {'job_name': 'Customer Validation'}
        custom_template = "cda_reports/{job_name}_{date}.html"

        output = expander.expand(custom_template, context)

        assert "Customer_Validation" in output
        assert "2025-11-22" in output


class TestPatternExpansionEdgeCases:
    """Test edge cases in pattern expansion across all CLI commands."""

    def test_nested_directories_created(self, tmp_path):
        """Test that nested directories are created for patterns."""
        expander = PathPatternExpander()

        nested_path = str(tmp_path / "reports" / "{date}" / "file.html")
        expanded = expander.expand(nested_path, {})

        # Verify directory was created
        parent_dir = Path(expanded).parent
        assert parent_dir.exists()

    def test_sanitization_in_paths(self):
        """Test that special characters in job names are sanitized."""
        expander = PathPatternExpander()

        context = {'job_name': 'Data/Validation (2024)'}
        template = "reports/{job_name}/file.html"

        output = expander.expand(template, context)

        # Verify special characters were sanitized
        assert '/' not in Path(output).parts[-2]
        assert '(' not in output
        assert ')' not in output

    def test_multiple_pattern_types_together(self):
        """Test mixing date/time and context patterns."""
        test_timestamp = datetime(2025, 11, 22, 14, 30, 45)
        expander = PathPatternExpander(run_timestamp=test_timestamp)

        context = {
            'job_name': 'My Job',
            'file_name': 'data',
            'table_name': 'customers'
        }

        template = "output/{date}/{job_name}/{file_name}_{table_name}_{timestamp}.html"
        output = expander.expand(template, context)

        assert "2025-11-22" in output
        assert "My_Job" in output
        assert "data" in output
        assert "customers" in output
        assert "20251122_143045" in output


class TestBackwardCompatibility:
    """Test that existing configs without patterns still work."""

    def test_literal_paths_unchanged(self, tmp_path):
        """Test that paths without patterns remain unchanged."""
        config_dict = {
            'validation_job': {
                'name': 'Test Job',
                'files': [{
                    'name': 'test',
                    'path': str(tmp_path / 'test.csv'),
                    'format': 'csv',
                    'validations': []
                }],
                'output': {
                    'html_report': 'validation_report.html',
                    'json_summary': 'validation_summary.json'
                }
            }
        }

        config = ValidationConfig(config_dict)

        # Paths should remain as-is (no expansion)
        assert config.html_report_path == 'validation_report.html'
        assert config.json_summary_path == 'validation_summary.json'

    def test_missing_output_section(self, tmp_path):
        """Test config without output section uses defaults."""
        config_dict = {
            'validation_job': {
                'name': 'Test Job',
                'files': [{
                    'name': 'test',
                    'path': str(tmp_path / 'test.csv'),
                    'format': 'csv',
                    'validations': []
                }]
            }
        }

        config = ValidationConfig(config_dict)

        # Should use default values
        assert config.html_report_path == 'validation_report.html'
        assert config.json_summary_path == 'validation_summary.json'
