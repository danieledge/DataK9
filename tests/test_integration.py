"""
Integration tests for the validation framework.

Tests end-to-end workflows and integration between components.
"""

import pytest
import tempfile
import yaml
import pandas as pd
from pathlib import Path

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.config import ValidationConfig
from validation_framework.core.results import Status


@pytest.fixture
def sample_csv_data():
    """Create sample CSV data for testing."""
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "email": ["alice@test.com", "bob@test.com", "invalid_email", "david@test.com", "eve@test.com"],
        "age": [25, 30, 35, 40, 45],
        "balance": [100.50, 200.75, 300.00, 400.25, 500.50],
        "status": ["active", "active", "inactive", "active", "active"]
    })


@pytest.fixture
def temp_data_file(sample_csv_data):
    """Create temporary data file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_csv_data.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink()


@pytest.fixture
def sample_config(temp_data_file):
    """Create sample configuration dictionary."""
    return {
        "validation_job": {
            "name": "Integration Test Validation",
            "version": "1.0",
            "description": "Test validation job for integration tests",
            "files": [
                {
                    "name": "test_customers",
                    "path": temp_data_file,
                    "format": "csv",
                    "validations": [
                        {
                            "type": "EmptyFileCheck",
                            "severity": "ERROR"
                        },
                        {
                            "type": "RowCountRangeCheck",
                            "severity": "WARNING",
                            "params": {
                                "min_rows": 1,
                                "max_rows": 1000
                            }
                        },
                        {
                            "type": "MandatoryFieldCheck",
                            "severity": "ERROR",
                            "params": {
                                "fields": ["customer_id", "name", "email"]
                            }
                        },
                        {
                            "type": "RangeCheck",
                            "severity": "WARNING",
                            "params": {
                                "field": "age",
                                "min_value": 18,
                                "max_value": 100
                            }
                        }
                    ]
                }
            ],
            "output": {
                "html_report": "test_report.html",
                "json_summary": "test_summary.json",
                "fail_on_error": False,
                "fail_on_warning": False
            }
        }
    }


@pytest.fixture
def temp_config_file(sample_config):
    """Create temporary configuration file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_config, f)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink()


@pytest.mark.integration
class TestEndToEndValidation:
    """End-to-end integration tests."""

    def test_full_validation_workflow(self, temp_config_file):
        """Test complete validation workflow from config to results."""
        # Create engine from config
        engine = ValidationEngine.from_config(temp_config_file)

        assert engine.config.job_name == "Integration Test Validation"
        assert len(engine.config.files) == 1

        # Run validation
        report = engine.run(verbose=False)

        # Verify report structure
        assert report.job_name == "Integration Test Validation"
        assert len(report.file_reports) == 1
        assert report.duration_seconds > 0

        # Verify file report
        file_report = report.file_reports[0]
        assert file_report.file_name == "test_customers"
        assert file_report.total_validations == 4
        assert file_report.execution_time > 0

    def test_validation_with_failures(self, temp_data_file):
        """Test validation that should produce failures."""
        config_dict = {
            "validation_job": {
                "name": "Test with Failures",
                "files": [
                    {
                        "name": "test_file",
                        "path": temp_data_file,
                        "format": "csv",
                        "validations": [
                            {
                                "type": "RegexCheck",
                                "severity": "ERROR",
                                "params": {
                                    "field": "email",
                                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                                }
                            }
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        # Should have at least one error (invalid_email)
        assert report.total_errors > 0
        assert report.overall_status == Status.FAILED

        # Verify validation result details
        file_report = report.file_reports[0]
        validation_result = file_report.validation_results[0]
        assert validation_result.passed is False
        assert validation_result.failed_count > 0
        assert len(validation_result.sample_failures) > 0

    def test_validation_all_passing(self, temp_data_file):
        """Test validation where all checks pass."""
        config_dict = {
            "validation_job": {
                "name": "All Passing Test",
                "files": [
                    {
                        "name": "test_file",
                        "path": temp_data_file,
                        "format": "csv",
                        "validations": [
                            {
                                "type": "EmptyFileCheck",
                                "severity": "ERROR"
                            },
                            {
                                "type": "MandatoryFieldCheck",
                                "severity": "ERROR",
                                "params": {
                                    "fields": ["customer_id", "name"]
                                }
                            },
                            {
                                "type": "RangeCheck",
                                "severity": "ERROR",
                                "params": {
                                    "field": "age",
                                    "min_value": 0,
                                    "max_value": 150
                                }
                            }
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        # All validations should pass
        assert report.total_errors == 0
        assert report.overall_status == Status.PASSED

        # Verify all individual validations passed
        file_report = report.file_reports[0]
        for validation_result in file_report.validation_results:
            assert validation_result.passed is True

    def test_multiple_files_validation(self, temp_data_file, sample_csv_data):
        """Test validation with multiple files."""
        # Create second data file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_csv_data.to_csv(f.name, index=False)
            temp_path2 = f.name

        try:
            config_dict = {
                "validation_job": {
                    "name": "Multi-File Test",
                    "files": [
                        {
                            "name": "file1",
                            "path": temp_data_file,
                            "format": "csv",
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"}
                            ]
                        },
                        {
                            "name": "file2",
                            "path": temp_path2,
                            "format": "csv",
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"}
                            ]
                        }
                    ]
                }
            }

            config = ValidationConfig(config_dict)
            engine = ValidationEngine(config)
            report = engine.run(verbose=False)

            # Should have results for both files
            assert len(report.file_reports) == 2
            assert report.file_reports[0].file_name == "file1"
            assert report.file_reports[1].file_name == "file2"
        finally:
            Path(temp_path2).unlink()

    def test_report_generation(self, temp_config_file):
        """Test HTML and JSON report generation."""
        engine = ValidationEngine.from_config(temp_config_file)
        report = engine.run(verbose=False)

        # Generate reports in temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            html_path = Path(temp_dir) / "test_report.html"
            json_path = Path(temp_dir) / "test_summary.json"

            engine.generate_html_report(report, str(html_path))
            engine.generate_json_report(report, str(json_path))

            # Verify files were created
            assert html_path.exists()
            assert json_path.exists()

            # Verify HTML file has content
            html_content = html_path.read_text()
            assert "Integration Test Validation" in html_content
            assert "test_customers" in html_content

            # Verify JSON file has valid structure
            import json
            json_data = json.loads(json_path.read_text())
            assert json_data["job_name"] == "Integration Test Validation"
            assert "overall_status" in json_data
            assert "files" in json_data


@pytest.mark.integration
class TestConfigIntegration:
    """Integration tests for configuration handling."""

    def test_config_with_all_options(self, temp_data_file):
        """Test configuration with all possible options."""
        config_dict = {
            "validation_job": {
                "name": "Full Config Test",
                "version": "2.0",
                "description": "Comprehensive config test",
                "files": [
                    {
                        "name": "test_file",
                        "path": temp_data_file,
                        "format": "csv",
                        "delimiter": ",",
                        "encoding": "utf-8",
                        "header": 0,
                        "validations": [
                            {
                                "type": "EmptyFileCheck",
                                "severity": "ERROR",
                                "description": "Check if file is empty"
                            }
                        ],
                        "metadata": {
                            "source": "test",
                            "owner": "test_user"
                        }
                    }
                ],
                "output": {
                    "html_report": "custom_report.html",
                    "json_summary": "custom_summary.json",
                    "fail_on_error": True,
                    "fail_on_warning": True
                },
                "processing": {
                    "chunk_size": 10000,
                    "parallel_files": False,
                    "max_sample_failures": 50
                }
            }
        }

        config = ValidationConfig(config_dict)

        # Verify all options were parsed
        assert config.job_name == "Full Config Test"
        assert config.version == "2.0"
        assert config.description == "Comprehensive config test"
        assert config.chunk_size == 10000
        assert config.max_sample_failures == 50
        assert config.fail_on_error is True
        assert config.fail_on_warning is True

        # Run validation to ensure config works
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        assert report.description == "Comprehensive config test"


@pytest.mark.integration
@pytest.mark.slow
class TestLargeDatasetIntegration:
    """Integration tests with larger datasets."""

    def test_chunked_processing(self):
        """Test that chunked processing works with larger datasets."""
        # Create a larger dataset
        large_df = pd.DataFrame({
            "id": range(10000),
            "value": range(10000, 20000)
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            large_df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            config_dict = {
                "validation_job": {
                    "name": "Large Dataset Test",
                    "files": [
                        {
                            "name": "large_file",
                            "path": temp_path,
                            "format": "csv",
                            "validations": [
                                {
                                    "type": "RowCountRangeCheck",
                                    "severity": "ERROR",
                                    "params": {
                                        "min_rows": 9000,
                                        "max_rows": 12000  # Allow for estimation variance
                                    }
                                },
                                {
                                    "type": "MandatoryFieldCheck",
                                    "severity": "ERROR",
                                    "params": {
                                        "fields": ["id", "value"]
                                    }
                                }
                            ]
                        }
                    ],
                    "processing": {
                        "chunk_size": 1000  # Process in chunks of 1000
                    }
                }
            }

            config = ValidationConfig(config_dict)
            engine = ValidationEngine(config)
            report = engine.run(verbose=False)

            # Verify processing completed successfully
            assert report.overall_status == Status.PASSED
            assert report.total_errors == 0
        finally:
            Path(temp_path).unlink()
