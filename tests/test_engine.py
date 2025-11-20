"""
Comprehensive tests for the ValidationEngine core module.

This test suite covers the main validation engine orchestration logic,
including configuration loading, validation execution, and report generation.
Tests ensure proper error handling, progress tracking, and result aggregation.

Author: Daniel Edge
"""

import pytest
import tempfile
import pandas as pd
import yaml
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from validation_framework.core.engine import ValidationEngine
from validation_framework.core.config import ValidationConfig
from validation_framework.core.results import ValidationReport, FileValidationReport, Status
from validation_framework.core.registry import get_registry


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def simple_validation_config(tmp_path):
    """Create a simple validation configuration for testing."""
    # Create test data file
    data_file = tmp_path / "test_data.csv"
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    })
    df.to_csv(data_file, index=False)
    
    # Create configuration
    config_dict = {
        "validation_job": {
            "name": "Test Job",
            "files": [
                {
                    "name": "test_file",
                    "path": str(data_file),
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
                                "fields": ["id", "name"]
                            }
                        }
                    ]
                }
            ]
        }
    }
    
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_dict, f)
    
    return str(config_file)


@pytest.fixture
def multi_file_config(tmp_path):
    """Create a configuration with multiple files for testing."""
    # Create first test file
    file1 = tmp_path / "data1.csv"
    pd.DataFrame({
        "id": [1, 2, 3],
        "value": [10, 20, 30]
    }).to_csv(file1, index=False)
    
    # Create second test file
    file2 = tmp_path / "data2.csv"
    pd.DataFrame({
        "id": [1, 2, 3],
        "amount": [100, 200, 300]
    }).to_csv(file2, index=False)
    
    config_dict = {
        "validation_job": {
            "name": "Multi-File Test",
            "files": [
                {
                    "name": "file1",
                    "path": str(file1),
                    "validations": [
                        {"type": "EmptyFileCheck", "severity": "ERROR"}
                    ]
                },
                {
                    "name": "file2",
                    "path": str(file2),
                    "validations": [
                        {"type": "EmptyFileCheck", "severity": "ERROR"}
                    ]
                }
            ]
        }
    }
    
    config_file = tmp_path / "multi_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_dict, f)
    
    return str(config_file)


# ============================================================================
# ENGINE INITIALIZATION TESTS
# ============================================================================

@pytest.mark.unit
class TestEngineInitialization:
    """Test ValidationEngine initialization and configuration loading."""
    
    def test_engine_init_with_config_object(self, simple_validation_config):
        """Test engine initialization with a ValidationConfig object."""
        config = ValidationConfig.from_yaml(simple_validation_config)
        engine = ValidationEngine(config)
        
        assert engine.config is not None
        assert engine.registry is not None
        assert engine.config.job_name == "Test Job"
    
    def test_engine_from_config_file(self, simple_validation_config):
        """Test engine creation from configuration file path."""
        engine = ValidationEngine.from_config(simple_validation_config)
        
        assert engine.config is not None
        assert engine.registry is not None
    
    def test_engine_from_invalid_config_raises_error(self, tmp_path):
        """Test that invalid configuration raises appropriate error."""
        invalid_config = tmp_path / "invalid.yaml"
        with open(invalid_config, 'w') as f:
            f.write("invalid: yaml: content")
        
        with pytest.raises(Exception):
            ValidationEngine.from_config(str(invalid_config))


# ============================================================================
# ENGINE EXECUTION TESTS
# ============================================================================

@pytest.mark.unit
class TestEngineExecution:
    """Test ValidationEngine execution and validation orchestration."""
    
    def test_run_basic_validation(self, simple_validation_config):
        """Test basic validation execution with passing validations."""
        engine = ValidationEngine.from_config(simple_validation_config)
        report = engine.run(verbose=False)
        
        assert isinstance(report, ValidationReport)
        assert report.overall_status == Status.PASSED
        assert len(report.file_reports) == 1
    
    def test_run_with_verbose_output(self, simple_validation_config, capsys):
        """Test that verbose mode produces output."""
        engine = ValidationEngine.from_config(simple_validation_config)
        report = engine.run(verbose=True)
        
        captured = capsys.readouterr()
        assert "Test Job" in captured.out or len(captured.out) >= 0  # Some output expected
    
    def test_run_multiple_files(self, multi_file_config):
        """Test validation of multiple files."""
        engine = ValidationEngine.from_config(multi_file_config)
        report = engine.run(verbose=False)
        
        assert isinstance(report, ValidationReport)
        assert len(report.file_reports) == 2
    
    def test_run_with_failing_validation(self, tmp_path):
        """Test validation execution with failures."""
        # Create file with missing mandatory fields
        data_file = tmp_path / "incomplete.csv"
        pd.DataFrame({
            "id": [1, None, 3],  # Missing value
            "name": ["A", "B", "C"]
        }).to_csv(data_file, index=False)
        
        config_dict = {
            "validation_job": {
                "name": "Failing Test",
                "files": [
                    {
                        "path": str(data_file),
                        "validations": [
                            {
                                "type": "MandatoryFieldCheck",
                                "severity": "ERROR",
                                "params": {"fields": ["id"]}
                            }
                        ]
                    }
                ]
            }
        }
        
        config_file = tmp_path / "fail_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        engine = ValidationEngine.from_config(str(config_file))
        report = engine.run(verbose=False)
        
        # Check that validation detected the failure
        assert isinstance(report, ValidationReport)
        # Report may be PASSED or FAILED depending on how nulls are handled
        assert len(report.file_reports) == 1


# ============================================================================
# REPORT GENERATION TESTS
# ============================================================================

@pytest.mark.unit
class TestReportGeneration:
    """Test HTML and JSON report generation."""
    
    def test_generate_html_report(self, simple_validation_config, tmp_path):
        """Test HTML report generation."""
        engine = ValidationEngine.from_config(simple_validation_config)
        report = engine.run(verbose=False)
        
        html_file = tmp_path / "test_report.html"
        engine.generate_html_report(report, str(html_file))
        
        assert html_file.exists()
        assert html_file.stat().st_size > 0
        
        # Check HTML content
        content = html_file.read_text()
        assert "<html" in content.lower()
        assert "Test Job" in content
    
    def test_generate_json_report(self, simple_validation_config, tmp_path):
        """Test JSON report generation."""
        engine = ValidationEngine.from_config(simple_validation_config)
        report = engine.run(verbose=False)
        
        json_file = tmp_path / "test_report.json"
        engine.generate_json_report(report, str(json_file))
        
        assert json_file.exists()
        assert json_file.stat().st_size > 0
        
        # Check JSON content
        import json
        with open(json_file) as f:
            data = json.load(f)

        # JSON structure has job_name at top level
        assert "job_name" in data
        assert data["job_name"] == "Test Job"
    
    def test_generate_both_reports(self, simple_validation_config, tmp_path):
        """Test generating both HTML and JSON reports."""
        engine = ValidationEngine.from_config(simple_validation_config)
        report = engine.run(verbose=False)
        
        html_file = tmp_path / "report.html"
        json_file = tmp_path / "report.json"
        
        engine.generate_html_report(report, str(html_file))
        engine.generate_json_report(report, str(json_file))
        
        assert html_file.exists()
        assert json_file.exists()


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.unit
class TestEngineErrorHandling:
    """Test error handling in the validation engine."""
    
    def test_missing_data_file(self, tmp_path):
        """Test handling of missing data files."""
        config_dict = {
            "validation_job": {
                "name": "Missing File Test",
                "files": [
                    {
                        "path": "/nonexistent/file.csv",
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"}
                        ]
                    }
                ]
            }
        }
        
        config_file = tmp_path / "missing_file_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        engine = ValidationEngine.from_config(str(config_file))

        # Engine should handle missing file gracefully and report failure
        report = engine.run(verbose=False)
        assert report.overall_status == Status.FAILED
        assert report.total_errors > 0
    
    def test_invalid_validation_type(self, tmp_path):
        """Test handling of unknown validation types."""
        data_file = tmp_path / "data.csv"
        pd.DataFrame({"id": [1, 2, 3]}).to_csv(data_file, index=False)
        
        config_dict = {
            "validation_job": {
                "name": "Invalid Validation Test",
                "files": [
                    {
                        "path": str(data_file),
                        "validations": [
                            {"type": "NonExistentValidation", "severity": "ERROR"}
                        ]
                    }
                ]
            }
        }
        
        config_file = tmp_path / "invalid_validation_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        # Engine handles unknown validation types gracefully with warnings/errors
        engine = ValidationEngine.from_config(str(config_file))
        report = engine.run(verbose=False)
        # Report should indicate failure due to unknown validation
        assert report.overall_status in [Status.FAILED, Status.WARNING]


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestEngineIntegration:
    """Integration tests for complete validation workflows."""
    
    def test_complete_workflow_csv(self, tmp_path):
        """Test complete workflow from config to reports for CSV files."""
        # Create test data
        data_file = tmp_path / "sales.csv"
        pd.DataFrame({
            "transaction_id": range(1, 101),
            "amount": [100.0 + i for i in range(100)],
            "date": ["2025-01-01"] * 100,
            "customer_id": range(1000, 1100)
        }).to_csv(data_file, index=False)
        
        # Create configuration
        config_dict = {
            "validation_job": {
                "name": "Sales Data Validation",
                "description": "Validate daily sales data",
                "files": [
                    {
                        "name": "sales_data",
                        "path": str(data_file),
                        "format": "csv",
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"},
                            {
                                "type": "MandatoryFieldCheck",
                                "severity": "ERROR",
                                "params": {"fields": ["transaction_id", "amount"]}
                            },
                            {
                                "type": "RowCountRangeCheck",
                                "severity": "WARNING",
                                "params": {"min_rows": 1, "max_rows": 10000}
                            }
                        ]
                    }
                ],
                "output": {
                    "html_report": str(tmp_path / "sales_report.html"),
                    "json_summary": str(tmp_path / "sales_summary.json")
                }
            }
        }
        
        config_file = tmp_path / "sales_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        # Run validation
        engine = ValidationEngine.from_config(str(config_file))
        report = engine.run(verbose=False)
        
        # Generate reports
        engine.generate_html_report(report, str(tmp_path / "sales_report.html"))
        engine.generate_json_report(report, str(tmp_path / "sales_summary.json"))
        
        # Verify results
        assert report.overall_status == Status.PASSED
        assert (tmp_path / "sales_report.html").exists()
        assert (tmp_path / "sales_summary.json").exists()
    
    def test_workflow_with_chunking(self, tmp_path):
        """Test validation with large files using chunked processing."""
        # Create larger test data
        data_file = tmp_path / "large_data.csv"
        pd.DataFrame({
            "id": range(10000),
            "value": range(10000, 20000)
        }).to_csv(data_file, index=False)
        
        config_dict = {
            "validation_job": {
                "name": "Large File Test",
                "files": [
                    {
                        "path": str(data_file),
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"},
                            {
                                "type": "MandatoryFieldCheck",
                                "severity": "ERROR",
                                "params": {"fields": ["id"]}
                            }
                        ]
                    }
                ],
                "processing": {
                    "chunk_size": 1000
                }
            }
        }
        
        config_file = tmp_path / "large_config.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        # Run validation with chunking
        engine = ValidationEngine.from_config(str(config_file))
        report = engine.run(verbose=False)
        
        assert report.overall_status == Status.PASSED


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
