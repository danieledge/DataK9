"""
Comprehensive tests for the reporter modules (HTML and JSON).

This test suite covers report generation, formatting, and output quality
for both HTML and JSON reporters. Tests ensure proper data serialization,
template rendering, and file output.

Author: Daniel Edge
"""

import pytest
import json
import tempfile
from pathlib import Path
from datetime import datetime

from validation_framework.reporters.html_reporter import HTMLReporter
from validation_framework.reporters.json_reporter import JSONReporter
from validation_framework.core.results import (
    ValidationReport,
    FileValidationReport,
    ValidationResult,
    Status,
    Severity
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_validation_result():
    """Create a sample validation result for testing."""
    return ValidationResult(
        rule_name="MandatoryFieldCheck",
        passed=True,
        message="All mandatory fields present",
        severity=Severity.ERROR,
        failed_count=0,
        total_count=100,
        sample_failures=[]
    )


@pytest.fixture
def failed_validation_result():
    """Create a failed validation result with sample failures."""
    return ValidationResult(
        rule_name="MandatoryFieldCheck",
        passed=False,
        message="Missing mandatory fields detected",
        severity=Severity.ERROR,
        failed_count=5,
        total_count=100,
        sample_failures=[
            {"row": 10, "field": "name", "value": None},
            {"row": 25, "field": "email", "value": None},
            {"row": 42, "field": "name", "value": None},
        ]
    )


@pytest.fixture
def file_validation_report(sample_validation_result, failed_validation_result):
    """Create a file validation report with mixed results."""
    report = FileValidationReport(
        file_name="test_data.csv",
        file_path="/path/to/test_data.csv",
        file_format="csv",
        status=Status.PASSED,
        metadata={"total_rows": 100}
    )
    report.add_result(sample_validation_result)
    report.add_result(failed_validation_result)
    report.update_status()
    return report


@pytest.fixture
def validation_report(file_validation_report):
    """Create a complete validation report."""
    report = ValidationReport(
        job_name="Test Validation Job",
        execution_time=datetime(2025, 1, 1, 10, 0, 0),
        duration_seconds=300.0,
        overall_status=Status.PASSED
    )
    report.add_file_report(file_validation_report)
    report.update_overall_status()
    return report


# ============================================================================
# HTML REPORTER TESTS
# ============================================================================

@pytest.mark.unit
class TestHTMLReporter:
    """Test HTML report generation."""
    
    def test_html_reporter_initialization(self):
        """Test HTMLReporter initialization."""
        reporter = HTMLReporter()
        assert reporter is not None
    
    def test_generate_html_report(self, validation_report, tmp_path):
        """Test basic HTML report generation."""
        reporter = HTMLReporter()
        output_file = tmp_path / "test_report.html"
        
        reporter.generate(validation_report, str(output_file))
        
        assert output_file.exists()
        assert output_file.stat().st_size > 0
    
    def test_html_report_contains_job_name(self, validation_report, tmp_path):
        """Test that HTML report contains validation job name."""
        reporter = HTMLReporter()
        output_file = tmp_path / "report.html"
        
        reporter.generate(validation_report, str(output_file))
        
        content = output_file.read_text()
        assert "Test Validation Job" in content
    
    def test_html_report_contains_file_info(self, validation_report, tmp_path):
        """Test that HTML report includes file information."""
        reporter = HTMLReporter()
        output_file = tmp_path / "report.html"
        
        reporter.generate(validation_report, str(output_file))
        
        content = output_file.read_text()
        assert "test_data.csv" in content
    
    def test_html_report_structure(self, validation_report, tmp_path):
        """Test that HTML report has proper HTML structure."""
        reporter = HTMLReporter()
        output_file = tmp_path / "report.html"
        
        reporter.generate(validation_report, str(output_file))
        
        content = output_file.read_text().lower()
        assert "<!doctype html>" in content or "<html" in content
        assert "<head>" in content or "<head" in content
        assert "<body>" in content or "<body" in content
    
    def test_html_report_with_passed_status(self, validation_report, tmp_path):
        """Test HTML report for passed validations."""
        # Modify report to be all passing
        for file_report in validation_report.file_reports:
            for validation in file_report.validation_results:
                validation.passed = True
                validation.failed_count = 0
        
        reporter = HTMLReporter()
        output_file = tmp_path / "passed_report.html"
        
        reporter.generate(validation_report, str(output_file))
        
        assert output_file.exists()
        content = output_file.read_text()
        # Should indicate success
        assert "pass" in content.lower() or "success" in content.lower()
    
    def test_html_report_with_failed_status(self, validation_report, tmp_path):
        """Test HTML report for failed validations."""
        # Ensure report has failures
        for file_report in validation_report.file_reports:
            file_report.validation_results[1].passed = False
            file_report.validation_results[1].failed_count = 5
        
        reporter = HTMLReporter()
        output_file = tmp_path / "failed_report.html"
        
        reporter.generate(validation_report, str(output_file))
        
        assert output_file.exists()
        content = output_file.read_text()
        # Should indicate failures
        assert "fail" in content.lower() or "error" in content.lower()
    
    def test_html_report_overwrite_existing(self, validation_report, tmp_path):
        """Test that HTML reporter can overwrite existing files."""
        reporter = HTMLReporter()
        output_file = tmp_path / "overwrite.html"
        
        # Create initial report
        reporter.generate(validation_report, str(output_file))
        initial_size = output_file.stat().st_size
        
        # Overwrite with same report
        reporter.generate(validation_report, str(output_file))
        final_size = output_file.stat().st_size
        
        assert output_file.exists()
        # Size should be similar (allowing for minor timestamp differences)
        assert abs(initial_size - final_size) < 1000


# ============================================================================
# JSON REPORTER TESTS
# ============================================================================

@pytest.mark.unit
class TestJSONReporter:
    """Test JSON report generation."""
    
    def test_json_reporter_initialization(self):
        """Test JSONReporter initialization."""
        reporter = JSONReporter()
        assert reporter is not None
    
    def test_generate_json_report(self, validation_report, tmp_path):
        """Test basic JSON report generation."""
        reporter = JSONReporter()
        output_file = tmp_path / "test_report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        assert output_file.exists()
        assert output_file.stat().st_size > 0
    
    def test_json_report_valid_json(self, validation_report, tmp_path):
        """Test that generated JSON is valid and parseable."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        # Should be parseable as JSON
        with open(output_file) as f:
            data = json.load(f)
        
        assert isinstance(data, dict)
    
    def test_json_report_contains_job_name(self, validation_report, tmp_path):
        """Test that JSON report contains validation job name."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        with open(output_file) as f:
            data = json.load(f)
        
        assert "job_name" in data
        assert data["job_name"] == "Test Validation Job"
    
    def test_json_report_contains_status(self, validation_report, tmp_path):
        """Test that JSON report includes overall status."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        with open(output_file) as f:
            data = json.load(f)
        
        assert "overall_status" in data
        assert data["overall_status"] in ["PASSED", "FAILED", "WARNING"]
    
    def test_json_report_contains_file_reports(self, validation_report, tmp_path):
        """Test that JSON report includes file-level reports."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        with open(output_file) as f:
            data = json.load(f)
        
        assert "files" in data
        assert isinstance(data["files"], list)
        assert len(data["files"]) == 1
    
    def test_json_report_file_details(self, validation_report, tmp_path):
        """Test that JSON report includes detailed file information."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        with open(output_file) as f:
            data = json.load(f)
        
        file_report = data["files"][0]
        assert "file_name" in file_report
        assert file_report["file_name"] == "test_data.csv"
        assert "metadata" in file_report
        assert file_report["metadata"]["total_rows"] == 100
    
    def test_json_report_validation_results(self, validation_report, tmp_path):
        """Test that JSON report includes validation results."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        with open(output_file) as f:
            data = json.load(f)
        
        validations = data["files"][0]["validation_results"]
        assert isinstance(validations, list)
        assert len(validations) == 2
        
        # Check validation structure
        validation = validations[0]
        assert "rule_name" in validation
        assert "passed" in validation
        assert "message" in validation
        assert "severity" in validation
    
    def test_json_report_timestamps(self, validation_report, tmp_path):
        """Test that JSON report includes timing information."""
        reporter = JSONReporter()
        output_file = tmp_path / "report.json"
        
        reporter.generate(validation_report, str(output_file))
        
        with open(output_file) as f:
            data = json.load(f)
        
        assert "execution_time" in data
        assert "duration_seconds" in data
    
    def test_json_report_overwrite_existing(self, validation_report, tmp_path):
        """Test that JSON reporter can overwrite existing files."""
        reporter = JSONReporter()
        output_file = tmp_path / "overwrite.json"
        
        # Create initial report
        reporter.generate(validation_report, str(output_file))
        
        # Overwrite
        reporter.generate(validation_report, str(output_file))
        
        assert output_file.exists()
        
        # Should still be valid JSON
        with open(output_file) as f:
            data = json.load(f)
        assert isinstance(data, dict)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestReporterIntegration:
    """Integration tests for both HTML and JSON reporters."""
    
    def test_generate_both_reports(self, validation_report, tmp_path):
        """Test generating both HTML and JSON reports for same validation."""
        html_reporter = HTMLReporter()
        json_reporter = JSONReporter()
        
        html_file = tmp_path / "report.html"
        json_file = tmp_path / "report.json"
        
        html_reporter.generate(validation_report, str(html_file))
        json_reporter.generate(validation_report, str(json_file))
        
        # Both should exist
        assert html_file.exists()
        assert json_file.exists()
        
        # Both should have content
        assert html_file.stat().st_size > 0
        assert json_file.stat().st_size > 0
        
        # JSON should be parseable
        with open(json_file) as f:
            data = json.load(f)
        assert data["job_name"] == "Test Validation Job"
    
    def test_reports_with_empty_validations(self, tmp_path):
        """Test report generation with no validations."""
        empty_report = ValidationReport(
            job_name="Empty Test",
            execution_time=datetime.now(),
            duration_seconds=0.0,
            overall_status=Status.PASSED
        )
        
        html_reporter = HTMLReporter()
        json_reporter = JSONReporter()
        
        html_file = tmp_path / "empty.html"
        json_file = tmp_path / "empty.json"
        
        html_reporter.generate(empty_report, str(html_file))
        json_reporter.generate(empty_report, str(json_file))
        
        assert html_file.exists()
        assert json_file.exists()
        
        # Verify JSON structure
        with open(json_file) as f:
            data = json.load(f)
        assert data["job_name"] == "Empty Test"
        assert len(data["files"]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
