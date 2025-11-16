"""
Comprehensive tests for the results module.

This test suite covers ValidationResult, FileValidationReport, and
ValidationReport classes, ensuring proper status tracking, serialization,
and aggregation of validation results.

Author: Daniel Edge
"""

import pytest
from datetime import datetime, timedelta

from validation_framework.core.results import (
    ValidationResult,
    FileValidationReport,
    ValidationReport,
    Status,
    Severity
)


# ============================================================================
# VALIDATION RESULT TESTS
# ============================================================================

@pytest.mark.unit
class TestValidationResult:
    """Test ValidationResult class."""
    
    def test_create_passing_result(self):
        """Test creating a passing validation result."""
        result = ValidationResult(
            validation_name="TestCheck",
            passed=True,
            message="Validation passed",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=100,
            sample_failures=[]
        )
        
        assert result.validation_name == "TestCheck"
        assert result.passed is True
        assert result.failed_count == 0
        assert result.total_count == 100
        assert len(result.sample_failures) == 0
    
    def test_create_failing_result(self):
        """Test creating a failing validation result."""
        failures = [
            {"row": 1, "column": "name", "value": None},
            {"row": 5, "column": "email", "value": "invalid"}
        ]
        
        result = ValidationResult(
            validation_name="MandatoryCheck",
            passed=False,
            message="Validation failed",
            severity=Severity.ERROR,
            failed_count=2,
            total_count=100,
            sample_failures=failures
        )
        
        assert result.passed is False
        assert result.failed_count == 2
        assert len(result.sample_failures) == 2
    
    def test_result_with_warning_severity(self):
        """Test validation result with WARNING severity."""
        result = ValidationResult(
            validation_name="WarningCheck",
            passed=False,
            message="Warning detected",
            severity=Severity.WARNING,
            failed_count=3,
            total_count=100,
            sample_failures=[]
        )
        
        assert result.severity == Severity.WARNING
        assert result.passed is False
    
    def test_result_serialization(self):
        """Test that ValidationResult can be converted to dict."""
        result = ValidationResult(
            validation_name="SerializeTest",
            passed=True,
            message="Test message",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=50,
            sample_failures=[]
        )
        
        result_dict = result.to_dict()
        
        assert isinstance(result_dict, dict)
        assert result_dict["validation_name"] == "SerializeTest"
        assert result_dict["passed"] is True
        assert result_dict["failed_count"] == 0
        assert result_dict["total_count"] == 50
    
    def test_result_with_execution_time(self):
        """Test validation result with execution time."""
        result = ValidationResult(
            validation_name="TimedCheck",
            passed=True,
            message="Completed in 2.5s",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=1000,
            sample_failures=[],
            execution_time=2.5
        )
        
        assert result.execution_time == 2.5


# ============================================================================
# FILE VALIDATION REPORT TESTS
# ============================================================================

@pytest.mark.unit
class TestFileValidationReport:
    """Test FileValidationReport class."""
    
    def test_create_file_report(self):
        """Test creating a file validation report."""
        validations = [
            ValidationResult(
                validation_name="Check1",
                passed=True,
                message="Pass",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            )
        ]
        
        report = FileValidationReport(
            file_name="test.csv",
            file_path="/path/to/test.csv",
            file_format="csv",
            total_rows=100,
            validations=validations
        )
        
        assert report.file_name == "test.csv"
        assert report.file_path == "/path/to/test.csv"
        assert report.total_rows == 100
        assert len(report.validations) == 1
    
    def test_file_report_status_all_passed(self):
        """Test file report status when all validations passed."""
        validations = [
            ValidationResult(
                validation_name=f"Check{i}",
                passed=True,
                message="Pass",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            )
            for i in range(3)
        ]
        
        report = FileValidationReport(
            file_name="test.csv",
            file_path="/path/to/test.csv",
            file_format="csv",
            total_rows=100,
            validations=validations
        )
        
        assert report.status == Status.PASSED
    
    def test_file_report_status_with_error(self):
        """Test file report status with ERROR failures."""
        validations = [
            ValidationResult(
                validation_name="PassingCheck",
                passed=True,
                message="Pass",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            ),
            ValidationResult(
                validation_name="FailingCheck",
                passed=False,
                message="Failed",
                severity=Severity.ERROR,
                failed_count=5,
                total_count=100,
                sample_failures=[]
            )
        ]
        
        report = FileValidationReport(
            file_name="test.csv",
            file_path="/path/to/test.csv",
            file_format="csv",
            total_rows=100,
            validations=validations
        )
        
        assert report.status == Status.FAILED
    
    def test_file_report_status_with_warning(self):
        """Test file report status with WARNING failures."""
        validations = [
            ValidationResult(
                validation_name="PassingCheck",
                passed=True,
                message="Pass",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            ),
            ValidationResult(
                validation_name="WarningCheck",
                passed=False,
                message="Warning",
                severity=Severity.WARNING,
                failed_count=2,
                total_count=100,
                sample_failures=[]
            )
        ]
        
        report = FileValidationReport(
            file_name="test.csv",
            file_path="/path/to/test.csv",
            file_format="csv",
            total_rows=100,
            validations=validations
        )
        
        assert report.status == Status.WARNING
    
    def test_file_report_serialization(self):
        """Test file report serialization to dict."""
        validations = [
            ValidationResult(
                validation_name="TestCheck",
                passed=True,
                message="OK",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=50,
                sample_failures=[]
            )
        ]
        
        report = FileValidationReport(
            file_name="data.csv",
            file_path="/data.csv",
            file_format="csv",
            total_rows=50,
            validations=validations
        )
        
        report_dict = report.to_dict()
        
        assert isinstance(report_dict, dict)
        assert report_dict["file_name"] == "data.csv"
        assert report_dict["total_rows"] == 50
        assert "validations" in report_dict
        assert len(report_dict["validations"]) == 1


# ============================================================================
# VALIDATION REPORT TESTS
# ============================================================================

@pytest.mark.unit
class TestValidationReport:
    """Test ValidationReport class."""
    
    def test_create_validation_report(self):
        """Test creating a validation report."""
        start_time = datetime(2025, 1, 1, 10, 0, 0)
        end_time = datetime(2025, 1, 1, 10, 5, 0)
        
        report = ValidationReport(
            validation_job_name="Test Job",
            start_time=start_time,
            end_time=end_time,
            file_reports=[]
        )
        
        assert report.validation_job_name == "Test Job"
        assert report.start_time == start_time
        assert report.end_time == end_time
        assert len(report.file_reports) == 0
    
    def test_validation_report_duration(self):
        """Test duration calculation in validation report."""
        start_time = datetime(2025, 1, 1, 10, 0, 0)
        end_time = datetime(2025, 1, 1, 10, 5, 30)
        
        report = ValidationReport(
            validation_job_name="Duration Test",
            start_time=start_time,
            end_time=end_time,
            file_reports=[]
        )
        
        assert report.duration == "5 minutes, 30 seconds" or "330" in report.duration
    
    def test_validation_report_status_all_passed(self):
        """Test overall status when all files passed."""
        file_reports = [
            FileValidationReport(
                file_name=f"file{i}.csv",
                file_path=f"/file{i}.csv",
                file_format="csv",
                total_rows=100,
                validations=[
                    ValidationResult(
                        validation_name="TestCheck",
                        passed=True,
                        message="OK",
                        severity=Severity.ERROR,
                        failed_count=0,
                        total_count=100,
                        sample_failures=[]
                    )
                ]
            )
            for i in range(3)
        ]
        
        report = ValidationReport(
            validation_job_name="All Passed",
            start_time=datetime.now(),
            end_time=datetime.now(),
            file_reports=file_reports
        )
        
        assert report.status == Status.PASSED
    
    def test_validation_report_status_with_failures(self):
        """Test overall status with some failures."""
        passing_file = FileValidationReport(
            file_name="pass.csv",
            file_path="/pass.csv",
            file_format="csv",
            total_rows=100,
            validations=[
                ValidationResult(
                    validation_name="PassCheck",
                    passed=True,
                    message="OK",
                    severity=Severity.ERROR,
                    failed_count=0,
                    total_count=100,
                    sample_failures=[]
                )
            ]
        )
        
        failing_file = FileValidationReport(
            file_name="fail.csv",
            file_path="/fail.csv",
            file_format="csv",
            total_rows=100,
            validations=[
                ValidationResult(
                    validation_name="FailCheck",
                    passed=False,
                    message="Failed",
                    severity=Severity.ERROR,
                    failed_count=10,
                    total_count=100,
                    sample_failures=[]
                )
            ]
        )
        
        report = ValidationReport(
            validation_job_name="Mixed Results",
            start_time=datetime.now(),
            end_time=datetime.now(),
            file_reports=[passing_file, failing_file]
        )
        
        assert report.status == Status.FAILED
    
    def test_validation_report_serialization(self):
        """Test validation report serialization to dict."""
        file_report = FileValidationReport(
            file_name="test.csv",
            file_path="/test.csv",
            file_format="csv",
            total_rows=50,
            validations=[
                ValidationResult(
                    validation_name="TestCheck",
                    passed=True,
                    message="OK",
                    severity=Severity.ERROR,
                    failed_count=0,
                    total_count=50,
                    sample_failures=[]
                )
            ]
        )
        
        report = ValidationReport(
            validation_job_name="Serialize Test",
            start_time=datetime(2025, 1, 1, 10, 0, 0),
            end_time=datetime(2025, 1, 1, 10, 5, 0),
            file_reports=[file_report]
        )
        
        report_dict = report.to_dict()
        
        assert isinstance(report_dict, dict)
        assert report_dict["validation_job_name"] == "Serialize Test"
        assert "status" in report_dict
        assert "file_reports" in report_dict
        assert len(report_dict["file_reports"]) == 1
    
    def test_empty_validation_report(self):
        """Test validation report with no file reports."""
        report = ValidationReport(
            validation_job_name="Empty Job",
            start_time=datetime.now(),
            end_time=datetime.now(),
            file_reports=[]
        )
        
        assert report.status == Status.PASSED  # Empty should be considered passed
        assert len(report.file_reports) == 0


# ============================================================================
# STATUS AND SEVERITY TESTS
# ============================================================================

@pytest.mark.unit
class TestStatusAndSeverity:
    """Test Status and Severity enums."""
    
    def test_status_values(self):
        """Test Status enum values."""
        assert Status.PASSED.value == "PASSED"
        assert Status.FAILED.value == "FAILED"
        assert Status.WARNING.value == "WARNING"
    
    def test_severity_values(self):
        """Test Severity enum values."""
        assert Severity.ERROR.value == "ERROR"
        assert Severity.WARNING.value == "WARNING"
    
    def test_status_comparison(self):
        """Test that statuses can be compared."""
        assert Status.PASSED == Status.PASSED
        assert Status.FAILED != Status.PASSED
        assert Status.WARNING != Status.FAILED


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
