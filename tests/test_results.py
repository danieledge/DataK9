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
            rule_name="TestCheck",
            passed=True,
            message="Validation passed",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=100,
            sample_failures=[]
        )
        
        assert result.rule_name == "TestCheck"
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
            rule_name="MandatoryCheck",
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
            rule_name="WarningCheck",
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
            rule_name="SerializeTest",
            passed=True,
            message="Test message",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=50,
            sample_failures=[]
        )
        
        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert result_dict["rule_name"] == "SerializeTest"
        assert result_dict["passed"] is True
        assert result_dict["failed_count"] == 0
        assert result_dict["total_count"] == 50
    
    def test_result_with_execution_time(self):
        """Test validation result with execution time."""
        result = ValidationResult(
            rule_name="TimedCheck",
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
        validation = ValidationResult(
            rule_name="Check1",
            passed=True,
            message="Pass",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=100,
            sample_failures=[]
        )

        report = FileValidationReport(
            file_name="test.csv",
            file_path="/path/to/test.csv",
            file_format="csv",
            status=Status.PASSED
        )
        report.add_result(validation)

        assert report.file_name == "test.csv"
        assert report.file_path == "/path/to/test.csv"
        assert report.metadata.get("total_rows", 0) == 0  # metadata is optional
        assert len(report.validation_results) == 1
    
    def test_file_report_status_all_passed(self):
        """Test file report status when all validations passed."""
        validations = [
            ValidationResult(
                rule_name=f"Check{i}",
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
            status=Status.PASSED
        )
        for validation in validations:
            report.add_result(validation)
        report.update_status()

        assert report.status == Status.PASSED
    
    def test_file_report_status_with_error(self):
        """Test file report status with ERROR failures."""
        validations = [
            ValidationResult(
                rule_name="PassingCheck",
                passed=True,
                message="Pass",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            ),
            ValidationResult(
                rule_name="FailingCheck",
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
            status=Status.PASSED
        )
        for validation in validations:
            report.add_result(validation)
        report.update_status()

        assert report.status == Status.FAILED
    
    def test_file_report_status_with_warning(self):
        """Test file report status with WARNING failures."""
        validations = [
            ValidationResult(
                rule_name="PassingCheck",
                passed=True,
                message="Pass",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            ),
            ValidationResult(
                rule_name="WarningCheck",
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
            status=Status.PASSED
        )
        for validation in validations:
            report.add_result(validation)
        report.update_status()

        assert report.status == Status.WARNING
    
    def test_file_report_serialization(self):
        """Test file report serialization to dict."""
        validation = ValidationResult(
            rule_name="TestCheck",
            passed=True,
            message="OK",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=50,
            sample_failures=[]
        )

        report = FileValidationReport(
            file_name="data.csv",
            file_path="/data.csv",
            file_format="csv",
            status=Status.PASSED,
            metadata={"total_rows": 50}
        )
        report.add_result(validation)

        report_dict = report.to_dict()

        assert isinstance(report_dict, dict)
        assert report_dict["file_name"] == "data.csv"
        assert report_dict["metadata"]["total_rows"] == 50
        assert "validation_results" in report_dict
        assert len(report_dict["validation_results"]) == 1


# ============================================================================
# VALIDATION REPORT TESTS
# ============================================================================

@pytest.mark.unit
class TestValidationReport:
    """Test ValidationReport class."""
    
    def test_create_validation_report(self):
        """Test creating a validation report."""
        exec_time = datetime(2025, 1, 1, 10, 0, 0)

        report = ValidationReport(
            job_name="Test Job",
            execution_time=exec_time,
            duration_seconds=300.0,
            overall_status=Status.PASSED
        )

        assert report.job_name == "Test Job"
        assert report.execution_time == exec_time
        assert report.duration_seconds == 300.0
        assert len(report.file_reports) == 0
    
    def test_validation_report_duration(self):
        """Test duration calculation in validation report."""
        exec_time = datetime(2025, 1, 1, 10, 0, 0)

        report = ValidationReport(
            job_name="Duration Test",
            execution_time=exec_time,
            duration_seconds=330.0,
            overall_status=Status.PASSED
        )

        assert report.duration_seconds == 330.0
    
    def test_validation_report_status_all_passed(self):
        """Test overall status when all files passed."""
        file_reports = []
        for i in range(3):
            validation = ValidationResult(
                rule_name="TestCheck",
                passed=True,
                message="OK",
                severity=Severity.ERROR,
                failed_count=0,
                total_count=100,
                sample_failures=[]
            )
            file_report = FileValidationReport(
                file_name=f"file{i}.csv",
                file_path=f"/file{i}.csv",
                file_format="csv",
                status=Status.PASSED
            )
            file_report.add_result(validation)
            file_report.update_status()
            file_reports.append(file_report)

        report = ValidationReport(
            job_name="All Passed",
            execution_time=datetime.now(),
            duration_seconds=10.0,
            overall_status=Status.PASSED
        )
        for fr in file_reports:
            report.add_file_report(fr)
        report.update_overall_status()

        assert report.overall_status == Status.PASSED
    
    def test_validation_report_status_with_failures(self):
        """Test overall status with some failures."""
        # Create passing file report
        pass_validation = ValidationResult(
            rule_name="PassCheck",
            passed=True,
            message="OK",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=100,
            sample_failures=[]
        )
        passing_file = FileValidationReport(
            file_name="pass.csv",
            file_path="/pass.csv",
            file_format="csv",
            status=Status.PASSED
        )
        passing_file.add_result(pass_validation)
        passing_file.update_status()

        # Create failing file report
        fail_validation = ValidationResult(
            rule_name="FailCheck",
            passed=False,
            message="Failed",
            severity=Severity.ERROR,
            failed_count=10,
            total_count=100,
            sample_failures=[]
        )
        failing_file = FileValidationReport(
            file_name="fail.csv",
            file_path="/fail.csv",
            file_format="csv",
            status=Status.PASSED
        )
        failing_file.add_result(fail_validation)
        failing_file.update_status()

        report = ValidationReport(
            job_name="Mixed Results",
            execution_time=datetime.now(),
            duration_seconds=10.0,
            overall_status=Status.PASSED
        )
        report.add_file_report(passing_file)
        report.add_file_report(failing_file)
        report.update_overall_status()

        assert report.overall_status == Status.FAILED
    
    def test_validation_report_serialization(self):
        """Test validation report serialization to dict."""
        validation = ValidationResult(
            rule_name="TestCheck",
            passed=True,
            message="OK",
            severity=Severity.ERROR,
            failed_count=0,
            total_count=50,
            sample_failures=[]
        )
        file_report = FileValidationReport(
            file_name="test.csv",
            file_path="/test.csv",
            file_format="csv",
            status=Status.PASSED
        )
        file_report.add_result(validation)

        report = ValidationReport(
            job_name="Serialize Test",
            execution_time=datetime(2025, 1, 1, 10, 0, 0),
            duration_seconds=300.0,
            overall_status=Status.PASSED
        )
        report.add_file_report(file_report)

        report_dict = report.to_dict()

        assert isinstance(report_dict, dict)
        assert report_dict["job_name"] == "Serialize Test"
        assert "overall_status" in report_dict
        assert "files" in report_dict
        assert len(report_dict["files"]) == 1
    
    def test_empty_validation_report(self):
        """Test validation report with no file reports."""
        report = ValidationReport(
            job_name="Empty Job",
            execution_time=datetime.now(),
            duration_seconds=0.0,
            overall_status=Status.PASSED
        )

        assert report.overall_status == Status.PASSED  # Empty should be considered passed
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
