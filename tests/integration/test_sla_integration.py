"""
Integration tests for SLA (Service Level Agreement) compliance framework.

Tests the full SLA evaluation flow from YAML config through validation
execution to SLA report generation.
"""

import pytest
import os
from pathlib import Path
from validation_framework.core.engine import ValidationEngine
from validation_framework.core.config import ValidationConfig
from validation_framework.cda import SLAStatus


# Path to test data
TEST_DATA_DIR = Path(__file__).parent.parent / "data" / "samples" / "csv"
SLA_TEST_FILE = TEST_DATA_DIR / "sla_test_transactions.csv"


class TestSLAIntegration:
    """Integration tests for SLA framework."""

    @pytest.fixture
    def sla_test_config(self):
        """Create test configuration with SLA settings."""
        return {
            "validation_job": {
                "name": "SLA Integration Test",
                "policy": "none",  # Disable policy for focused testing
                "sla_defaults": {
                    "warning_at": 0.8,
                    "default_tier": "standard"
                },
                "files": [{
                    "name": "transactions",
                    "path": str(SLA_TEST_FILE),
                    "format": "csv",
                    "quoting": 0,
                    "critical_data_attributes": [
                        {"field": "transaction_id", "tier": "critical"},
                        {"field": "customer_id", "tier": "critical"},
                        {"field": "email", "tier": "high"},
                        {"field": "phone", "tier": "standard"},
                        {"field": "amount", "tier": "high"},
                    ],
                    "validations": [{
                        "type": "MandatoryFieldCheck",
                        "severity": "ERROR",
                        "params": {
                            "fields": ["transaction_id", "customer_id", "email", "phone", "amount"]
                        }
                    }]
                }]
            }
        }

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_report_generated(self, sla_test_config):
        """Test that SLA report is generated after validation."""
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        # Check file report has SLA report attached
        file_report = report.file_reports[0]
        assert file_report.sla_report is not None
        assert file_report.sla_report.file_name == "transactions"

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_evaluates_all_cdas(self, sla_test_config):
        """Test that all CDAs are evaluated."""
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report
        assert len(sla_report.results) == 5  # 5 CDAs defined

        # Check all fields are represented
        fields = {r.field for r in sla_report.results}
        assert fields == {"transaction_id", "customer_id", "email", "phone", "amount"}

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_tier_assignment(self, sla_test_config):
        """Test that tiers are correctly assigned."""
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report
        results_by_field = {r.field: r for r in sla_report.results}

        assert results_by_field["transaction_id"].tier_name == "critical"
        assert results_by_field["customer_id"].tier_name == "critical"
        assert results_by_field["email"].tier_name == "high"
        assert results_by_field["phone"].tier_name == "standard"
        assert results_by_field["amount"].tier_name == "high"

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_detects_failures(self, sla_test_config):
        """Test that SLA correctly detects data quality failures.

        NOTE: When MandatoryFieldCheck validates multiple fields, the failed_row_ids
        represents the union of ALL rows that failed for ANY field. The SLA evaluator
        then uses this union for each CDA that the validator covers.

        The test dataset has 50 rows with these data quality issues:
        - Row 3: missing email
        - Row 5: missing phone
        - Row 8: missing customer_id
        - Row 11: missing email
        - Row 13: missing phone
        - Row 23: missing email
        - Row 26: missing phone

        Total: 7 rows with at least one missing value = 14% failure rate
        Since MandatoryFieldCheck covers all 5 CDAs, each CDA sees 7 bad records.
        """
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report
        results_by_field = {r.field: r for r in sla_report.results}

        # All fields have the same failure rate (union of all failures)
        # since they're all covered by the same MandatoryFieldCheck
        for field in ["transaction_id", "customer_id", "email", "phone", "amount"]:
            result = results_by_field[field]
            assert result.bad_records == 7  # 7 rows with any missing value
            assert result.evaluated_records == 50
            # 14% failure rate breaches all tiers except maybe 'low' (5%)
            # But we don't have a low tier CDA in this test

        # Critical tier CDAs (0% tolerance) are RED with any failure
        assert results_by_field["transaction_id"].status == SLAStatus.RED
        assert results_by_field["customer_id"].status == SLAStatus.RED

        # High tier CDAs (0.1% tolerance) are RED with 14% failure
        assert results_by_field["email"].status == SLAStatus.RED
        assert results_by_field["amount"].status == SLAStatus.RED

        # Standard tier CDAs (1% tolerance) are RED with 14% failure
        assert results_by_field["phone"].status == SLAStatus.RED

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_aggregation_uses_union(self, sla_test_config):
        """Test that aggregation uses union method when row IDs available."""
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report

        # All results should use union method since MandatoryFieldCheck
        # provides failed_row_ids
        for result in sla_report.results:
            assert result.aggregation_method == "union"

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_contributing_validations(self, sla_test_config):
        """Test that contributing validations are tracked."""
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report

        for result in sla_report.results:
            assert "MandatoryFieldCheck" in result.contributing_validations

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_sla_in_json_output(self, sla_test_config):
        """Test that SLA report is included in JSON output."""
        config = ValidationConfig(sla_test_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        file_report = report.file_reports[0]
        report_dict = file_report.to_dict()

        assert "sla_report" in report_dict
        assert report_dict["sla_report"]["file_name"] == "transactions"
        assert "results" in report_dict["sla_report"]
        assert len(report_dict["sla_report"]["results"]) == 5


class TestSLACustomTolerance:
    """Test custom tolerance configuration."""

    @pytest.fixture
    def custom_tolerance_config(self):
        """Create config with custom tolerances."""
        return {
            "validation_job": {
                "name": "Custom Tolerance Test",
                "policy": "none",
                "sla_defaults": {
                    "warning_at": 0.8,
                    "default_tier": "standard"
                },
                "files": [{
                    "name": "transactions",
                    "path": str(SLA_TEST_FILE),
                    "format": "csv",
                    "quoting": 0,
                    "critical_data_attributes": [
                        # Custom tolerance: 5% - should be GREEN for 2% failure
                        {"field": "customer_id", "tolerance": 0.05},
                        # Custom tolerance: 10% - should be GREEN for 6% failure
                        {"field": "email", "tolerance": 0.10},
                    ],
                    "validations": [{
                        "type": "MandatoryFieldCheck",
                        "severity": "ERROR",
                        "params": {
                            "fields": ["customer_id", "email"]
                        }
                    }]
                }]
            }
        }

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_custom_tolerance_applied(self, custom_tolerance_config):
        """Test that custom tolerances are correctly applied."""
        config = ValidationConfig(custom_tolerance_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report
        results_by_field = {r.field: r for r in sla_report.results}

        # Both should have tier_name "custom"
        assert results_by_field["customer_id"].tier_name == "custom"
        assert results_by_field["email"].tier_name == "custom"

        # Check tolerances are set correctly
        assert results_by_field["customer_id"].tolerance == 0.05
        assert results_by_field["email"].tolerance == 0.10


class TestSLAMultipleValidations:
    """Test SLA with multiple validations covering same field."""

    @pytest.fixture
    def multi_validation_config(self):
        """Create config with multiple validations per field."""
        return {
            "validation_job": {
                "name": "Multi Validation Test",
                "policy": "none",
                "sla_defaults": {
                    "warning_at": 0.8
                },
                "files": [{
                    "name": "transactions",
                    "path": str(SLA_TEST_FILE),
                    "format": "csv",
                    "quoting": 0,
                    "critical_data_attributes": [
                        {"field": "email", "tier": "standard"},
                    ],
                    "validations": [
                        {
                            "type": "MandatoryFieldCheck",
                            "severity": "ERROR",
                            "params": {"fields": ["email"]}
                        },
                        {
                            "type": "RegexCheck",
                            "severity": "WARNING",
                            "params": {
                                "field": "email",
                                "pattern": "^[^@]+@[^@]+\\.[^@]+$"
                            }
                        }
                    ]
                }]
            }
        }

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_multiple_validations_aggregated(self, multi_validation_config):
        """Test that failures from multiple validations are aggregated."""
        config = ValidationConfig(multi_validation_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report

        assert len(sla_report.results) == 1
        email_result = sla_report.results[0]

        # Should have both validations contributing
        assert len(email_result.contributing_validations) == 2
        assert "MandatoryFieldCheck" in email_result.contributing_validations
        assert "RegexCheck" in email_result.contributing_validations

        # Should use union aggregation
        assert email_result.aggregation_method == "union"


class TestSLANoValidationCoverage:
    """Test SLA behavior when no validations cover a CDA."""

    @pytest.fixture
    def uncovered_cda_config(self):
        """Create config with CDA not covered by any validation."""
        return {
            "validation_job": {
                "name": "Uncovered CDA Test",
                "policy": "none",
                "files": [{
                    "name": "transactions",
                    "path": str(SLA_TEST_FILE),
                    "format": "csv",
                    "quoting": 0,
                    "critical_data_attributes": [
                        {"field": "merchant_category", "tier": "standard"},  # Not validated
                    ],
                    "validations": [{
                        "type": "MandatoryFieldCheck",
                        "severity": "ERROR",
                        "params": {
                            "fields": ["transaction_id"]  # Doesn't cover merchant_category
                        }
                    }]
                }]
            }
        }

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_uncovered_cda_not_evaluated(self, uncovered_cda_config):
        """Test that uncovered CDAs get NOT_EVALUATED status."""
        config = ValidationConfig(uncovered_cda_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report

        assert len(sla_report.results) == 1
        result = sla_report.results[0]

        assert result.field == "merchant_category"
        assert result.status == SLAStatus.NOT_EVALUATED
        assert result.aggregation_method == "none"
        assert result.contributing_validations == []


class TestSLADefaultTier:
    """Test default tier assignment."""

    @pytest.fixture
    def default_tier_config(self):
        """Create config testing default tier."""
        return {
            "validation_job": {
                "name": "Default Tier Test",
                "policy": "none",
                "sla_defaults": {
                    "default_tier": "low"  # 5% tolerance
                },
                "files": [{
                    "name": "transactions",
                    "path": str(SLA_TEST_FILE),
                    "format": "csv",
                    "quoting": 0,
                    "critical_data_attributes": [
                        {"field": "email"},  # No tier specified, should use default
                    ],
                    "validations": [{
                        "type": "MandatoryFieldCheck",
                        "severity": "ERROR",
                        "params": {"fields": ["email"]}
                    }]
                }]
            }
        }

    @pytest.mark.skipif(not SLA_TEST_FILE.exists(), reason="Test data file not found")
    def test_default_tier_used(self, default_tier_config):
        """Test that default tier is applied when not specified."""
        config = ValidationConfig(default_tier_config)
        engine = ValidationEngine(config)
        report = engine.run(verbose=False)

        sla_report = report.file_reports[0].sla_report

        assert len(sla_report.results) == 1
        result = sla_report.results[0]

        assert result.tier_name == "low"
        assert result.tolerance == 0.05  # 5% for low tier
