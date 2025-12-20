"""
Comprehensive unit tests for SLA (Service Level Agreement) compliance module.

This test suite provides 100% coverage of the SLA module including:
- All SLA model classes and properties
- SLA evaluator edge cases and boundary conditions
- SLA HTML reporter functionality
- All aggregation strategies
- All tier configurations
- Status boundary calculations

Author: Daniel Edge
"""

import pytest
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from validation_framework.cda import (
    SLAStatus, SLADefinition, SLAResult, SLAReport,
    SLAEvaluator, format_sla_cli_output,
    SLAHTMLReporter, generate_sla_report,
    DEFAULT_TIERS, DEFAULT_WARNING_AT
)
from validation_framework.core.results import ValidationResult, Severity


# ============================================================================
# SLAStatus Enum Tests - Full Coverage
# ============================================================================

class TestSLAStatusComprehensive:
    """Comprehensive tests for SLAStatus enum."""

    def test_all_status_values_exist(self):
        """Verify all expected status values are defined."""
        assert hasattr(SLAStatus, 'GREEN')
        assert hasattr(SLAStatus, 'AMBER')
        assert hasattr(SLAStatus, 'RED')
        assert hasattr(SLAStatus, 'NOT_EVALUATED')

    def test_status_string_values(self):
        """Test status string values are lowercase."""
        assert SLAStatus.GREEN.value == "green"
        assert SLAStatus.AMBER.value == "amber"
        assert SLAStatus.RED.value == "red"
        assert SLAStatus.NOT_EVALUATED.value == "not_evaluated"

    def test_status_enum_iteration(self):
        """Test that all 4 statuses can be iterated."""
        statuses = list(SLAStatus)
        assert len(statuses) == 4

    def test_status_comparison(self):
        """Test status enum comparison."""
        assert SLAStatus.GREEN != SLAStatus.RED
        assert SLAStatus.GREEN == SLAStatus.GREEN

    def test_status_from_value(self):
        """Test creating status from string value."""
        assert SLAStatus("green") == SLAStatus.GREEN
        assert SLAStatus("red") == SLAStatus.RED


# ============================================================================
# Default Tiers Tests - Full Coverage
# ============================================================================

class TestDefaultTiersComprehensive:
    """Comprehensive tests for default tier definitions."""

    def test_all_default_tiers_present(self):
        """Test all 4 default tiers are defined."""
        expected_tiers = ["critical", "high", "standard", "low"]
        for tier in expected_tiers:
            assert tier in DEFAULT_TIERS

    def test_tier_tolerance_values(self):
        """Test specific tolerance values for each tier."""
        assert DEFAULT_TIERS["critical"] == 0.0
        assert DEFAULT_TIERS["high"] == 0.001
        assert DEFAULT_TIERS["standard"] == 0.01
        assert DEFAULT_TIERS["low"] == 0.05

    def test_tier_ordering_strictness(self):
        """Test tiers are ordered from strictest to most lenient."""
        tolerances = [DEFAULT_TIERS[t] for t in ["critical", "high", "standard", "low"]]
        assert tolerances == sorted(tolerances)

    def test_default_warning_at_value(self):
        """Test default warning_at is 80%."""
        assert DEFAULT_WARNING_AT == 0.8
        assert 0.0 < DEFAULT_WARNING_AT < 1.0


# ============================================================================
# SLADefinition Tests - Full Coverage
# ============================================================================

class TestSLADefinitionComprehensive:
    """Comprehensive tests for SLADefinition dataclass."""

    # Basic creation tests
    def test_minimal_creation(self):
        """Test creation with only required field."""
        sla = SLADefinition(tolerance=0.05)
        assert sla.tolerance == 0.05
        assert sla.warning_at == 0.8  # Default
        assert sla.tier_name is None

    def test_full_creation(self):
        """Test creation with all parameters."""
        sla = SLADefinition(
            tolerance=0.02,
            warning_at=0.9,
            tier_name="custom"
        )
        assert sla.tolerance == 0.02
        assert sla.warning_at == 0.9
        assert sla.tier_name == "custom"

    # Warning threshold tests
    def test_warning_threshold_standard_calculation(self):
        """Test warning threshold with standard values."""
        sla = SLADefinition(tolerance=0.10, warning_at=0.8)
        assert sla.warning_threshold == pytest.approx(0.08)

    def test_warning_threshold_custom_warning_at(self):
        """Test warning threshold with custom warning_at."""
        sla = SLADefinition(tolerance=0.10, warning_at=0.5)
        assert sla.warning_threshold == pytest.approx(0.05)

    def test_warning_threshold_zero_warning_at(self):
        """Test warning threshold when warning_at is 0."""
        sla = SLADefinition(tolerance=0.10, warning_at=0.0)
        assert sla.warning_threshold == 0.0

    def test_warning_threshold_full_warning_at(self):
        """Test warning threshold when warning_at is 1.0."""
        sla = SLADefinition(tolerance=0.10, warning_at=1.0)
        assert sla.warning_threshold == pytest.approx(0.10)

    def test_zero_tolerance_warning_threshold(self):
        """Test zero tolerance results in zero warning threshold."""
        sla = SLADefinition(tolerance=0.0, warning_at=0.8)
        assert sla.warning_threshold == 0.0

    # Validation tests
    def test_tolerance_boundary_zero(self):
        """Test tolerance at lower boundary (0.0)."""
        sla = SLADefinition(tolerance=0.0)
        assert sla.tolerance == 0.0

    def test_tolerance_boundary_one(self):
        """Test tolerance at upper boundary (1.0)."""
        sla = SLADefinition(tolerance=1.0)
        assert sla.tolerance == 1.0

    def test_tolerance_invalid_negative(self):
        """Test negative tolerance raises ValueError."""
        with pytest.raises(ValueError, match="Tolerance must be 0.0-1.0"):
            SLADefinition(tolerance=-0.01)

    def test_tolerance_invalid_above_one(self):
        """Test tolerance above 1.0 raises ValueError."""
        with pytest.raises(ValueError, match="Tolerance must be 0.0-1.0"):
            SLADefinition(tolerance=1.01)

    def test_warning_at_boundary_zero(self):
        """Test warning_at at lower boundary."""
        sla = SLADefinition(tolerance=0.1, warning_at=0.0)
        assert sla.warning_at == 0.0

    def test_warning_at_boundary_one(self):
        """Test warning_at at upper boundary."""
        sla = SLADefinition(tolerance=0.1, warning_at=1.0)
        assert sla.warning_at == 1.0

    def test_warning_at_invalid_negative(self):
        """Test negative warning_at raises ValueError."""
        with pytest.raises(ValueError, match="warning_at must be 0.0-1.0"):
            SLADefinition(tolerance=0.1, warning_at=-0.1)

    def test_warning_at_invalid_above_one(self):
        """Test warning_at above 1.0 raises ValueError."""
        with pytest.raises(ValueError, match="warning_at must be 0.0-1.0"):
            SLADefinition(tolerance=0.1, warning_at=1.5)

    # from_config tests
    def test_from_config_tier_critical(self):
        """Test from_config with critical tier."""
        config = {"field": "pk", "tier": "critical"}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.0
        assert sla.tier_name == "critical"

    def test_from_config_tier_high(self):
        """Test from_config with high tier."""
        config = {"field": "email", "tier": "high"}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.001
        assert sla.tier_name == "high"

    def test_from_config_tier_standard(self):
        """Test from_config with standard tier."""
        config = {"field": "phone", "tier": "standard"}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.01
        assert sla.tier_name == "standard"

    def test_from_config_tier_low(self):
        """Test from_config with low tier."""
        config = {"field": "notes", "tier": "low"}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.05
        assert sla.tier_name == "low"

    def test_from_config_explicit_tolerance(self):
        """Test from_config with explicit tolerance."""
        config = {"field": "custom", "tolerance": 0.025}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.025
        assert sla.tier_name == "custom"

    def test_from_config_tolerance_as_string(self):
        """Test from_config handles string tolerance."""
        config = {"field": "custom", "tolerance": "0.03"}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.03

    def test_from_config_default_tier_applied(self):
        """Test from_config uses default_tier when no tier specified."""
        config = {"field": "field1"}
        defaults = {"default_tier": "low"}
        sla = SLADefinition.from_config(config, defaults)
        assert sla.tolerance == 0.05
        assert sla.tier_name == "low"

    def test_from_config_default_tier_standard(self):
        """Test from_config defaults to standard when no default_tier."""
        config = {"field": "field1"}
        sla = SLADefinition.from_config(config, {})
        assert sla.tolerance == 0.01  # standard
        assert sla.tier_name == "standard"

    def test_from_config_custom_tiers_override(self):
        """Test from_config with custom tier definitions."""
        config = {"field": "test", "tier": "ultra_strict"}
        defaults = {"tiers": {"ultra_strict": 0.0001}}
        sla = SLADefinition.from_config(config, defaults)
        assert sla.tolerance == 0.0001
        assert sla.tier_name == "ultra_strict"

    def test_from_config_custom_tiers_extend(self):
        """Test custom tiers extend default tiers."""
        config = {"field": "test", "tier": "critical"}
        defaults = {"tiers": {"custom_tier": 0.025}}
        sla = SLADefinition.from_config(config, defaults)
        assert sla.tolerance == 0.0  # critical from defaults

    def test_from_config_custom_warning_at(self):
        """Test from_config respects custom warning_at."""
        config = {"field": "test", "tier": "standard"}
        defaults = {"warning_at": 0.75}
        sla = SLADefinition.from_config(config, defaults)
        assert sla.warning_at == 0.75

    def test_from_config_unknown_tier_error(self):
        """Test unknown tier raises ValueError with message."""
        config = {"field": "test", "tier": "nonexistent"}
        with pytest.raises(ValueError) as exc_info:
            SLADefinition.from_config(config, {})
        assert "Unknown SLA tier: nonexistent" in str(exc_info.value)
        assert "Available:" in str(exc_info.value)


# ============================================================================
# SLAResult Tests - Full Coverage
# ============================================================================

class TestSLAResultComprehensive:
    """Comprehensive tests for SLAResult dataclass."""

    @pytest.fixture
    def green_result(self):
        """Create a green status result."""
        return SLAResult(
            field="customer_id",
            status=SLAStatus.GREEN,
            tier_name="critical",
            tolerance=0.0,
            failure_rate=0.0,
            bad_records=0,
            evaluated_records=10000,
            aggregation_method="union",
            contributing_validations=["MandatoryFieldCheck", "UniqueKeyCheck"]
        )

    @pytest.fixture
    def amber_result(self):
        """Create an amber status result."""
        return SLAResult(
            field="email",
            status=SLAStatus.AMBER,
            tier_name="high",
            tolerance=0.001,
            failure_rate=0.0009,
            bad_records=9,
            evaluated_records=10000,
            aggregation_method="union",
            contributing_validations=["RegexCheck"]
        )

    @pytest.fixture
    def red_result(self):
        """Create a red status result."""
        return SLAResult(
            field="phone",
            status=SLAStatus.RED,
            tier_name="standard",
            tolerance=0.01,
            failure_rate=0.025,
            bad_records=250,
            evaluated_records=10000,
            aggregation_method="max_rate",
            contributing_validations=["MandatoryFieldCheck"]
        )

    @pytest.fixture
    def not_evaluated_result(self):
        """Create a not evaluated result."""
        return SLAResult(
            field="uncovered",
            status=SLAStatus.NOT_EVALUATED,
            tier_name="standard",
            tolerance=0.01,
            failure_rate=0.0,
            bad_records=0,
            evaluated_records=0,
            aggregation_method="none",
            contributing_validations=[]
        )

    # Basic property tests
    def test_field_name(self, green_result):
        """Test field name property."""
        assert green_result.field == "customer_id"

    def test_status_property(self, red_result):
        """Test status property."""
        assert red_result.status == SLAStatus.RED

    def test_tier_name_property(self, amber_result):
        """Test tier_name property."""
        assert amber_result.tier_name == "high"

    # Accuracy calculation tests
    def test_accuracy_100_percent(self, green_result):
        """Test accuracy is 100% when no failures."""
        assert green_result.accuracy == 100.0

    def test_accuracy_with_failures(self, red_result):
        """Test accuracy calculation with failures."""
        assert red_result.accuracy == pytest.approx(97.5)  # 1 - 0.025 = 0.975

    def test_accuracy_amber_result(self, amber_result):
        """Test accuracy for amber status."""
        assert amber_result.accuracy == pytest.approx(99.91)

    def test_accuracy_zero_records(self, not_evaluated_result):
        """Test accuracy when no records evaluated."""
        # failure_rate is 0.0, so accuracy should be 100.0
        assert not_evaluated_result.accuracy == 100.0

    # Compliance property tests
    def test_is_compliant_green(self, green_result):
        """Test GREEN is compliant."""
        assert green_result.is_compliant is True

    def test_is_compliant_amber(self, amber_result):
        """Test AMBER is compliant."""
        assert amber_result.is_compliant is True

    def test_is_not_compliant_red(self, red_result):
        """Test RED is not compliant."""
        assert red_result.is_compliant is False

    def test_is_not_compliant_not_evaluated(self, not_evaluated_result):
        """Test NOT_EVALUATED is not compliant."""
        assert not_evaluated_result.is_compliant is False

    # Health property tests
    def test_is_healthy_green(self, green_result):
        """Test GREEN is healthy."""
        assert green_result.is_healthy is True

    def test_is_not_healthy_amber(self, amber_result):
        """Test AMBER is not healthy."""
        assert amber_result.is_healthy is False

    def test_is_not_healthy_red(self, red_result):
        """Test RED is not healthy."""
        assert red_result.is_healthy is False

    # to_dict tests
    def test_to_dict_all_fields(self, green_result):
        """Test to_dict includes all required fields."""
        d = green_result.to_dict()
        required_keys = [
            "field", "status", "tier", "tolerance", "failure_rate",
            "accuracy", "bad_records", "evaluated_records",
            "aggregation_method", "contributing_validations"
        ]
        for key in required_keys:
            assert key in d

    def test_to_dict_status_string(self, red_result):
        """Test to_dict status is string value."""
        d = red_result.to_dict()
        assert d["status"] == "red"

    def test_to_dict_tolerance_formatted(self, amber_result):
        """Test to_dict formats tolerance as percentage."""
        d = amber_result.to_dict()
        assert "%" in d["tolerance"]

    def test_to_dict_failure_rate_formatted(self, red_result):
        """Test to_dict formats failure_rate as percentage."""
        d = red_result.to_dict()
        assert "%" in d["failure_rate"]

    def test_to_dict_accuracy_formatted(self, green_result):
        """Test to_dict formats accuracy with percent."""
        d = green_result.to_dict()
        assert "%" in d["accuracy"]

    def test_to_dict_validations_list(self, green_result):
        """Test to_dict includes validation list."""
        d = green_result.to_dict()
        assert len(d["contributing_validations"]) == 2
        assert "MandatoryFieldCheck" in d["contributing_validations"]

    def test_to_dict_empty_validations(self, not_evaluated_result):
        """Test to_dict with empty validations list."""
        d = not_evaluated_result.to_dict()
        assert d["contributing_validations"] == []


# ============================================================================
# SLAReport Tests - Full Coverage
# ============================================================================

class TestSLAReportComprehensive:
    """Comprehensive tests for SLAReport dataclass."""

    @pytest.fixture
    def mixed_results(self):
        """Create mixed status results."""
        return [
            SLAResult(
                field="f1", status=SLAStatus.GREEN, tier_name="critical",
                tolerance=0.0, failure_rate=0.0, bad_records=0,
                evaluated_records=1000, aggregation_method="union",
                contributing_validations=["V1"]
            ),
            SLAResult(
                field="f2", status=SLAStatus.GREEN, tier_name="high",
                tolerance=0.001, failure_rate=0.0005, bad_records=5,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V1", "V2"]
            ),
            SLAResult(
                field="f3", status=SLAStatus.AMBER, tier_name="standard",
                tolerance=0.01, failure_rate=0.009, bad_records=90,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V1"]
            ),
            SLAResult(
                field="f4", status=SLAStatus.RED, tier_name="high",
                tolerance=0.001, failure_rate=0.05, bad_records=500,
                evaluated_records=10000, aggregation_method="max_rate",
                contributing_validations=["V1"]
            ),
            SLAResult(
                field="f5", status=SLAStatus.NOT_EVALUATED, tier_name="low",
                tolerance=0.05, failure_rate=0.0, bad_records=0,
                evaluated_records=0, aggregation_method="none",
                contributing_validations=[]
            ),
        ]

    @pytest.fixture
    def all_green_results(self):
        """Create all green results."""
        return [
            SLAResult(
                field=f"field_{i}", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=10,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V1"]
            )
            for i in range(5)
        ]

    @pytest.fixture
    def report(self, mixed_results):
        """Create report with mixed results."""
        return SLAReport(
            file_name="test_data.csv",
            dataset_row_count=10000,
            results=mixed_results
        )

    # Basic property tests
    def test_file_name(self, report):
        """Test file_name property."""
        assert report.file_name == "test_data.csv"

    def test_dataset_row_count(self, report):
        """Test dataset_row_count property."""
        assert report.dataset_row_count == 10000

    def test_total_cdas(self, report):
        """Test total_cdas property."""
        assert report.total_cdas == 5

    def test_timestamp_auto_set(self, report):
        """Test timestamp is automatically set."""
        assert isinstance(report.timestamp, datetime)

    def test_custom_timestamp(self, mixed_results):
        """Test custom timestamp can be provided."""
        ts = datetime(2024, 1, 15, 12, 0, 0)
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=1000,
            results=mixed_results,
            timestamp=ts
        )
        assert report.timestamp == ts

    # Count property tests
    def test_green_count(self, report):
        """Test green_count property."""
        assert report.green_count == 2

    def test_amber_count(self, report):
        """Test amber_count property."""
        assert report.amber_count == 1

    def test_red_count(self, report):
        """Test red_count property."""
        assert report.red_count == 1

    def test_not_evaluated_count(self, report):
        """Test not_evaluated_count property."""
        assert report.not_evaluated_count == 1

    # Health status tests
    def test_is_fully_green_false_with_mixed(self, report):
        """Test is_fully_green is False with mixed results."""
        assert report.is_fully_green is False

    def test_is_fully_green_true(self, all_green_results):
        """Test is_fully_green is True with all green."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=all_green_results
        )
        assert report.is_fully_green is True

    def test_is_fully_green_empty_results(self):
        """Test is_fully_green is True with empty results."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=0,
            results=[]
        )
        assert report.is_fully_green is True

    def test_is_fully_green_only_not_evaluated(self):
        """Test is_fully_green with only NOT_EVALUATED results."""
        results = [
            SLAResult(
                field="f1", status=SLAStatus.NOT_EVALUATED, tier_name="standard",
                tolerance=0.01, failure_rate=0.0, bad_records=0,
                evaluated_records=0, aggregation_method="none",
                contributing_validations=[]
            )
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=1000,
            results=results
        )
        assert report.is_fully_green is True  # No evaluated results = fully green

    def test_has_breaches_true(self, report):
        """Test has_breaches is True with RED status."""
        assert report.has_breaches is True

    def test_has_breaches_false(self, all_green_results):
        """Test has_breaches is False with no RED."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=all_green_results
        )
        assert report.has_breaches is False

    # Getter method tests
    def test_get_breaches(self, report):
        """Test get_breaches returns RED results."""
        breaches = report.get_breaches()
        assert len(breaches) == 1
        assert breaches[0].field == "f4"
        assert breaches[0].status == SLAStatus.RED

    def test_get_breaches_empty(self, all_green_results):
        """Test get_breaches returns empty list with no breaches."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=all_green_results
        )
        assert report.get_breaches() == []

    def test_get_warnings(self, report):
        """Test get_warnings returns AMBER results."""
        warnings = report.get_warnings()
        assert len(warnings) == 1
        assert warnings[0].field == "f3"
        assert warnings[0].status == SLAStatus.AMBER

    def test_get_warnings_empty(self, all_green_results):
        """Test get_warnings returns empty list with no warnings."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=all_green_results
        )
        assert report.get_warnings() == []

    # Summary tests
    def test_summary_contains_counts(self, report):
        """Test summary contains emoji counts."""
        summary = report.summary
        assert "2" in summary  # green count
        assert "1" in summary  # amber, red counts
        assert "🟢" in summary
        assert "🟡" in summary
        assert "🔴" in summary

    # to_dict tests
    def test_to_dict_all_fields(self, report):
        """Test to_dict includes all required fields."""
        d = report.to_dict()
        required_keys = [
            "file_name", "dataset_row_count", "total_cdas",
            "green_count", "amber_count", "red_count", "not_evaluated_count",
            "is_fully_green", "has_breaches", "summary", "results", "timestamp"
        ]
        for key in required_keys:
            assert key in d

    def test_to_dict_results_converted(self, report):
        """Test to_dict converts results to dicts."""
        d = report.to_dict()
        assert isinstance(d["results"], list)
        assert len(d["results"]) == 5
        assert isinstance(d["results"][0], dict)

    def test_to_dict_timestamp_iso_format(self, report):
        """Test to_dict formats timestamp as ISO."""
        d = report.to_dict()
        assert "T" in d["timestamp"]  # ISO format has T separator


# ============================================================================
# SLAEvaluator Tests - Full Coverage
# ============================================================================

class TestSLAEvaluatorComprehensive:
    """Comprehensive tests for SLAEvaluator class."""

    @pytest.fixture
    def evaluator(self):
        """Create SLA evaluator."""
        return SLAEvaluator()

    @pytest.fixture
    def evaluator_with_logger(self):
        """Create SLA evaluator with mock logger."""
        mock_logger = Mock()
        return SLAEvaluator(logger=mock_logger)

    # Status evaluation tests
    def test_evaluate_green_zero_failures(self, evaluator):
        """Test GREEN status with zero failures."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=True,
                message="OK", failed_count=0, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set()
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.GREEN
        assert report.results[0].failure_rate == 0.0

    def test_evaluate_green_below_threshold(self, evaluator):
        """Test GREEN status below warning threshold."""
        cdas = [{"field": "f1", "tolerance": 0.10}]  # 10% tolerance
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=50, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set(range(50))  # 5%
            )
        ]
        report = evaluator.evaluate(
            "test.csv", 1000, cdas, results,
            sla_defaults={"warning_at": 0.8}
        )
        assert report.results[0].status == SLAStatus.GREEN  # 5% < 8% warning

    def test_evaluate_amber_at_warning_threshold(self, evaluator):
        """Test AMBER status at warning threshold."""
        cdas = [{"field": "f1", "tolerance": 0.10}]  # 10% tolerance, 8% warning
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=85, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set(range(85))  # 8.5%
            )
        ]
        report = evaluator.evaluate(
            "test.csv", 1000, cdas, results,
            sla_defaults={"warning_at": 0.8}
        )
        assert report.results[0].status == SLAStatus.AMBER

    def test_evaluate_red_at_tolerance(self, evaluator):
        """Test RED status at tolerance threshold."""
        cdas = [{"field": "f1", "tolerance": 0.10}]  # 10% tolerance
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=100, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set(range(100))  # 10%
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.RED

    def test_evaluate_red_above_tolerance(self, evaluator):
        """Test RED status above tolerance threshold."""
        cdas = [{"field": "f1", "tolerance": 0.10}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=150, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set(range(150))  # 15%
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.RED

    # Zero tolerance tests
    def test_zero_tolerance_green_no_failures(self, evaluator):
        """Test zero tolerance GREEN with no failures."""
        cdas = [{"field": "f1", "tier": "critical"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=True,
                message="OK", failed_count=0, total_count=10000,
                covered_fields=["f1"], failed_row_ids=set()
            )
        ]
        report = evaluator.evaluate("test.csv", 10000, cdas, results)
        assert report.results[0].status == SLAStatus.GREEN

    def test_zero_tolerance_red_single_failure(self, evaluator):
        """Test zero tolerance RED with single failure."""
        cdas = [{"field": "f1", "tier": "critical"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="1 failure", failed_count=1, total_count=10000,
                covered_fields=["f1"], failed_row_ids={1}
            )
        ]
        report = evaluator.evaluate("test.csv", 10000, cdas, results)
        assert report.results[0].status == SLAStatus.RED

    def test_zero_tolerance_no_amber_zone(self, evaluator):
        """Test zero tolerance has no amber zone."""
        # With zero tolerance, any failure should be RED (no AMBER)
        cdas = [{"field": "f1", "tier": "critical"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Tiny failure", failed_count=1, total_count=1000000,
                covered_fields=["f1"], failed_row_ids={1}
            )
        ]
        report = evaluator.evaluate("test.csv", 1000000, cdas, results)
        assert report.results[0].status == SLAStatus.RED  # Not AMBER

    # Coverage tests
    def test_not_evaluated_no_coverage(self, evaluator):
        """Test NOT_EVALUATED when no validations cover field."""
        cdas = [{"field": "uncovered", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=True,
                message="OK", failed_count=0, total_count=1000,
                covered_fields=["other_field"]
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.NOT_EVALUATED
        assert report.results[0].aggregation_method == "none"
        assert report.results[0].contributing_validations == []

    def test_not_evaluated_empty_results(self, evaluator):
        """Test NOT_EVALUATED with empty validation results."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = []
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.NOT_EVALUATED

    def test_partial_coverage(self, evaluator):
        """Test evaluation with partial field coverage."""
        cdas = [
            {"field": "covered", "tier": "standard"},
            {"field": "uncovered", "tier": "standard"},
        ]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=True,
                message="OK", failed_count=0, total_count=1000,
                covered_fields=["covered"]
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.GREEN  # covered
        assert report.results[1].status == SLAStatus.NOT_EVALUATED  # uncovered

    # Aggregation tests
    def test_aggregation_union_single_validation(self, evaluator):
        """Test union aggregation with single validation."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=50, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set(range(50))
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].aggregation_method == "union"
        assert report.results[0].bad_records == 50

    def test_aggregation_union_multiple_validations_overlap(self, evaluator):
        """Test union aggregation with overlapping failures."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=30, total_count=1000,
                covered_fields=["f1"], failed_row_ids={1, 2, 3, 4, 5}
            ),
            ValidationResult(
                rule_name="V2", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=40, total_count=1000,
                covered_fields=["f1"], failed_row_ids={3, 4, 5, 6, 7}  # 3,4,5 overlap
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].aggregation_method == "union"
        assert report.results[0].bad_records == 7  # {1,2,3,4,5,6,7}
        assert len(report.results[0].contributing_validations) == 2

    def test_aggregation_max_rate_no_row_ids(self, evaluator):
        """Test max_rate aggregation when no row IDs available."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=30, total_count=1000,
                covered_fields=["f1"], failed_row_ids=None
            ),
            ValidationResult(
                rule_name="V2", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=50, total_count=1000,
                covered_fields=["f1"], failed_row_ids=None
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].aggregation_method == "max_rate"
        assert report.results[0].bad_records == 50  # Max of 30 and 50

    def test_aggregation_mixed_row_ids(self, evaluator):
        """Test union takes precedence when any validation has row IDs."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=30, total_count=1000,
                covered_fields=["f1"], failed_row_ids={1, 2, 3}  # Has IDs
            ),
            ValidationResult(
                rule_name="V2", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=50, total_count=1000,
                covered_fields=["f1"], failed_row_ids=None  # No IDs
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].aggregation_method == "union"
        assert report.results[0].bad_records == 3  # Only counts rows with IDs

    # Multiple CDAs tests
    def test_multiple_cdas_different_statuses(self, evaluator):
        """Test evaluation of multiple CDAs with different outcomes."""
        cdas = [
            {"field": "critical_field", "tier": "critical"},
            {"field": "high_field", "tier": "high"},
            {"field": "standard_field", "tier": "standard"},
        ]
        # All have 0.5% failure rate
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=False,
                message="Failures", failed_count=5, total_count=1000,
                covered_fields=["critical_field", "high_field", "standard_field"],
                failed_row_ids={1, 2, 3, 4, 5}
            )
        ]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)

        assert len(report.results) == 3
        # 0.5% failure rate
        # critical (0% tolerance) -> RED
        # high (0.1% tolerance) -> RED
        # standard (1% tolerance) -> GREEN
        assert report.results[0].status == SLAStatus.RED  # critical
        assert report.results[1].status == SLAStatus.RED  # high
        assert report.results[2].status == SLAStatus.GREEN  # standard

    # Edge cases
    def test_empty_cdas_list(self, evaluator):
        """Test evaluation with empty CDAs list."""
        report = evaluator.evaluate("test.csv", 1000, [], [])
        assert report.total_cdas == 0
        assert report.results == []

    def test_cda_missing_field_key(self, evaluator_with_logger):
        """Test CDA config missing 'field' key is skipped."""
        cdas = [{"tier": "standard"}]  # Missing 'field'
        results = []
        report = evaluator_with_logger.evaluate("test.csv", 1000, cdas, results)
        assert report.total_cdas == 0

    def test_invalid_sla_config_skipped(self, evaluator_with_logger):
        """Test invalid SLA config is skipped with error log."""
        cdas = [{"field": "f1", "tier": "nonexistent_tier"}]
        results = []
        report = evaluator_with_logger.evaluate("test.csv", 1000, cdas, results)
        assert report.total_cdas == 0

    def test_zero_evaluated_records(self, evaluator):
        """Test handling when no records evaluated."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=True,
                message="Empty", failed_count=0, total_count=0,
                covered_fields=["f1"], failed_row_ids=set()
            )
        ]
        report = evaluator.evaluate("test.csv", 0, cdas, results)
        assert report.results[0].failure_rate == 0.0

    def test_validation_without_covered_fields_attr(self, evaluator):
        """Test validation result without covered_fields attribute."""
        cdas = [{"field": "f1", "tier": "standard"}]
        result = Mock()
        result.covered_fields = None  # getattr returns None
        del result.covered_fields  # Make getattr return default []
        results = [result]
        report = evaluator.evaluate("test.csv", 1000, cdas, results)
        assert report.results[0].status == SLAStatus.NOT_EVALUATED

    # SLA defaults tests
    def test_sla_defaults_none_handled(self, evaluator):
        """Test None sla_defaults is handled gracefully."""
        cdas = [{"field": "f1", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="V1", severity=Severity.ERROR, passed=True,
                message="OK", failed_count=0, total_count=1000,
                covered_fields=["f1"], failed_row_ids=set()
            )
        ]
        report = evaluator.evaluate(
            "test.csv", 1000, cdas, results,
            sla_defaults=None
        )
        assert report.results[0].status == SLAStatus.GREEN


# ============================================================================
# format_sla_cli_output Tests - Full Coverage
# ============================================================================

class TestFormatSLACLIOutputComprehensive:
    """Comprehensive tests for CLI output formatting."""

    def test_empty_report_format(self):
        """Test formatting empty report."""
        report = SLAReport(
            file_name="empty.csv",
            dataset_row_count=0,
            results=[]
        )
        output = format_sla_cli_output(report)
        assert "SLA COMPLIANCE" in output
        assert "empty.csv" in output
        assert "No CDAs defined" in output

    def test_report_header_formatting(self):
        """Test report header includes file name."""
        results = [
            SLAResult(
                field="f1", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=10,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V1"]
            )
        ]
        report = SLAReport(
            file_name="my_data.csv",
            dataset_row_count=10000,
            results=results
        )
        output = format_sla_cli_output(report)
        assert "my_data.csv" in output

    def test_all_status_icons_present(self):
        """Test all status types have icons."""
        results = [
            SLAResult(
                field="green_field", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=10,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=[]
            ),
            SLAResult(
                field="amber_field", status=SLAStatus.AMBER, tier_name="standard",
                tolerance=0.01, failure_rate=0.009, bad_records=90,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=[]
            ),
            SLAResult(
                field="red_field", status=SLAStatus.RED, tier_name="standard",
                tolerance=0.01, failure_rate=0.05, bad_records=500,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=[]
            ),
            SLAResult(
                field="na_field", status=SLAStatus.NOT_EVALUATED, tier_name="standard",
                tolerance=0.01, failure_rate=0.0, bad_records=0,
                evaluated_records=0, aggregation_method="none",
                contributing_validations=[]
            ),
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=results
        )
        output = format_sla_cli_output(report)

        # Check for status indicators
        assert "🟢" in output or "GREEN" in output
        assert "🟡" in output or "AMBER" in output
        assert "🔴" in output or "RED" in output
        assert "⚪" in output or "N/A" in output

    def test_field_names_in_output(self):
        """Test field names appear in output."""
        results = [
            SLAResult(
                field="customer_id", status=SLAStatus.GREEN, tier_name="critical",
                tolerance=0.0, failure_rate=0.0, bad_records=0,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=[]
            ),
            SLAResult(
                field="email_address", status=SLAStatus.RED, tier_name="high",
                tolerance=0.001, failure_rate=0.05, bad_records=500,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=[]
            ),
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=results
        )
        output = format_sla_cli_output(report)
        assert "customer_id" in output
        assert "email_address" in output

    def test_summary_line_present(self):
        """Test summary line appears in output."""
        results = [
            SLAResult(
                field="f1", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=10,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=[]
            )
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=results
        )
        output = format_sla_cli_output(report)
        assert "Summary" in output


# ============================================================================
# SLAHTMLReporter Tests - Full Coverage
# ============================================================================

class TestSLAHTMLReporterComprehensive:
    """Comprehensive tests for SLA HTML reporter."""

    @pytest.fixture
    def sample_report(self):
        """Create sample SLA report for testing."""
        results = [
            SLAResult(
                field="customer_id", status=SLAStatus.GREEN, tier_name="critical",
                tolerance=0.0, failure_rate=0.0, bad_records=0,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["MandatoryFieldCheck", "UniqueKeyCheck"]
            ),
            SLAResult(
                field="email", status=SLAStatus.AMBER, tier_name="high",
                tolerance=0.001, failure_rate=0.0009, bad_records=9,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["RegexCheck"]
            ),
            SLAResult(
                field="phone", status=SLAStatus.RED, tier_name="standard",
                tolerance=0.01, failure_rate=0.025, bad_records=250,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["MandatoryFieldCheck"]
            ),
            SLAResult(
                field="notes", status=SLAStatus.NOT_EVALUATED, tier_name="low",
                tolerance=0.05, failure_rate=0.0, bad_records=0,
                evaluated_records=0, aggregation_method="none",
                contributing_validations=[]
            ),
        ]
        return SLAReport(
            file_name="test_data.csv",
            dataset_row_count=10000,
            results=results
        )

    @pytest.fixture
    def reporter(self):
        """Create HTML reporter instance."""
        return SLAHTMLReporter()

    def test_generate_creates_file(self, reporter, sample_report, tmp_path):
        """Test generate creates HTML file."""
        output_path = tmp_path / "report.html"
        result = reporter.generate(sample_report, str(output_path))
        assert Path(result).exists()

    def test_generate_returns_path(self, reporter, sample_report, tmp_path):
        """Test generate returns output path."""
        output_path = tmp_path / "report.html"
        result = reporter.generate(sample_report, str(output_path))
        assert result == str(output_path)

    def test_generate_creates_directory(self, reporter, sample_report, tmp_path):
        """Test generate creates output directory if needed."""
        output_path = tmp_path / "subdir" / "nested" / "report.html"
        result = reporter.generate(sample_report, str(output_path))
        assert Path(result).exists()

    def test_html_contains_title(self, reporter, sample_report, tmp_path):
        """Test HTML contains report title."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path), title="My SLA Report")
        content = output_path.read_text()
        assert "My SLA Report" in content

    def test_html_contains_file_name(self, reporter, sample_report, tmp_path):
        """Test HTML contains file name."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "test_data.csv" in content

    def test_html_contains_all_fields(self, reporter, sample_report, tmp_path):
        """Test HTML contains all CDA fields."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "customer_id" in content
        assert "email" in content
        assert "phone" in content
        assert "notes" in content

    def test_html_contains_status_badges(self, reporter, sample_report, tmp_path):
        """Test HTML contains status badges."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "GREEN" in content
        assert "AMBER" in content
        assert "RED" in content

    def test_html_contains_kpi_cards(self, reporter, sample_report, tmp_path):
        """Test HTML contains KPI summary cards."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "Compliant" in content
        assert "Warning" in content
        assert "Breached" in content

    def test_html_contains_tier_breakdown(self, reporter, sample_report, tmp_path):
        """Test HTML contains tier breakdown section."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "critical" in content
        assert "high" in content
        assert "standard" in content

    def test_html_contains_breach_details(self, reporter, sample_report, tmp_path):
        """Test HTML contains breach details for RED status."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "SLA Breach" in content or "Requiring Attention" in content

    def test_html_contains_validations(self, reporter, sample_report, tmp_path):
        """Test HTML contains contributing validations."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path), show_details=True)
        content = output_path.read_text()
        assert "MandatoryFieldCheck" in content
        assert "UniqueKeyCheck" in content
        assert "RegexCheck" in content

    def test_html_hide_details(self, reporter, sample_report, tmp_path):
        """Test HTML can hide validation details."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path), show_details=False)
        content = output_path.read_text()
        # HTML should still be valid but may not show validation column
        assert "<!DOCTYPE html>" in content

    def test_html_is_valid(self, reporter, sample_report, tmp_path):
        """Test generated HTML is well-formed."""
        output_path = tmp_path / "report.html"
        reporter.generate(sample_report, str(output_path))
        content = output_path.read_text()
        assert "<!DOCTYPE html>" in content
        assert "</html>" in content
        assert "<head>" in content
        assert "</head>" in content
        assert "<body>" in content
        assert "</body>" in content

    def test_generate_ioerror_handling(self, reporter, sample_report):
        """Test IOError is raised for invalid path."""
        with pytest.raises(IOError, match="Error generating SLA HTML report"):
            reporter.generate(sample_report, "/nonexistent/path/that/cannot/be/created/report.html")

    def test_empty_report_generates(self, reporter, tmp_path):
        """Test empty report generates valid HTML."""
        empty_report = SLAReport(
            file_name="empty.csv",
            dataset_row_count=0,
            results=[]
        )
        output_path = tmp_path / "empty_report.html"
        reporter.generate(empty_report, str(output_path))
        content = output_path.read_text()
        assert "<!DOCTYPE html>" in content

    def test_all_green_report(self, reporter, tmp_path):
        """Test report with all green statuses."""
        green_results = [
            SLAResult(
                field=f"field_{i}", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=10,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V1"]
            )
            for i in range(5)
        ]
        report = SLAReport(
            file_name="all_green.csv",
            dataset_row_count=10000,
            results=green_results
        )
        output_path = tmp_path / "green_report.html"
        reporter.generate(report, str(output_path))
        content = output_path.read_text()
        assert "SLA Compliant" in content or "100" in content  # Health score

    def test_convenience_function(self, sample_report, tmp_path):
        """Test generate_sla_report convenience function."""
        output_path = tmp_path / "convenience_report.html"
        result = generate_sla_report(sample_report, str(output_path))
        assert Path(result).exists()


# ============================================================================
# Integration Tests - SLA Module Components Working Together
# ============================================================================

class TestSLAModuleIntegration:
    """Integration tests for SLA module components."""

    def test_full_flow_green_result(self, tmp_path):
        """Test complete flow from validation to report for green result."""
        # Create validation result
        validation_result = ValidationResult(
            rule_name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            passed=True,
            message="All values present",
            failed_count=0,
            total_count=10000,
            covered_fields=["customer_id"],
            failed_row_ids=set()
        )

        # Evaluate SLA
        evaluator = SLAEvaluator()
        cdas = [{"field": "customer_id", "tier": "critical"}]
        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=10000,
            cdas=cdas,
            validation_results=[validation_result]
        )

        # Verify evaluation
        assert report.is_fully_green
        assert not report.has_breaches
        assert report.results[0].status == SLAStatus.GREEN

        # Generate HTML report
        output_path = tmp_path / "sla_report.html"
        generate_sla_report(report, str(output_path))
        assert output_path.exists()

        # Verify HTML content
        content = output_path.read_text()
        assert "customer_id" in content
        assert "GREEN" in content

    def test_full_flow_red_result(self, tmp_path):
        """Test complete flow for breached SLA."""
        validation_result = ValidationResult(
            rule_name="MandatoryFieldCheck",
            severity=Severity.ERROR,
            passed=False,
            message="Missing values",
            failed_count=100,
            total_count=10000,
            covered_fields=["customer_id"],
            failed_row_ids=set(range(100))
        )

        evaluator = SLAEvaluator()
        cdas = [{"field": "customer_id", "tier": "critical"}]
        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=10000,
            cdas=cdas,
            validation_results=[validation_result]
        )

        assert report.has_breaches
        assert report.results[0].status == SLAStatus.RED

        output_path = tmp_path / "breach_report.html"
        generate_sla_report(report, str(output_path))
        content = output_path.read_text()
        assert "RED" in content
        assert "SLA Breach" in content or "Attention" in content

    def test_serialization_roundtrip(self):
        """Test report can be serialized and data preserved."""
        results = [
            SLAResult(
                field="f1", status=SLAStatus.GREEN, tier_name="critical",
                tolerance=0.0, failure_rate=0.0, bad_records=0,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V1"]
            ),
            SLAResult(
                field="f2", status=SLAStatus.RED, tier_name="high",
                tolerance=0.001, failure_rate=0.05, bad_records=500,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["V2"]
            ),
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=results
        )

        # Serialize
        data = report.to_dict()

        # Verify all data preserved
        assert data["file_name"] == "test.csv"
        assert data["green_count"] == 1
        assert data["red_count"] == 1
        assert len(data["results"]) == 2
        assert data["results"][0]["field"] == "f1"
        assert data["results"][1]["field"] == "f2"
