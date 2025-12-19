"""
Unit tests for SLA (Service Level Agreement) compliance module.

Tests cover:
- SLA models (SLAStatus, SLADefinition, SLAResult, SLAReport)
- SLA evaluator logic
- Aggregation strategies (union vs max_rate)
- Traffic light status calculation
"""

import pytest
from datetime import datetime
from validation_framework.cda import (
    SLAStatus, SLADefinition, SLAResult, SLAReport,
    SLAEvaluator, format_sla_cli_output,
    DEFAULT_TIERS, DEFAULT_WARNING_AT
)
from validation_framework.core.results import ValidationResult, Severity


class TestSLAStatus:
    """Tests for SLAStatus enum."""

    def test_status_values(self):
        """Test all status values exist."""
        assert SLAStatus.GREEN.value == "green"
        assert SLAStatus.AMBER.value == "amber"
        assert SLAStatus.RED.value == "red"
        assert SLAStatus.NOT_EVALUATED.value == "not_evaluated"


class TestDefaultTiers:
    """Tests for default tier values."""

    def test_default_tiers_exist(self):
        """Test all default tiers are defined."""
        assert "critical" in DEFAULT_TIERS
        assert "high" in DEFAULT_TIERS
        assert "standard" in DEFAULT_TIERS
        assert "low" in DEFAULT_TIERS

    def test_critical_tier_zero_tolerance(self):
        """Test critical tier has 0% tolerance."""
        assert DEFAULT_TIERS["critical"] == 0.0

    def test_tier_ordering(self):
        """Test tiers are ordered by strictness."""
        assert DEFAULT_TIERS["critical"] < DEFAULT_TIERS["high"]
        assert DEFAULT_TIERS["high"] < DEFAULT_TIERS["standard"]
        assert DEFAULT_TIERS["standard"] < DEFAULT_TIERS["low"]

    def test_default_warning_at(self):
        """Test default warning threshold is 80%."""
        assert DEFAULT_WARNING_AT == 0.8


class TestSLADefinition:
    """Tests for SLADefinition dataclass."""

    def test_basic_creation(self):
        """Test basic SLA definition creation."""
        sla = SLADefinition(tolerance=0.01)
        assert sla.tolerance == 0.01
        assert sla.warning_at == 0.8  # Default
        assert sla.tier_name is None

    def test_warning_threshold_calculation(self):
        """Test warning threshold is calculated correctly."""
        sla = SLADefinition(tolerance=0.10, warning_at=0.8)
        assert sla.warning_threshold == pytest.approx(0.08)  # 80% of 10%

    def test_zero_tolerance_no_amber(self):
        """Test zero tolerance has no amber zone."""
        sla = SLADefinition(tolerance=0.0, warning_at=0.8)
        assert sla.warning_threshold == 0.0

    def test_validation_tolerance_range(self):
        """Test tolerance must be 0.0-1.0."""
        with pytest.raises(ValueError):
            SLADefinition(tolerance=1.5)
        with pytest.raises(ValueError):
            SLADefinition(tolerance=-0.1)

    def test_validation_warning_at_range(self):
        """Test warning_at must be 0.0-1.0."""
        with pytest.raises(ValueError):
            SLADefinition(tolerance=0.1, warning_at=1.5)

    def test_from_config_with_tier(self):
        """Test creating from config with named tier."""
        config = {"field": "customer_id", "tier": "critical"}
        defaults = {}
        sla = SLADefinition.from_config(config, defaults)

        assert sla.tolerance == 0.0
        assert sla.tier_name == "critical"

    def test_from_config_with_tolerance(self):
        """Test creating from config with explicit tolerance."""
        config = {"field": "email", "tolerance": 0.02}
        defaults = {}
        sla = SLADefinition.from_config(config, defaults)

        assert sla.tolerance == 0.02
        assert sla.tier_name == "custom"

    def test_from_config_default_tier(self):
        """Test using default tier when not specified."""
        config = {"field": "phone"}
        defaults = {"default_tier": "standard"}
        sla = SLADefinition.from_config(config, defaults)

        assert sla.tolerance == 0.01  # standard tier
        assert sla.tier_name == "standard"

    def test_from_config_unknown_tier_raises(self):
        """Test unknown tier raises ValueError."""
        config = {"field": "test", "tier": "unknown_tier"}
        defaults = {}
        with pytest.raises(ValueError, match="Unknown SLA tier"):
            SLADefinition.from_config(config, defaults)

    def test_from_config_custom_tiers(self):
        """Test using custom tier definitions."""
        config = {"field": "test", "tier": "custom_tier"}
        defaults = {"tiers": {"custom_tier": 0.025}}
        sla = SLADefinition.from_config(config, defaults)

        assert sla.tolerance == 0.025
        assert sla.tier_name == "custom_tier"

    def test_from_config_custom_warning_at(self):
        """Test using custom warning_at from defaults."""
        config = {"field": "test", "tier": "standard"}
        defaults = {"warning_at": 0.9}
        sla = SLADefinition.from_config(config, defaults)

        assert sla.warning_at == 0.9
        assert sla.warning_threshold == pytest.approx(0.009)  # 90% of 1%


class TestSLAResult:
    """Tests for SLAResult dataclass."""

    def test_basic_creation(self):
        """Test basic SLA result creation."""
        result = SLAResult(
            field="customer_id",
            status=SLAStatus.GREEN,
            tier_name="critical",
            tolerance=0.0,
            failure_rate=0.0,
            bad_records=0,
            evaluated_records=1000,
            aggregation_method="union",
            contributing_validations=["MandatoryFieldCheck"]
        )
        assert result.field == "customer_id"
        assert result.status == SLAStatus.GREEN
        assert result.is_compliant is True
        assert result.is_healthy is True

    def test_accuracy_calculation(self):
        """Test accuracy is calculated correctly."""
        result = SLAResult(
            field="email",
            status=SLAStatus.RED,
            tier_name="high",
            tolerance=0.001,
            failure_rate=0.05,
            bad_records=50,
            evaluated_records=1000,
            aggregation_method="union",
            contributing_validations=[]
        )
        assert result.accuracy == 95.0

    def test_is_compliant_green(self):
        """Test GREEN status is compliant."""
        result = SLAResult(
            field="test", status=SLAStatus.GREEN, tier_name="standard",
            tolerance=0.01, failure_rate=0.005, bad_records=5,
            evaluated_records=1000, aggregation_method="union",
            contributing_validations=[]
        )
        assert result.is_compliant is True
        assert result.is_healthy is True

    def test_is_compliant_amber(self):
        """Test AMBER status is compliant but not healthy."""
        result = SLAResult(
            field="test", status=SLAStatus.AMBER, tier_name="standard",
            tolerance=0.01, failure_rate=0.009, bad_records=9,
            evaluated_records=1000, aggregation_method="union",
            contributing_validations=[]
        )
        assert result.is_compliant is True
        assert result.is_healthy is False

    def test_is_not_compliant_red(self):
        """Test RED status is not compliant."""
        result = SLAResult(
            field="test", status=SLAStatus.RED, tier_name="standard",
            tolerance=0.01, failure_rate=0.02, bad_records=20,
            evaluated_records=1000, aggregation_method="union",
            contributing_validations=[]
        )
        assert result.is_compliant is False
        assert result.is_healthy is False

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = SLAResult(
            field="email",
            status=SLAStatus.GREEN,
            tier_name="high",
            tolerance=0.001,
            failure_rate=0.0005,
            bad_records=5,
            evaluated_records=10000,
            aggregation_method="union",
            contributing_validations=["MandatoryFieldCheck", "RegexCheck"]
        )
        d = result.to_dict()

        assert d["field"] == "email"
        assert d["status"] == "green"
        assert d["tier"] == "high"
        assert "tolerance" in d
        assert "failure_rate" in d
        assert d["aggregation_method"] == "union"
        assert len(d["contributing_validations"]) == 2


class TestSLAReport:
    """Tests for SLAReport dataclass."""

    @pytest.fixture
    def sample_results(self):
        """Create sample SLA results."""
        return [
            SLAResult(
                field="customer_id", status=SLAStatus.GREEN, tier_name="critical",
                tolerance=0.0, failure_rate=0.0, bad_records=0,
                evaluated_records=1000, aggregation_method="union",
                contributing_validations=["MandatoryFieldCheck"]
            ),
            SLAResult(
                field="email", status=SLAStatus.AMBER, tier_name="high",
                tolerance=0.001, failure_rate=0.0008, bad_records=8,
                evaluated_records=10000, aggregation_method="union",
                contributing_validations=["MandatoryFieldCheck", "RegexCheck"]
            ),
            SLAResult(
                field="phone", status=SLAStatus.RED, tier_name="standard",
                tolerance=0.01, failure_rate=0.02, bad_records=200,
                evaluated_records=10000, aggregation_method="max_rate",
                contributing_validations=["MandatoryFieldCheck"]
            ),
        ]

    def test_basic_creation(self, sample_results):
        """Test basic report creation."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=sample_results
        )
        assert report.file_name == "test.csv"
        assert report.total_cdas == 3

    def test_status_counts(self, sample_results):
        """Test status counting."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=sample_results
        )
        assert report.green_count == 1
        assert report.amber_count == 1
        assert report.red_count == 1

    def test_is_fully_green(self):
        """Test fully green report detection."""
        green_results = [
            SLAResult(
                field="test", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=1,
                evaluated_records=1000, aggregation_method="union",
                contributing_validations=[]
            )
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=1000,
            results=green_results
        )
        assert report.is_fully_green is True
        assert report.has_breaches is False

    def test_has_breaches(self, sample_results):
        """Test breach detection."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=sample_results
        )
        assert report.has_breaches is True
        assert len(report.get_breaches()) == 1

    def test_get_warnings(self, sample_results):
        """Test warning retrieval."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=sample_results
        )
        warnings = report.get_warnings()
        assert len(warnings) == 1
        assert warnings[0].field == "email"

    def test_summary_format(self, sample_results):
        """Test summary string format."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=sample_results
        )
        # Summary should contain counts with emoji
        assert "1" in report.summary
        assert "🟢" in report.summary or "GREEN" in report.summary.upper()

    def test_to_dict(self, sample_results):
        """Test conversion to dictionary."""
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=10000,
            results=sample_results
        )
        d = report.to_dict()

        assert d["file_name"] == "test.csv"
        assert d["dataset_row_count"] == 10000
        assert d["total_cdas"] == 3
        assert d["green_count"] == 1
        assert d["amber_count"] == 1
        assert d["red_count"] == 1
        assert len(d["results"]) == 3

    def test_not_evaluated_count(self):
        """Test NOT_EVALUATED status counting."""
        results = [
            SLAResult(
                field="test", status=SLAStatus.NOT_EVALUATED, tier_name="standard",
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
        assert report.not_evaluated_count == 1
        assert report.is_fully_green is True  # Empty evaluated set is considered green


class TestSLAEvaluator:
    """Tests for SLAEvaluator class."""

    @pytest.fixture
    def evaluator(self):
        """Create SLA evaluator."""
        return SLAEvaluator()

    def test_evaluate_green_status(self, evaluator):
        """Test evaluation returns GREEN for low failure rate."""
        cdas = [{"field": "customer_id", "tier": "standard"}]  # 1% tolerance
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=True,
                message="All checks passed",
                failed_count=0,
                total_count=1000,
                covered_fields=["customer_id"],
                failed_row_ids=set()
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=1000,
            cdas=cdas,
            validation_results=results
        )

        assert len(report.results) == 1
        assert report.results[0].status == SLAStatus.GREEN
        assert report.results[0].failure_rate == 0.0

    def test_evaluate_red_status(self, evaluator):
        """Test evaluation returns RED for high failure rate."""
        cdas = [{"field": "email", "tier": "high"}]  # 0.1% tolerance
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=False,
                message="Missing values found",
                failed_count=50,
                total_count=1000,
                covered_fields=["email"],
                failed_row_ids={1, 2, 3, 4, 5}  # Only 5 unique rows
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=1000,
            cdas=cdas,
            validation_results=results
        )

        assert len(report.results) == 1
        assert report.results[0].status == SLAStatus.RED
        assert report.results[0].failure_rate == 0.005  # 5/1000

    def test_evaluate_amber_status(self, evaluator):
        """Test evaluation returns AMBER when approaching tolerance."""
        cdas = [{"field": "phone", "tolerance": 0.10}]  # 10% tolerance
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=False,
                message="Missing values found",
                failed_count=85,
                total_count=1000,
                covered_fields=["phone"],
                failed_row_ids=set(range(85))  # 85 unique rows = 8.5%
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=1000,
            cdas=cdas,
            validation_results=results,
            sla_defaults={"warning_at": 0.8}  # AMBER at 8%
        )

        assert len(report.results) == 1
        assert report.results[0].status == SLAStatus.AMBER

    def test_evaluate_not_evaluated_no_coverage(self, evaluator):
        """Test NOT_EVALUATED when no validations cover field."""
        cdas = [{"field": "uncovered_field", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=True,
                message="All checks passed",
                failed_count=0,
                total_count=1000,
                covered_fields=["other_field"],  # Doesn't cover the CDA
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=1000,
            cdas=cdas,
            validation_results=results
        )

        assert len(report.results) == 1
        assert report.results[0].status == SLAStatus.NOT_EVALUATED

    def test_aggregation_union_method(self, evaluator):
        """Test union aggregation with failed_row_ids."""
        cdas = [{"field": "email", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=False,
                message="Missing emails",
                failed_count=3,
                total_count=100,
                covered_fields=["email"],
                failed_row_ids={1, 2, 3}
            ),
            ValidationResult(
                rule_name="RegexCheck",
                severity=Severity.WARNING,
                passed=False,
                message="Invalid email format",
                failed_count=5,
                total_count=100,
                covered_fields=["email"],
                failed_row_ids={3, 4, 5, 6, 7}  # Row 3 overlaps
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=100,
            cdas=cdas,
            validation_results=results
        )

        assert report.results[0].aggregation_method == "union"
        # Union of {1,2,3} and {3,4,5,6,7} = {1,2,3,4,5,6,7} = 7 unique rows
        assert report.results[0].bad_records == 7
        assert report.results[0].failure_rate == 0.07

    def test_aggregation_max_rate_method(self, evaluator):
        """Test max_rate aggregation without failed_row_ids."""
        cdas = [{"field": "email", "tier": "standard"}]
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=False,
                message="Missing emails",
                failed_count=3,
                total_count=100,
                covered_fields=["email"],
                failed_row_ids=None  # No row IDs
            ),
            ValidationResult(
                rule_name="RegexCheck",
                severity=Severity.WARNING,
                passed=False,
                message="Invalid email format",
                failed_count=8,
                total_count=100,
                covered_fields=["email"],
                failed_row_ids=None  # No row IDs
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=100,
            cdas=cdas,
            validation_results=results
        )

        assert report.results[0].aggregation_method == "max_rate"
        # Max rate is 8/100 = 8%
        assert report.results[0].bad_records == 8
        assert report.results[0].failure_rate == 0.08

    def test_zero_tolerance_breach(self, evaluator):
        """Test zero tolerance (critical tier) immediately breaches."""
        cdas = [{"field": "transaction_id", "tier": "critical"}]
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=False,
                message="1 missing ID",
                failed_count=1,
                total_count=10000,
                covered_fields=["transaction_id"],
                failed_row_ids={42}
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=10000,
            cdas=cdas,
            validation_results=results
        )

        assert report.results[0].status == SLAStatus.RED  # Any failure = RED

    def test_zero_tolerance_green(self, evaluator):
        """Test zero tolerance is GREEN with no failures."""
        cdas = [{"field": "transaction_id", "tier": "critical"}]
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=True,
                message="All IDs present",
                failed_count=0,
                total_count=10000,
                covered_fields=["transaction_id"],
                failed_row_ids=set()
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=10000,
            cdas=cdas,
            validation_results=results
        )

        assert report.results[0].status == SLAStatus.GREEN

    def test_multiple_cdas(self, evaluator):
        """Test evaluation of multiple CDAs."""
        cdas = [
            {"field": "customer_id", "tier": "critical"},
            {"field": "email", "tier": "high"},
            {"field": "phone", "tier": "standard"},
        ]
        results = [
            ValidationResult(
                rule_name="MandatoryFieldCheck",
                severity=Severity.ERROR,
                passed=False,
                message="Missing values",
                failed_count=5,
                total_count=1000,
                covered_fields=["customer_id", "email", "phone"],
                failed_row_ids={1, 2, 3, 4, 5}
            )
        ]

        report = evaluator.evaluate(
            file_name="test.csv",
            dataset_row_count=1000,
            cdas=cdas,
            validation_results=results
        )

        assert len(report.results) == 3
        # All three CDAs have same failure rate but different tolerances
        # customer_id: critical (0%) -> RED
        # email: high (0.1%) -> RED
        # phone: standard (1%) -> GREEN (0.5% < 1%)
        assert report.results[0].status == SLAStatus.RED  # customer_id
        assert report.results[1].status == SLAStatus.RED  # email
        assert report.results[2].status == SLAStatus.GREEN  # phone


class TestFormatSLACLIOutput:
    """Tests for CLI output formatting."""

    def test_format_empty_report(self):
        """Test formatting report with no results."""
        report = SLAReport(
            file_name="empty.csv",
            dataset_row_count=0,
            results=[]
        )
        output = format_sla_cli_output(report)

        assert "SLA COMPLIANCE" in output
        assert "empty.csv" in output
        assert "No CDAs defined" in output

    def test_format_with_results(self):
        """Test formatting report with results."""
        results = [
            SLAResult(
                field="customer_id", status=SLAStatus.GREEN, tier_name="critical",
                tolerance=0.0, failure_rate=0.0, bad_records=0,
                evaluated_records=1000, aggregation_method="union",
                contributing_validations=["MandatoryFieldCheck"]
            ),
            SLAResult(
                field="email", status=SLAStatus.RED, tier_name="high",
                tolerance=0.001, failure_rate=0.05, bad_records=50,
                evaluated_records=1000, aggregation_method="union",
                contributing_validations=["RegexCheck"]
            ),
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=1000,
            results=results
        )
        output = format_sla_cli_output(report)

        assert "SLA COMPLIANCE" in output
        assert "test.csv" in output
        assert "customer_id" in output
        assert "email" in output
        assert "GREEN" in output or "🟢" in output
        assert "RED" in output or "🔴" in output

    def test_format_contains_summary(self):
        """Test output contains summary line."""
        results = [
            SLAResult(
                field="test", status=SLAStatus.GREEN, tier_name="standard",
                tolerance=0.01, failure_rate=0.001, bad_records=1,
                evaluated_records=1000, aggregation_method="union",
                contributing_validations=[]
            )
        ]
        report = SLAReport(
            file_name="test.csv",
            dataset_row_count=1000,
            results=results
        )
        output = format_sla_cli_output(report)

        assert "Summary" in output
