"""
Unit tests for the Validation Policy System.

Tests policy schema, built-in policies, analyzer, and config integration.
"""

import pytest
import tempfile
import yaml
from pathlib import Path

from validation_framework.policy.schema import (
    ValidationPolicy,
    PolicyViolation,
    EnforcementMode,
)
from validation_framework.policy.builtin_policies import (
    POLICIES,
    get_policy,
    list_policies,
    DEFAULT_POLICY,
)
from validation_framework.policy.analyzer import PolicyAnalyzer, check_policy_compliance
from validation_framework.policy.defaults import (
    get_default_check_config,
    is_auto_fixable,
    get_auto_fixable_checks,
)


# ============================================================================
# POLICY SCHEMA TESTS
# ============================================================================

@pytest.mark.unit
class TestValidationPolicy:
    """Test ValidationPolicy dataclass and methods."""

    def test_create_policy_with_defaults(self):
        """Test creating a policy with default values."""
        policy = ValidationPolicy(name="test")
        assert policy.name == "test"
        assert policy.version == "1.0"
        assert policy.enforcement == EnforcementMode.WARN
        assert policy.universal_checks == []
        assert policy.format_checks == {}
        assert policy.cda_require_all == []
        assert policy.cda_require_one_of == []
        assert policy.recommended_checks == []

    def test_create_policy_with_values(self):
        """Test creating a policy with custom values."""
        policy = ValidationPolicy(
            name="custom",
            version="2.0",
            description="Test policy",
            enforcement=EnforcementMode.ERROR,
            universal_checks=["EmptyFileCheck"],
            format_checks={"csv": ["CSVFormatCheck"]},
            cda_require_all=["MandatoryFieldCheck"],
            cda_require_one_of=["RegexCheck", "ValidValuesCheck"],
            recommended_checks=["RowCountRangeCheck"],
        )

        assert policy.name == "custom"
        assert policy.version == "2.0"
        assert policy.enforcement == EnforcementMode.ERROR
        assert "EmptyFileCheck" in policy.universal_checks
        assert "CSVFormatCheck" in policy.format_checks.get("csv", [])
        assert "MandatoryFieldCheck" in policy.cda_require_all
        assert "RegexCheck" in policy.cda_require_one_of

    def test_from_dict(self):
        """Test creating policy from dictionary."""
        data = {
            "name": "from_dict",
            "enforcement": "error",
            "universal_checks": ["EmptyFileCheck"],
            "format_checks": {"csv": ["CSVFormatCheck"]},
        }
        policy = ValidationPolicy.from_dict(data)

        assert policy.name == "from_dict"
        assert policy.enforcement == EnforcementMode.ERROR
        assert "EmptyFileCheck" in policy.universal_checks

    def test_from_dict_invalid_enforcement(self):
        """Test from_dict with invalid enforcement defaults to WARN."""
        data = {"name": "test", "enforcement": "invalid"}
        policy = ValidationPolicy.from_dict(data)
        assert policy.enforcement == EnforcementMode.WARN

    def test_to_dict(self):
        """Test converting policy to dictionary."""
        policy = ValidationPolicy(
            name="test",
            universal_checks=["EmptyFileCheck"],
        )
        result = policy.to_dict()

        assert result["name"] == "test"
        assert result["enforcement"] == "warn"
        assert "EmptyFileCheck" in result["universal_checks"]

    def test_copy_with_enforcement(self):
        """Test creating copy with different enforcement mode."""
        policy = ValidationPolicy(
            name="test",
            enforcement=EnforcementMode.WARN,
            universal_checks=["EmptyFileCheck"],
        )

        copy = policy.copy_with_enforcement(EnforcementMode.ERROR)

        assert copy.enforcement == EnforcementMode.ERROR
        assert copy.name == policy.name
        assert copy.universal_checks == policy.universal_checks
        # Original unchanged
        assert policy.enforcement == EnforcementMode.WARN

    def test_get_required_checks_for_format(self):
        """Test getting required checks for specific format."""
        policy = ValidationPolicy(
            name="test",
            universal_checks=["EmptyFileCheck"],
            format_checks={
                "csv": ["CSVFormatCheck"],
                "parquet": ["SchemaMatchCheck"],
            },
        )

        csv_checks = policy.get_required_checks_for_format("csv")
        parquet_checks = policy.get_required_checks_for_format("parquet")
        json_checks = policy.get_required_checks_for_format("json")

        assert "EmptyFileCheck" in csv_checks
        assert "CSVFormatCheck" in csv_checks
        assert "EmptyFileCheck" in parquet_checks
        assert "SchemaMatchCheck" in parquet_checks
        assert "EmptyFileCheck" in json_checks
        assert "CSVFormatCheck" not in json_checks


@pytest.mark.unit
class TestPolicyViolation:
    """Test PolicyViolation dataclass."""

    def test_create_violation(self):
        """Test creating a policy violation."""
        violation = PolicyViolation(
            file_name="test.csv",
            check_type="EmptyFileCheck",
            reason="Required check missing",
            severity="required",
        )

        assert violation.file_name == "test.csv"
        assert violation.check_type == "EmptyFileCheck"
        assert violation.severity == "required"
        assert violation.auto_fixable is True  # Default

    def test_violation_str(self):
        """Test string representation of violation."""
        violation = PolicyViolation(
            file_name="test.csv",
            check_type="EmptyFileCheck",
            reason="Missing required check",
            severity="required",
        )

        str_repr = str(violation)
        assert "test.csv" in str_repr
        assert "EmptyFileCheck" in str_repr

    def test_violation_with_cda_field(self):
        """Test violation for CDA field."""
        violation = PolicyViolation(
            file_name="test.csv",
            check_type="MandatoryFieldCheck",
            reason="CDA field missing validation",
            severity="required",
            cda_field="customer_id",
        )

        str_repr = str(violation)
        assert "customer_id" in str_repr


# ============================================================================
# BUILT-IN POLICIES TESTS
# ============================================================================

@pytest.mark.unit
class TestBuiltinPolicies:
    """Test built-in policy definitions."""

    def test_policies_exist(self):
        """Test that all expected policies exist."""
        assert "none" in POLICIES
        assert "minimal" in POLICIES
        assert "standard" in POLICIES
        assert "strict" in POLICIES

    def test_get_policy(self):
        """Test getting policy by name."""
        policy = get_policy("standard")
        assert policy.name == "standard"

    def test_get_policy_case_insensitive(self):
        """Test policy lookup is case insensitive."""
        policy = get_policy("STANDARD")
        assert policy.name == "standard"

    def test_get_policy_invalid(self):
        """Test getting non-existent policy raises error."""
        with pytest.raises(KeyError):
            get_policy("nonexistent")

    def test_list_policies(self):
        """Test listing all policies."""
        policies = list_policies()
        assert len(policies) >= 4
        assert "standard" in policies
        assert all(isinstance(v, str) for v in policies.values())

    def test_none_policy_has_no_requirements(self):
        """Test 'none' policy has no required checks."""
        policy = POLICIES["none"]
        assert policy.universal_checks == []
        assert policy.format_checks == {}
        assert policy.cda_require_all == []
        assert policy.cda_require_one_of == []

    def test_minimal_policy(self):
        """Test 'minimal' policy requirements."""
        policy = POLICIES["minimal"]
        assert "EmptyFileCheck" in policy.universal_checks
        assert len(policy.format_checks) == 0

    def test_standard_policy(self):
        """Test 'standard' policy requirements."""
        policy = POLICIES["standard"]
        assert "EmptyFileCheck" in policy.universal_checks
        assert "CSVFormatCheck" in policy.format_checks.get("csv", [])
        assert len(policy.cda_require_one_of) > 0

    def test_strict_policy(self):
        """Test 'strict' policy requirements."""
        policy = POLICIES["strict"]
        assert policy.enforcement == EnforcementMode.ERROR
        assert "EmptyFileCheck" in policy.universal_checks
        assert "RowCountRangeCheck" in policy.universal_checks
        assert "SchemaMatchCheck" in policy.format_checks.get("csv", [])
        assert "MandatoryFieldCheck" in policy.cda_require_all

    def test_default_policy(self):
        """Test default policy is 'standard'."""
        assert DEFAULT_POLICY == "standard"


# ============================================================================
# DEFAULT CHECK CONFIGS TESTS
# ============================================================================

@pytest.mark.unit
class TestDefaultCheckConfigs:
    """Test default check configurations for auto-injection."""

    def test_get_default_check_config(self):
        """Test getting default config for known check."""
        config = get_default_check_config("EmptyFileCheck")

        assert config["type"] == "EmptyFileCheck"
        assert "severity" in config
        assert "params" in config

    def test_get_default_check_config_unknown(self):
        """Test getting config for unknown check returns generic."""
        config = get_default_check_config("UnknownCheck")

        assert config["type"] == "UnknownCheck"
        assert "severity" in config

    def test_is_auto_fixable(self):
        """Test checking if check is auto-fixable."""
        assert is_auto_fixable("EmptyFileCheck") is True
        assert is_auto_fixable("CSVFormatCheck") is True
        # Checks requiring params are not auto-fixable
        assert is_auto_fixable("MandatoryFieldCheck") is False
        assert is_auto_fixable("RegexCheck") is False

    def test_get_auto_fixable_checks(self):
        """Test getting list of auto-fixable checks."""
        fixable = get_auto_fixable_checks()

        assert "EmptyFileCheck" in fixable
        assert "CSVFormatCheck" in fixable
        # Param-requiring checks not in list
        assert "MandatoryFieldCheck" not in fixable


# ============================================================================
# POLICY ANALYZER TESTS
# ============================================================================

@pytest.mark.unit
class TestPolicyAnalyzer:
    """Test PolicyAnalyzer functionality."""

    @pytest.fixture
    def minimal_config(self):
        """Minimal config with no validations."""
        return {
            "validation_job": {
                "name": "Test Job",
                "files": [
                    {
                        "name": "test_file",
                        "path": "test.csv",
                        "format": "csv",
                        "validations": [],
                    }
                ],
            }
        }

    @pytest.fixture
    def compliant_config(self):
        """Config that complies with standard policy."""
        return {
            "validation_job": {
                "name": "Test Job",
                "files": [
                    {
                        "name": "test_file",
                        "path": "test.csv",
                        "format": "csv",
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"},
                            {"type": "CSVFormatCheck", "severity": "ERROR"},
                            {"type": "RowCountRangeCheck", "severity": "WARNING"},
                            {"type": "SchemaMatchCheck", "severity": "ERROR"},
                        ],
                    }
                ],
            }
        }

    def test_analyze_empty_config(self, minimal_config):
        """Test analyzing config with no validations."""
        policy = get_policy("standard")
        analyzer = PolicyAnalyzer(policy)

        violations = analyzer.analyze(minimal_config)

        assert len(violations) > 0
        # Should detect missing EmptyFileCheck and CSVFormatCheck
        check_types = {v.check_type for v in violations}
        assert "EmptyFileCheck" in check_types
        assert "CSVFormatCheck" in check_types

    def test_analyze_compliant_config(self, compliant_config):
        """Test analyzing compliant config."""
        policy = get_policy("standard")
        analyzer = PolicyAnalyzer(policy)

        violations = analyzer.analyze(compliant_config)

        # No required violations
        required = [v for v in violations if v.severity == "required"]
        assert len(required) == 0

    def test_analyze_with_none_policy(self, minimal_config):
        """Test with 'none' policy reports no violations."""
        policy = get_policy("none")
        analyzer = PolicyAnalyzer(policy)

        violations = analyzer.analyze(minimal_config)

        assert len(violations) == 0

    def test_analyze_format_specific(self):
        """Test format-specific check detection."""
        config = {
            "validation_job": {
                "files": [
                    {
                        "name": "csv_file",
                        "path": "test.csv",
                        "format": "csv",
                        "validations": [{"type": "EmptyFileCheck"}],
                    },
                    {
                        "name": "parquet_file",
                        "path": "test.parquet",
                        "format": "parquet",
                        "validations": [{"type": "EmptyFileCheck"}],
                    },
                ],
            }
        }

        policy = get_policy("strict")
        analyzer = PolicyAnalyzer(policy)
        violations = analyzer.analyze(config)

        # CSV should need CSVFormatCheck
        csv_violations = [v for v in violations if v.file_name == "csv_file"]
        csv_checks = {v.check_type for v in csv_violations}
        assert "CSVFormatCheck" in csv_checks

        # Parquet should need SchemaMatchCheck (per strict policy)
        parquet_violations = [v for v in violations if v.file_name == "parquet_file"]
        parquet_checks = {v.check_type for v in parquet_violations}
        assert "SchemaMatchCheck" in parquet_checks

    def test_analyze_cda_coverage(self):
        """Test CDA coverage analysis."""
        config = {
            "validation_job": {
                "files": [
                    {
                        "name": "test_file",
                        "path": "test.csv",
                        "format": "csv",
                        "critical_data_attributes": [
                            {"field": "customer_id", "description": "Primary ID"},
                            {"field": "email", "description": "Contact email"},
                        ],
                        "validations": [
                            {"type": "EmptyFileCheck"},
                            {"type": "CSVFormatCheck"},
                            # Only customer_id has validation, email doesn't
                            {
                                "type": "MandatoryFieldCheck",
                                "params": {"fields": ["customer_id"]},
                            },
                        ],
                    }
                ],
            }
        }

        policy = get_policy("standard")
        analyzer = PolicyAnalyzer(policy)
        violations = analyzer.analyze(config)

        # Should detect that email CDA lacks coverage
        cda_violations = [v for v in violations if v.cda_field == "email"]
        assert len(cda_violations) > 0

    def test_get_summary(self, minimal_config):
        """Test getting violation summary."""
        policy = get_policy("standard")
        analyzer = PolicyAnalyzer(policy)
        violations = analyzer.analyze(minimal_config)

        summary = analyzer.get_summary(violations)

        assert "policy_name" in summary
        assert "total_violations" in summary
        assert "required_violations" in summary
        assert "recommended_violations" in summary
        assert summary["policy_name"] == "standard"


@pytest.mark.unit
class TestCheckPolicyCompliance:
    """Test the convenience function."""

    def test_compliant_config(self):
        """Test compliance check with compliant config."""
        config = {
            "validation_job": {
                "files": [
                    {
                        "name": "test",
                        "path": "test.csv",
                        "format": "csv",
                        "validations": [
                            {"type": "EmptyFileCheck"},
                            {"type": "CSVFormatCheck"},
                        ],
                    }
                ],
            }
        }

        policy = get_policy("minimal")
        is_compliant, violations = check_policy_compliance(config, policy)

        assert is_compliant is True
        # May still have recommended violations
        required = [v for v in violations if v.severity == "required"]
        assert len(required) == 0

    def test_non_compliant_config(self):
        """Test compliance check with non-compliant config."""
        config = {
            "validation_job": {
                "files": [
                    {
                        "name": "test",
                        "path": "test.csv",
                        "format": "csv",
                        "validations": [],
                    }
                ],
            }
        }

        policy = get_policy("strict")
        is_compliant, violations = check_policy_compliance(config, policy)

        assert is_compliant is False
        required = [v for v in violations if v.severity == "required"]
        assert len(required) > 0


# ============================================================================
# CONFIG INTEGRATION TESTS
# ============================================================================

@pytest.mark.unit
class TestConfigPolicyIntegration:
    """Test policy integration in ValidationConfig."""

    def test_config_loads_default_policy(self, tmp_path):
        """Test config loads default policy when not specified."""
        from validation_framework.core.config import ValidationConfig

        # Create temp CSV file
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {
                        "name": "test",
                        "path": str(csv_file),
                        "format": "csv",
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"},
                            {"type": "CSVFormatCheck", "severity": "ERROR"},
                        ],
                    }
                ],
            }
        }

        config = ValidationConfig(config_dict)

        assert config.policy is not None
        assert config.policy.name == DEFAULT_POLICY

    def test_config_loads_specified_policy(self, tmp_path):
        """Test config loads specified policy."""
        from validation_framework.core.config import ValidationConfig

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        config_dict = {
            "validation_job": {
                "name": "Test",
                "policy": "minimal",
                "files": [
                    {
                        "name": "test",
                        "path": str(csv_file),
                        "validations": [{"type": "EmptyFileCheck"}],
                    }
                ],
            }
        }

        config = ValidationConfig(config_dict)

        assert config.policy.name == "minimal"

    def test_config_policy_enforcement_override(self, tmp_path):
        """Test policy enforcement mode override."""
        from validation_framework.core.config import ValidationConfig

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_dict = {
            "validation_job": {
                "name": "Test",
                "policy": "standard",
                "policy_enforcement": "auto",
                "files": [
                    {
                        "name": "test",
                        "path": str(csv_file),
                        "validations": [],  # Empty - will trigger auto-inject
                    }
                ],
            }
        }

        config = ValidationConfig(config_dict)

        assert config.policy.enforcement == EnforcementMode.AUTO
        # Auto-inject should have added EmptyFileCheck
        check_types = {v["type"] for v in config.files[0]["validations"]}
        assert "EmptyFileCheck" in check_types

    def test_config_strict_policy_fails(self, tmp_path):
        """Test strict policy raises error on violations."""
        from validation_framework.core.config import ValidationConfig
        from validation_framework.core.exceptions import ConfigValidationError

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_dict = {
            "validation_job": {
                "name": "Test",
                "policy": "strict",
                "policy_enforcement": "error",
                "files": [
                    {
                        "name": "test",
                        "path": str(csv_file),
                        "validations": [],  # Missing required checks
                    }
                ],
            }
        }

        with pytest.raises(ConfigValidationError) as exc_info:
            ValidationConfig(config_dict)

        assert "Policy" in str(exc_info.value)
        assert "violations" in str(exc_info.value).lower()

    def test_config_none_policy_allows_empty(self, tmp_path):
        """Test 'none' policy allows empty validations."""
        from validation_framework.core.config import ValidationConfig

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n")

        config_dict = {
            "validation_job": {
                "name": "Test",
                "policy": "none",
                "files": [
                    {
                        "name": "test",
                        "path": str(csv_file),
                        "validations": [],  # No validations
                    }
                ],
            }
        }

        # Should not raise
        config = ValidationConfig(config_dict)
        assert config.policy.name == "none"
        assert len(config.policy_violations) == 0
