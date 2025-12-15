"""
Policy Analyzer

Analyzes validation configurations against policy requirements to detect
violations and generate recommendations.
"""

import logging
from typing import Dict, List, Any, Set, Optional

from validation_framework.policy.schema import (
    ValidationPolicy,
    PolicyViolation,
    EnforcementMode,
)
from validation_framework.policy.defaults import is_auto_fixable

logger = logging.getLogger(__name__)


class PolicyAnalyzer:
    """
    Analyzes validation configurations against policy requirements.

    Detects missing required checks at multiple levels:
    - Universal checks (apply to all files)
    - Format-specific checks (e.g., CSVFormatCheck for CSV files)
    - CDA coverage requirements (critical data attributes)
    - Recommended checks (generate warnings only)
    """

    # Mapping of validation types to the fields they validate
    # Used for CDA coverage analysis
    FIELD_VALIDATION_TYPES = {
        'MandatoryFieldCheck': 'fields',
        'RegexCheck': 'field',
        'ValidValuesCheck': 'field',
        'RangeCheck': 'field',
        'DateFormatCheck': 'field',
        'InlineRegexCheck': 'field',
        'StringLengthCheck': 'field',
        'NumericPrecisionCheck': 'field',
        'UniqueKeyCheck': 'fields',
        'DuplicateRowCheck': 'key_fields',
        'CrossFieldComparisonCheck': ['field1', 'field2'],
        'StatisticalOutlierCheck': 'field',
        'CompletenessCheck': 'fields',
        'FreshnessCheck': 'date_field',
        'InlineBusinessRuleCheck': 'fields',
        'InlineLookupCheck': 'field',
        'CorrelationCheck': ['field1', 'field2'],
    }

    def __init__(self, policy: ValidationPolicy, logger_instance: Optional[logging.Logger] = None):
        """
        Initialize the analyzer with a policy.

        Args:
            policy: ValidationPolicy to check against
            logger_instance: Optional logger for output
        """
        self.policy = policy
        self.logger = logger_instance or logger

    def analyze(self, config: Dict[str, Any]) -> List[PolicyViolation]:
        """
        Analyze a complete validation configuration for policy violations.

        Args:
            config: Parsed YAML configuration dictionary

        Returns:
            List of PolicyViolation objects
        """
        violations = []

        # Handle both wrapped and unwrapped config formats
        job_config = config.get('validation_job', config)
        files = job_config.get('files', [])

        for file_config in files:
            file_violations = self._check_file(file_config)
            violations.extend(file_violations)

        return violations

    def _check_file(self, file_config: Dict[str, Any]) -> List[PolicyViolation]:
        """
        Check a single file configuration against policy.

        Args:
            file_config: File configuration dictionary

        Returns:
            List of violations for this file
        """
        violations = []
        file_name = file_config.get('name', 'unnamed')
        file_format = file_config.get('format', 'csv').lower()

        # Get configured validation types
        configured_checks = self._get_configured_checks(file_config)

        # Check universal requirements
        for check in self.policy.universal_checks:
            if check not in configured_checks:
                violations.append(PolicyViolation(
                    file_name=file_name,
                    check_type=check,
                    reason="Required universal check missing",
                    severity="required",
                    auto_fixable=is_auto_fixable(check),
                ))

        # Check format-specific requirements
        format_checks = self.policy.format_checks.get(file_format, [])
        for check in format_checks:
            if check not in configured_checks:
                violations.append(PolicyViolation(
                    file_name=file_name,
                    check_type=check,
                    reason=f"Required for {file_format.upper()} format",
                    severity="required",
                    auto_fixable=is_auto_fixable(check),
                ))

        # Check CDA coverage if CDAs are defined
        cda_violations = self._check_cda_coverage(file_config)
        violations.extend(cda_violations)

        # Check recommended (always "recommended" severity)
        for check in self.policy.recommended_checks:
            if check not in configured_checks:
                violations.append(PolicyViolation(
                    file_name=file_name,
                    check_type=check,
                    reason="Recommended check missing",
                    severity="recommended",
                    auto_fixable=is_auto_fixable(check),
                ))

        return violations

    def _get_configured_checks(self, file_config: Dict[str, Any]) -> Set[str]:
        """
        Extract set of configured validation types from file config.

        Args:
            file_config: File configuration dictionary

        Returns:
            Set of validation type names
        """
        validations = file_config.get('validations', [])
        return {
            v.get('type', '')
            for v in validations
            if v.get('enabled', True)
        }

    def _check_cda_coverage(self, file_config: Dict[str, Any]) -> List[PolicyViolation]:
        """
        Check that Critical Data Attributes have required validation coverage.

        Args:
            file_config: File configuration dictionary

        Returns:
            List of CDA-related violations
        """
        violations = []
        file_name = file_config.get('name', 'unnamed')

        # Get CDA definitions
        cdas = file_config.get('critical_data_attributes', [])
        if not cdas:
            return violations

        # Skip CDA checks if policy has no CDA requirements
        if not self.policy.cda_require_all and not self.policy.cda_require_one_of:
            return violations

        # Build field coverage map
        field_coverage = self._extract_field_coverage(file_config)

        for cda in cdas:
            cda_field = cda.get('field', '')
            if not cda_field:
                continue

            covering_checks = field_coverage.get(cda_field, set())

            # Check require_all: CDA must have ALL of these checks
            for required_check in self.policy.cda_require_all:
                if required_check not in covering_checks:
                    violations.append(PolicyViolation(
                        file_name=file_name,
                        check_type=required_check,
                        reason=f"CDA field '{cda_field}' missing required check",
                        severity="required",
                        auto_fixable=False,  # CDA checks need field params
                        cda_field=cda_field,
                    ))

            # Check require_one_of: CDA must have AT LEAST ONE of these checks
            if self.policy.cda_require_one_of:
                has_any = bool(covering_checks.intersection(self.policy.cda_require_one_of))
                if not has_any:
                    check_options = ", ".join(self.policy.cda_require_one_of)
                    violations.append(PolicyViolation(
                        file_name=file_name,
                        check_type=f"one of [{check_options}]",
                        reason=f"CDA field '{cda_field}' needs at least one value validation",
                        severity="required",
                        auto_fixable=False,
                        cda_field=cda_field,
                    ))

        return violations

    def _extract_field_coverage(self, file_config: Dict[str, Any]) -> Dict[str, Set[str]]:
        """
        Extract which fields are covered by which validation types.

        Args:
            file_config: File configuration dictionary

        Returns:
            Dictionary mapping field names to sets of covering validation types
        """
        field_coverage: Dict[str, Set[str]] = {}
        validations = file_config.get('validations', [])

        for validation in validations:
            if not validation.get('enabled', True):
                continue

            val_type = validation.get('type', '')
            params = validation.get('params', {})

            # Get field parameter names for this validation type
            field_param = self.FIELD_VALIDATION_TYPES.get(val_type)
            if not field_param:
                continue

            # Extract fields from params
            fields = self._get_fields_from_params(params, field_param)

            # Add coverage
            for field in fields:
                if field not in field_coverage:
                    field_coverage[field] = set()
                field_coverage[field].add(val_type)

        return field_coverage

    def _get_fields_from_params(
        self,
        params: Dict[str, Any],
        field_param: Any
    ) -> List[str]:
        """
        Extract field names from validation parameters.

        Args:
            params: Validation parameters dictionary
            field_param: Parameter name(s) containing field info

        Returns:
            List of field names
        """
        fields = []

        if isinstance(field_param, str):
            # Single parameter name (e.g., 'field' or 'fields')
            value = params.get(field_param)
            if isinstance(value, str):
                fields.append(value)
            elif isinstance(value, list):
                fields.extend(value)
        elif isinstance(field_param, list):
            # Multiple parameter names (e.g., ['field1', 'field2'])
            for param_name in field_param:
                value = params.get(param_name)
                if isinstance(value, str):
                    fields.append(value)

        return fields

    def get_summary(self, violations: List[PolicyViolation]) -> Dict[str, Any]:
        """
        Get a summary of policy violations.

        Args:
            violations: List of PolicyViolation objects

        Returns:
            Summary dictionary with counts and details
        """
        required = [v for v in violations if v.severity == "required"]
        recommended = [v for v in violations if v.severity == "recommended"]

        return {
            'policy_name': self.policy.name,
            'enforcement_mode': self.policy.enforcement.value,
            'total_violations': len(violations),
            'required_violations': len(required),
            'recommended_violations': len(recommended),
            'auto_fixable_count': sum(1 for v in violations if v.auto_fixable),
            'files_with_violations': len(set(v.file_name for v in violations)),
            'violations_by_check': self._group_by_check(violations),
            'violations_by_file': self._group_by_file(violations),
        }

    def _group_by_check(self, violations: List[PolicyViolation]) -> Dict[str, int]:
        """Group violations by check type."""
        counts: Dict[str, int] = {}
        for v in violations:
            counts[v.check_type] = counts.get(v.check_type, 0) + 1
        return counts

    def _group_by_file(self, violations: List[PolicyViolation]) -> Dict[str, int]:
        """Group violations by file name."""
        counts: Dict[str, int] = {}
        for v in violations:
            counts[v.file_name] = counts.get(v.file_name, 0) + 1
        return counts


def check_policy_compliance(
    config: Dict[str, Any],
    policy: ValidationPolicy
) -> tuple[bool, List[PolicyViolation]]:
    """
    Convenience function to check if a config complies with a policy.

    Args:
        config: Parsed YAML configuration dictionary
        policy: ValidationPolicy to check against

    Returns:
        Tuple of (is_compliant, violations)
        is_compliant is True if no required violations found
    """
    analyzer = PolicyAnalyzer(policy)
    violations = analyzer.analyze(config)

    required_violations = [v for v in violations if v.severity == "required"]
    is_compliant = len(required_violations) == 0

    return is_compliant, violations
