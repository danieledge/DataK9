"""
Default Check Configurations

Provides sensible default configurations for auto-injection of missing checks.
These defaults are used when policy enforcement mode is set to AUTO.
"""

from typing import Dict, Any, Optional
from validation_framework.core.results import Severity


# ============================================================================
# DEFAULT CHECK CONFIGURATIONS
# ============================================================================

DEFAULT_CHECK_CONFIGS: Dict[str, Dict[str, Any]] = {
    # -------------------------------------------------------------------------
    # File-Level Checks
    # -------------------------------------------------------------------------
    "EmptyFileCheck": {
        "type": "EmptyFileCheck",
        "severity": Severity.ERROR,
        "params": {"check_data_rows": True},
        "description": "Auto-injected by policy: Ensures file is not empty",
        "enabled": True,
    },

    "RowCountRangeCheck": {
        "type": "RowCountRangeCheck",
        "severity": Severity.WARNING,
        "params": {"min_rows": 1},
        "description": "Auto-injected by policy: Ensures file has at least 1 row",
        "enabled": True,
    },

    "FileSizeCheck": {
        "type": "FileSizeCheck",
        "severity": Severity.WARNING,
        "params": {"min_size_mb": 0},
        "description": "Auto-injected by policy: Basic file size check",
        "enabled": True,
    },

    "CSVFormatCheck": {
        "type": "CSVFormatCheck",
        "severity": Severity.ERROR,
        "params": {"sample_rows": 1000, "max_errors": 10},
        "description": "Auto-injected by policy: Validates CSV format integrity",
        "enabled": True,
    },

    # -------------------------------------------------------------------------
    # Schema Checks
    # -------------------------------------------------------------------------
    "SchemaMatchCheck": {
        "type": "SchemaMatchCheck",
        "severity": Severity.ERROR,
        "params": {"allow_extra": True, "allow_missing": False},
        "description": "Auto-injected by policy: Validates schema structure",
        "enabled": True,
        "auto_fixable": False,  # Requires expected_columns param
    },

    "ColumnPresenceCheck": {
        "type": "ColumnPresenceCheck",
        "severity": Severity.ERROR,
        "params": {},
        "description": "Auto-injected by policy: Checks required columns exist",
        "enabled": True,
        "auto_fixable": False,  # Requires required_columns param
    },

    # -------------------------------------------------------------------------
    # Field-Level Checks
    # -------------------------------------------------------------------------
    "MandatoryFieldCheck": {
        "type": "MandatoryFieldCheck",
        "severity": Severity.ERROR,
        "params": {},
        "description": "Auto-injected by policy: Checks for required field values",
        "enabled": True,
        "auto_fixable": False,  # Requires fields param
    },

    "RegexCheck": {
        "type": "RegexCheck",
        "severity": Severity.ERROR,
        "params": {},
        "description": "Auto-injected by policy: Pattern validation",
        "enabled": True,
        "auto_fixable": False,  # Requires field and pattern params
    },

    "ValidValuesCheck": {
        "type": "ValidValuesCheck",
        "severity": Severity.ERROR,
        "params": {},
        "description": "Auto-injected by policy: Validates allowed values",
        "enabled": True,
        "auto_fixable": False,  # Requires field and valid_values params
    },

    "RangeCheck": {
        "type": "RangeCheck",
        "severity": Severity.ERROR,
        "params": {},
        "description": "Auto-injected by policy: Numeric range validation",
        "enabled": True,
        "auto_fixable": False,  # Requires field, min_value, max_value params
    },

    "DateFormatCheck": {
        "type": "DateFormatCheck",
        "severity": Severity.ERROR,
        "params": {"format": "%Y-%m-%d"},
        "description": "Auto-injected by policy: Date format validation",
        "enabled": True,
        "auto_fixable": False,  # Requires field param
    },

    # -------------------------------------------------------------------------
    # Record-Level Checks
    # -------------------------------------------------------------------------
    "DuplicateRowCheck": {
        "type": "DuplicateRowCheck",
        "severity": Severity.WARNING,
        "params": {},
        "description": "Auto-injected by policy: Checks for duplicate rows",
        "enabled": True,
        "auto_fixable": False,  # Requires key_fields param for meaningful check
    },

    "BlankRecordCheck": {
        "type": "BlankRecordCheck",
        "severity": Severity.WARNING,
        "params": {},
        "description": "Auto-injected by policy: Checks for blank records",
        "enabled": True,
    },

    "UniqueKeyCheck": {
        "type": "UniqueKeyCheck",
        "severity": Severity.ERROR,
        "params": {},
        "description": "Auto-injected by policy: Validates key uniqueness",
        "enabled": True,
        "auto_fixable": False,  # Requires fields param
    },

    # -------------------------------------------------------------------------
    # Advanced Checks
    # -------------------------------------------------------------------------
    "CompletenessCheck": {
        "type": "CompletenessCheck",
        "severity": Severity.WARNING,
        "params": {"min_completeness": 0.9},
        "description": "Auto-injected by policy: Checks field completeness",
        "enabled": True,
        "auto_fixable": False,  # Requires fields param
    },
}


def get_default_check_config(check_type: str) -> Dict[str, Any]:
    """
    Get default configuration for a check type.

    Args:
        check_type: Name of the validation check

    Returns:
        Dictionary with default configuration for the check
    """
    if check_type in DEFAULT_CHECK_CONFIGS:
        # Return a copy to prevent mutation
        config = DEFAULT_CHECK_CONFIGS[check_type].copy()
        config['params'] = config.get('params', {}).copy()
        return config

    # Generic fallback for unknown check types
    return {
        "type": check_type,
        "severity": Severity.WARNING,
        "params": {},
        "description": f"Auto-injected by policy: {check_type}",
        "enabled": True,
    }


def is_auto_fixable(check_type: str) -> bool:
    """
    Check if a validation type can be auto-injected with defaults.

    Some checks require specific parameters (like field names) and cannot
    be meaningfully auto-injected without user configuration.

    Args:
        check_type: Name of the validation check

    Returns:
        True if check can be auto-injected with sensible defaults
    """
    if check_type not in DEFAULT_CHECK_CONFIGS:
        return False

    config = DEFAULT_CHECK_CONFIGS[check_type]
    return config.get('auto_fixable', True)


def get_auto_fixable_checks() -> list:
    """
    Get list of check types that can be auto-injected.

    Returns:
        List of check type names that support auto-injection
    """
    return [
        check_type
        for check_type, config in DEFAULT_CHECK_CONFIGS.items()
        if config.get('auto_fixable', True)
    ]
