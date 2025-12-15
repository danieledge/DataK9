"""
Built-in Validation Policies

Provides pre-configured policies for common use cases:
- minimal: Basic checks only (EmptyFileCheck)
- standard: Recommended for most projects (default)
- strict: Comprehensive checks for critical data pipelines
- none: No policy enforcement (bypass all checks)
"""

from validation_framework.policy.schema import ValidationPolicy, EnforcementMode


# ============================================================================
# BUILT-IN POLICIES
# ============================================================================

POLICIES = {
    # -------------------------------------------------------------------------
    # NONE: No policy enforcement
    # -------------------------------------------------------------------------
    "none": ValidationPolicy(
        name="none",
        version="1.0",
        description="No policy enforcement - all checks are optional",
        enforcement=EnforcementMode.WARN,
        universal_checks=[],
        format_checks={},
        cda_require_all=[],
        cda_require_one_of=[],
        recommended_checks=[],
    ),

    # -------------------------------------------------------------------------
    # MINIMAL: Basic sanity checks only
    # -------------------------------------------------------------------------
    "minimal": ValidationPolicy(
        name="minimal",
        version="1.0",
        description="Basic sanity checks - ensures files are not empty",
        enforcement=EnforcementMode.WARN,
        universal_checks=[
            "EmptyFileCheck",
        ],
        format_checks={},
        cda_require_all=[],
        cda_require_one_of=[],
        recommended_checks=[
            "RowCountRangeCheck",
        ],
    ),

    # -------------------------------------------------------------------------
    # STANDARD: Recommended for most projects (DEFAULT)
    # -------------------------------------------------------------------------
    "standard": ValidationPolicy(
        name="standard",
        version="1.0",
        description="Recommended policy for most data validation projects",
        enforcement=EnforcementMode.WARN,
        universal_checks=[
            "EmptyFileCheck",
        ],
        format_checks={
            "csv": ["CSVFormatCheck"],
        },
        cda_require_all=[],
        cda_require_one_of=[
            "MandatoryFieldCheck",
            "RegexCheck",
            "ValidValuesCheck",
            "RangeCheck",
        ],
        recommended_checks=[
            "RowCountRangeCheck",
            "SchemaMatchCheck",
        ],
    ),

    # -------------------------------------------------------------------------
    # STRICT: Comprehensive checks for critical pipelines
    # -------------------------------------------------------------------------
    "strict": ValidationPolicy(
        name="strict",
        version="1.0",
        description="Comprehensive policy for critical data pipelines - fails on violations",
        enforcement=EnforcementMode.ERROR,
        universal_checks=[
            "EmptyFileCheck",
            "RowCountRangeCheck",
        ],
        format_checks={
            "csv": ["CSVFormatCheck", "SchemaMatchCheck"],
            "parquet": ["SchemaMatchCheck"],
            "excel": ["SchemaMatchCheck"],
            "json": ["SchemaMatchCheck"],
        },
        cda_require_all=[
            "MandatoryFieldCheck",
        ],
        cda_require_one_of=[
            "RegexCheck",
            "ValidValuesCheck",
            "RangeCheck",
            "DateFormatCheck",
        ],
        recommended_checks=[
            "DuplicateRowCheck",
            "BlankRecordCheck",
            "CompletenessCheck",
        ],
    ),
}


# Default policy when none specified
DEFAULT_POLICY = "standard"


def get_policy(name: str) -> ValidationPolicy:
    """
    Get a built-in policy by name.

    Args:
        name: Policy name (none, minimal, standard, strict)

    Returns:
        ValidationPolicy instance

    Raises:
        KeyError: If policy name not found
    """
    if name.lower() not in POLICIES:
        available = ", ".join(POLICIES.keys())
        raise KeyError(f"Unknown policy '{name}'. Available policies: {available}")
    return POLICIES[name.lower()]


def list_policies() -> dict:
    """
    Get summary of all available policies.

    Returns:
        Dictionary mapping policy names to their descriptions
    """
    return {
        name: policy.description
        for name, policy in POLICIES.items()
    }
