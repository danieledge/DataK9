"""
Registration of all built-in validation rules.

This module automatically registers all built-in validations
with the global registry when imported.
"""

from validation_framework.core.registry import register_validation

# Import all built-in validation classes
from validation_framework.validations.builtin.file_checks import (
    EmptyFileCheck,
    RowCountRangeCheck,
    FileSizeCheck,
    CSVFormatCheck,
)

from validation_framework.validations.builtin.schema_checks import (
    SchemaMatchCheck,
    ColumnPresenceCheck,
)

from validation_framework.validations.builtin.field_checks import (
    MandatoryFieldCheck,
    RegexCheck,
    ValidValuesCheck,
    RangeCheck,
    DateFormatCheck,
)

from validation_framework.validations.builtin.record_checks import (
    DuplicateRowCheck,
    BlankRecordCheck,
    UniqueKeyCheck,
)

from validation_framework.validations.builtin.inline_checks import (
    InlineRegexCheck,
    InlineBusinessRuleCheck,
    InlineLookupCheck,
)
from validation_framework.validations.builtin.conditional import ConditionalValidation

from validation_framework.validations.builtin.advanced_checks import (
    StatisticalOutlierCheck,
    CrossFieldComparisonCheck,
    FreshnessCheck,
    CompletenessCheck,
    StringLengthCheck,
    NumericPrecisionCheck,
)

from validation_framework.validations.builtin.cross_file_checks import (
    ReferentialIntegrityCheck,
    CrossFileComparisonCheck,
    CrossFileDuplicateCheck,
)

from validation_framework.validations.builtin.database_checks import (
    SQLCustomCheck,
    DatabaseReferentialIntegrityCheck,
    DatabaseConstraintCheck,
)

from validation_framework.validations.builtin.temporal_checks import (
    BaselineComparisonCheck,
    TrendDetectionCheck,
)

from validation_framework.validations.builtin.statistical_checks import (
    DistributionCheck,
    CorrelationCheck,
    AdvancedAnomalyDetectionCheck,
)


def register_all_builtin_validations():
    """
    Register all built-in validation rules with the global registry.

    This function is called automatically when the module is imported.
    """
    # File-level checks
    register_validation("EmptyFileCheck", EmptyFileCheck)
    register_validation("RowCountRangeCheck", RowCountRangeCheck)
    register_validation("FileSizeCheck", FileSizeCheck)
    register_validation("CSVFormatCheck", CSVFormatCheck)

    # Schema checks
    register_validation("SchemaMatchCheck", SchemaMatchCheck)
    register_validation("ColumnPresenceCheck", ColumnPresenceCheck)

    # Field-level checks
    register_validation("MandatoryFieldCheck", MandatoryFieldCheck)
    register_validation("RegexCheck", RegexCheck)
    register_validation("ValidValuesCheck", ValidValuesCheck)
    register_validation("RangeCheck", RangeCheck)
    register_validation("DateFormatCheck", DateFormatCheck)

    # Record-level checks
    register_validation("DuplicateRowCheck", DuplicateRowCheck)
    register_validation("BlankRecordCheck", BlankRecordCheck)
    register_validation("UniqueKeyCheck", UniqueKeyCheck)

    # Inline/Custom checks (Bespoke validations)
    register_validation("InlineRegexCheck", InlineRegexCheck)
    register_validation("InlineBusinessRuleCheck", InlineBusinessRuleCheck)
    register_validation("InlineLookupCheck", InlineLookupCheck)

    # Conditional validation wrapper
    register_validation("ConditionalValidation", ConditionalValidation)

    # Advanced checks (statistical, cross-field, freshness)
    register_validation("StatisticalOutlierCheck", StatisticalOutlierCheck)
    register_validation("CrossFieldComparisonCheck", CrossFieldComparisonCheck)
    register_validation("FreshnessCheck", FreshnessCheck)
    register_validation("CompletenessCheck", CompletenessCheck)
    register_validation("StringLengthCheck", StringLengthCheck)
    register_validation("NumericPrecisionCheck", NumericPrecisionCheck)

    # Cross-file validation checks
    register_validation("ReferentialIntegrityCheck", ReferentialIntegrityCheck)
    register_validation("CrossFileComparisonCheck", CrossFileComparisonCheck)
    register_validation("CrossFileDuplicateCheck", CrossFileDuplicateCheck)

    # Database validation checks
    register_validation("SQLCustomCheck", SQLCustomCheck)
    register_validation("DatabaseReferentialIntegrityCheck", DatabaseReferentialIntegrityCheck)
    register_validation("DatabaseConstraintCheck", DatabaseConstraintCheck)

    # Temporal/Historical validation checks
    register_validation("BaselineComparisonCheck", BaselineComparisonCheck)
    register_validation("TrendDetectionCheck", TrendDetectionCheck)

    # Advanced statistical validation checks
    register_validation("DistributionCheck", DistributionCheck)
    register_validation("CorrelationCheck", CorrelationCheck)
    register_validation("AdvancedAnomalyDetectionCheck", AdvancedAnomalyDetectionCheck)


# Auto-register on import
register_all_builtin_validations()
