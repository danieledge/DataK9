"""
CDA Gap Analyzer

Analyzes validation configurations to detect gaps in Critical Data Attribute coverage.
"""

import logging
from typing import Dict, List, Any, Set, Optional
from datetime import datetime

from .models import (
    CDADefinition, CDAFieldCoverage,
    CDAGapResult, CDAAnalysisReport
)


class CDAGapAnalyzer:
    """
    Analyzes validation configurations to identify CDA coverage gaps.

    This analyzer compares defined Critical Data Attributes against configured
    validations to detect fields that lack validation coverage.
    """

    # Mapping of validation types to the fields they validate
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

    # Schema-level validations that cover multiple fields
    SCHEMA_VALIDATION_TYPES = {
        'SchemaMatchCheck': 'expected_schema',
        'ColumnPresenceCheck': 'required_columns',
    }

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the analyzer with optional logger."""
        self.logger = logger or logging.getLogger(__name__)

    def analyze(self, config: Dict[str, Any]) -> CDAAnalysisReport:
        """
        Analyze a complete validation configuration for CDA coverage gaps.

        Args:
            config: Parsed YAML configuration dictionary

        Returns:
            CDAAnalysisReport containing gap analysis for all files
        """
        job_config = config.get('validation_job', config)
        job_name = job_config.get('name', 'Unnamed Job')

        # Get CDA definitions from top-level
        cda_definitions = job_config.get('critical_data_attributes', {})

        if not cda_definitions:
            self.logger.info("No critical_data_attributes defined - skipping CDA analysis")
            return CDAAnalysisReport(job_name=job_name)

        files = job_config.get('files', [])
        results = []

        for file_config in files:
            file_name = file_config.get('name', 'unnamed')

            # Get CDAs for this specific file
            file_cdas = cda_definitions.get(file_name, [])

            if file_cdas:
                result = self._analyze_file(file_name, file_cdas, file_config)
                results.append(result)

        return CDAAnalysisReport(
            job_name=job_name,
            results=results,
            analysis_timestamp=datetime.now()
        )

    def _analyze_file(
        self,
        file_name: str,
        cda_list: List[Dict],
        file_config: Dict[str, Any]
    ) -> CDAGapResult:
        """
        Analyze CDA coverage for a single file.

        Args:
            file_name: Name of the file being analyzed
            cda_list: List of CDA definitions for this file
            file_config: File configuration including validations

        Returns:
            CDAGapResult with coverage analysis
        """
        # Parse CDA definitions
        cdas = [CDADefinition.from_dict(cda) for cda in cda_list]

        # Extract all validated fields from the configuration
        validated_fields = self._extract_validated_fields(file_config)

        # Analyze coverage for each CDA
        field_coverage = []

        for cda in cdas:
            coverage = self._check_field_coverage(cda, validated_fields)
            field_coverage.append(coverage)

        # Calculate totals
        total_cdas = len(cdas)
        covered_cdas = sum(1 for fc in field_coverage if fc.is_covered)
        gap_cdas = total_cdas - covered_cdas

        return CDAGapResult(
            file_name=file_name,
            total_cdas=total_cdas,
            covered_cdas=covered_cdas,
            gap_cdas=gap_cdas,
            field_coverage=field_coverage,
            analysis_timestamp=datetime.now()
        )

    def _extract_validated_fields(self, file_config: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Extract all fields that have validation coverage.

        Args:
            file_config: File configuration with validations

        Returns:
            Dict mapping field names to list of validation types covering them
        """
        validated_fields: Dict[str, List[str]] = {}
        validations = file_config.get('validations', [])

        for validation in validations:
            val_type = validation.get('type', '')
            params = validation.get('params', {})

            # Extract fields from field-level validations
            if val_type in self.FIELD_VALIDATION_TYPES:
                field_param = self.FIELD_VALIDATION_TYPES[val_type]
                fields = self._get_fields_from_params(params, field_param)

                for field in fields:
                    if field not in validated_fields:
                        validated_fields[field] = []
                    validated_fields[field].append(val_type)

            # Extract fields from schema validations
            if val_type in self.SCHEMA_VALIDATION_TYPES:
                schema_param = self.SCHEMA_VALIDATION_TYPES[val_type]
                schema_value = params.get(schema_param, {})

                if isinstance(schema_value, dict):
                    # SchemaMatchCheck with expected_schema dict
                    fields = list(schema_value.keys())
                elif isinstance(schema_value, list):
                    # ColumnPresenceCheck with required_columns list
                    fields = schema_value
                else:
                    fields = []

                for field in fields:
                    if field not in validated_fields:
                        validated_fields[field] = []
                    validated_fields[field].append(val_type)

        return validated_fields

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
            # Single parameter name
            value = params.get(field_param)
            if isinstance(value, str):
                fields.append(value)
            elif isinstance(value, list):
                fields.extend(value)
        elif isinstance(field_param, list):
            # Multiple parameter names (e.g., field1, field2)
            for param_name in field_param:
                value = params.get(param_name)
                if isinstance(value, str):
                    fields.append(value)

        return fields

    def _check_field_coverage(
        self,
        cda: CDADefinition,
        validated_fields: Dict[str, List[str]]
    ) -> CDAFieldCoverage:
        """
        Check validation coverage for a specific CDA field.

        Args:
            cda: CDA definition to check
            validated_fields: Dict of validated fields and their validation types

        Returns:
            CDAFieldCoverage with coverage details
        """
        covering_validations = validated_fields.get(cda.field, [])
        is_covered = len(covering_validations) > 0

        # Build coverage details
        coverage_details = []
        for val_type in covering_validations:
            coverage_details.append(f"Covered by {val_type}")

        return CDAFieldCoverage(
            cda=cda,
            is_covered=is_covered,
            covering_validations=covering_validations,
            coverage_details=coverage_details
        )

    def get_recommendations(self, result: CDAGapResult) -> List[Dict[str, Any]]:
        """
        Generate validation recommendations for uncovered CDAs.

        Args:
            result: Gap analysis result for a file

        Returns:
            List of recommended validations to add
        """
        recommendations = []

        for fc in result.gaps:
            cda = fc.cda
            rec = {
                'field': cda.field,
                'description': cda.description,
                'suggested_validations': self._suggest_validations(cda)
            }
            recommendations.append(rec)

        return recommendations

    def _suggest_validations(self, cda: CDADefinition) -> List[str]:
        """
        Suggest appropriate validations for an uncovered CDA.

        Args:
            cda: CDA definition needing coverage

        Returns:
            List of suggested validation types
        """
        # Recommend basic validations for critical fields
        suggestions = ['MandatoryFieldCheck', 'ValidValuesCheck']

        return suggestions
