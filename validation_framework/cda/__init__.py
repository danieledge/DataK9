"""
Critical Data Attribute (CDA) Analysis Module

This module provides functionality for tracking and analyzing Critical Data Attributes
to ensure validation coverage and SLA compliance.

Key Components:
- CDADefinition: Data class representing a critical field definition
- CDAGapAnalyzer: Engine for detecting validation coverage gaps
- CDAReporter: HTML report generator for gap analysis results
- SLAEvaluator: Evaluates SLA compliance per CDA field
- SLAReport: Complete SLA compliance report
"""

from .models import CDADefinition, CDAGapResult, CDAFieldCoverage
from .analyzer import CDAGapAnalyzer
from .reporter import CDAReporter
from .sla_models import (
    SLAStatus, SLADefinition, SLAResult, SLAReport,
    DEFAULT_TIERS, DEFAULT_WARNING_AT
)
from .sla_evaluator import SLAEvaluator, format_sla_cli_output

__all__ = [
    # CDA Gap Analysis
    'CDADefinition',
    'CDAGapResult',
    'CDAFieldCoverage',
    'CDAGapAnalyzer',
    'CDAReporter',
    # SLA Compliance
    'SLAStatus',
    'SLADefinition',
    'SLAResult',
    'SLAReport',
    'SLAEvaluator',
    'format_sla_cli_output',
    'DEFAULT_TIERS',
    'DEFAULT_WARNING_AT',
]
