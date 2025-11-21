"""
Critical Data Attribute (CDA) Analysis Module

This module provides functionality for tracking and analyzing Critical Data Attributes
to ensure validation coverage for regulatory, financial, and operational data fields.

Key Components:
- CDADefinition: Data class representing a critical field definition
- CDATier: Enum for classification tiers (TIER_1, TIER_2, TIER_3)
- CDAGapAnalyzer: Engine for detecting validation coverage gaps
- CDAReporter: HTML report generator for gap analysis results
"""

from .models import CDADefinition, CDATier, CDAGapResult, CDAFieldCoverage
from .analyzer import CDAGapAnalyzer
from .reporter import CDAReporter

__all__ = [
    'CDADefinition',
    'CDATier',
    'CDAGapResult',
    'CDAFieldCoverage',
    'CDAGapAnalyzer',
    'CDAReporter',
]
