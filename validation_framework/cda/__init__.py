"""
Critical Data Attribute (CDA) Analysis Module

This module provides functionality for tracking and analyzing Critical Data Attributes
to ensure validation coverage.

Key Components:
- CDADefinition: Data class representing a critical field definition
- CDAGapAnalyzer: Engine for detecting validation coverage gaps
- CDAReporter: HTML report generator for gap analysis results
"""

from .models import CDADefinition, CDAGapResult, CDAFieldCoverage
from .analyzer import CDAGapAnalyzer
from .reporter import CDAReporter

__all__ = [
    'CDADefinition',
    'CDAGapResult',
    'CDAFieldCoverage',
    'CDAGapAnalyzer',
    'CDAReporter',
]
