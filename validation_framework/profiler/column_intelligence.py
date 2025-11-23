"""
Smart column analysis for intelligent profiling.

Detects column semantics from names and adjusts sampling strategies accordingly.
"""

import re
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class ColumnIntelligence:
    """Intelligence about a column based on name and patterns."""

    semantic_type: Optional[str] = None  # id, date, timestamp, amount, email, etc.
    recommended_sample_size: int = 10000  # Default sample size
    needs_full_scan: bool = False  # Whether this needs full data scan
    confidence: float = 0.0  # Confidence in semantic type detection (0-1)
    reasoning: str = ""  # Why we made this determination


class SmartColumnAnalyzer:
    """
    Analyzes column names and patterns to determine intelligent sampling strategies.

    Industry best practices for data profiling:
    - ID fields: Small sample sufficient (1K), only need uniqueness check
    - Dates/timestamps: Medium sample (5K), check for patterns and gaps
    - Amounts/prices: Medium sample (5K), need statistics
    - Free text: Small sample (2K), high cardinality expected
    - Categories/enums: Small sample (2K), low cardinality expected
    - Email/phone: Small sample (1K), pattern detection
    """

    # Pattern definitions for semantic type detection
    # Order matters! More specific patterns should come first
    PATTERNS = {
        'email': {
            'patterns': [
                r'(?i).*(email|e_mail|mail).*',
            ],
            'sample_size': 1000,
            'needs_full_scan': False,
            'reasoning': 'Email fields have predictable patterns; small sample sufficient'
        },
        'phone': {
            'patterns': [
                r'(?i).*(phone|tel|mobile|fax).*',
            ],
            'sample_size': 1000,
            'needs_full_scan': False,
            'reasoning': 'Phone fields have predictable patterns; small sample sufficient'
        },
        'id': {
            'patterns': [
                r'(?i)^(id|.*_id)$',
                r'(?i)^(uid|uuid|guid)$',
                r'(?i)^(key|.*_key)$',
                r'(?i)^(pk|primary_key)$',
            ],
            'sample_size': 1000,
            'needs_full_scan': False,
            'reasoning': 'ID fields typically have high uniqueness; small sample sufficient for pattern detection'
        },
        'date': {
            'patterns': [
                r'(?i).*(date|dt|day)$',
                r'(?i)^(created|updated|modified|deleted).*',
                r'(?i).*(timestamp|ts|time)$',
            ],
            'sample_size': 5000,
            'needs_full_scan': False,
            'reasoning': 'Date fields need medium sample for temporal pattern detection and gap analysis'
        },
        'amount': {
            'patterns': [
                r'(?i).*(amount|price|cost|value|total|sum).*',  # Match anywhere, not just end
                r'(?i).*(revenue|sales|fee|charge).*',
                r'(?i).*(balance|quantity|qty).*',
            ],
            'sample_size': 5000,
            'needs_full_scan': False,
            'reasoning': 'Numeric amounts need medium sample for distribution and outlier detection'
        },
        'category': {
            'patterns': [
                r'(?i).*(category|type|status|state|class).*',
                r'(?i)^(is[_\s]|has[_\s]|can[_\s]).*',  # Boolean flags with underscore or space
            ],
            'sample_size': 2000,
            'needs_full_scan': False,
            'reasoning': 'Categorical fields typically have low cardinality; small sample sufficient'
        },
        'text': {
            'patterns': [
                r'(?i).*(description|comment|note|text|message|content).*',
                r'(?i).*(title|name|label)$',
            ],
            'sample_size': 2000,
            'needs_full_scan': False,
            'reasoning': 'Free text fields have high cardinality; small sample sufficient for pattern detection'
        },
        'code': {
            'patterns': [
                r'(?i).*(code|cd)$',
                r'(?i)^(sku|upc|barcode).*',
            ],
            'sample_size': 2000,
            'needs_full_scan': False,
            'reasoning': 'Code fields typically follow patterns; small sample sufficient'
        }
    }

    @classmethod
    def analyze_column(cls, column_name: str) -> ColumnIntelligence:
        """
        Analyze a column name and return intelligence about optimal profiling strategy.

        Args:
            column_name: Name of the column

        Returns:
            ColumnIntelligence with recommended strategy
        """
        # Try to match against known patterns
        for semantic_type, config in cls.PATTERNS.items():
            for pattern in config['patterns']:
                if re.match(pattern, column_name):
                    return ColumnIntelligence(
                        semantic_type=semantic_type,
                        recommended_sample_size=config['sample_size'],
                        needs_full_scan=config['needs_full_scan'],
                        confidence=0.8,  # High confidence for name-based detection
                        reasoning=config['reasoning']
                    )

        # Default strategy for unknown columns
        return ColumnIntelligence(
            semantic_type='unknown',
            recommended_sample_size=10000,  # Conservative default
            needs_full_scan=False,
            confidence=0.0,
            reasoning='Column name does not match known patterns; using default sampling strategy'
        )

    @classmethod
    def get_sampling_summary(cls, column_name: str, total_rows: int, intelligence: ColumnIntelligence) -> str:
        """
        Generate a human-readable summary of the sampling strategy used.

        Args:
            column_name: Name of the column
            total_rows: Total rows in dataset
            intelligence: Column intelligence

        Returns:
            Human-readable sampling summary
        """
        sample_size = min(intelligence.recommended_sample_size, total_rows)
        sample_pct = (sample_size / total_rows * 100) if total_rows > 0 else 0

        if sample_size >= total_rows:
            return f"Analyzed all {total_rows:,} rows"

        semantic_info = f" ({intelligence.semantic_type} field)" if intelligence.semantic_type != 'unknown' else ""

        return f"Sampled {sample_size:,} of {total_rows:,} rows ({sample_pct:.1f}%){semantic_info}"
