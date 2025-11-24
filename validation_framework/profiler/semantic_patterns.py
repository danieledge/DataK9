"""
Semantic Pattern Detection for DataK9 Profiler

Lightweight pattern detection for common data types without external dependencies.
Detects emails, phones, URLs, credit cards, dates, and other semantic patterns.
"""

import re
from typing import List, Dict, Any, Set
from dataclasses import dataclass


@dataclass
class PatternMatch:
    """Result of pattern matching"""
    pattern_type: str
    confidence: float  # 0.0 to 1.0
    match_count: int
    total_sampled: int
    examples: List[str]


class SemanticPatternDetector:
    """
    Detects semantic patterns in data without external dependencies.

    Based on common regex patterns but implemented using standard Python re module.
    Optimized for performance with compiled patterns.
    """

    # Compiled regex patterns for performance
    PATTERNS = {
        'email': re.compile(
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            re.IGNORECASE
        ),
        'phone': re.compile(
            # Require phone-specific formatting (parentheses, dashes, dots, or +)
            # Avoid matching plain digit sequences that could be IDs/accounts
            r'(?:'
            r'(?:\+\d{1,3}[-.\s]?)?\(?\d{3}\)[-.\s]\d{3}[-.\s]\d{4}|'  # (123) 456-7890 or +1 (123) 456-7890
            r'(?:\+\d{1,3}[-.\s]?)??\d{3}[-. ]\d{3}[-. ]\d{4}|'  # 123-456-7890 or 123.456.7890
            r'\+\d{1,3}\s?\d{1,14}'  # International format with +
            r')'
            r'(?:\s?(?:ext|x)\s?\d{2,5})?',  # Optional extension
            re.IGNORECASE
        ),
        'url': re.compile(
            r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
            re.IGNORECASE
        ),
        'ipv4': re.compile(
            r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}'
            r'(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b'
        ),
        'credit_card': re.compile(
            r'\b(?:4[0-9]{12}(?:[0-9]{3})?|'  # Visa
            r'5[1-5][0-9]{14}|'  # MasterCard
            r'3[47][0-9]{13}|'  # American Express
            r'3(?:0[0-5]|[68][0-9])[0-9]{11}|'  # Diners Club
            r'6(?:011|5[0-9]{2})[0-9]{12}|'  # Discover
            r'(?:2131|1800|35\d{3})\d{11})\b'  # JCB
        ),
        'ssn': re.compile(
            r'\b\d{3}-\d{2}-\d{4}\b'
        ),
        'zip_code': re.compile(
            r'\b\d{5}(?:-\d{4})?\b'
        ),
        'price': re.compile(
            r'\$\s*\d+(?:,\d{3})*(?:\.\d{2})?'
        ),
        'date_iso': re.compile(
            r'\b\d{4}-\d{2}-\d{2}\b'
        ),
        'time': re.compile(
            r'\b\d{1,2}:\d{2}(?::\d{2})?(?:\s?[AP]M)?\b',
            re.IGNORECASE
        )
    }

    @classmethod
    def detect_patterns(
        cls,
        sample_values: List[Any],
        min_confidence: float = 0.50,
        column_name: str = None
    ) -> Dict[str, PatternMatch]:
        """
        Detect semantic patterns in sample data.

        Args:
            sample_values: List of values to analyze
            min_confidence: Minimum match ratio to report (default 50%)
            column_name: Column name for context-based filtering (optional)

        Returns:
            Dictionary mapping pattern types to PatternMatch results
        """
        if not sample_values:
            return {}

        # Convert all values to strings
        str_values = [str(v) for v in sample_values if v is not None]
        if not str_values:
            return {}

        total_sampled = len(str_values)
        results = {}

        # Apply column name-based heuristics to avoid false positives
        column_lower = column_name.lower() if column_name else ''
        skip_patterns = set()

        # Skip phone detection for account/ID/transaction fields
        if any(term in column_lower for term in ['account', 'id', 'transaction', 'reference', 'number', 'code']):
            skip_patterns.add('phone')

        # Skip credit card detection for account/ID/bank fields
        if any(term in column_lower for term in ['account', 'id', 'bank', 'transaction', 'reference', 'routing']):
            skip_patterns.add('credit_card')

        for pattern_name, pattern in cls.PATTERNS.items():
            # Skip patterns based on column name heuristics
            if pattern_name in skip_patterns:
                continue

            matches: Set[str] = set()
            match_count = 0

            for value in str_values:
                found = pattern.search(value)
                if found:
                    match_count += 1
                    if len(matches) < 5:  # Keep up to 5 examples
                        matches.add(found.group())

            if match_count > 0:
                confidence = match_count / total_sampled
                if confidence >= min_confidence:
                    results[pattern_name] = PatternMatch(
                        pattern_type=pattern_name,
                        confidence=confidence,
                        match_count=match_count,
                        total_sampled=total_sampled,
                        examples=list(matches)[:5]
                    )

        return results

    @classmethod
    def get_primary_pattern(
        cls,
        sample_values: List[Any],
        min_confidence: float = 0.50
    ) -> str:
        """
        Get the most likely semantic pattern for the data.

        Args:
            sample_values: List of values to analyze
            min_confidence: Minimum confidence to return a pattern (default 50%)

        Returns:
            Pattern type name or 'unknown' if no confident match
        """
        patterns = cls.detect_patterns(sample_values, min_confidence=min_confidence)

        if not patterns:
            return 'unknown'

        # Return pattern with highest confidence
        best = max(patterns.values(), key=lambda p: p.confidence)
        return best.pattern_type if best.confidence >= min_confidence else 'unknown'

    @classmethod
    def suggest_validation(cls, pattern_type: str) -> Dict[str, Any]:
        """
        Suggest appropriate validation based on detected pattern.

        Args:
            pattern_type: The detected pattern type

        Returns:
            Dictionary with validation suggestion details
        """
        suggestions = {
            'email': {
                'validation_type': 'RegexCheck',
                'params': {
                    'pattern': r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$',
                    'pattern_name': 'Valid Email Format'
                },
                'severity': 'ERROR',
                'reason': 'Email addresses detected - validate format'
            },
            'phone': {
                'validation_type': 'RegexCheck',
                'params': {
                    'pattern': r'^(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}$',
                    'pattern_name': 'Valid Phone Number'
                },
                'severity': 'WARNING',
                'reason': 'Phone numbers detected - validate format'
            },
            'url': {
                'validation_type': 'RegexCheck',
                'params': {
                    'pattern': r'^https?://.+',
                    'pattern_name': 'Valid URL'
                },
                'severity': 'WARNING',
                'reason': 'URLs detected - validate format'
            },
            'credit_card': {
                'validation_type': 'RegexCheck',
                'params': {
                    'pattern': r'^\d{13,19}$',
                    'pattern_name': 'Valid Credit Card Number'
                },
                'severity': 'ERROR',
                'reason': 'Credit card numbers detected - validate and ensure PCI compliance'
            },
            'ssn': {
                'validation_type': 'RegexCheck',
                'params': {
                    'pattern': r'^\d{3}-\d{2}-\d{4}$',
                    'pattern_name': 'Valid SSN Format'
                },
                'severity': 'ERROR',
                'reason': 'SSN detected - validate format and ensure data security'
            },
            'ipv4': {
                'validation_type': 'RegexCheck',
                'params': {
                    'pattern': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
                    'pattern_name': 'Valid IPv4 Address'
                },
                'severity': 'WARNING',
                'reason': 'IP addresses detected - validate format'
            }
        }

        return suggestions.get(pattern_type, {})
