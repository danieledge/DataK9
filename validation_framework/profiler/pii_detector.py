"""
PII (Personally Identifiable Information) detection for data profiling.

Detects sensitive data patterns including:
- Email addresses
- Phone numbers (US, international)
- Social Security Numbers (SSN)
- Credit card numbers (with Luhn validation)
- IP addresses (IPv4, IPv6)
- Postal codes
- Names, addresses (semantic detection)

Provides privacy risk scoring and redaction strategy recommendations.

Pattern and indicator definitions are loaded from external JSON files
(reference_data/pii/) for maintainability and auditability.
"""

import re
from typing import Dict, List, Any, Optional
import logging

from validation_framework.reference_data import ReferenceDataLoader


logger = logging.getLogger(__name__)


class PIIDetector:
    """
    Detect PII and sensitive data patterns in columns.

    Uses pattern-based (regex) and semantic (column name) detection
    to identify personally identifiable information.

    Patterns and indicators are loaded from external JSON files via
    ReferenceDataLoader for maintainability and auditability.
    """

    # Default patterns (fallback if JSON not available)
    _DEFAULT_PATTERNS = {
        'email': {
            'regex': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'name': 'Email Address',
            'risk_score': 70,
            'regulatory': ['GDPR Article 6', 'CCPA', 'PIPEDA']
        },
        'ssn': {
            'regex': r'\b\d{3}-\d{2}-\d{4}\b',
            'name': 'Social Security Number',
            'risk_score': 95,
            'regulatory': ['HIPAA §164.514', 'SOX', 'PCI-DSS']
        },
        'credit_card': {
            'regex': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
            'name': 'Credit Card Number',
            'risk_score': 100,
            'regulatory': ['PCI-DSS', 'SOX']
        }
    }

    # Default column indicators (fallback if JSON not available)
    _DEFAULT_COLUMN_INDICATORS = {
        'email': {
            'keywords': ['email', 'e_mail', 'mail'],
            'risk_score': 70,
            'pii_type': 'email'
        },
        'ssn': {
            'keywords': ['ssn', 'social_security'],
            'risk_score': 95,
            'pii_type': 'ssn'
        }
    }

    def __init__(self, min_confidence: float = 0.5, sample_size: int = 1000):
        """
        Initialize PII detector.

        Args:
            min_confidence: Minimum confidence threshold (0.0-1.0) for PII detection
            sample_size: Maximum number of sample values to check per column
        """
        self.min_confidence = min_confidence
        self.sample_size = sample_size

        # Load patterns from external JSON via ReferenceDataLoader
        self.patterns = ReferenceDataLoader.get_pii_patterns()
        if not self.patterns:
            logger.warning("No PII patterns loaded from JSON, using defaults")
            self.patterns = self._DEFAULT_PATTERNS

        # Load column name indicators from external JSON
        self.column_indicators = ReferenceDataLoader.get_column_indicators()
        if not self.column_indicators:
            logger.warning("No column indicators loaded from JSON, using defaults")
            self.column_indicators = self._DEFAULT_COLUMN_INDICATORS

        logger.debug(
            f"PIIDetector initialized: {len(self.patterns)} patterns, "
            f"{len(self.column_indicators)} column indicators"
        )

        # Compile regex patterns for performance
        self.compiled_patterns = {
            pii_type: re.compile(info['regex'], re.IGNORECASE)
            for pii_type, info in self.patterns.items()
        }

    def detect_pii_in_column(
        self,
        column_name: str,
        sample_values: List[Any],
        total_rows: int
    ) -> Dict[str, Any]:
        """
        Detect PII in a column using pattern-based and semantic detection.

        Args:
            column_name: Column name
            sample_values: Sample values from the column
            total_rows: Total number of rows in the dataset

        Returns:
            Dict with PII detection results including:
                - detected: Whether PII was detected
                - pii_types: List of detected PII types
                - confidence: Confidence score (0.0-1.0)
                - risk_score: Privacy risk score (0-100)
                - redaction_strategy: Recommended redaction approach
                - regulatory_frameworks: Applicable regulatory frameworks
        """
        result = {
            "detected": False,
            "pii_types": [],
            "confidence": 0.0,
            "risk_score": 0,
            "regulatory_frameworks": [],
            "redaction_strategy": None
        }

        # Semantic detection (column name analysis)
        semantic_match = self._check_column_name_semantics(column_name)

        # Pattern-based detection (value analysis)
        pattern_matches = self._check_value_patterns(sample_values)

        # CRITICAL FIX: Prevent credit card detection when column semantics indicate non-card ID
        # Addresses ChatGPT review: "Account" fields should never be flagged as PCI-DSS credit cards
        if semantic_match and semantic_match.get("pii_type") in ["account_number", "customer_id", "user_id"]:
            # Remove any credit_card matches from pattern detection
            original_count = len(pattern_matches)
            pattern_matches = [
                m for m in pattern_matches
                if m.get("pii_type") != "credit_card"
            ]
            if len(pattern_matches) < original_count:
                logger.debug(
                    f"Credit card detection blocked for '{column_name}': "
                    f"Column semantically identified as {semantic_match.get('pii_type')}"
                )

        # Combine results
        all_matches = []

        if semantic_match:
            all_matches.append(semantic_match)

        all_matches.extend(pattern_matches)

        if not all_matches:
            return result

        # Aggregate results
        result["detected"] = True

        # Get unique PII types
        pii_types_detected = {}
        max_risk_score = 0
        all_regulatory = set()

        for match in all_matches:
            pii_type = match["pii_type"]
            if pii_type not in pii_types_detected or match["confidence"] > pii_types_detected[pii_type]["confidence"]:
                pii_types_detected[pii_type] = match

            max_risk_score = max(max_risk_score, match["risk_score"])
            all_regulatory.update(match.get("regulatory", []))

        result["pii_types"] = [
            {
                "type": pii_type,
                "name": info["name"],
                "confidence": info["confidence"],
                "detection_method": info["method"],
                "match_count": info.get("match_count", 0)
            }
            for pii_type, info in pii_types_detected.items()
        ]

        # Calculate overall confidence (highest individual confidence)
        result["confidence"] = max(match["confidence"] for match in all_matches)

        result["risk_score"] = max_risk_score
        result["regulatory_frameworks"] = sorted(list(all_regulatory))

        # Suggest redaction strategy
        result["redaction_strategy"] = self._suggest_redaction_strategy(
            list(pii_types_detected.keys()),
            max_risk_score
        )

        return result

    def _check_column_name_semantics(self, column_name: str) -> Optional[Dict[str, Any]]:
        """
        Check if column name indicates PII.

        Args:
            column_name: Column name to check

        Returns:
            Dict with match info if PII indicator found, None otherwise
        """
        column_name_lower = column_name.lower().strip()
        # Normalize separators to underscores for word boundary matching
        # e.g., "credit-card" -> "credit_card", "credit card" -> "credit_card"
        column_name_normalized = re.sub(r'[\s\-\.]+', '_', column_name_lower)
        # Split into individual words/tokens for exact matching
        column_tokens = set(column_name_normalized.split('_'))
        # Also include the full normalized name for multi-word keyword matching
        column_tokens.add(column_name_normalized)

        for category, info in self.column_indicators.items():
            for keyword in info['keywords']:
                # Use word boundary matching to avoid false positives like 'cc' in 'account'
                # Check if keyword matches a full token OR if keyword (with underscores)
                # appears as a complete substring bounded by underscores or string edges
                keyword_pattern = r'(?:^|_)' + re.escape(keyword) + r'(?:_|$)'
                if keyword in column_tokens or re.search(keyword_pattern, column_name_normalized):
                    return {
                        "pii_type": info['pii_type'],
                        "name": category.title(),
                        "confidence": 0.7,  # Moderate confidence from name alone
                        "risk_score": info['risk_score'],
                        "method": "semantic",
                        "regulatory": self._get_regulatory_frameworks(info['pii_type'])
                    }

        return None

    def _check_value_patterns(self, sample_values: List[Any]) -> List[Dict[str, Any]]:
        """
        Check sample values against PII patterns.

        Args:
            sample_values: Sample values to check

        Returns:
            List of pattern matches
        """
        matches = []

        # Limit sample size
        samples = [str(v) for v in sample_values[:self.sample_size] if v is not None]

        if not samples:
            return matches

        # Check each PII pattern
        for pii_type, pattern_re in self.compiled_patterns.items():
            match_count = 0
            matched_values = []
            all_matched_values = []  # Store all matches for Luhn validation

            for value in samples:
                if pattern_re.search(value):
                    match_count += 1
                    all_matched_values.append(value)
                    if len(matched_values) < 5:  # Keep up to 5 examples for display
                        matched_values.append(value)

            if match_count > 0:
                # Calculate confidence based on match percentage
                confidence = match_count / len(samples)

                # CRITICAL FIX: Postal code validation - prevent float values from matching
                # Values like 0.04781 contain "04781" which matches 5-digit pattern
                # Real postal codes are whole numbers, not decimal values
                if pii_type in ('postal_code_us', 'postal_code_uk', 'postal_code_ca'):
                    # Check if values contain decimal points - if so, NOT postal codes
                    contains_decimal = any(
                        '.' in str(v) or 'e' in str(v).lower() or 'E' in str(v)
                        for v in all_matched_values[:100]
                    )

                    if contains_decimal:
                        # Contains decimal points - these are float values, not postal codes
                        logger.debug(
                            f"Postal code detection rejected: values contain decimals "
                            f"(e.g., {all_matched_values[0] if all_matched_values else 'N/A'})"
                        )
                        continue

                # CRITICAL FIX: Enhanced credit card validation (Luhn algorithm on large sample)
                # Addresses ChatGPT review: Sample 100+ values, require 80%+ Luhn pass rate
                if pii_type == 'credit_card':
                    # First check: Do any values contain letters? If so, NOT credit cards
                    contains_letters = any(
                        any(c.isalpha() for c in str(v))
                        for v in all_matched_values[:100]  # Check first 100 matches
                    )

                    if contains_letters:
                        # Contains letters - definitely not credit cards, skip this detection
                        continue

                    # Second check: Luhn algorithm validation on larger sample
                    # Sample up to 100 matched values for Luhn validation
                    luhn_sample = all_matched_values[:100]
                    luhn_valid_count = sum(
                        1 for v in luhn_sample
                        if self._validate_credit_card_luhn(v)
                    )

                    # Calculate Luhn pass ratio
                    luhn_ratio = luhn_valid_count / len(luhn_sample) if luhn_sample else 0.0

                    # Require strong Luhn support: at least 80% of samples must pass
                    # This prevents random numeric IDs (~10% Luhn pass rate) from being flagged
                    if luhn_ratio < 0.80:
                        # Not a true credit card column - skip this detection
                        logger.debug(
                            f"Credit card detection rejected for pattern match: "
                            f"Luhn pass rate {luhn_ratio:.1%} below 80% threshold "
                            f"({luhn_valid_count}/{len(luhn_sample)} samples)"
                        )
                        continue

                # Only report if confidence meets threshold
                if confidence >= self.min_confidence:
                    pattern_info = self.patterns[pii_type]

                    matches.append({
                        "pii_type": pii_type,
                        "name": pattern_info['name'],
                        "confidence": min(confidence, 1.0),
                        "risk_score": pattern_info['risk_score'],
                        "method": "pattern",
                        "match_count": match_count,
                        "match_percentage": round(confidence * 100, 2),
                        "sample_matches": matched_values,
                        "regulatory": pattern_info.get('regulatory', [])
                    })

        return matches

    def _validate_credit_card_luhn(self, card_number: str) -> bool:
        """
        Validate credit card number using Luhn algorithm.

        Args:
            card_number: Credit card number (may contain spaces/dashes)

        Returns:
            True if valid credit card number, False otherwise
        """
        # Remove spaces and dashes
        card_number = re.sub(r'[\s-]', '', card_number)

        # Check if all digits
        if not card_number.isdigit():
            return False

        # Check length (13-19 digits)
        if len(card_number) < 13 or len(card_number) > 19:
            return False

        # Luhn algorithm
        digits = [int(d) for d in card_number]
        checksum = 0

        # Process from right to left
        for i in range(len(digits) - 2, -1, -1):
            digit = digits[i]

            # Double every second digit from right
            if (len(digits) - 1 - i) % 2 == 1:
                digit *= 2
                # Subtract 9 if doubled value is > 9
                if digit > 9:
                    digit -= 9

            checksum += digit

        # Add check digit
        checksum += digits[-1]

        # Valid if checksum is multiple of 10
        return checksum % 10 == 0

    def _get_regulatory_frameworks(self, pii_type: str) -> List[str]:
        """Get applicable regulatory frameworks for PII type."""
        if pii_type in self.patterns:
            return self.patterns[pii_type].get('regulatory', [])
        return []

    def _suggest_redaction_strategy(
        self,
        pii_types: List[str],
        risk_score: int
    ) -> Dict[str, Any]:
        """
        Suggest redaction/masking strategy based on PII types and risk.

        Args:
            pii_types: List of detected PII types
            risk_score: Maximum risk score

        Returns:
            Dict with redaction strategy recommendations
        """
        strategies = {
            'email': {
                'method': 'partial_mask',
                'description': 'Mask local part, keep domain (e.g., j***@example.com)',
                'regex': r'(.{1})[^@]*(@.*)',
                'replacement': r'\1***\2'
            },
            'ssn': {
                'method': 'full_mask',
                'description': 'Replace with placeholder (XXX-XX-XXXX)',
                'regex': r'\d{3}-\d{2}-\d{4}',
                'replacement': 'XXX-XX-XXXX'
            },
            'phone_us': {
                'method': 'partial_mask',
                'description': 'Mask middle digits (e.g., (555) ***-1234)',
                'regex': r'(\d{3})[-.\s]?(\d{3})[-.\s]?(\d{4})',
                'replacement': r'\1-***-\3'
            },
            'credit_card': {
                'method': 'full_mask',
                'description': 'Show last 4 digits only (e.g., **** **** **** 1234)',
                'regex': r'(\d{4})[\s-]?(\d{4})[\s-]?(\d{4})[\s-]?(\d{4})',
                'replacement': '**** **** **** \\4'
            },
            'ip_v4': {
                'method': 'partial_mask',
                'description': 'Mask last octet (e.g., 192.168.1.***)',
                'regex': r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.)\d{1,3}',
                'replacement': r'\1***'
            }
        }

        # Select strategy based on detected PII types
        recommended_strategies = []
        for pii_type in pii_types:
            if pii_type in strategies:
                recommended_strategies.append({
                    "pii_type": pii_type,
                    **strategies[pii_type]
                })

        # General recommendation
        if risk_score >= 80:
            recommendation = "high_security"
            description = "Full encryption or tokenization recommended for high-risk PII"
        elif risk_score >= 50:
            recommendation = "moderate_security"
            description = "Partial masking or hashing recommended"
        else:
            recommendation = "low_security"
            description = "Basic masking or aggregation may be sufficient"

        return {
            "risk_level": recommendation,
            "description": description,
            "specific_strategies": recommended_strategies,
            "additional_measures": self._additional_security_measures(risk_score)
        }

    def _additional_security_measures(self, risk_score: int) -> List[str]:
        """Suggest additional security measures based on risk score."""
        measures = []

        if risk_score >= 80:
            measures.extend([
                "Implement column-level encryption",
                "Restrict access using role-based access control (RBAC)",
                "Enable audit logging for all PII access",
                "Consider tokenization for production use",
                "Implement data retention policies"
            ])
        elif risk_score >= 50:
            measures.extend([
                "Implement data masking in non-production environments",
                "Enable access logging",
                "Consider hashing for analytics purposes",
                "Implement data classification labels"
            ])
        else:
            measures.extend([
                "Apply basic masking for display purposes",
                "Document PII handling procedures",
                "Implement data minimization practices"
            ])

        return measures

    def calculate_dataset_privacy_risk(
        self,
        pii_columns: List[Dict[str, Any]],
        total_columns: int,
        total_rows: int
    ) -> Dict[str, Any]:
        """
        Calculate overall privacy risk score for the dataset.

        Args:
            pii_columns: List of PII detection results for all columns
            total_columns: Total number of columns
            total_rows: Total number of rows

        Returns:
            Dict with dataset-level privacy risk assessment
        """
        if not pii_columns:
            return {
                "risk_score": 0,
                "risk_level": "none",
                "pii_column_count": 0,
                "pii_column_percentage": 0.0,
                "recommendations": ["No PII detected. Proceed with standard data governance practices."]
            }

        # Calculate dataset risk score
        max_column_risk = max(col["risk_score"] for col in pii_columns)
        avg_column_risk = sum(col["risk_score"] for col in pii_columns) / len(pii_columns)
        pii_percentage = len(pii_columns) / total_columns * 100

        # Weighted dataset risk (40% max risk, 30% avg risk, 30% percentage of PII columns)
        dataset_risk_score = (
            0.4 * max_column_risk +
            0.3 * avg_column_risk +
            0.3 * pii_percentage
        )

        # Classify risk level
        if dataset_risk_score >= 70:
            risk_level = "critical"
        elif dataset_risk_score >= 50:
            risk_level = "high"
        elif dataset_risk_score >= 30:
            risk_level = "moderate"
        else:
            risk_level = "low"

        # Aggregate regulatory frameworks
        all_regulatory = set()
        for col in pii_columns:
            all_regulatory.update(col.get("regulatory_frameworks", []))

        # Generate recommendations
        recommendations = self._generate_dataset_recommendations(
            risk_level,
            len(pii_columns),
            total_rows,
            list(all_regulatory)
        )

        return {
            "risk_score": round(dataset_risk_score, 2),
            "risk_level": risk_level,
            "pii_column_count": len(pii_columns),
            "pii_column_percentage": round(pii_percentage, 2),
            "max_column_risk": max_column_risk,
            "avg_column_risk": round(avg_column_risk, 2),
            "regulatory_frameworks": sorted(list(all_regulatory)),
            "recommendations": recommendations
        }

    def _generate_dataset_recommendations(
        self,
        risk_level: str,
        pii_column_count: int,
        total_rows: int,
        regulatory_frameworks: List[str]
    ) -> List[str]:
        """Generate dataset-level recommendations based on risk assessment."""
        recommendations = []

        if risk_level == "critical":
            recommendations.extend([
                f"⚠ CRITICAL: {pii_column_count} PII columns detected with high sensitivity",
                "Implement comprehensive data protection strategy immediately",
                "Conduct privacy impact assessment (PIA)",
                "Implement data encryption at rest and in transit",
                "Establish data access audit trail",
                "Consider data minimization and pseudonymization"
            ])
        elif risk_level == "high":
            recommendations.extend([
                f"⚠ HIGH RISK: {pii_column_count} PII columns detected",
                "Implement data masking in non-production environments",
                "Review and document data retention policies",
                "Establish role-based access controls",
                "Enable PII access logging"
            ])
        elif risk_level == "moderate":
            recommendations.extend([
                f"{pii_column_count} PII columns detected with moderate sensitivity",
                "Apply data masking for sensitive fields",
                "Document PII handling procedures",
                "Implement basic access controls"
            ])
        else:
            recommendations.extend([
                f"{pii_column_count} PII columns detected with low sensitivity",
                "Apply standard data governance practices",
                "Document data classification"
            ])

        # Add regulatory-specific recommendations
        if regulatory_frameworks:
            recommendations.append(
                f"Ensure compliance with: {', '.join(regulatory_frameworks)}"
            )

        # Add row count context
        if total_rows > 1_000_000:
            recommendations.append(
                f"Dataset contains {total_rows:,} rows - consider data sampling for development/testing"
            )

        return recommendations
