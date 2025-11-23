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
"""

import re
from typing import Dict, List, Any, Optional
import logging


logger = logging.getLogger(__name__)


class PIIDetector:
    """
    Detect PII and sensitive data patterns in columns.

    Uses pattern-based (regex) and semantic (column name) detection
    to identify personally identifiable information.
    """

    # Regex patterns for common PII types
    PATTERNS = {
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
        'ssn_no_dash': {
            'regex': r'\b\d{9}\b',
            'name': 'SSN (No Dashes)',
            'risk_score': 85,
            'regulatory': ['HIPAA §164.514', 'SOX']
        },
        'phone_us': {
            'regex': r'\b(\d{3}[-.\s]?\d{3}[-.\s]?\d{4}|\(\d{3}\)\s*\d{3}[-.\s]?\d{4}|1[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4})\b',
            'name': 'US Phone Number',
            'risk_score': 60,
            'regulatory': ['GDPR Article 6', 'CCPA']
        },
        'phone_intl': {
            'regex': r'\+\d{1,3}\s?\d{1,14}',
            'name': 'International Phone Number',
            'risk_score': 60,
            'regulatory': ['GDPR Article 6']
        },
        'credit_card': {
            'regex': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
            'name': 'Credit Card Number',
            'risk_score': 100,
            'regulatory': ['PCI-DSS', 'SOX']
        },
        'ip_v4': {
            'regex': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
            'name': 'IPv4 Address',
            'risk_score': 40,
            'regulatory': ['GDPR Article 6', 'CCPA']
        },
        'ip_v6': {
            'regex': r'\b([0-9a-fA-F]{0,4}:){7}[0-9a-fA-F]{0,4}\b',
            'name': 'IPv6 Address',
            'risk_score': 40,
            'regulatory': ['GDPR Article 6']
        },
        'postal_code_us': {
            'regex': r'\b\d{5}(-\d{4})?\b',
            'name': 'US Postal Code',
            'risk_score': 30,
            'regulatory': ['HIPAA §164.514(b)']
        },
        'postal_code_uk': {
            'regex': r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s?\d[A-Z]{2}\b',
            'name': 'UK Postal Code',
            'risk_score': 30,
            'regulatory': ['GDPR Article 6']
        },
        'postal_code_ca': {
            'regex': r'\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b',
            'name': 'Canadian Postal Code',
            'risk_score': 30,
            'regulatory': ['PIPEDA']
        },
        'date_of_birth': {
            'regex': r'\b(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/(19|20)\d{2}\b',
            'name': 'Date of Birth (MM/DD/YYYY)',
            'risk_score': 75,
            'regulatory': ['HIPAA §164.514', 'GDPR Article 9']
        }
    }

    # Semantic column name indicators
    COLUMN_NAME_INDICATORS = {
        'email': {
            'keywords': ['email', 'e_mail', 'mail', 'email_address', 'contact_email'],
            'risk_score': 70,
            'pii_type': 'email'
        },
        'ssn': {
            'keywords': ['ssn', 'social_security', 'social_security_number', 'tax_id', 'tin'],
            'risk_score': 95,
            'pii_type': 'ssn'
        },
        'phone': {
            'keywords': ['phone', 'telephone', 'mobile', 'cell', 'phone_number', 'tel', 'fax'],
            'risk_score': 60,
            'pii_type': 'phone_us'
        },
        'credit_card': {
            'keywords': ['cc', 'credit_card', 'card_number', 'pan', 'card_num', 'cc_number'],
            'risk_score': 100,
            'pii_type': 'credit_card'
        },
        'name': {
            'keywords': ['first_name', 'last_name', 'full_name', 'name', 'surname', 'given_name', 'family_name'],
            'risk_score': 50,
            'pii_type': 'name'
        },
        'address': {
            'keywords': ['address', 'street', 'street_address', 'address_line', 'city', 'state', 'zip', 'postal'],
            'risk_score': 60,
            'pii_type': 'address'
        },
        'dob': {
            'keywords': ['dob', 'date_of_birth', 'birthdate', 'birth_date', 'birthday'],
            'risk_score': 75,
            'pii_type': 'date_of_birth'
        },
        'account': {
            'keywords': ['account_number', 'account_num', 'acct_num', 'bank_account'],
            'risk_score': 85,
            'pii_type': 'account_number'
        },
        'license': {
            'keywords': ['license', 'licence', 'drivers_license', 'dl_number', 'license_number'],
            'risk_score': 80,
            'pii_type': 'license'
        },
        'passport': {
            'keywords': ['passport', 'passport_number', 'passport_num'],
            'risk_score': 90,
            'pii_type': 'passport'
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

        # Compile regex patterns for performance
        self.compiled_patterns = {
            pii_type: re.compile(info['regex'], re.IGNORECASE)
            for pii_type, info in self.PATTERNS.items()
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

        for category, info in self.COLUMN_NAME_INDICATORS.items():
            for keyword in info['keywords']:
                if keyword in column_name_lower:
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

            for value in samples:
                if pattern_re.search(value):
                    match_count += 1
                    if len(matched_values) < 5:  # Keep up to 5 examples
                        matched_values.append(value)

            if match_count > 0:
                # Calculate confidence based on match percentage
                confidence = match_count / len(samples)

                # Special validation for credit cards (Luhn algorithm)
                if pii_type == 'credit_card':
                    valid_cc_count = sum(
                        1 for v in matched_values
                        if self._validate_credit_card_luhn(v)
                    )
                    if valid_cc_count == 0:
                        # No valid credit cards found, reduce confidence
                        confidence *= 0.3

                # Only report if confidence meets threshold
                if confidence >= self.min_confidence:
                    pattern_info = self.PATTERNS[pii_type]

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
        if pii_type in self.PATTERNS:
            return self.PATTERNS[pii_type].get('regulatory', [])
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
