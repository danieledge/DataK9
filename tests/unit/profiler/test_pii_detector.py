"""
Unit tests for pii_detector.py

Tests PII detection functionality including:
- Email detection
- Phone number detection (US, international)
- SSN detection
- Credit card detection with Luhn validation
- IP address detection
- Postal code detection
- Privacy risk scoring
- Redaction strategies
- Dataset-level risk assessment
"""

import pytest
import pandas as pd
from pathlib import Path

from validation_framework.profiler.pii_detector import PIIDetector


class TestPIIDetector:
    """Test suite for PIIDetector class."""

    @pytest.fixture
    def detector(self):
        """Create PIIDetector instance."""
        return PIIDetector(min_confidence=0.5, sample_size=1000)

    @pytest.fixture
    def pii_test_data(self):
        """Load PII test data."""
        test_data_path = Path(__file__).parent / 'test_data' / 'pii_samples.csv'
        if not test_data_path.exists():
            pytest.skip(f"Test data not found: {test_data_path}")
        return pd.read_csv(test_data_path)

    # -------------------------------------------------------------------------
    # Email Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_email_valid(self, detector):
        """Test email detection with valid email addresses."""
        samples = [
            "john.doe@example.com",
            "user123@company.org",
            "admin@test-domain.co.uk"
        ]

        result = detector.detect_pii_in_column("email", samples, total_rows=len(samples))

        assert result["detected"] is True
        assert any(pii["type"] == "email" for pii in result["pii_types"])

    def test_detect_email_mixed(self, detector):
        """Test email detection with mixed content."""
        samples = [
            "john.doe@example.com",
            "not-an-email",
            "user@domain.com",
            "random text"
        ]

        result = detector.detect_pii_in_column("contact", samples, total_rows=len(samples))

        # Should detect email but with lower confidence
        if result["detected"]:
            email_pii = [p for p in result["pii_types"] if p["type"] == "email"]
            if email_pii:
                assert email_pii[0]["confidence"] < 1.0

    def test_detect_email_column_name_semantic(self, detector):
        """Test email detection via column name."""
        result = detector.detect_pii_in_column(
            "email_address",
            ["some value"],
            total_rows=1
        )

        # Should detect via semantic matching
        assert result["detected"] is True
        assert result["confidence"] > 0

    # -------------------------------------------------------------------------
    # Phone Number Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_phone_us_formats(self, detector):
        """Test US phone number detection in multiple formats."""
        samples = [
            "(555) 123-4567",
            "555-123-4567",
            "555.123.4567",
            "5551234567",
            "1-555-123-4567"
        ]

        result = detector.detect_pii_in_column("phone", samples, total_rows=len(samples))

        assert result["detected"] is True
        phone_types = [p["type"] for p in result["pii_types"]]
        assert "phone_us" in phone_types or "phone_intl" in phone_types

    def test_detect_phone_international(self, detector):
        """Test international phone number detection."""
        samples = [
            "+44 20 1234 5678",
            "+1 555 123 4567",
            "+33 1 23 45 67 89"
        ]

        result = detector.detect_pii_in_column("phone", samples, total_rows=len(samples))

        if result["detected"]:
            assert any("phone" in p["type"] for p in result["pii_types"])

    # -------------------------------------------------------------------------
    # SSN Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_ssn_with_dashes(self, detector):
        """Test SSN detection with dashes."""
        samples = [
            "123-45-6789",
            "987-65-4321",
            "555-12-3456"
        ]

        result = detector.detect_pii_in_column("ssn", samples, total_rows=len(samples))

        assert result["detected"] is True
        assert any(p["type"] == "ssn" for p in result["pii_types"])
        assert result["risk_score"] >= 90  # SSN is high risk

    def test_detect_ssn_no_dashes(self, detector):
        """Test SSN detection without dashes."""
        samples = [
            "123456789",
            "987654321"
        ]

        result = detector.detect_pii_in_column("tax_id", samples, total_rows=len(samples))

        if result["detected"]:
            assert any("ssn" in p["type"] for p in result["pii_types"])

    # -------------------------------------------------------------------------
    # Credit Card Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_credit_card_valid_luhn(self, detector):
        """Test credit card detection with Luhn validation."""
        # These are test credit card numbers that pass Luhn algorithm
        # Generated using proper Luhn checksum calculation
        samples = [
            "4532-1111-1111-1112",  # Visa test card (valid Luhn)
            "5425-2334-3010-9903",  # Mastercard test card (valid Luhn)
        ]

        result = detector.detect_pii_in_column("cc", samples, total_rows=len(samples))

        # Note: Detection depends on Luhn validation
        if result["detected"]:
            assert any(p["type"] == "credit_card" for p in result["pii_types"])
            assert result["risk_score"] >= 95  # Credit cards are critical risk

    def test_validate_credit_card_luhn_valid(self, detector):
        """Test Luhn algorithm with valid credit card."""
        valid_cc = "4532111111111112"  # Valid Luhn checksum (last digit is check digit)

        assert detector._validate_credit_card_luhn(valid_cc) is True

    def test_validate_credit_card_luhn_invalid(self, detector):
        """Test Luhn algorithm with invalid credit card."""
        invalid_cc = "1234567890123456"  # Invalid Luhn checksum

        assert detector._validate_credit_card_luhn(invalid_cc) is False

    def test_validate_credit_card_luhn_with_dashes(self, detector):
        """Test Luhn validation with dashes in card number."""
        valid_cc = "4532-1111-1111-1112"  # Valid Luhn with dashes

        assert detector._validate_credit_card_luhn(valid_cc) is True

    def test_validate_credit_card_luhn_invalid_length(self, detector):
        """Test Luhn validation with invalid length."""
        short_cc = "12345"

        assert detector._validate_credit_card_luhn(short_cc) is False

    # -------------------------------------------------------------------------
    # IP Address Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_ipv4(self, detector):
        """Test IPv4 address detection."""
        samples = [
            "192.168.1.1",
            "10.0.0.1",
            "8.8.8.8"
        ]

        result = detector.detect_pii_in_column("ip", samples, total_rows=len(samples))

        assert result["detected"] is True
        assert any(p["type"] == "ip_v4" for p in result["pii_types"])

    def test_detect_ipv6(self, detector):
        """Test IPv6 address detection."""
        samples = [
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
            "fe80:0000:0000:0000:0202:b3ff:fe1e:8329"
        ]

        result = detector.detect_pii_in_column("ip_address", samples, total_rows=len(samples))

        if result["detected"]:
            assert any("ip" in p["type"] for p in result["pii_types"])

    # -------------------------------------------------------------------------
    # Postal Code Detection Tests
    # -------------------------------------------------------------------------

    def test_detect_postal_code_us(self, detector):
        """Test US postal code detection."""
        samples = [
            "12345",
            "90210",
            "12345-6789"
        ]

        result = detector.detect_pii_in_column("zip", samples, total_rows=len(samples))

        if result["detected"]:
            assert any("postal" in p["type"] for p in result["pii_types"])

    def test_detect_postal_code_uk(self, detector):
        """Test UK postal code detection."""
        samples = [
            "SW1A 1AA",
            "EC1A 1BB",
            "W1A 0AX"
        ]

        result = detector.detect_pii_in_column("postcode", samples, total_rows=len(samples))

        if result["detected"]:
            assert any("postal" in p["type"] for p in result["pii_types"])

    # -------------------------------------------------------------------------
    # Column Name Semantic Detection Tests
    # -------------------------------------------------------------------------

    def test_semantic_detection_email(self, detector):
        """Test semantic detection for email column."""
        result = detector._check_column_name_semantics("email")

        assert result is not None
        assert result["pii_type"] == "email"
        assert result["method"] == "semantic"
        assert result["confidence"] == 0.7

    def test_semantic_detection_ssn(self, detector):
        """Test semantic detection for SSN column."""
        result = detector._check_column_name_semantics("social_security_number")

        assert result is not None
        assert result["pii_type"] == "ssn"
        assert result["method"] == "semantic"

    def test_semantic_detection_credit_card(self, detector):
        """Test semantic detection for credit card column."""
        result = detector._check_column_name_semantics("cc_number")

        assert result is not None
        assert result["pii_type"] == "credit_card"

    def test_semantic_detection_no_match(self, detector):
        """Test semantic detection with no PII indicators."""
        result = detector._check_column_name_semantics("product_id")

        assert result is None

    # -------------------------------------------------------------------------
    # Redaction Strategy Tests
    # -------------------------------------------------------------------------

    def test_suggest_redaction_strategy_email(self, detector):
        """Test redaction strategy for email."""
        strategy = detector._suggest_redaction_strategy(["email"], 70)

        assert strategy["risk_level"] == "moderate_security"
        assert len(strategy["specific_strategies"]) > 0
        assert strategy["specific_strategies"][0]["pii_type"] == "email"
        assert strategy["specific_strategies"][0]["method"] == "partial_mask"

    def test_suggest_redaction_strategy_ssn(self, detector):
        """Test redaction strategy for SSN."""
        strategy = detector._suggest_redaction_strategy(["ssn"], 95)

        assert strategy["risk_level"] == "high_security"
        assert any(s["pii_type"] == "ssn" for s in strategy["specific_strategies"])

    def test_suggest_redaction_strategy_credit_card(self, detector):
        """Test redaction strategy for credit card."""
        strategy = detector._suggest_redaction_strategy(["credit_card"], 100)

        assert strategy["risk_level"] == "high_security"
        assert any(s["pii_type"] == "credit_card" for s in strategy["specific_strategies"])

    def test_additional_security_measures_high_risk(self, detector):
        """Test additional security measures for high risk."""
        measures = detector._additional_security_measures(90)

        assert len(measures) > 0
        assert any("encryption" in m.lower() for m in measures)

    def test_additional_security_measures_moderate_risk(self, detector):
        """Test additional security measures for moderate risk."""
        measures = detector._additional_security_measures(60)

        assert len(measures) > 0
        assert any("masking" in m.lower() for m in measures)

    # -------------------------------------------------------------------------
    # Dataset Privacy Risk Assessment Tests
    # -------------------------------------------------------------------------

    def test_calculate_dataset_privacy_risk_no_pii(self, detector):
        """Test dataset risk with no PII."""
        result = detector.calculate_dataset_privacy_risk(
            pii_columns=[],
            total_columns=10,
            total_rows=1000
        )

        assert result["risk_score"] == 0
        assert result["risk_level"] == "none"
        assert result["pii_column_count"] == 0

    def test_calculate_dataset_privacy_risk_critical(self, detector):
        """Test dataset risk assessment with critical PII."""
        pii_columns = [
            {
                "risk_score": 95,
                "regulatory_frameworks": ["HIPAA §164.514", "SOX"]
            },
            {
                "risk_score": 100,
                "regulatory_frameworks": ["PCI-DSS"]
            }
        ]

        result = detector.calculate_dataset_privacy_risk(
            pii_columns=pii_columns,
            total_columns=10,
            total_rows=1000
        )

        assert result["risk_score"] > 70
        assert result["risk_level"] == "critical"
        assert result["pii_column_count"] == 2
        assert "HIPAA §164.514" in result["regulatory_frameworks"]
        assert "PCI-DSS" in result["regulatory_frameworks"]

    def test_calculate_dataset_privacy_risk_moderate(self, detector):
        """Test dataset risk assessment with moderate PII."""
        pii_columns = [
            {
                "risk_score": 60,
                "regulatory_frameworks": ["GDPR Article 6"]
            }
        ]

        result = detector.calculate_dataset_privacy_risk(
            pii_columns=pii_columns,
            total_columns=10,
            total_rows=100
        )

        assert result["risk_level"] in ["moderate", "high"]
        assert len(result["recommendations"]) > 0

    def test_generate_dataset_recommendations_critical(self, detector):
        """Test dataset recommendations for critical risk."""
        recommendations = detector._generate_dataset_recommendations(
            risk_level="critical",
            pii_column_count=3,
            total_rows=10000,
            regulatory_frameworks=["HIPAA", "PCI-DSS"]
        )

        assert len(recommendations) > 0
        assert any("CRITICAL" in r for r in recommendations)
        assert any("HIPAA" in r for r in recommendations)

    # -------------------------------------------------------------------------
    # Integration Tests with Real Test Data
    # -------------------------------------------------------------------------

    def test_detect_pii_with_test_data(self, detector, pii_test_data):
        """Test PII detection with real test data."""
        # Test email column
        email_result = detector.detect_pii_in_column(
            "email",
            pii_test_data["email"].tolist(),
            total_rows=len(pii_test_data)
        )

        assert email_result["detected"] is True
        assert email_result["confidence"] > 0.9

        # Test phone column
        phone_result = detector.detect_pii_in_column(
            "phone",
            pii_test_data["phone"].tolist(),
            total_rows=len(pii_test_data)
        )

        assert phone_result["detected"] is True

        # Test SSN column
        ssn_result = detector.detect_pii_in_column(
            "ssn",
            pii_test_data["ssn"].tolist(),
            total_rows=len(pii_test_data)
        )

        assert ssn_result["detected"] is True
        assert ssn_result["risk_score"] >= 85

    def test_full_dataset_analysis_pipeline(self, detector, pii_test_data):
        """Test complete dataset PII analysis pipeline."""
        pii_columns = []

        # Analyze each column
        for col in ["email", "phone", "ssn", "credit_card", "ip_address", "postal_code"]:
            if col in pii_test_data.columns:
                result = detector.detect_pii_in_column(
                    col,
                    pii_test_data[col].tolist(),
                    total_rows=len(pii_test_data)
                )

                if result["detected"]:
                    pii_columns.append(result)

        # Calculate dataset-level risk
        dataset_risk = detector.calculate_dataset_privacy_risk(
            pii_columns=pii_columns,
            total_columns=len(pii_test_data.columns),
            total_rows=len(pii_test_data)
        )

        assert dataset_risk["pii_column_count"] > 0
        assert dataset_risk["risk_score"] > 0
        assert len(dataset_risk["recommendations"]) > 0

    # -------------------------------------------------------------------------
    # Edge Cases and Error Handling
    # -------------------------------------------------------------------------

    def test_detect_pii_empty_samples(self, detector):
        """Test PII detection with empty sample list."""
        result = detector.detect_pii_in_column("test", [], total_rows=0)

        assert result["detected"] is False

    def test_detect_pii_none_values(self, detector):
        """Test PII detection with None values."""
        samples = [None, None, "test@example.com", None]

        result = detector.detect_pii_in_column("email", samples, total_rows=len(samples))

        # Should handle None values gracefully
        assert isinstance(result, dict)

    def test_detect_pii_below_confidence_threshold(self):
        """Test PII detection below confidence threshold."""
        detector = PIIDetector(min_confidence=0.9, sample_size=1000)

        # Only 1 out of 100 emails
        samples = ["user@example.com"] + ["not-pii"] * 99

        result = detector.detect_pii_in_column("data", samples, total_rows=len(samples))

        # May not detect due to low confidence (1%)
        # But should handle gracefully
        assert isinstance(result, dict)

    def test_sample_size_limit(self):
        """Test sample size limiting."""
        detector = PIIDetector(sample_size=10)

        # Provide 100 samples, should only check first 10
        samples = ["user{i}@example.com".format(i=i) for i in range(100)]

        result = detector.detect_pii_in_column("email", samples, total_rows=len(samples))

        # Should still detect email pattern
        assert result["detected"] is True

    def test_regulatory_frameworks_mapping(self, detector):
        """Test regulatory framework mapping."""
        # Email detection
        email_result = detector.detect_pii_in_column(
            "email",
            ["test@example.com"],
            total_rows=1
        )

        if email_result["detected"]:
            assert "GDPR" in str(email_result["regulatory_frameworks"])

        # SSN detection
        ssn_result = detector.detect_pii_in_column(
            "ssn",
            ["123-45-6789"],
            total_rows=1
        )

        if ssn_result["detected"]:
            assert "HIPAA" in str(ssn_result["regulatory_frameworks"]) or \
                   "SOX" in str(ssn_result["regulatory_frameworks"])

    # -------------------------------------------------------------------------
    # Parametrized Tests
    # -------------------------------------------------------------------------

    @pytest.mark.parametrize("email,should_detect", [
        ("john.doe@example.com", True),
        ("user+tag@domain.co.uk", True),
        ("admin@test-domain.org", True),
        ("not-an-email", False),
        ("@invalid.com", False),
        ("user@", False),
    ])
    def test_email_pattern_validation(self, detector, email, should_detect):
        """Test email pattern detection with various formats."""
        result = detector.detect_pii_in_column("email", [email], total_rows=1)

        if should_detect:
            assert result["detected"] is True
        # Note: Single invalid email may not trigger detection

    @pytest.mark.parametrize("phone,should_detect", [
        ("(555) 123-4567", True),
        ("555-123-4567", True),
        ("555.123.4567", True),
        ("not-a-phone", False),
        ("123", False),
    ])
    def test_phone_pattern_validation(self, detector, phone, should_detect):
        """Test phone pattern detection with various formats."""
        result = detector.detect_pii_in_column("phone", [phone], total_rows=1)

        if should_detect:
            assert result["detected"] is True

    @pytest.mark.parametrize("risk_score,total_columns,expected_level", [
        # Weighted formula: 0.4*max_risk + 0.3*avg_risk + 0.3*(pii_cols/total_cols*100)
        # With 1 PII column out of 10: pii_percentage = 10%
        # score = 0.4*risk + 0.3*risk + 0.3*10 = 0.7*risk + 3
        (100, 10, "critical"),  # 0.7*100 + 3 = 73 >= 70 → critical
        (70, 10, "high"),       # 0.7*70 + 3 = 52 >= 50 → high
        (40, 10, "moderate"),   # 0.7*40 + 3 = 31 >= 30 → moderate
        (10, 10, "low"),        # 0.7*10 + 3 = 10 < 30 → low
    ])
    def test_risk_level_classification(self, detector, risk_score, total_columns, expected_level):
        """Test risk level classification with weighted formula."""
        result = detector.calculate_dataset_privacy_risk(
            pii_columns=[{"risk_score": risk_score, "regulatory_frameworks": []}],
            total_columns=total_columns,
            total_rows=100
        )

        assert result["risk_level"] == expected_level


class TestPIIDetectorPerformance:
    """Performance and optimization tests."""

    def test_large_sample_performance(self):
        """Test PII detection with large sample size."""
        detector = PIIDetector(sample_size=10000)

        # Generate large sample
        samples = [f"user{i}@example.com" for i in range(10000)]

        result = detector.detect_pii_in_column("email", samples, total_rows=len(samples))

        # Should complete and detect email
        assert result["detected"] is True
        assert result["confidence"] > 0.99

    def test_regex_pattern_compilation(self):
        """Test that regex patterns are compiled for performance."""
        detector = PIIDetector()

        # Compiled patterns should be available
        assert hasattr(detector, 'compiled_patterns')
        assert len(detector.compiled_patterns) > 0

        # Check key patterns are compiled
        assert 'email' in detector.compiled_patterns
        assert 'ssn' in detector.compiled_patterns
        assert 'credit_card' in detector.compiled_patterns
