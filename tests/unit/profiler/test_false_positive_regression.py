"""
Regression tests for profiler false positive fixes.

This test suite prevents regression of critical false positive issues identified
during code review and user testing on sample_transactions.csv dataset.

Tests verify:
1. Account field is NOT flagged as credit card PII (was: false positive)
2. Timestamp field is NOT suggested as UniqueKeyCheck (was: false positive)
3. Is Laundering binary field does NOT trigger CV warnings (was: false positive)
4. From Bank/To Bank ID fields do NOT trigger statistical analysis (was: false positive)
"""

import pytest
import tempfile
import csv
from pathlib import Path

from validation_framework.profiler.engine import DataProfiler


@pytest.fixture
def sample_transactions_csv():
    """
    Create a minimal sample_transactions.csv fixture for regression testing.

    Mimics the structure that caused false positives:
    - Account: 9-character alphanumeric (AAA999999) - was flagged as credit card
    - Timestamp: Hourly datetime - was suggested as unique key
    - Is Laundering: Binary 0/1 flag - triggered 445% CV warning
    - From Bank, To Bank: Integer IDs 1-99 - triggered outlier warnings
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow([
            "Timestamp", "From Bank", "Account", "To Bank",
            "Amount Received", "Receiving Currency",
            "Amount Paid", "Payment Currency",
            "Payment Format", "Is Laundering"
        ])

        # 100 rows of synthetic data matching the false positive patterns
        for i in range(100):
            writer.writerow([
                f"2023-01-{(i % 28) + 1:02d} {i % 24:02d}:00:00",  # Unique hourly timestamps
                (i % 99) + 1,  # From Bank: 1-99
                f"AAA{100000 + i:06d}",  # Account: 9-char alphanumeric (AAA100000-AAA100099)
                ((i + 10) % 99) + 1,  # To Bank: 1-99
                round(100.0 + (i * 7.5), 2),  # Amount Received
                ["USD", "GBP", "EUR"][i % 3],  # Receiving Currency
                round(95.0 + (i * 7.0), 2),  # Amount Paid
                ["USD", "GBP", "EUR"][i % 3],  # Payment Currency
                ["Wire", "Cheque", "Credit Card", "ACH"][i % 4],  # Payment Format
                i % 2  # Is Laundering: Binary 0/1
            ])

        return f.name


def test_account_not_flagged_as_credit_card(sample_transactions_csv):
    """
    Regression test: Account field should NOT be detected as credit card PII.

    Issue: Account values like "AAA999999" (9 characters, contains letters) were
    incorrectly flagged as PCI-DSS credit card fields.

    Fixes applied:
    1. Enhanced Luhn validation (100+ samples, 80% threshold) - pii_detector.py:330-363
    2. Column name semantics blocking - pii_detector.py:217-230
    """
    profiler = DataProfiler()

    # Profile the CSV
    result = profiler.profile_file(sample_transactions_csv)

    # Find the Account column profile
    account_col = next(
        (col for col in result.columns if col.name == "Account"),
        None
    )

    assert account_col is not None, "Account column not found in profile"

    # Check PII detection results
    if hasattr(account_col.statistics, 'pii_detected') and account_col.statistics.pii_detected:
        pii_types = [pii.get("pii_type") for pii in account_col.statistics.pii_types]
        assert "credit_card" not in pii_types, \
            f"REGRESSION: Account field incorrectly flagged as credit card PII. PII types: {pii_types}"

    # Also check suggested validations don't include credit card patterns
    for suggestion in result.suggested_validations:
        if "Account" in str(suggestion.params):
            assert "credit" not in suggestion.validation_type.lower(), \
                f"REGRESSION: Credit card validation suggested for Account field: {suggestion}"


def test_timestamp_not_suggested_as_unique_key(sample_transactions_csv):
    """
    Regression test: Timestamp field should NOT be suggested as UniqueKeyCheck.

    Issue: Naturally unique timestamps (hourly data) were incorrectly flagged
    as candidate primary keys.

    Fix applied:
    - Added datetime/timestamp/date exclusion to UniqueKeyCheck - engine.py:1708-1723
    """
    profiler = DataProfiler()
    result = profiler.profile_file(sample_transactions_csv)

    # Check that no UniqueKeyCheck is suggested for Timestamp field
    timestamp_unique_suggestions = [
        s for s in result.suggested_validations
        if s.validation_type == "UniqueKeyCheck" and "Timestamp" in s.params.get("fields", [])
    ]

    assert len(timestamp_unique_suggestions) == 0, \
        f"REGRESSION: Timestamp field incorrectly suggested as UniqueKeyCheck: {timestamp_unique_suggestions}"


def test_binary_field_no_cv_warning(sample_transactions_csv):
    """
    Regression test: Is Laundering binary field should NOT trigger CV warnings.

    Issue: Binary 0/1 fields generated meaningless "CV=445%" OutlierDetectionCheck
    warnings (mathematically trivial for binary variables).

    Fix applied:
    - Binary field exclusion from CV analysis - engine.py:1575-1599
    """
    profiler = DataProfiler()
    result = profiler.profile_file(sample_transactions_csv)

    # Check that no OutlierDetectionCheck is suggested for Is Laundering field
    laundering_outlier_suggestions = [
        s for s in result.suggested_validations
        if s.validation_type == "OutlierDetectionCheck" and
           s.params.get("field") == "Is Laundering"
    ]

    assert len(laundering_outlier_suggestions) == 0, \
        f"REGRESSION: Is Laundering binary field triggered CV warning: {laundering_outlier_suggestions}"


def test_bank_id_fields_no_statistical_analysis(sample_transactions_csv):
    """
    Regression test: From Bank/To Bank ID fields should NOT trigger statistical analysis.

    Issue: Bank ID integers (1-99) triggered outlier detection warnings despite
    being categorical code fields.

    Fix applied:
    - ID/code field exclusion from statistical validation - engine.py:1563-1572
    """
    profiler = DataProfiler()
    result = profiler.profile_file(sample_transactions_csv)

    # Check that no OutlierDetectionCheck is suggested for bank ID fields
    bank_outlier_suggestions = [
        s for s in result.suggested_validations
        if s.validation_type == "OutlierDetectionCheck" and
           s.params.get("field") in ["From Bank", "To Bank"]
    ]

    assert len(bank_outlier_suggestions) == 0, \
        f"REGRESSION: Bank ID fields triggered statistical analysis: {bank_outlier_suggestions}"


def test_overall_suggestion_quality(sample_transactions_csv):
    """
    Integration test: Verify overall suggestion quality is improved.

    The enhanced profiler should generate meaningful, actionable suggestions
    without the noise of false positives.
    """
    profiler = DataProfiler()
    result = profiler.profile_file(sample_transactions_csv)

    # Count suggestions by type
    suggestion_counts = {}
    for s in result.suggested_validations:
        suggestion_counts[s.validation_type] = suggestion_counts.get(s.validation_type, 0) + 1

    # Verify expected validation types are present
    assert "MandatoryFieldCheck" in suggestion_counts, \
        "MandatoryFieldCheck should be suggested for high-completeness fields"

    assert "ValidValuesCheck" in suggestion_counts, \
        "ValidValuesCheck should be suggested for low-cardinality categoricals"

    # Verify problematic validation types are absent or minimal
    # With 100 rows and 9 non-datetime fields, some high-cardinality fields may still
    # be suggested as unique keys (Account, From Bank/To Bank combos).
    # The key fix is that Timestamp is NO LONGER suggested.
    timestamp_unique = any(
        s.validation_type == "UniqueKeyCheck" and "Timestamp" in s.params.get("fields", [])
        for s in result.suggested_validations
    )
    assert not timestamp_unique, \
        "Timestamp field should NOT be suggested as UniqueKeyCheck"

    assert suggestion_counts.get("UniqueKeyCheck", 0) <= 4, \
        f"Too many UniqueKeyCheck suggestions (expected ≤4): {suggestion_counts.get('UniqueKeyCheck', 0)}"

    assert suggestion_counts.get("OutlierDetectionCheck", 0) <= 2, \
        f"Too many OutlierDetectionCheck suggestions (expected ≤2): {suggestion_counts.get('OutlierDetectionCheck', 0)}"

    # Log summary for debugging
    print(f"\n📊 Suggestion Summary:")
    for validation_type, count in sorted(suggestion_counts.items()):
        print(f"  {validation_type}: {count}")


@pytest.fixture(autouse=True)
def cleanup(sample_transactions_csv):
    """Clean up temporary CSV file after test."""
    yield
    Path(sample_transactions_csv).unlink(missing_ok=True)
