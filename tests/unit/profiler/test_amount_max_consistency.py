"""
Regression tests for Amount MAX value consistency across profiler outputs.

This test suite prevents regression of the Amount MAX inconsistency issue where
different parts of the profiler reported different maximum values:
- Column statistics
- Benford analysis narrative
- ML outlier detection
- Cross-field ratio analysis

Issue: Benford narrative was computing its own max from a different data path,
leading to inconsistent values being displayed in the report.

Fix: All max values now derive from the canonical ColumnStatistics.max_value.
"""

import pytest
import tempfile
import csv
from pathlib import Path

from validation_framework.profiler.engine import DataProfiler


@pytest.fixture
def amount_dataset_csv():
    """
    Create a dataset with known Amount values for MAX consistency testing.

    The Amount column has:
    - Known min: 100.0
    - Known max: 99999.0
    - Various values in between
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
        writer = csv.writer(f)

        # Header
        writer.writerow([
            "Transaction_ID", "Amount", "Account_ID", "Timestamp"
        ])

        # Create 200 rows with controlled Amount values
        # Include the exact min (100.0) and max (99999.0) values
        amounts = [
            100.0,      # Known min
            99999.0,    # Known max
        ]

        # Add various values in between
        for i in range(198):
            amounts.append(100.0 + (i * 500))  # Range from 100 to ~99100

        for i, amount in enumerate(amounts):
            writer.writerow([
                f"TX{i:06d}",
                amount,
                f"ACC{i % 50:04d}",
                f"2023-01-{(i % 28) + 1:02d} {i % 24:02d}:00:00"
            ])

        return f.name


def test_amount_max_in_column_statistics(amount_dataset_csv):
    """
    Test that Amount column max_value is correctly captured in statistics.
    """
    profiler = DataProfiler()
    result = profiler.profile_file(amount_dataset_csv)

    # Find the Amount column
    amount_col = next(
        (col for col in result.columns if col.name == "Amount"),
        None
    )

    assert amount_col is not None, "Amount column not found in profile"
    assert amount_col.statistics.max_value is not None, "max_value should not be None"

    # The max should be exactly 99999.0 (our known max)
    assert float(amount_col.statistics.max_value) == 99999.0, \
        f"Expected max 99999.0, got {amount_col.statistics.max_value}"


def test_amount_max_consistency_with_ml_findings(amount_dataset_csv):
    """
    Test that ML findings (if present) use consistent max values.

    This verifies that any ML analysis (Benford, outliers) reports
    statistics consistent with ColumnStatistics.
    """
    profiler = DataProfiler()
    result = profiler.profile_file(amount_dataset_csv)

    # Find the Amount column
    amount_col = next(
        (col for col in result.columns if col.name == "Amount"),
        None
    )

    assert amount_col is not None, "Amount column not found"
    canonical_max = float(amount_col.statistics.max_value)

    # Check ML findings if present
    if hasattr(result, 'ml_findings') and result.ml_findings:
        ml_findings = result.ml_findings

        # Check Benford analysis
        if 'benford_analysis' in ml_findings:
            benford = ml_findings['benford_analysis']
            for col_name, col_data in benford.items():
                if col_name == "Amount" and isinstance(col_data, dict):
                    # If ML has a max value stored, it should match
                    if 'max_value' in col_data:
                        ml_max = float(col_data['max_value'])
                        assert abs(ml_max - canonical_max) < 0.01, \
                            f"Benford max {ml_max} != stats max {canonical_max}"

        # Check outlier analysis
        if 'outliers' in ml_findings:
            outliers = ml_findings['outliers']
            for col_name, col_data in outliers.items():
                if col_name == "Amount" and isinstance(col_data, dict):
                    if 'max' in col_data:
                        outlier_max = float(col_data['max'])
                        # Outlier max should be <= canonical max (might be from sample)
                        assert outlier_max <= canonical_max * 1.001, \
                            f"Outlier max {outlier_max} > stats max {canonical_max}"


def test_amount_min_consistency(amount_dataset_csv):
    """
    Test that Amount column min_value is correctly captured.
    """
    profiler = DataProfiler()
    result = profiler.profile_file(amount_dataset_csv)

    amount_col = next(
        (col for col in result.columns if col.name == "Amount"),
        None
    )

    assert amount_col is not None, "Amount column not found"
    assert amount_col.statistics.min_value is not None, "min_value should not be None"

    # The min should be exactly 100.0 (our known min)
    assert float(amount_col.statistics.min_value) == 100.0, \
        f"Expected min 100.0, got {amount_col.statistics.min_value}"


def test_amount_statistics_complete(amount_dataset_csv):
    """
    Test that Amount column has complete statistics for numeric type.
    """
    profiler = DataProfiler()
    result = profiler.profile_file(amount_dataset_csv)

    amount_col = next(
        (col for col in result.columns if col.name == "Amount"),
        None
    )

    assert amount_col is not None, "Amount column not found"
    stats = amount_col.statistics

    # Verify all key statistics are present
    assert stats.min_value is not None, "min_value missing"
    assert stats.max_value is not None, "max_value missing"
    assert stats.mean is not None, "mean missing"
    assert stats.median is not None, "median missing"

    # Verify value relationships
    min_val = float(stats.min_value)
    max_val = float(stats.max_value)
    mean_val = float(stats.mean)

    assert min_val <= mean_val <= max_val, \
        f"Invalid statistics: min={min_val}, mean={mean_val}, max={max_val}"


def test_rangecheck_not_suggested_for_amount(amount_dataset_csv):
    """
    Regression test: Amount fields should NOT have bounded RangeCheck suggested.

    Issue: Amount fields are unbounded by nature (financial amounts can grow
    infinitely), so a bounded RangeCheck (with max_value) is inappropriate.

    However, a FIBO-derived non-negative check (min_value=0, no max) IS appropriate
    since financial amounts should generally be >= 0.

    Fix: engine.py:_should_suggest_range_check() excludes 'amount' keyword for
    bounded range checks, but FIBO semantic rules can still suggest non-negative checks.
    """
    profiler = DataProfiler()
    result = profiler.profile_file(amount_dataset_csv)

    # Check validation suggestions - exclude FIBO non-negative checks (min_value=0, no max)
    bounded_rangecheck = [
        s for s in result.suggested_validations
        if s.validation_type == "RangeCheck"
        and s.params.get("field") == "Amount"
        and s.params.get("max_value") is not None  # Bounded range check
    ]

    assert len(bounded_rangecheck) == 0, \
        f"Bounded RangeCheck should NOT be suggested for Amount field (financial amounts are unbounded). Found: {bounded_rangecheck}"

    # Non-negative check (min_value=0/min_value >= 0, no max) is acceptable
    nonnegative_checks = [
        s for s in result.suggested_validations
        if s.validation_type == "RangeCheck"
        and s.params.get("field") == "Amount"
        and s.params.get("min_value") is not None
        and s.params.get("max_value") is None  # No upper bound
    ]

    # If present, verify it's a sensible non-negative check
    for check in nonnegative_checks:
        min_val = check.params.get("min_value", 0)
        assert min_val >= 0, f"Non-negative check should have min_value >= 0, got {min_val}"
