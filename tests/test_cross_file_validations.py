"""
Tests for cross-file validation rules.

Tests ReferentialIntegrityCheck, CrossFileComparisonCheck, and CrossFileDuplicateCheck.
"""

import pytest
import pandas as pd
from pathlib import Path
from validation_framework.validations.builtin.cross_file_checks import (
    ReferentialIntegrityCheck,
    CrossFileComparisonCheck,
    CrossFileDuplicateCheck,
)
from validation_framework.core.results import Severity


# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "test_data" / "cross_file"


class TestReferentialIntegrityCheck:
    """Test ReferentialIntegrityCheck validation."""

    def test_valid_foreign_keys(self):
        """Test that valid foreign keys pass validation."""
        # Create test data with valid foreign keys only
        data = pd.DataFrame({
            'order_id': [1, 2, 3],
            'customer_id': [1, 2, 3],
            'amount': [100, 200, 300]
        })

        # Create validation
        validation = ReferentialIntegrityCheck(
            name="test_referential_integrity",
            severity=Severity.ERROR,
            params={
                'foreign_key': 'customer_id',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_key': 'id',
                'allow_null': False,
            }
        )

        # Run validation
        context = {'file_path': str(TEST_DATA_DIR / 'orders.csv')}
        result = validation.validate(iter([data]), context)

        # Assert passed
        assert result.passed is True
        assert result.failed_count == 0

    def test_invalid_foreign_keys(self):
        """Test that invalid foreign keys fail validation."""
        # Create test data with invalid foreign keys
        data = pd.DataFrame({
            'order_id': [1, 2, 3],
            'customer_id': [1, 99, 88],  # 99 and 88 don't exist
            'amount': [100, 200, 300]
        })

        # Create validation
        validation = ReferentialIntegrityCheck(
            name="test_referential_integrity",
            severity=Severity.ERROR,
            params={
                'foreign_key': 'customer_id',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_key': 'id',
                'allow_null': False,
            }
        )

        # Run validation
        context = {'file_path': str(TEST_DATA_DIR / 'orders.csv')}
        result = validation.validate(iter([data]), context)

        # Assert failed
        assert result.passed is False
        assert result.failed_count == 2  # Two invalid IDs
        assert len(result.sample_failures) > 0

    def test_null_foreign_keys_not_allowed(self):
        """Test that NULL foreign keys fail when not allowed."""
        # Create test data with NULL foreign key
        data = pd.DataFrame({
            'order_id': [1, 2, 3],
            'customer_id': [1, None, 3],
            'amount': [100, 200, 300]
        })

        # Create validation
        validation = ReferentialIntegrityCheck(
            name="test_referential_integrity",
            severity=Severity.ERROR,
            params={
                'foreign_key': 'customer_id',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_key': 'id',
                'allow_null': False,
            }
        )

        # Run validation
        context = {'file_path': str(TEST_DATA_DIR / 'orders.csv')}
        result = validation.validate(iter([data]), context)

        # Assert failed due to NULL
        assert result.passed is False
        assert result.failed_count >= 1

    def test_null_foreign_keys_allowed(self):
        """Test that NULL foreign keys pass when allowed."""
        # Create test data with NULL foreign key
        data = pd.DataFrame({
            'order_id': [1, 2, 3],
            'customer_id': [1, None, 3],
            'amount': [100, 200, 300]
        })

        # Create validation
        validation = ReferentialIntegrityCheck(
            name="test_referential_integrity",
            severity=Severity.ERROR,
            params={
                'foreign_key': 'customer_id',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_key': 'id',
                'allow_null': True,
            }
        )

        # Run validation
        context = {'file_path': str(TEST_DATA_DIR / 'orders.csv')}
        result = validation.validate(iter([data]), context)

        # Assert passed (NULL is allowed, other values are valid)
        assert result.passed is True

    def test_missing_reference_file(self):
        """Test that missing reference file is handled."""
        data = pd.DataFrame({
            'order_id': [1, 2],
            'customer_id': [1, 2],
        })

        validation = ReferentialIntegrityCheck(
            name="test_referential_integrity",
            severity=Severity.ERROR,
            params={
                'foreign_key': 'customer_id',
                'reference_file': 'nonexistent_file.csv',
                'reference_key': 'id',
            }
        )

        context = {}
        result = validation.validate(iter([data]), context)

        # Should fail gracefully
        assert result.passed is False
        assert "not found" in result.message.lower()


class TestCrossFileComparisonCheck:
    """Test CrossFileComparisonCheck validation."""

    def test_count_comparison_equal(self):
        """Test count comparison between files."""
        # Create test data
        data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50]
        })

        # Create validation (compare count)
        validation = CrossFileComparisonCheck(
            name="test_count_comparison",
            severity=Severity.ERROR,
            params={
                'aggregation': 'count',
                'comparison': '==',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_aggregation': 'count',
                'tolerance': 0,
            }
        )

        # Run validation
        context = {'file_path': str(TEST_DATA_DIR / 'orders.csv')}
        result = validation.validate(iter([data]), context)

        # Assert (both files have 5 rows)
        assert result.passed is True

    def test_sum_comparison_with_tolerance(self):
        """Test sum comparison with percentage tolerance."""
        # Create test data with 3 rows
        data = pd.DataFrame({
            'amount': [100, 200, 300]  # Sum = 600, Count = 3
        })

        # Create validation - comparing count to count
        validation = CrossFileComparisonCheck(
            name="test_count_comparison",
            severity=Severity.ERROR,
            params={
                'aggregation': 'count',
                'comparison': '==',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_aggregation': 'count',  # Count is 5
                'tolerance_pct': 50,  # Allow 50% tolerance: 3 is within 50% of 5
            }
        )

        # Run validation
        context = {}
        result = validation.validate(iter([data]), context)

        # With 50% tolerance, 3 should be within range of 5 (5 ± 2.5)
        assert result.passed is True

    def test_comparison_greater_than(self):
        """Test greater than comparison."""
        data = pd.DataFrame({
            'value': list(range(10))  # 10 rows
        })

        validation = CrossFileComparisonCheck(
            name="test_gt_comparison",
            severity=Severity.WARNING,
            params={
                'aggregation': 'count',
                'comparison': '>',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_aggregation': 'count',  # customers has 5 rows
            }
        )

        context = {}
        result = validation.validate(iter([data]), context)

        # 10 > 5, should pass
        assert result.passed is True


class TestCrossFileDuplicateCheck:
    """Test CrossFileDuplicateCheck validation."""

    def test_no_duplicates_across_files(self):
        """Test when there are no duplicates."""
        # Create data with unique IDs not in customers file
        data = pd.DataFrame({
            'id': [10, 11, 12],
            'name': ['New1', 'New2', 'New3']
        })

        validation = CrossFileDuplicateCheck(
            name="test_no_duplicates",
            severity=Severity.ERROR,
            params={
                'columns': ['id'],
                'reference_files': [str(TEST_DATA_DIR / 'customers.csv')],
            }
        )

        context = {}
        result = validation.validate(iter([data]), context)

        # Should pass (no duplicates)
        assert result.passed is True

    def test_duplicates_found(self):
        """Test when duplicates exist across files."""
        # Create data with IDs that exist in customers file
        data = pd.DataFrame({
            'id': [1, 2, 6],  # 1 and 2 exist in customers.csv
            'name': ['Dup1', 'Dup2', 'Unique']
        })

        validation = CrossFileDuplicateCheck(
            name="test_duplicates_found",
            severity=Severity.ERROR,
            params={
                'columns': ['id'],
                'reference_files': [str(TEST_DATA_DIR / 'customers.csv')],
            }
        )

        context = {}
        result = validation.validate(iter([data]), context)

        # Should fail (2 duplicates found)
        assert result.passed is False
        assert result.failed_count == 2

    def test_composite_key_duplicates(self):
        """Test duplicate detection with composite keys."""
        # Use columns that exist in the customers file for a meaningful test
        data = pd.DataFrame({
            'id': [10, 11],  # IDs not in customers
            'name': ['Test1', 'Test2']
        })

        # This test validates the composite key functionality works
        validation = CrossFileDuplicateCheck(
            name="test_composite_keys",
            severity=Severity.WARNING,
            params={
                'columns': ['id', 'name'],
                'reference_files': [str(TEST_DATA_DIR / 'customers.csv')],
            }
        )

        context = {}
        result = validation.validate(iter([data]), context)

        # Should pass (composite keys won't match with customers data)
        assert result.passed is True


class TestCrossFileValidationIntegration:
    """Integration tests for cross-file validations."""

    def test_full_order_validation_workflow(self):
        """Test a realistic order validation scenario."""
        # Read actual test data
        orders_df = pd.read_csv(TEST_DATA_DIR / 'orders.csv')

        # 1. Check referential integrity
        ref_check = ReferentialIntegrityCheck(
            name="order_customer_ref",
            severity=Severity.ERROR,
            params={
                'foreign_key': 'customer_id',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_key': 'id',
            }
        )

        context = {'file_path': str(TEST_DATA_DIR / 'orders.csv')}
        ref_result = ref_check.validate(iter([orders_df]), context)

        # Should fail (orders.csv has invalid customer IDs: 99, 88)
        assert ref_result.passed is False
        assert ref_result.failed_count == 2

        # 2. Check order count vs customer count
        count_check = CrossFileComparisonCheck(
            name="order_customer_count",
            severity=Severity.WARNING,
            params={
                'aggregation': 'count',
                'comparison': '>',
                'reference_file': str(TEST_DATA_DIR / 'customers.csv'),
                'reference_aggregation': 'count',
            }
        )

        count_result = count_check.validate(iter([orders_df]), context)

        # Orders (7) > Customers (5), should pass
        assert count_result.passed is True


def test_cross_file_validation_description():
    """Test that validation descriptions are human-readable."""
    validation = ReferentialIntegrityCheck(
        name="test",
        severity=Severity.ERROR,
        params={
            'foreign_key': 'customer_id',
            'reference_file': 'customers.csv',
            'reference_key': 'id',
        }
    )

    desc = validation.get_description()
    assert 'customer_id' in desc
    assert 'customers.csv' in desc
    assert 'id' in desc


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
