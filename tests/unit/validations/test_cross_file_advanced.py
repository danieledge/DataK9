"""
Comprehensive tests for advanced cross-file validation rules.

This test suite covers CrossFileKeyCheck validation with all 4 check modes:
- exact_match: Strict foreign key validation
- overlap: Partial data matching
- subset: All current keys must exist in reference
- superset: Reference must be subset of current keys

Tests cover single and composite keys, null handling, and memory-efficient processing.

Author: Daniel Edge
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

from validation_framework.validations.builtin.cross_file_advanced import CrossFileKeyCheck
from validation_framework.core.results import Severity
from tests.conftest import create_data_iterator


# ============================================================================
# TEST FIXTURES
# ============================================================================

@pytest.fixture
def temp_csv_file():
    """
    Creates a temporary CSV file for testing reference files.

    Yields:
        Path to temporary CSV file
    """
    temp_fd, temp_path = tempfile.mkstemp(suffix='.csv')
    os.close(temp_fd)
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.remove(temp_path)


def create_reference_file(path: str, data: pd.DataFrame):
    """
    Helper to create a CSV reference file from DataFrame.

    Args:
        path: File path to create
        data: DataFrame to write
    """
    data.to_csv(path, index=False)


# ============================================================================
# EXACT MATCH MODE TESTS
# ============================================================================

@pytest.mark.unit
class TestCrossFileKeyCheckExactMatch:
    """Test CrossFileKeyCheck validation with exact_match mode."""

    def test_all_keys_valid(self, temp_csv_file):
        """Test validation passes when all foreign keys exist in reference."""
        # Create reference file
        reference_df = pd.DataFrame({
            "id": [101, 102, 103, 104, 105]
        })
        create_reference_file(temp_csv_file, reference_df)

        # Create data with valid foreign keys
        df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 103]
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "exact_match",
                "allow_null": False
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True
        assert result.failed_count == 0
        assert "valid" in result.message.lower()

    def test_invalid_keys_detected(self, temp_csv_file):
        """Test validation fails when foreign keys don't exist in reference."""
        # Create reference file
        reference_df = pd.DataFrame({
            "id": [101, 102, 103]
        })
        create_reference_file(temp_csv_file, reference_df)

        # Create data with invalid foreign key
        df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": [101, 102, 999]  # 999 is invalid
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "exact_match"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count == 1
        assert "violation" in result.message.lower()

    def test_null_handling_disallowed(self, temp_csv_file):
        """Test that null foreign keys fail when allow_null=False."""
        reference_df = pd.DataFrame({"id": [101, 102]})
        create_reference_file(temp_csv_file, reference_df)

        df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": [101, None, 102]
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "exact_match",
                "allow_null": False
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 1

    def test_null_handling_allowed(self, temp_csv_file):
        """Test that null foreign keys pass when allow_null=True."""
        reference_df = pd.DataFrame({"id": [101, 102]})
        create_reference_file(temp_csv_file, reference_df)

        df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": [101, None, 102]
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "exact_match",
                "allow_null": True
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True
        assert result.failed_count == 0

    def test_composite_keys(self, temp_csv_file):
        """Test validation with multi-column composite keys."""
        reference_df = pd.DataFrame({
            "country": ["US", "US", "CA"],
            "state": ["NY", "CA", "ON"]
        })
        create_reference_file(temp_csv_file, reference_df)

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "country": ["US", "US", "MX"],  # MX-DF is invalid
            "state": ["NY", "CA", "DF"]
        })

        validation = CrossFileKeyCheck(
            name="LocationFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": ["country", "state"],
                "reference_file": temp_csv_file,
                "reference_key": ["country", "state"],
                "check_mode": "exact_match"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 1


# ============================================================================
# OVERLAP MODE TESTS
# ============================================================================

@pytest.mark.unit
class TestCrossFileKeyCheckOverlap:
    """Test CrossFileKeyCheck validation with overlap mode."""

    def test_sufficient_overlap_passes(self, temp_csv_file):
        """Test validation passes when overlap exceeds minimum threshold."""
        reference_df = pd.DataFrame({"id": [101, 102, 103, 104, 105]})
        create_reference_file(temp_csv_file, reference_df)

        # 75% overlap (3 out of 4 unique keys match)
        df = pd.DataFrame({
            "order_id": [1, 2, 3, 4, 5],
            "customer_id": [101, 102, 103, 999, 101]  # 999 doesn't exist
        })

        validation = CrossFileKeyCheck(
            name="CustomerOverlap",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "overlap",
                "min_overlap_pct": 50.0  # Require 50% overlap
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True
        assert "overlap" in result.message.lower()
        assert "passed" in result.message.lower()

    def test_insufficient_overlap_fails(self, temp_csv_file):
        """Test validation fails when overlap below minimum threshold."""
        reference_df = pd.DataFrame({"id": [101, 102, 103]})
        create_reference_file(temp_csv_file, reference_df)

        # Only 33% overlap
        df = pd.DataFrame({
            "customer_id": [101, 999, 888]  # Only 101 exists
        })

        validation = CrossFileKeyCheck(
            name="CustomerOverlap",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "overlap",
                "min_overlap_pct": 50.0  # Require 50% overlap
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "failed" in result.message.lower()

    def test_default_min_overlap(self, temp_csv_file):
        """Test that default min_overlap_pct is 1% (any overlap passes)."""
        reference_df = pd.DataFrame({"id": [101, 102, 103]})
        create_reference_file(temp_csv_file, reference_df)

        # Very low overlap but still > 0%
        df = pd.DataFrame({
            "customer_id": [101, 999, 888, 777]  # 25% overlap
        })

        validation = CrossFileKeyCheck(
            name="CustomerOverlap",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "overlap"
                # No min_overlap_pct specified - should default to 1%
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True


# ============================================================================
# SUBSET MODE TESTS
# ============================================================================

@pytest.mark.unit
class TestCrossFileKeyCheckSubset:
    """Test CrossFileKeyCheck validation with subset mode."""

    def test_valid_subset(self, temp_csv_file):
        """Test validation passes when current keys are subset of reference."""
        reference_df = pd.DataFrame({"id": [101, 102, 103, 104, 105]})
        create_reference_file(temp_csv_file, reference_df)

        # All keys exist in reference
        df = pd.DataFrame({
            "customer_id": [101, 102, 103]
        })

        validation = CrossFileKeyCheck(
            name="CustomerSubset",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "subset"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_invalid_subset(self, temp_csv_file):
        """Test validation fails when current has keys not in reference."""
        reference_df = pd.DataFrame({"id": [101, 102, 103]})
        create_reference_file(temp_csv_file, reference_df)

        # Contains key not in reference
        df = pd.DataFrame({
            "customer_id": [101, 102, 999]
        })

        validation = CrossFileKeyCheck(
            name="CustomerSubset",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "subset"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert result.failed_count >= 1


# ============================================================================
# SUPERSET MODE TESTS
# ============================================================================

@pytest.mark.unit
class TestCrossFileKeyCheckSuperset:
    """Test CrossFileKeyCheck validation with superset mode."""

    def test_valid_superset(self, temp_csv_file):
        """Test validation passes when current keys are superset of reference."""
        reference_df = pd.DataFrame({"id": [101, 102, 103]})
        create_reference_file(temp_csv_file, reference_df)

        # Contains all reference keys plus more
        df = pd.DataFrame({
            "customer_id": [101, 102, 103, 104, 105]
        })

        validation = CrossFileKeyCheck(
            name="CustomerSuperset",
            severity=Severity.WARNING,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "superset"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True

    def test_invalid_superset(self, temp_csv_file):
        """Test validation fails when missing reference keys."""
        reference_df = pd.DataFrame({"id": [101, 102, 103, 104, 105]})
        create_reference_file(temp_csv_file, reference_df)

        # Missing keys 104 and 105
        df = pd.DataFrame({
            "customer_id": [101, 102, 103]
        })

        validation = CrossFileKeyCheck(
            name="CustomerSuperset",
            severity=Severity.WARNING,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "superset"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "not found" in result.message.lower()


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================

@pytest.mark.unit
class TestCrossFileKeyCheckErrors:
    """Test error handling in CrossFileKeyCheck."""

    def test_missing_reference_file(self):
        """Test proper error when reference file doesn't exist."""
        df = pd.DataFrame({
            "customer_id": [101, 102]
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": "/nonexistent/file.csv",
                "reference_key": "id",
                "check_mode": "exact_match"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "not found" in result.message.lower()

    def test_missing_foreign_key_column(self, temp_csv_file):
        """Test error when foreign key column doesn't exist in data."""
        reference_df = pd.DataFrame({"id": [101, 102]})
        create_reference_file(temp_csv_file, reference_df)

        df = pd.DataFrame({
            "order_id": [1, 2]
            # No customer_id column
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",  # This column doesn't exist
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "exact_match"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "not found" in result.message.lower()

    def test_invalid_check_mode(self, temp_csv_file):
        """Test error when invalid check_mode is specified."""
        reference_df = pd.DataFrame({"id": [101, 102]})
        create_reference_file(temp_csv_file, reference_df)

        df = pd.DataFrame({
            "customer_id": [101, 102]
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": temp_csv_file,
                "reference_key": "id",
                "check_mode": "invalid_mode"  # Invalid
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "invalid" in result.message.lower()

    def test_missing_required_params(self):
        """Test error when required parameters are missing."""
        df = pd.DataFrame({
            "customer_id": [101, 102]
        })

        # Missing reference_file parameter
        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_key": "id"
                # Missing reference_file
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is False
        assert "required" in result.message.lower()


# ============================================================================
# DIFFERENT FILE FORMATS TESTS
# ============================================================================

@pytest.mark.unit
class TestCrossFileKeyCheckFormats:
    """Test CrossFileKeyCheck with different file formats."""

    def test_parquet_reference_file(self, tmp_path):
        """Test validation with Parquet reference file."""
        # Create Parquet reference file
        reference_path = tmp_path / "reference.parquet"
        reference_df = pd.DataFrame({"id": [101, 102, 103]})
        reference_df.to_parquet(reference_path)

        df = pd.DataFrame({
            "customer_id": [101, 102, 103]
        })

        validation = CrossFileKeyCheck(
            name="CustomerFK",
            severity=Severity.ERROR,
            params={
                "foreign_key": "customer_id",
                "reference_file": str(reference_path),
                "reference_key": "id",
                "reference_file_format": "parquet",
                "check_mode": "exact_match"
            }
        )
        result = validation.validate(create_data_iterator(df), {})

        assert result.passed is True
