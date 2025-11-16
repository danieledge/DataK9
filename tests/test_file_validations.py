"""
Comprehensive tests for file-level validation rules.

This test suite covers all file validation checks including empty file detection,
row count validation, and file size validation.

Author: Daniel Edge
"""

import pytest
import pandas as pd
import tempfile
from pathlib import Path

from validation_framework.validations.builtin.file_checks import (
    EmptyFileCheck,
    RowCountRangeCheck,
    FileSizeCheck
)
from tests.conftest import create_data_iterator


# ============================================================================
# EMPTY FILE CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestEmptyFileCheck:
    """Test EmptyFileCheck validation."""
    
    def test_non_empty_file_passes(self):
        """Test validation passes for non-empty files."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })
        
        validation = EmptyFileCheck()
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is True
    
    def test_empty_dataframe_fails(self):
        """Test validation fails for empty DataFrame."""
        df = pd.DataFrame()
        
        validation = EmptyFileCheck()
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is False
    
    def test_dataframe_with_headers_only(self):
        """Test validation fails when DataFrame has only headers but no data."""
        df = pd.DataFrame(columns=["id", "name", "email"])
        
        validation = EmptyFileCheck()
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is False
    
    def test_single_row_file_passes(self):
        """Test validation passes for file with single row."""
        df = pd.DataFrame({
            "id": [1],
            "name": ["Alice"]
        })
        
        validation = EmptyFileCheck()
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is True


# ============================================================================
# ROW COUNT RANGE CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestRowCountRangeCheck:
    """Test RowCountRangeCheck validation."""
    
    def test_row_count_within_range(self):
        """Test validation passes when row count is within range."""
        df = pd.DataFrame({
            "id": range(50),
            "value": range(100, 150)
        })
        
        validation = RowCountRangeCheck(min_rows=10, max_rows=100)
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is True
    
    def test_row_count_below_minimum(self):
        """Test validation fails when row count is below minimum."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30]
        })
        
        validation = RowCountRangeCheck(min_rows=10, max_rows=100)
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is False
    
    def test_row_count_above_maximum(self):
        """Test validation fails when row count exceeds maximum."""
        df = pd.DataFrame({
            "id": range(150),
            "value": range(200, 350)
        })
        
        validation = RowCountRangeCheck(min_rows=10, max_rows=100)
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is False
    
    def test_row_count_at_boundaries(self):
        """Test validation passes for row counts at exact boundaries."""
        # Test minimum boundary
        df_min = pd.DataFrame({
            "id": range(10),
            "value": range(10)
        })
        
        validation = RowCountRangeCheck(min_rows=10, max_rows=100)
        result_min = validation.validate(create_data_iterator(df_min), {})
        
        assert result_min.passed is True
        
        # Test maximum boundary
        df_max = pd.DataFrame({
            "id": range(100),
            "value": range(100)
        })
        
        result_max = validation.validate(create_data_iterator(df_max), {})
        
        assert result_max.passed is True
    
    def test_min_rows_only(self):
        """Test validation with only minimum row count specified."""
        df = pd.DataFrame({
            "id": range(50),
            "value": range(50)
        })
        
        validation = RowCountRangeCheck(min_rows=10, max_rows=None)
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is True
    
    def test_max_rows_only(self):
        """Test validation with only maximum row count specified."""
        df = pd.DataFrame({
            "id": range(50),
            "value": range(50)
        })
        
        validation = RowCountRangeCheck(min_rows=None, max_rows=100)
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is True
    
    def test_empty_file_fails_min_rows(self):
        """Test that empty file fails minimum row check."""
        df = pd.DataFrame()
        
        validation = RowCountRangeCheck(min_rows=1, max_rows=None)
        result = validation.validate(create_data_iterator(df), {})
        
        assert result.passed is False


# ============================================================================
# FILE SIZE CHECK TESTS
# ============================================================================

@pytest.mark.unit
class TestFileSizeCheck:
    """Test FileSizeCheck validation."""
    
    def test_file_size_within_limit(self, tmp_path):
        """Test validation passes when file size is within limit."""
        # Create a small test file
        test_file = tmp_path / "small_file.csv"
        df = pd.DataFrame({
            "id": range(10),
            "name": [f"Name{i}" for i in range(10)]
        })
        df.to_csv(test_file, index=False)
        
        # Get file size
        file_size = test_file.stat().st_size
        
        # Validate with limit above file size
        validation = FileSizeCheck(max_size_bytes=file_size * 2)
        result = validation.validate(
            create_data_iterator(df),
            {"file_path": str(test_file)}
        )
        
        assert result.passed is True
    
    def test_file_size_exceeds_limit(self, tmp_path):
        """Test validation fails when file size exceeds limit."""
        # Create a test file
        test_file = tmp_path / "large_file.csv"
        df = pd.DataFrame({
            "id": range(100),
            "data": [f"LongStringData{i}" * 10 for i in range(100)]
        })
        df.to_csv(test_file, index=False)
        
        # Get file size
        file_size = test_file.stat().st_size
        
        # Validate with limit below file size
        validation = FileSizeCheck(max_size_bytes=100)  # 100 bytes limit
        result = validation.validate(
            create_data_iterator(df),
            {"file_path": str(test_file)}
        )
        
        assert result.passed is False
    
    def test_file_size_megabytes(self, tmp_path):
        """Test file size validation with MB limits."""
        test_file = tmp_path / "test_file.csv"
        df = pd.DataFrame({
            "id": range(50),
            "data": [f"Data{i}" for i in range(50)]
        })
        df.to_csv(test_file, index=False)
        
        # 1 MB = 1,048,576 bytes
        validation = FileSizeCheck(max_size_bytes=1048576)
        result = validation.validate(
            create_data_iterator(df),
            {"file_path": str(test_file)}
        )
        
        # Small file should pass 1MB limit
        assert result.passed is True
    
    def test_zero_byte_file(self, tmp_path):
        """Test validation for zero-byte files."""
        test_file = tmp_path / "empty.csv"
        test_file.touch()  # Create empty file
        
        validation = FileSizeCheck(max_size_bytes=1000)
        
        # Empty DataFrame
        df = pd.DataFrame()
        result = validation.validate(
            create_data_iterator(df),
            {"file_path": str(test_file)}
        )
        
        # Zero-byte file should pass size check
        assert result.passed is True


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

@pytest.mark.integration
class TestFileValidationsIntegration:
    """Integration tests for file-level validations."""
    
    def test_combined_file_validations(self, tmp_path):
        """Test multiple file validations together."""
        # Create test file
        test_file = tmp_path / "combined_test.csv"
        df = pd.DataFrame({
            "id": range(50),
            "name": [f"User{i}" for i in range(50)],
            "value": range(100, 150)
        })
        df.to_csv(test_file, index=False)
        
        context = {"file_path": str(test_file)}
        
        # Run all file validations
        empty_check = EmptyFileCheck()
        row_check = RowCountRangeCheck(min_rows=10, max_rows=100)
        size_check = FileSizeCheck(max_size_bytes=1048576)
        
        empty_result = empty_check.validate(create_data_iterator(df), context)
        row_result = row_check.validate(create_data_iterator(df), context)
        size_result = size_check.validate(create_data_iterator(df), context)
        
        # All should pass
        assert empty_result.passed is True
        assert row_result.passed is True
        assert size_result.passed is True
    
    def test_large_file_validation(self, tmp_path):
        """Test file validations with larger dataset."""
        test_file = tmp_path / "large_dataset.csv"
        df = pd.DataFrame({
            "id": range(10000),
            "value": range(10000, 20000),
            "category": [f"Cat{i % 100}" for i in range(10000)]
        })
        df.to_csv(test_file, index=False)
        
        context = {"file_path": str(test_file)}
        
        # Validate row count
        row_check = RowCountRangeCheck(min_rows=5000, max_rows=15000)
        result = row_check.validate(create_data_iterator(df), context)
        
        assert result.passed is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
