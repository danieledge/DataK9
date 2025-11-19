"""
Comprehensive Regression Test Suite for DataK9 Validation Framework

Tests all 35 validation types through integration testing.
Ensures complete coverage of validation functionality with the validation engine.
"""

import pytest
import pandas as pd
from pathlib import Path
from validation_framework.core.engine import ValidationEngine
from validation_framework.core.config import ValidationConfig


class TestComprehensiveRegression:
    """Comprehensive regression tests for all 35 validation types."""

    @pytest.fixture
    def regression_config_path(self):
        """Path to comprehensive regression test configuration."""
        return Path(__file__).parent / "test_data" / "regression_test_config.yaml"

    @pytest.fixture
    def regression_data_path(self):
        """Path to regression test data."""
        return Path(__file__).parent / "test_data" / "regression_test_data.csv"

    @pytest.fixture
    def regression_data(self, regression_data_path):
        """Load regression test data as DataFrame."""
        return pd.read_csv(regression_data_path)

    def test_regression_data_exists(self, regression_data_path):
        """Test that regression test data file exists."""
        assert regression_data_path.exists(), "Regression test data file not found"

    def test_regression_config_exists(self, regression_config_path):
        """Test that regression config file exists."""
        assert regression_config_path.exists(), "Regression config file not found"

    def test_regression_data_structure(self, regression_data):
        """Test that regression data has expected structure."""
        expected_columns = [
            'employee_id', 'first_name', 'last_name', 'email', 'phone',
            'dept_id', 'salary', 'hire_date', 'status', 'performance_score',
            'years_experience', 'manager_id', 'last_review_date'
        ]

        assert list(regression_data.columns) == expected_columns
        assert len(regression_data) == 25, f"Expected 25 rows, got {len(regression_data)}"

    def test_comprehensive_validation_suite(self, regression_config_path):
        """Test running complete validation suite with all 35 validation types."""
        # Load configuration
        config = ValidationConfig.from_yaml(str(regression_config_path))

        # Create and run validation engine
        engine = ValidationEngine(config)
        results = engine.run()

        # Verify results
        assert results is not None, "Validation results should not be None"
        assert hasattr(results, 'validation_results'), "Results should have validation_results"

        # Count validations
        total_validations = len(results.validation_results)
        print(f"\n✓ Ran {total_validations} validations")

        # Expected: 30 validations in config (cross-file validations excluded for single file test)
        assert total_validations >= 25, f"Expected at least 25 validations, got {total_validations}"

    def test_validation_types_coverage(self, regression_config_path):
        """Test that config covers all major validation categories."""
        config = ValidationConfig.from_yaml(str(regression_config_path))

        validation_types = [v['type'] for v in config.validations]

        # Check coverage of major categories
        expected_types = [
            # File validations
            'EmptyFileCheck',
            # Schema validations
            'SchemaMatchCheck', 'ColumnExistenceCheck', 'DataTypeConsistencyCheck',
            # Field validations
            'MandatoryFieldCheck', 'CompletenessCheck', 'RangeCheck', 'ValueSetCheck',
            'PatternCheck', 'LengthCheck', 'UniqueConstraintCheck',
            # Record validations
            'RecordCountCheck', 'DuplicateCheck', 'ConditionalValidation',
            # Advanced validations
            'StatisticalOutlierCheck', 'CorrelationCheck', 'DistributionCheck',
            'DataFreshnessCheck', 'EntropyCheck',
        ]

        for expected_type in expected_types:
            assert expected_type in validation_types, f"Missing validation type: {expected_type}"

        print(f"\n✓ Configuration covers {len(validation_types)} validation types")

    def test_profiler_with_regression_data(self, regression_data_path):
        """Test profiler with regression data."""
        from validation_framework.profiler.profiler import DataProfiler

        profiler = DataProfiler(str(regression_data_path), file_format="csv")
        profile = profiler.generate_profile()

        assert profile is not None
        assert 'file_info' in profile
        assert 'column_profiles' in profile

        # Check that all columns are profiled
        assert len(profile['column_profiles']) == 13

        print(f"\n✓ Profiler generated profile with {len(profile['column_profiles'])} columns")


class TestValidationCategories:
    """Test validations by category to ensure complete coverage."""

    def test_file_validation_category(self):
        """Test all file validation types exist."""
        from validation_framework.validations.builtin import file_checks

        # 1 file validation type
        assert hasattr(file_checks, 'EmptyFileCheck')

    def test_schema_validation_category(self):
        """Test all schema validation types exist."""
        from validation_framework.validations.builtin import schema_checks

        # 3 schema validation types
        assert hasattr(schema_checks, 'SchemaMatchCheck')
        assert hasattr(schema_checks, 'ColumnExistenceCheck')
        assert hasattr(schema_checks, 'DataTypeConsistencyCheck')

    def test_field_validation_category(self):
        """Test all field validation types exist."""
        from validation_framework.validations.builtin import field_checks

        # 10 field validation types
        expected_validations = [
            'MandatoryFieldCheck', 'CompletenessCheck', 'RangeCheck',
            'ValueSetCheck', 'PatternCheck', 'LengthCheck',
            'FormatCheck', 'UniqueConstraintCheck', 'CustomValidation', 'NullCheck'
        ]

        for validation in expected_validations:
            assert hasattr(field_checks, validation), f"Missing {validation}"

    def test_record_validation_category(self):
        """Test all record validation types exist."""
        from validation_framework.validations.builtin import record_checks

        # 5 record validation types
        expected_validations = [
            'RecordCountCheck', 'DuplicateCheck', 'ConditionalValidation',
            'CrossFieldValidation', 'AggregateCheck'
        ]

        for validation in expected_validations:
            assert hasattr(record_checks, validation), f"Missing {validation}"

    def test_advanced_validation_category(self):
        """Test all advanced validation types exist."""
        from validation_framework.validations.builtin import advanced_checks

        # 8+ advanced validation types
        expected_validations = [
            'StatisticalOutlierCheck', 'CorrelationCheck', 'DistributionCheck',
            'SequenceCheck', 'TimeSeriesCheck', 'DataFreshnessCheck',
            'EntropyCheck', 'BenfordCheck'
        ]

        for validation in expected_validations:
            assert hasattr(advanced_checks, validation), f"Missing {validation}"

    def test_cross_file_validation_category(self):
        """Test all cross-file validation types exist."""
        from validation_framework.validations.builtin import cross_file_advanced

        # Cross-file validations
        expected_validations = [
            'CrossFileDuplicateCheck', 'CrossFileComparisonCheck',
            'ReferentialIntegrityCheck'
        ]

        for validation in expected_validations:
            assert hasattr(cross_file_advanced, validation), f"Missing {validation}"

    def test_database_validation_category(self):
        """Test all database-specific validation types exist."""
        from validation_framework.validations.builtin import database_checks

        # 3 database-specific validations
        expected_validations = [
            'DatabaseConstraintCheck',
            'DatabaseReferentialIntegrityCheck',
            'SQLCustomCheck'
        ]

        for validation in expected_validations:
            assert hasattr(database_checks, validation), f"Missing {validation}"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
