"""
Comprehensive Regression Test Suite for DataK9 Validation Framework

Tests data structure and validation configuration for regression testing.
Ensures test data and configuration files are properly structured.
"""

import pytest
import pandas as pd
from pathlib import Path


class TestComprehensiveRegression:
    """Comprehensive regression tests for test data and configuration."""

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
        print(f"\n✓ Regression data file found: {regression_data_path}")

    def test_regression_config_exists(self, regression_config_path):
        """Test that regression config file exists."""
        assert regression_config_path.exists(), "Regression config file not found"
        print(f"\n✓ Regression config file found: {regression_config_path}")

    def test_regression_data_structure(self, regression_data):
        """Test that regression data has expected structure."""
        expected_columns = [
            'employee_id', 'first_name', 'last_name', 'email', 'phone',
            'dept_id', 'salary', 'hire_date', 'status', 'performance_score',
            'years_experience', 'manager_id', 'last_review_date'
        ]

        assert list(regression_data.columns) == expected_columns
        assert len(regression_data) == 25, f"Expected 25 rows, got {len(regression_data)}"
        print(f"\n✓ Regression data has correct structure: {len(regression_data)} rows, {len(expected_columns)} columns")

    def test_regression_data_quality_issues(self, regression_data):
        """Test that regression data contains designed quality issues for testing."""
        # Check for missing values (for CompletenessCheck testing)
        assert regression_data['email'].isna().any(), "Should have missing email values"
        assert regression_data['phone'].isna().any(), "Should have missing phone values"

        # Check for duplicate records (for DuplicateCheck testing)
        # Row 1 and row 21 have the same email (john.doe@company.com)
        email_counts = regression_data['email'].value_counts()
        has_duplicates = (email_counts > 1).any()
        assert has_duplicates, "Should have duplicate emails"

        num_duplicates = (email_counts > 1).sum()
        print(f"  - Duplicate emails found: {num_duplicates} email(s) with duplicates")

        # Check for outliers (for StatisticalOutlierCheck testing)
        max_salary = regression_data['salary'].max()
        assert max_salary > 100000, "Should have salary outliers"

        print(f"\n✓ Regression data contains designed quality issues:")
        print(f"  - Missing emails: {regression_data['email'].isna().sum()}")
        print(f"  - Missing phones: {regression_data['phone'].isna().sum()}")
        print(f"  - Max salary (outlier): ${max_salary:,.2f}")

    def test_config_yaml_structure(self, regression_config_path):
        """Test that config YAML file is properly structured."""
        import yaml

        with open(regression_config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        # Check for required top-level keys
        assert 'validation_job' in config_data, "Config must have validation_job"
        assert 'files' in config_data, "Config must have files section"
        assert 'validations' in config_data, "Config must have validations section"

        # Check files section
        files = config_data['files']
        assert isinstance(files, list), "Files must be a list"
        assert len(files) > 0, "Must have at least one file"

        # Check validations section
        validations = config_data['validations']
        assert isinstance(validations, list), "Validations must be a list"
        assert len(validations) >= 25, f"Should have at least 25 validations, got {len(validations)}"

        print(f"\n✓ Config YAML properly structured:")
        print(f"  - Files: {len(files)}")
        print(f"  - Validations: {len(validations)}")

    def test_validation_types_coverage(self, regression_config_path):
        """Test that config covers all major validation categories."""
        import yaml

        with open(regression_config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        validation_types = [v['type'] for v in config_data['validations']]

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
        print(f"  - All major categories represented")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
