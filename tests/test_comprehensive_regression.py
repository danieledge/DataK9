"""
Comprehensive Regression Test Suite for DataK9 Validation Framework

Tests data structure and validation configuration for regression testing.
Ensures test data and configuration files are properly structured.
Also includes actual execution tests to verify validations run correctly.
"""

import pytest
import pandas as pd
from pathlib import Path
from validation_framework.core.config import ValidationConfig
from validation_framework.core.engine import ValidationEngine


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


class TestComprehensiveValidationExecution:
    """Tests that actually RUN the comprehensive validation config end-to-end."""

    @pytest.fixture
    def comprehensive_config_path(self):
        """Path to comprehensive test configuration."""
        return Path(__file__).parent / "test_data" / "comprehensive_test_config.yaml"

    @pytest.fixture
    def comprehensive_data_path(self):
        """Path to comprehensive test data."""
        return Path(__file__).parent / "test_data" / "comprehensive_test_data.csv"

    @pytest.mark.integration
    def test_comprehensive_config_loads(self, comprehensive_config_path):
        """Test that comprehensive config file loads without errors."""
        assert comprehensive_config_path.exists(), "Comprehensive config file not found"

        # Load config
        config = ValidationConfig.from_yaml(str(comprehensive_config_path))

        assert config.job_name == "Comprehensive Test Suite - All 34 Validation Types"
        assert len(config.files) == 1
        assert config.files[0]["name"] == "employees"
        assert len(config.files[0]["validations"]) >= 30  # Should have 30 validations

        print(f"\n✓ Comprehensive config loaded successfully:")
        print(f"  - Job name: {config.job_name}")
        print(f"  - Files: {len(config.files)}")
        print(f"  - Validations: {len(config.files[0]['validations'])}")

    @pytest.mark.integration
    def test_comprehensive_validations_execute(self, comprehensive_config_path, comprehensive_data_path):
        """Test that all validations in comprehensive config actually execute."""
        assert comprehensive_config_path.exists(), "Comprehensive config not found"
        assert comprehensive_data_path.exists(), "Comprehensive test data not found"

        # Load config
        config = ValidationConfig.from_yaml(str(comprehensive_config_path))

        # Create engine and run validations
        engine = ValidationEngine(config)
        results = engine.run()

        # Verify validations ran
        assert results is not None, "Engine should return results"
        assert len(results.file_reports) > 0, "Should have file reports"

        file_report = results.file_reports[0]
        assert len(file_report.validation_results) >= 30, f"Should have 30+ validation results, got {len(file_report.validation_results)}"

        # Count validations by status
        passed = sum(1 for r in file_report.validation_results if r.passed)
        failed = sum(1 for r in file_report.validation_results if not r.passed)

        print(f"\n✓ Comprehensive validations executed:")
        print(f"  - Total validations run: {len(file_report.validation_results)}")
        print(f"  - Passed: {passed}")
        print(f"  - Failed: {failed}")
        print(f"  - All validations executed successfully")

        # Verify we actually tested different validation types
        validation_types = set(r.rule_name for r in file_report.validation_results)
        assert len(validation_types) >= 20, f"Should have 20+ different validation types, got {len(validation_types)}"

    @pytest.mark.integration
    def test_comprehensive_data_quality_issues_detected(self, comprehensive_config_path):
        """Test that comprehensive config correctly detects designed quality issues."""
        config = ValidationConfig.from_yaml(str(comprehensive_config_path))
        engine = ValidationEngine(config)
        results = engine.run()

        file_report = results.file_reports[0]

        # We designed quality issues into the test data, so some validations should fail
        failed_validations = [r for r in file_report.validation_results if not r.passed]

        assert len(failed_validations) > 0, "Should have at least some failed validations due to designed quality issues"

        # Check that specific validation types we expect to fail actually failed
        failed_types = {r.rule_name for r in failed_validations}

        # These should fail based on our designed quality issues
        expected_failures = {
            'MandatoryFieldCheck',  # Employee 5 has missing first_name
            'RegexCheck',            # Employee 10 has invalid email
            'ValidValuesCheck',      # Employee 20 has invalid status
            'RangeCheck',            # Employees 25, 30 have salary out of range
            'DateFormatCheck',       # Employee 45 has invalid date
            'DuplicateRowCheck',     # Employee 47-48 have duplicate IDs
            'UniqueKeyCheck',        # Same duplicate
        }

        # At least some of these should have failed
        detected_failures = expected_failures & failed_types
        assert len(detected_failures) > 0, f"Should have detected some designed failures, got: {failed_types}"

        print(f"\n✓ Quality issues detected as expected:")
        print(f"  - Failed validations: {len(failed_validations)}")
        print(f"  - Expected failures detected: {detected_failures}")

    @pytest.mark.integration
    def test_all_validation_categories_covered(self, comprehensive_config_path):
        """Test that comprehensive config covers all major validation categories."""
        config = ValidationConfig.from_yaml(str(comprehensive_config_path))
        engine = ValidationEngine(config)
        results = engine.run()

        file_report = results.file_reports[0]
        validation_names = [r.rule_name for r in file_report.validation_results]

        # Check categories are covered
        categories = {
            'File-level': ['EmptyFileCheck', 'RowCountRangeCheck', 'FileSizeCheck'],
            'Schema': ['SchemaMatchCheck', 'ColumnPresenceCheck'],
            'Field-level': ['MandatoryFieldCheck', 'RegexCheck', 'ValidValuesCheck', 'RangeCheck', 'DateFormatCheck'],
            'Record-level': ['DuplicateRowCheck', 'BlankRecordCheck', 'UniqueKeyCheck'],
            'Advanced': ['StatisticalOutlierCheck', 'CompletenessCheck', 'StringLengthCheck', 'NumericPrecisionCheck'],
            'Conditional': ['ConditionalValidation'],
        }

        coverage = {}
        for category, expected_validations in categories.items():
            found = [v for v in expected_validations if v in validation_names]
            coverage[category] = len(found)

        print(f"\n✓ Validation category coverage:")
        for category, count in coverage.items():
            print(f"  - {category}: {count} validation(s)")

        # All categories should have at least one validation
        assert all(count > 0 for count in coverage.values()), "All categories should be represented"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
