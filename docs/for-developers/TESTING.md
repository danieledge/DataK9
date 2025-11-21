

# Testing Guide for Data Validation Framework

Comprehensive guide to running, writing, and maintaining tests for the data validation framework.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Test Organization](#test-organization)
- [Running Tests](#running-tests)
- [Test Coverage](#test-coverage)
- [Writing Tests](#writing-tests)
- [Test Fixtures](#test-fixtures)
- [CI/CD Integration](#cicd-integration)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### Running All Tests

```bash
# Interactive menu
./run_tests.sh

# Command line - all tests
./run_tests.sh --all

# With coverage report
./run_tests.sh --coverage
```

### Running Specific Test Suites

```bash
# Unit tests only (fast)
./run_tests.sh --unit

# Integration tests
./run_tests.sh --integration

# Security tests
./run_tests.sh --security

# CLI tests
./run_tests.sh --cli
```

---

## Test Organization

### Directory Structure

```
tests/
├── conftest.py                        # Shared fixtures and configuration
├── test_cli.py                        # CLI command tests (65 tests)
├── test_config.py                     # Configuration parsing tests
├── test_conditional_validations.py    # Conditional logic tests
├── test_cross_file_validations.py     # Cross-file validation tests
├── test_integration.py                # End-to-end integration tests
├── test_loaders.py                    # Data loader tests
├── test_profiler.py                   # Data profiling tests
├── test_registry.py                   # Validation registry tests
├── test_validations.py                # Basic validation tests
├── test_security.py                   # Security tests (SQL injection, DoS, etc.)
├── test_async_engine.py               # Async validation engine tests
├── test_advanced_validations.py       # Advanced validation rule tests
└── fixtures/                          # Test data files
    ├── csv/
    ├── json/
    ├── excel/
    └── cross_file/
```

### Test Categories

Tests are organized using pytest markers:

- **`@pytest.mark.unit`** - Fast, isolated unit tests
- **`@pytest.mark.integration`** - End-to-end workflow tests
- **`@pytest.mark.security`** - Security-focused tests
- **`@pytest.mark.cli`** - Command-line interface tests
- **`@pytest.mark.performance`** - Performance and load tests
- **`@pytest.mark.slow`** - Tests that take significant time

### Test Count by Module

| Module | Test File | Test Count | Coverage |
|--------|-----------|------------|----------|
| CLI | test_cli.py | 65 | 85%+ |
| Security | test_security.py | 40+ | N/A |
| Async Engine | test_async_engine.py | 25+ | 70%+ |
| Advanced Validations | test_advanced_validations.py | 45+ | 60%+ |
| Loaders | test_loaders.py | 30+ | 80%+ |
| Configuration | test_config.py | 25+ | 90%+ |
| Integration | test_integration.py | 15+ | N/A |
| Profiler | test_profiler.py | 49 | 70%+ |
| **TOTAL** | **9 files** | **294+** | **70%+** |

---

## Running Tests

### Using the Test Runner Script

The `run_tests.sh` script provides an interactive menu and command-line options:

#### Interactive Mode

```bash
./run_tests.sh
```

**Menu Options:**
1. Run All Tests
2. Run Unit Tests Only
3. Run Integration Tests Only
4. Run Security Tests Only
5. Run CLI Tests Only
6. Run Performance Tests Only
7. Run Fast Tests (Skip Slow)
8. Run with Coverage Report
9. Run Specific Test File
10. Run Tests in Parallel
11. View Test Statistics
12. Clean Test Artifacts

#### Command Line Options

```bash
# All tests
./run_tests.sh --all

# By category
./run_tests.sh --unit
./run_tests.sh --integration
./run_tests.sh --security
./run_tests.sh --cli
./run_tests.sh --performance

# Fast tests (skip slow)
./run_tests.sh --fast

# With coverage
./run_tests.sh --coverage

# Specific file
./run_tests.sh --file tests/test_cli.py

# Parallel execution
./run_tests.sh --parallel

# Help
./run_tests.sh --help
```

### Using pytest Directly

```bash
# All tests
python3 -m pytest tests/

# Specific test file
python3 -m pytest tests/test_cli.py

# Specific test class
python3 -m pytest tests/test_cli.py::TestValidateCommand

# Specific test function
python3 -m pytest tests/test_cli.py::TestValidateCommand::test_validate_basic_success

# By marker
python3 -m pytest -m unit
python3 -m pytest -m "not slow"

# With verbose output
python3 -m pytest -v tests/

# With coverage
python3 -m pytest --cov=validation_framework --cov-report=html tests/

# Stop on first failure
python3 -m pytest -x tests/

# Show local variables on failure
python3 -m pytest -l tests/

# Parallel execution (requires pytest-xdist)
python3 -m pytest -n auto tests/
```

---

## Test Coverage

### Current Coverage Status

**Overall Coverage: 70%+** (Target: >70%)

#### Coverage by Module

| Module | Coverage | Status | Notes |
|--------|----------|--------|-------|
| CLI | 85%+ | ✅ Excellent | Was 0%, now comprehensive |
| Loaders | 80%+ | ✅ Good | CSV, JSON, Excel, Parquet |
| Configuration | 90%+ | ✅ Excellent | Parser and validation |
| Core Engine | 75%+ | ✅ Good | Sync and async |
| Validations (basic) | 70%+ | ✅ Good | Field, record, file checks |
| Validations (advanced) | 65%+ | ⚠️ Fair | Statistical, temporal |
| Security Utils | 60%+ | ⚠️ Fair | SQL injection prevention |
| Profiler | 70%+ | ✅ Good | Data analysis engine |
| Reporters | 75%+ | ✅ Good | HTML and JSON |

### Viewing Coverage Reports

#### Terminal Report

```bash
python3 -m pytest --cov=validation_framework --cov-report=term-missing tests/
```

#### HTML Report

```bash
python3 -m pytest --cov=validation_framework --cov-report=html tests/
# Open htmlcov/index.html in browser
xdg-open htmlcov/index.html
```

#### Coverage with Threshold

```bash
# Fail if coverage < 70%
python3 -m pytest --cov=validation_framework --cov-fail-under=70 tests/
```

### Improving Coverage

To identify uncovered code:

```bash
# Show missing lines
python3 -m pytest --cov=validation_framework --cov-report=term-missing tests/

# Focus on specific module
python3 -m pytest --cov=validation_framework.cli --cov-report=term-missing tests/test_cli.py
```

---

## Writing Tests

### Test Structure

Follow this standard structure for all tests:

```python
"""
Module docstring describing what is being tested.

Test organization and purpose.
"""

import pytest
import pandas as pd
from pathlib import Path

from validation_framework.core.xyz import ComponentUnderTest


@pytest.mark.unit  # Add appropriate markers
class TestComponentName:
    """Test suite for ComponentName."""

    def test_specific_functionality(self, fixture_name):
        """
        Test description explaining what is being validated.

        Clear explanation of test scenario, expected behavior,
        and any important context.
        """
        # Arrange - Set up test data and conditions
        test_input = "example"
        expected_output = "EXAMPLE"

        # Act - Execute the code under test
        actual_output = ComponentUnderTest.process(test_input)

        # Assert - Verify the results
        assert actual_output == expected_output
        assert len(actual_output) > 0

    def test_error_handling(self):
        """Test that errors are handled correctly."""
        with pytest.raises(ValueError) as exc_info:
            ComponentUnderTest.process(invalid_input)

        assert "expected error message" in str(exc_info.value)
```

### Best Practices

#### 1. Use Descriptive Names

```python
# Good
def test_mandatory_field_check_fails_when_values_missing(self):

# Bad
def test_check1(self):
```

#### 2. Test One Thing Per Test

```python
# Good - focused test
def test_csv_loader_reads_data(self):
    loader = CSVLoader("data.csv")
    data = next(loader.load())
    assert len(data) > 0

# Bad - testing multiple things
def test_csv_loader(self):
    loader = CSVLoader("data.csv")
    data = next(loader.load())
    assert len(data) > 0
    assert "column1" in data.columns
    metadata = loader.get_metadata()
    assert metadata["size"] > 0
    # ... too many assertions
```

#### 3. Use Fixtures for Common Setup

```python
@pytest.fixture
def sample_dataframe():
    """Create standard test DataFrame."""
    return pd.DataFrame({
        "id": [1, 2, 3],
        "value": [100, 200, 300]
    })

def test_with_fixture(sample_dataframe):
    assert len(sample_dataframe) == 3
```

#### 4. Test Edge Cases

```python
def test_empty_input(self):
    """Test handling of empty input."""
    result = validate([])
    assert result.passed is True

def test_null_values(self):
    """Test handling of null values."""
    data = pd.DataFrame({"col": [None, None]})
    result = validate(data)
    # Assert expected behavior

def test_very_large_input(self):
    """Test handling of large datasets."""
    large_data = create_large_dataset(100000)
    result = validate(large_data)
    # Assert completes without memory issues
```

#### 5. Use Parametrize for Multiple Inputs

```python
@pytest.mark.parametrize("input,expected", [
    ("test@example.com", True),
    ("invalid", False),
    ("", False),
    (None, False),
])
def test_email_validation(input, expected):
    result = is_valid_email(input)
    assert result == expected
```

### Testing Async Code

For async functions, use `@pytest.mark.asyncio`:

```python
import pytest

@pytest.mark.asyncio
async def test_async_loader():
    """Test async data loader."""
    loader = AsyncCSVLoader("data.csv")

    chunks = []
    async for chunk in loader.load():
        chunks.append(chunk)

    assert len(chunks) > 0
```

### Testing CLI Commands

Use Click's test runner:

```python
from click.testing import CliRunner
from validation_framework.cli import cli

def test_cli_command():
    """Test CLI command."""
    runner = CliRunner()
    result = runner.invoke(cli, ['validate', 'config.yaml'])

    assert result.exit_code == 0
    assert "PASSED" in result.output
```

---

## Test Fixtures

### Available Fixtures

All fixtures are defined in `tests/conftest.py`:

#### Data Fixtures

```python
# Standard sample data with mixed types
def test_with_sample_data(sample_dataframe):
    assert len(sample_dataframe) == 5

# Clean data (no nulls, no errors)
def test_with_clean_data(clean_dataframe):
    assert clean_dataframe.isnull().sum().sum() == 0

# Large dataset (10,000 rows)
def test_with_large_data(large_dataframe):
    assert len(large_dataframe) == 10000

# Data with duplicates
def test_duplicates(dataframe_with_duplicates):
    duplicates = dataframe_with_duplicates[dataframe_with_duplicates.duplicated()]
    assert len(duplicates) > 0

# Data with outliers
def test_outliers(dataframe_with_outliers):
    assert dataframe_with_outliers['value'].max() > 200
```

#### File Fixtures

```python
# Temporary CSV file (auto-cleanup)
def test_csv_file(temp_csv_file):
    assert Path(temp_csv_file).exists()

# Large CSV file
def test_large_csv(temp_large_csv_file):
    size = Path(temp_large_csv_file).stat().st_size
    assert size > 100000  # > 100KB

# Empty file (0 bytes)
def test_empty(temp_empty_file):
    assert Path(temp_empty_file).stat().st_size == 0

# JSON file
def test_json(temp_json_file):
    assert temp_json_file.endswith('.json')

# Excel file
def test_excel(temp_excel_file):
    assert temp_excel_file.endswith('.xlsx')

# Parquet file
def test_parquet(temp_parquet_file):
    assert temp_parquet_file.endswith('.parquet')
```

#### Configuration Fixtures

```python
# Valid complete configuration
def test_config(valid_config_dict):
    config = ValidationConfig(valid_config_dict)
    assert config.job_name == "Test Validation Job"

# Minimal configuration
def test_minimal_config(minimal_config_dict):
    config = ValidationConfig(minimal_config_dict)
    assert len(config.files) == 1

# Configuration file
def test_config_file(temp_config_file):
    config = ValidationConfig.from_yaml(temp_config_file)
    assert config is not None
```

#### Database Fixtures

```python
# SQLite database with sample data
def test_database(temp_sqlite_db):
    conn_string, conn = temp_sqlite_db
    cursor = conn.cursor()
    result = cursor.execute("SELECT COUNT(*) FROM customers").fetchone()
    assert result[0] == 5
```

### Creating Custom Fixtures

Add new fixtures to `tests/conftest.py`:

```python
@pytest.fixture
def my_custom_fixture():
    """
    Description of fixture purpose.

    Returns:
        type: Description of return value
    """
    # Setup
    resource = create_resource()

    yield resource

    # Teardown (optional)
    cleanup_resource(resource)
```

---

## CI/CD Integration

### Running Tests in CI/CD Pipelines

#### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-cov pytest-asyncio

    - name: Run tests with coverage
      run: |
        ./run_tests.sh --coverage

    - name: Upload coverage reports
      uses: codecov/codecov-action@v2
      with:
        file: ./coverage.xml
```

#### GitLab CI

```yaml
test:
  stage: test
  image: python:3.9
  script:
    - pip install -r requirements.txt
    - pip install pytest pytest-cov pytest-asyncio
    - ./run_tests.sh --coverage
  coverage: '/TOTAL.*\s+(\d+%)$/'
  artifacts:
    reports:
      coverage_report:
        coverage_format: cobertura
        path: coverage.xml
```

#### Jenkins

```groovy
pipeline {
    agent any

    stages {
        stage('Test') {
            steps {
                sh 'pip install -r requirements.txt'
                sh 'pip install pytest pytest-cov pytest-asyncio'
                sh './run_tests.sh --coverage'
            }
        }
    }

    post {
        always {
            junit 'test-results/*.xml'
            publishHTML([
                reportDir: 'htmlcov',
                reportFiles: 'index.html',
                reportName: 'Coverage Report'
            ])
        }
    }
}
```

### Exit Codes

The test runner uses standard exit codes for CI/CD integration:

- **0** - All tests passed
- **1** - One or more tests failed
- **2** - Error in test collection or execution
- **3** - Internal error
- **4** - pytest usage error
- **5** - No tests collected

### Coverage Enforcement

Set minimum coverage threshold:

```bash
# In pytest.ini
[pytest]
addopts = --cov-fail-under=70

# Or command line
pytest --cov-fail-under=70 tests/
```

---

## Troubleshooting

### Common Issues

#### 1. Import Errors

**Problem:**
```
ModuleNotFoundError: No module named 'validation_framework'
```

**Solution:**
```bash
# Install package in editable mode
pip install -e .

# Or add to PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/path/to/data-validation-tool"
```

#### 2. Fixture Not Found

**Problem:**
```
E       fixture 'sample_dataframe' not found
```

**Solution:**
Ensure `conftest.py` is in the tests directory and fixtures are properly defined.

#### 3. Async Test Failures

**Problem:**
```
RuntimeError: no running event loop
```

**Solution:**
```bash
# Install pytest-asyncio
pip install pytest-asyncio

# Use @pytest.mark.asyncio decorator
@pytest.mark.asyncio
async def test_async_function():
    ...
```

#### 4. Coverage Not Generating

**Problem:**
Coverage reports are empty or missing.

**Solution:**
```bash
# Install coverage packages
pip install pytest-cov coverage

# Verify coverage is running
pytest --cov=validation_framework --cov-report=term tests/
```

#### 5. Tests Hanging

**Problem:**
Tests hang indefinitely.

**Solution:**
```bash
# Add timeout
pytest --timeout=30 tests/

# Identify slow tests
pytest --durations=10 tests/
```

#### 6. Database Fixture Errors

**Problem:**
SQLite database errors in tests.

**Solution:**
Ensure proper cleanup in fixtures and check file permissions.

### Debugging Tests

#### Verbose Output

```bash
# Maximum verbosity
pytest -vv tests/

# Show print statements
pytest -s tests/

# Show local variables on failure
pytest -l tests/
```

#### Debug Specific Test

```bash
# Drop into debugger on failure
pytest --pdb tests/test_file.py::test_function

# Drop into debugger at start
pytest --trace tests/test_file.py::test_function
```

#### Capture Warnings

```bash
# Show warnings
pytest -W all tests/

# Treat warnings as errors
pytest -W error tests/
```

### Performance Issues

#### Identify Slow Tests

```bash
# Show 10 slowest tests
pytest --durations=10 tests/

# Profile test execution
pytest --profile tests/
```

#### Speed Up Tests

```bash
# Skip slow tests
pytest -m "not slow" tests/

# Run in parallel
pytest -n auto tests/
```

---

## Best Practices Summary

### DO:

✅ Write clear, descriptive test names
✅ Test one thing per test function
✅ Use fixtures for common setup
✅ Test edge cases and error conditions
✅ Add docstrings to tests
✅ Use appropriate markers (@pytest.mark.unit, etc.)
✅ Keep tests independent and isolated
✅ Clean up resources in fixtures
✅ Maintain >70% code coverage
✅ Run tests before committing

### DON'T:

❌ Test implementation details
❌ Create interdependent tests
❌ Use hard-coded paths (use fixtures)
❌ Skip cleanup (causes test pollution)
❌ Ignore failing tests
❌ Write tests without assertions
❌ Mix unit and integration tests
❌ Leave debug code in tests

---

## Resources

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-cov Documentation](https://pytest-cov.readthedocs.io/)
- [pytest-asyncio Documentation](https://pytest-asyncio.readthedocs.io/)
- [Testing Best Practices](https://docs.python-guide.org/writing/tests/)

---

## Getting Help

If you encounter issues with tests:

1. Check this guide for common solutions
2. Review test output for error messages
3. Run tests with verbose output (`-vv`)
4. Check fixture definitions in `conftest.py`
5. Verify all dependencies are installed
6. Review recent code changes

For persistent issues, provide:
- Full error message
- Test command used
- Python version
- Installed package versions

---

**Last Updated:** 2025-11-15
**Framework Version:** 0.1.0
**Test Coverage:** 70%+
