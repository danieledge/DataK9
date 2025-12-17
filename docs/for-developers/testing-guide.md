# Testing Guide

**Comprehensive Testing Strategies for DataK9**

Quality testing is critical for a data quality framework. This guide covers testing strategies, patterns, and best practices for DataK9 components - from unit tests to integration tests to end-to-end validation scenarios.

---

## Table of Contents

1. [Testing Philosophy](#testing-philosophy)
2. [Test Setup](#test-setup)
3. [Unit Testing](#unit-testing)
4. [Integration Testing](#integration-testing)
5. [End-to-End Testing](#end-to-end-testing)
6. [Testing Custom Components](#testing-custom-components)
7. [Test Data Management](#test-data-management)
8. [Coverage and Quality](#coverage-and-quality)
9. [CI/CD Integration](#cicd-integration)
10. [Best Practices](#best-practices)
11. [Test Inventory](#test-inventory)

---

## Testing Philosophy

### DataK9's Testing Approach

**DataK9 is a data quality framework - quality must be built-in, not bolted-on.**

Our testing strategy follows these principles:

1. **Trust Through Testing** - Users trust DataK9 to guard their data. We earn that trust through comprehensive tests.

2. **Fail Fast** - Tests catch bugs before they reach production. Better to fail in CI than in a customer's pipeline.

3. **Test Pyramid** - Many unit tests (fast), fewer integration tests (medium), some E2E tests (slow).

4. **Real-World Scenarios** - Test with realistic data and configurations, not just happy paths.

5. **Documentation Through Tests** - Tests serve as living documentation of how components work.

### Current Test Coverage

```
DataK9 Test Suite (as of v1.53):
- 115+ tests
- 48% code coverage
- All core validations tested
- Integration tests for full workflows
- Performance benchmarks included
```

**Goal:** 80%+ coverage for production code.

---

## Test Setup

### Installing Test Dependencies

```bash
# Install test requirements
pip install -r requirements-dev.txt

# Should include:
# - pytest>=7.0.0
# - pytest-cov>=3.0.0
# - pytest-mock>=3.6.0
# - pytest-xdist>=2.5.0 (parallel execution)
```

### Project Test Structure

```
data-validation-tool/
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Shared fixtures
│   ├── fixtures/                    # Test data files
│   │   ├── sample.csv
│   │   ├── sample.json
│   │   └── sample.parquet
│   ├── unit/                        # Unit tests
│   │   ├── test_validations/
│   │   │   ├── test_field_checks.py
│   │   │   ├── test_file_checks.py
│   │   │   └── test_record_checks.py
│   │   ├── test_loaders/
│   │   │   ├── test_csv_loader.py
│   │   │   └── test_json_loader.py
│   │   └── test_reporters/
│   │       ├── test_html_reporter.py
│   │       └── test_json_reporter.py
│   ├── integration/                 # Integration tests
│   │   ├── test_engine.py
│   │   ├── test_full_workflow.py
│   │   └── test_cross_file.py
│   └── e2e/                         # End-to-end tests
│       ├── test_cli.py
│       └── test_real_world_scenarios.py
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_validations/test_field_checks.py

# Run tests matching pattern
pytest -k "test_regex"

# Run with coverage
pytest --cov=validation_framework --cov-report=html

# Run in parallel (faster)
pytest -n auto

# Verbose output
pytest -v

# Stop on first failure
pytest -x

# Show print statements
pytest -s
```

---

## Unit Testing

Unit tests verify individual components in isolation.

### Testing Validations

**Template for validation tests:**

```python
"""
Unit tests for MandatoryFieldCheck validation.

Tests:
- Validation passes with all fields present
- Validation fails with missing fields
- Conditional validation works correctly
- Edge cases (empty data, missing columns, etc.)
"""
import pytest
import pandas as pd
from validation_framework.validations.field_checks import MandatoryFieldCheck
from validation_framework.core.results import Severity


@pytest.fixture
def valid_data():
    """Sample data with all mandatory fields"""
    return pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'email': ['alice@example.com', 'bob@example.com',
                  'charlie@example.com', 'diana@example.com', 'eve@example.com'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE']
    })


@pytest.fixture
def data_with_nulls():
    """Sample data with some null values"""
    return pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5],
        'name': ['Alice', None, 'Charlie', 'Diana', None],
        'email': ['alice@example.com', 'bob@example.com', None,
                  'diana@example.com', 'eve@example.com'],
        'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE', 'ACTIVE']
    })


class TestMandatoryFieldCheck:
    """Test suite for MandatoryFieldCheck"""

    def test_passes_with_all_fields_present(self, valid_data):
        """Test validation passes when all mandatory fields are present"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['customer_id', 'name', 'email']}
        )

        def data_iter():
            yield valid_data

        context = {'max_sample_failures': 100}
        result = validation.validate(data_iter(), context)

        assert result.passed is True
        assert result.failed_count == 0
        assert "present" in result.message.lower()

    def test_fails_with_missing_values(self, data_with_nulls):
        """Test validation fails when mandatory fields have nulls"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['name', 'email']}
        )

        def data_iter():
            yield data_with_nulls

        context = {'max_sample_failures': 100}
        result = validation.validate(data_iter(), context)

        assert result.passed is False
        assert result.failed_count == 3  # 2 null names + 1 null email
        assert result.total_count == 5
        assert len(result.sample_failures) == 3

    def test_with_conditional_filter(self, data_with_nulls):
        """Test validation with condition - only check ACTIVE records"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields (Active Only)",
            severity=Severity.ERROR,
            params={'fields': ['name']},
            condition="status == 'ACTIVE'"
        )

        def data_iter():
            yield data_with_nulls

        context = {'max_sample_failures': 100}
        result = validation.validate(data_iter(), context)

        # Only 2 null names in ACTIVE records (rows 1 and 4)
        assert result.passed is False
        assert result.failed_count == 2

    def test_with_missing_column(self, valid_data):
        """Test validation fails gracefully when column doesn't exist"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['nonexistent_field']}
        )

        def data_iter():
            yield valid_data

        context = {'max_sample_failures': 100}
        result = validation.validate(data_iter(), context)

        assert result.passed is False
        assert "not found" in result.message.lower()

    def test_with_empty_dataframe(self):
        """Test validation handles empty DataFrame"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['name', 'email']}
        )

        empty_df = pd.DataFrame(columns=['name', 'email'])

        def data_iter():
            yield empty_df

        context = {'max_sample_failures': 100}
        result = validation.validate(data_iter(), context)

        # Empty data should pass (no nulls found)
        assert result.passed is True
        assert result.total_count == 0

    def test_chunked_data_processing(self, valid_data):
        """Test validation works correctly across multiple chunks"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['name']}
        )

        # Split data into chunks
        def data_iter():
            yield valid_data.iloc[0:2]
            yield valid_data.iloc[2:4]
            yield valid_data.iloc[4:5]

        context = {'max_sample_failures': 100}
        result = validation.validate(data_iter(), context)

        assert result.passed is True
        assert result.total_count == 5  # All rows processed

    def test_sample_failures_limited(self):
        """Test that sample failures are limited to max_sample_failures"""
        # Create data with many nulls
        data_with_many_nulls = pd.DataFrame({
            'id': range(1000),
            'name': [None] * 1000  # All nulls
        })

        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['name']}
        )

        def data_iter():
            yield data_with_many_nulls

        context = {'max_sample_failures': 10}  # Limit to 10
        result = validation.validate(data_iter(), context)

        assert result.failed_count == 1000
        assert len(result.sample_failures) == 10  # Limited to 10

    def test_get_description(self):
        """Test get_description returns meaningful text"""
        validation = MandatoryFieldCheck(
            name="Mandatory Fields",
            severity=Severity.ERROR,
            params={'fields': ['id', 'name', 'email']}
        )

        description = validation.get_description()

        assert 'mandatory' in description.lower() or 'required' in description.lower()
        assert 'id' in description
        assert 'name' in description
        assert 'email' in description
```

### Testing Loaders

**Template for loader tests:**

```python
"""
Unit tests for CSVLoader.

Tests:
- Loads CSV files correctly
- Handles chunking properly
- Supports custom delimiters
- Handles encoding correctly
- Gets metadata accurately
"""
import pytest
from pathlib import Path
import pandas as pd
from validation_framework.loaders.csv_loader import CSVLoader


@pytest.fixture
def sample_csv(tmp_path):
    """Create sample CSV file for testing"""
    csv_path = tmp_path / "sample.csv"

    csv_content = """customer_id,name,email,age
1,Alice,alice@example.com,32
2,Bob,bob@example.com,28
3,Charlie,charlie@example.com,45
4,Diana,diana@example.com,29
5,Eve,eve@example.com,35"""

    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def pipe_delimited_csv(tmp_path):
    """Create pipe-delimited CSV file"""
    csv_path = tmp_path / "pipe.csv"

    csv_content = """customer_id|name|email
1|Alice|alice@example.com
2|Bob|bob@example.com"""

    csv_path.write_text(csv_content)
    return csv_path


class TestCSVLoader:
    """Test suite for CSVLoader"""

    def test_loads_csv_file(self, sample_csv):
        """Test basic CSV file loading"""
        loader = CSVLoader(file_path=str(sample_csv))

        chunks = list(loader.load())

        assert len(chunks) >= 1
        assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)

        # Combine all chunks
        df = pd.concat(chunks, ignore_index=True)

        assert len(df) == 5
        assert list(df.columns) == ['customer_id', 'name', 'email', 'age']
        assert df.loc[0, 'name'] == 'Alice'

    def test_chunking_works(self, tmp_path):
        """Test data is yielded in correct chunk sizes"""
        # Create large CSV
        csv_path = tmp_path / "large.csv"

        lines = ['id,value\n']
        for i in range(100):
            lines.append(f'{i},value{i}\n')

        csv_path.write_text(''.join(lines))

        # Load with small chunk size
        loader = CSVLoader(file_path=str(csv_path), chunk_size=25)

        chunks = list(loader.load())

        # Should have 4 chunks (100 rows / 25 chunk_size)
        assert len(chunks) == 4

        # Each chunk (except possibly last) should have chunk_size rows
        assert len(chunks[0]) == 25
        assert len(chunks[1]) == 25
        assert len(chunks[2]) == 25
        assert len(chunks[3]) == 25

    def test_custom_delimiter(self, pipe_delimited_csv):
        """Test custom delimiter support"""
        loader = CSVLoader(
            file_path=str(pipe_delimited_csv),
            delimiter='|'
        )

        chunks = list(loader.load())
        df = pd.concat(chunks, ignore_index=True)

        assert len(df) == 2
        assert list(df.columns) == ['customer_id', 'name', 'email']

    def test_gets_metadata(self, sample_csv):
        """Test get_metadata returns correct information"""
        loader = CSVLoader(file_path=str(sample_csv))

        metadata = loader.get_metadata()

        assert metadata['row_count'] == 5
        assert metadata['column_count'] == 4
        assert set(metadata['columns']) == {'customer_id', 'name', 'email', 'age'}
        assert metadata['file_format'] == 'csv'

    def test_empty_file_handling(self, tmp_path):
        """Test handling of empty CSV file"""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("id,name\n")  # Header only

        loader = CSVLoader(file_path=str(csv_path))

        chunks = list(loader.load())

        # Should yield empty DataFrame or no chunks
        if chunks:
            assert len(chunks[0]) == 0
        else:
            assert len(chunks) == 0

    def test_file_not_found(self):
        """Test error when file doesn't exist"""
        with pytest.raises(FileNotFoundError):
            loader = CSVLoader(file_path="/nonexistent/file.csv")

    def test_encoding_support(self, tmp_path):
        """Test different encoding support"""
        csv_path = tmp_path / "utf8.csv"

        # Write with UTF-8 encoding (special characters)
        content = "id,name\n1,Café\n2,Naïve"
        csv_path.write_text(content, encoding='utf-8')

        loader = CSVLoader(file_path=str(csv_path), encoding='utf-8')

        chunks = list(loader.load())
        df = pd.concat(chunks, ignore_index=True)

        assert df.loc[0, 'name'] == 'Café'
        assert df.loc[1, 'name'] == 'Naïve'
```

### Testing Reporters

```python
"""
Unit tests for JSONReporter.

Tests:
- Generates valid JSON
- Contains all required fields
- Handles missing data gracefully
"""
import pytest
import json
from pathlib import Path
from datetime import datetime
from validation_framework.reporters.json_reporter import JSONReporter
from validation_framework.core.results import (
    ValidationReport,
    FileValidationReport,
    ValidationResult,
    Status,
    Severity
)


@pytest.fixture
def sample_report():
    """Create sample validation report"""
    validation1 = ValidationResult(
        rule_name="Email Check",
        severity=Severity.ERROR,
        passed=True,
        message="All emails valid",
        failed_count=0,
        total_count=100,
        execution_time=1.5
    )

    validation2 = ValidationResult(
        rule_name="Age Range Check",
        severity=Severity.WARNING,
        passed=False,
        message="5 ages out of range",
        failed_count=5,
        total_count=100,
        sample_failures=[{"row": 42, "age": 150}],
        execution_time=0.8
    )

    file_report = FileValidationReport(
        file_name="test.csv",
        file_path="/data/test.csv",
        file_format="csv",
        status=Status.WARNING,
        validation_results=[validation1, validation2],
        metadata={'row_count': 100},
        execution_time=2.3
    )

    return ValidationReport(
        job_name="Test Job",
        description="Test validation",
        execution_time=datetime(2024, 1, 15, 14, 30),
        duration_seconds=2.5,
        overall_status=Status.WARNING,
        config={},
        file_reports=[file_report]
    )


class TestJSONReporter:
    """Test suite for JSONReporter"""

    def test_generates_valid_json(self, sample_report, tmp_path):
        """Test that generated JSON is valid and parseable"""
        output_path = tmp_path / "report.json"

        reporter = JSONReporter()
        reporter.generate(sample_report, str(output_path))

        # Should create file
        assert output_path.exists()

        # Should be valid JSON
        with open(output_path) as f:
            data = json.load(f)

        assert isinstance(data, dict)

    def test_contains_required_fields(self, sample_report, tmp_path):
        """Test JSON contains all required fields"""
        output_path = tmp_path / "report.json"

        reporter = JSONReporter()
        reporter.generate(sample_report, str(output_path))

        with open(output_path) as f:
            data = json.load(f)

        # Top-level fields
        assert 'job_name' in data
        assert 'status' in data
        assert 'execution_time' in data
        assert 'duration_seconds' in data
        assert 'errors' in data
        assert 'warnings' in data
        assert 'files' in data

        # File-level fields
        assert len(data['files']) == 1
        file_data = data['files'][0]

        assert 'file_name' in file_data
        assert 'status' in file_data
        assert 'validations' in file_data

        # Validation-level fields
        assert len(file_data['validations']) == 2
        validation_data = file_data['validations'][0]

        assert 'rule_name' in validation_data
        assert 'severity' in validation_data
        assert 'passed' in validation_data
        assert 'message' in validation_data

    def test_empty_report(self, tmp_path):
        """Test handling of report with no files"""
        output_path = tmp_path / "empty.json"

        report = ValidationReport(
            job_name="Empty Job",
            description="No files",
            execution_time=datetime.now(),
            duration_seconds=0.1,
            overall_status=Status.PASSED,
            config={},
            file_reports=[]
        )

        reporter = JSONReporter()
        reporter.generate(report, str(output_path))

        with open(output_path) as f:
            data = json.load(f)

        assert data['job_name'] == "Empty Job"
        assert data['files'] == []
```

---

## Integration Testing

Integration tests verify components work together correctly.

### Testing Full Workflows

```python
"""
Integration tests for complete validation workflows.

Tests end-to-end validation: config → loader → validation → report
"""
import pytest
import yaml
from pathlib import Path
from validation_framework.core.engine import ValidationEngine


class TestFullWorkflow:
    """Integration tests for complete validation workflows"""

    def test_csv_validation_workflow(self, tmp_path):
        """Test complete workflow: CSV file → validation → HTML report"""

        # Create test data file
        data_file = tmp_path / "customers.csv"
        data_file.write_text("""customer_id,name,email,age
1,Alice,alice@example.com,32
2,Bob,bob@example.com,28
3,Charlie,charlie@example.com,45
4,Diana,diana@example.com,29
5,Eve,eve@example.com,35""")

        # Create configuration
        config = {
            'validation_job': {
                'name': 'Customer Validation',
                'version': '1.0'
            },
            'settings': {
                'chunk_size': 1000,
                'max_sample_failures': 100
            },
            'files': [
                {
                    'name': 'customers',
                    'path': str(data_file),
                    'format': 'csv',
                    'validations': [
                        {
                            'type': 'EmptyFileCheck',
                            'severity': 'ERROR'
                        },
                        {
                            'type': 'MandatoryFieldCheck',
                            'severity': 'ERROR',
                            'params': {
                                'fields': ['customer_id', 'email']
                            }
                        },
                        {
                            'type': 'RangeCheck',
                            'severity': 'WARNING',
                            'params': {
                                'field': 'age',
                                'min_value': 18,
                                'max_value': 120
                            }
                        }
                    ]
                }
            ]
        }

        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))

        # Run validation
        engine = ValidationEngine.from_config(str(config_file))
        report = engine.run(verbose=False)

        # Verify results
        assert report.overall_status.value == "PASSED"
        assert len(report.file_reports) == 1

        file_report = report.file_reports[0]
        assert file_report.file_name == "customers"
        assert len(file_report.validation_results) == 3
        assert all(v.passed for v in file_report.validation_results)

    def test_validation_failure_workflow(self, tmp_path):
        """Test workflow when validation fails"""

        # Create data with issues
        data_file = tmp_path / "bad_data.csv"
        data_file.write_text("""customer_id,name,email
1,Alice,alice@example.com
2,Bob,invalid-email
3,Charlie,""")  # Missing email

        config = {
            'validation_job': {
                'name': 'Bad Data Test'
            },
            'files': [
                {
                    'name': 'bad_data',
                    'path': str(data_file),
                    'format': 'csv',
                    'validations': [
                        {
                            'type': 'RegexCheck',
                            'severity': 'ERROR',
                            'params': {
                                'field': 'email',
                                'pattern': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
                            }
                        }
                    ]
                }
            ]
        }

        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))

        # Run validation
        engine = ValidationEngine.from_config(str(config_file))
        report = engine.run(verbose=False)

        # Should fail
        assert report.overall_status.value == "FAILED"
        assert report.total_errors > 0

        # Check failure details
        file_report = report.file_reports[0]
        regex_result = file_report.validation_results[0]

        assert not regex_result.passed
        assert regex_result.failed_count >= 2  # At least 2 bad emails
        assert len(regex_result.sample_failures) >= 2
```

---

## End-to-End Testing

E2E tests verify the complete system from CLI to reports.

### Testing CLI

```python
"""
End-to-end tests for CLI interface.

Tests command-line usage, exit codes, and output files.
"""
import pytest
import subprocess
from pathlib import Path


class TestCLI:
    """E2E tests for DataK9 CLI"""

    def test_cli_validate_command_success(self, tmp_path):
        """Test CLI validate command with passing validation"""

        # Create test file and config...
        # (setup code similar to integration test)

        # Run CLI command
        result = subprocess.run(
            ['python3', '-m', 'validation_framework.cli',
             'validate', str(config_file)],
            capture_output=True,
            text=True
        )

        # Should exit 0 (success)
        assert result.returncode == 0
        assert "PASSED" in result.stdout or "passed" in result.stdout.lower()

    def test_cli_validate_command_failure(self, tmp_path):
        """Test CLI validate command with failing validation"""

        # Create bad data and config...

        # Run CLI command
        result = subprocess.run(
            ['python3', '-m', 'validation_framework.cli',
             'validate', str(config_file)],
            capture_output=True,
            text=True
        )

        # Should exit 1 (validation failed)
        assert result.returncode == 1
        assert "FAILED" in result.stdout or "failed" in result.stdout.lower()

    def test_cli_list_validations(self):
        """Test CLI list-validations command"""

        result = subprocess.run(
            ['python3', '-m', 'validation_framework.cli',
             'list-validations'],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0
        assert "EmptyFileCheck" in result.stdout
        assert "RegexCheck" in result.stdout
        assert "MandatoryFieldCheck" in result.stdout
```

---

## Testing Custom Components

### Parameterized Tests

Use `pytest.mark.parametrize` for testing multiple scenarios:

```python
import pytest


class TestRegexCheck:
    """Test RegexCheck with multiple patterns"""

    @pytest.mark.parametrize("email,should_pass", [
        ("alice@example.com", True),
        ("bob.smith@company.co.uk", True),
        ("invalid-email", False),
        ("missing@domain", False),
        ("@example.com", False),
        ("user@", False),
    ])
    def test_email_validation(self, email, should_pass):
        """Test email regex with various inputs"""

        data = pd.DataFrame({'email': [email]})

        validation = RegexCheck(
            name="Email Check",
            severity=Severity.ERROR,
            params={
                'field': 'email',
                'pattern': '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
            }
        )

        def data_iter():
            yield data

        result = validation.validate(data_iter(), {})

        assert result.passed == should_pass
```

---

## Test Data Management

### Using Fixtures

```python
@pytest.fixture(scope="session")
def sample_customer_data():
    """Reusable customer data for tests"""
    return pd.DataFrame({
        'customer_id': range(1, 101),
        'name': [f'Customer {i}' for i in range(1, 101)],
        'email': [f'customer{i}@example.com' for i in range(1, 101)],
        'age': [random.randint(18, 75) for _ in range(100)],
        'status': random.choices(['ACTIVE', 'INACTIVE'], k=100)
    })


@pytest.fixture
def tmp_csv_file(tmp_path, sample_customer_data):
    """Create temporary CSV file from DataFrame"""
    csv_path = tmp_path / "data.csv"
    sample_customer_data.to_csv(csv_path, index=False)
    return csv_path
```

### Test Data in Files

Keep test data files in `tests/fixtures/`:

```
tests/fixtures/
├── sample_clean.csv       # Valid data
├── sample_dirty.csv       # Data with quality issues
├── sample_empty.csv       # Empty file
└── sample_large.parquet   # Large file for performance testing
```

---

## Coverage and Quality

### Measuring Coverage

```bash
# Generate coverage report
pytest --cov=validation_framework --cov-report=html

# View report
open htmlcov/index.html

# Show missing lines
pytest --cov=validation_framework --cov-report=term-missing
```

### Coverage Goals

- **Core Components**: 90%+ coverage
- **Validations**: 85%+ coverage
- **Loaders**: 80%+ coverage
- **Reporters**: 75%+ coverage

### Quality Metrics

```bash
# Run linter
flake8 validation_framework/

# Run type checker
mypy validation_framework/

# Check code formatting
black --check validation_framework/
```

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: DataK9 Tests

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
          pip install -r requirements-dev.txt

      - name: Run tests
        run: pytest --cov=validation_framework --cov-report=xml

      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

---

## Best Practices

### 1. **Test One Thing Per Test**

```python
# ❌ BAD: Tests multiple things
def test_validation():
    # Tests loading AND validation AND reporting
    pass

# ✅ GOOD: Focused tests
def test_validation_passes_with_valid_data():
    pass

def test_validation_fails_with_invalid_data():
    pass

def test_validation_handles_empty_data():
    pass
```

### 2. **Use Descriptive Test Names**

```python
# ❌ BAD
def test_1():
    pass

# ✅ GOOD
def test_mandatory_field_check_fails_when_required_field_is_null():
    pass
```

### 3. **Arrange-Act-Assert Pattern**

```python
def test_example():
    # Arrange: Set up test data
    data = create_test_data()
    validation = create_validation()

    # Act: Execute the code under test
    result = validation.validate(data)

    # Assert: Verify the outcome
    assert result.passed is True
```

### 4. **Test Edge Cases**

```python
# Test edge cases:
# - Empty data
# - Single row
# - Very large data
# - Null/None values
# - Special characters
# - Boundary values
```

### 5. **Keep Tests Fast**

```python
# Use tmp_path for file operations
def test_with_files(tmp_path):
    test_file = tmp_path / "test.csv"
    # Fast, isolated, automatically cleaned up
```

---

## Test Inventory

This section provides a reference to the actual test files in the DataK9 test suite.

### Test Statistics

```
Total test files: 85+
Test categories:
- Unit tests: 50+ files
- Integration tests: 10+ files
- CLI tests: 5+ files
- Dataset/E2E tests: 5+ files
```

### Directory Structure

```
tests/
├── conftest.py                          # Shared fixtures and configuration
├── __init__.py
│
├── cli/                                 # CLI-specific tests
│   ├── test_cli.py                     # Main CLI command tests
│   └── test_pattern_integration.py     # Pattern matching tests
│
├── datasets/                            # Dataset-specific tests
│   ├── test_dataset_profiling.py       # Profiling various datasets
│   └── test_large_dataset_e2e.py       # Large dataset end-to-end tests
│
├── integration/                         # Integration tests
│   ├── test_expression_security.py     # Expression security tests
│   ├── test_integration.py             # Full workflow tests
│   └── test_profiler_enhancements.py   # Profiler integration tests
│
├── testsuite/                           # Test data and generators
│   ├── generators/                     # Test data generators
│   ├── data/                           # Sample test data files
│   │   ├── profiler/
│   │   ├── samples/
│   │   ├── validation/
│   │   ├── regression/
│   │   ├── cross_file/
│   │   └── json/
│   └── generate_validation_test_data.py
│
└── unit/                                # Unit tests by component
    ├── cli/                            # CLI unit tests
    │   ├── test_delimiter_mapping.py
    │   └── test_file_lock.py
    │
    ├── core/                           # Core framework tests
    │   ├── test_async_engine.py
    │   ├── test_config.py
    │   ├── test_constants.py
    │   ├── test_engine.py
    │   ├── test_exceptions.py
    │   ├── test_expression_validator.py
    │   ├── test_optimized_engine.py
    │   ├── test_registry.py
    │   ├── test_reporters.py
    │   ├── test_results.py
    │   ├── test_sampling_engine.py
    │   ├── test_security.py
    │   ├── test_code_quality_regression.py
    │   └── test_comprehensive_regression.py
    │
    ├── loaders/                        # Data loader tests
    │   ├── test_loaders.py             # General loader tests
    │   ├── test_async_loaders.py       # Async loading tests
    │   ├── test_csv_loader_debug.py
    │   ├── test_database_integration.py
    │   ├── test_database_loader_comprehensive.py
    │   ├── test_database_profiling_json.py
    │   └── test_resource_management.py
    │
    ├── profiler/                       # Profiler tests
    │   ├── test_profiler.py            # Core profiler tests
    │   ├── test_polars_profiler.py     # Polars backend tests
    │   ├── test_polars_engine_comprehensive.py
    │   ├── test_pii_detector.py        # PII detection tests
    │   ├── test_ml_analyzer.py         # ML analyzer tests
    │   ├── test_ml_analyzer_comprehensive.py
    │   ├── test_enhanced_correlation.py
    │   ├── test_enhanced_correlation_extended.py
    │   ├── test_temporal_analysis.py
    │   ├── test_semantic_tagger.py
    │   ├── test_context_discovery.py
    │   ├── test_validation_suggester.py
    │   ├── test_sampling_utils.py
    │   ├── test_streaming_and_sampling.py
    │   ├── test_data_generator.py
    │   ├── test_amount_max_consistency.py
    │   ├── test_benchmark_fixes.py
    │   ├── test_false_positive_regression.py
    │   └── test_missing_statistical_temporal.py
    │
    ├── validations/                    # Validation tests
    │   ├── test_validations.py         # General validation tests
    │   ├── test_field_validations.py   # Field-level validations
    │   ├── test_file_validations.py    # File-level validations
    │   ├── test_record_validations.py  # Record-level validations
    │   ├── test_schema_validations.py  # Schema validations
    │   ├── test_advanced_validations.py
    │   ├── test_conditional_validations.py
    │   ├── test_cross_file_validations.py
    │   ├── test_cross_file_advanced.py
    │   ├── test_database_validations.py
    │   ├── test_inline_validations.py
    │   ├── test_async_validators_comprehensive.py
    │   ├── test_cda.py                 # CDA validation tests
    │   └── test_policy.py
    │
    └── utils/                          # Utility tests
        └── test_path_patterns.py
```

### Running Tests by Category

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run CLI tests
pytest tests/cli/

# Run profiler tests
pytest tests/unit/profiler/

# Run validation tests
pytest tests/unit/validations/

# Run loader tests
pytest tests/unit/loaders/

# Run core framework tests
pytest tests/unit/core/

# Run with specific marker
pytest -m unit
pytest -m integration
pytest -m slow
pytest -m security
```

### Available Fixtures

The `conftest.py` provides these shared fixtures:

| Fixture | Description |
|---------|-------------|
| `sample_dataframe` | 5-row DataFrame with mixed types and some nulls |
| `clean_dataframe` | 5-row DataFrame with all valid data |
| `large_dataframe` | 10,000-row DataFrame for performance tests |
| `dataframe_with_duplicates` | DataFrame with intentional duplicates |
| `dataframe_with_outliers` | DataFrame with statistical outliers |
| `temp_csv_file` | Temporary CSV file (auto-cleaned) |
| `temp_large_csv_file` | Large temporary CSV file |
| `temp_empty_file` | Empty (0-byte) file |
| `temp_json_file` | Temporary JSON file |
| `temp_excel_file` | Temporary Excel file |
| `temp_parquet_file` | Temporary Parquet file |
| `temp_yaml_config` | Temporary validation config YAML |
| `sqlite_database` | Temporary SQLite database |

### Custom Pytest Markers

```python
@pytest.mark.unit          # Unit tests for individual components
@pytest.mark.integration   # Integration tests for full workflows
@pytest.mark.slow          # Tests that take significant time
@pytest.mark.security      # Security-related tests
@pytest.mark.cli           # Command-line interface tests
@pytest.mark.performance   # Performance and load tests
@pytest.mark.requires_data # Tests that require sample data files
```

### Adding New Tests

When adding new tests:

1. **Choose the right location**:
   - Unit tests go in `tests/unit/<component>/`
   - Integration tests go in `tests/integration/`
   - CLI tests go in `tests/cli/`

2. **Follow naming conventions**:
   - Test files: `test_<feature>.py`
   - Test classes: `Test<Feature>`
   - Test functions: `test_<behavior>`

3. **Use appropriate fixtures** from `conftest.py`

4. **Add markers** for test categorization

5. **Include docstrings** explaining what the test validates

---

## Next Steps

- **[Custom Validations](custom-validations.md)** - Build and test custom validations
- **[Contributing Guide](contributing.md)** - Contribute to DataK9 test suite
- **[Architecture](architecture.md)** - Understand what to test

---

**Trust through testing - DataK9 guards your data with confidence**
