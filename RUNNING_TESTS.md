# Running Tests - DataK9

Quick guide to executing the DataK9 test suite.

---

## Quick Start

### Option 1: Interactive Menu (Easiest)

```bash
./run_tests.sh
```

This launches an interactive menu where you can choose:
- Run all tests
- Run specific test categories
- Run with coverage
- Run specific test file

### Option 2: Command Line Options

```bash
# Run all tests
./run_tests.sh --all

# Run with coverage report
./run_tests.sh --coverage

# Run specific categories
./run_tests.sh --unit          # Unit tests only
./run_tests.sh --integration   # Integration tests only
./run_tests.sh --security      # Security tests only
./run_tests.sh --cli           # CLI tests only

# Run fast (skip slow tests)
./run_tests.sh --fast

# Run specific test file
./run_tests.sh --file tests/test_engine.py
```

### Option 3: Direct pytest (For Developers)

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_engine.py

# Run specific test function
pytest tests/test_engine.py::test_validation_engine_basic

# Run with coverage
pytest --cov=validation_framework --cov-report=html

# Run and show print statements
pytest -s

# Run in parallel (faster)
pytest -n auto
```

---

## Test Categories

### Unit Tests
Test individual components in isolation:
```bash
./run_tests.sh --unit
# OR
pytest tests/test_field_validations.py tests/test_file_validations.py
```

### Integration Tests
Test components working together:
```bash
./run_tests.sh --integration
# OR
pytest tests/test_engine.py tests/test_reporters.py
```

### Security Tests
Test security features and input validation:
```bash
./run_tests.sh --security
# OR
pytest tests/test_security.py
```

### CLI Tests
Test command-line interface:
```bash
./run_tests.sh --cli
# OR
pytest tests/test_cli.py
```

---

## Coverage Reports

### Generate Coverage Report

```bash
# Using test runner script (option 8 in interactive menu)
./run_tests.sh
# Then select: 8) Run with Coverage Report

# OR using command line
./run_tests.sh --coverage

# OR using pytest directly
pytest --cov=validation_framework --cov-report=html --cov-report=term

# View HTML coverage report
open htmlcov/index.html
```

### Current Coverage Stats

- **Total Coverage:** 48%
- **Total Tests:** 115+
- **Minimum Required:** 43%

### Coverage Threshold Behavior

**Important:** The 43% coverage threshold is only enforced when running the **complete test suite**:

- ✅ **All Tests with Coverage** (option 8) → Must meet 43% threshold
- ✅ **Individual Test Categories** (options 2-7) → Show coverage info but don't fail on threshold
  - Example: Running only CLI tests might show 41% coverage (tests only CLI module)
  - This is expected and will NOT fail the test run
  - The 43% threshold applies to the entire codebase, not individual modules

```bash
# This enforces 43% coverage requirement
./run_tests.sh --coverage

# This shows coverage but doesn't enforce threshold
pytest tests/test_cli.py --cov=validation_framework --cov-report=term
```

---

## Test Structure

```
tests/
├── conftest.py                      # Pytest fixtures and config
├── test_engine.py                   # Validation engine tests
├── test_field_validations.py        # Field-level validation tests
├── test_file_validations.py         # File-level validation tests
├── test_schema_validations.py       # Schema validation tests
├── test_record_validations.py       # Record-level validation tests
├── test_advanced_validations.py     # Advanced validation tests
├── test_cli.py                      # CLI interface tests
├── test_reporters.py                # Report generation tests
├── test_results.py                  # Result object tests
├── test_async_engine.py             # Async engine tests
└── test_security.py                 # Security tests
```

---

## Common Test Commands

### Run Tests with Different Output

```bash
# Minimal output (quietest)
pytest -q

# Normal output
pytest

# Verbose output (show test names)
pytest -v

# Very verbose (show full diff on failures)
pytest -vv

# Show print statements
pytest -s
```

### Run Specific Tests

```bash
# Run tests matching keyword
pytest -k "test_mandatory"

# Run tests by marker
pytest -m "slow"       # Run slow tests only
pytest -m "not slow"   # Skip slow tests

# Run last failed tests only
pytest --lf

# Run failed tests first, then rest
pytest --ff
```

### Performance Options

```bash
# Run in parallel (requires pytest-xdist)
pytest -n auto

# Stop on first failure
pytest -x

# Stop after N failures
pytest --maxfail=3

# Show slowest N tests
pytest --durations=10
```

---

## Troubleshooting

### Tests Not Found

**Error:** `ERROR: file or directory not found`

**Solution:**
```bash
# Make sure you're in project root
cd /home/daniel/www/dqa/data-validation-tool

# Verify tests directory exists
ls tests/

# Run with explicit path
pytest tests/
```

### Import Errors

**Error:** `ModuleNotFoundError: No module named 'validation_framework'`

**Solution:**
```bash
# Install package in development mode
pip install -e .

# OR add to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)
```

### Permission Denied

**Error:** `Permission denied: ./run_tests.sh`

**Solution:**
```bash
# Make script executable
chmod +x run_tests.sh run_coverage_tests.sh

# Then run
./run_tests.sh
```

### Missing Dependencies

**Error:** `No module named 'pytest'`

**Solution:**
```bash
# Install test dependencies
pip install -r requirements-dev.txt

# OR install individually
pip install pytest pytest-cov pytest-xdist
```

---

## CI/CD Integration

### GitHub Actions Example

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
          python-version: 3.8
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
      - name: Run tests
        run: ./run_tests.sh --all --coverage
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

### Exit Codes

- `0` - All tests passed
- `1` - Tests failed
- `2` - Test execution interrupted
- `3` - Internal error
- `4` - Command line usage error

---

## Writing New Tests

### Test File Template

```python
"""
Tests for [component name].
"""
import pytest
from validation_framework.[module] import [Component]


class Test[ComponentName]:
    """Test suite for [ComponentName]."""

    def test_[feature]_success(self):
        """Test [feature] with valid input."""
        # Arrange
        component = Component()

        # Act
        result = component.do_something()

        # Assert
        assert result is True

    def test_[feature]_failure(self):
        """Test [feature] with invalid input."""
        # Arrange
        component = Component()

        # Act & Assert
        with pytest.raises(ValueError):
            component.do_something_invalid()


@pytest.fixture
def sample_data():
    """Fixture providing sample test data."""
    return {"key": "value"}


def test_with_fixture(sample_data):
    """Test using fixture."""
    assert "key" in sample_data
```

### Running Your New Tests

```bash
# Run your new test file
pytest tests/test_my_new_feature.py -v

# Run with coverage for your module
pytest tests/test_my_new_feature.py \
    --cov=validation_framework.my_module \
    --cov-report=term
```

---

## Best Practices

### Before Committing

```bash
# Always run tests before committing
./run_tests.sh --all

# Check coverage didn't drop
./run_coverage_tests.sh

# Run linting
flake8 validation_framework/

# Format code
black validation_framework/
```

### Test Naming Conventions

- Test files: `test_*.py`
- Test classes: `Test*`
- Test methods: `test_*`
- Use descriptive names: `test_mandatory_field_check_with_missing_values`

### Test Organization

- One test file per module
- Group related tests in classes
- Use fixtures for common setup
- Keep tests independent (no shared state)

---

## Quick Reference

| Command | Description |
|---------|-------------|
| `./run_tests.sh` | Interactive test menu |
| `./run_tests.sh --all` | Run all tests |
| `./run_coverage_tests.sh` | Generate coverage report |
| `pytest` | Run all tests (simple) |
| `pytest -v` | Run with verbose output |
| `pytest -k "keyword"` | Run tests matching keyword |
| `pytest --lf` | Run last failed only |
| `pytest -x` | Stop on first failure |
| `pytest -n auto` | Run in parallel |

---

## Getting Help

```bash
# Pytest help
pytest --help

# Test runner help
./run_tests.sh --help

# List all available tests
pytest --collect-only
```

---

**🐕 Keep those tests passing!**

For more details, see:
- [Testing Guide](docs/for-developers/testing-guide.md) - Comprehensive testing documentation
- [Contributing Guide](docs/for-developers/contributing.md) - How to contribute tests
- [pytest documentation](https://docs.pytest.org/) - Official pytest docs

Author: Daniel Edge
