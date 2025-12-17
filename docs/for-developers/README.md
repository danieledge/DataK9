# Developer Guide

Welcome to the **DataK9 Developer Guide**. This documentation covers the technical architecture, extension points, and contribution guidelines for DataK9.

---

## 📚 Documentation Overview

### Architecture & Design
Understand how DataK9 works internally:
- **[Architecture Overview](architecture.md)** - System design, components, data flow
- **[Design Patterns](design-patterns.md)** - Patterns used throughout the codebase
- **[API Reference](api-reference.md)** - Python API documentation

### Extending DataK9
Build custom components:
- **[Custom Validations](custom-validations.md)** - Create new validation rules
- **[Custom Loaders](custom-loaders.md)** - Add support for new file formats
- **[Custom Reporters](custom-reporters.md)** - Generate custom report formats

### Development & Testing
- **[Testing Guide](testing-guide.md)** - Writing and running tests
- **[Contributing](contributing.md)** - How to contribute to DataK9

---

## 🚀 Quick Start for Developers

### 1. Clone and Setup Development Environment

```bash
# Clone repository
git clone https://github.com/danieledge/datak9.git
cd datak9/data-validation-tool

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install in editable mode with dev dependencies
pip install -e ".[dev]"
```

### 2. Run Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=validation_framework

# Run specific test file
pytest tests/test_validations.py
```

### 3. Code Style

DataK9 follows PEP 8 with Black formatting:

```bash
# Format code
black validation_framework/

# Check linting
flake8 validation_framework/

# Run both
black validation_framework/ && flake8 validation_framework/
```

---

## 🏗️ Architecture Overview

### High-Level Components

```
┌─────────────────────────────────────────────────────────┐
│                     DataK9 CLI                           │
│                   (Click Framework)                      │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  Validation Engine                       │
│          (Orchestrates entire workflow)                  │
└──┬────────────┬────────────┬─────────────┬──────────────┘
   │            │            │             │
   ▼            ▼            ▼             ▼
┌──────┐   ┌────────┐   ┌──────────┐  ┌─────────┐
│Config│   │Loaders │   │Validation│  │Reporters│
│System│   │Factory │   │ Registry │  │         │
└──────┘   └────────┘   └──────────┘  └─────────┘
              │              │              │
              ▼              ▼              ▼
         ┌─────────┐    ┌──────────┐  ┌──────────┐
         │CSV/Excel│    │36 Built- │  │HTML/JSON │
         │JSON/Parq│    │in Rules  │  │  Output  │
         │Database │    │          │  │          │
         └─────────┘    └──────────┘  └──────────┘
```

**See:** [Complete Architecture Documentation](architecture.md)

---

## 🔌 Extension Points

### 1. Custom Validation Rules

Create new validation types by extending `DataValidationRule`:

```python
from validation_framework.core.base import DataValidationRule
from validation_framework.core.registry import register_validation

class MyCustomCheck(DataValidationRule):
    """Custom validation rule."""

    def validate(self, data_iterator, context):
        """
        Implement validation logic.

        Args:
            data_iterator: Iterator yielding DataFrame chunks
            context: Validation context with metadata

        Returns:
            ValidationResult
        """
        passed = True
        failures = []

        for chunk in data_iterator:
            # Your validation logic here
            invalid_rows = chunk[chunk['field'] > 100]

            if not invalid_rows.empty:
                passed = False
                failures.extend(
                    self._format_failures(invalid_rows, chunk)
                )

        return self._create_result(
            passed=passed,
            message="Custom validation message",
            failures=failures
        )

# Register the validation
register_validation("MyCustomCheck", MyCustomCheck)
```

**See:** [Custom Validations Guide](custom-validations.md)

---

### 2. Custom Data Loaders

Support new file formats by implementing the Loader interface:

```python
from validation_framework.loaders.base import BaseLoader

class MyFormatLoader(BaseLoader):
    """Loader for custom file format."""

    def load(self):
        """
        Yield data chunks.

        Yields:
            DataFrame: Chunks of data
        """
        # Your loading logic here
        for chunk in self._read_file_in_chunks():
            yield chunk

    def get_metadata(self):
        """Return file metadata."""
        return {
            'format': 'my_format',
            'size': self._get_file_size(),
            'rows': self._get_row_count()
        }
```

**See:** [Custom Loaders Guide](custom-loaders.md)

---

### 3. Custom Reporters

Generate custom report formats:

```python
from validation_framework.reporters.base import BaseReporter

class MyCustomReporter(BaseReporter):
    """Generate custom format reports."""

    def generate(self, report, output_path):
        """
        Generate report.

        Args:
            report: ValidationReport object
            output_path: Where to save report
        """
        # Your reporting logic here
        with open(output_path, 'w') as f:
            f.write(self._format_report(report))
```

**See:** [Custom Reporters Guide](custom-reporters.md)

---

## 🧪 Testing

### Test Structure

```
tests/
├── test_validations.py      # Validation rule tests
├── test_loaders.py           # Data loader tests
├── test_engine.py            # Engine tests
├── test_config.py            # Configuration tests
├── test_reporters.py         # Reporter tests
└── fixtures/                 # Test data files
    ├── valid_data.csv
    ├── invalid_data.csv
    └── config_samples/
```

### Writing Tests

```python
import pytest
from validation_framework.validations.field_checks import MandatoryFieldCheck

def test_mandatory_field_check_passes():
    """Test MandatoryFieldCheck with valid data."""
    validation = MandatoryFieldCheck(params={
        'fields': ['customer_id', 'email']
    })

    data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'email': ['a@b.com', 'c@d.com', 'e@f.com']
    })

    result = validation.validate(iter([data]), context={})

    assert result.passed is True
    assert len(result.failures) == 0

def test_mandatory_field_check_fails():
    """Test MandatoryFieldCheck with missing values."""
    validation = MandatoryFieldCheck(params={
        'fields': ['email']
    })

    data = pd.DataFrame({
        'email': ['a@b.com', None, 'c@d.com']
    })

    result = validation.validate(iter([data]), context={})

    assert result.passed is False
    assert len(result.failures) == 1
```

**See:** [Complete Testing Guide](testing-guide.md)

---

## 📦 Package Structure

```
validation_framework/
├── __init__.py
├── cli.py                    # Command-line interface
├── core/
│   ├── engine.py             # Main validation engine
│   ├── config.py             # Configuration parser
│   ├── registry.py           # Validation registry
│   ├── base.py               # Base classes
│   └── results.py            # Result classes
├── loaders/
│   ├── base.py               # Base loader
│   ├── csv_loader.py         # CSV loader
│   ├── excel_loader.py       # Excel loader
│   ├── json_loader.py        # JSON loader
│   ├── parquet_loader.py     # Parquet loader
│   └── database_loader.py    # Database loader
├── validations/
│   ├── file_checks.py        # File-level validations
│   ├── schema_checks.py      # Schema validations
│   ├── field_checks.py       # Field-level validations
│   ├── record_checks.py      # Record-level validations
│   ├── advanced_checks.py    # Advanced validations
│   ├── conditional.py        # Conditional validation
│   ├── cross_file_checks.py  # Cross-file validations
│   ├── database_checks.py    # Database validations
│   ├── temporal_checks.py    # Temporal validations
│   └── statistical_checks.py # Statistical validations
├── reporters/
│   ├── html_reporter.py      # HTML report generator
│   └── json_reporter.py      # JSON report generator
└── profiler/
    ├── profiler.py           # Data profiler
    └── profile_reporter.py   # Profile report generator
```

---

## 🎯 Design Principles

DataK9 follows these principles:

### 1. Plugin Architecture
- Validations register themselves
- Easy to add new validations
- No code changes to use custom validations

### 2. Memory Efficiency
- Chunked processing for large files
- Iterator-based data loading
- Configurable memory usage

### 3. Separation of Concerns
- Clear boundaries between components
- Each module has single responsibility
- Easy to test and maintain

### 4. Configuration-Driven
- No coding required for standard use cases
- YAML configuration for all settings
- Visual IDE for non-technical users

### 5. Extensibility
- Well-defined extension points
- Base classes for custom components
- Registry pattern for discovery

---

## 🛠️ Development Workflow

### 1. Create Feature Branch

```bash
git checkout -b feature/my-new-feature
```

### 2. Make Changes

- Write code following PEP 8
- Add comprehensive docstrings
- Include type hints
- Add tests

### 3. Run Tests

```bash
# Run tests
pytest

# Check coverage
pytest --cov=validation_framework --cov-report=html

# Format code
black validation_framework/

# Lint
flake8 validation_framework/
```

### 4. Commit and Push

```bash
git add .
git commit -m "Add new feature: description"
git push origin feature/my-new-feature
```

### 5. Create Pull Request

- Describe changes clearly
- Link related issues
- Ensure all tests pass
- Wait for code review

---

## 📖 Key Documents

### Must-Read for Developers
- **[Architecture](architecture.md)** - Understand the system
- **[Design Patterns](design-patterns.md)** - Patterns used
- **[Custom Validations](custom-validations.md)** - Extend functionality

### Reference
- **[API Reference](api-reference.md)** - Complete API docs
- **[Testing Guide](testing-guide.md)** - Testing best practices
- **[Contributing](contributing.md)** - Contribution guidelines

---

## 🤝 Contributing

We welcome contributions! Please see:

- **[Contributing Guide](contributing.md)** - How to contribute
- **[Code of Conduct](contributing.md#code-of-conduct)** - Community guidelines
- **[GitHub Issues](https://github.com/danieledge/datak9/issues)** - Bug reports and feature requests

---

## 🆘 Need Help?

- **[Developer FAQ](api-reference.md#faq)** - Common developer questions
- **[GitHub Discussions](https://github.com/danieledge/datak9/discussions)** - Ask questions
- **[Architecture Docs](architecture.md)** - Deep dive into design

---

**🐕 Help build DataK9 - the K9 guardian for data quality!**
