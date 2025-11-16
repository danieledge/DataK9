# Contributing to DataK9

**Join the Pack - Help Us Guard Data Quality Everywhere**

Thank you for your interest in contributing to DataK9! Whether you're fixing bugs, adding features, improving documentation, or sharing ideas, your contributions make DataK9 better for everyone.

This guide will help you get started.

---

## Table of Contents

1. [How to Contribute](#how-to-contribute)
2. [Development Setup](#development-setup)
3. [Code Standards](#code-standards)
4. [Pull Request Process](#pull-request-process)
5. [Testing Requirements](#testing-requirements)
6. [Documentation](#documentation)
7. [Reporting Issues](#reporting-issues)
8. [Community Guidelines](#community-guidelines)

---

## How to Contribute

### Ways to Contribute

You can contribute to DataK9 in many ways:

**Code Contributions:**
- 🐛 Fix bugs
- ✨ Add new validation types
- 🔌 Create new data loaders
- 📊 Build new reporters
- ⚡ Improve performance
- 🔒 Enhance security

**Documentation:**
- 📚 Improve user guides
- 📖 Add examples
- 🎓 Create tutorials
- 💡 Write blog posts
- 🎥 Record videos

**Community:**
- 💬 Answer questions
- 🐞 Report bugs
- 💡 Suggest features
- 📣 Spread the word
- ⭐ Star the repo

**Testing:**
- 🧪 Add test cases
- 🔍 Test on different platforms
- 📊 Performance benchmarking
- 🛡️ Security testing

---

## Development Setup

### 1. Fork and Clone

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/YOUR_USERNAME/data-validation-tool.git
cd data-validation-tool

# Add upstream remote
git remote add upstream https://github.com/danieledge/data-validation-tool.git

# Verify remotes
git remote -v
# origin    https://github.com/YOUR_USERNAME/data-validation-tool.git (fetch)
# origin    https://github.com/YOUR_USERNAME/data-validation-tool.git (push)
# upstream  https://github.com/danieledge/data-validation-tool.git (fetch)
# upstream  https://github.com/danieledge/data-validation-tool.git (push)
```

### 2. Set Up Development Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install DataK9 in development mode
pip install -e .

# Install development dependencies
pip install -r requirements-dev.txt

# This includes:
# - pytest (testing)
# - pytest-cov (coverage)
# - black (code formatting)
# - flake8 (linting)
# - mypy (type checking)
```

### 3. Verify Setup

```bash
# Run tests to verify everything works
pytest

# You should see all tests passing
# ===== 115 passed in 12.45s =====
```

### 4. Create a Branch

```bash
# Always create a new branch for your work
git checkout -b feature/my-awesome-feature

# Use descriptive branch names:
# - feature/add-xml-loader
# - fix/regex-check-unicode-bug
# - docs/improve-quickstart
# - refactor/simplify-registry
```

---

## Code Standards

### Python Style Guide

DataK9 follows **PEP 8** with these conventions:

**Formatting:**
- 4 spaces for indentation (no tabs)
- Maximum line length: 100 characters
- Use double quotes for strings: `"hello"`
- Add trailing commas in multi-line structures

**Naming Conventions:**
```python
# Classes: PascalCase
class EmailValidationCheck:
    pass

# Functions and methods: snake_case
def validate_email_format():
    pass

# Constants: UPPER_SNAKE_CASE
MAX_SAMPLE_FAILURES = 100

# Private: prefix with underscore
def _internal_helper():
    pass
```

### Type Hints

Use type hints for all public methods:

```python
from typing import Iterator, Dict, Any, Optional
import pandas as pd

def validate(
    self,
    data_iterator: Iterator[pd.DataFrame],
    context: Dict[str, Any]
) -> ValidationResult:
    """
    Execute validation.

    Args:
        data_iterator: Iterator yielding DataFrame chunks
        context: Validation context

    Returns:
        ValidationResult object
    """
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int) -> bool:
    """
    Brief one-line description.

    More detailed description if needed. Can span multiple lines
    and include additional context.

    Args:
        param1: Description of param1
        param2: Description of param2

    Returns:
        Description of return value

    Raises:
        ValueError: When param1 is empty
        TypeError: When param2 is negative

    Example:
        >>> my_function("test", 42)
        True
    """
    pass
```

### Code Quality Tools

Before submitting, run these tools:

```bash
# Format code with black
black validation_framework/

# Check linting with flake8
flake8 validation_framework/

# Run type checking with mypy
mypy validation_framework/

# Run all checks at once
make lint  # if Makefile exists
```

### Writing Good Code

**1. Keep It Simple**
```python
# ❌ BAD: Overly complex
result = True if condition == True else False

# ✅ GOOD: Simple and clear
result = condition
```

**2. Use Meaningful Names**
```python
# ❌ BAD: Unclear names
def fn(d, x):
    return d[x] if x in d else None

# ✅ GOOD: Descriptive names
def get_field_value(data_row: dict, field_name: str) -> Any:
    return data_row.get(field_name)
```

**3. Error Handling**
```python
# ✅ GOOD: Comprehensive error handling
try:
    result = process_data(data)
except FileNotFoundError as e:
    logger.error(f"File not found: {str(e)}")
    raise
except ValueError as e:
    logger.error(f"Invalid data: {str(e)}")
    raise
except Exception as e:
    logger.exception("Unexpected error in process_data")
    raise
```

**4. Logging**
```python
import logging

logger = logging.getLogger(__name__)

# Use appropriate log levels
logger.debug("Detailed debugging information")
logger.info("🐕 DataK9 starting validation")
logger.warning("Potential issue detected")
logger.error("Error occurred")
logger.exception("Critical error with traceback")
```

---

## Pull Request Process

### Before Creating PR

1. **Update from upstream**
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run all tests**
   ```bash
   pytest
   ```

3. **Check code style**
   ```bash
   black validation_framework/ --check
   flake8 validation_framework/
   ```

4. **Update documentation**
   - Add docstrings to new functions
   - Update relevant .md files
   - Add examples if applicable

### Creating the Pull Request

1. **Push your branch**
   ```bash
   git push origin feature/my-awesome-feature
   ```

2. **Create PR on GitHub**
   - Go to your fork on GitHub
   - Click "Pull Request"
   - Choose `danieledge/data-validation-tool` as base
   - Choose your feature branch as compare

3. **Write a Good PR Description**

   ```markdown
   ## Description
   Brief description of what this PR does.

   ## Changes
   - Added XMLLoader for loading XML files
   - Registered XMLLoader in LoaderFactory
   - Added unit tests for XMLLoader
   - Updated documentation

   ## Testing
   - [x] All existing tests pass
   - [x] Added new tests for XMLLoader
   - [x] Manually tested with sample XML files

   ## Documentation
   - [x] Updated custom-loaders.md
   - [x] Added example configuration

   ## Related Issues
   Fixes #123
   Related to #456
   ```

### PR Review Process

1. **Automated Checks**
   - CI tests must pass
   - Code coverage maintained or improved
   - Linting checks pass

2. **Code Review**
   - Maintainer will review your code
   - Address feedback constructively
   - Update PR as needed

3. **Approval and Merge**
   - Once approved, maintainer will merge
   - Your contribution is now part of DataK9!

### After PR is Merged

```bash
# Update your local main branch
git checkout main
git pull upstream main

# Delete your feature branch
git branch -d feature/my-awesome-feature
git push origin --delete feature/my-awesome-feature
```

---

## Testing Requirements

### Test Coverage

All contributions must include tests:

- **New Features:** 80%+ test coverage required
- **Bug Fixes:** Include test that reproduces the bug
- **Refactoring:** Maintain or improve existing coverage

### Writing Tests

```python
"""
Tests for MyNewValidation.

Author: Your Name
"""
import pytest
import pandas as pd
from validation_framework.validations.custom.my_validation import MyNewValidation


class TestMyNewValidation:
    """Test suite for MyNewValidation"""

    def test_passes_with_valid_data(self):
        """Test validation passes with valid input"""
        # Arrange
        data = pd.DataFrame({'field': [1, 2, 3]})
        validation = MyNewValidation(
            name="test",
            severity=Severity.ERROR,
            params={}
        )

        # Act
        def data_iter():
            yield data

        result = validation.validate(data_iter(), {})

        # Assert
        assert result.passed is True
        assert result.failed_count == 0

    def test_fails_with_invalid_data(self):
        """Test validation fails with invalid input"""
        # Test implementation...

    def test_handles_edge_cases(self):
        """Test edge cases like empty data, nulls, etc."""
        # Test implementation...
```

### Running Tests Locally

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_validations/test_my_validation.py

# Run with coverage
pytest --cov=validation_framework

# Run tests matching pattern
pytest -k "email"
```

---

## Documentation

### Code Documentation

Every public function, class, and method needs docstrings:

```python
class EmailValidator(DataValidationRule):
    """
    Validates email addresses using regex pattern.

    DataK9 uses this validator to ensure email fields
    contain properly formatted email addresses.

    Configuration:
        field (str): Name of email field to validate
        pattern (str): Optional custom regex pattern

    Example YAML:
        - type: "EmailValidator"
          severity: "ERROR"
          params:
            field: "email"

    Example Python:
        validator = EmailValidator(
            name="Email Check",
            severity=Severity.ERROR,
            params={"field": "email"}
        )
    """
```

### User Documentation

When adding features, update relevant docs:

- **New Validation:** Add to `docs/reference/validation-reference.md`
- **New Loader:** Add to `docs/for-developers/custom-loaders.md`
- **New Feature:** Update `docs/README.md` and quickstart
- **Examples:** Add to `docs/examples/`

### Documentation Style

- Use active voice: "DataK9 validates..." not "Validation is performed..."
- Include examples for every feature
- Use the K9 theme: guard, vigilant, loyal, protective
- Keep it clear and concise
- Test all code examples

---

## Reporting Issues

### Before Reporting

1. **Search existing issues** - Your issue might already be reported
2. **Check documentation** - Solution might already exist
3. **Try latest version** - Bug might be fixed in newer release

### Bug Reports

Use the bug report template:

```markdown
## Bug Description
Clear description of the bug

## Steps to Reproduce
1. Create config file with...
2. Run validation with...
3. See error

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Environment
- DataK9 Version: 1.53
- Python Version: 3.9.5
- OS: Ubuntu 20.04
- Installation: pip / source

## Configuration File
```yaml
# Paste your config (redact sensitive data)
```

## Error Message
```
Paste error message and stack trace
```

## Additional Context
Any other relevant information
```

### Feature Requests

Use the feature request template:

```markdown
## Feature Description
What feature would you like to see?

## Use Case
Why is this needed? What problem does it solve?

## Proposed Solution
How should it work?

## Example Configuration
```yaml
# How might users configure this?
```

## Alternatives Considered
What alternatives did you consider?

## Additional Context
Any other relevant information
```

---

## Community Guidelines

### Code of Conduct

DataK9 follows a simple code of conduct:

1. **Be Respectful** - Treat everyone with respect and kindness
2. **Be Constructive** - Provide helpful, actionable feedback
3. **Be Inclusive** - Welcome contributors of all backgrounds and skill levels
4. **Be Patient** - Remember everyone was a beginner once
5. **Be Professional** - Keep discussions on-topic and professional

### Communication Channels

- **GitHub Issues** - Bug reports, feature requests
- **GitHub Discussions** - Questions, ideas, general discussion
- **Pull Requests** - Code contributions, reviews

### Attribution

All contributors are credited in:
- Git commit history
- Release notes
- Contributors list (if maintained)

---

## License

By contributing to DataK9, you agree that your contributions will be licensed under the MIT License.

---

## Need Help?

### Getting Help with Contributing

- **Documentation:** Read the [Developer Guide](README.md)
- **Examples:** Look at existing code for patterns
- **Questions:** Open a GitHub Discussion
- **Stuck?** Ask in your PR - maintainers are happy to help

### Good First Issues

Look for issues labeled:
- `good first issue` - Great for first-time contributors
- `help wanted` - We'd love help with these
- `documentation` - Documentation improvements
- `enhancement` - New features

---

## Recognition

### Top Contributors

We recognize top contributors through:
- Mentions in release notes
- Contributor badges (if implemented)
- Special thanks in documentation
- Community recognition

### How We Say Thank You

Every contribution matters:
- 🐛 Bug fixes prevent issues for all users
- ✨ New features enable new use cases
- 📚 Documentation helps everyone learn
- 💬 Community support builds the pack
- ⭐ Stars show appreciation

**Thank you for making DataK9 better!** 🐕

---

## Quick Reference

### Essential Commands

```bash
# Get latest changes
git fetch upstream
git rebase upstream/main

# Create feature branch
git checkout -b feature/my-feature

# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest

# Format code
black validation_framework/

# Check linting
flake8 validation_framework/

# Type checking
mypy validation_framework/

# Push changes
git push origin feature/my-feature
```

### Checklist Before PR

- [ ] Code follows style guide
- [ ] Tests added/updated
- [ ] Tests pass locally
- [ ] Documentation updated
- [ ] Commit messages are clear
- [ ] Branch is up to date with main
- [ ] No sensitive data in code

---

**🐕 Join the pack - Help DataK9 guard data quality everywhere!**

*This contributing guide is a living document. Suggestions for improvements are welcome.*
