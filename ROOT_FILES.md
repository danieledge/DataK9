# Root Directory Files - DataK9

This document explains the essential files in the root directory.

## Essential Files (Keep in Root)

### User-Facing
- **demo.sh** - Interactive demo script (primary user entry point)
- **README.md** - Project overview and quick start guide
- **CHANGELOG.md** - Version history and release notes
- **LICENSE** - MIT License

### Configuration
- **requirements.txt** - Python dependencies for production
- **requirements-dev.txt** - Development dependencies
- **setup.py** - Python package setup
- **.gitignore** - Git ignore patterns

### Application
- **datak9-studio.html** - DataK9 Studio IDE (single-file app)

---

## Organized Subdirectories

### Core Framework
- **validation_framework/** - Python framework source code
  - `validation_definitions.json` - Validation metadata (moved here)
  - `validation_definitions_schema.json` - JSON schema (moved here)

### Testing
- **tests/** - Unit and integration tests
- **scripts/** - Test runners and utilities
  - `run_tests.sh` - Interactive test runner
  - `run_coverage_tests.sh` - Coverage reporting

### Documentation
- **docs/** - User-facing documentation
  - `development/` - Development configs (pytest.ini, mypy.ini)
  - `VERSION` - Version number file

### Data
- **test-data/** - Test datasets organized by tier
  - `configs/` - Pre-built validation configs
  - `tiny/` - Sample datasets
  - `small/` - E-commerce dataset
  - Medium/Large/Ultimate require download

### Examples
- **examples/** - Sample data and configs
  - `sample_data/` - Small test files
  - `configs/` - Example configurations

### Resources
- **resources/** - Static assets
  - `images/` - Logos and ASCII art

### Archive
- **archive/** - Historical files (git-ignored)
  - `test-reports/` - Generated reports
  - `demo-examples/` - Demo files and backups
  - `documentation/` - Old documentation

### Working
- **wip/** - Work in progress (git-ignored)
- **demo-tmp/** - Demo artifacts (git-ignored)

---

## File Movement Summary

**Moved from root** → **New location**:
- Test scripts → `scripts/`
- Test configs → `test-data/configs/`
- Test reports → `archive/test-reports/`
- Demo backups → `archive/demo-examples/`
- Old docs → `archive/documentation/`
- Validation defs → `validation_framework/`
- pytest.ini, mypy.ini → `docs/development/`
- VERSION → `docs/`
- test_phase1.js → `tests/`

**Result**: Clean root with only essential files.

---

🐕 **DataK9 - Your K9 Guardian for Data Quality** 🐕
