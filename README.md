<div align="center">
  <img src="resources/images/datak9-web.png" alt="DataK9 Logo" width="300">

  # DataK9 Data Quality Framework

  ## 🐕 Your K9 guardian for data quality

  **Data validation framework with comprehensive testing across files and databases**
</div>

DataK9 is a robust, extensible Python framework for validating data quality before loading to databases, data warehouses, or analytics platforms. Like a well-trained K9 unit, DataK9 vigilantly guards your data, sniffing out quality issues before they become problems.

[![Version 0.2.0](https://img.shields.io/badge/version-0.2.0-blue.svg)](#changelog)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Tests: 115+ passing](https://img.shields.io/badge/tests-115%2B%20passing-brightgreen.svg)](tests/)
[![Coverage: 48%](https://img.shields.io/badge/coverage-48%25-yellow.svg)](htmlcov/)

**⚡ High-Performance Validation** • Memory Efficient • 200GB+ File Support • [Complete Documentation →](docs/)

---

## 📑 Table of Contents

- [Why DataK9?](#-why-datak9)
- [Quick Start](#-quick-start)
- [Key Features](#-key-features)
- [Installation](#-installation)
- [Core Capabilities](#-core-capabilities)
  - [CLI Commands](#cli-commands)
  - [Data Profiling](#data-profiling)
  - [Validation Types](#validation-types)
- [Documentation](#-documentation)
- [Performance](#-performance)
- [Integration](#-integration)
- [Contributing](#-contributing)

---

## 🐕 Why DataK9?

### Built for Scale
- ✅ **Large file support** - Handles 200GB+ files, tested with 357M rows on Raspberry Pi 4
- ✅ **Memory-efficient** - Chunked processing with automatic disk spillover
- ✅ **Fast with Polars** - 179M rows validated in 5 minutes
- ✅ **Tested** - 115+ tests, 48% coverage (growing), comprehensive error handling

### Easy to Use
- ✅ **No coding required** - Simple YAML configuration
- ✅ **Visual IDE** - DataK9 Studio for point-and-click configuration
- ✅ **35 validations** - Ready-to-use validation types across 10 categories
- ✅ **Auto-profiling** - Analyzes data and suggests validations

### Flexible & Extensible
- ✅ **Multiple formats** - CSV, Excel, JSON, Parquet, Database connections
- ✅ **File & database** - Validate both files and live databases
- ✅ **Plugin architecture** - Easy to add custom validations
- ✅ **AutoSys/CI/CD ready** - Standard exit codes, JSON output

---

## 🚀 Quick Start

### 1. Install

```bash
cd data-validation-tool
pip install -r requirements.txt

# Optional: Install Polars for high performance (recommended)
pip install polars
```

### 2. Profile Your Data

```bash
# Profile a file to understand its structure
python3 -m validation_framework.cli profile data.csv

# Profile a database table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/db" \
  --table customers
```

This generates:
- **HTML report** - Interactive profile with quality metrics
- **JSON data** - Machine-readable profile
- **YAML config** - Auto-generated validation configuration

### 3. Create Validation Config

Use the auto-generated config or create your own:

```yaml
validation_job:
  name: "Customer Data Quality Check"

files:
  - name: "customers"
    path: "customers.csv"
    format: "csv"

    validations:
      # File must not be empty
      - type: "EmptyFileCheck"
        severity: "ERROR"

      # Required fields must have values
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      # Email format validation
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

### 4. Run Validation

```bash
# Run with Polars backend (fast, recommended)
python3 -m validation_framework.cli validate config.yaml

# Run with pandas backend (for Excel files)
python3 -m validation_framework.cli validate config.yaml --backend pandas
```

### 5. View Results

Open `validation_report.html` to see:
- ✅ Passed validations (green)
- ❌ Failed validations (red) with sample failures
- ⚠️ Warnings (yellow) requiring review
- 📊 Summary statistics

**Exit codes for automation:**
- `0` - All validations passed
- `1` - ERROR-severity failures found
- `2` - Command error (bad config, file not found)

---

## ✨ Key Features

### 35 Built-in Validation Types

**File-Level (3):** EmptyFileCheck, RowCountRangeCheck, FileSizeCheck
**Schema (2):** SchemaMatchCheck, ColumnPresenceCheck
**Field-Level (6):** MandatoryFieldCheck, RegexCheck, ValidValuesCheck, RangeCheck, DateFormatCheck, InlineRegexCheck
**Record-Level (3):** DuplicateRowCheck, BlankRecordCheck, UniqueKeyCheck
**Advanced (9):** StatisticalOutlierCheck, CrossFieldComparisonCheck, FreshnessCheck, CompletenessCheck, StringLengthCheck, NumericPrecisionCheck, InlineBusinessRuleCheck, InlineLookupCheck, and more
**Cross-File (4):** ReferentialIntegrityCheck, CrossFileComparisonCheck, CrossFileDuplicateCheck, CrossFileKeyCheck
**Database (3):** SQLCustomCheck, DatabaseReferentialIntegrityCheck, DatabaseConstraintCheck
**Temporal (2):** BaselineComparisonCheck, TrendDetectionCheck
**Statistical (3):** DistributionCheck, CorrelationCheck, AdvancedAnomalyDetectionCheck

→ **[Complete Validation Reference](VALIDATION_REFERENCE.md)**

### Multiple Data Sources

**Files:** CSV, Excel (.xls, .xlsx), JSON, Parquet
**Databases:** PostgreSQL, MySQL, SQL Server, Oracle, SQLite
**Mixed:** Validate files against database reference data

### Visual Configuration

**DataK9 Studio** - Visual IDE interface:
- 🎨 Visual validation builder with 35 validation types
- 💻 Monaco editor with YAML syntax highlighting
- 📱 Mobile-responsive design
- 🌙 Modern dark theme
- 🏢 Offline-ready (no external CDN dependencies)

**→ [Launch DataK9 Studio](https://raw.githack.com/danieledge/data-validation-tool/main/datak9-studio.html)**

### Data Profiling

Comprehensive data analysis with:
- Type inference (known vs inferred)
- Statistical distributions
- Quality metrics and scoring
- Correlation analysis
- PII detection
- Auto-generated validation configs

---

## 🔧 Installation

### Requirements
- Python 3.8+
- pip

### Install DataK9

```bash
# Clone repository
git clone https://github.com/danieledge/data-validation-tool.git
cd data-validation-tool

# Install dependencies
pip install -r requirements.txt

# Verify installation
python3 -m validation_framework.cli --help
```

### Optional Dependencies

```bash
# Polars backend - High performance (RECOMMENDED)
pip install polars

# Excel support
pip install openpyxl

# Parquet support (10x faster than CSV)
pip install pyarrow

# Database support
pip install sqlalchemy psycopg2-binary pymysql

# Development tools
pip install -r requirements-dev.txt
```

---

## 🎯 Core Capabilities

### CLI Commands

```bash
# Validate data
python3 -m validation_framework.cli validate config.yaml

# Profile data file
python3 -m validation_framework.cli profile data.csv

# Profile database table
python3 -m validation_framework.cli profile \
  --database "sqlite:///test.db" --table users

# List all validations
python3 -m validation_framework.cli list-validations

# List validations by category
python3 -m validation_framework.cli list-validations --category field

# List database-compatible validations
python3 -m validation_framework.cli list-validations --source database

# Generate sample config
python3 -m validation_framework.cli init-config my_config.yaml
```

→ **[Complete CLI Guide](CLI_GUIDE.md)**

### Data Profiling

```bash
# Profile CSV file
python3 -m validation_framework.cli profile customers.csv

# Profile with custom outputs
python3 -m validation_framework.cli profile data.csv \
  -o profile.html \
  -j profile.json \
  -c validation_config.yaml

# Profile Parquet file with Polars backend
python3 -m validation_framework.cli profile large_data.parquet \
  --backend polars \
  --chunk-size 200000

# Profile database table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/mydb" \
  --table orders \
  -o orders_profile.html

# Profile with custom SQL query
python3 -m validation_framework.cli profile \
  --database "sqlite:///sales.db" \
  --query "SELECT * FROM transactions WHERE date > '2024-01-01'" \
  -o recent_sales_profile.html
```

**Profiler generates:**
- Interactive HTML report with charts and metrics
- JSON profile data for programmatic use
- Auto-generated validation YAML configuration
- Quality scores and validation suggestions

### Validation Types

**35 validation types across 10 categories:**

| Category | Count | Examples |
|----------|-------|----------|
| File-Level | 3 | EmptyFileCheck, RowCountRangeCheck, FileSizeCheck |
| Schema | 2 | SchemaMatchCheck, ColumnPresenceCheck |
| Field-Level | 6 | MandatoryFieldCheck, RegexCheck, RangeCheck, DateFormatCheck |
| Record-Level | 3 | DuplicateRowCheck, UniqueKeyCheck, BlankRecordCheck |
| Advanced | 9 | StatisticalOutlierCheck, FreshnessCheck, NumericPrecisionCheck |
| Cross-File | 4 | ReferentialIntegrityCheck, CrossFileComparisonCheck |
| Database | 3 | SQLCustomCheck, DatabaseReferentialIntegrityCheck |
| Conditional | 1 | ConditionalValidation (if-then-else logic) |
| Temporal | 2 | BaselineComparisonCheck, TrendDetectionCheck |
| Statistical | 3 | DistributionCheck, CorrelationCheck, AdvancedAnomalyDetectionCheck |

**Source Compatibility:**
- **File sources only:** 7 validations (EmptyFileCheck, FileSizeCheck, Cross-File validations, etc.)
- **Database sources only:** 3 validations (DatabaseConstraintCheck, DatabaseReferentialIntegrityCheck, SQLCustomCheck)
- **Both file and database:** 25 validations (most field, schema, and record validations)

→ **[Complete Validation Reference](VALIDATION_REFERENCE.md)**

---

## 📚 Documentation

### Getting Started
- **[5-Minute Quickstart](docs/getting-started/quickstart-5min.md)** - Get running fast
- **[Installation Guide](docs/getting-started/installation.md)** - Detailed setup
- **[CLI Reference](CLI_GUIDE.md)** - Complete command-line guide ⭐
- **[Validation Reference](VALIDATION_REFERENCE.md)** - All 35 validations ⭐

### Using DataK9
- **[Configuration Guide](docs/using-datak9/configuration-guide.md)** - YAML syntax and examples
- **[Data Profiling](docs/using-datak9/data-profiling.md)** - Analyze data before validation
- **[DataK9 Studio Guide](docs/using-datak9/studio-guide.md)** - Visual configuration IDE
- **[Best Practices](docs/using-datak9/best-practices.md)** - Recommended patterns
- **[Performance Tuning](docs/using-datak9/performance-tuning.md)** - Optimize for speed/memory
- **[Large Files Guide](docs/using-datak9/large-files.md)** - Handle 200GB+ datasets
- **[AutoSys Integration](docs/using-datak9/autosys-integration.md)** - Job scheduling integration
- **[CI/CD Integration](docs/using-datak9/cicd-integration.md)** - GitHub Actions, GitLab, Jenkins

### For Developers
- **[Architecture](docs/for-developers/architecture.md)** - System design
- **[Custom Validations](docs/for-developers/custom-validations.md)** - Build your own
- **[API Reference](docs/for-developers/api-reference.md)** - Python API
- **[Contributing Guide](docs/for-developers/contributing.md)** - Help improve DataK9

### Examples
- **[Finance](docs/examples/finance.md)** - Banking, trading, AML validation
- **[Healthcare](docs/examples/healthcare.md)** - HIPAA-compliant patient data
- **[E-Commerce](docs/examples/ecommerce.md)** - Customer, order, inventory

---

## 🚀 Performance

### Benchmarks (Polars Backend - Recommended)

| File Size | Format | Rows | Time | Memory | Platform |
|-----------|--------|------|------|--------|----------|
| 1 MB | CSV | 10K | <1 sec | ~10 MB | Any |
| 100 MB | CSV | 1M | ~10 sec | ~100 MB | Any |
| 1 GB | Parquet | 10M | ~30 sec | ~200 MB | Desktop |
| 5.1 GB | Parquet | 179M | ~5 min | ~10 GB | Desktop |
| **10.1 GB** | **Parquet** | **357M** | **~55 min** | **~3.5 GB** | **Raspberry Pi 4** |

**Large-Scale Test:** 357M rows, 30 comprehensive validations (cross-file, ML anomaly detection, statistical tests) on Raspberry Pi 4 with only 4GB RAM - demonstrates validation capabilities on modest hardware.

### pandas Backend (Excel Compatibility)

| File Size | Format | Rows | Time | Memory | Use Case |
|-----------|--------|------|------|--------|----------|
| 1 MB | CSV | 10K | <1 sec | ~50 MB | Small files |
| 100 MB | CSV | 1M | ~15 sec | ~150 MB | Medium files |
| 1 GB | Parquet | 10M | ~2 min | ~500 MB | Large files |

**Performance Tips:**
1. Install Polars: `pip install polars`
2. Use Polars backend: `--backend polars` (default)
3. Convert CSV to Parquet for 10x faster processing
4. Increase chunk size: `--chunk-size 200000` for large files

→ **[Performance Tuning Guide](docs/using-datak9/performance-tuning.md)**

---

## 🔌 Integration

### AutoSys Job Scheduling

```bash
# AutoSys JIL Definition
insert_job: VALIDATE_DATA
job_type: c
command: /apps/validation/run_validation.sh
condition: success(EXTRACT_DATA)
alarm_if_fail: yes

insert_job: LOAD_DATA
job_type: c
command: /apps/etl/load.sh
condition: success(VALIDATE_DATA)  # Only runs if DataK9 approves
```

### CI/CD Pipeline

```yaml
# GitHub Actions
- name: DataK9 Validation
  run: |
    python3 -m validation_framework.cli validate config.yaml
    if [ $? -ne 0 ]; then
      echo "DataK9 validation failed"
      exit 1
    fi
```

### Python API

```python
from validation_framework.core.engine import ValidationEngine

# Run validation
engine = ValidationEngine.from_config("config.yaml")
report = engine.run()

# Check results
if report.has_errors():
    raise ValueError(f"Validation failed: {report.total_errors} errors")
```

→ **[AutoSys Integration Guide](docs/using-datak9/autosys-integration.md)**
→ **[CI/CD Integration Guide](docs/using-datak9/cicd-integration.md)**
→ **[API Reference](docs/for-developers/api-reference.md)**

---

## 🧪 Testing

```bash
# Run all tests
pytest

# With coverage
pytest --cov=validation_framework --cov-report=html

# Open coverage report
open htmlcov/index.html
```

**Test Results:**
- 115+ tests passing
- 48% code coverage
- Unit, integration, and end-to-end tests
- Comprehensive profiler test suite

---

## 🤝 Contributing

Contributions welcome! See **[Contributing Guide](docs/for-developers/contributing.md)** for:
- Development environment setup
- Creating custom validations
- Writing tests
- Contribution guidelines

---

## 📝 License

MIT License - see [LICENSE](LICENSE) for details

---

## 👤 Author

**Daniel Edge** - Data quality enthusiast and guardian of clean data

---

## 🔗 Quick Links

**New Users:**
- [5-Minute Quickstart →](docs/getting-started/quickstart-5min.md)
- [CLI Reference →](CLI_GUIDE.md)
- [Validation Reference →](VALIDATION_REFERENCE.md)
- [DataK9 Studio →](https://raw.githack.com/danieledge/data-validation-tool/main/datak9-studio.html)

**Power Users:**
- [Performance Tuning →](docs/using-datak9/performance-tuning.md)
- [Large Files Guide →](docs/using-datak9/large-files.md)
- [AutoSys Integration →](docs/using-datak9/autosys-integration.md)

**Developers:**
- [Architecture →](docs/for-developers/architecture.md)
- [Custom Validations →](docs/for-developers/custom-validations.md)
- [API Reference →](docs/for-developers/api-reference.md)

**Support:**
- [GitHub Issues →](https://github.com/danieledge/data-validation-tool/issues)
- [Error Codes →](docs/reference/error-codes.md)
- [FAQ →](docs/using-datak9/faq.md)

---

<div align="center">
  <strong>🐕 DataK9 - Your K9 guardian for data quality</strong>
</div>
