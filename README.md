<div align="center">
  <img src="resources/images/datak9-web.png" alt="DataK9 Logo" width="300">

  # DataK9 Data Quality Framework

  ## 🐕 Your K9 guardian for data quality

  **Data validation for files and databases**

  [![Version 0.2.0](https://img.shields.io/badge/version-0.2.0-blue.svg)](#changelog)
  [![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
  [![Tests: 115+ passing](https://img.shields.io/badge/tests-115%2B%20passing-brightgreen.svg)](tests/)
  [![Coverage: 48%](https://img.shields.io/badge/coverage-48%25-yellow.svg)](htmlcov/)

  **[Quick Start](#-quick-start-3-minutes)** • **[Documentation](docs/)** • **[Examples](examples/)** • **[CLI Guide](CLI_GUIDE.md)**
</div>

---

## 🚀 Quick Start (3 Minutes)

```bash
# 1. Install
cd data-validation-tool
pip install -r requirements.txt
pip install polars  # Optional: 5-10x faster

# 2. Profile your data (auto-generates validation config)
python3 -m validation_framework.cli profile data.csv

# 3. Run validations
python3 -m validation_framework.cli validate config.yaml
```

**→ [5-Minute Quickstart Guide](docs/getting-started/quickstart-5min.md)**

---

## ✨ What is DataK9?

DataK9 is a Python framework for validating data quality before loading to databases, data warehouses, or analytics platforms. Like a well-trained K9 unit, DataK9 vigilantly guards your data, sniffing out quality issues before they become problems.

### Key Capabilities

- ✅ **Validates files & databases** - CSV, Excel, JSON, Parquet, PostgreSQL, MySQL, SQL Server, Oracle, SQLite
- ✅ **Handles massive datasets** - 200GB+ files with memory-efficient processing (tested on 357M rows)
- ✅ **35 built-in validations** - File, Schema, Field, Record, Advanced, Cross-File, Database, Statistical
- ✅ **Visual IDE** - DataK9 Studio for point-and-click configuration
- ✅ **High performance** - Polars backend for 5-10x faster processing
- ✅ **Enterprise-ready** - AutoSys/CI/CD integration, proper exit codes, JSON output
- ✅ **Date/time patterns** - Automatic timestamping prevents file overwrites, improves audit trails

---

## 📚 Documentation

<details>
<summary><b>📖 Complete Documentation Index</b> ▸ <i>Click to expand</i></summary>

### 🚀 Getting Started
- **[5-Minute Quickstart](docs/getting-started/quickstart-5min.md)** - Get running fast
- **[Installation Guide](docs/getting-started/installation.md)** - Detailed setup
- **[File Quick Start](docs/reference/quick-reference/FILE_QUICKSTART.md)** - CSV, Excel, JSON, Parquet 📁
- **[Database Quick Start](docs/guides/database/DATABASE_QUICKSTART.md)** - PostgreSQL, MySQL, SQL Server 🗄️

### 📖 Quick References
- **[CLI Reference](CLI_GUIDE.md)** - All command-line options
- **[Validation Reference](VALIDATION_REFERENCE.md)** - All 35 validation types
- **[YAML Reference](docs/reference/yaml-reference.md)** - Config file syntax
- **[Validation Compatibility](docs/reference/VALIDATION_COMPATIBILITY.md)** - Files vs databases

### 🎯 Specialized Guides

**Performance:** [Optimization Guide](docs/guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md) • [Polars Backend](docs/guides/performance/POLARS_BACKEND_GUIDE.md) • [Chunk Sizing](docs/guides/performance/CHUNK_SIZE_GUIDE.md)

**Database:** [Validation Guide](docs/guides/database/DATABASE_VALIDATION_GUIDE.md) • [Safety Features](docs/guides/database/DATABASE_SAFETY.md) • [Credential Security](docs/guides/database/DATABASE_CREDENTIALS_SECURITY.md)

**Advanced:** [Understanding CDAs](docs/guides/advanced/UNDERSTANDING_CDAS.md) • [CDA Gap Analysis](docs/guides/advanced/CDA_GAP_ANALYSIS_GUIDE.md) • [Cross-File Validation](docs/guides/advanced/CROSS_FILE_VALIDATION_QUICK_REFERENCE.md)

### 👥 Using DataK9
- **[Configuration Guide](docs/using-datak9/configuration-guide.md)** - YAML syntax
- **[Data Profiling](docs/using-datak9/data-profiling.md)** - Analyze data quality
- **[DataK9 Studio](docs/using-datak9/studio-guide.md)** - Visual IDE
- **[Best Practices](docs/using-datak9/best-practices.md)** - Recommended patterns
- **[Large Files](docs/using-datak9/large-files.md)** - Handle 200GB+ datasets
- **[AutoSys Integration](docs/using-datak9/autosys-integration.md)** - Job scheduling
- **[CI/CD Integration](docs/using-datak9/cicd-integration.md)** - GitHub Actions, GitLab
- **[Troubleshooting](docs/using-datak9/troubleshooting.md)** - Common issues
- **[FAQ](docs/using-datak9/faq.md)** - Frequently asked questions

### 💻 For Developers
- **[Architecture](docs/for-developers/architecture.md)** - System design
- **[Custom Validations](docs/for-developers/custom-validations.md)** - Build your own
- **[API Reference](docs/for-developers/api-reference.md)** - Python API
- **[Testing Guide](docs/for-developers/testing-guide.md)** - Write tests
- **[Contributing](docs/for-developers/contributing.md)** - Help improve DataK9

**→ [Complete Documentation Index with Reading Paths](docs/README.md)**

</details>

---

## 🎯 Key Features

<details>
<summary><b>📁 File & Database Validation</b> ▸ <i>Click to expand</i></summary>

### File Formats Supported
- **CSV** - Any delimiter, custom encoding, headers
- **Excel** - .xlsx, .xls with multi-sheet support
- **JSON** - Line-delimited and array formats
- **Parquet** - Columnar format (10x faster than CSV)

### Database Support
- **PostgreSQL** - Full validation support
- **MySQL** - All validation types
- **SQL Server** - Native driver
- **Oracle** - cx_Oracle integration
- **SQLite** - Lightweight databases

**→ [File Quick Start](docs/reference/quick-reference/FILE_QUICKSTART.md)** • **[Database Quick Start](docs/guides/database/DATABASE_QUICKSTART.md)**

</details>

<details>
<summary><b>✅ 35 Built-In Validations</b> ▸ <i>Click to expand</i></summary>

DataK9 includes 35 built-in validation types across 10 categories:

### File-Level Validations
- FileSizeCheck, RowCountCheck, FileExistsCheck, EncodingCheck

### Schema Validations
- SchemaMatchCheck, ColumnPresenceCheck, DataTypeCheck, ColumnOrderCheck

### Field-Level Validations
- MandatoryFieldCheck, RegexCheck, ValidValuesCheck, RangeCheck, DateFormatCheck, StringLengthCheck, NumericPrecisionCheck

### Record-Level Validations
- UniqueKeyCheck, DuplicateRowCheck, CrossFieldComparisonCheck, RecordCompletenessCheck

### Advanced Validations
- InlineBusinessRuleCheck, InlineLookupCheck, ConditionalValidation

### Cross-File Validations
- ReferentialIntegrityCheck, CrossFileAggregateCheck

### Database Validations
- TableExistsCheck, RecordCountComparisonCheck, DatabaseSchemaCheck

### Temporal Validations
- FreshnessCheck, TemporalConsistencyCheck

### Statistical Validations
- StatisticalOutlierCheck, CorrelationCheck, DistributionCheck

### Custom Validations
- Easy plugin architecture for custom validation logic

**→ [Complete Validation Reference](VALIDATION_REFERENCE.md)**

</details>

<details>
<summary><b>📊 Data Profiling</b> ▸ <i>Click to expand</i></summary>

Automatically analyze your data and generate validation configurations:

```bash
python3 -m validation_framework.cli profile data.csv
```

**Features:**
- **Enhanced analysis** - Distribution, anomaly detection, temporal patterns, dependency discovery
- **User-friendly** - Plain-language interpretations for non-technical users
- **PII detection** - Automatic flagging of emails, phones, SSN, credit cards
- **Context-aware** - Smart validation suggestions based on profiled data
- **Auto-generate configs** - Creates YAML validation configuration automatically

**Outputs:**
- Interactive HTML report with expandable cards
- JSON data for programmatic access
- Auto-generated YAML validation config

**→ [Data Profiling Guide](docs/using-datak9/data-profiling.md)**

</details>

<details>
<summary><b>🎨 DataK9 Studio (Visual IDE)</b> ▸ <i>Click to expand</i></summary>

Point-and-click configuration builder with no coding required:

- **Monaco Editor** - VS Code-powered YAML editor with syntax highlighting
- **Visual validation builder** - Drag-and-drop interface
- **Profiling integration** - Profile data directly in Studio
- **Documentation generator** - Export validation specs as HTML
- **Single HTML file** - 170KB, self-contained, no backend required
- **Responsive design** - Desktop, tablet, mobile layouts
- **Keyboard shortcuts** - Ctrl+S (save), Ctrl+N (new), Ctrl+B (sidebar)

**Location:** `data-validation-tool/datak9-studio.html` (open in browser)

**→ [DataK9 Studio Guide](docs/using-datak9/studio-guide.md)**

</details>

<details>
<summary><b>🔍 CDA Gap Analysis</b> ▸ <i>Click to expand</i></summary>

Track **Critical Data Attributes** (fields essential for regulatory compliance, financial accuracy, or business operations):

- Define critical fields inline with file configurations
- Detect validation coverage gaps automatically
- Generate audit-ready HTML reports
- CI/CD integration with `--fail-on-gaps`

```bash
python3 -m validation_framework.cli cda-analysis config.yaml
```

**Example:**
```yaml
files:
  - name: "transactions"
    critical_data_attributes:
      - field: "transaction_id"
        description: "Unique transaction identifier"
        regulatory_reference: "SOX Section 404"

      - field: "amount"
        description: "Transaction amount"
        owner: "Finance Team"

    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["transaction_id"]
      # amount not validated - will be flagged as GAP!
```

**→ [Understanding CDAs - What & Why](docs/guides/advanced/UNDERSTANDING_CDAS.md)**
**→ [CDA Gap Analysis Guide - How](docs/guides/advanced/CDA_GAP_ANALYSIS_GUIDE.md)**

</details>

<details>
<parameter name="summary"><b>📅 Date/Time Pattern Support</b> ▸ <i>Click to expand</i></summary>

Prevent file overwrites and improve audit trails with automatic date/time pattern substitution:

```bash
# CLI with patterns
python3 -m validation_framework.cli validate config.yaml \
  -o "reports/{date}/validation_{time}.html" \
  -j "results/{timestamp}.json"

# YAML config with patterns
output:
  html_report: "reports/{job_name}_{timestamp}.html"
  json_summary: "results/{date}/{job_name}_{time}.json"
```

**Supported Patterns:**

| Pattern | Example | Use Case |
|---------|---------|----------|
| `{date}` | `2025-11-22` | Daily reports |
| `{time}` | `14-30-45` | Multiple runs per day |
| `{timestamp}` | `20251122_143045` | Unique per run ⭐ |
| `{datetime}` | `2025-11-22_14-30-45` | Combined |
| `{job_name}` | `My_Validation` | Organization |
| `{file_name}` | `customers` | File-specific (profile) |
| `{table_name}` | `orders` | Table-specific (database) |

**Benefits:**
- ✅ No file overwrites (each run gets unique timestamp)
- ✅ Better audit trails (filenames show when validation ran)
- ✅ Organized outputs (automatic directory structures)
- ✅ Fully backward compatible (existing configs work unchanged)

**→ [CLI Guide - Pattern Documentation](CLI_GUIDE.md#date-time-patterns)**

</details>

---

## 🚀 Performance

<details>
<summary><b>⚡ Benchmarks (Polars Backend - Recommended)</b> ▸ <i>Click to expand</i></summary>

| File Size | Format | Rows | Time | Memory | Platform |
|-----------|--------|------|------|--------|----------|
| 1 MB | CSV | 10K | <1 sec | ~10 MB | Any |
| 100 MB | CSV | 1M | ~10 sec | ~100 MB | Any |
| 1 GB | Parquet | 10M | ~30 sec | ~200 MB | Desktop |
| 5.1 GB | Parquet | 179M | ~5 min | ~10 GB | Desktop |
| **10.1 GB** | **Parquet** | **357M** | **~55 min** | **~3.5 GB** | **Raspberry Pi 4** |

**Large-Scale Test:** 357M rows, 30 comprehensive validations (cross-file, ML anomaly detection, statistical tests) on Raspberry Pi 4 with only 4GB RAM - demonstrates validation capabilities on modest hardware.

### Performance Tips

1. **Install Polars** for 5-10x speedup:
   ```bash
   pip install polars
   ```

2. **Use Polars backend** (default):
   ```bash
   python3 -m validation_framework.cli validate config.yaml --backend polars
   ```

3. **Convert CSV to Parquet** for 10x faster processing

4. **Increase chunk size** for large files:
   ```bash
   python3 -m validation_framework.cli validate config.yaml --chunk-size 200000
   ```

**→ [Performance Optimization Guide](docs/guides/performance/PERFORMANCE_OPTIMIZATION_GUIDE.md)**

</details>

---

## 🔌 Integration

<details>
<summary><b>🔄 AutoSys Job Scheduling</b> ▸ <i>Click to expand</i></summary>

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
condition: success(VALIDATE_DATA)
```

**→ [AutoSys Integration Guide](docs/using-datak9/autosys-integration.md)**

</details>

<details>
<summary><b>🔧 CI/CD Pipelines</b> ▸ <i>Click to expand</i></summary>

### GitHub Actions

```yaml
name: Data Validation
on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install polars

      - name: Run validation
        run: |
          python3 -m validation_framework.cli validate config.yaml \
            -j validation_summary.json \
            -o validation_report.html

      - name: Upload reports
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: validation-reports
          path: |
            validation_summary.json
            validation_report.html
```

**→ [CI/CD Integration Guide](docs/using-datak9/cicd-integration.md)**

</details>

---

## 📦 Installation

<details>
<summary><b>Installation Instructions</b> ▸ <i>Click to expand</i></summary>

### Basic Installation

```bash
cd data-validation-tool
pip install -r requirements.txt
```

### Recommended: High-Performance Setup

```bash
pip install -r requirements.txt
pip install polars  # 5-10x faster processing
```

### Database Support (Optional)

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install mysql-connector-python

# SQL Server
pip install pyodbc

# Oracle
pip install cx_Oracle
```

### Requirements

- **Python:** 3.8 or higher
- **OS:** Linux, macOS, Windows
- **Memory:** 2GB minimum (4GB+ recommended for large files)

**→ [Complete Installation Guide](docs/getting-started/installation.md)**

</details>

---

## 💡 Examples

<details>
<summary><b>Quick Examples</b> ▸ <i>Click to expand</i></summary>

### Basic File Validation

```yaml
validation_job:
  name: "Customer Data Validation"

files:
  - name: "customers"
    path: "customers.csv"
    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["customer_id", "email", "first_name", "last_name"]

      - type: "RegexCheck"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      - type: "UniqueKeyCheck"
        params:
          fields: ["customer_id"]
```

### Database Validation

```yaml
validation_job:
  name: "Customer Database Validation"

database:
  connection_string: "postgresql://localhost/mydb"
  tables:
    - name: "customers"
      validations:
        - type: "RecordCountCheck"
          params:
            min_records: 1

        - type: "MandatoryFieldCheck"
          params:
            fields: ["customer_id", "email"]
```

**More examples:**
- **[Finance](docs/examples/finance.md)** - Banking, AML validation
- **[Healthcare](docs/examples/healthcare.md)** - HIPAA-compliant data
- **[E-Commerce](docs/examples/ecommerce.md)** - Orders, inventory

**→ [Examples Directory](examples/)**

</details>

---

## 🛠️ CLI Commands

<details>
<summary><b>Common Commands</b> ▸ <i>Click to expand</i></summary>

### Validate Data

```bash
python3 -m validation_framework.cli validate config.yaml
```

### Profile Data

```bash
python3 -m validation_framework.cli profile data.csv
```

### CDA Gap Analysis

```bash
python3 -m validation_framework.cli cda-analysis config.yaml
```

### List Available Validations

```bash
python3 -m validation_framework.cli list-validations
```

### Generate Sample Config

```bash
python3 -m validation_framework.cli init-config
```

**→ [Complete CLI Reference](CLI_GUIDE.md)**

</details>

---

## 🤝 Contributing

We welcome contributions! Please see our **[Contributing Guide](docs/for-developers/contributing.md)** for details.

### Development Setup

```bash
git clone https://github.com/danieledge/DataK9.git
cd DataK9/data-validation-tool
pip install -r requirements.txt
pip install -r requirements-dev.txt  # Testing dependencies
```

### Running Tests

```bash
./run_tests.sh  # Interactive test menu
# or
pytest -v  # Run all tests
pytest --cov --cov-report=html  # With coverage
```

---

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details

---

## 🆘 Support

- **[FAQ](docs/using-datak9/faq.md)** - Frequently asked questions
- **[Troubleshooting](docs/using-datak9/troubleshooting.md)** - Common issues
- **[GitHub Issues](https://github.com/danieledge/DataK9/issues)** - Report bugs or request features

---

## 📊 Project Status

- **Version:** 0.2.0
- **Status:** Active development
- **Tests:** 115+ passing (48% coverage, growing)
- **Python:** 3.8+
- **Production Tested:** 357M row dataset on Raspberry Pi 4

---

<div align="center">

**🐕 Guard your data pipelines with DataK9**

**Your K9 guardian for data quality**

[Documentation](docs/) • [Quick Start](docs/getting-started/quickstart-5min.md) • [CLI Guide](CLI_GUIDE.md) • [Examples](examples/)

---

**Copyright © 2025 Daniel Edge**

</div>
