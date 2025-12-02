<div align="center">
  <img src="resources/images/datak9-web.png" alt="DataK9 Logo" width="300">

  # DataK9 Data Quality Framework

  ## 🐕 Your K9 guardian for data quality

  **Data validation for files and databases**

  [![Version 0.2.0-beta](https://img.shields.io/badge/version-0.2.0--beta-orange.svg)](#changelog)
  [![Status: Beta](https://img.shields.io/badge/status-Beta%20%7C%20WIP-orange.svg)](#️-beta-software---testing-required)
  [![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
  [![Tests: 900+ passing](https://img.shields.io/badge/tests-900%2B%20passing-brightgreen.svg)](tests/)
  [![Coverage: 48%](https://img.shields.io/badge/coverage-48%25-yellow.svg)](htmlcov/)
  [![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
  [![Commercial Use: ✓](https://img.shields.io/badge/Commercial%20Use-✓%20Unrestricted-brightgreen.svg)](#commercial-use)

  **[Quick Start](#-quick-start-3-minutes)** • **[Documentation](docs/)** • **[Examples](examples/)** • **[CLI Guide](CLI_GUIDE.md)**
</div>

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

## 🆕 What's New: Profiler Overhaul (v1.55)

The DataK9 Profiler has received a **major enhancement** with intelligent semantic classification:

| Feature | Description |
|---------|-------------|
| **Dual Ontology Classification** | FIBO (financial) + Schema.org (general) for comprehensive semantic understanding |
| **Smart Validation Suggestions** | Semantic-aware rules that transfer across datasets (Age: 0-120, not 0.42-80) |
| **Binary Flag Detection** | Auto-classifies 0/1 columns as Boolean with BooleanCheck suggestions |
| **Intelligent UniqueKeyCheck** | Uses semantic types to avoid false positives (names aren't unique identifiers) |
| **Executive HTML Reports** | Redesigned with plain-English explanations and consolidated sampling info |

**Example improvements:**
- `Age` field → RangeCheck 0-120 (human-sensible) instead of exact observed range
- `Survived` (0/1) → BooleanCheck instead of ValidValuesCheck with string values
- `Name` → No UniqueKeyCheck (correctly identified as `schema:name`, not an identifier)
- `Fare` → Non-negative only (no restrictive upper bound for monetary fields)

**[Read the full Profiler Guide →](docs/using-datak9/data-profiling.md)**

---

## 🔥 Killer Features

<details>
<summary><b>🔍 Data Profiling</b> - Know your data before you validate</summary>

| Feature | Description |
|---------|-------------|
| **FIBO Semantic Intelligence** | Auto-tags columns using Financial Industry Business Ontology (transaction, currency, counterparty) |
| **ML-Powered Anomaly Detection** | Isolation Forest, DBSCAN clustering, correlation analysis find hidden issues |
| **PII Auto-Detection** | Flags emails, phones, SSN, credit cards, account numbers automatically |
| **Smart Validation Suggestions** | Generates ready-to-use YAML configs based on your data patterns |
| **Memory-Efficient at Scale** | Profile 200GB+ files with ~400MB RAM (tested on 357M rows) |
| **Executive HTML Reports** | Interactive dashboards with plain-English explanations |

```bash
python3 -m validation_framework.cli profile data.csv --beta-ml
```
</details>

<details>
<summary><b>✓ Data Validation</b> - 35 validations across 10 categories</summary>

| Feature | Description |
|---------|-------------|
| **Declarative YAML** | Human-readable configs - no code required |
| **Critical Data Attributes** | CDA gap analysis for regulatory compliance (SOX, Basel) |
| **Cross-File Validation** | Referential integrity between related datasets |
| **Database-Native Checks** | Primary keys, foreign keys, constraint validation |
| **Statistical Validations** | Distribution, correlation, anomaly thresholds |
| **Conditional Rules** | Apply validations only when conditions are met |
| **CI/CD Integration** | Proper exit codes, JSON output, `--fail-on-gaps` flags |

```bash
python3 -m validation_framework.cli validate config.yaml --output-json results.json
```
</details>

<details>
<summary><b>🎨 DataK9 Studio</b> - Visual configuration, no backend required</summary>

| Feature | Description |
|---------|-------------|
| **Zero Installation** | Single HTML file, runs in any browser |
| **Monaco Editor** | VS Code-style YAML editing with syntax highlighting |
| **Point-and-Click Config** | Build validations visually, no YAML knowledge needed |
| **Import Profiles** | Load profiler JSON, auto-suggest validations |
| **Multi-File Projects** | Manage complex validation jobs with multiple files |
| **Responsive Design** | Works on desktop, tablet, and mobile |

```
Just open datak9-studio.html in your browser!
```
</details>

---

<a id="commercial-use"></a>

## ✅ Commercial Use

**DataK9 is 100% free for commercial use without restrictions.**

- ✓ **MIT License** - Use in any commercial environment
- ✓ **All Dependencies** - Permissive licenses (MIT, BSD-3-Clause, Apache-2.0)
- ✓ **FIBO Ontology** - MIT License for semantic intelligence
- ✓ **No Copyleft** - No GPL/AGPL restrictions
- ✓ **No Fees** - Forever free
- ✓ **Full Attribution** - See [NOTICE](NOTICE) and [DEPENDENCIES.md](docs/for-developers/DEPENDENCIES.md)

**Enterprise Ready:** Deploy in financial services, healthcare, government, or any commercial setting without licensing concerns.

**Learn More:** [Commercial Use Guide](docs/for-developers/DEPENDENCIES.md)

---

## ⚠️ Beta Software - Testing Required

**IMPORTANT: DataK9 is currently in BETA.**

- 🧪 **Work in Progress** - Active development, features may change
- ✅ **Extensive Testing Required** - Thoroughly test with your data before deploying
- 🔍 **Validate Results** - Verify validation outputs match your expectations
- 📊 **Start Small** - Test on sample datasets before running on production data
- 🐛 **Report Issues** - Found a bug? [Open an issue](https://github.com/danieledge/DataK9/issues)

**This software is provided "AS IS" without warranty. See [LICENSE](LICENSE) for details.**

**Before Deployment:**
1. Test all validations with representative sample data
2. Verify profiler output accuracy
3. Test memory usage with your largest datasets
4. Review and customize auto-generated configs
5. Validate in non-production environment first

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

## 🎯 Why DataK9 Exists

### The Problem Every Organization Faces

**Data quality issues are universal, expensive, and preventable.**

Every organization struggles with the same challenges:
- 📊 **"Garbage in, garbage out"** - Bad data leads to bad decisions, wasted time, and lost revenue
- 🔥 **Firefighting mode** - Discovering data issues after they're in production
- 💰 **Hidden costs** - Studies show poor data quality costs organizations 15-25% of revenue
- 🤯 **Complexity barrier** - Existing enterprise tools require expensive licenses, consultants, and months of training
- 🔒 **Vendor lock-in** - Proprietary platforms that don't adapt to your workflow
- ⏰ **Too late, too slow** - Finding out about data problems after they've already caused damage

### The Existing Solutions Landscape

**Excellent open source tools exist, but often with trade-offs:**

There are established data quality tools in the ecosystem (Great Expectations, dbt tests, etc.) that are powerful and well-maintained. However, many teams find:

- 📚 **Steep learning curve** - Complex abstractions and domain-specific languages requiring significant training time
- 🧩 **Over-engineered for simple needs** - Feature-rich frameworks that feel heavyweight for straightforward validation tasks
- 🔧 **Opinionated workflows** - Tools designed for specific tech stacks or methodologies (notebook-first, dbt-only, etc.)
- 📖 **Documentation overload** - Extensive documentation that can be overwhelming when you just need basic validation
- 🎯 **Configuration complexity** - Programmatic config generation when you just want readable YAML

**These are great tools with strong communities. They're the right choice for many teams.**

### Why DataK9 Exists

**For teams who want simplicity without sacrificing power.**

DataK9 was built for a different philosophy:
- ✅ **Free & Open** - MIT license, no vendor lock-in, zero cost
- ✅ **Simple by Design** - Human-readable YAML configs you can understand at a glance, no DSL to learn
- ✅ **Understand First, Validate Second** - Built-in profiler shows you what your data actually looks like before you write a single rule
- ✅ **Start Small, Scale Big** - Works on your laptop with sample data, scales to 200GB+ files without code changes
- ✅ **No Black Boxes** - Plain-language validation rules, clear error messages, transparent logic
- ✅ **Fits Your Workflow** - CLI for automation, Studio for visual config, works with your existing tools (not the other way around)
- ✅ **Low Barrier to Entry** - 5-minute quickstart to first validation, not days of setup

**DataK9 isn't trying to replace other tools—it's offering a simpler path for teams who don't need (or want) the complexity.**

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
- EmptyFileCheck, FileSizeCheck, RowCountRangeCheck, CSVFormatCheck

### Schema Validations
- SchemaMatchCheck, ColumnPresenceCheck

### Field-Level Validations
- MandatoryFieldCheck, RegexCheck, InlineRegexCheck, ValidValuesCheck, RangeCheck, DateFormatCheck, StringLengthCheck, NumericPrecisionCheck

### Record-Level Validations
- UniqueKeyCheck, DuplicateRowCheck, CrossFieldComparisonCheck, BlankRecordCheck

### Advanced Validations
- StatisticalOutlierCheck, AdvancedAnomalyDetectionCheck, InlineBusinessRuleCheck, InlineLookupCheck, CompletenessCheck, ConditionalValidation

### Cross-File Validations
- ReferentialIntegrityCheck, CrossFileComparisonCheck, CrossFileDuplicateCheck

### Database Validations
- SQLCustomCheck, DatabaseConstraintCheck, DatabaseReferentialIntegrityCheck

### Temporal Validations
- FreshnessCheck, BaselineComparisonCheck, TrendDetectionCheck

### Statistical Validations
- DistributionCheck, CorrelationCheck

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
<summary><b>📅 Date/Time Pattern Support</b> ▸ <i>Click to expand</i></summary>

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
- **Tests:** 900+ passing (48% coverage, growing)
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
