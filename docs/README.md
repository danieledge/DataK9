<div align="center">
  <img src="../resources/images/datak9-web.png" alt="DataK9 Logo" width="300">

  # DataK9 Documentation
  ## 🐕 Your K9 guardian for data quality
</div>

Welcome to **DataK9** - a production-grade data quality framework that guards your data pipelines with vigilance and precision. Like a K9 unit sniffing out problems before they escalate, DataK9 catches data quality issues before they reach production.

---

## 🎯 Choose Your Path

### 👥 Using DataK9
**I want to validate data, build quality checks, and integrate with my pipeline**

Whether you're a business analyst, data engineer, or QA professional, this guide covers everything you need to use DataK9 effectively.

**Start here:** [What is DataK9?](using-datak9/what-is-datak9.md)
**Then:** [5-Minute Quickstart](getting-started/quickstart-5min.md)
**Optimize:** [Polars Backend Guide](POLARS_BACKEND_GUIDE.md) - High Performance ⚡
**Configure:** [Configuration Guide](using-datak9/configuration-guide.md)
**Reference:** [Validation Reference](reference/validation-reference.md) (35 validations)

→ **[Using DataK9 Guide](using-datak9/README.md)**

---

### 💻 For Developers
**I want to understand the architecture, extend the framework, or contribute**

Deep dive into DataK9's internals, create custom validations, and contribute to the project.

**Start here:** [Architecture Overview](for-developers/architecture.md)
**Then:** [Creating Custom Validations](for-developers/custom-validations.md)
**Reference:** [API Documentation](for-developers/api-reference.md)

→ **[Developer Guide](for-developers/README.md)**

---

## 🔍 Quick Links

| What do you need? | Go here |
|-------------------|---------|
| Install DataK9 | [Installation Guide](getting-started/installation.md) |
| High-performance validation | [Polars Backend Guide](POLARS_BACKEND_GUIDE.md) ⚡ |
| Optimize large-scale validations | [Performance Optimization Guide](PERFORMANCE_OPTIMIZATION_GUIDE.md) 🚀 |
| See all 34 validation types | [Validation Reference](reference/validation-reference.md) |
| Real-world examples | [Examples & Recipes](examples/) |
| Optimize performance | [Performance Tuning](using-datak9/performance-tuning.md) |
| Troubleshoot an issue | [Troubleshooting Guide](using-datak9/troubleshooting.md) |
| FAQ | [Frequently Asked Questions](using-datak9/faq.md) |
| DataK9 Studio IDE | [Studio Guide](using-datak9/studio-guide.md) |
| Data Profiling | [Profiling Guide](using-datak9/data-profiling.md) |
| Best Practices | [Best Practices](using-datak9/best-practices.md) |
| AutoSys Integration | [AutoSys Integration](using-datak9/autosys-integration.md) |
| CI/CD Integration | [CI/CD Integration](using-datak9/cicd-integration.md) |

---

## 🐕 What is DataK9?

DataK9 is a production-grade data quality framework that validates data before it enters your warehouse or analytics platform. It helps you:

- ✅ **Catch data quality issues early** - Before they reach production
- ✅ **Define validation rules easily** - YAML configuration or visual IDE
- ✅ **Handle massive datasets** - 200GB+ files with memory-efficient processing
- ✅ **Generate actionable reports** - Beautiful HTML reports with dark theme
- ✅ **Integrate seamlessly** - AutoSys, CI/CD pipelines, Airflow
- ✅ **Support multiple formats** - CSV, Excel, Parquet, JSON, databases

---

## 🚀 Quick Start (5 Minutes)

### 1. Install DataK9

```bash
# Clone the repository
git clone https://github.com/danieledge/data-validation-tool.git
cd data-validation-tool

# Install dependencies
pip install -r requirements.txt

# Install DataK9
pip install -e .
```

### 2. Create Your First Validation

Create `my_validation.yaml`:

```yaml
validation_job:
  name: "Customer Data Quality Check"
  version: "1.0"

  files:
    - name: "customers"
      path: "data/customers.csv"
      format: "csv"

      validations:
        # File must not be empty
        - type: "EmptyFileCheck"
          severity: "ERROR"

        # Email addresses must be valid
        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "email"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

        # Age must be reasonable
        - type: "RangeCheck"
          severity: "WARNING"
          params:
            field: "age"
            min_value: 18
            max_value: 120

  output:
    html_report: "validation_report.html"
    json_summary: "validation_summary.json"
    fail_on_error: true
```

### 3. Run the Validation

```bash
[DataK9] 🐕
python3 -m validation_framework.cli validate my_validation.yaml
```

### 4. View the Report

Open `validation_report.html` in your browser to see:
- ✅ Passed validations (green)
- ❌ Failed validations (red) with sample failures
- ⚠️ Warnings (yellow) requiring review
- 📊 Summary statistics and charts

→ **[Complete Quickstart Guide](getting-started/quickstart-5min.md)**

---

## 📖 Popular Topics

### Essential Reading
- [Best Practices: ERROR vs WARNING](using-datak9/best-practices.md#error-vs-warning)
- [Handling Large Files (200GB+)](using-datak9/large-files.md)
- [AutoSys Integration](using-datak9/autosys-integration.md)
- [CI/CD Integration](using-datak9/cicd-integration.md)

### Data Profiling
- [Data Profiling Guide](using-datak9/data-profiling.md)
- [Auto-Generate Validation Configs](using-datak9/data-profiling.md#auto-generate-configs)
- [Understanding Profile Reports](using-datak9/data-profiling.md#reading-reports)

### Advanced Topics
- [Conditional Validations](using-datak9/configuration-guide.md#conditional-logic)
- [Cross-File Validations](using-datak9/configuration-guide.md#cross-file)
- [Database Validations](using-datak9/configuration-guide.md#database)
- [Statistical Anomaly Detection](using-datak9/configuration-guide.md#statistical)

---

## 🛡️ Key Features

### 35 Built-in Validation Types Across 10 Categories

**File-Level (3):** EmptyFileCheck, RowCountRangeCheck, FileSizeCheck
**Schema (2):** SchemaMatchCheck, ColumnPresenceCheck
**Field-Level (5):** MandatoryFieldCheck, RegexCheck, ValidValuesCheck, RangeCheck, DateFormatCheck
**Record-Level (3):** DuplicateRowCheck, BlankRecordCheck, UniqueKeyCheck
**Advanced (9):** StatisticalOutlierCheck, CrossFieldComparisonCheck, FreshnessCheck, CompletenessCheck, StringLengthCheck, NumericPrecisionCheck, InlineRegexCheck, InlineBusinessRuleCheck, InlineLookupCheck
**Conditional (1):** ConditionalValidation with if-then-else logic
**Cross-File (4):** ReferentialIntegrityCheck, CrossFileComparisonCheck, CrossFileDuplicateCheck, CrossFileKeyCheck
**Database (3):** SQLCustomCheck, DatabaseReferentialIntegrityCheck, DatabaseConstraintCheck
**Temporal (2):** BaselineComparisonCheck, TrendDetectionCheck
**Statistical (3):** DistributionCheck, CorrelationCheck, AdvancedAnomalyDetectionCheck

→ **[Complete Validation Catalog](reference/validation-reference.md)**

### DataK9 Studio - Visual IDE

Build validation rules without writing YAML:
- 🎨 Visual validation builder with drag-and-drop
- 💻 Live YAML preview with syntax highlighting
- 📁 Project management with file organization
- 🔄 Import/export configurations
- 📱 Mobile-responsive design
- 🌙 Professional dark theme

→ **[DataK9 Studio Guide](using-datak9/studio-guide.md)**

### Memory-Efficient Processing

- Handles 200GB+ files with ~400MB memory
- Chunked processing (default: 50,000 rows/chunk)
- Configurable chunk sizes
- Parallel processing support

### Enterprise-Ready

- ✅ AutoSys integration with proper exit codes
- ✅ CI/CD pipeline ready (JSON output)
- ✅ Airflow/orchestration compatible
- ✅ Production-grade error handling
- ✅ Comprehensive logging

### Multiple Format Support

- **CSV** - Any delimiter, quoted fields, headers
- **Excel** - XLS, XLSX, multiple sheets
- **Parquet** - Columnar format (10x faster than CSV)
- **JSON** - Standard arrays, JSON Lines (JSONL/NDJSON)
- **Databases** - Direct SQL queries

---

## 💡 Common Use Cases

### Financial Data Validation
```yaml
# Account balance validation
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "account_balance"
    min_value: 0

# UK sort code format
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "sort_code"
    pattern: "^[0-9]{2}-[0-9]{2}-[0-9]{2}$"
```

### Customer Data Validation
```yaml
# Email format
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

# Valid countries only
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "country_code"
    allowed_values: ["US", "UK", "CA", "AU", "DE", "FR"]
```

### Transaction Data Validation
```yaml
# Transaction amount must be positive
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "amount > 0"
    message: "Transaction amount must be positive"

# Date cannot be in future
- type: "FreshnessCheck"
  severity: "ERROR"
  params:
    date_field: "transaction_date"
    max_age_days: 0
```

→ **[More Examples](examples/)**

---

## 🔧 CLI Reference

### Validate Data
```bash
# Run validation
python3 -m validation_framework.cli validate config.yaml

# With custom output directory
python3 -m validation_framework.cli validate config.yaml --output-dir ./reports/

# Verbose mode
python3 -m validation_framework.cli validate config.yaml --verbose
```

### List Available Validations
```bash
python3 -m validation_framework.cli list-validations
```

### Profile Data Files
```bash
# Profile a data file
python3 -m validation_framework.cli profile data/customers.csv

# Generate auto-config
python3 -m validation_framework.cli profile data/customers.csv --auto-config
```

→ **[Complete CLI Reference](reference/cli-reference.md)**

---

## 📚 Documentation Structure

### Getting Started
- [Quickstart (5 minutes)](getting-started/quickstart-5min.md)
- [Installation](getting-started/installation.md)

### Using DataK9
- [What is DataK9?](using-datak9/what-is-datak9.md)
- [Configuration Guide](using-datak9/configuration-guide.md)
- [Validation Catalog](using-datak9/validation-catalog.md)
- [Best Practices](using-datak9/best-practices.md)
- [DataK9 Studio Guide](using-datak9/studio-guide.md)
- [Data Profiling](using-datak9/data-profiling.md)
- [Reading Reports](using-datak9/reading-reports.md)
- [Performance Tuning](using-datak9/performance-tuning.md)
- [AutoSys Integration](using-datak9/autosys-integration.md)
- [CI/CD Integration](using-datak9/cicd-integration.md)
- [Handling Large Files](using-datak9/large-files.md)
- [Troubleshooting](using-datak9/troubleshooting.md)
- [FAQ](using-datak9/faq.md)

### For Developers
- [Architecture](for-developers/architecture.md)
- [Custom Validations](for-developers/custom-validations.md)
- [Custom Loaders](for-developers/custom-loaders.md)
- [Custom Reporters](for-developers/custom-reporters.md)
- [API Reference](for-developers/api-reference.md)
- [Testing Guide](for-developers/testing-guide.md)
- [Contributing](for-developers/contributing.md)
- [Design Patterns](for-developers/design-patterns.md)

### Examples
- [Finance](examples/finance.md) - Banking, trading, AML validation
- [Healthcare](examples/healthcare.md) - HIPAA-compliant patient data
- [E-Commerce](examples/ecommerce.md) - Customer, order, inventory validation

### Reference
- [CLI Reference](reference/cli-reference.md)
- [Validation Reference](reference/validation-reference.md)
- [YAML Reference](reference/yaml-reference.md)
- [Error Codes](reference/error-codes.md)
- [Glossary](reference/glossary.md)

---

## 🆘 Need Help?

### Quick Help
- **[FAQ](using-datak9/faq.md)** - Frequently asked questions
- **[Troubleshooting](using-datak9/troubleshooting.md)** - Common issues and solutions
- **[Examples](examples/)** - Real-world validation configurations
- **[GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)** - Report bugs or request features

### Community
- **GitHub:** [danieledge/data-validation-tool](https://github.com/danieledge/data-validation-tool)
- **Issues:** [Report bugs or request features](https://github.com/danieledge/data-validation-tool/issues)
- **Contributions:** [See Contributing Guide](for-developers/contributing.md)

---

## 🏆 Why DataK9?

### Vigilant Data Guardian
Like a K9 unit that sniffs out problems before they escalate, DataK9:
- 🐕 **Detects issues early** - Before they reach production
- 🛡️ **Guards your pipelines** - Continuous data quality protection
- ✅ **Reliable validation** - Production-grade, enterprise-ready
- 🎯 **Trainable** - Configure to your specific needs
- 👥 **Friendly** - Accessible to all skill levels

### Production-Grade Performance
- Handles 200GB+ files efficiently
- Memory-efficient chunked processing
- Tested with 115+ unit tests (48% code coverage)
- Enterprise-ready with AutoSys/CI/CD integration

### Open Source & Free
- MIT License
- Community-driven development
- No vendor lock-in
- Extensible architecture

---

## 📊 Performance Benchmarks

**With Polars Backend (Recommended):**

| File Size | Format | Rows | Time | Memory | Platform |
|-----------|--------|------|------|--------|----------|
| 1 MB | CSV | 10K | <1 sec | ~10 MB | Any |
| 100 MB | CSV | 1M | ~10 sec | ~100 MB | Any |
| 1 GB | Parquet | 10M | ~30 sec | ~200 MB | Desktop |
| 5.1 GB | Parquet | 179M | ~5 min | ~10 GB | Desktop |
| 10.1 GB | Parquet | 357M | ~55 min | ~3.5 GB | Raspberry Pi 4 |

**Ultimate Showcase:** 357M rows, 30 comprehensive validations (including cross-file, ML anomaly detection, statistical tests) on Raspberry Pi 4 (4GB RAM) - proves enterprise validation on budget hardware.

**With pandas Backend (Excel Compatibility):**

| File Size | Format | Rows | Time | Memory | Notes |
|-----------|--------|------|------|--------|-------|
| 1 MB | CSV | 10K | <1 sec | ~50 MB | Fine for small files |
| 100 MB | CSV | 1M | ~15 sec | ~150 MB | OK |
| 1 GB | Parquet | 10M | ~2 min | ~500 MB | Good for medium files |
| 5.1 GB | Parquet | 179M | ~42 min | 15+ GB | Large files |

**Performance Tip:** Install Polars for high-performance validation: `pip install polars`

---

## 📝 License & Copyright

**Copyright:** © 2025 Daniel Edge and Contributors
**License:** MIT License
**Repository:** https://github.com/danieledge/data-validation-tool
**Author:** Daniel Edge

---

## 🚀 Next Steps

**New to DataK9?** → Start with our **[5-Minute Quickstart](getting-started/quickstart-5min.md)**

**Need to validate data?** → Read **[Using DataK9 Guide](using-datak9/README.md)**

**Want to use the visual IDE?** → Try **[DataK9 Studio](using-datak9/studio-guide.md)**

**Extending the framework?** → Understand the **[Architecture](for-developers/architecture.md)**

---

**🐕 Guard your data pipelines with DataK9 - Your K9 guardian for data quality**
