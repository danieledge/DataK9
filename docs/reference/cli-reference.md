# CLI Reference

**Complete Command-Line Interface Documentation**

DataK9's command-line interface provides powerful tools for data validation, profiling, and inspection. This reference documents all commands, options, and usage patterns.

---

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Global Options](#global-options)
4. [Commands](#commands)
   - [validate](#validate)
   - [profile](#profile)
   - [list-validations](#list-validations)
   - [cda-analysis](#cda-analysis)
   - [check-policy](#check-policy)
   - [list-policies](#list-policies)
   - [init-config](#init-config)
5. [Exit Codes](#exit-codes)
6. [Environment Variables](#environment-variables)
7. [Configuration Files](#configuration-files)
8. [Output Files](#output-files)
9. [Common Patterns](#common-patterns)
10. [Troubleshooting](#troubleshooting)

---

## Overview

### Command Structure

```bash
python3 -m validation_framework.cli <command> [options] [arguments]
```

### Available Commands

| Command | Purpose | Typical Use |
|---------|---------|-------------|
| `validate` | Execute validation rules | Production validation gates |
| `profile` | Analyze data files | Initial data discovery |
| `list-validations` | Show available validation types | Configuration planning |
| `cda-analysis` | CDA gap analysis | Compliance auditing |
| `check-policy` | Validate config against policy | Quality gates |
| `list-policies` | Show available policies | Policy selection |
| `init-config` | Generate sample config | Getting started |

### Quick Examples

```bash
# Validate data
python3 -m validation_framework.cli validate config.yaml

# Profile a CSV file
python3 -m validation_framework.cli profile data/customers.csv

# List all available validations
python3 -m validation_framework.cli list-validations
```

---

## Installation

### Prerequisites

```bash
# Python 3.9+
python3 --version

# Install DataK9
pip install -r requirements.txt
pip install -e .
```

### Verify Installation

```bash
# Should display help
python3 -m validation_framework.cli --help

# Should display version
python3 -m validation_framework.cli --version
```

---

## Global Options

Options available for all commands:

### `--help` / `-h`

Display help information.

```bash
# General help
python3 -m validation_framework.cli --help

# Command-specific help
python3 -m validation_framework.cli validate --help
```

### `--version`

Display DataK9 version.

```bash
python3 -m validation_framework.cli --version
# Output: DataK9 version 1.0.0
```

### `--verbose` / `-v`

Enable detailed output.

```bash
python3 -m validation_framework.cli validate config.yaml --verbose
```

**Output with --verbose:**
```
Loading configuration from config.yaml...
Validating 2 files with 15 total validations...
Processing customers.csv (10,000 rows)...
  ✅ MandatoryFieldCheck - PASSED
  ✅ EmailFormatCheck - PASSED
  ❌ AgeRangeCheck - FAILED (15 failures)
Processing orders.csv (5,000 rows)...
  ✅ OrderIdUniqueCheck - PASSED
  ⚠️  OrderDateFreshnessCheck - WARNING
Generating reports...
Validation completed in 12.5 seconds
```

**Output without --verbose:**
```
❌ Validation FAILED: 1 error, 1 warning
```

---

## Commands

## validate

Execute data validation based on YAML configuration.

### Syntax

```bash
python3 -m validation_framework.cli validate <config_file> [options]
```

### Arguments

#### `<config_file>` (Required)

Path to YAML configuration file.

```bash
# Absolute path
python3 -m validation_framework.cli validate /path/to/config.yaml

# Relative path
python3 -m validation_framework.cli validate configs/customers.yaml

# Current directory
python3 -m validation_framework.cli validate config.yaml
```

### Options

#### `--verbose` / `-v`

Enable detailed progress output.

```bash
python3 -m validation_framework.cli validate config.yaml --verbose
```

**Use Cases:**
- Debugging configuration issues
- Monitoring long-running validations
- Understanding validation execution flow

#### Backend Selection (Automatic)

DataK9 automatically selects the optimal backend based on file format and available libraries:

- **Polars** (default when installed) - Used for CSV, Parquet, JSON files
- **pandas** - Used for Excel files, or as fallback when Polars unavailable

```bash
# DataK9 automatically selects the best backend
python3 -m validation_framework.cli validate config.yaml
```

**Performance Characteristics:**

| Backend | Speed | Memory | Used For |
|---------|-------|--------|----------|
| **Polars** | **8x faster** | **50% less** | CSV, Parquet, JSON (auto-selected) |
| pandas | Baseline | Baseline | Excel files (auto-selected) |

**Polars is included** with DataK9 (installed via requirements.txt).

**Performance Benchmark** (179M rows, 5.1GB Parquet):
- **Polars**: 5 minutes, 10.2GB memory, 15/15 validations completed
- **pandas**: 42 minutes, 15.2GB+ memory, 12/15 validations (OOM)

**See:** [Performance Tuning](../using-datak9/performance-tuning.md)

#### `--html-output` / `-o`

Specify path for HTML report output.

```bash
python3 -m validation_framework.cli validate config.yaml -o report.html
```

**Examples:**
```bash
# Output to specific file
python3 -m validation_framework.cli validate config.yaml -o /var/reports/validation.html

# Output with date pattern
python3 -m validation_framework.cli validate config.yaml -o "reports/{date}/validation.html"
```

#### `--json-output` / `-j`

Specify path for JSON report output.

```bash
python3 -m validation_framework.cli validate config.yaml -j results.json
```

**Use Cases:**
- CI/CD pipelines (machine-readable output)
- Automated processing and integration
- Programmatic access to validation results

### CSV Processing Options

These options allow fine-grained control over CSV file parsing, matching the profiler's capabilities.

#### `--encoding` / `-e`

Specify file encoding (auto-detected if omitted).

```bash
python3 -m validation_framework.cli validate config.yaml --encoding utf-8-sig
```

**Common encodings:**
- `utf-8` - Standard UTF-8 (default)
- `utf-8-sig` - UTF-8 with BOM (common in Excel exports)
- `cp1252` - Windows Western European
- `latin-1` / `iso-8859-1` - Legacy encoding

**Use Cases:**
- Files exported from legacy systems
- Excel-generated CSVs with special characters
- International character sets

#### `--quoting`

Set CSV quoting mode to handle files with problematic quote characters.

```bash
python3 -m validation_framework.cli validate config.yaml --quoting none
```

**Options:**
- `minimal` - Quote only fields containing special characters (default)
- `all` - Quote all fields
- `none` - Don't use quotes (use when file has unescaped quotes causing parse errors)
- `nonnumeric` - Quote all non-numeric fields

**Use Cases:**
- Files with inconsistent quoting
- Legacy exports with unescaped quotes
- Tab-delimited files with embedded quotes

#### `--skip-rows`

Skip a number of rows before the header row.

```bash
python3 -m validation_framework.cli validate config.yaml --skip-rows 3
```

**Use Cases:**
- Files with metadata rows before the header (e.g., export timestamps, system info)
- Reports with title rows
- Multi-header files where only one header matters

**Note:** These options override any encoding/quoting/skip-rows settings in the YAML configuration file for all files in the job.

#### `--fail-on-warning`

Treat WARNING-severity failures as errors (exit code 2).

```bash
python3 -m validation_framework.cli validate config.yaml --fail-on-warning
```

**Behavior:**
- By default, only ERROR-severity failures cause non-zero exit
- With this flag, WARNING failures also cause validation to fail
- Useful for strict quality gates in CI/CD pipelines

#### `--no-optimize`

Disable single-pass optimization (use standard validation engine).

```bash
python3 -m validation_framework.cli validate config.yaml --no-optimize
```

**Use Cases:**
- Debugging validation issues
- When optimization causes unexpected behavior

### Exit Codes

The `validate` command uses exit codes for programmatic integration:

| Exit Code | Status | Meaning |
|-----------|--------|---------|
| `0` | SUCCESS | All validations passed |
| `1` | FAILURE | One or more ERROR-severity validations failed |
| `2` | ERROR | Configuration or runtime error |

**Examples:**

```bash
# Check exit code in bash
python3 -m validation_framework.cli validate config.yaml
if [ $? -eq 0 ]; then
    echo "✅ Validation passed"
    # Continue with data processing
else
    echo "❌ Validation failed"
    exit 1
fi

# Conditional execution
python3 -m validation_framework.cli validate config.yaml && \
    python3 process_data.py || \
    echo "Validation failed, skipping processing"
```

### Examples

#### Basic Validation

```bash
python3 -m validation_framework.cli validate config.yaml
```

#### Verbose Output to Custom Directory

```bash
python3 -m validation_framework.cli validate config.yaml \
    --verbose \
    --output-dir /var/reports/validation/
```

#### CI/CD Pipeline (JSON only, fail-fast)

```bash
python3 -m validation_framework.cli validate config.yaml \
    --no-html \
    --fail-fast \
    --output-dir artifacts/
```

#### Configuration Validation Only

```bash
python3 -m validation_framework.cli validate config.yaml --config-only
```

#### Production Validation with Logging

```bash
python3 -m validation_framework.cli validate config.yaml \
    --verbose \
    --output-dir logs/validation/ \
    2>&1 | tee validation.log
```

---

## profile

Analyze data files and generate profiling reports with auto-generated validation suggestions. The profiler provides comprehensive data analysis including semantic classification, anomaly detection, PII identification, and correlation analysis.

### Syntax

```bash
python3 -m validation_framework.cli profile <file_path> [options]
# Or for database sources:
python3 -m validation_framework.cli profile --database <connection_string> --table <table_name>
```

### Arguments

#### `<file_path>` (Optional if using --database)

Path to data file to profile.

```bash
# Profile CSV file
python3 -m validation_framework.cli profile data/customers.csv

# Profile Excel file
python3 -m validation_framework.cli profile data/sales.xlsx

# Profile Parquet file (recommended for large files)
python3 -m validation_framework.cli profile data/transactions.parquet

# Profile JSON file
python3 -m validation_framework.cli profile data/products.json
```

**Supported Formats:**
- CSV (`.csv`) - with auto-detected delimiter
- Excel (`.xlsx`, `.xls`)
- Parquet (`.parquet`) - recommended for large files
- JSON (`.json`, `.jsonl`)

### Core Options

#### `--html-output` / `-o`

Specify HTML report output path.

```bash
python3 -m validation_framework.cli profile data.csv -o profile_report.html
```

**Default:** `<filename>_profile_report_<date>.html`

#### `--json-output` / `-j`

Export profile results to JSON for programmatic use.

```bash
python3 -m validation_framework.cli profile data.csv -j profile.json
```

#### `--config-output` / `-c`

Specify path for auto-generated validation configuration YAML.

```bash
python3 -m validation_framework.cli profile data.csv -c validation_config.yaml
```

**Default:** `<filename>_validation_<timestamp>.yaml`

### Data Source Options

#### `--format` / `-f`

Explicitly specify file format (auto-detected if omitted).

```bash
python3 -m validation_framework.cli profile data.txt --format csv
```

**When to Use:** File extension doesn't match format, or files without extensions.

#### `--delimiter` / `-d`

Column delimiter for CSV files (auto-detected if omitted).

```bash
python3 -m validation_framework.cli profile data.tsv --delimiter "\t"
```

#### `--database` / `--db`

Connection string for database profiling.

```bash
python3 -m validation_framework.cli profile --database "postgresql://user:pass@host/db" --table customers
```

#### `--table` / `-t`

Table name for database profiling.

#### `--query` / `-q`

SQL query as alternative to table (for database sources).

```bash
python3 -m validation_framework.cli profile --db "postgresql://..." --query "SELECT * FROM orders WHERE date > '2024-01-01'"
```

### Processing Options

#### `--sample` / `-s`

Profile first N rows only (useful for very large files).

```bash
python3 -m validation_framework.cli profile large_file.csv --sample 100000
```

#### `--chunk-size`

Rows per processing chunk (auto-calculated based on available memory if omitted).

```bash
python3 -m validation_framework.cli profile data.csv --chunk-size 50000
```

#### `--analysis-sample-size`

Sample size threshold for internal analysis (default: 100,000 rows).

```bash
python3 -m validation_framework.cli profile data.csv --analysis-sample-size 200000
```

#### `--full-analysis`

Disable internal sampling for maximum accuracy (slower but more thorough).

```bash
python3 -m validation_framework.cli profile data.csv --full-analysis
```

### Enhancement Flags

**All enhancements are ENABLED by default since v1.54.** Use disable flags to turn them off.

#### `--disable-temporal`

Disable temporal analysis for datetime columns.

```bash
python3 -m validation_framework.cli profile data.csv --disable-temporal
```

#### `--disable-pii`

Disable PII detection with privacy risk scoring.

```bash
python3 -m validation_framework.cli profile data.csv --disable-pii
```

#### `--disable-correlation`

Disable enhanced multi-method correlation analysis.

```bash
python3 -m validation_framework.cli profile data.csv --disable-correlation
```

#### `--correlation-threshold`

Set the minimum absolute correlation coefficient to report. Default is 0.3 (Cohen's medium effect size). Only correlations with |r| > threshold are included in reports.

```bash
# Only report strong correlations (|r| > 0.7)
python3 -m validation_framework.cli profile data.csv --correlation-threshold 0.7

# Report all detectable correlations (|r| > 0.1)
python3 -m validation_framework.cli profile data.csv --correlation-threshold 0.1
```

**Correlation Strength Thresholds:**

| Range | Label |
|-------|-------|
| 0.9+ | Very Strong |
| 0.7 - 0.9 | Strong |
| 0.5 - 0.7 | Moderate |
| 0.3 - 0.5 | Weak |

**Note:** Can also be set via context file (see `--field-descriptions`). CLI value takes precedence over context file.

#### `--disable-all-enhancements`

Minimal profiling mode - disables temporal, PII, and correlation analysis.

```bash
python3 -m validation_framework.cli profile data.csv --disable-all-enhancements
```

### ML Options

#### `--no-ml`

Disable ML-based anomaly detection (Isolation Forest, Benford's Law, etc.).

```bash
python3 -m validation_framework.cli profile data.csv --no-ml
```

### Context & Metadata

#### `--field-descriptions` ⭐ NEW

YAML file providing friendly names, descriptions, and value labels for columns. This enables **context-aware anomaly explanations** and more readable reports.

```bash
python3 -m validation_framework.cli profile data.csv --field-descriptions context.yaml
```

**Why Use This:**
- Transform cryptic column names into readable labels
- Provide meaningful value labels for categorical codes
- Enable context-aware anomaly explanations
- Improve correlation insight readability

**YAML Format:**

```yaml
# context.yaml - Field descriptions for your dataset
field_descriptions:
  # Basic field with friendly name and description
  Pclass:
    friendly_name: "Passenger Class"
    description: "Ticket class indicating travel accommodation level"

  # Field with value labels for categorical codes
  Embarked:
    friendly_name: "Port of Embarkation"
    description: "Port where passenger boarded the ship"
    value_labels:
      "S": "Southampton"
      "C": "Cherbourg"
      "Q": "Queenstown"

  # Binary/flag field with semantic labels
  Survived:
    friendly_name: "Survival Status"
    value_labels:
      "0": "Did Not Survive"
      "1": "Survived"

  # Numeric field with domain context
  Fare:
    friendly_name: "Ticket Fare"
    description: "Price paid for ticket in British pounds"

  # Abbreviated field names
  SibSp:
    friendly_name: "Siblings/Spouses"
    description: "Number of siblings and spouses aboard"

  Parch:
    friendly_name: "Parents/Children"
    description: "Number of parents and children aboard"

# Profiler settings (optional)
profiler_settings:
  # Minimum correlation coefficient to report (default: 0.3)
  # CLI --correlation-threshold overrides this value
  correlation_threshold: 0.3
```

**How It Improves Reports:**

| Without Field Descriptions | With Field Descriptions |
|---------------------------|------------------------|
| "Pclass = 1 shows 7.5x higher Fare" | "1st Class shows 7.5x higher Ticket Fare" |
| "Anomaly in Fare for Pclass=3" | "Anomaly in Ticket Fare for 3rd Class passengers" |
| "Strong correlation: Pclass ↔ Fare" | "Strong correlation: Passenger Class ↔ Ticket Fare" |
| "SibSp affects survival rate" | "Siblings/Spouses affects survival rate" |

**Context-Aware Anomaly Detection:**

When you provide field descriptions, the profiler uses them for smarter anomaly explanations:

```
Without context:
  "Value 512.33 in Fare is 3.8σ above mean"

With context:
  "Ticket Fare of $512.33 is unusually high but expected for 1st Class passengers
   (average Fare for Passenger Class=1st Class is $84.15)"
```

**Example with Titanic Dataset:**

```bash
# Profile with context for readable insights
python3 -m validation_framework.cli profile titanic.csv \
  --field-descriptions titanic_context.yaml \
  -o titanic_profile.html

# See examples/titanic_field_descriptions.yaml for a complete example
```

### Memory Options

#### `--no-memory-check`

Disable memory safety warnings and checks. **Use with caution** - may cause out-of-memory errors on large files.

```bash
python3 -m validation_framework.cli profile huge_file.csv --no-memory-check
```

#### `--log-level`

Set logging verbosity.

```bash
python3 -m validation_framework.cli profile data.csv --log-level DEBUG
```

**Options:** `DEBUG`, `INFO`, `WARNING` (default), `ERROR`

### Output Files

The `profile` command generates:

#### 1. Profile Report (HTML)

**Filename:** `<filename>_profile_report_<date>.html`

**Contents:**
- Executive summary with quality score
- Semantic classification (FIBO + Schema.org)
- Column statistics and distributions
- Correlation analysis with insights
- ML-based anomaly findings
- PII detection results
- Validation suggestions with YAML snippets

#### 2. Validation Configuration (YAML)

**Filename:** `<filename>_validation_<timestamp>.yaml`

**Contents:**
- Auto-generated validation rules
- Based on semantic understanding and discovered patterns
- Ready to use with `validate` command

#### 3. JSON Profile (Optional)

**Filename:** Specified with `-j` flag

**Contents:**
- Complete profile data in machine-readable format
- Useful for programmatic analysis or integration

### Examples

#### Basic Profiling (All Enhancements Enabled)

```bash
python3 -m validation_framework.cli profile data/customers.csv -o profile.html
```

#### Profile with Context for Better Insights

```bash
python3 -m validation_framework.cli profile financial_data.csv \
  --field-descriptions field_context.yaml \
  -o readable_profile.html \
  -c validations.yaml
```

#### Full Analysis with ML Anomaly Detection

```bash
python3 -m validation_framework.cli profile transactions.parquet \
  --full-analysis \
  -o full_analysis.html \
  -j analysis.json
```

#### Large File with Sampling

```bash
python3 -m validation_framework.cli profile huge_file.csv \
  --sample 1000000 \
  -o quick_profile.html
```

#### Database Table Profiling

```bash
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/mydb" \
  --table customers \
  -o db_profile.html
```

#### Minimal Fast Profiling

```bash
python3 -m validation_framework.cli profile data.csv \
  --disable-all-enhancements \
  --no-ml \
  -o minimal.html
```

#### Complete Output Suite

```bash
python3 -m validation_framework.cli profile data.csv \
  --field-descriptions context.yaml \
  -o profile.html \
  -c validation.yaml \
  -j profile.json
```

---

## list-validations

Display all available validation types grouped by category with file/database compatibility icons.

### Syntax

```bash
python3 -m validation_framework.cli list-validations
```

### Output Format

Validations are grouped into 10 categories with compatibility icons:
- 📁 = Works with files (CSV, Excel, Parquet, JSON)
- 🗄️ = Works with databases (PostgreSQL, MySQL, SQLite, etc.)

### Example

```bash
python3 -m validation_framework.cli list-validations
```

**Output:**
```
File-Level (5)
----------------------------------------
  📁   EmptyFileCheck
       Validates that the file is not empty
  📁🗄️ RowCountRangeCheck
       Validates that the number of rows falls within bounds

Schema (2)
----------------------------------------
  📁🗄️ ColumnPresenceCheck
       Checks that required columns exist in the file
  📁🗄️ SchemaMatchCheck
       Validates columns match expected schema

Field-Level (5)
----------------------------------------
  📁🗄️ MandatoryFieldCheck
       Validates that specified fields are not null or empty
  📁🗄️ RegexCheck
       Validates field values match a regular expression
  ...

Total: 36 validations
  📁 File-compatible: 34
  🗄️  Database-compatible: 33
```

### Categories

| Category | Count | Purpose |
|----------|-------|---------|
| File-Level | 5 | File properties (size, format, row count) |
| Schema | 2 | Column structure validation |
| Field-Level | 5 | Individual field value checks |
| Record-Level | 3 | Row-level checks (duplicates, blanks) |
| Conditional | 1 | IF-THEN validation logic |
| Advanced | 9 | Complex validations (cross-field, business rules) |
| Cross-File | 4 | Multi-file relationship checks |
| Database | 3 | Database-specific validations |
| Temporal | 2 | Time-based comparisons |
| Statistical | 3 | Statistical analysis (outliers, distributions) |

---

## cda-analysis

Analyzes your validation configuration to detect gaps in Critical Data Attribute (CDA) coverage. Essential for audit compliance and demonstrating data quality controls.

### Syntax

```bash
python3 -m validation_framework.cli cda-analysis <config_file> [options]
```

### Arguments

#### `<config_file>` (Required)

Path to YAML configuration file with `critical_data_attributes` defined.

### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--output` | `-o` | Path for HTML gap analysis report | `cda_gap_analysis_{timestamp}.html` |
| `--json-output` | `-j` | Path for JSON output | None |
| `--fail-on-gaps` | | Exit with error if any gaps detected | False |

### Examples

```bash
# Basic CDA gap analysis
python3 -m validation_framework.cli cda-analysis config.yaml

# Custom output path
python3 -m validation_framework.cli cda-analysis config.yaml -o gaps.html

# Fail pipeline if any gaps detected (recommended for CI/CD)
python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps

# Generate JSON for CI/CD integration
python3 -m validation_framework.cli cda-analysis config.yaml -j gaps.json
```

### Exit Codes

- `0` - Success (all CDAs covered or no `--fail-on-gaps`)
- `1` - Gaps detected (with `--fail-on-gaps`)
- `2` - Command error (bad config, file not found)

---

## check-policy

Validates a configuration file against a policy level to ensure required validation coverage.

### Syntax

```bash
python3 -m validation_framework.cli check-policy <config_file> [options]
```

### Arguments

#### `<config_file>` (Required)

Path to YAML configuration file to check.

### Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--policy` | `-p` | Policy level: none, minimal, standard, strict | `standard` |
| `--fix` | | Generate fixed config with missing checks added | False |
| `--output` | `-o` | Output path for fixed config | `{config}_fixed.yaml` |
| `--json-output` | `-j` | Path for JSON policy report | None |

### Examples

```bash
# Check against standard policy
python3 -m validation_framework.cli check-policy config.yaml

# Check against strict policy
python3 -m validation_framework.cli check-policy config.yaml -p strict

# Generate fixed config with missing checks
python3 -m validation_framework.cli check-policy config.yaml --fix

# Output JSON report for CI/CD
python3 -m validation_framework.cli check-policy config.yaml -j policy_report.json
```

---

## list-policies

Lists available policy levels and their requirements.

### Syntax

```bash
python3 -m validation_framework.cli list-policies
```

### Output

Displays all available policy levels with their required validations:
- **none** - No requirements
- **minimal** - Basic file and schema checks
- **standard** - Comprehensive data quality checks
- **strict** - Full coverage including cross-file and statistical checks

---

## init-config

Generates a sample YAML configuration file with common validation patterns.

### Syntax

```bash
python3 -m validation_framework.cli init-config <output_path>
```

### Arguments

#### `<output_path>` (Required)

Path where sample config should be written.

### Examples

```bash
# Generate sample config
python3 -m validation_framework.cli init-config my_validation.yaml

# Create in specific directory
python3 -m validation_framework.cli init-config configs/sample_validation.yaml
```

### Generated Config Includes

- File-level checks (EmptyFileCheck, RowCountRangeCheck)
- Schema validation (SchemaMatchCheck)
- Field-level validations (MandatoryFieldCheck, RegexCheck, RangeCheck)
- Record-level checks (DuplicateRowCheck)
- Output configuration
- Processing options

---

## Exit Codes

DataK9 uses standard exit codes for integration with scripts and orchestration tools.

### Exit Code Reference

| Code | Status | Condition | Action |
|------|--------|-----------|--------|
| `0` | SUCCESS | All validations passed | Proceed with data processing |
| `1` | FAILURE | Validation failed (data quality errors) | Stop processing, investigate failures |
| `2` | WARNING_FAILURE | Warnings treated as failure (`--fail-on-warning`) | Review warnings |
| `3` | TIMEOUT | Timeout exceeded (`--timeout`) | Extend timeout or optimize |
| `4` | LOCK_CONFLICT | Lock file conflict (`--lock-file`) | Wait or check for stuck process |
| `5` | ENV_ERROR | Environment error (missing deps, Python version) | Fix environment |
| `130` | INTERRUPTED | Process interrupted (SIGINT/SIGTERM) | Check if manual or system |
| `137` | MEMORY | Memory limit exceeded | Reduce chunk size or sample

### Detailed Behaviors

#### Exit Code 0 (SUCCESS)

**Conditions:**
- All validations passed, OR
- Only WARNING-severity validations failed

**Example:**
```bash
python3 -m validation_framework.cli validate config.yaml
# All checks passed
# Exit code: 0

python3 -m validation_framework.cli validate config.yaml
# 0 ERRORs, 3 WARNINGs
# Exit code: 0 (WARNINGs don't fail validation)
```

**Integration:**
```bash
# Continue pipeline on success
python3 -m validation_framework.cli validate config.yaml && \
    load_to_database.sh
```

#### Exit Code 1 (FAILURE)

**Conditions:**
- One or more ERROR-severity validations failed

**Example:**
```bash
python3 -m validation_framework.cli validate config.yaml
# 3 ERRORs, 2 WARNINGs
# Exit code: 1
```

**Integration:**
```bash
# Stop pipeline on failure
python3 -m validation_framework.cli validate config.yaml || {
    echo "❌ Validation failed, halting pipeline"
    exit 1
}
```

#### Exit Code 2 (ERROR)

**Conditions:**
- Configuration file not found
- Invalid YAML syntax
- Missing required parameters
- File read/write errors
- Python exceptions

**Example:**
```bash
python3 -m validation_framework.cli validate missing.yaml
# Error: Configuration file not found: missing.yaml
# Exit code: 2

python3 -m validation_framework.cli validate invalid.yaml
# Error: Invalid YAML syntax at line 15
# Exit code: 2
```

**Integration:**
```bash
# Handle configuration errors
python3 -m validation_framework.cli validate config.yaml
case $? in
    0)
        echo "✅ Validation passed"
        ;;
    1)
        echo "❌ Validation failed"
        send_alert "Data quality issues detected"
        ;;
    2)
        echo "⚠️ Configuration error"
        send_alert "DataK9 configuration needs attention"
        ;;
esac
```

### Exit Code Examples

#### Bash Script

```bash
#!/bin/bash

# Run validation
python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

# Handle exit code
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Validation passed - proceeding with ETL"
    python3 etl_pipeline.py
elif [ $EXIT_CODE -eq 1 ]; then
    echo "❌ Validation failed - halting pipeline"
    python3 send_failure_alert.py
    exit 1
else
    echo "⚠️ Configuration error - check DataK9 setup"
    python3 send_config_alert.py
    exit 2
fi
```

#### Python Script

```python
import subprocess
import sys

# Run validation
result = subprocess.run(
    ["python3", "-m", "validation_framework.cli", "validate", "config.yaml"],
    capture_output=True,
    text=True
)

# Handle exit code
if result.returncode == 0:
    print("✅ Validation passed")
    # Continue with data processing
    process_data()
elif result.returncode == 1:
    print("❌ Validation failed")
    print(result.stdout)
    send_alert("Data quality issues detected")
    sys.exit(1)
else:
    print("⚠️ Configuration error")
    print(result.stderr)
    send_alert("DataK9 configuration needs attention")
    sys.exit(2)
```

---

## Environment Variables

### `DATAK9_CONFIG_PATH`

Default directory for configuration files.

```bash
export DATAK9_CONFIG_PATH=/etc/datak9/configs/
python3 -m validation_framework.cli validate customers.yaml
# Looks for /etc/datak9/configs/customers.yaml
```

### `DATAK9_OUTPUT_DIR`

Default directory for output reports.

```bash
export DATAK9_OUTPUT_DIR=/var/reports/datak9/
python3 -m validation_framework.cli validate config.yaml
# Writes reports to /var/reports/datak9/
```

### `DATAK9_LOG_LEVEL`

Control logging verbosity.

```bash
export DATAK9_LOG_LEVEL=DEBUG
python3 -m validation_framework.cli validate config.yaml
```

**Levels:**
- `DEBUG` - Detailed debug information
- `INFO` - General informational messages (default)
- `WARNING` - Warning messages only
- `ERROR` - Error messages only

### `DATAK9_CHUNK_SIZE`

Override default chunk size for data loading.

```bash
export DATAK9_CHUNK_SIZE=100000
python3 -m validation_framework.cli validate config.yaml
```

**Default:** 50,000 rows

### Environment Variable Example

```bash
#!/bin/bash
# Production environment setup

export DATAK9_CONFIG_PATH=/opt/datak9/configs/
export DATAK9_OUTPUT_DIR=/var/log/datak9/reports/
export DATAK9_LOG_LEVEL=INFO
export DATAK9_CHUNK_SIZE=50000

# Run validation with environment defaults
python3 -m validation_framework.cli validate daily_validation.yaml
```

---

## Configuration Files

### Location Priority

DataK9 searches for configuration files in this order:

1. **Explicit path** (highest priority)
   ```bash
   python3 -m validation_framework.cli validate /path/to/config.yaml
   ```

2. **Current working directory**
   ```bash
   python3 -m validation_framework.cli validate config.yaml
   # Looks for ./config.yaml
   ```

3. **DATAK9_CONFIG_PATH environment variable**
   ```bash
   export DATAK9_CONFIG_PATH=/etc/datak9/
   python3 -m validation_framework.cli validate config.yaml
   # Looks for /etc/datak9/config.yaml
   ```

4. **Home directory** (~/.datak9/)
   ```bash
   python3 -m validation_framework.cli validate config.yaml
   # Falls back to ~/.datak9/config.yaml
   ```

### Configuration Validation

Always validate configuration before deployment:

```bash
# Test configuration syntax
python3 -m validation_framework.cli validate config.yaml --config-only

# Dry run with verbose output
python3 -m validation_framework.cli validate config.yaml --verbose --fail-fast
```

---

## Output Files

### Generated Files

| File | Description | Format | Default Location |
|------|-------------|--------|------------------|
| `validation_report.html` | Interactive visual report | HTML | Current directory |
| `validation_summary.json` | Machine-readable results | JSON | Current directory |
| `<file>_profile_report.html` | Data profile analysis | HTML | Same as input file |
| `<file>_validation.yaml` | Auto-generated config | YAML | Same as input file |

### File Naming Patterns

#### Validation Reports

**HTML Report:**
```
validation_report.html                    # Default
validation_report_20240115_143022.html    # With timestamp
customers_validation_report.html          # Custom prefix
```

**JSON Report:**
```
validation_summary.json                   # Default
validation_summary_20240115_143022.json   # With timestamp
customers_validation_summary.json         # Custom prefix
```

#### Profile Reports

**HTML Profile:**
```
customers_profile_report.html             # For customers.csv
sales_Q1_profile_report.html              # For sales_Q1.xlsx
```

**Validation Config:**
```
customers_validation.yaml                 # For customers.csv
sales_Q1_validation.yaml                  # For sales_Q1.xlsx
```

### Custom Output Paths

```bash
# Specify output directory
python3 -m validation_framework.cli validate config.yaml \
    --output-dir /var/reports/$(date +%Y%m%d)/

# Redirect output
python3 -m validation_framework.cli validate config.yaml 2>&1 | \
    tee logs/validation_$(date +%Y%m%d_%H%M%S).log
```

---

## Common Patterns

### Pattern 1: Daily Production Validation

```bash
#!/bin/bash
# daily_validation.sh

DATE=$(date +%Y%m%d)
REPORT_DIR="/var/reports/validation/${DATE}"

mkdir -p "${REPORT_DIR}"

python3 -m validation_framework.cli validate \
    /opt/configs/daily_validation.yaml \
    --verbose \
    --output-dir "${REPORT_DIR}" \
    2>&1 | tee "${REPORT_DIR}/execution.log"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Validation passed - triggering ETL pipeline"
    /opt/scripts/run_etl.sh
else
    echo "❌ Validation failed - sending alerts"
    /opt/scripts/send_alert.sh "Daily validation failed"
    exit 1
fi
```

### Pattern 2: Multi-File Validation with Summary

```bash
#!/bin/bash
# validate_all.sh

CONFIGS=(
    "customers.yaml"
    "orders.yaml"
    "products.yaml"
    "inventory.yaml"
)

FAILED=0

for config in "${CONFIGS[@]}"; do
    echo "Validating ${config}..."
    python3 -m validation_framework.cli validate "configs/${config}"

    if [ $? -ne 0 ]; then
        echo "❌ ${config} FAILED"
        FAILED=$((FAILED + 1))
    else
        echo "✅ ${config} PASSED"
    fi
done

echo ""
echo "Summary: ${FAILED} configuration(s) failed"

if [ $FAILED -gt 0 ]; then
    exit 1
fi
```

### Pattern 3: Profile Before Validate

```bash
#!/bin/bash
# profile_and_validate.sh

DATA_FILE="$1"

if [ -z "$DATA_FILE" ]; then
    echo "Usage: $0 <data_file>"
    exit 1
fi

# Profile the data first
echo "📊 Profiling ${DATA_FILE}..."
python3 -m validation_framework.cli profile "${DATA_FILE}"

if [ $? -ne 0 ]; then
    echo "❌ Profiling failed"
    exit 2
fi

# Extract base name
BASE_NAME=$(basename "${DATA_FILE}" | sed 's/\.[^.]*$//')
CONFIG_FILE="${BASE_NAME}_validation.yaml"

# Validate using auto-generated config
echo "✅ Profile complete, running validation..."
python3 -m validation_framework.cli validate "${CONFIG_FILE}"
```

### Pattern 4: CI/CD Integration with Artifacts

```bash
#!/bin/bash
# ci_validation.sh

# Fail on any error
set -e

# Create artifacts directory
mkdir -p artifacts/validation

# Run validation
python3 -m validation_framework.cli validate \
    config.yaml \
    --no-html \
    --output-dir artifacts/validation \
    --fail-fast

# Extract key metrics from JSON
jq -r '.overall_status' artifacts/validation/validation_summary.json

# Upload artifacts (example: AWS S3)
if [ -n "$S3_BUCKET" ]; then
    aws s3 cp artifacts/validation/ "s3://${S3_BUCKET}/validation-reports/" --recursive
fi
```

### Pattern 5: Conditional Processing Based on Quality

```bash
#!/bin/bash
# conditional_processing.sh

# Run validation
python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    # Perfect quality - full processing
    echo "✅ Full quality - running complete ETL"
    python3 etl_full.py

elif [ $EXIT_CODE -eq 1 ]; then
    # Check if only warnings
    ERROR_COUNT=$(jq -r '.total_errors' validation_summary.json)

    if [ "$ERROR_COUNT" -eq 0 ]; then
        # Only warnings - partial processing
        echo "⚠️ Warnings only - running filtered ETL"
        python3 etl_filtered.py --skip-warnings
    else
        # Real errors - abort
        echo "❌ Errors detected - aborting"
        exit 1
    fi
else
    # Configuration error
    echo "⚠️ Configuration error"
    exit 2
fi
```

---

## Troubleshooting

### Common Issues

#### Issue: "Configuration file not found"

**Error:**
```
Error: Configuration file not found: config.yaml
Exit code: 2
```

**Solutions:**

1. **Check file path:**
   ```bash
   # Verify file exists
   ls -l config.yaml

   # Use absolute path
   python3 -m validation_framework.cli validate /full/path/to/config.yaml
   ```

2. **Check current directory:**
   ```bash
   pwd
   # Make sure you're in the right directory
   ```

3. **Check permissions:**
   ```bash
   # Ensure file is readable
   chmod 644 config.yaml
   ```

#### Issue: "Invalid YAML syntax"

**Error:**
```
Error: Invalid YAML syntax at line 15
Exit code: 2
```

**Solutions:**

1. **Validate YAML syntax:**
   ```bash
   # Use yamllint
   yamllint config.yaml

   # Use Python
   python3 -c "import yaml; yaml.safe_load(open('config.yaml'))"
   ```

2. **Check indentation:**
   ```yaml
   # Correct (2-space indent)
   files:
     - path: "data.csv"
       validations:
         - type: "MandatoryFieldCheck"

   # Incorrect (mixed tabs/spaces)
   files:
   	- path: "data.csv"
       validations:
     - type: "MandatoryFieldCheck"
   ```

3. **Use config validation:**
   ```bash
   python3 -m validation_framework.cli validate config.yaml --config-only
   ```

#### Issue: "File not found" during validation

**Error:**
```
Error: Input file not found: data/customers.csv
```

**Solutions:**

1. **Check file paths in config:**
   ```yaml
   files:
     - path: "/absolute/path/to/customers.csv"  # Absolute path
     # OR
     - path: "data/customers.csv"  # Relative to config file location
   ```

2. **Verify file exists:**
   ```bash
   ls -l data/customers.csv
   ```

3. **Check file permissions:**
   ```bash
   chmod 644 data/customers.csv
   ```

#### Issue: "Permission denied" writing reports

**Error:**
```
Error: Permission denied: /var/reports/validation_report.html
```

**Solutions:**

1. **Check directory permissions:**
   ```bash
   ls -ld /var/reports/

   # Fix permissions
   sudo chmod 755 /var/reports/
   ```

2. **Use writable directory:**
   ```bash
   python3 -m validation_framework.cli validate config.yaml \
       --output-dir ~/reports/
   ```

3. **Run with appropriate user:**
   ```bash
   # Create reports as current user
   python3 -m validation_framework.cli validate config.yaml
   ```

#### Issue: Validation takes too long

**Symptom:**
```
Validation running for hours on large file
```

**Solutions:**

1. **Use Parquet format:**
   ```bash
   # Convert CSV to Parquet (10x faster)
   python3 -c "import pandas as pd; pd.read_csv('data.csv').to_parquet('data.parquet')"

   # Update config to use Parquet
   ```

2. **Increase chunk size:**
   ```yaml
   settings:
     chunk_size: 100000  # Increase from default 50000
   ```

3. **Optimize validation order:**
   ```yaml
   # Put fast validations first with fail-fast
   validations:
     - type: "EmptyFileCheck"  # Fast
     - type: "SchemaMatchCheck"  # Fast
     - type: "StatisticalOutlierCheck"  # Slow (put last)
   ```

4. **Use sampling for development:**
   ```bash
   # Profile sample first
   python3 -m validation_framework.cli profile large_file.csv \
       --sample-size 100000
   ```

#### Issue: Out of memory errors

**Error:**
```
MemoryError: Unable to allocate array
```

**Solutions:**

1. **Reduce chunk size:**
   ```yaml
   settings:
     chunk_size: 10000  # Reduce from default 50000
   ```

2. **Limit sample failures:**
   ```yaml
   settings:
     max_sample_failures: 10  # Reduce from default 100
   ```

3. **Use Parquet (columnar format):**
   ```yaml
   files:
     - path: "data.parquet"  # More memory-efficient
       format: "parquet"
   ```

4. **Disable HTML report:**
   ```bash
   python3 -m validation_framework.cli validate config.yaml --no-html
   ```

### Debugging Commands

#### Enable Debug Logging

```bash
# Maximum verbosity
export DATAK9_LOG_LEVEL=DEBUG
python3 -m validation_framework.cli validate config.yaml --verbose
```

#### Dry Run Configuration

```bash
# Validate config without running validations
python3 -m validation_framework.cli validate config.yaml --config-only
```

#### Test with Small Sample

```bash
# Profile sample first
python3 -m validation_framework.cli profile data.csv --sample-size 1000

# Edit auto-generated config to use sample
# Then validate
```

#### Check Validation Registry

```bash
# List all available validations
python3 -m validation_framework.cli list-validations

# Check specific category
python3 -m validation_framework.cli list-validations --category field-level
```

---

## Performance Tips

### 1. Use Appropriate File Formats

```bash
# Parquet is 10x faster than CSV
python3 -c "
import pandas as pd
df = pd.read_csv('data.csv')
df.to_parquet('data.parquet', compression='snappy')
"

# Update config to use Parquet
```

**Performance Comparison:**
- CSV (1 GB): ~120 seconds
- Parquet (1 GB): ~12 seconds

### 2. Optimize Chunk Size

```yaml
settings:
  chunk_size: 50000  # Default - balanced
  # chunk_size: 100000  # Faster, more memory
  # chunk_size: 10000   # Slower, less memory
```

**Guidelines:**
- **Small files (<10 MB):** Use 10,000
- **Medium files (10-100 MB):** Use 50,000 (default)
- **Large files (>100 MB):** Use 100,000
- **Very large files (>10 GB):** Use 200,000

### 3. Use Fail-Fast in Development

```bash
# Get quick feedback
python3 -m validation_framework.cli validate config.yaml --fail-fast
```

### 4. Profile Before Full Validation

```bash
# Quick profile with sample
python3 -m validation_framework.cli profile data.csv --sample-size 10000

# Review profile, adjust config
# Then run full validation
```

### 5. Parallel Validation (Multiple Files)

```bash
# Validate files in parallel
for config in configs/*.yaml; do
    python3 -m validation_framework.cli validate "$config" &
done
wait
```

---

## Next Steps

**You've mastered the DataK9 CLI! Now:**

1. **[YAML Reference](yaml-reference.md)** - Complete configuration syntax
2. **[Validation Reference](validation-reference.md)** - All 36 validation types
3. **[Error Codes Reference](error-codes.md)** - Detailed error messages
4. **[Best Practices](../using-datak9/best-practices.md)** - Production deployment guidance

---

**🐕 DataK9 CLI - Command your data quality guardian**
