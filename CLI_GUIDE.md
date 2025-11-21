# DataK9 CLI Reference

Complete command-line reference for the DataK9 Data Quality Framework.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [CLI Commands](#cli-commands)
  - [validate](#validate---run-data-validation)
  - [profile](#profile---profile-data-files-and-databases)
  - [cda-analysis](#cda-analysis---critical-data-attribute-gap-analysis)
  - [list-validations](#list-validations---list-available-validations)
  - [init-config](#init-config---generate-sample-configuration)
  - [version](#version---display-version-information)
- [Common Workflows](#common-workflows)
- [Exit Codes](#exit-codes)
- [Backend Selection](#backend-selection)
- [Performance Tips](#performance-tips)
- [Troubleshooting](#troubleshooting)

---

## Overview

DataK9 provides a comprehensive command-line interface for data validation and profiling. All commands are invoked through:

```bash
python3 -m validation_framework.cli <command> [options]
```

**Quick Help:**
```bash
# Show all available commands
python3 -m validation_framework.cli --help

# Show help for specific command
python3 -m validation_framework.cli validate --help
python3 -m validation_framework.cli profile --help
```

---

## Installation

```bash
# Install DataK9
cd data-validation-tool
pip install -r requirements.txt

# Optional: Install Polars for high performance (recommended)
pip install polars

# Optional: Install database support
pip install sqlalchemy psycopg2-binary pymysql

# Verify installation
python3 -m validation_framework.cli --help
```

---

## CLI Commands

### validate - Run Data Validation

Executes validation rules from a YAML configuration file.

**Syntax:**
```bash
python3 -m validation_framework.cli validate <config_file> [options]
```

**Arguments:**
- `config_file` (required) - Path to YAML configuration file

**Options:**

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--html-output` | `-o` | Path for HTML report output | `validation_report.html` |
| `--json-output` | `-j` | Path for JSON report output | None |
| `--verbose` | `-v` | Verbose output (shows progress) | True |
| `--quiet` | `-q` | Minimal output | False |
| `--fail-on-warning` | | Fail if warnings are found | False |
| `--log-level` | | Logging level (DEBUG, INFO, WARNING, ERROR) | INFO |
| `--log-file` | | Optional log file path | None |

**Examples:**

```bash
# Basic validation
python3 -m validation_framework.cli validate config.yaml

# With custom output paths
python3 -m validation_framework.cli validate config.yaml \
  -o reports/validation.html \
  -j reports/validation.json

# Fail on warnings
python3 -m validation_framework.cli validate config.yaml --fail-on-warning

# Debug mode with log file
python3 -m validation_framework.cli validate config.yaml \
  --log-level DEBUG \
  --log-file validation.log

# Quiet mode (minimal output)
python3 -m validation_framework.cli validate config.yaml --quiet
```

**Exit Codes:**
- `0` - Validation passed (all ERROR validations passed; warnings OK)
- `1` - Validation failed (ERROR-severity issues found)
- `2` - Command error (bad config, file not found, etc.)

**Sample YAML Configuration:**

```yaml
validation_job:
  name: "Customer Data Validation"

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"

    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

output:
  html_report: "validation_report.html"
  json_summary: "validation_summary.json"
  fail_on_error: true
```

---

### profile - Profile Data Files and Databases

Analyzes data files or database tables to understand structure, quality, and patterns. Automatically generates validation configurations.

**Syntax:**

```bash
# Profile a file
python3 -m validation_framework.cli profile <file_path> [options]

# Profile a database table
python3 -m validation_framework.cli profile \
  --database <connection_string> \
  --table <table_name> [options]

# Profile with custom SQL query
python3 -m validation_framework.cli profile \
  --database <connection_string> \
  --query <sql_query> [options]
```

**Arguments:**
- `file_path` (optional) - Path to data file to profile (not required if using `--database`)

**Options:**

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--format` | `-f` | File format (csv, excel, json, parquet) | Auto-detected |
| `--database` | `--db` | Database connection string | None |
| `--table` | `-t` | Database table name to profile | None |
| `--query` | `-q` | SQL query to profile (alternative to `--table`) | None |
| `--html-output` | `-o` | Path for HTML profile report | `<file>_profile_report.html` |
| `--json-output` | `-j` | Path for JSON profile output | None |
| `--config-output` | `-c` | Path to save generated validation config | `<file>_validation.yaml` |
| `--chunk-size` | | Rows per chunk for large files/tables | 50000 |
| `--log-level` | | Logging level | INFO |

**Examples:**

**File Profiling:**

```bash
# Profile a CSV file
python3 -m validation_framework.cli profile data/customers.csv

# Profile with custom outputs
python3 -m validation_framework.cli profile data.csv \
  -o profile.html \
  -j profile.json \
  -c validation_config.yaml

# Profile Excel file (auto-detects format)
python3 -m validation_framework.cli profile sales_data.xlsx

# Profile Parquet file with explicit format
python3 -m validation_framework.cli profile large_data.parquet \
  --format parquet \
  --chunk-size 100000

# Profile JSON file
python3 -m validation_framework.cli profile transactions.json \
  --format json
```

**Database Profiling:**

```bash
# Profile PostgreSQL table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:password@localhost:5432/mydb" \
  --table customers \
  -o customers_profile.html

# Profile MySQL table
python3 -m validation_framework.cli profile \
  --database "mysql+pymysql://user:password@localhost/mydb" \
  --table orders

# Profile SQLite table
python3 -m validation_framework.cli profile \
  --database "sqlite:///test.db" \
  --table users

# Profile with custom SQL query
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/sales_db" \
  --query "SELECT * FROM transactions WHERE date > '2024-01-01'" \
  -o recent_sales_profile.html

# Profile with filtered data
python3 -m validation_framework.cli profile \
  --db "sqlite:///analytics.db" \
  --query "SELECT * FROM events WHERE user_type = 'premium'" \
  -o premium_users_profile.html
```

**Security Note:** The examples below show credentials in connection strings for illustration purposes only. In production, use environment variables or secure credential storage. See **[Database Credentials Security Guide](docs/DATABASE_CREDENTIALS_SECURITY.md)** for best practices.

**Database Connection Strings:**

| Database | Format | Example |
|----------|--------|---------|
| PostgreSQL | `postgresql://user:pass@host:port/db` | `postgresql://admin:secret@localhost:5432/mydb` |
| MySQL | `mysql+pymysql://user:pass@host:port/db` | `mysql+pymysql://root:pass@localhost/sales` |
| SQL Server | `mssql+pyodbc://user:pass@host/db?driver=...` | `mssql+pyodbc://sa:pass@localhost/db?driver=ODBC+Driver+17` |
| Oracle | `oracle+cx_oracle://user:pass@host:port/sid` | `oracle+cx_oracle://scott:tiger@localhost:1521/xe` |
| SQLite | `sqlite:///path/to/database.db` | `sqlite:///data/test.db` |

**Profiler Output:**

The profiler generates three outputs:

1. **HTML Report** (`profile_report.html`) - Interactive report with:
   - Data quality score
   - Column-by-column analysis
   - Statistical distributions
   - Type inference (known vs inferred)
   - Correlation matrices
   - Validation suggestions

2. **JSON Profile** (optional) - Machine-readable profile data for programmatic use

3. **Validation Config** (`<file>_validation.yaml`) - Auto-generated validation configuration ready to use:

```yaml
validation_job:
  name: "Auto-generated validation for customers.csv"

files:
  - name: "customers"
    path: "customers.csv"
    format: "csv"

    validations:
      # Auto-generated based on profiling
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      - type: "RegexCheck"
        severity: "WARNING"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

---

### cda-analysis - Critical Data Attribute Gap Analysis

Analyzes your validation configuration to detect gaps in Critical Data Attribute coverage. Essential for audit compliance.

**Syntax:**
```bash
python3 -m validation_framework.cli cda-analysis <config_file> [options]
```

**Arguments:**
- `config_file` (required) - Path to YAML configuration file with `critical_data_attributes` section

**Options:**

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--output` | `-o` | Path for HTML gap analysis report | `cda_gap_analysis.html` |
| `--json-output` | `-j` | Path for JSON output (for automation) | None |
| `--fail-on-gaps` | | Exit with error if any gaps detected | False |
| `--fail-on-tier1` | | Exit with error if TIER_1 gaps detected | True |

**Examples:**

```bash
# Basic CDA gap analysis
python3 -m validation_framework.cli cda-analysis config.yaml

# Custom output path
python3 -m validation_framework.cli cda-analysis config.yaml -o gaps.html

# Generate JSON for CI/CD integration
python3 -m validation_framework.cli cda-analysis config.yaml -j gaps.json

# Fail pipeline if any gaps detected
python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps
```

**CDA Tiers:**

| Tier | Name | Priority | Description |
|------|------|----------|-------------|
| TIER_1 | Regulatory | Highest | Fields required for regulatory compliance |
| TIER_2 | Financial | High | Fields used in financial calculations |
| TIER_3 | Operational | Normal | Fields important for business operations |

**Exit Codes:**
- `0` - Success (all CDAs covered or gaps acceptable)
- `1` - TIER_1 gaps detected (with `--fail-on-tier1`) or any gaps (with `--fail-on-gaps`)

**See also:** [CDA Gap Analysis Guide](docs/CDA_GAP_ANALYSIS_GUIDE.md) for complete documentation.

---

### list-validations - List Available Validations

Lists all available validation types with descriptions and source compatibility.

**Syntax:**
```bash
python3 -m validation_framework.cli list-validations [options]
```

**Options:**

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--category` | `-c` | Filter by category | all |
| `--source` | `-s` | Filter by source compatibility (file or database) | None |
| `--show-compatibility` | | Show source compatibility for each validation | False |

**Categories:**
- `all` - All validations (default)
- `file` - File-level checks
- `schema` - Schema validation
- `field` - Field-level checks
- `record` - Record-level checks

**Examples:**

```bash
# List all validations
python3 -m validation_framework.cli list-validations

# List only field-level validations
python3 -m validation_framework.cli list-validations --category field

# List validations compatible with files
python3 -m validation_framework.cli list-validations --source file

# List validations compatible with databases
python3 -m validation_framework.cli list-validations --source database

# Show source compatibility for all validations
python3 -m validation_framework.cli list-validations --show-compatibility

# List file-level validations with compatibility
python3 -m validation_framework.cli list-validations \
  --category file \
  --show-compatibility
```

**Sample Output:**

```
Available Validations: 35

📊 Source Compatibility Summary:
   Total validations: 35
   📁 File-compatible: 32
   🗄️  Database-compatible: 28
   Both: 25

📁 EmptyFileCheck
    Validates that the file is not empty and optionally contains data rows

📁 🗄️ MandatoryFieldCheck
    Validates that specified fields are not null or empty

📁 🗄️ RegexCheck
    Validates field values match a regular expression pattern

...
```

---

### init-config - Generate Sample Configuration

Generates a sample YAML configuration file with common validation patterns.

**Syntax:**
```bash
python3 -m validation_framework.cli init-config <output_path>
```

**Arguments:**
- `output_path` (required) - Path where sample config should be written

**Example:**

```bash
# Generate sample config
python3 -m validation_framework.cli init-config my_validation.yaml

# Create in specific directory
python3 -m validation_framework.cli init-config configs/sample_validation.yaml
```

**Generated Config Includes:**
- File-level checks (EmptyFileCheck, RowCountRangeCheck)
- Schema validation (SchemaMatchCheck)
- Field-level validations (MandatoryFieldCheck, RegexCheck, RangeCheck)
- Record-level checks (DuplicateRowCheck)
- Output configuration
- Processing options

---

### version - Display Version Information

Displays DataK9 version information.

**Syntax:**
```bash
python3 -m validation_framework.cli version
```

**Example:**
```bash
python3 -m validation_framework.cli version
```

**Output:**
```
Data Validation Framework v0.1.0
A robust tool for pre-load data quality validation
```

---

## Common Workflows

### Workflow 1: First-Time Validation Setup

```bash
# Step 1: Profile your data
python3 -m validation_framework.cli profile data/customers.csv \
  -o profile.html \
  -c validation_config.yaml

# Step 2: Review profile report
open profile.html

# Step 3: Edit generated config (customize validations)
nano validation_config.yaml

# Step 4: Run validation
python3 -m validation_framework.cli validate validation_config.yaml \
  -o validation_report.html

# Step 5: Review results
open validation_report.html
```

### Workflow 2: Database Validation

```bash
# Step 1: Profile database table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/db" \
  --table customers \
  -o db_profile.html \
  -c db_validation.yaml

# Step 2: Run validation
python3 -m validation_framework.cli validate db_validation.yaml \
  -o db_validation_report.html

# Step 3: Check exit code for automation
if [ $? -eq 0 ]; then
  echo "Validation passed - safe to proceed with ETL"
else
  echo "Validation failed - check report"
  exit 1
fi
```

### Workflow 3: Production Pipeline Integration

```bash
#!/bin/bash
# production_validation.sh

# Configuration
CONFIG="production_validation.yaml"
REPORT_DIR="reports/$(date +%Y%m%d)"
REPORT_HTML="${REPORT_DIR}/validation_report.html"
REPORT_JSON="${REPORT_DIR}/validation_summary.json"
LOG_FILE="${REPORT_DIR}/validation.log"

# Create report directory
mkdir -p ${REPORT_DIR}

# Run validation
python3 -m validation_framework.cli validate ${CONFIG} \
  -o ${REPORT_HTML} \
  -j ${REPORT_JSON} \
  --log-file ${LOG_FILE} \
  --log-level INFO

# Capture exit code
EXIT_CODE=$?

# Log result
if [ $EXIT_CODE -eq 0 ]; then
  echo "$(date): Validation PASSED" >> validation_history.log
  # Proceed with ETL
  ./run_etl.sh
else
  echo "$(date): Validation FAILED - Exit code ${EXIT_CODE}" >> validation_history.log
  # Send alert
  ./send_alert.sh "${REPORT_HTML}"
  exit $EXIT_CODE
fi
```

### Workflow 4: Continuous Profiling

```bash
#!/bin/bash
# continuous_profile.sh - Profile data daily for monitoring

DATE=$(date +%Y%m%d)
PROFILE_DIR="profiles/${DATE}"
mkdir -p ${PROFILE_DIR}

# Profile all data files
for file in data/*.csv; do
  filename=$(basename "$file" .csv)
  python3 -m validation_framework.cli profile "$file" \
    -o "${PROFILE_DIR}/${filename}_profile.html" \
    -j "${PROFILE_DIR}/${filename}_profile.json"
done

# Compare to previous day for quality trends
./compare_profiles.py "${PROFILE_DIR}" "profiles/$(date -d yesterday +%Y%m%d)"
```

---

## Exit Codes

DataK9 uses standard exit codes for automation and pipeline integration:

| Exit Code | Status | Description | Action |
|-----------|--------|-------------|--------|
| `0` | SUCCESS | All ERROR-severity validations passed (warnings OK) | Proceed with pipeline |
| `1` | VALIDATION_FAILED | ERROR-severity validation failures found | Block pipeline, review failures |
| `2` | COMMAND_ERROR | Bad config, file not found, syntax error | Fix configuration/paths |

**Using Exit Codes in Scripts:**

```bash
# Basic check
python3 -m validation_framework.cli validate config.yaml
if [ $? -ne 0 ]; then
  echo "Validation failed"
  exit 1
fi

# Detailed check
python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "✓ Validation passed - proceeding with load"
  ./load_data.sh
elif [ $EXIT_CODE -eq 1 ]; then
  echo "✗ Validation failed - data quality issues"
  ./send_alert.sh
  exit 1
elif [ $EXIT_CODE -eq 2 ]; then
  echo "✗ Configuration error - check config file"
  exit 2
fi
```

**AutoSys Integration:**

```bash
# validation_job.sh
python3 -m validation_framework.cli validate config.yaml --fail-on-warning

# Exit code automatically fails AutoSys job if validation fails
exit $?
```

---

## Backend Selection

DataK9 supports two backends: **Polars** (high-performance) and **pandas** (compatibility).

### Polars Backend (Default, Recommended)

**When to Use:**
- Files > 1GB or 10M+ rows
- Memory is limited (<16GB)
- Speed is critical
- CSV, JSON, or Parquet formats

**Performance:**
- 5-10x faster than pandas
- Memory-efficient (uses less RAM)
- Handles 200GB+ files

**Install:**
```bash
pip install polars
```

**Usage:**
```bash
# Polars is the default backend
python3 -m validation_framework.cli validate config.yaml

# Explicit Polars backend
python3 -m validation_framework.cli validate config.yaml --backend polars

# Profile with Polars
python3 -m validation_framework.cli profile large_data.parquet --backend polars
```

### pandas Backend

**When to Use:**
- Processing Excel files (.xlsx, .xls)
- Small files (< 100MB)
- Compatibility requirements

**Install:**
```bash
pip install pandas openpyxl
```

**Usage:**
```bash
# Use pandas backend for Excel files
python3 -m validation_framework.cli validate config.yaml --backend pandas

# Profile Excel file
python3 -m validation_framework.cli profile sales_data.xlsx --backend pandas
```

**Backend Comparison:**

| Feature | Polars | pandas |
|---------|--------|--------|
| **Speed** | 5-10x faster | Baseline |
| **Memory** | 50-70% less | Baseline |
| **Max File Size** | 200GB+ | ~10GB |
| **Excel Support** | Limited | Full |
| **CSV Performance** | Excellent | Good |
| **Parquet Performance** | Excellent | Good |

---

## Performance Tips

### 1. Use Polars Backend

```bash
# Install Polars
pip install polars

# Use for large files
python3 -m validation_framework.cli validate config.yaml --backend polars
```

**Benefit:** 5-10x faster, 50% less memory

### 2. Convert CSV to Parquet

```bash
# Convert once
python3 -c "
import polars as pl
pl.read_csv('large_data.csv').write_parquet('large_data.parquet')
"

# Use Parquet in config
# 10x faster reads
```

**Benefit:** 10x faster file reading

### 3. Increase Chunk Size

```yaml
settings:
  chunk_size: 200000  # Default: 50000
```

**Benefit:** Better performance on large files

### 4. Use Parquet for Reference Files

```yaml
# Instead of CSV
- type: "ReferentialIntegrityCheck"
  params:
    reference_file: "customers.parquet"  # Much faster
```

**Benefit:** Faster cross-file validations

### 5. Optimize Validation Order

```yaml
# Put fast validations first (EmptyFileCheck)
# Put slow validations last (StatisticalOutlierCheck)
validations:
  - type: "EmptyFileCheck"  # Fast, fails early
  - type: "MandatoryFieldCheck"  # Fast
  - type: "StatisticalOutlierCheck"  # Slow, run last
```

**Benefit:** Fail fast on common issues

---

## Troubleshooting

### Common Issues

**1. Command Not Found**

```bash
# Error: python3: No module named validation_framework
# Solution: Ensure you're in the correct directory
cd data-validation-tool
python3 -m validation_framework.cli --help
```

**2. File Not Found**

```bash
# Error: File not found: data.csv
# Solution: Use absolute paths or verify current directory
python3 -m validation_framework.cli validate config.yaml --verbose
```

**3. Memory Issues**

```bash
# Error: MemoryError
# Solution: Reduce chunk size
python3 -m validation_framework.cli validate config.yaml --chunk-size 25000
```

**4. Slow Performance**

```bash
# Solution 1: Install Polars
pip install polars

# Solution 2: Convert CSV to Parquet
python3 -c "import polars as pl; pl.read_csv('data.csv').write_parquet('data.parquet')"

# Solution 3: Increase chunk size
python3 -m validation_framework.cli validate config.yaml --chunk-size 200000
```

**5. Database Connection Issues**

```bash
# Error: Could not connect to database
# Solution: Check connection string format
python3 -m validation_framework.cli profile \
  --database "postgresql://user:password@localhost:5432/dbname" \
  --table customers

# Test connection first
python3 -c "
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:pass@localhost/db')
print('Connection successful!')
"
```

### Debug Mode

```bash
# Enable debug logging
python3 -m validation_framework.cli validate config.yaml \
  --log-level DEBUG \
  --log-file debug.log \
  --verbose
```

### Getting Help

```bash
# General help
python3 -m validation_framework.cli --help

# Command-specific help
python3 -m validation_framework.cli validate --help
python3 -m validation_framework.cli profile --help
python3 -m validation_framework.cli list-validations --help
```

---

## See Also

- **[Validation Reference](VALIDATION_REFERENCE.md)** - All 35 validation types
- **[Configuration Guide](docs/using-datak9/configuration-guide.md)** - YAML syntax
- **[Performance Tuning](docs/using-datak9/performance-tuning.md)** - Optimization guide
- **[AutoSys Integration](docs/using-datak9/autosys-integration.md)** - Enterprise scheduling

---

**🐕 DataK9 - Your K9 guardian for data quality**
