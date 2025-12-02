# Data Validation Guide

**Guard Your Data with DataK9 Validator**

The DataK9 Validator executes validation rules against your data files and databases, catching quality issues before they cause downstream problems. Like a vigilant K9 unit, the validator systematically checks every aspect of your data against defined rules.

---

## Table of Contents

- [Quick Start](#quick-start)
- [How Validation Works](#how-validation-works)
- [Configuration Structure](#configuration-structure)
- [Validation Categories](#validation-categories)
- [Severity Levels](#severity-levels)
- [Conditional Validations](#conditional-validations)
- [Output Reports](#output-reports)
- [Exit Codes](#exit-codes)
- [Command Reference](#command-reference)
- [Best Practices](#best-practices)

---

## Quick Start

**Run your first validation in 30 seconds:**

```bash
# Validate a file using a YAML config
python3 -m validation_framework.cli validate config.yaml

# With HTML and JSON output
python3 -m validation_framework.cli validate config.yaml -o report.html -j summary.json

# Validate with timestamped outputs (prevents overwrites)
python3 -m validation_framework.cli validate config.yaml -o "reports/{timestamp}_report.html"
```

**Minimal configuration example:**

```yaml
validation_job:
  name: "My First Validation"

files:
  - name: "customers"
    path: "data/customers.csv"
    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]
```

---

## How Validation Works

### Execution Flow

```
1. Load Configuration
   └── Parse YAML config file
   └── Validate configuration structure
   └── Resolve file paths

2. For Each File:
   └── Create data loader (CSV, Excel, JSON, Parquet)
   └── Get file metadata (row count, columns)

   3. For Each Validation:
      └── Check if conditions are met
      └── Process data in chunks (memory efficient)
      └── Collect failures (up to max_sample_failures)
      └── Create validation result

4. Generate Reports
   └── HTML report (interactive dashboard)
   └── JSON summary (for CI/CD)

5. Return Exit Code
   └── 0 = All validations passed
   └── 1 = Validation failures (ERROR severity)
   └── 2 = Configuration or runtime error
```

### Chunked Processing

DataK9 processes data in configurable chunks to handle files of any size:

```yaml
settings:
  chunk_size: 50000      # Rows per chunk (default: 50,000)
  max_sample_failures: 100  # Max failure samples to collect
```

**Memory efficiency:**
- Only `chunk_size` rows in memory at a time
- 50,000 rows x 50 columns x 8 bytes ≈ 20 MB per chunk
- Process 200GB+ files with ~400MB RAM

---

## Configuration Structure

### Complete Configuration Template

```yaml
# Job metadata
validation_job:
  name: "Customer Data Validation"
  description: "Validates daily customer extract"

# Global settings
settings:
  chunk_size: 50000
  max_sample_failures: 100
  fail_on_error: true
  fail_on_warning: false
  log_level: INFO

# Output configuration
output:
  html_report: "reports/{job_name}_{timestamp}.html"
  json_summary: "results/{job_name}_{timestamp}.json"
  fail_on_error: true
  fail_on_warning: false

# File validations
files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"  # Optional: auto-detected from extension

    # Critical Data Attributes (for compliance tracking)
    critical_data_attributes:
      - field: "customer_id"
        description: "Unique customer identifier"
        owner: "Data Team"
        regulatory_reference: "SOX Section 404"

    # Validation rules
    validations:
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          message: "Invalid email format"

# Database validations (optional)
database:
  connection_string: "postgresql://user:pass@localhost/mydb"
  tables:
    - name: "orders"
      validations:
        - type: "RecordCountCheck"
          params:
            min_records: 1
```

### Date/Time Patterns

Use patterns in output paths to prevent file overwrites:

| Pattern | Example | Description |
|---------|---------|-------------|
| `{date}` | `2025-11-22` | ISO date |
| `{time}` | `14-30-45` | Time (HH-MM-SS) |
| `{timestamp}` | `20251122_143045` | Unique per run |
| `{datetime}` | `2025-11-22_14-30-45` | Combined |
| `{job_name}` | `Customer_Validation` | Sanitized job name |

---

## Validation Categories

DataK9 includes **35 built-in validation types** organized into 10 categories:

### File-Level Validations
Check properties of the entire file before processing rows.

| Validation | Purpose |
|------------|---------|
| `EmptyFileCheck` | Ensure file is not empty |
| `RowCountRangeCheck` | Verify row count within bounds |
| `FileSizeCheck` | Check file size limits |
| `CSVFormatCheck` | Validate CSV formatting (delimiters, quoting) |

### Schema Validations
Validate the structure of your data.

| Validation | Purpose |
|------------|---------|
| `SchemaMatchCheck` | Verify columns match expected schema |
| `ColumnPresenceCheck` | Ensure required columns exist |

### Field-Level Validations
Check individual field values.

| Validation | Purpose |
|------------|---------|
| `MandatoryFieldCheck` | Required fields are not null |
| `RegexCheck` | Values match pattern |
| `InlineRegexCheck` | Inline regex with custom error messages |
| `ValidValuesCheck` | Values in allowed list |
| `RangeCheck` | Numeric values within bounds |
| `DateFormatCheck` | Dates match expected format |
| `StringLengthCheck` | String length constraints |
| `NumericPrecisionCheck` | Decimal precision validation |

### Record-Level Validations
Check across rows.

| Validation | Purpose |
|------------|---------|
| `UniqueKeyCheck` | Primary key uniqueness |
| `DuplicateRowCheck` | Find duplicate records |
| `CrossFieldComparisonCheck` | Compare fields (e.g., start < end) |
| `BlankRecordCheck` | Detect completely blank rows |

### Advanced Validations
Complex business rules and statistics.

| Validation | Purpose |
|------------|---------|
| `StatisticalOutlierCheck` | Detect statistical anomalies |
| `AdvancedAnomalyDetectionCheck` | ML-based anomaly detection |
| `InlineBusinessRuleCheck` | Custom business logic |
| `InlineLookupCheck` | Reference data validation |
| `CompletenessCheck` | Field completeness percentage |
| `ConditionalValidation` | Apply validations based on conditions |

### Cross-File Validations
Validate relationships between files.

| Validation | Purpose |
|------------|---------|
| `ReferentialIntegrityCheck` | Foreign key validation |
| `CrossFileComparisonCheck` | Compare data across files |
| `CrossFileDuplicateCheck` | Find duplicates across files |

### Database Validations
Direct database validation support.

| Validation | Purpose |
|------------|---------|
| `SQLCustomCheck` | Custom SQL validation |
| `DatabaseConstraintCheck` | Validate database constraints |
| `DatabaseReferentialIntegrityCheck` | Check foreign key relationships |

### Temporal Validations
Time-based validations.

| Validation | Purpose |
|------------|---------|
| `FreshnessCheck` | Data recency validation |
| `TrendDetectionCheck` | Detect anomalous trends |
| `BaselineComparisonCheck` | Compare against historical baseline |

### Statistical Validations
Advanced statistical tests.

| Validation | Purpose |
|------------|---------|
| `DistributionCheck` | Validate data distribution |
| `CorrelationCheck` | Check field correlations |

**[Complete Validation Reference →](validation-catalog.md)**

---

## Severity Levels

### ERROR vs WARNING

| Severity | Use When | Effect |
|----------|----------|--------|
| **ERROR** | Data is fundamentally broken | Causes validation failure (exit code 1) |
| **WARNING** | Quality issue, but data usable | Logged but doesn't fail job |

### Choosing Severity

**Use ERROR for:**
- Missing primary keys
- Invalid foreign keys
- Malformed required formats
- Schema violations
- Missing mandatory fields

**Use WARNING for:**
- Statistical outliers
- Low completeness percentages
- Unusual value patterns
- Date freshness issues

```yaml
# ERROR: Cannot process without valid customer_id
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["customer_id"]

# WARNING: Should review but can continue
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "phone"
    min_completeness_pct: 80
```

---

## Conditional Validations

Apply validations only when conditions are met using pandas query syntax:

```yaml
# Only validate discount for retail customers
- type: "RangeCheck"
  severity: "ERROR"
  condition: "customer_type == 'retail'"
  params:
    field: "discount_pct"
    min_value: 0
    max_value: 50

# Only check adults
- type: "RangeCheck"
  severity: "ERROR"
  condition: "age >= 18"
  params:
    field: "credit_limit"
    min_value: 0

# Complex conditions
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  condition: "(status == 'active') & (balance > 0)"
  params:
    fields: ["last_contact_date"]
```

---

## Output Reports

### HTML Report

Interactive dashboard with:
- Executive summary (pass/fail counts)
- Per-file validation results
- Sample failure records
- Visual charts and indicators

```bash
python3 -m validation_framework.cli validate config.yaml -o report.html
```

### JSON Summary

Machine-readable output for CI/CD integration:

```json
{
  "job_name": "Customer Validation",
  "execution_time": "2025-11-22T14:30:45",
  "overall_status": "FAILED",
  "summary": {
    "total_validations": 15,
    "passed": 12,
    "failed": 3,
    "errors": 2,
    "warnings": 1
  },
  "files": [
    {
      "name": "customers",
      "status": "FAILED",
      "validations": [...]
    }
  ]
}
```

```bash
python3 -m validation_framework.cli validate config.yaml -j summary.json
```

---

## Exit Codes

| Code | Meaning | When |
|------|---------|------|
| **0** | Success | All validations passed (or warnings-only without `--fail-on-warning`) |
| **1** | Failure | ERROR-severity validations failed, file not found, or runtime error |
| **2** | Warning Failure | WARNING-severity validations failed (only with `--fail-on-warning` flag) |

### CI/CD Integration

```bash
# Fail build on validation errors
python3 -m validation_framework.cli validate config.yaml
if [ $? -eq 1 ]; then
  echo "Validation failed!"
  exit 1
fi

# Also fail on warnings (strict mode)
python3 -m validation_framework.cli validate config.yaml --fail-on-warning
```

---

## Command Reference

### Basic Commands

```bash
# Run validation
python3 -m validation_framework.cli validate config.yaml

# With outputs
python3 -m validation_framework.cli validate config.yaml \
  -o report.html \
  -j summary.json

# Verbose output
python3 -m validation_framework.cli validate config.yaml -v

# Custom chunk size
python3 -m validation_framework.cli validate config.yaml --chunk-size 100000

# Use Polars backend (faster)
python3 -m validation_framework.cli validate config.yaml --backend polars
```

### All Options

| Option | Description |
|--------|-------------|
| `-o, --output` | HTML report path |
| `-j, --json` | JSON summary path |
| `-v, --verbose` | Verbose output |
| `--chunk-size` | Rows per chunk |
| `--backend` | Processing backend (polars/pandas) |
| `--fail-on-warning` | Exit code 1 for warnings |
| `--log-level` | Logging level (DEBUG/INFO/WARNING/ERROR) |

---

## Best Practices

### 1. Layer Validations Logically

Apply in order - fail fast:

```
1. File-Level     → File exists, not empty
2. Schema         → Columns present
3. Field-Level    → Values valid, not null
4. Record-Level   → Row integrity, uniqueness
5. Advanced       → Statistical checks
6. Cross-File     → Relationships valid
```

### 2. Set Realistic Bounds

Use business limits, not technical limits:

```yaml
# BAD: Technical limit
max_value: 2147483647

# GOOD: Business limit
max_value: 120  # for age field
```

### 3. Use Meaningful Error Messages

```yaml
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    message: "Email must be valid format (e.g., user@example.com)"
```

### 4. Profile Before Validating

Use the profiler to understand your data first:

```bash
# 1. Profile to understand data
python3 -m validation_framework.cli profile data.csv

# 2. Review generated config
# 3. Customize and run
python3 -m validation_framework.cli validate data_validation.yaml
```

### 5. Test Incrementally

```
1. Start with 2-3 critical validations
2. Run against sample data
3. Add more validations
4. Test again
5. Gradually build complete suite
```

---

## Next Steps

- **[Validation Catalog](validation-catalog.md)** - Complete reference of all 35 validations
- **[Configuration Guide](configuration-guide.md)** - Detailed YAML syntax
- **[Best Practices](best-practices.md)** - Production deployment tips
- **[CDA Gap Analysis](../guides/advanced/CDA_GAP_ANALYSIS_GUIDE.md)** - Compliance tracking

---

**DataK9 Validator - Your K9 guardian for data quality**
