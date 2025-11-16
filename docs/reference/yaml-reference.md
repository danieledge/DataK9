# YAML Reference

**Complete YAML Configuration Syntax**

This reference provides the complete YAML configuration syntax for DataK9. Use this as your go-to guide for configuration structure, parameters, and values.

---

## Table of Contents

1. [Overview](#overview)
2. [File Structure](#file-structure)
3. [Root Elements](#root-elements)
4. [validation_job](#validation_job)
5. [settings](#settings)
6. [files](#files)
7. [validations](#validations)
8. [YAML Syntax Rules](#yaml-syntax-rules)
9. [Complete Example](#complete-example)
10. [Advanced Features](#advanced-features)
11. [Best Practices](#best-practices)

---

## Overview

### Configuration Format

DataK9 uses YAML for configuration. YAML is human-readable and easy to learn.

**Advantages:**
- No programming required
- Easy to read and edit
- Version-control friendly
- Supports comments
- Standard format

### Minimum Valid Configuration

```yaml
validation_job:
  name: "My Validation"

files:
  - path: "data.csv"
    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"
```

---

## File Structure

### Top-Level Structure

Every DataK9 configuration file has this structure:

```yaml
# Optional job metadata
validation_job:
  name: "string"
  description: "string"

# Optional global settings
settings:
  chunk_size: integer
  max_sample_failures: integer

# Required: files to validate
files:
  - # File 1 configuration
  - # File 2 configuration
  # ...
```

### Organization

```
config.yaml
├── validation_job (optional)
│   ├── name
│   └── description
├── settings (optional)
│   ├── chunk_size
│   └── max_sample_failures
└── files (required)
    ├── File 1
    │   ├── name
    │   ├── path
    │   ├── format
    │   └── validations
    │       ├── Validation 1
    │       ├── Validation 2
    │       └── ...
    └── File 2
        └── ...
```

---

## Root Elements

### Required Elements

**files** - List of files to validate

```yaml
files:
  - path: "data.csv"
    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"
```

### Optional Elements

**validation_job** - Job metadata

```yaml
validation_job:
  name: "Daily Customer Validation"
  description: "Validates daily customer data extracts"
```

**settings** - Global configuration

```yaml
settings:
  chunk_size: 50000
  max_sample_failures: 100
```

---

## validation_job

Job metadata and description.

### Structure

```yaml
validation_job:
  name: string          # Required: Job name
  description: string   # Optional: Job description
```

### Parameters

#### name

**Type:** String
**Required:** No (but recommended)
**Default:** "Data Validation Job"

**Description:** Human-readable name for this validation job

**Examples:**
```yaml
name: "Daily Customer Validation"
name: "Q1 Sales Data Quality Check"
name: "Production ETL Validation"
```

#### description

**Type:** String
**Required:** No
**Default:** None

**Description:** Detailed description of what this job validates

**Examples:**
```yaml
description: "Validates daily customer extracts before loading to warehouse"
description: "Quarterly sales data quality checks for reporting"
description: |
  Multi-line description
  Can span several lines
  Using YAML multi-line syntax
```

### Complete Example

```yaml
validation_job:
  name: "Customer Data Validation"
  description: "Validates customer data from CRM system before warehouse load"
```

---

## settings

Global configuration for validation execution.

### Structure

```yaml
settings:
  chunk_size: integer           # Optional: Rows per chunk
  max_sample_failures: integer  # Optional: Max failures to report
```

### Parameters

#### chunk_size

**Type:** Integer
**Required:** No
**Default:** 50000
**Range:** 1 to 1000000

**Description:** Number of rows to process at once. Lower = less memory, higher = faster.

**Recommendations:**
- Small files (<10 MB): 10,000
- Medium files (10-100 MB): 50,000 (default)
- Large files (100 MB - 10 GB): 100,000
- Very large files (>10 GB): 200,000

**Examples:**
```yaml
chunk_size: 10000   # Small files, less memory
chunk_size: 50000   # Default, balanced
chunk_size: 100000  # Large files, faster processing
```

#### max_sample_failures

**Type:** Integer
**Required:** No
**Default:** 100
**Range:** 1 to 10000

**Description:** Maximum number of failure samples to include in reports per validation

**Recommendations:**
- Development: 100 (default)
- Production: 10-50 (smaller reports)
- Debugging: 500-1000 (more detail)

**Examples:**
```yaml
max_sample_failures: 10    # Minimal reporting
max_sample_failures: 100   # Default
max_sample_failures: 1000  # Detailed debugging
```

### Complete Example

```yaml
settings:
  chunk_size: 50000
  max_sample_failures: 100
```

---

## files

List of files to validate. Each file has its own validation rules.

### Structure

```yaml
files:
  - name: string              # Optional: Friendly name
    path: string              # Required: File path
    format: string            # Optional: File format (auto-detected)

    # Format-specific options
    delimiter: string         # CSV only
    encoding: string          # CSV/text only
    sheet_name: string|int    # Excel only
    flatten: boolean          # JSON only
    lines: boolean            # JSON only
    columns: list             # Parquet only

    # Validations for this file
    validations:
      - # Validation rules
```

### Common Parameters

#### name

**Type:** String
**Required:** No
**Default:** Filename without extension

**Description:** Friendly name for this file in reports

**Examples:**
```yaml
name: "customers"
name: "Q1 Sales Data"
name: "production_orders"
```

#### path

**Type:** String
**Required:** Yes

**Description:** Path to data file (relative or absolute)

**Path Types:**
- Relative: `"data/customers.csv"` (relative to config file)
- Absolute: `"/var/data/customers.csv"` (full path)

**Examples:**
```yaml
path: "data/customers.csv"              # Relative
path: "/opt/data/customers.csv"         # Absolute
path: "../shared/customers.csv"         # Parent directory
path: "s3://bucket/data/customers.csv"  # S3 (if supported)
```

#### format

**Type:** String
**Required:** No (auto-detected from extension)
**Values:** `csv`, `excel`, `json`, `parquet`

**Description:** File format override (usually not needed)

**Auto-Detection:**
- `.csv` → csv
- `.xlsx`, `.xls` → excel
- `.json`, `.jsonl` → json
- `.parquet` → parquet

**Examples:**
```yaml
format: "csv"
format: "excel"
format: "json"
format: "parquet"
```

### CSV-Specific Parameters

#### delimiter

**Type:** String
**Default:** `","`

**Description:** Field delimiter character

**Common Values:**
```yaml
delimiter: ","    # Comma (default)
delimiter: "|"    # Pipe
delimiter: "\t"   # Tab
delimiter: ";"    # Semicolon
```

#### encoding

**Type:** String
**Default:** `"utf-8"`

**Description:** Character encoding

**Common Values:**
```yaml
encoding: "utf-8"      # Default, modern standard
encoding: "latin-1"    # Legacy Western systems
encoding: "iso-8859-1" # Older European systems
encoding: "cp1252"     # Windows
```

#### header

**Type:** Integer
**Default:** 0

**Description:** Row number of header (0-indexed)

**Examples:**
```yaml
header: 0   # First row is header (default)
header: 1   # Second row is header
header: -1  # No header
```

### Excel-Specific Parameters

#### sheet_name

**Type:** String or Integer
**Default:** First sheet

**Description:** Sheet name or index to validate

**Examples:**
```yaml
sheet_name: "Q1 Sales"   # By name
sheet_name: 0            # First sheet (0-indexed)
sheet_name: 2            # Third sheet
```

### JSON-Specific Parameters

#### flatten

**Type:** Boolean
**Default:** `true`

**Description:** Flatten nested JSON structures

**Examples:**
```yaml
flatten: true   # Flatten nested objects
flatten: false  # Keep nested structure
```

**Flattening Example:**
```json
// Input
{"user": {"address": {"city": "NYC"}}}

// With flatten: true
{"user_address_city": "NYC"}
```

#### lines

**Type:** Boolean
**Default:** `false`

**Description:** JSON Lines format (one JSON object per line)

**Examples:**
```yaml
lines: false  # Standard JSON array
lines: true   # JSON Lines format
```

### Parquet-Specific Parameters

#### columns

**Type:** List of strings
**Default:** All columns

**Description:** Specific columns to load (performance optimization)

**Examples:**
```yaml
columns:
  - "customer_id"
  - "email"
  - "order_date"
```

### File Configuration Examples

#### CSV File

```yaml
- name: "customers"
  path: "data/customers.csv"
  format: "csv"
  delimiter: ","
  encoding: "utf-8"
  validations:
    # ...
```

#### Excel File

```yaml
- name: "sales_report"
  path: "reports/Q1_sales.xlsx"
  format: "excel"
  sheet_name: "Sales Data"
  validations:
    # ...
```

#### JSON File

```yaml
- name: "api_data"
  path: "data/response.json"
  format: "json"
  flatten: true
  validations:
    # ...
```

#### JSON Lines File

```yaml
- name: "log_data"
  path: "logs/events.jsonl"
  format: "json"
  lines: true
  flatten: true
  validations:
    # ...
```

#### Parquet File

```yaml
- name: "analytics"
  path: "warehouse/analytics.parquet"
  format: "parquet"
  columns:  # Only load these columns
    - "customer_id"
    - "order_total"
    - "order_date"
  validations:
    # ...
```

---

## validations

List of validation rules for a file.

### Structure

```yaml
validations:
  - type: string              # Required: Validation type
    severity: string          # Required: ERROR or WARNING
    params:                   # Optional: Validation parameters
      key: value
    condition: string         # Optional: When to apply
    description: string       # Optional: Documentation
    enabled: boolean          # Optional: Enable/disable
```

### Common Parameters

#### type

**Type:** String
**Required:** Yes

**Description:** Validation rule type

**Values:** See [Validation Reference](validation-reference.md) for all 35+ types

**Common Types:**
```yaml
type: "EmptyFileCheck"
type: "MandatoryFieldCheck"
type: "RegexCheck"
type: "RangeCheck"
type: "ValidValuesCheck"
type: "DuplicateRowCheck"
```

#### severity

**Type:** String
**Required:** Yes
**Values:** `ERROR`, `WARNING`

**Description:** Failure severity level

**ERROR:**
- Critical issues
- Fails validation (exit code 1)
- Stops data processing

**WARNING:**
- Quality issues
- Logs but doesn't fail (exit code 0)
- Allows data processing

**Examples:**
```yaml
severity: "ERROR"     # Must fix
severity: "WARNING"   # Should review
```

#### params

**Type:** Object (key-value pairs)
**Required:** Depends on validation type

**Description:** Parameters specific to validation type

**Structure:**
```yaml
params:
  parameter_name: value
  another_parameter: value
```

**Examples:**
```yaml
# MandatoryFieldCheck
params:
  fields: ["customer_id", "email"]

# RangeCheck
params:
  field: "age"
  min_value: 0
  max_value: 120

# RegexCheck
params:
  field: "email"
  pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

#### condition

**Type:** String (boolean expression)
**Required:** No
**Default:** Always apply

**Description:** Conditional execution - only apply validation if condition is true

**Syntax:**
- Comparisons: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Logical: `AND`, `OR`, `NOT`
- Field references: Use column names directly

**Examples:**
```yaml
condition: "account_type == 'BUSINESS'"
condition: "order_total > 10000"
condition: "age >= 18 AND age <= 65"
condition: "country == 'US' OR country == 'CA'"
```

#### description

**Type:** String
**Required:** No
**Default:** None

**Description:** Human-readable description for documentation

**Examples:**
```yaml
description: "Validates email format"
description: "Checks customer ID is unique"
description: "Ensures age is within valid range"
```

#### enabled

**Type:** Boolean
**Required:** No
**Default:** `true`

**Description:** Enable or disable this validation

**Use Cases:**
- Temporarily disable validations
- A/B testing validation rules
- Gradual rollout

**Examples:**
```yaml
enabled: true   # Active (default)
enabled: false  # Disabled (skip)
```

### Validation Examples

#### File-Level Validation

```yaml
- type: "EmptyFileCheck"
  severity: "ERROR"
  description: "Ensures file has data"
  params:
    check_data_rows: true
```

#### Field-Level Validation

```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  description: "Required fields must not be null"
  params:
    fields:
      - "customer_id"
      - "email"
      - "first_name"
```

#### Pattern Validation

```yaml
- type: "RegexCheck"
  severity: "ERROR"
  description: "Email format validation"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    message: "Invalid email format"
```

#### Range Validation

```yaml
- type: "RangeCheck"
  severity: "WARNING"
  description: "Age should be realistic"
  params:
    field: "age"
    min_value: 0
    max_value: 120
```

#### Conditional Validation

```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  description: "Business accounts need company name"
  params:
    fields: ["company_name", "tax_id"]
  condition: "account_type == 'BUSINESS'"
```

#### Disabled Validation

```yaml
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  description: "Outlier detection (disabled during testing)"
  params:
    field: "transaction_amount"
    method: "zscore"
    threshold: 3.0
  enabled: false
```

---

## YAML Syntax Rules

### Indentation

**Rule:** Use 2 spaces per level (not tabs)

**Correct:**
```yaml
validation_job:
  name: "My Job"
  description: "Description"
```

**Incorrect:**
```yaml
validation_job:
name: "My Job"  # Wrong indentation
  description: "Description"
```

### Lists

**Rule:** Lists use `-` prefix

**Syntax:**
```yaml
files:
  - path: "file1.csv"
  - path: "file2.csv"
  - path: "file3.csv"
```

**Inline Syntax:**
```yaml
fields: ["field1", "field2", "field3"]
valid_values: ["A", "B", "C"]
```

### Strings

**Unquoted (simple):**
```yaml
name: simple_name
path: data/file.csv
```

**Quoted (recommended):**
```yaml
name: "Customer Validation"
path: "data/file.csv"
```

**Must quote if contains:**
- Special characters: `@`, `#`, `%`, etc.
- Leading/trailing spaces
- Colons: `:`
- Commas in strings

**Multi-line strings:**
```yaml
# Literal block (preserves newlines)
description: |
  Line 1
  Line 2
  Line 3

# Folded block (joins lines)
description: >
  This is a long description
  that spans multiple lines
  but becomes a single line.
```

### Numbers

**Integers:**
```yaml
chunk_size: 50000
min_value: 0
max_value: 100
```

**Floats:**
```yaml
min_completeness: 80.5
tolerance_pct: 0.01
threshold: 3.14159
```

### Booleans

**Values:** `true`, `false`

**Examples:**
```yaml
check_data_rows: true
allow_null: false
case_sensitive: true
enabled: false
```

### Comments

**Syntax:** `#` starts a comment

**Examples:**
```yaml
# This is a comment
validation_job:
  name: "My Job"  # Inline comment
  # description: "Disabled"
```

### Null Values

**Syntax:** `null` or omit the key

**Examples:**
```yaml
description: null  # Explicit null
# description not included = null
```

---

## Complete Example

### Basic Configuration

```yaml
validation_job:
  name: "Customer Data Validation"
  description: "Daily customer extract validation"

settings:
  chunk_size: 50000
  max_sample_failures: 100

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"

    validations:
      # File not empty
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      # Required columns present
      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        params:
          columns:
            - "customer_id"
            - "email"
            - "name"

      # Required fields not null
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "customer_id"
            - "email"

      # Email format
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      # Customer ID unique
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id"]

      # Age range (if present)
      - type: "RangeCheck"
        severity: "WARNING"
        params:
          field: "age"
          min_value: 0
          max_value: 120
```

### Advanced Configuration

```yaml
validation_job:
  name: "Multi-File Customer Validation"
  description: "Validates customer and order data with cross-file checks"

settings:
  chunk_size: 100000
  max_sample_failures: 50

files:
  # Customer file
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"
    delimiter: ","
    encoding: "utf-8"

    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      - type: "RowCountRangeCheck"
        severity: "WARNING"
        params:
          min_rows: 1000
          max_rows: 1000000

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email", "name"]

      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      # Conditional: Business accounts need company name
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["company_name", "tax_id"]
        condition: "account_type == 'BUSINESS'"

  # Order file
  - name: "orders"
    path: "data/orders.csv"
    format: "csv"

    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["order_id", "customer_id", "order_date"]

      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["order_id"]

      # Foreign key check
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "customer_id"
          reference_file: "data/customers.csv"
          reference_key: "customer_id"

      # Date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "order_date"
          format: "%Y-%m-%d"

      # Conditional: Large orders need approval
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["manager_approval"]
        condition: "order_total > 10000"
```

---

## Advanced Features

### Environment Variables

**Syntax:** `${ENV_VAR_NAME}`

**Example:**
```yaml
files:
  - path: "${DATA_DIR}/customers.csv"
  - path: "${S3_BUCKET}/orders.csv"
```

**Usage:**
```bash
export DATA_DIR=/opt/data
export S3_BUCKET=s3://my-bucket
python3 -m validation_framework.cli validate config.yaml
```

### YAML Anchors and Aliases

**Purpose:** Reuse configuration blocks

**Syntax:**
- Define anchor: `&anchor_name`
- Reference: `*anchor_name`
- Merge: `<<: *anchor_name`

**Example:**
```yaml
# Define common validation set
common_checks: &common
  - type: "EmptyFileCheck"
    severity: "ERROR"
  - type: "ColumnPresenceCheck"
    severity: "ERROR"
    params:
      columns: ["id"]

files:
  - path: "file1.csv"
    validations: *common  # Reuse

  - path: "file2.csv"
    validations:
      <<: *common  # Merge and extend
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["id"]
```

### Multiple Configuration Files

**Approach 1: Include files**

Create separate configs and validate individually:

```bash
python3 -m validation_framework.cli validate config1.yaml
python3 -m validation_framework.cli validate config2.yaml
```

**Approach 2: Merge configs**

Use YAML merge or preprocessing tools to combine configs.

---

## Best Practices

### 1. Organization

**Good:**
```yaml
validation_job:
  name: "Clear descriptive name"
  description: "What this validates and why"

settings:
  chunk_size: 50000  # Comment on non-default values

files:
  # Group related validations
  - name: "customers"
    validations:
      # File-level checks first
      - type: "EmptyFileCheck"
        severity: "ERROR"

      # Schema checks
      - type: "ColumnPresenceCheck"
        severity: "ERROR"

      # Field checks
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
```

### 2. Comments

Add comments to explain:
- Non-obvious validation rules
- Business logic
- Temporary changes
- Performance tuning

```yaml
# Increased chunk size for 50GB Parquet file
chunk_size: 200000

# Temporary: Disabled until source system fixed
- type: "RangeCheck"
  enabled: false
  # ...
```

### 3. Severity Consistency

Be consistent with severity levels:

```yaml
# Critical data quality
- type: "MandatoryFieldCheck"
  severity: "ERROR"      # Missing required fields = ERROR

- type: "UniqueKeyCheck"
  severity: "ERROR"      # Duplicate keys = ERROR

# Data quality monitoring
- type: "CompletenessCheck"
  severity: "WARNING"    # Optional fields = WARNING

- type: "StatisticalOutlierCheck"
  severity: "WARNING"    # Outliers = WARNING
```

### 4. Validation Order

Order validations from fast to slow:

```yaml
validations:
  # Fast: file-level
  - type: "EmptyFileCheck"      # ~instant
  - type: "RowCountRangeCheck"  # ~instant

  # Fast: schema
  - type: "ColumnPresenceCheck" # ~instant

  # Medium: field validations
  - type: "MandatoryFieldCheck" # per-row
  - type: "RegexCheck"          # per-row

  # Slow: statistical
  - type: "StatisticalOutlierCheck"  # multiple passes
```

### 5. Version Control

Store configs in version control:

```yaml
validation_job:
  name: "Customer Validation v2.1"
  description: |
    Customer data validation
    Version: 2.1
    Author: Data Team
    Last Updated: 2024-01-15
    Changes: Added email format validation
```

### 6. Testing

Test configurations before production:

```bash
# Validate config syntax
python3 -m validation_framework.cli validate config.yaml --config-only

# Test with fail-fast
python3 -m validation_framework.cli validate config.yaml --fail-fast

# Test with sample data
python3 -m validation_framework.cli profile sample.csv
```

---

## Next Steps

**You've mastered YAML configuration! Now:**

1. **[Validation Reference](validation-reference.md)** - All 35+ validation types
2. **[CLI Reference](cli-reference.md)** - Command-line usage
3. **[Configuration Guide](../using-datak9/configuration-guide.md)** - Detailed examples
4. **[Best Practices](../using-datak9/best-practices.md)** - Production patterns

---

**🐕 DataK9 YAML - Configure your K9's behavior for data quality**
