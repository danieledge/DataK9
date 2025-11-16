# Validation Reference

**Quick Reference Guide for All 35 Validation Types**

This reference provides a comprehensive, quick-lookup guide for every DataK9 validation rule organized across 10 categories. Use this when you need fast answers about parameters, examples, and behavior.

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Reference Matrix](#quick-reference-matrix)
3. [File-Level Validations](#file-level-validations) (3)
4. [Schema Validations](#schema-validations) (2)
5. [Field-Level Validations](#field-level-validations) (5)
6. [Record-Level Validations](#record-level-validations) (3)
7. [Conditional Validations](#conditional-validations) (1)
8. [Advanced Validations](#advanced-validations) (9)
9. [Cross-File Validations](#cross-file-validations) (4)
10. [Database Validations](#database-validations) (3)
11. [Temporal Validations](#temporal-validations) (2)
12. [Statistical Validations](#statistical-validations) (3)
13. [Parameter Reference](#parameter-reference)
14. [Common Patterns](#common-patterns)

---

## Overview

### Validation Categories

DataK9 provides **35 built-in validation types** organized into **10 categories**:

| Category | Count | Purpose |
|----------|-------|---------|
| File-Level | 3 | Check overall file properties |
| Schema | 2 | Validate column structure |
| Field-Level | 5 | Check individual field values |
| Record-Level | 3 | Check across rows |
| Conditional | 1 | Apply rules conditionally |
| Advanced | 9 | Statistical and complex checks |
| Cross-File | 4 | Validate relationships between files |
| Database | 3 | Direct database validation |
| Temporal | 2 | Compare against historical data |
| Statistical | 3 | Advanced statistical tests |

### Validation Structure

All validations follow this YAML structure:

```yaml
validations:
  - type: "ValidationName"
    severity: "ERROR"  # or "WARNING"
    params:
      parameter1: value1
      parameter2: value2
    condition: "optional_condition"  # Optional inline condition
```

---

## Quick Reference Matrix

| Validation | Category | Primary Use | Key Parameters | Severity Recommendation |
|------------|----------|-------------|----------------|-------------------------|
| **EmptyFileCheck** | File | File not empty | `check_data_rows` | ERROR |
| **RowCountRangeCheck** | File | Row count validation | `min_rows`, `max_rows` | WARNING |
| **FileSizeCheck** | File | File size validation | `min_size_mb`, `max_size_mb` | WARNING |
| **SchemaMatchCheck** | Schema | Exact schema match | `expected_columns`, `allow_extra` | ERROR |
| **ColumnPresenceCheck** | Schema | Required columns | `columns` | ERROR |
| **MandatoryFieldCheck** | Field | Non-null fields | `fields` | ERROR |
| **RegexCheck** | Field | Pattern matching | `field`, `pattern` | ERROR |
| **ValidValuesCheck** | Field | Enumerated values | `field`, `valid_values` | ERROR |
| **RangeCheck** | Field | Numeric ranges | `field`, `min_value`, `max_value` | ERROR/WARNING |
| **DateFormatCheck** | Field | Date format | `field`, `format` | ERROR |
| **DuplicateRowCheck** | Record | Duplicate detection | `key_fields` | ERROR |
| **BlankRecordCheck** | Record | Empty row detection | None | WARNING |
| **UniqueKeyCheck** | Record | Unique values | `fields` | ERROR |
| **ConditionalValidation** | Conditional | If-then-else logic | `condition`, `then_validate` | Varies |
| **StatisticalOutlierCheck** | Advanced | Outlier detection | `field`, `method`, `threshold` | WARNING |
| **CrossFieldComparisonCheck** | Advanced | Field comparisons | `field1`, `field2`, `operator` | ERROR |
| **FreshnessCheck** | Advanced | Data recency | `check_type`, `max_age_hours` | ERROR |
| **CompletenessCheck** | Advanced | Completeness % | `field`, `min_completeness` | WARNING |
| **StringLengthCheck** | Advanced | String length | `field`, `min_length`, `max_length` | ERROR |
| **NumericPrecisionCheck** | Advanced | Decimal precision | `field`, `max_decimal_places` | ERROR |
| **ReferentialIntegrityCheck** | Cross-File | Foreign keys | `foreign_key`, `reference_file` | ERROR |
| **CrossFileComparisonCheck** | Cross-File | Aggregate comparison | `aggregation`, `reference_file` | ERROR |
| **CrossFileDuplicateCheck** | Cross-File | Cross-file duplicates | `columns`, `reference_files` | ERROR |
| **SQLCustomCheck** | Database | Custom SQL | `connection_string`, `sql_query` | Varies |
| **DatabaseReferentialIntegrityCheck** | Database | DB foreign keys | `foreign_key_table`, `reference_table` | ERROR |
| **DatabaseConstraintCheck** | Database | DB constraints | `table`, `constraint_query` | ERROR |
| **BaselineComparisonCheck** | Temporal | Historical comparison | `baseline_file`, `tolerance_pct` | WARNING |
| **TrendDetectionCheck** | Temporal | Trend analysis | `baseline_file`, `max_growth_pct` | WARNING |
| **DistributionCheck** | Statistical | Distribution test | `column`, `expected_distribution` | WARNING |
| **CorrelationCheck** | Statistical | Correlation test | `column1`, `column2`, `min_correlation` | WARNING |
| **AdvancedAnomalyDetectionCheck** | Statistical | Anomaly detection | `column`, `method`, `max_anomaly_pct` | WARNING |

---

## File-Level Validations

### EmptyFileCheck

**Purpose:** Ensures file contains data

**Parameters:**
- `check_data_rows` (boolean, optional): Also check for data rows. Default: `false`

**Example:**
```yaml
- type: "EmptyFileCheck"
  severity: "ERROR"
  params:
    check_data_rows: true
```

**Passes:** File has content (and data rows if check_data_rows=true)
**Fails:** File empty or header-only

---

### RowCountRangeCheck

**Purpose:** Validates row count within range

**Parameters:**
- `min_rows` (integer, optional): Minimum rows
- `max_rows` (integer, optional): Maximum rows

**Example:**
```yaml
- type: "RowCountRangeCheck"
  severity: "WARNING"
  params:
    min_rows: 1000
    max_rows: 1000000
```

**Passes:** Row count within range
**Fails:** Too few or too many rows

---

### FileSizeCheck

**Purpose:** Validates file size within range

**Parameters:**
- `min_size_mb` (float, optional): Minimum size (MB)
- `max_size_mb` (float, optional): Maximum size (MB)

**Example:**
```yaml
- type: "FileSizeCheck"
  severity: "WARNING"
  params:
    min_size_mb: 1.0
    max_size_mb: 100.0
```

**Passes:** File size within range
**Fails:** File too small or too large

---

## Schema Validations

### SchemaMatchCheck

**Purpose:** Validates exact schema match

**Parameters:**
- `expected_columns` (list, required): Expected column names in order
- `allow_extra` (boolean): Allow additional columns. Default: `false`
- `allow_missing` (boolean): Allow missing columns. Default: `false`

**Example:**
```yaml
- type: "SchemaMatchCheck"
  severity: "ERROR"
  params:
    expected_columns:
      - "customer_id"
      - "name"
      - "email"
    allow_extra: false
```

**Passes:** Schema matches exactly (or with allowed flexibility)
**Fails:** Missing, extra, or misordered columns

---

### ColumnPresenceCheck

**Purpose:** Ensures required columns exist (order doesn't matter)

**Parameters:**
- `columns` (list, required): Required column names

**Example:**
```yaml
- type: "ColumnPresenceCheck"
  severity: "ERROR"
  params:
    columns:
      - "customer_id"
      - "email"
```

**Passes:** All specified columns present
**Fails:** Any column missing

---

## Field-Level Validations

### MandatoryFieldCheck

**Purpose:** Validates fields are not null/empty

**Parameters:**
- `fields` (list, required): Field names to check
- `allow_whitespace` (boolean): Allow whitespace-only values. Default: `false`

**Example:**
```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields:
      - "customer_id"
      - "email"
      - "name"
    allow_whitespace: false
```

**Passes:** All fields have non-null, non-empty values
**Fails:** Any field is null, empty, or whitespace-only

---

### RegexCheck

**Purpose:** Validates field matches regex pattern

**Parameters:**
- `field` (string, required): Field name
- `pattern` (string, required): Regular expression
- `message` (string, optional): Custom error message
- `invert` (boolean): Invert match (fail if matches). Default: `false`

**Example:**
```yaml
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    message: "Invalid email format"
```

**Common Patterns:**
- Email: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$`
- Phone (US): `^[0-9]{3}-[0-9]{3}-[0-9]{4}$`
- ZIP: `^[0-9]{5}(-[0-9]{4})?$`
- UUID: `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`

**Passes:** Value matches pattern
**Fails:** Value doesn't match

---

### ValidValuesCheck

**Purpose:** Validates field value in allowed list

**Parameters:**
- `field` (string, required): Field name
- `valid_values` (list, required): Allowed values
- `case_sensitive` (boolean): Case-sensitive comparison. Default: `true`

**Example:**
```yaml
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "status"
    valid_values:
      - "PENDING"
      - "PROCESSING"
      - "SHIPPED"
      - "DELIVERED"
```

**Passes:** Field value in allowed list
**Fails:** Field value not in list

---

### RangeCheck

**Purpose:** Validates numeric value within range

**Parameters:**
- `field` (string, required): Field name
- `min_value` (number, optional): Minimum value (inclusive)
- `max_value` (number, optional): Maximum value (inclusive)

**Example:**
```yaml
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "age"
    min_value: 18
    max_value: 120
```

**Passes:** Value within min and max
**Fails:** Value below min or above max

---

### DateFormatCheck

**Purpose:** Validates date field matches format

**Parameters:**
- `field` (string, required): Field name
- `format` (string, required): Date format (strftime)
- `allow_null` (boolean): Allow null values. Default: `true`

**Example:**
```yaml
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "birth_date"
    format: "%Y-%m-%d"
    allow_null: false
```

**Common Formats:**
- ISO: `%Y-%m-%d` (2024-01-15)
- US: `%m/%d/%Y` (01/15/2024)
- EU: `%d/%m/%Y` (15/01/2024)
- Timestamp: `%Y-%m-%d %H:%M:%S` (2024-01-15 14:30:22)

**Passes:** Date string matches format
**Fails:** Date unparseable or wrong format

---

## Record-Level Validations

### DuplicateRowCheck

**Purpose:** Detects duplicate records

**Parameters:**
- `key_fields` (list, required): Fields that should be unique together

**Example:**
```yaml
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields:
      - "customer_id"
```

**Passes:** All key combinations unique
**Fails:** Duplicate keys found

---

### BlankRecordCheck

**Purpose:** Detects completely empty rows

**Parameters:** None

**Example:**
```yaml
- type: "BlankRecordCheck"
  severity: "WARNING"
```

**Passes:** No blank rows
**Fails:** One or more rows all null/empty

---

### UniqueKeyCheck

**Purpose:** Validates fields have unique values (checked individually)

**Parameters:**
- `fields` (list, required): Fields to check for uniqueness

**Example:**
```yaml
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    fields:
      - "customer_id"
      - "email"
```

**Passes:** Each field has all unique values
**Fails:** Any field has duplicates

**Note:** Checks each field independently. Use `DuplicateRowCheck` for composite keys.

---

## Conditional Validations

### ConditionalValidation

**Purpose:** Execute validations based on conditions (if-then-else)

**Parameters:**
- `condition` (string, required): Boolean expression
- `then_validate` (list, required): Validations if condition is True
- `else_validate` (list, optional): Validations if condition is False

**Example:**
```yaml
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "customer_type == 'INDIVIDUAL'"
    then_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["first_name", "last_name"]
    else_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["company_name", "tax_id"]
```

**Condition Syntax:**
- Comparisons: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Logical: `AND`, `OR`, `NOT`
- Examples:
  - `"status == 'ACTIVE'"`
  - `"age >= 18 AND age <= 65"`
  - `"balance < 0 OR credit_score < 500"`

**Passes:** All executed sub-validations pass
**Fails:** Any executed sub-validation fails

**Inline Alternative:**
```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["company_name"]
  condition: "account_type == 'BUSINESS'"
```

---

## Advanced Validations

### StatisticalOutlierCheck

**Purpose:** Detects statistical outliers

**Parameters:**
- `field` (string, required): Numeric field to analyze
- `method` (string): Detection method - "zscore" or "iqr". Default: `"zscore"`
- `threshold` (number, optional):
  - For zscore: std deviations (default: 3.0)
  - For iqr: IQR multiplier (default: 1.5)

**Example:**
```yaml
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "transaction_amount"
    method: "zscore"
    threshold: 3.0
```

**Methods:**
- `zscore`: Z-score method (3 standard deviations)
- `iqr`: Interquartile range method

**Passes:** No statistical outliers
**Fails:** Values significantly deviate from norms

---

### CrossFieldComparisonCheck

**Purpose:** Validates logical relationships between fields

**Parameters:**
- `field1` (string, required): First field
- `field2` (string, required): Second field
- `operator` (string, required): Comparison operator - `<`, `<=`, `>`, `>=`, `==`, `!=`

**Example:**
```yaml
- type: "CrossFieldComparisonCheck"
  severity: "ERROR"
  params:
    field1: "start_date"
    field2: "end_date"
    operator: "<="
```

**Common Use Cases:**
- Start date before end date
- Discount less than price
- Min value less than max value

**Passes:** All rows satisfy comparison
**Fails:** Any row violates comparison

---

### FreshnessCheck

**Purpose:** Validates data is fresh (recently updated)

**Parameters:**
- `check_type` (string, required): "file_modified" or "field_value"
- `field` (string, optional): Date field (if check_type is "field_value")
- `max_age_hours` (number, required): Maximum age in hours

**Example:**
```yaml
# File modification check
- type: "FreshnessCheck"
  severity: "ERROR"
  params:
    check_type: "file_modified"
    max_age_hours: 24

# Field value check
- type: "FreshnessCheck"
  severity: "WARNING"
  params:
    check_type: "field_value"
    field: "transaction_timestamp"
    max_age_hours: 48
```

**Passes:** Data within max_age_hours
**Fails:** Data exceeds max_age_hours

---

### CompletenessCheck

**Purpose:** Validates field completeness percentage

**Parameters:**
- `field` (string, required): Field to check
- `min_completeness` (number, required): Minimum percentage (0-100)

**Example:**
```yaml
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "phone_number"
    min_completeness: 80.0
```

**Passes:** Percentage non-null >= min_completeness
**Fails:** Completeness below threshold

---

### StringLengthCheck

**Purpose:** Validates string field length

**Parameters:**
- `field` (string, required): Field name
- `min_length` (integer, optional): Minimum length
- `max_length` (integer, optional): Maximum length

**Example:**
```yaml
- type: "StringLengthCheck"
  severity: "ERROR"
  params:
    field: "password"
    min_length: 8
    max_length: 32
```

**Passes:** String length within range
**Fails:** String too short or too long

---

### NumericPrecisionCheck

**Purpose:** Validates numeric decimal precision

**Parameters:**
- `field` (string, required): Numeric field
- `max_decimal_places` (integer, required): Max decimal places

**Example:**
```yaml
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "price"
    max_decimal_places: 2
```

**Passes:** All values have <= max_decimal_places
**Fails:** Any value exceeds precision

---

## Cross-File Validations

### How Cross-File Validation Works

Cross-file validations check relationships between multiple data files efficiently using memory-bounded algorithms:

**Memory-Bounded Tracking with Disk Spillover:**

DataK9 uses a hybrid in-memory/on-disk strategy to handle cross-file validations at any scale:

1. **In-Memory Processing**: Reference data loaded into memory-efficient sets/dictionaries
2. **Automatic Spillover**: When memory threshold reached, DataK9 automatically spills to disk using temporary files
3. **Chunk-Based Streaming**: Both source and reference files processed in chunks (never fully loaded)
4. **Constant Memory**: Memory usage stays constant regardless of file size (typically 200-400 MB)

**This means you can:**
- Validate 200GB files against 50GB reference files on modest hardware
- Process cross-file checks on budget systems (4GB RAM)
- Handle millions of foreign key lookups without OOM errors

**Performance Characteristics:**

| Validation Type | Algorithm | Time Complexity | Memory Usage | Best For |
|----------------|-----------|-----------------|--------------|----------|
| **ReferentialIntegrityCheck** | Set membership (exact match) | O(n + m) | O(m) for ref set | Foreign keys, IDs |
| **CrossFileComparisonCheck** | Aggregation + comparison | O(n + m) | O(1) aggregates | Sum/count reconciliation |
| **CrossFileDuplicateCheck** | Hash-based deduplication | O(n + m) | O(n + m) hashes* | Duplicate detection |

*With disk spillover when needed

**Exact Match vs Subset vs Overlap:**

```
Exact Match (ReferentialIntegrityCheck):
- Every value in File A must exist in File B
- Example: All customer_ids in orders must exist in customers
- Fails if: ANY foreign key missing from reference

Subset Check:
- File A values are subset of File B values
- Example: Active products are subset of all products
- Allows: Reference file to have extra values

Overlap Check:
- File A and File B share some common values
- Example: Customers who placed orders (intersection)
- Measures: Percentage overlap, unique to each file
```

**Memory Efficiency Example:**

```bash
# Cross-file validation: 10GB orders vs 2GB customers
# Traditional approach: Load all customer IDs into memory (~500MB)
# DataK9 approach: Load customer IDs in chunks, use disk spillover if needed (~200MB)

# Benchmark: 357M rows across 2 files on Raspberry Pi 4
# Result: 55% of runtime, 3.5GB peak memory (would need 15GB+ without optimization)
```

**Optimization Tips:**
- **Pre-sort reference files** for faster lookups when possible
- **Use Parquet format** for columnar efficiency (10x faster)
- **Index reference columns** if using database sources
- **Consider sampling** for very large files during development

---

### ReferentialIntegrityCheck

**Purpose:** Validates foreign key relationships

**Parameters:**
- `foreign_key` (string, required): Column in current file
- `reference_file` (string, required): Path to reference file
- `reference_key` (string, required): Column in reference file
- `allow_null` (boolean): Allow NULL values. Default: `false`
- `reference_file_format` (string): File format. Default: `"csv"`

**Example:**
```yaml
- type: "ReferentialIntegrityCheck"
  severity: "ERROR"
  params:
    foreign_key: "customer_id"
    reference_file: "customers.csv"
    reference_key: "id"
    allow_null: false
```

**Passes:** All foreign key values exist in reference
**Fails:** Orphaned foreign keys found

---

### CrossFileComparisonCheck

**Purpose:** Compares aggregate values between files

**Parameters:**
- `aggregation` (string, required): Type - `sum`, `count`, `mean`, `min`, `max`
- `column` (string, optional): Column to aggregate (not needed for count)
- `comparison` (string, required): Operator - `==`, `!=`, `>`, `<`, `>=`, `<=`
- `reference_file` (string, required): Path to reference file
- `reference_aggregation` (string, required): Aggregation in reference
- `reference_column` (string, optional): Column in reference file
- `reference_file_format` (string): Format. Default: `"csv"`
- `tolerance` (float): Absolute tolerance. Default: `0`
- `tolerance_pct` (float): Percentage tolerance. Default: `0`

**Example:**
```yaml
- type: "CrossFileComparisonCheck"
  severity: "ERROR"
  params:
    aggregation: "sum"
    column: "total_amount"
    comparison: "=="
    reference_file: "order_items.csv"
    reference_aggregation: "sum"
    reference_column: "item_amount"
    tolerance_pct: 0.01
```

**Passes:** Aggregates match (within tolerance)
**Fails:** Aggregates differ

---

### CrossFileDuplicateCheck

**Purpose:** Detects duplicates across multiple files

**Parameters:**
- `columns` (list, required): Columns to check
- `reference_files` (list, required): Files to check against
- `reference_file_format` (string): Format. Default: `"csv"`

**Example:**
```yaml
- type: "CrossFileDuplicateCheck"
  severity: "ERROR"
  params:
    columns: ["customer_id"]
    reference_files:
      - "customers_archive.csv"
      - "customers_old.csv"
```

**Passes:** No duplicates across files
**Fails:** Duplicates found

---

## Database Validations

### SQLCustomCheck

**Purpose:** Execute custom SQL validation

**Parameters:**
- `connection_string` (string, required): Database connection string
- `sql_query` (string, required): SQL query returning failures
- `db_type` (string): Database type. Default: `"postgresql"`
- `max_sample_size` (integer): Max sample failures. Default: `10`

**Supported Databases:**
- PostgreSQL
- MySQL
- SQL Server
- Oracle
- SQLite

**Example:**
```yaml
- type: "SQLCustomCheck"
  severity: "WARNING"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    db_type: "postgresql"
    sql_query: |
      SELECT customer_id, email
      FROM customers
      WHERE email NOT LIKE '%@%'
```

**Passes:** Query returns no rows
**Fails:** Query returns rows (failures)

---

### DatabaseReferentialIntegrityCheck

**Purpose:** Validates database foreign keys

**Parameters:**
- `connection_string` (string, required): Database connection
- `foreign_key_table` (string, required): Table with foreign key
- `foreign_key_column` (string, required): Foreign key column
- `reference_table` (string, required): Reference table
- `reference_key_column` (string, required): Primary key column
- `allow_null` (boolean): Allow NULL. Default: `false`
- `db_type` (string): Database type. Default: `"postgresql"`

**Example:**
```yaml
- type: "DatabaseReferentialIntegrityCheck"
  severity: "ERROR"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    foreign_key_table: "orders"
    foreign_key_column: "customer_id"
    reference_table: "customers"
    reference_key_column: "id"
```

**Passes:** All foreign keys valid
**Fails:** Orphaned records found

---

### DatabaseConstraintCheck

**Purpose:** Validates database constraints

**Parameters:**
- `connection_string` (string, required): Database connection
- `table` (string, required): Table to check
- `constraint_query` (string, required): SQL finding violations
- `constraint_name` (string, optional): Constraint name
- `db_type` (string): Database type. Default: `"postgresql"`

**Example:**
```yaml
- type: "DatabaseConstraintCheck"
  severity: "ERROR"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    table: "customers"
    constraint_name: "age_check"
    constraint_query: |
      SELECT customer_id, age
      FROM customers
      WHERE age < 0 OR age > 150
```

**Passes:** No constraint violations
**Fails:** Violations found

---

## Temporal Validations

### BaselineComparisonCheck

**Purpose:** Compares current metrics against historical average

**Parameters:**
- `metric` (string, required): Metric - `count`, `sum`, `mean`, `min`, `max`
- `column` (string, optional): Column to aggregate
- `baseline_file` (string, required): Historical baseline CSV
- `baseline_date_column` (string): Date column. Default: `"date"`
- `baseline_value_column` (string): Value column. Default: `"value"`
- `lookback_days` (integer): Days to average. Default: `30`
- `tolerance_pct` (float, required): Allowed deviation %

**Example:**
```yaml
- type: "BaselineComparisonCheck"
  severity: "WARNING"
  params:
    metric: "count"
    baseline_file: "historical_counts.csv"
    lookback_days: 30
    tolerance_pct: 20
```

**Passes:** Current metric within tolerance of baseline
**Fails:** Metric deviates beyond tolerance

---

### TrendDetectionCheck

**Purpose:** Detects unusual trends vs historical patterns

**Parameters:**
- `metric` (string, required): Metric - `count`, `sum`, `mean`
- `column` (string, optional): Column to aggregate
- `baseline_file` (string, required): Historical data file
- `baseline_date_column` (string): Date column. Default: `"date"`
- `baseline_value_column` (string): Value column. Default: `"value"`
- `max_growth_pct` (float, optional): Max growth %
- `max_decline_pct` (float, optional): Max decline %
- `comparison_period` (integer): Days to compare. Default: `1`

**Example:**
```yaml
- type: "TrendDetectionCheck"
  severity: "WARNING"
  params:
    metric: "count"
    baseline_file: "daily_customer_counts.csv"
    max_growth_pct: 50
    max_decline_pct: 30
    comparison_period: 1
```

**Passes:** Trend within acceptable range
**Fails:** Unusual growth or decline detected

---

## Statistical Validations

### DistributionCheck

**Purpose:** Validates data follows expected distribution

**Parameters:**
- `column` (string, required): Column to check
- `expected_distribution` (string, required): Type - `normal`, `uniform`, `exponential`
- `significance_level` (float): Significance level. Default: `0.05`
- `min_sample_size` (integer): Min samples. Default: `30`

**Example:**
```yaml
- type: "DistributionCheck"
  severity: "WARNING"
  params:
    column: "age"
    expected_distribution: "normal"
    significance_level: 0.05
```

**Passes:** Data follows distribution
**Fails:** Distribution doesn't match

---

### CorrelationCheck

**Purpose:** Validates correlation between columns

**Parameters:**
- `column1` (string, required): First column
- `column2` (string, required): Second column
- `expected_correlation` (float, optional): Expected correlation (-1 to 1)
- `min_correlation` (float, optional): Minimum acceptable
- `max_correlation` (float, optional): Maximum acceptable
- `correlation_type` (string): Type - `pearson`, `spearman`, `kendall`. Default: `"pearson"`
- `tolerance` (float): Tolerance for expected. Default: `0.1`

**Example:**
```yaml
- type: "CorrelationCheck"
  severity: "WARNING"
  params:
    column1: "price"
    column2: "quantity_sold"
    min_correlation: -0.8
    max_correlation: -0.3
```

**Passes:** Correlation within range
**Fails:** Correlation outside range

---

### AdvancedAnomalyDetectionCheck

**Purpose:** Detects anomalies using multiple methods

**Parameters:**
- `column` (string, required): Column to check
- `method` (string): Method - `iqr`, `zscore`, `modified_zscore`, `isolation_forest`. Default: `"iqr"`
- `threshold` (float, optional): Threshold for method
- `max_anomaly_pct` (float): Max acceptable anomaly %. Default: `5`

**Methods:**
- `iqr`: Interquartile Range
- `zscore`: Standard Z-score
- `modified_zscore`: Robust to outliers
- `isolation_forest`: ML-based (requires scikit-learn)

**Example:**
```yaml
- type: "AdvancedAnomalyDetectionCheck"
  severity: "WARNING"
  params:
    column: "price"
    method: "iqr"
    max_anomaly_pct: 2
```

**Passes:** Anomalies below threshold
**Fails:** Too many anomalies detected

---

## Parameter Reference

### Common Parameters

These parameters appear across multiple validations:

#### Severity

**Values:** `ERROR`, `WARNING`

**Impact:**
- `ERROR`: Fails validation (exit code 1)
- `WARNING`: Logged but doesn't fail (exit code 0)

```yaml
severity: "ERROR"  # or "WARNING"
```

#### Condition (Inline)

**Type:** String (boolean expression)

**Purpose:** Apply validation conditionally

```yaml
condition: "account_type == 'BUSINESS'"
```

**Syntax:**
- Comparisons: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Logical: `AND`, `OR`, `NOT`

#### File Paths

**Type:** String

**Format:** Relative or absolute paths

```yaml
reference_file: "data/customers.csv"  # Relative
reference_file: "/path/to/customers.csv"  # Absolute
```

#### Date Formats

**Type:** String (strftime format)

**Common Codes:**
- `%Y` - 4-digit year (2024)
- `%m` - Month (01-12)
- `%d` - Day (01-31)
- `%H` - Hour 24h (00-23)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)

```yaml
format: "%Y-%m-%d"  # 2024-01-15
```

### Database Parameters

#### Connection Strings

**Format varies by database:**

```yaml
# PostgreSQL
connection_string: "postgresql://user:password@host:port/database"

# MySQL
connection_string: "mysql://user:password@host:port/database"

# SQL Server
connection_string: "mssql://user:password@host:port/database"

# SQLite
connection_string: "sqlite:///path/to/database.db"
```

#### Database Types

**Values:** `postgresql`, `mysql`, `mssql`, `oracle`, `sqlite`

```yaml
db_type: "postgresql"
```

---

## Common Patterns

### Pattern 1: Essential Validation Suite

**Every file should have these basics:**

```yaml
validations:
  # File exists and not empty
  - type: "EmptyFileCheck"
    severity: "ERROR"
    params:
      check_data_rows: true

  # Required columns present
  - type: "ColumnPresenceCheck"
    severity: "ERROR"
    params:
      columns: ["id", "name", "email"]

  # Primary key not null
  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["id"]

  # Primary key unique
  - type: "UniqueKeyCheck"
    severity: "ERROR"
    params:
      fields: ["id"]
```

### Pattern 2: Tier-Based Validation

**Different rules for different tiers:**

```yaml
# Basic customers: simple validation
- type: "RangeCheck"
  severity: "WARNING"
  params:
    field: "credit_limit"
    min_value: 0
    max_value: 5000
  condition: "customer_tier == 'BASIC'"

# Premium customers: higher limits
- type: "RangeCheck"
  severity: "WARNING"
  params:
    field: "credit_limit"
    min_value: 5000
    max_value: 100000
  condition: "customer_tier == 'PREMIUM'"
```

### Pattern 3: Multi-Field Format Validation

**Validate related format fields:**

```yaml
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "phone"
    pattern: "^[0-9]{3}-[0-9]{3}-[0-9]{4}$"

- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "birth_date"
    format: "%Y-%m-%d"
```

### Pattern 4: Progressive Validation

**Order validations from fast to slow:**

```yaml
validations:
  # Fast: file-level checks
  - type: "EmptyFileCheck"
    severity: "ERROR"

  - type: "RowCountRangeCheck"
    severity: "WARNING"
    params:
      min_rows: 1000

  # Medium: schema checks
  - type: "ColumnPresenceCheck"
    severity: "ERROR"
    params:
      columns: ["id", "email"]

  # Medium: field checks
  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["id", "email"]

  - type: "RegexCheck"
    severity: "ERROR"
    params:
      field: "email"
      pattern: "..."

  # Slower: statistical checks (last)
  - type: "StatisticalOutlierCheck"
    severity: "WARNING"
    params:
      field: "order_amount"
```

### Pattern 5: Conditional Business Rules

**Complex business logic:**

```yaml
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    # High-value orders need approval
    condition: "order_total > 10000"
    then_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["manager_approval", "approval_timestamp"]
      - type: "RegexCheck"
        params:
          field: "manager_approval"
          pattern: "^MGR[0-9]{6}$"
    else_validate:
      # Low-value orders: no approval needed
      - type: "ValidValuesCheck"
        params:
          field: "auto_approved"
          valid_values: ["YES"]
```

### Pattern 6: Data Quality Monitoring

**Track quality metrics:**

```yaml
# Completeness monitoring
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "phone_number"
    min_completeness: 80.0

# Freshness monitoring
- type: "FreshnessCheck"
  severity: "ERROR"
  params:
    check_type: "file_modified"
    max_age_hours: 24

# Outlier monitoring
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "transaction_amount"
    method: "zscore"
    threshold: 3.0
```

---

## Performance Guidelines

### Fast Validations

**Run on all data with minimal overhead:**

- EmptyFileCheck
- RowCountRangeCheck
- FileSizeCheck
- ColumnPresenceCheck
- SchemaMatchCheck

**Execution Time:** <1 second for any file size

### Medium Speed Validations

**Per-row processing:**

- MandatoryFieldCheck
- RegexCheck
- ValidValuesCheck
- RangeCheck
- DateFormatCheck
- StringLengthCheck
- NumericPrecisionCheck

**Execution Time:** ~1-2 seconds per 10,000 rows

### Slower Validations

**Multiple passes or heavy compute:**

- DuplicateRowCheck (large datasets)
- UniqueKeyCheck (large datasets)
- StatisticalOutlierCheck
- CrossFieldComparisonCheck
- All Cross-File validations
- All Database validations
- All Temporal validations
- All Statistical validations

**Execution Time:** Varies by data size and complexity

### Optimization Tips

1. **Order validations fast → slow**
2. **Use fail-fast for development**
3. **Use Parquet for 10x speed**
4. **Increase chunk_size for large files**
5. **Sample data for profiling first**

---

## Severity Guidelines

### Use ERROR for:

- Data corruption that prevents processing
- Schema violations
- Missing required fields
- Invalid primary/foreign keys
- Business rule violations
- Referential integrity failures

**Example:**
```yaml
severity: "ERROR"  # Stops pipeline
```

### Use WARNING for:

- Data quality issues
- Optional field incompleteness
- Statistical anomalies
- Unexpected but processable data
- Metrics outside normal range

**Example:**
```yaml
severity: "WARNING"  # Logs but continues
```

---

## Next Steps

**You've mastered the validation reference! Now:**

1. **[CLI Reference](cli-reference.md)** - Command-line usage
2. **[YAML Reference](yaml-reference.md)** - Configuration syntax
3. **[Validation Catalog](../using-datak9/validation-catalog.md)** - Detailed examples
4. **[Best Practices](../using-datak9/best-practices.md)** - Production patterns

---

**🐕 DataK9 validation rules - your K9's training manual for guarding data quality**
