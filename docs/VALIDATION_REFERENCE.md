# DataK9 Validation Reference

Complete reference for all 35 validation types in DataK9 Data Quality Framework.

---

## 📑 Table of Contents

- [Overview](#overview)
- [Category Matrix](#category-matrix) - Quick visual overview
- [Quick Reference Table](#quick-reference-table) - All 35 validations at a glance
- [Detailed Validation Catalog](#detailed-validation-catalog) - Complete docs with examples
  - [File-Level Validations (4)](#file-level-validations)
  - [Schema Validations (2)](#schema-validations)
  - [Field-Level Validations (6)](#field-level-validations)
  - [Record-Level Validations (3)](#record-level-validations)
  - [Advanced Validations (9)](#advanced-validations)
  - [Cross-File Validations (4)](#cross-file-validations)
  - [Conditional Validations (1)](#conditional-validations)
  - [Database Validations (3)](#database-validations)
  - [Temporal Validations (2)](#temporal-validations)
  - [Statistical Validations (3)](#statistical-validations)

---

## Overview

DataK9 provides **36 validation types** organized into **10 categories**. Each validation can be configured with:

- **Severity levels**: `ERROR` (fails validation) or `WARNING` (flags for review)
- **Parameters**: Customize validation behavior
- **Source compatibility**: Works with files, databases, or both

---

## Category Matrix

Quick overview of all validation categories:

| Category | Count | File | DB | Both | Common Use Cases |
|----------|-------|------|----|----|------------------|
| **File-Level** | 3 | 3 | 0 | 0 | File existence, size, row count validation |
| **Schema** | 2 | 0 | 0 | 2 | Column structure, schema contracts |
| **Field-Level** | 6 | 0 | 0 | 6 | Required fields, patterns, ranges, formats |
| **Record-Level** | 3 | 0 | 0 | 3 | Duplicates, blank rows, unique keys |
| **Advanced** | 9 | 0 | 0 | 9 | Outliers, freshness, completeness, precision |
| **Cross-File** | 4 | 4 | 0 | 0 | Referential integrity between files |
| **Conditional** | 1 | 0 | 0 | 1 | If-then-else validation logic |
| **Database** | 3 | 0 | 3 | 0 | Database constraints, SQL queries |
| **Temporal** | 2 | 2 | 0 | 0 | Baseline comparison, trend detection |
| **Statistical** | 3 | 0 | 0 | 3 | Distributions, correlations, ML anomalies |
| **TOTAL** | **35** | **9** | **3** | **23** | |

**Legend:** File = File sources only | DB = Database sources only | Both = Works with both

**Quick CLI Reference:**
```bash
# List validations by category
python3 -m validation_framework.cli list-validations --category field

# List by source compatibility
python3 -m validation_framework.cli list-validations --source database

# Show all with compatibility info
python3 -m validation_framework.cli list-validations --show-compatibility
```

---

## Quick Reference Table

All 35 validations at a glance - click validation name to jump to detailed documentation:

| Validation | Category | File | DB | Description | Key Parameters |
|------------|----------|------|-------|-------------|----------------|
| [EmptyFileCheck](#1-emptyfilecheck) | File | ✅ | ❌ | Prevent empty files | check_data_rows |
| [RowCountRangeCheck](#2-rowcountrangecheck) | File | ✅ | ❌ | Validate row volumes | min_rows, max_rows |
| [FileSizeCheck](#3-filesizecheck) | File | ✅ | ❌ | Check file size limits | min_size_mb, max_size_gb |
| [CSVFormatCheck](#4-csvformatcheck) | File | ✅ | ❌ | Detect malformed CSV files | delimiter, sample_rows |
| [SchemaMatchCheck](#5-schemamatchcheck) | Schema | ✅ | ✅ | Enforce schema contracts | expected_columns, allow_extra |
| [ColumnPresenceCheck](#6-columnpresencecheck) | Schema | ✅ | ✅ | Required columns exist | required_columns |
| [MandatoryFieldCheck](#7-mandatoryfieldcheck) | Field | ✅ | ✅ | Required fields not null | fields |
| [RegexCheck](#8-regexcheck) | Field | ✅ | ✅ | Pattern matching validation | field, pattern |
| [ValidValuesCheck](#9-validvaluescheck) | Field | ✅ | ✅ | Whitelist/blacklist values | field, valid_values |
| [RangeCheck](#10-rangecheck) | Field | ✅ | ✅ | Numeric/date ranges | field, min_value, max_value |
| [DateFormatCheck](#11-dateformatcheck) | Field | ✅ | ✅ | Date format validation | field, format |
| [InlineRegexCheck](#12-inlineregexcheck) | Field | ✅ | ✅ | Quick inline regex | field, pattern |
| [DuplicateRowCheck](#13-duplicaterowcheck) | Record | ✅ | ✅ | Find duplicate records | key_fields |
| [BlankRecordCheck](#14-blankrecordcheck) | Record | ✅ | ✅ | Detect empty rows | None |
| [UniqueKeyCheck](#15-uniquekeycheck) | Record | ✅ | ✅ | Primary key uniqueness | key_fields |
| [CompletenessCheck](#16-completenesscheck) | Advanced | ✅ | ✅ | Field completeness % | field, min_completeness |
| [StatisticalOutlierCheck](#17-statisticaloutliercheck) | Advanced | ✅ | ✅ | Detect anomalies | field, method, threshold |
| [CrossFieldComparisonCheck](#18-crossfieldcomparisoncheck) | Advanced | ✅ | ✅ | Field relationships | field_a, operator, field_b |
| [FreshnessCheck](#19-freshnesscheck) | Advanced | ✅ | ✅ | Data recency validation | timestamp_field, max_age_hours |
| [StringLengthCheck](#20-stringlengthcheck) | Advanced | ✅ | ✅ | String length constraints | field, min_length, max_length |
| [NumericPrecisionCheck](#21-numericprecisioncheck) | Advanced | ✅ | ✅ | Decimal precision | field, max_decimals |
| [InlineBusinessRuleCheck](#22-inlinebusinessrulecheck) | Advanced | ✅ | ✅ | Custom business rules | expression, message |
| [InlineLookupCheck](#23-inlinelookupcheck) | Advanced | ✅ | ✅ | Inline reference data | field, lookup_values |
| [ReferentialIntegrityCheck](#24-referentialintegritycheck) | Cross-File | ✅ | ❌ | Foreign key validation | local_field, reference_file |
| [CrossFileComparisonCheck](#25-crossfilecomparisoncheck) | Cross-File | ✅ | ❌ | Compare metrics | metric, field, reference_file |
| [CrossFileDuplicateCheck](#26-crossfileduplicatecheck) | Cross-File | ✅ | ❌ | Cross-file duplicates | key_fields, reference_files |
| [CrossFileKeyCheck](#27-crossfilekeycheck) | Cross-File | ✅ | ❌ | Cross-file key analysis | foreign_key, reference_file |
| [ConditionalValidation](#28-conditionalvalidation) | Conditional | ✅ | ✅ | If-then-else logic | condition, validations |
| [DatabaseConstraintCheck](#29-databaseconstraintcheck) | Database | ❌ | ✅ | DB constraint validation | connection_string, constraints |
| [DatabaseReferentialIntegrityCheck](#30-databasereferentialintegritycheck) | Database | ❌ | ✅ | DB foreign keys | connection_string, reference_table |
| [SQLCustomCheck](#31-sqlcustomcheck) | Database | ❌ | ✅ | Custom SQL queries | connection_string, query |
| [BaselineComparisonCheck](#32-baselinecomparisoncheck) | Temporal | ✅ | ❌ | Historical comparison | metric, baseline_file |
| [TrendDetectionCheck](#33-trenddetectioncheck) | Temporal | ✅ | ❌ | Detect unusual trends | timestamp_field, value_field |
| [DistributionCheck](#34-distributioncheck) | Statistical | ✅ | ✅ | Statistical distributions | field, distribution_type |
| [CorrelationCheck](#35-correlationcheck) | Statistical | ✅ | ✅ | Field correlations | field_a, field_b, method |
| [AdvancedAnomalyDetectionCheck](#36-advancedanomalydetectioncheck) | Statistical | ✅ | ✅ | ML-based anomaly detection | fields, method, contamination |

---

## Detailed Validation Catalog

Complete documentation with parameters, YAML examples, use cases, and tips for each validation.

### File-Level Validations

Validate file-level properties like existence, size, and row counts.

#### 1. EmptyFileCheck

**Description:** Validates that the file is not empty and optionally contains data rows.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `check_data_rows` (boolean, optional, default: `true`) - If true, checks for actual data rows (not just headers)

**YAML Example:**

```yaml
- type: "EmptyFileCheck"
  severity: "ERROR"
  params:
    check_data_rows: true
```

**Use Cases:**
- Prevent processing empty files before ETL
- Ensure data extracts contain data
- Fail early in pipelines if source is empty

**Tips:**
- Use ERROR severity to block pipeline execution
- Combine with RowCountRangeCheck for more specific volume validation

---

#### 2. RowCountRangeCheck

**Description:** Validates that the number of rows falls within specified bounds.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `min_rows` (number, optional) - Minimum acceptable number of rows
- `max_rows` (number, optional) - Maximum acceptable number of rows

**YAML Example:**

```yaml
- type: "RowCountRangeCheck"
  severity: "WARNING"
  params:
    min_rows: 1000
    max_rows: 1000000
```

**Use Cases:**
- Validate daily extract has expected volume
- Detect missing or duplicate data loads
- Alert on unusual data volumes

**Tips:**
- Set bounds based on historical averages
- Use WARNING severity to flag unusual volumes without blocking
- Combine with BaselineComparisonCheck for trend-based volume monitoring

---

#### 3. FileSizeCheck

**Description:** Validates file size is within acceptable limits.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `min_size_mb` (number, optional) - Minimum file size in megabytes
- `max_size_mb` (number, optional) - Maximum file size in megabytes
- `max_size_gb` (number, optional) - Maximum file size in gigabytes (alternative to max_size_mb)

**YAML Example:**

```yaml
- type: "FileSizeCheck"
  severity: "WARNING"
  params:
    min_size_mb: 1
    max_size_gb: 10
```

**Use Cases:**
- Catch corrupt or truncated files
- Prevent processing unexpectedly large files
- Detect failed extracts (too small)

**Tips:**
- Use to catch malformed files early in processing
- Set max size to prevent memory issues with very large files
- Combine with RowCountRangeCheck for comprehensive volume validation

---

#### 4. CSVFormatCheck

**Description:** Validates CSV file format integrity before processing. Detects malformed files with inconsistent column counts, delimiter issues, and encoding problems.

**Source Compatibility:** 📁 CSV files only

**Parameters:**
- `delimiter` (string, optional) - Expected delimiter character. Auto-detected if not specified.
- `sample_rows` (number, optional, default: `1000`) - Number of rows to check for consistency
- `max_errors` (number, optional, default: `10`) - Maximum formatting errors before failing

**YAML Example:**

```yaml
- type: "CSVFormatCheck"
  severity: "ERROR"
  params:
    delimiter: ","
    sample_rows: 5000
    max_errors: 5
```

**Use Cases:**
- Detect malformed CSV files before processing
- Catch delimiter mismatches (e.g., pipe-delimited file parsed as comma-delimited)
- Identify quoting issues and unescaped delimiters in fields
- Validate encoding compatibility

**Tips:**
- Run this check first before other validations to catch format issues early
- Auto-detection works for common delimiters (comma, tab, pipe, semicolon)
- If you know the delimiter, specify it explicitly for faster validation
- High error rates (>10%) indicate fundamental format problems

---

### Schema Validations

Validate data structure and column definitions.

#### 4. SchemaMatchCheck

**Description:** Validates columns and their data types match expected schema.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `expected_columns` (text, required) - Comma-separated list of expected column names
- `allow_extra` (boolean, optional, default: `false`) - Permit additional columns not in expected list
- `allow_missing` (boolean, optional, default: `false`) - Permit missing columns from expected list

**YAML Example:**

```yaml
- type: "SchemaMatchCheck"
  severity: "ERROR"
  params:
    expected_columns: "customer_id,name,email,created_date"
    allow_extra: false
    allow_missing: false
```

**Use Cases:**
- Enforce strict schema contracts at system boundaries
- Prevent schema drift in data interfaces
- Validate file formats match database table definitions

**Tips:**
- Use strict matching (both `false`) for critical interfaces
- Set `allow_extra: true` for backward compatibility
- Use ColumnPresenceCheck for lighter-weight column existence validation

---

#### 5. ColumnPresenceCheck

**Description:** Checks that required columns exist in the file.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `required_columns` (text, required) - Comma-separated list of required column names
- `case_sensitive` (boolean, optional, default: `true`) - Whether column name matching is case-sensitive

**YAML Example:**

```yaml
- type: "ColumnPresenceCheck"
  severity: "ERROR"
  params:
    required_columns: "customer_id,order_id,amount"
    case_sensitive: false
```

**Use Cases:**
- Ensure critical columns exist before processing
- Validate required fields in extracts
- Quick schema validation without type checking

**Tips:**
- Lighter weight than SchemaMatchCheck (doesn't validate types)
- Use when you only care about column existence
- Set `case_sensitive: false` for more forgiving validation

---

### Field-Level Validations

Validate individual field values and formats.

#### 6. MandatoryFieldCheck

**Description:** Validates that specified fields are not null or empty.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `fields` (text, required) - Comma-separated list of fields that cannot be null or empty

**YAML Example:**

```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: "customer_id,email,order_id"
```

**Use Cases:**
- Enforce NOT NULL constraints
- Validate primary/foreign keys are populated
- Ensure critical business fields have values

**Tips:**
- Use ERROR severity for primary keys and critical fields
- Combine with ValidValuesCheck to also validate allowed values
- For percentage-based completeness, use CompletenessCheck instead

---

#### 7. RegexCheck

**Description:** Validates field values match a regular expression pattern.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Field to validate
- `pattern` (text, required) - Regular expression pattern to match
- `message` (text, optional) - Custom error message for failures
- `invert` (boolean, optional, default: `false`) - Fail if pattern DOES match (instead of does not match)

**YAML Example:**

```yaml
# Email validation
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    message: "Invalid email format"

# UK postcode validation
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "postcode"
    pattern: "^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$"

# Phone number (invert to reject invalid patterns)
- type: "RegexCheck"
  severity: "WARNING"
  params:
    field: "phone"
    pattern: "^[0-9]{3}-[0-9]{3}-[0-9]{4}$"
    invert: false
```

**Common Patterns:**

| Format | Pattern |
|--------|---------|
| Email | `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$` |
| US Phone | `^[0-9]{3}-[0-9]{3}-[0-9]{4}$` |
| US SSN | `^[0-9]{3}-[0-9]{2}-[0-9]{4}$` |
| UK Postcode | `^[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}$` |
| UUID | `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$` |

**Tips:**
- Test patterns thoroughly using online regex testers (regex101.com)
- Escape backslashes in YAML (`\\` becomes `\` in regex)
- Use `invert: true` to blacklist patterns instead of whitelist
- For simple value lists, use ValidValuesCheck instead

---

#### 8. ValidValuesCheck

**Description:** Validates field values are from allowed list.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Field to validate
- `valid_values` (text, required) - Comma-separated list of acceptable values
- `case_sensitive` (boolean, optional, default: `true`) - Whether value matching is case-sensitive

**YAML Example:**

```yaml
# Status field with allowed values
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "status"
    valid_values: "Active,Pending,Completed,Cancelled"
    case_sensitive: true

# Country codes (case insensitive)
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "country_code"
    valid_values: "US,UK,CA,AU,DE,FR,JP"
    case_sensitive: false
```

**Use Cases:**
- Validate enum-like fields (status, type, category)
- Enforce allowed values for categorical data
- Check reference data membership

**Tips:**
- Ideal for fields with fixed set of values (< 50)
- Use ReferentialIntegrityCheck for large reference lists
- Set `case_sensitive: false` for more forgiving validation

---

#### 9. RangeCheck

**Description:** Validates numeric field values are within specified range.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Numeric field to validate
- `min_value` (number, optional) - Minimum acceptable value (inclusive)
- `max_value` (number, optional) - Maximum acceptable value (inclusive)

**YAML Example:**

```yaml
# Age validation
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "age"
    min_value: 18
    max_value: 120

# Price validation (only min)
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "price"
    min_value: 0.01

# Percentage (0-100)
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "discount_percent"
    min_value: 0
    max_value: 100
```

**Use Cases:**
- Validate ages, prices, quantities, percentages
- Enforce business rules (min order amount, max discount)
- Catch data entry errors and outliers

**Tips:**
- Specify only min or only max if one-sided bound
- For outlier detection, use StatisticalOutlierCheck instead
- Combine with NumericPrecisionCheck to validate decimal places

---

#### 10. DateFormatCheck

**Description:** Validates date field matches expected format.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Date field to validate
- `format` (text, required, default: `%Y-%m-%d`) - Expected date format (Python strftime)
- `allow_null` (boolean, optional, default: `true`) - Allow null/empty values

**YAML Example:**

```yaml
# ISO 8601 date format
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "created_date"
    format: "%Y-%m-%d"
    allow_null: false

# US date format
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "order_date"
    format: "%m/%d/%Y"

# Datetime with timestamp
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "timestamp"
    format: "%Y-%m-%d %H:%M:%S"
```

**Common Date Formats:**

| Format | Example | Pattern |
|--------|---------|---------|
| ISO 8601 | 2024-12-31 | `%Y-%m-%d` |
| US Format | 12/31/2024 | `%m/%d/%Y` |
| EU Format | 31/12/2024 | `%d/%m/%Y` |
| Timestamp | 2024-12-31 14:30:00 | `%Y-%m-%d %H:%M:%S` |
| Long Format | December 31, 2024 | `%B %d, %Y` |

**Tips:**
- Test format strings with sample data first
- Use FreshnessCheck to validate date recency
- Set `allow_null: false` for mandatory date fields

---

#### 11. InlineRegexCheck

**Description:** Inline regex validation with custom pattern and field (lightweight alternative to RegexCheck).

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Field to validate
- `pattern` (text, required) - Regular expression pattern

**YAML Example:**

```yaml
- type: "InlineRegexCheck"
  severity: "ERROR"
  params:
    field: "account_number"
    pattern: "^ACC[0-9]{8}$"
```

**Tips:**
- Lightweight alternative to RegexCheck for simple patterns
- Use RegexCheck for more complex validations with custom messages

---

### Record-Level Validations

Validate entire records and row-level properties.

#### 12. DuplicateRowCheck

**Description:** Checks for duplicate rows based on key fields.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `key_fields` (text, required) - Comma-separated list of fields that define uniqueness

**YAML Example:**

```yaml
# Check for duplicate orders
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields: "customer_id,order_id"

# Check for duplicate emails
- type: "DuplicateRowCheck"
  severity: "WARNING"
  params:
    key_fields: "email"
```

**Use Cases:**
- Detect duplicate records in data loads
- Validate composite primary keys
- Find data quality issues in source systems

**Tips:**
- Use for business key uniqueness validation
- For single-field uniqueness, consider UniqueKeyCheck
- Memory-efficient with automatic disk spillover for large files

---

#### 13. BlankRecordCheck

**Description:** Checks for completely blank rows (all fields empty/null).

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:** None

**YAML Example:**

```yaml
- type: "BlankRecordCheck"
  severity: "WARNING"
```

**Use Cases:**
- Detect trailing blank rows in CSV exports
- Find malformed data extracts
- Identify data quality issues

**Tips:**
- Useful for catching CSV export issues
- Usually WARNING severity (rarely blocks processing)
- Automatically ignores header rows

---

#### 14. UniqueKeyCheck

**Description:** Validates specified field(s) contain unique values.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `key_fields` (text, required) - Comma-separated list of fields that must be unique

**YAML Example:**

```yaml
# Single field uniqueness
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    key_fields: "transaction_id"

# Composite key uniqueness
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    key_fields: "customer_id,account_id"
```

**Use Cases:**
- Validate primary key constraints
- Ensure unique identifiers
- Check composite key uniqueness

**Tips:**
- Similar to DuplicateRowCheck but focused on key uniqueness
- Memory-efficient with disk spillover for large datasets
- Use ERROR severity for primary keys

---

### Advanced Validations

Advanced data quality checks including statistical analysis, cross-field logic, and business rules.

#### 15. CompletenessCheck

**Description:** Validates field completeness (non-null percentage) meets threshold.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Field to check completeness
- `min_completeness` (number, required, range: 0-100, default: 95) - Minimum acceptable percentage of non-null values

**YAML Example:**

```yaml
# Email should be 95% complete
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "email"
    min_completeness: 95

# Phone number 80% complete
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "phone_number"
    min_completeness: 80
```

**Use Cases:**
- Monitor optional field population rates
- Track data quality metrics
- Alert on declining data completeness

**Tips:**
- Use WARNING for optional fields
- Use ERROR for critical fields with allowable null rate
- For 100% completeness (no nulls), use MandatoryFieldCheck instead

---

#### 16. StatisticalOutlierCheck

**Description:** Detects statistical outliers using IQR or Z-score method.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Numeric field to check for outliers
- `method` (select, optional, options: `iqr` or `zscore`, default: `iqr`) - Statistical method for outlier detection
- `threshold` (number, optional, default: 1.5) - IQR multiplier (default 1.5) or Z-score threshold (default 3)

**YAML Example:**

```yaml
# Detect price outliers using IQR
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "price"
    method: "iqr"
    threshold: 1.5

# Detect outliers using Z-score
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "transaction_amount"
    method: "zscore"
    threshold: 3
```

**Methods:**

| Method | Description | When to Use |
|--------|-------------|-------------|
| **IQR** | Interquartile range method | More robust for non-normal distributions |
| **Z-score** | Standard deviation method | Better for normally distributed data |

**Tips:**
- IQR is more robust and recommended for most use cases
- Adjust threshold to control sensitivity (lower = more outliers detected)
- Use WARNING severity as outliers may be valid data
- Combine with AdvancedAnomalyDetectionCheck for ML-based detection

---

#### 17. CrossFieldComparisonCheck

**Description:** Validates relationship between two fields (e.g., start < end).

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field_a` (text, required) - First field in comparison
- `operator` (select, required, options: `<`, `<=`, `>`, `>=`, `==`, `!=`) - Comparison operator
- `field_b` (text, required) - Second field in comparison

**YAML Example:**

```yaml
# Start date before end date
- type: "CrossFieldComparisonCheck"
  severity: "ERROR"
  params:
    field_a: "start_date"
    operator: "<"
    field_b: "end_date"

# Discount less than or equal to price
- type: "CrossFieldComparisonCheck"
  severity: "ERROR"
  params:
    field_a: "discount"
    operator: "<="
    field_b: "price"

# Amount paid equals amount received
- type: "CrossFieldComparisonCheck"
  severity: "WARNING"
  params:
    field_a: "amount_paid"
    operator: "=="
    field_b: "amount_received"
```

**Use Cases:**
- Validate date ranges (start < end)
- Ensure logical consistency (discount <= price)
- Check field relationships (min <= max)

**Tips:**
- Useful for date range validation
- Combine with numeric tolerance for approximate equality
- Use ERROR for critical business logic violations

---

#### 18. FreshnessCheck

**Description:** Validates data freshness based on timestamp field.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `timestamp_field` (text, required) - Field containing timestamp/date
- `max_age_hours` (number, required, min: 0, default: 24) - Maximum acceptable age in hours

**YAML Example:**

```yaml
# Data must be within 24 hours
- type: "FreshnessCheck"
  severity: "WARNING"
  params:
    timestamp_field: "created_at"
    max_age_hours: 24

# Real-time data (within 1 hour)
- type: "FreshnessCheck"
  severity: "ERROR"
  params:
    timestamp_field: "last_updated"
    max_age_hours: 1
```

**Use Cases:**
- Validate data recency in near-real-time pipelines
- Detect stale data in caches
- Ensure data meets SLA requirements

**Tips:**
- Use WARNING for monitoring, ERROR for hard requirements
- Adjust hours based on data refresh frequency
- Useful for real-time and near-real-time data pipelines

---

#### 19. StringLengthCheck

**Description:** Validates string field length is within bounds.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - String field to validate
- `min_length` (number, optional, min: 0) - Minimum acceptable string length
- `max_length` (number, optional, min: 1) - Maximum acceptable string length

**YAML Example:**

```yaml
# Phone number length
- type: "StringLengthCheck"
  severity: "ERROR"
  params:
    field: "phone_number"
    min_length: 10
    max_length: 15

# Description max length
- type: "StringLengthCheck"
  severity: "WARNING"
  params:
    field: "description"
    max_length: 500
```

**Use Cases:**
- Validate field sizes match database constraints
- Enforce string length requirements
- Catch truncation issues

**Tips:**
- Use to prevent database truncation errors
- Set max_length based on database column definitions
- Combine with RegexCheck for format + length validation

---

#### 20. NumericPrecisionCheck

**Description:** Validates numeric precision (decimal places).

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Numeric field to validate
- `max_decimals` (number, required, min: 0, default: 2) - Maximum allowed decimal places

**YAML Example:**

```yaml
# Currency with 2 decimals
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "amount"
    max_decimals: 2

# Percentage with 1 decimal
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "discount_rate"
    max_decimals: 1
```

**Use Cases:**
- Ensure currency amounts have correct precision
- Validate numeric precision matches database columns
- Catch data truncation issues

**Tips:**
- Optimized with vectorized operations (very fast)
- Use ERROR to prevent database precision errors
- Set max_decimals based on database DECIMAL(n,p) definition

---

#### 21. InlineBusinessRuleCheck

**Description:** Custom business rule expressed as Python-like expression.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `expression` (textarea, required) - Python-like expression to evaluate
- `message` (text, optional) - Custom error message when rule fails

**YAML Example:**

```yaml
# Validate calculated total
- type: "InlineBusinessRuleCheck"
  severity: "ERROR"
  params:
    expression: "(quantity * unit_price) == total_amount"
    message: "Total amount does not match quantity * unit_price"

# Validate discount rules
- type: "InlineBusinessRuleCheck"
  severity: "ERROR"
  params:
    expression: "discount_amount <= total_amount"
```

**Tips:**
- Flexible business logic without writing Python code
- Use for complex calculated field validation
- Expression evaluated row-by-row (may be slower for large datasets)

---

#### 22. InlineLookupCheck

**Description:** Validates field values against inline lookup list.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Field to validate
- `lookup_values` (json, required) - Valid values as JSON array

**YAML Example:**

```yaml
- type: "InlineLookupCheck"
  severity: "ERROR"
  params:
    field: "region"
    lookup_values: '["North", "South", "East", "West"]'
```

**Tips:**
- Alternative to ValidValuesCheck with JSON syntax
- Use ValidValuesCheck for simpler comma-separated values

---

### Cross-File Validations

Validate relationships and consistency across multiple files.

#### 23. ReferentialIntegrityCheck

**Description:** Validates foreign key relationships between files.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `local_field` (text, required) - Field in current file (foreign key)
- `reference_file` (text, required) - File containing reference data
- `reference_field` (text, required) - Field in reference file (primary key)

**YAML Example:**

```yaml
# Validate customer_id exists in customers file
- type: "ReferentialIntegrityCheck"
  severity: "ERROR"
  params:
    local_field: "customer_id"
    reference_file: "data/customers.csv"
    reference_field: "id"
```

**Use Cases:**
- Validate foreign key relationships
- Ensure referential integrity across files
- Catch orphaned records

**Tips:**
- Memory-efficient with disk spillover for large reference files
- Use ERROR severity to block loads with orphaned records
- Reference file loaded once and cached

---

#### 24. CrossFileComparisonCheck

**Description:** Compares metrics between two files (row counts, sums, etc.).

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `metric` (select, required, options: `row_count`, `sum`, `average`, `min`, `max`) - What to compare
- `field` (text, optional) - Field to aggregate (not needed for row_count)
- `reference_file` (text, required) - File to compare against
- `reference_field` (text, optional) - Field in reference file (if different from field)
- `tolerance_percent` (number, optional, min: 0, default: 0) - Acceptable variance percentage

**YAML Example:**

```yaml
# Compare row counts
- type: "CrossFileComparisonCheck"
  severity: "WARNING"
  params:
    metric: "row_count"
    reference_file: "previous_load.csv"
    tolerance_percent: 5

# Compare total amounts
- type: "CrossFileComparisonCheck"
  severity: "WARNING"
  params:
    metric: "sum"
    field: "amount"
    reference_file: "transactions.csv"
    reference_field: "amount"
    tolerance_percent: 1
```

**Use Cases:**
- Data reconciliation across files
- Validate incremental loads
- Compare current vs previous data

**Tips:**
- Use tolerance_percent for approximate comparisons
- Useful for data reconciliation and consistency checks
- Combine with BaselineComparisonCheck for time-series monitoring

---

#### 25. CrossFileDuplicateCheck

**Description:** Checks for duplicate keys across multiple files.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `key_fields` (text, required) - Comma-separated list of fields that define uniqueness
- `reference_files` (text, required) - Comma-separated list of files to check for duplicates

**YAML Example:**

```yaml
# Check transaction_id unique across all files
- type: "CrossFileDuplicateCheck"
  severity: "ERROR"
  params:
    key_fields: "transaction_id"
    reference_files: "transactions_2024.csv,transactions_archive.csv"
```

**Use Cases:**
- Prevent duplicates across incremental loads
- Validate uniqueness across partitioned files
- Check for overlapping data

**Tips:**
- Checks current file + all reference files
- Memory-efficient with disk spillover
- Use ERROR to prevent duplicate loads

---

#### 26. CrossFileKeyCheck

**Description:** Advanced cross-file referential integrity check with multiple check modes and memory-efficient processing.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `foreign_key` (text, required) - Column(s) to validate (comma-separated for composite keys)
- `reference_file` (text, required) - Path to reference file containing valid keys
- `reference_key` (text, required) - Column(s) in reference file to check against
- `check_mode` (select, optional, options: `exact_match`, `overlap`, `subset`, `superset`, default: `exact_match`) - Type of referential integrity check
- `allow_null` (boolean, optional, default: `false`) - Whether to allow NULL foreign key values
- `min_overlap_pct` (number, optional, range: 0-100, default: 1.0) - Minimum percentage of keys that must overlap (for overlap mode)
- `reference_file_format` (select, optional, options: `csv`, `parquet`, `json`, `excel`, default: `csv`) - Format of reference file

**YAML Example:**

```yaml
# Exact match (strict FK check)
- type: "CrossFileKeyCheck"
  severity: "ERROR"
  params:
    foreign_key: "customer_id"
    reference_file: "customers.csv"
    reference_key: "id"
    check_mode: "exact_match"

# Overlap mode (partial matching)
- type: "CrossFileKeyCheck"
  severity: "WARNING"
  params:
    foreign_key: "account_id"
    reference_file: "accounts.parquet"
    reference_key: "account_number"
    reference_file_format: "parquet"
    check_mode: "overlap"
    min_overlap_pct: 80

# Composite key validation
- type: "CrossFileKeyCheck"
  severity: "ERROR"
  params:
    foreign_key: "customer_id,account_id"
    reference_file: "customer_accounts.csv"
    reference_key: "cust_id,acct_id"
    check_mode: "exact_match"
```

**Check Modes:**

| Mode | Description | Use Case |
|------|-------------|----------|
| `exact_match` | All foreign keys must exist in reference | Strict FK validation |
| `overlap` | Minimum percentage of keys must overlap | Partial matching validation |
| `subset` | All data keys must be in reference (allows extra in reference) | Subset validation |
| `superset` | All reference keys must be in data (allows extra in data) | Completeness check |

**Tips:**
- Memory-efficient with automatic disk spillover (handles billions of keys)
- Supports composite keys for multi-column relationships
- Use `exact_match` for strict FK constraints
- Use `overlap` with min_overlap_pct for flexible matching
- Supports Parquet reference files for faster loading

---

### Conditional Validations

Execute validations conditionally based on data values.

#### 27. ConditionalValidation

**Description:** Executes validation rules conditionally based on SQL-like expression.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `condition` (text, required) - SQL-like WHERE clause to evaluate
- `validations` (json, required) - List of validations to run when condition is true (as JSON array)

**YAML Example:**

```yaml
# If account type is BUSINESS, validate company fields
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "account_type == 'BUSINESS'"
    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["company_name", "tax_id"]

# If status is Active, validate required fields
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "status == 'Active'"
    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["email", "phone"]
      - type: "RangeCheck"
        params:
          field: "account_balance"
          min_value: 0
```

**Use Cases:**
- Complex business rule validation
- Type-specific validation (B2B vs B2C)
- Status-dependent validation

**Tips:**
- Powerful for complex conditional business logic
- Nest multiple validations within condition
- Use SQL-like syntax for conditions (==, !=, AND, OR, etc.)

---

### Database Validations

Validate data against live database constraints and relationships.

#### 28. DatabaseConstraintCheck

**Description:** Validates data against database constraints (PK, FK, unique, not null).

**Source Compatibility:** 🗄️ Database sources only

**Parameters:**
- `connection_string` (text, required) - Database connection string
- `table_name` (text, required) - Target database table
- `constraints` (select, required, options: `all`, `primary_key`, `foreign_key`, `unique`, `not_null`) - Which constraints to validate

**YAML Example:**

```yaml
# Validate all constraints
- type: "DatabaseConstraintCheck"
  severity: "ERROR"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    table_name: "customers"
    constraints: "all"

# Validate only primary key
- type: "DatabaseConstraintCheck"
  severity: "ERROR"
  params:
    connection_string: "sqlite:///test.db"
    table_name: "orders"
    constraints: "primary_key"
```

**Use Cases:**
- Catch constraint violations before database insert
- Validate data against target database schema
- Pre-flight validation for ETL loads

**Tips:**
- Runs actual database constraint checks
- Use ERROR severity to prevent database load failures
- Faster than attempting insert and rolling back

---

#### 29. DatabaseReferentialIntegrityCheck

**Description:** Validates foreign key relationships against actual database.

**Source Compatibility:** 🗄️ Database sources only

**Parameters:**
- `connection_string` (text, required) - Database connection string
- `local_field` (text, required) - Foreign key field in data
- `reference_table` (text, required) - Database table containing valid values
- `reference_column` (text, required) - Column in reference table (primary key)

**YAML Example:**

```yaml
# Validate customer_id against database
- type: "DatabaseReferentialIntegrityCheck"
  severity: "ERROR"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    local_field: "customer_id"
    reference_table: "customers"
    reference_column: "id"
```

**Use Cases:**
- Real-time FK validation against live database
- Validate against master data in database
- Ensure referential integrity before load

**Tips:**
- Validates against live database (not file)
- Use for real-time FK validation
- More current than file-based ReferentialIntegrityCheck

---

#### 30. SQLCustomCheck

**Description:** Executes custom SQL query for validation.

**Source Compatibility:** 🗄️ Database sources only

**Parameters:**
- `connection_string` (text, required) - Database connection string
- `query` (textarea, required) - Custom SQL query to execute (must return boolean or row count)
- `expected_result` (text, optional) - Expected query result for validation

**YAML Example:**

```yaml
# Custom business rule via SQL
- type: "SQLCustomCheck"
  severity: "ERROR"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    query: "SELECT COUNT(*) FROM orders WHERE status = 'Pending'"
    expected_result: "0"

# Complex validation query
- type: "SQLCustomCheck"
  severity: "WARNING"
  params:
    connection_string: "sqlite:///test.db"
    query: |
      SELECT COUNT(*)
      FROM transactions t
      LEFT JOIN accounts a ON t.account_id = a.id
      WHERE a.id IS NULL
    expected_result: "0"
```

**Use Cases:**
- Custom business logic validation via SQL
- Complex database-driven validations
- Maximum flexibility for database checks

**Tips:**
- Maximum flexibility for complex validations
- Query can return boolean or numeric result
- Use for validations not covered by built-in checks

---

### Temporal Validations

Validate historical trends and time-based patterns.

#### 31. BaselineComparisonCheck

**Description:** Compares current data against historical baseline.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `metric` (select, required, options: `row_count`, `sum`, `average`, `min`, `max`) - What to compare
- `field` (text, optional) - Field to measure (not needed for row_count)
- `baseline_file` (text, required) - Historical baseline file path
- `tolerance_percent` (number, required, min: 0, default: 10) - Acceptable variance from baseline

**YAML Example:**

```yaml
# Compare to yesterday's row count
- type: "BaselineComparisonCheck"
  severity: "WARNING"
  params:
    metric: "row_count"
    baseline_file: "previous_day.csv"
    tolerance_percent: 10

# Compare average order value
- type: "BaselineComparisonCheck"
  severity: "WARNING"
  params:
    metric: "average"
    field: "order_value"
    baseline_file: "last_month.csv"
    tolerance_percent: 20
```

**Use Cases:**
- Detect unusual data volumes
- Monitor metric changes over time
- Alert on significant deviations from baseline

**Tips:**
- Use for trend monitoring and anomaly detection
- Adjust tolerance based on expected variance
- Combine with TrendDetectionCheck for advanced time-series analysis

---

#### 32. TrendDetectionCheck

**Description:** Detects trends and anomalies in time-series data.

**Source Compatibility:** 📁 File sources only

**Parameters:**
- `timestamp_field` (text, required) - Field containing timestamp
- `value_field` (text, required) - Numeric field to analyze for trends
- `trend_type` (select, required, options: `increasing`, `decreasing`, `stable`, `anomaly`) - Type of trend to detect
- `sensitivity` (select, optional, options: `low`, `medium`, `high`, default: `medium`) - Detection sensitivity

**YAML Example:**

```yaml
# Detect anomalous sales trends
- type: "TrendDetectionCheck"
  severity: "WARNING"
  params:
    timestamp_field: "order_date"
    value_field: "daily_sales"
    trend_type: "anomaly"
    sensitivity: "medium"

# Detect increasing error rates
- type: "TrendDetectionCheck"
  severity: "WARNING"
  params:
    timestamp_field: "timestamp"
    value_field: "error_count"
    trend_type: "increasing"
    sensitivity: "high"
```

**Use Cases:**
- Monitor KPIs for unusual patterns
- Detect sudden trend changes
- Alert on anomalous behavior

**Tips:**
- Useful for time-series monitoring
- Adjust sensitivity to control alert frequency
- Use for KPI monitoring and trend analysis

---

### Statistical Validations

Advanced statistical analysis and ML-based anomaly detection.

#### 33. DistributionCheck

**Description:** Validates data distribution matches expected pattern.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field` (text, required) - Field to analyze distribution
- `distribution_type` (select, required, options: `normal`, `uniform`, `exponential`, `custom`) - Expected statistical distribution
- `significance_level` (number, optional, range: 0-1, default: 0.05) - P-value threshold for statistical test

**YAML Example:**

```yaml
# Validate ages follow normal distribution
- type: "DistributionCheck"
  severity: "WARNING"
  params:
    field: "age"
    distribution_type: "normal"
    significance_level: 0.05
```

**Use Cases:**
- Validate statistical assumptions
- Detect distribution shifts
- Advanced data quality analysis

**Tips:**
- Advanced statistical validation for data scientists
- Use for validating ML feature distributions
- Requires sufficient sample size for accurate results

---

#### 34. CorrelationCheck

**Description:** Validates correlation between two numeric fields.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `field_a` (text, required) - First numeric field
- `field_b` (text, required) - Second numeric field
- `min_correlation` (number, optional, range: -1 to 1) - Minimum acceptable correlation coefficient
- `max_correlation` (number, optional, range: -1 to 1) - Maximum acceptable correlation coefficient
- `method` (select, optional, options: `pearson`, `spearman`, `kendall`, default: `pearson`) - Statistical correlation method

**YAML Example:**

```yaml
# Validate price and quantity correlation
- type: "CorrelationCheck"
  severity: "WARNING"
  params:
    field_a: "price"
    field_b: "quantity"
    min_correlation: -0.5
    max_correlation: -0.1
    method: "pearson"
```

**Use Cases:**
- Detect unexpected relationships between variables
- Validate expected correlations
- Monitor feature relationships for ML models

**Tips:**
- Pearson for linear relationships
- Spearman for monotonic relationships
- Use for detecting unexpected data patterns

---

#### 35. AdvancedAnomalyDetectionCheck

**Description:** ML-based anomaly detection using isolation forest or local outlier factor.

**Source Compatibility:** 📁 File sources | 🗄️ Database sources

**Parameters:**
- `fields` (text, required) - Comma-separated list of numeric fields to analyze
- `method` (select, optional, options: `isolation_forest`, `local_outlier_factor`, `one_class_svm`, default: `isolation_forest`) - Machine learning algorithm
- `contamination` (number, optional, range: 0-0.5, default: 0.1) - Expected proportion of outliers

**YAML Example:**

```yaml
# Detect fraudulent transactions
- type: "AdvancedAnomalyDetectionCheck"
  severity: "WARNING"
  params:
    fields: "amount,frequency,location_score"
    method: "isolation_forest"
    contamination: 0.05
```

**Use Cases:**
- Fraud detection
- Complex anomaly detection across multiple features
- ML-based quality checks

**Tips:**
- Advanced ML-based detection for complex patterns
- Isolation Forest recommended for most use cases
- Adjust contamination based on expected outlier rate
- Combine with StatisticalOutlierCheck for comprehensive outlier detection

---

## See Also

- **[CLI Guide](CLI_GUIDE.md)** - Complete command-line reference
- **[Configuration Guide](docs/using-datak9/configuration-guide.md)** - YAML syntax and examples
- **[Best Practices](docs/using-datak9/best-practices.md)** - Production patterns
- **[Examples](docs/examples/)** - Industry-specific validation examples

---

**🐕 DataK9 - Your K9 guardian for data quality**
