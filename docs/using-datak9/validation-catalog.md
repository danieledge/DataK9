# DataK9 Validation Catalog

**Complete reference of all 35 validation types** 🐕

This catalog provides comprehensive documentation for every validation check available in DataK9. Each entry includes purpose, parameters, examples, and use cases. Like a well-trained K9, these validations sniff out data quality issues with precision!

---

## Table of Contents

### By Category
- [File-Level Validations](#file-level-validations) - Check overall file properties
- [Schema Validations](#schema-validations) - Validate column structure
- [Field-Level Validations](#field-level-validations) - Check individual field values
- [Record-Level Validations](#record-level-validations) - Check across rows
- [Conditional Validations](#conditional-validations) - Apply rules conditionally
- [Advanced Validations](#advanced-validations) - Statistical and complex checks
- [Cross-File Validations](#cross-file-validations) - Validate relationships between files
- [Database Validations](#database-validations) - Direct database validation support
- [Temporal/Historical Validations](#temporalhistorical-validations) - Compare against historical baselines
- [Statistical Validations](#statistical-validations) - Advanced statistical tests

### Quick Reference
[Jump to Quick Reference Table](#quick-reference-table)

---

## File-Level Validations

These validations check properties of the entire file before processing individual rows.

### EmptyFileCheck

**Purpose:** Ensures the file contains data (not empty or 0 bytes) and optionally has data rows

**Use Cases:**
- Detect upstream pipeline failures
- Catch missing or corrupted files
- Prevent processing empty exports
- Detect header-only files with no data rows

**Parameters:**
- `check_data_rows` (boolean, optional): If `true`, also verifies file has at least one data row (not just headers). Default: `false`

**Examples:**
```yaml
# Check only for literal empty files (0 bytes)
- type: "EmptyFileCheck"
  severity: "ERROR"

# Check for both empty files AND header-only files
- type: "EmptyFileCheck"
  severity: "ERROR"
  params:
    check_data_rows: true
```

**Passes when:**
- Basic mode: File has content (size > 0 bytes)
- With `check_data_rows: true`: File has content AND contains at least one data row

**Fails when:**
- Basic mode: File is empty or 0 bytes
- With `check_data_rows: true`: File is empty OR contains only headers with no data rows

**Supported Formats:** CSV, Excel, JSON, Parquet

---

### RowCountRangeCheck

**Purpose:** Validates file has expected number of rows

**Use Cases:**
- Detect incomplete data loads
- Verify full extracts completed
- Catch unexpected data volumes
- Monitor data growth

**Parameters:**
- `min_rows` (integer, optional): Minimum acceptable row count
- `max_rows` (integer, optional): Maximum acceptable row count

**Example:**
```yaml
# Expect between 1,000 and 1,000,000 rows
- type: "RowCountRangeCheck"
  severity: "WARNING"
  params:
    min_rows: 1000
    max_rows: 1000000

# At least 100 rows required
- type: "RowCountRangeCheck"
  severity: "ERROR"
  params:
    min_rows: 100

# No more than 50,000 rows
- type: "RowCountRangeCheck"
  severity: "WARNING"
  params:
    max_rows: 50000
```

**Passes when:** Row count is within specified range

**Fails when:** Row count is outside range

---

### FileSizeCheck

**Purpose:** Validates file size is within acceptable range

**Use Cases:**
- Detect incomplete transfers
- Catch unexpectedly large files
- Monitor data volume growth
- Prevent memory issues

**Parameters:**
- `min_size_mb` (float, optional): Minimum file size in megabytes
- `max_size_mb` (float, optional): Maximum file size in megabytes

**Example:**
```yaml
# File should be 1-100 MB
- type: "FileSizeCheck"
  severity: "WARNING"
  params:
    min_size_mb: 1.0
    max_size_mb: 100.0

# File must be at least 5 MB
- type: "FileSizeCheck"
  severity: "ERROR"
  params:
    min_size_mb: 5.0
```

**Passes when:** File size is within range

**Fails when:** File size is outside range

---

## Schema Validations

These validations check the structure and schema of your data.

### SchemaMatchCheck

**Purpose:** Validates columns match expected schema exactly

**Use Cases:**
- Enforce strict schema contracts
- Detect schema drift
- Validate API contract compliance
- Ensure data warehouse compatibility

**Parameters:**
- `expected_columns` (list): List of expected column names in order
- `allow_extra` (boolean): Allow extra columns not in expected list (default: false)
- `allow_missing` (boolean): Allow missing columns from expected list (default: false)

**Example:**
```yaml
# Exact schema match required
- type: "SchemaMatchCheck"
  severity: "ERROR"
  params:
    expected_columns:
      - "customer_id"
      - "first_name"
      - "last_name"
      - "email"
      - "created_date"

# Allow extra columns
- type: "SchemaMatchCheck"
  severity: "ERROR"
  params:
    expected_columns: ["id", "name", "email"]
    allow_extra: true

# Flexible: must have these columns but others OK
- type: "SchemaMatchCheck"
  severity: "WARNING"
  params:
    expected_columns: ["id", "name"]
    allow_extra: true
    allow_missing: false
```

**Passes when:** Schema matches expectations

**Fails when:** Columns missing, extra, or in wrong order

---

### ColumnPresenceCheck

**Purpose:** Ensures required columns exist (order doesn't matter)

**Use Cases:**
- Verify required fields exist
- More flexible than SchemaMatchCheck
- Check for critical columns only

**Parameters:**
- `columns` (list): List of required column names

**Example:**
```yaml
# These columns must exist (any order, extra columns OK)
- type: "ColumnPresenceCheck"
  severity: "ERROR"
  params:
    columns:
      - "customer_id"
      - "email"
      - "transaction_date"
```

**Passes when:** All specified columns exist

**Fails when:** Any specified column is missing

---

## Field-Level Validations

These validations check individual field values row by row.

### MandatoryFieldCheck

**Purpose:** Ensures required fields have values (not null or empty)

**Use Cases:**
- Validate primary keys
- Enforce required business fields
- Check data completeness
- Prevent null values in critical fields

**Parameters:**
- `fields` (list): List of field names to check
- `allow_whitespace` (boolean): Allow whitespace-only values (default: false)

**Example:**
```yaml
# Basic required fields
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields:
      - "customer_id"
      - "email"
      - "first_name"
      - "last_name"

# Don't allow whitespace-only values
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["name"]
    allow_whitespace: false
```

**Passes when:** All specified fields have non-null, non-empty values

**Fails when:** Any field is null, empty, or whitespace-only

**Conditional Example:**
```yaml
# Company name required only for business accounts
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["company_name", "tax_id"]
  condition: "account_type == 'BUSINESS'"
```

---

### RegexCheck

**Purpose:** Validates field values match a regular expression pattern

**Use Cases:**
- Email format validation
- Phone number format
- Postal codes
- Account numbers
- Product codes
- Any pattern-based validation

**Parameters:**
- `field` (string): Field name to validate
- `pattern` (string): Regular expression pattern
- `message` (string, optional): Custom error message
- `invert` (boolean, optional): Invert match (fail if matches) (default: false)

**Example:**
```yaml
# Email validation
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    message: "Invalid email format"

# US phone number (xxx-xxx-xxxx)
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "phone"
    pattern: "^[0-9]{3}-[0-9]{3}-[0-9]{4}$"

# US ZIP code
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "zip_code"
    pattern: "^[0-9]{5}(-[0-9]{4})?$"

# Product code (PROD-XXXX)
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "product_code"
    pattern: "^PROD-[0-9]{4}$"

# Social Security Number
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "ssn"
    pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"

# Invert: fail if matches (detect test data)
- type: "RegexCheck"
  severity: "WARNING"
  params:
    field: "email"
    pattern: "test@example\\.com"
    invert: true
    message: "Test email addresses detected"
```

**Passes when:** Value matches pattern (or doesn't match if inverted)

**Fails when:** Value doesn't match pattern (or matches if inverted)

**Common Patterns:**
- Email: `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$`
- URL: `^https?://[^\\s/$.?#].[^\\s]*$`
- IP Address: `^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$`
- Date (YYYY-MM-DD): `^[0-9]{4}-[0-9]{2}-[0-9]{2}$`
- UUID: `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`

---

### ValidValuesCheck

**Purpose:** Validates field values are in a list of allowed values

**Use Cases:**
- Status codes
- Categories or types
- Country codes
- State abbreviations
- Enumerated values
- Lookup validation

**Parameters:**
- `field` (string): Field name to validate
- `valid_values` (list): List of acceptable values
- `case_sensitive` (boolean, optional): Case-sensitive comparison (default: true)

**Example:**
```yaml
# Order status must be one of these
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "status"
    valid_values:
      - "PENDING"
      - "PROCESSING"
      - "SHIPPED"
      - "DELIVERED"
      - "CANCELLED"

# Country codes (case-insensitive)
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "country"
    valid_values: ["US", "UK", "CA", "AU", "DE", "FR"]
    case_sensitive: false

# Payment methods
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "payment_method"
    valid_values:
      - "CREDIT_CARD"
      - "DEBIT_CARD"
      - "PAYPAL"
      - "BANK_TRANSFER"
      - "CASH"
```

**Passes when:** Field value is in the valid_values list

**Fails when:** Field value is not in the list

---

### RangeCheck

**Purpose:** Validates numeric values fall within acceptable range

**Use Cases:**
- Age validation
- Price ranges
- Quantity limits
- Percentage values (0-100)
- Financial limits
- Sensor readings

**Parameters:**
- `field` (string): Field name to validate
- `min_value` (number, optional): Minimum acceptable value (inclusive)
- `max_value` (number, optional): Maximum acceptable value (inclusive)

**Example:**
```yaml
# Age must be 18-120
- type: "RangeCheck"
  severity: "WARNING"
  params:
    field: "age"
    min_value: 18
    max_value: 120

# Price must be positive
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "price"
    min_value: 0

# Percentage (0-100)
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "discount_percentage"
    min_value: 0
    max_value: 100

# Transaction amount limits
- type: "RangeCheck"
  severity: "WARNING"
  params:
    field: "transaction_amount"
    min_value: 0
    max_value: 50000

# Temperature sensor range
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "temperature_celsius"
    min_value: -40
    max_value: 85
```

**Passes when:** Value is within min and max (inclusive)

**Fails when:** Value is below min or above max

**Conditional Example:**
```yaml
# Different ranges for different account types
- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "credit_limit"
    min_value: 50000
    max_value: 1000000
  condition: "account_tier == 'PREMIUM'"
```

---

### DateFormatCheck

**Purpose:** Validates date fields match expected format

**Use Cases:**
- Standardize date formats
- Validate date parsing
- Ensure date field quality
- Prepare for database loading

**Parameters:**
- `field` (string): Field name to validate
- `format` (string): Expected date format (strftime format)
- `allow_null` (boolean, optional): Allow null values (default: true)

**Example:**
```yaml
# ISO date format (YYYY-MM-DD)
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "birth_date"
    format: "%Y-%m-%d"
    allow_null: false

# US date format (MM/DD/YYYY)
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "transaction_date"
    format: "%m/%d/%Y"

# Date with time (YYYY-MM-DD HH:MM:SS)
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "created_timestamp"
    format: "%Y-%m-%d %H:%M:%S"

# European format (DD/MM/YYYY)
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "order_date"
    format: "%d/%m/%Y"

# Year-Month only (YYYY-MM)
- type: "DateFormatCheck"
  severity: "WARNING"
  params:
    field: "month_year"
    format: "%Y-%m"
```

**Common Format Codes:**
- `%Y` - 4-digit year (2024)
- `%y` - 2-digit year (24)
- `%m` - Month (01-12)
- `%d` - Day (01-31)
- `%H` - Hour 24h (00-23)
- `%I` - Hour 12h (01-12)
- `%M` - Minute (00-59)
- `%S` - Second (00-59)
- `%p` - AM/PM

**Passes when:** Date string matches format exactly

**Fails when:** Date string doesn't match format or is unparseable

---

## Record-Level Validations

These validations check properties across multiple rows.

### DuplicateRowCheck

**Purpose:** Detects duplicate records based on key fields

**Use Cases:**
- Prevent duplicate customer records
- Detect duplicate transactions
- Ensure unique identifiers
- Data deduplication
- Primary key validation

**Parameters:**
- `key_fields` (list): Fields that together should be unique

**Example:**
```yaml
# Customer ID must be unique
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields: ["customer_id"]

# Combination must be unique
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields: ["customer_id", "order_date", "product_id"]

# Email must be unique
- type: "DuplicateRowCheck"
  severity: "WARNING"
  params:
    key_fields: ["email"]

# Composite key
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields: ["account_number", "transaction_id"]
```

**Passes when:** All combinations of key fields are unique

**Fails when:** Duplicate key combinations found

---

### BlankRecordCheck

**Purpose:** Detects completely empty rows (all fields null/empty)

**Use Cases:**
- Find Excel phantom rows
- Detect CSV formatting issues
- Clean up empty records
- Data quality monitoring

**Parameters:** None

**Example:**
```yaml
- type: "BlankRecordCheck"
  severity: "WARNING"
```

**Passes when:** No completely blank rows found

**Fails when:** One or more rows have all fields null/empty

---

### UniqueKeyCheck

**Purpose:** Validates specified fields contain unique values

**Use Cases:**
- Primary key validation
- Unique constraint enforcement
- Email uniqueness
- Account number uniqueness

**Parameters:**
- `fields` (list): Fields that should be unique (each checked individually)

**Example:**
```yaml
# Each of these must be unique across all rows
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    fields:
      - "customer_id"
      - "email"
      - "account_number"

# Single field uniqueness
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    fields: ["transaction_id"]
```

**Passes when:** Each specified field has all unique values

**Fails when:** Any field has duplicate values

**Note:** This checks each field independently. Use DuplicateRowCheck for composite keys.

---

## Conditional Validations

These validations apply rules based on data conditions.

### ConditionalValidation

**Purpose:** Execute different validations based on conditional logic (if-then-else)

**Use Cases:**
- Different rules for different account types
- Extra validation for high-value transactions
- Region-specific requirements
- Tier-based validation
- Complex business rules

**Parameters:**
- `condition` (string): Boolean expression to evaluate
- `then_validate` (list): Validations to run if condition is True
- `else_validate` (list, optional): Validations to run if condition is False

**Example:**
```yaml
# Different required fields for business vs individual
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "customer_type == 'INDIVIDUAL'"
    then_validate:
      # Individual customers
      - type: "MandatoryFieldCheck"
        params:
          fields: ["first_name", "last_name", "date_of_birth"]
      - type: "RangeCheck"
        params:
          field: "age"
          min_value: 18
    else_validate:
      # Business customers
      - type: "MandatoryFieldCheck"
        params:
          fields: ["company_name", "tax_id", "registration_number"]

# High-value orders need approval
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "order_total > 10000"
    then_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["manager_approval", "approval_timestamp"]
      - type: "RegexCheck"
        params:
          field: "manager_approval"
          pattern: "^MGR[0-9]{6}$"

# Payment method determines required fields
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "payment_method == 'CREDIT_CARD'"
    then_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["card_number", "expiry_date", "cvv"]
      - type: "RegexCheck"
        params:
          field: "card_number"
          pattern: "^[0-9]{16}$"
    else_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["bank_account", "routing_number"]
```

**Condition Syntax:**
- Comparison: `==`, `!=`, `>`, `>=`, `<`, `<=`
- Logical: `AND`, `OR`, `NOT`
- Examples:
  - `"status == 'ACTIVE'"`
  - `"age >= 18 AND age <= 65"`
  - `"balance < 0 OR credit_score < 500"`

**Passes when:** All executed sub-validations pass

**Fails when:** Any executed sub-validation fails

**Note:** Can also use inline conditions on any validation:
```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["company_name"]
  condition: "account_type == 'BUSINESS'"
```

---

## Advanced Validations

These validations provide statistical and complex checks.

### StatisticalOutlierCheck

**Purpose:** Detects statistical outliers using Z-score or IQR methods

**Use Cases:**
- Fraud detection
- Anomaly detection
- Sensor error detection
- Data quality monitoring
- Price anomaly detection

**Parameters:**
- `field` (string): Numeric field to analyze
- `method` (string): Detection method - "zscore" or "iqr" (default: "zscore")
- `threshold` (number, optional):
  - For zscore: number of standard deviations (default: 3.0)
  - For iqr: multiplier for interquartile range (default: 1.5)

**Example:**
```yaml
# Z-score method (3 standard deviations)
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "transaction_amount"
    method: "zscore"
    threshold: 3.0

# IQR method (interquartile range)
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "price"
    method: "iqr"
    threshold: 1.5

# Strict outlier detection
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "order_value"
    method: "zscore"
    threshold: 2.5  # More sensitive
```

**Passes when:** No statistical outliers detected

**Fails when:** Values significantly deviate from statistical norms

---

### CrossFieldComparisonCheck

**Purpose:** Validates logical relationships between two fields

**Use Cases:**
- Start date before end date
- Discount less than price
- Min value less than max value
- Salary within pay grade range

**Parameters:**
- `field1` (string): First field name
- `field2` (string): Second field name
- `operator` (string): Comparison operator - "<", "<=", ">", ">=", "==", "!="

**Example:**
```yaml
# Start date must be before end date
- type: "CrossFieldComparisonCheck"
  severity: "ERROR"
  params:
    field1: "start_date"
    field2: "end_date"
    operator: "<="

# Discount must be less than price
- type: "CrossFieldComparisonCheck"
  severity: "ERROR"
  params:
    field1: "discount_amount"
    field2: "product_price"
    operator: "<="

# Min salary must be less than max salary
- type: "CrossFieldComparisonCheck"
  severity: "ERROR"
  params:
    field1: "min_salary"
    field2: "max_salary"
    operator: "<"

# Actual value matches expected value
- type: "CrossFieldComparisonCheck"
  severity: "WARNING"
  params:
    field1: "actual_count"
    field2: "expected_count"
    operator: "=="
```

**Passes when:** All row pairs satisfy the comparison

**Fails when:** Any row violates the comparison

---

### FreshnessCheck

**Purpose:** Validates data is fresh (recently updated)

**Use Cases:**
- Ensure timely data loads
- Detect stale data
- Monitor pipeline health
- SLA compliance

**Parameters:**
- `check_type` (string): "file_modified" or "field_value"
- `field` (string, optional): Date field to check (if check_type is "field_value")
- `max_age_hours` (number): Maximum acceptable age in hours

**Example:**
```yaml
# File must be modified within last 24 hours
- type: "FreshnessCheck"
  severity: "ERROR"
  params:
    check_type: "file_modified"
    max_age_hours: 24

# Transaction dates must be within last 48 hours
- type: "FreshnessCheck"
  severity: "WARNING"
  params:
    check_type: "field_value"
    field: "transaction_timestamp"
    max_age_hours: 48

# Data must be updated daily
- type: "FreshnessCheck"
  severity: "ERROR"
  params:
    check_type: "file_modified"
    max_age_hours: 24
```

**Passes when:** Data is within max_age_hours

**Fails when:** Data exceeds max_age_hours

---

### CompletenessCheck

**Purpose:** Validates field completeness (percentage of non-null values)

**Use Cases:**
- Data quality metrics
- Optional field monitoring
- Data completeness SLAs
- Profile data quality

**Parameters:**
- `field` (string): Field to check
- `min_completeness` (number): Minimum acceptable percentage (0-100)

**Example:**
```yaml
# Phone number should be at least 80% complete
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "phone_number"
    min_completeness: 80.0

# Optional field monitoring
- type: "CompletenessCheck"
  severity: "WARNING"
  params:
    field: "middle_name"
    min_completeness: 50.0

# Critical field must be 95% complete
- type: "CompletenessCheck"
  severity: "ERROR"
  params:
    field: "customer_email"
    min_completeness: 95.0
```

**Passes when:** Percentage of non-null values >= min_completeness

**Fails when:** Completeness below threshold

---

### StringLengthCheck

**Purpose:** Validates string field length is within range

**Use Cases:**
- Database field length validation
- Truncation prevention
- Data quality checks
- Form validation

**Parameters:**
- `field` (string): Field name to check
- `min_length` (integer, optional): Minimum string length
- `max_length` (integer, optional): Maximum string length

**Example:**
```yaml
# Password must be 8-32 characters
- type: "StringLengthCheck"
  severity: "ERROR"
  params:
    field: "password"
    min_length: 8
    max_length: 32

# Description max length (database constraint)
- type: "StringLengthCheck"
  severity: "ERROR"
  params:
    field: "description"
    max_length: 255

# Name must have at least 2 characters
- type: "StringLengthCheck"
  severity: "ERROR"
  params:
    field: "last_name"
    min_length: 2
```

**Passes when:** String length within range

**Fails when:** String too short or too long

---

### NumericPrecisionCheck

**Purpose:** Validates numeric field decimal precision

**Use Cases:**
- Financial data validation
- Database precision constraints
- Rounding validation
- Scientific data quality

**Parameters:**
- `field` (string): Numeric field name
- `max_decimal_places` (integer): Maximum allowed decimal places

**Example:**
```yaml
# Price should have max 2 decimal places
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "price"
    max_decimal_places: 2

# Percentage to 1 decimal place
- type: "NumericPrecisionCheck"
  severity: "WARNING"
  params:
    field: "interest_rate"
    max_decimal_places: 1

# Scientific measurements (4 decimal places)
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "measurement"
    max_decimal_places: 4
```

**Passes when:** All values have <= max_decimal_places

**Fails when:** Any value exceeds precision

---

## Cross-File Validations

Validate relationships and consistency across multiple data files.

### ReferentialIntegrityCheck

**Purpose:** Validates foreign key relationships between two files

**Use Cases:**
- Ensure customer IDs in orders file exist in customers file
- Validate product references across related files
- Check referential integrity before data loads

**Parameters:**
- `foreign_key` (string, required): Column name in current file
- `reference_file` (string, required): Path to reference file
- `reference_key` (string, required): Column name in reference file
- `allow_null` (boolean, optional): Allow NULL values in foreign key. Default: false
- `reference_file_format` (string, optional): Format of reference file. Default: csv

**Examples:**
```yaml
# Validate customer_id exists in customers file
- type: "ReferentialIntegrityCheck"
  severity: "ERROR"
  params:
    foreign_key: "customer_id"
    reference_file: "customers.csv"
    reference_key: "id"
    allow_null: false
```

---

### CrossFileComparisonCheck

**Purpose:** Compares aggregate values between two files

**Use Cases:**
- Validate total order amount matches sum in order_items file
- Compare row counts between related files
- Ensure aggregates match across systems

**Parameters:**
- `aggregation` (string, required): Type: sum, count, mean, min, max
- `column` (string, optional): Column to aggregate (not needed for count)
- `comparison` (string, required): Operator: ==, !=, >, <, >=, <=
- `reference_file` (string, required): Path to reference file
- `reference_aggregation` (string, required): Aggregation in reference file
- `reference_column` (string, optional): Column in reference file
- `reference_file_format` (string, optional): Format. Default: csv
- `tolerance` (float, optional): Absolute tolerance. Default: 0
- `tolerance_pct` (float, optional): Percentage tolerance. Default: 0

**Examples:**
```yaml
# Check total amounts match within 0.01%
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

---

### CrossFileDuplicateCheck

**Purpose:** Detects duplicate records across multiple files

**Use Cases:**
- Check for duplicate customer IDs across archive files
- Ensure no overlap between active and historical data
- Validate data migration completeness

**Parameters:**
- `columns` (list, required): Columns to check for duplicates
- `reference_files` (list, required): List of files to check against
- `reference_file_format` (string, optional): Format. Default: csv

**Examples:**
```yaml
# Check for duplicate customer IDs across files
- type: "CrossFileDuplicateCheck"
  severity: "ERROR"
  params:
    columns: ["customer_id"]
    reference_files:
      - "customers_archive.csv"
      - "customers_old.csv"
```

---

## Database Validations

Validate data directly from databases using SQL.

### SQLCustomCheck

**Purpose:** Execute custom SQL-based validation logic

**Use Cases:**
- Complex business rules requiring SQL
- Validate data in databases without extracting to files
- Leverage database-specific functions

**Parameters:**
- `connection_string` (string, required): Database connection string
- `sql_query` (string, required): SQL query that returns failing records
- `db_type` (string, optional): Database type. Default: postgresql
- `max_sample_size` (int, optional): Max sample failures. Default: 10

**Supported Databases:**
- PostgreSQL
- MySQL
- SQL Server
- Oracle
- SQLite

**Examples:**
```yaml
# Find customers with invalid emails
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

---

### DatabaseReferentialIntegrityCheck

**Purpose:** Validates foreign key relationships within a database

**Use Cases:**
- Check referential integrity without loading data
- Validate database constraints are satisfied
- Find orphaned records

**Parameters:**
- `connection_string` (string, required): Database connection string
- `foreign_key_table` (string, required): Table with foreign key
- `foreign_key_column` (string, required): Foreign key column
- `reference_table` (string, required): Reference table
- `reference_key_column` (string, required): Primary key column
- `allow_null` (boolean, optional): Allow NULL values. Default: false
- `db_type` (string, optional): Database type. Default: postgresql

**Examples:**
```yaml
# Validate order.customer_id references customers.id
- type: "DatabaseReferentialIntegrityCheck"
  severity: "ERROR"
  params:
    connection_string: "postgresql://user:pass@localhost/db"
    foreign_key_table: "orders"
    foreign_key_column: "customer_id"
    reference_table: "customers"
    reference_key_column: "id"
```

---

### DatabaseConstraintCheck

**Purpose:** Validates database constraints are not violated

**Use Cases:**
- Check for constraint violations
- Validate UNIQUE constraints
- Find data violating CHECK constraints

**Parameters:**
- `connection_string` (string, required): Database connection string
- `table` (string, required): Table to check
- `constraint_query` (string, required): SQL query finding violations
- `constraint_name` (string, optional): Constraint name for reporting
- `db_type` (string, optional): Database type. Default: postgresql

**Examples:**
```yaml
# Check for age constraint violations
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

---

## Temporal/Historical Validations

Compare current data against historical baselines.

### BaselineComparisonCheck

**Purpose:** Compares current metrics against historical average

**Use Cases:**
- Detect unusual drops/spikes in row counts
- Validate sales totals against historical norms
- Alert on data volume anomalies

**Parameters:**
- `metric` (string, required): Metric to compare: count, sum, mean, min, max
- `column` (string, optional): Column to aggregate (not needed for count)
- `baseline_file` (string, required): Path to historical baseline file (CSV with date and value columns)
- `baseline_date_column` (string, optional): Date column. Default: "date"
- `baseline_value_column` (string, optional): Value column. Default: "value"
- `lookback_days` (int, optional): Days to average for baseline. Default: 30
- `tolerance_pct` (float, required): Allowed deviation percentage

**Examples:**
```yaml
# Validate row count within 20% of 30-day average
- type: "BaselineComparisonCheck"
  severity: "WARNING"
  params:
    metric: "count"
    baseline_file: "historical_counts.csv"
    lookback_days: 30
    tolerance_pct: 20
```

---

### TrendDetectionCheck

**Purpose:** Detects unusual trends compared to historical patterns

**Use Cases:**
- Alert on unexpected growth/decline rates
- Detect data quality degradation over time
- Monitor business metric changes

**Parameters:**
- `metric` (string, required): Metric to track: count, sum, mean
- `column` (string, optional): Column to aggregate
- `baseline_file` (string, required): Historical data file
- `baseline_date_column` (string, optional): Date column. Default: "date"
- `baseline_value_column` (string, optional): Value column. Default: "value"
- `max_growth_pct` (float, optional): Maximum allowed growth percentage
- `max_decline_pct` (float, optional): Maximum allowed decline percentage
- `comparison_period` (int, optional): Days to compare against. Default: 1

**Examples:**
```yaml
# Detect unusual growth in customer count
- type: "TrendDetectionCheck"
  severity: "WARNING"
  params:
    metric: "count"
    baseline_file: "daily_customer_counts.csv"
    max_growth_pct: 50
    max_decline_pct: 30
    comparison_period: 1
```

---

## Statistical Validations

Advanced statistical tests for data quality.

### DistributionCheck

**Purpose:** Validates data follows an expected statistical distribution

**Use Cases:**
- Detect when data patterns change unexpectedly
- Validate randomness assumptions
- Monitor data generation processes

**Parameters:**
- `column` (string, required): Column to check
- `expected_distribution` (string, required): Type: normal, uniform, exponential
- `significance_level` (float, optional): Significance level. Default: 0.05
- `min_sample_size` (int, optional): Minimum samples required. Default: 30

**Examples:**
```yaml
# Validate age follows normal distribution
- type: "DistributionCheck"
  severity: "WARNING"
  params:
    column: "age"
    expected_distribution: "normal"
    significance_level: 0.05
```

---

### CorrelationCheck

**Purpose:** Validates correlation between columns is within expected range

**Use Cases:**
- Detect when expected relationships break down
- Monitor feature engineering pipelines
- Validate business assumptions

**Parameters:**
- `column1` (string, required): First column
- `column2` (string, required): Second column
- `expected_correlation` (float, optional): Expected correlation (-1 to 1)
- `min_correlation` (float, optional): Minimum acceptable correlation
- `max_correlation` (float, optional): Maximum acceptable correlation
- `correlation_type` (string, optional): Type: pearson, spearman, kendall. Default: pearson
- `tolerance` (float, optional): Tolerance for expected_correlation. Default: 0.1

**Examples:**
```yaml
# Validate price and quantity have negative correlation
- type: "CorrelationCheck"
  severity: "WARNING"
  params:
    column1: "price"
    column2: "quantity_sold"
    min_correlation: -0.8
    max_correlation: -0.3
```

---

### AdvancedAnomalyDetectionCheck

**Purpose:** Detects anomalies using multiple statistical methods

**Use Cases:**
- Detect outliers more accurately
- Find fraudulent transactions
- Identify data entry errors

**Parameters:**
- `column` (string, required): Column to check for anomalies
- `method` (string, optional): Method: iqr, zscore, modified_zscore, isolation_forest. Default: iqr
- `threshold` (float, optional): Threshold for method. Default varies by method
- `max_anomaly_pct` (float, optional): Maximum acceptable percentage of anomalies. Default: 5

**Methods:**
- `iqr`: Interquartile Range method
- `zscore`: Standard Z-score method
- `modified_zscore`: Robust to outliers
- `isolation_forest`: Machine learning-based (requires scikit-learn)

**Examples:**
```yaml
# Detect price anomalies using IQR
- type: "AdvancedAnomalyDetectionCheck"
  severity: "WARNING"
  params:
    column: "price"
    method: "iqr"
    max_anomaly_pct: 2

# Detect anomalies using Isolation Forest
- type: "AdvancedAnomalyDetectionCheck"
  severity: "WARNING"
  params:
    column: "transaction_amount"
    method: "isolation_forest"
    threshold: 0.1
    max_anomaly_pct: 5
```

---

## Quick Reference Table

| Category | Validation | Purpose | Common Parameters |
|----------|-----------|---------|-------------------|
| **File** | EmptyFileCheck | File not empty | check_data_rows |
| **File** | RowCountRangeCheck | Row count in range | min_rows, max_rows |
| **File** | FileSizeCheck | File size in range | min_size_mb, max_size_mb |
| **Schema** | SchemaMatchCheck | Exact schema match | expected_columns, allow_extra |
| **Schema** | ColumnPresenceCheck | Required columns exist | columns |
| **Field** | MandatoryFieldCheck | Fields not null/empty | fields |
| **Field** | RegexCheck | Value matches pattern | field, pattern |
| **Field** | ValidValuesCheck | Value in allowed list | field, valid_values |
| **Field** | RangeCheck | Numeric value in range | field, min_value, max_value |
| **Field** | DateFormatCheck | Date matches format | field, format |
| **Record** | DuplicateRowCheck | No duplicate records | key_fields |
| **Record** | BlankRecordCheck | No empty rows | None |
| **Record** | UniqueKeyCheck | Fields have unique values | fields |
| **Conditional** | ConditionalValidation | If-then-else logic | condition, then_validate |
| **Advanced** | StatisticalOutlierCheck | Detect outliers | field, method, threshold |
| **Advanced** | CrossFieldComparisonCheck | Compare two fields | field1, field2, operator |
| **Advanced** | FreshnessCheck | Data recency check | check_type, max_age_hours |
| **Advanced** | CompletenessCheck | Field completeness % | field, min_completeness |
| **Advanced** | StringLengthCheck | String length range | field, min_length, max_length |
| **Advanced** | NumericPrecisionCheck | Decimal places limit | field, max_decimal_places |
| **Cross-File** | ReferentialIntegrityCheck | Foreign key validation | foreign_key, reference_file |
| **Cross-File** | CrossFileComparisonCheck | Compare aggregates | aggregation, reference_file |
| **Cross-File** | CrossFileDuplicateCheck | Duplicates across files | columns, reference_files |
| **Database** | SQLCustomCheck | Custom SQL validation | connection_string, sql_query |
| **Database** | DatabaseReferentialIntegrityCheck | DB foreign keys | foreign_key_table, reference_table |
| **Database** | DatabaseConstraintCheck | DB constraints | table, constraint_query |
| **Temporal** | BaselineComparisonCheck | Compare to historical | baseline_file, tolerance_pct |
| **Temporal** | TrendDetectionCheck | Detect unusual trends | baseline_file, max_growth_pct |
| **Statistical** | DistributionCheck | Distribution match | column, expected_distribution |
| **Statistical** | CorrelationCheck | Correlation range | column1, column2, min_correlation |
| **Statistical** | AdvancedAnomalyDetectionCheck | Advanced anomaly detection | column, method, max_anomaly_pct |

---

## Usage Tips

### Choosing Validations

1. **Start with basics:**
   - EmptyFileCheck
   - MandatoryFieldCheck for key fields
   - Basic format checks (RegexCheck, DateFormatCheck)

2. **Add business rules:**
   - RangeCheck for numeric constraints
   - ValidValuesCheck for enumerations
   - DuplicateRowCheck for uniqueness

3. **Advanced checks last:**
   - Statistical outlier detection
   - Completeness metrics
   - Freshness monitoring

### Performance Considerations

**Fast validations** (run on all data):
- EmptyFileCheck
- RowCountRangeCheck
- MandatoryFieldCheck
- ColumnPresenceCheck

**Medium speed** (process per-row):
- RegexCheck
- RangeCheck
- ValidValuesCheck
- DateFormatCheck

**Slower validations** (multiple passes or heavy compute):
- StatisticalOutlierCheck
- DuplicateRowCheck (for large datasets)
- CrossFieldComparisonCheck

### Combining Validations

```yaml
validations:
  # Fast checks first - DataK9 sniffs these out quickly! 🐕
  - type: "EmptyFileCheck"
    severity: "ERROR"

  - type: "ColumnPresenceCheck"
    severity: "ERROR"
    params:
      columns: ["id", "email"]

  # Per-field checks
  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["id", "email", "name"]

  - type: "RegexCheck"
    severity: "ERROR"
    params:
      field: "email"
      pattern: "..."

  # Business rules
  - type: "RangeCheck"
    severity: "WARNING"
    params:
      field: "age"
      min_value: 18

  # Advanced checks (optional)
  - type: "StatisticalOutlierCheck"
    severity: "WARNING"
    params:
      field: "order_amount"
```

---

## Next Steps

- **[Configuration Guide](configuration-guide.md)** - Complete YAML reference
- **[Best Practices](best-practices.md)** - ERROR vs WARNING, production tips
- **[Examples](../examples/)** - Real-world validation scenarios
- **[DataK9 Studio Guide](studio-guide.md)** - Visual configuration builder

---

**🐕 Guard your data with DataK9 - your K9 vigilant for data quality!**
