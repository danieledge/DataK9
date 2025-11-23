# DataK9 Configuration Guide

**Complete guide to configuring DataK9 validations**

This guide is for business analysts, data engineers, and anyone who needs to validate data quality without writing code. Let DataK9 guard your data! 🐕

---

## Table of Contents

1. [Introduction](#introduction)
2. [Core Concepts](#core-concepts)
3. [Configuration File Structure](#configuration-file-structure)
4. [Supported File Formats](#supported-file-formats)
5. [Writing Validation Rules](#writing-validation-rules)
6. [Severity Levels](#severity-levels)
7. [Conditional Validations](#conditional-validations)
8. [Running Validations](#running-validations)
9. [Understanding Reports](#understanding-reports)
10. [Best Practices](#best-practices)
11. [Performance Tips](#performance-tips)
12. [Troubleshooting](#troubleshooting)

---

## Introduction

DataK9 helps you ensure data quality by checking your data against rules you define. Everything is configured in YAML - no programming required! Like a vigilant K9 unit, DataK9 sniffs out data quality problems before they escalate. 🐕

**Key Features:**
- ✅ **No coding required** - Define validations in simple YAML configuration
- 📊 **Multiple file formats** - CSV, Excel, JSON, Parquet
- 🔄 **Handles large files** - Processes data in chunks for memory efficiency (200GB+)
- 📝 **Professional reports** - HTML and JSON output formats
- ⚡ **Fast execution** - Optimized for performance
- 🎯 **Conditional logic** - Apply rules based on data values
- 🔌 **Extensible** - Add custom validations when needed

---

## Core Concepts

### Validation Job

A **validation job** is a complete validation run. It includes:
- Job metadata (name, description)
- Settings (chunk size, max failures to report)
- One or more files to validate
- Validation rules for each file

### Validation Rules

**Validation rules** define what to check in your data. Each rule has:
- **Type**: What kind of check (e.g., MandatoryFieldCheck, RangeCheck)
- **Severity**: ERROR or WARNING
- **Parameters**: Configuration specific to that check
- **Condition** (optional): When to apply the rule

### Severity Levels

- **ERROR**: Critical issues that must be fixed. Causes validation to fail.
- **WARNING**: Issues that should be reviewed but don't fail validation.

### Results

Validation produces:
- **Pass/Fail status** for each rule and overall
- **Counts** of errors and warnings
- **Sample failures** showing specific rows and issues
- **Reports** in HTML or JSON format

---

## Configuration File Structure

All validations are defined in a YAML file:

```yaml
# Job information
validation_job:
  name: "My Validation Job"
  description: "Optional description of what this validates"

# Global settings
settings:
  chunk_size: 50000             # Rows to process at once (default: 50,000)
  max_sample_failures: 100      # Max failures to report per validation

# Files to validate
files:
  - name: "customers"           # Friendly name
    path: "data/customers.csv"  # File location
    format: "csv"               # File type

    # Validations for this file
    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

# Output configuration (supports date/time patterns)
output:
  html_report: "reports/{date}/validation_{job_name}_{time}.html"
  json_summary: "results/{job_name}_{timestamp}.json"
  fail_on_error: true
  fail_on_warning: false
```

### YAML Basics

If you're new to YAML:
- **Indentation matters** - Use 2 spaces (not tabs)
- **Lists start with `-`**
- **Key-value pairs**: `key: value`
- **Nested structures** use indentation
- **Strings** can be quoted or unquoted
- **Comments** start with `#`

---

## Supported File Formats

### CSV Files

```yaml
- name: "sales_data"
  path: "data/sales.csv"
  format: "csv"
  delimiter: ","         # Optional: default is comma
  encoding: "utf-8"      # Optional: default is utf-8
  header: 0              # Optional: which row is header (0-indexed)
```

**Use cases:**
- Database exports
- Simple data feeds
- Text-based data files

**Tips:**
- Specify `delimiter: "|"` for pipe-delimited files
- Specify `delimiter: "\t"` for tab-delimited files
- Use `encoding: "latin-1"` for legacy systems

### Excel Files

```yaml
- name: "sales_report"
  path: "reports/sales.xlsx"
  format: "excel"
  sheet_name: "Q1 Sales"    # Optional: sheet name or index
```

**Use cases:**
- Business user reports
- Multi-sheet workbooks
- Formatted data files

**Tips:**
- Use `sheet_name: 0` for first sheet (0-indexed)
- Leave blank to use first sheet
- Supports both .xlsx and .xls formats

### JSON Files

```yaml
- name: "api_response"
  path: "data/response.json"
  format: "json"
  flatten: true          # Optional: flatten nested structures
  lines: false           # Optional: true for JSON Lines format
```

**Supported JSON formats:**

**1. Standard JSON array:**
```json
[
  {"id": 1, "name": "John"},
  {"id": 2, "name": "Jane"}
]
```

**2. JSON Lines (JSONL):**
```json
{"id": 1, "name": "John"}
{"id": 2, "name": "Jane"}
```

**Use cases:**
- API responses
- NoSQL database exports
- Log files
- Modern data interchange

**Tips:**
- Set `flatten: true` to flatten nested JSON (e.g., `user.address.city` becomes `user_address_city`)
- Set `lines: true` for JSON Lines format (one JSON object per line)
- Large JSON files are automatically chunked

### Parquet Files

```yaml
- name: "analytics_data"
  path: "warehouse/analytics.parquet"
  format: "parquet"
```

**Use cases:**
- Data warehouse exports
- Large analytical datasets (100GB+)
- Columnar data storage
- Big data applications

**Tips:**
- Parquet is **10x faster** than CSV for large files (GB+)
- Preserves data types precisely
- Best for analytics and data science workflows

---

## Writing Validation Rules

### Rule Structure

Every validation rule has this structure:

```yaml
- type: "ValidationTypeName"
  severity: "ERROR"  # or "WARNING"
  params:
    # Parameters specific to this validation type
  condition: "optional condition"  # Optional: when to apply this rule
  description: "Optional description"  # Optional: for documentation
  enabled: true  # Optional: set to false to temporarily disable
```

### Common Validation Types

See the **[Validation Catalog](validation-catalog.md)** for complete list with examples.

#### 1. Check Required Fields

```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["customer_id", "email", "first_name"]
```

#### 2. Validate Field Format (Regex)

```yaml
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

#### 3. Check Numeric Ranges

```yaml
- type: "RangeCheck"
  severity: "WARNING"
  params:
    field: "age"
    min_value: 0
    max_value: 120
```

#### 4. Validate Against List of Values

```yaml
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "country"
    valid_values: ["US", "UK", "CA", "AU"]
    case_sensitive: false
```

#### 5. Check Date Format

```yaml
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "birth_date"
    format: "%Y-%m-%d"
    allow_null: false
```

#### 6. Find Duplicate Records

```yaml
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields: ["customer_id", "email"]
```

#### 7. Check Row Count

```yaml
- type: "RowCountRangeCheck"
  severity: "WARNING"
  params:
    min_rows: 100
    max_rows: 1000000
```

---

## Severity Levels

### ERROR

Use **ERROR** for critical issues that must be fixed:
- Missing required fields (customer ID, email, etc.)
- Invalid data formats (malformed emails, dates)
- Business rule violations (negative balances, invalid states)
- Duplicate keys
- Data integrity issues

```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"  # Must be fixed
  params:
    fields: ["customer_id"]
```

**Impact:** If any ERROR validations fail, DataK9 reports overall validation status as FAILED.

### WARNING

Use **WARNING** for issues that should be reviewed but aren't critical:
- Data quality concerns (outliers, unusual values)
- Missing optional fields
- Data completeness issues
- Formatting inconsistencies

```yaml
- type: "RangeCheck"
  severity: "WARNING"  # Should review
  params:
    field: "age"
    min_value: 18
    max_value: 120
```

**Impact:** WARNING failures don't cause overall validation to fail, but are reported for review.

### Choosing Severity

Ask yourself:
- **Can the data be processed without fixing this?**
  - No → ERROR
  - Yes → WARNING

- **Does this violate a business rule or requirement?**
  - Yes → ERROR
  - No → WARNING

- **Would stakeholders refuse this data?**
  - Yes → ERROR
  - No → WARNING

**See:** [Best Practices Guide](best-practices.md) for comprehensive severity guidance.

---

## Conditional Validations

Apply validation rules based on data values. Perfect for:
- Different rules for different account types
- Extra checks for high-value transactions
- Region-specific or tier-specific validations

### Method 1: Inline Conditions (Simple)

Add a `condition` parameter to any validation:

```yaml
# Only check company_name for business accounts
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["company_name", "tax_id"]
  condition: "account_type == 'BUSINESS'"
```

**More examples:**

```yaml
# Check manager approval only for large orders
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["manager_approval"]
  condition: "order_total > 10000"

# Validate SSN only for US customers
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "ssn"
    pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"
  condition: "country == 'US'"

# Multiple conditions with AND
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["rush_fee"]
  condition: "shipping_method == 'EXPRESS' AND order_total > 100"
```

### Method 2: If-Then-Else (Complex)

For complex scenarios with different validations:

```yaml
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "customer_type == 'INDIVIDUAL'"

    # IF customer is INDIVIDUAL
    then_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["first_name", "last_name", "date_of_birth"]
      - type: "RangeCheck"
        params:
          field: "age"
          min_value: 18

    # ELSE (customer is BUSINESS)
    else_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["company_name", "tax_id", "registration_number"]
```

**More examples:**

```yaml
# Payment method determines required fields
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "payment_method == 'CREDIT_CARD'"
    then_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["card_number", "expiry_date", "cvv"]
    else_validate:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["bank_account", "routing_number"]
```

### Condition Syntax

**Comparison operators:**
- `==` equal to
- `!=` not equal to
- `>` greater than
- `>=` greater than or equal
- `<` less than
- `<=` less than or equal

**Logical operators:**
- `AND` - both conditions must be true
- `OR` - either condition must be true
- `NOT` - negates a condition

**Examples:**
```yaml
"status == 'ACTIVE'"
"age >= 18"
"balance < 0"
"country == 'US' AND state == 'CA'"
"status == 'PENDING' OR status == 'PROCESSING'"
"NOT (status == 'CANCELLED')"
```

**Important:**
- Use single quotes for text values: `'BUSINESS'` not `BUSINESS`
- Field names must exist in your data
- Conditions are case-sensitive
- Use AND/OR (not &&, ||)

---

## Output Configuration

### Date/Time Patterns in Output Paths

Output filenames support automatic date/time pattern substitution to prevent overwrites and improve audit trails:

```yaml
validation_job:
  name: "Customer Data Validation"

output:
  # Use patterns for automatic timestamping
  html_report: "reports/{date}/validation_{job_name}_{time}.html"
  json_summary: "results/{job_name}_{timestamp}.json"
```

**Result:**
```
reports/2025-11-22/validation_Customer_Data_Validation_14-30-45.html
results/Customer_Data_Validation_20251122_143045.json
```

**Available Patterns:**

| Pattern | Example | Use Case |
|---------|---------|----------|
| `{date}` | `2025-11-22` | Daily reports, date-based organization |
| `{time}` | `14-30-45` | Multiple runs per day |
| `{timestamp}` | `20251122_143045` | Unique per run (recommended) |
| `{datetime}` | `2025-11-22_14-30-45` | Combined date and time |
| `{job_name}` | `Customer_Data_Validation` | Organization by job |

**Benefits:**
- ✅ No file overwrites (each run gets unique filename)
- ✅ Better audit trails (timestamp shows when validation ran)
- ✅ Organized outputs (automatic directory structures like `reports/{date}/`)
- ✅ Backward compatible (paths without patterns work unchanged)

**Basic Output Configuration:**

Without patterns (simple, may overwrite):
```yaml
output:
  html_report: "validation_report.html"
  json_summary: "validation_summary.json"
  fail_on_error: true
  fail_on_warning: false
```

With patterns (recommended for production):
```yaml
output:
  html_report: "reports/{date}/{job_name}_{time}.html"
  json_summary: "results/{job_name}_{timestamp}.json"
  fail_on_error: true
  fail_on_warning: false
```

**See:** [CLI Guide](../../CLI_GUIDE.md#datetimepatterns) for using patterns in CLI arguments.

---

## Running Validations

### Basic Command

```bash
python3 -m validation_framework.cli validate my_config.yaml
```

### Generate HTML Report

```bash
# Simple filename
python3 -m validation_framework.cli validate my_config.yaml --html report.html

# With date/time patterns (prevents overwrites)
python3 -m validation_framework.cli validate my_config.yaml \
  --html "reports/{date}/validation_{time}.html"
```

### Generate JSON Report

```bash
# Simple filename
python3 -m validation_framework.cli validate my_config.yaml --json results.json

# With timestamp pattern (recommended)
python3 -m validation_framework.cli validate my_config.yaml \
  --json "results/validation_{timestamp}.json"
```

### Generate Both Reports

```bash
# Without patterns
python3 -m validation_framework.cli validate my_config.yaml \
  --html report.html \
  --json results.json

# With patterns (recommended for production)
python3 -m validation_framework.cli validate my_config.yaml \
  --html "reports/{date}/{job_name}_{time}.html" \
  --json "results/{job_name}_{timestamp}.json"
```

### Verbose Output

```bash
python3 -m validation_framework.cli validate my_config.yaml --verbose
```

### List Available Validations

```bash
python3 -m validation_framework.cli list-validations
```

### Exit Codes

DataK9 returns different exit codes for automation:
- `0` - Validation passed (no errors)
- `1` - Validation failed (errors found)
- `2` - Command error (bad config, file not found, etc.)

**Use in scripts:**
```bash
python3 -m validation_framework.cli validate config.yaml
if [ $? -eq 0 ]; then
    echo "✅ DataK9 approved! Data quality is good."
else
    echo "🐕 DataK9 detected issues! Check the report."
fi
```

---

## Understanding Reports

### Console Output

Real-time feedback as validation runs:

```
================================================================================
DataK9 Data Quality Framework
================================================================================
Job: Customer Data Validation
Files to validate: 1

[1/1] Processing: customers
  Path: customers.csv
  Format: csv
  Validations: 5

  Executing validations:
    - EmptyFileCheck... ✓ PASS
    - MandatoryFieldCheck... ✗ FAIL
    - RegexCheck... ✓ PASS
    - RangeCheck... ✗ FAIL
    - RangeCheck... ✗ FAIL

  Status: FAILED
  Errors: 3
  Warnings: 0
  Duration: 0.12s

================================================================================
Validation Summary
================================================================================
Overall Status: FAILED
Total Errors: 3
Total Warnings: 0
Total Duration: 0.12s
```

### HTML Report

Professional, shareable report with:

**Summary Section:**
- Overall status (PASSED/FAILED)
- Job name and description
- Total errors and warnings
- Execution time

**File Details:**
- File name and path
- File metadata (rows, size, columns)
- Pass/fail status per file

**Validation Results:**
- Each validation rule with pass/fail
- Error and warning counts
- Detailed failure information
- Sample failing rows with specific issues

**Benefits:**
- ✅ Easy to share with stakeholders
- ✅ Professional formatting
- ✅ Dark theme, interactive UI
- ✅ Can be emailed or archived

### JSON Report

Machine-readable format for:
- CI/CD pipeline integration
- Automated processing
- Data quality dashboards
- Alerting systems
- Database storage

**Structure:**
```json
{
  "job_name": "Customer Data Validation",
  "execution_time": "2024-11-13T10:30:00",
  "duration_seconds": 0.12,
  "overall_status": "FAILED",
  "total_errors": 3,
  "total_warnings": 0,
  "file_reports": [
    {
      "file_name": "customers",
      "status": "FAILED",
      "validations": [...]
    }
  ]
}
```

**See:** [Reading Reports Guide](reading-reports.md) for detailed report analysis.

---

## Best Practices

### 1. Start Simple, Add Gradually

Begin with basic checks, add more as needed:

```yaml
# Start with basics
validations:
  - type: "EmptyFileCheck"
    severity: "ERROR"

  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["id", "email"]

# Then add format checks
  - type: "RegexCheck"
    severity: "ERROR"
    params:
      field: "email"
      pattern: "..."

# Finally add business rules
  - type: "RangeCheck"
    severity: "WARNING"
    params:
      field: "age"
      min_value: 18
```

### 2. Use Descriptive Names

```yaml
validation_job:
  name: "Customer Master Data - Daily Validation"  # Good
  # not: "validation1"  # Bad

files:
  - name: "customers"  # Good
    # not: "file1"  # Bad
```

### 3. Organize Validations Logically

Group related validations together:

```yaml
validations:
  # File-level checks first
  - type: "EmptyFileCheck"
    severity: "ERROR"
  - type: "RowCountRangeCheck"
    severity: "WARNING"
    params:
      min_rows: 100

  # Required fields
  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["id", "email", "name"]

  # Format validations
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
```

### 4. Use Comments for Documentation

```yaml
validations:
  # Per PCI compliance requirements - must validate card format
  - type: "RegexCheck"
    severity: "ERROR"
    params:
      field: "card_number"
      pattern: "^[0-9]{16}$"

  # Business rule: accounts cannot have negative balance
  # Exception: overdraft accounts handled separately
  - type: "RangeCheck"
    severity: "ERROR"
    params:
      field: "balance"
      min_value: 0
    condition: "account_type != 'OVERDRAFT'"
```

### 5. Choose Appropriate Severity

- Use ERROR for must-fix issues
- Use WARNING for should-review issues

```yaml
# ERROR: Critical - blocks processing
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["customer_id"]  # Can't process without ID

# WARNING: Important but not blocking
- type: "MandatoryFieldCheck"
  severity: "WARNING"
  params:
    fields: ["phone"]  # Nice to have but not required
```

### 6. Test With Sample Data First

Before running on production data:
1. Create a small sample file (10-100 rows)
2. Include both valid and invalid data
3. Test your validations
4. Verify failures are caught correctly
5. Then run on full dataset

### 7. Version Control Your Configs

```bash
git add validations/customer_validation.yaml
git commit -m "Add email format validation"
git push
```

Benefits:
- Track changes over time
- Collaborate with team
- Rollback if needed
- Document why rules changed

### 8. Use Conditional Logic Wisely

```yaml
# Good: Clear conditional logic
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["company_name"]
  condition: "account_type == 'BUSINESS'"

# Avoid: Overly complex conditions
# If too complex, consider splitting into multiple validations
```

### 9. Monitor Performance

For large files:
- Adjust `chunk_size` (default: 50,000)
- Reduce `max_sample_failures` if too many failures
- Consider running during off-hours

```yaml
settings:
  chunk_size: 100000  # Process more rows at once
  max_sample_failures: 50  # Limit samples for faster reporting
```

### 10. Document Your Validations

Create a README alongside your config:

```markdown
# Customer Data Validation

## Purpose
Validates customer master data before loading to CRM system.

## Schedule
Runs daily at 2 AM via AutoSys job.

## Validation Rules
- Must have customer_id, email, name
- Email must be valid format
- Age must be 18-120
- Balance must be non-negative

## Contact
Data Quality Team - dq@company.com
```

---

## Performance Tips

### For Large Files (1GB+)

**1. Increase chunk size:**
```yaml
settings:
  chunk_size: 100000  # Process 100,000 rows at once
```

**2. Use Parquet format when possible:**
- **10x faster than CSV**
- More memory efficient
- Better for large analytical datasets

**3. Limit sample failures:**
```yaml
settings:
  max_sample_failures: 50  # Don't collect more than 50 samples
```

**4. Run validations in parallel:**
If validating multiple files, run separate processes:

```bash
# Terminal 1
python3 -m validation_framework.cli validate file1_config.yaml &

# Terminal 2
python3 -m validation_framework.cli validate file2_config.yaml &
```

### For Many Validations

- Order validations by speed (fast checks first)
- Use file-level checks before row-level checks
- Disable expensive validations during development

```yaml
validations:
  # Fast: File-level checks (run once)
  - type: "EmptyFileCheck"
    severity: "ERROR"

  # Fast: Check required fields (simple null check)
  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["id"]

  # Slower: Regex checks (per-row processing)
  - type: "RegexCheck"
    severity: "ERROR"
    params:
      field: "email"
      pattern: "..."

  # Slowest: Statistical checks (require multiple passes)
  - type: "StatisticalOutlierCheck"
    severity: "WARNING"
    params:
      field: "amount"
    enabled: false  # Disable during development
```

**See:** [Performance Tuning Guide](performance-tuning.md) for advanced optimization.

---

## Troubleshooting

### Common Errors

#### "FileNotFoundError"

**Problem:** File path is incorrect

**Solution:**
```yaml
# Use absolute path
- name: "customers"
  path: "/full/path/to/data/customers.csv"

# Or relative path from where you run the command
- name: "customers"
  path: "data/customers.csv"
```

#### "ValidationError: Field not found"

**Problem:** Field name in validation doesn't match actual column name

**Solution:**
- Check column names are exact (case-sensitive)
- Check for extra spaces in column names
- Look at actual data file to verify names

```yaml
# If your CSV has column: "Email Address"
- type: "MandatoryFieldCheck"
  params:
    fields: ["Email Address"]  # Must match exactly
```

#### "YAML parsing error"

**Problem:** Invalid YAML syntax

**Solution:**
- Check indentation (use 2 spaces, not tabs)
- Ensure colons have space after: `key: value` not `key:value`
- Check quotes are balanced
- Use a YAML validator online

#### "No validations executed"

**Problem:** Validations are empty or disabled

**Solution:**
```yaml
# Check enabled flag
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  enabled: true  # Make sure this isn't false
  params:
    fields: ["id"]

# Check validations array isn't empty
validations:  # Don't leave this empty
  - type: "EmptyFileCheck"
    severity: "ERROR"
```

#### "Memory Error" with large files

**Problem:** File too large to process in memory

**Solution:**
```yaml
# Reduce chunk size
settings:
  chunk_size: 10000  # Smaller chunks

# Or use Parquet format
- name: "big_data"
  path: "data.parquet"  # More memory efficient
  format: "parquet"
```

### Getting Help

1. **Check this guide** - Search for your issue
2. **Review examples** - See [Examples](../examples/)
3. **Check validation catalog** - See [Validation Catalog](validation-catalog.md)
4. **Run with --verbose** - Get detailed output
5. **Check troubleshooting** - See [Troubleshooting Guide](troubleshooting.md)
6. **Review FAQ** - See [FAQ](faq.md)

---

## Next Steps

- **[Validation Catalog](validation-catalog.md)** - Complete reference of all 35+ validation types
- **[Best Practices](best-practices.md)** - ERROR vs WARNING, production tips
- **[Examples](../examples/)** - Real-world scenarios and recipes
- **[DataK9 Studio Guide](studio-guide.md)** - Visual configuration builder

---

**🐕 Let DataK9 guard your data quality!**
