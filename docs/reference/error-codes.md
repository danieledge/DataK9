# Error Codes Reference

**Complete Guide to Exit Codes and Error Messages**

This reference documents all exit codes, error messages, and troubleshooting guidance for DataK9.

---

## Table of Contents

1. [Overview](#overview)
2. [Exit Codes](#exit-codes)
3. [Configuration Errors](#configuration-errors)
4. [Runtime Errors](#runtime-errors)
5. [Validation Errors](#validation-errors)
6. [File I/O Errors](#file-io-errors)
7. [Database Errors](#database-errors)
8. [Memory Errors](#memory-errors)
9. [Troubleshooting Guide](#troubleshooting-guide)
10. [Error Handling in Scripts](#error-handling-in-scripts)

---

## Overview

### Error Categories

DataK9 uses structured error codes and messages for predictable integration:

| Category | Exit Code | Description |
|----------|-----------|-------------|
| **Success** | 0 | All validations passed (or only WARNINGs) |
| **Validation Failure** | 1 | ERROR-severity validation(s) failed |
| **Configuration Error** | 2 | Invalid configuration or runtime error |

### Error Message Format

All DataK9 errors follow this format:

```
ERROR: [Category] Message with details
  Location: file.yaml:line:column
  Suggestion: How to fix
```

**Example:**
```
ERROR: [Configuration] Invalid YAML syntax
  Location: config.yaml:15:3
  Suggestion: Check indentation - use 2 spaces, not tabs
```

---

## Exit Codes

### Exit Code 0: SUCCESS

**Status:** Validation passed

**Conditions:**
- All validations passed, OR
- Only WARNING-severity validations failed

**Output:**
```
✅ Validation PASSED
Files validated: 2
Checks passed: 15
Warnings: 3
```

**Bash Example:**
```bash
python3 -m validation_framework.cli validate config.yaml
if [ $? -eq 0 ]; then
    echo "Success - proceeding with data load"
    load_data.sh
fi
```

**Python Example:**
```python
import subprocess

result = subprocess.run(
    ["python3", "-m", "validation_framework.cli", "validate", "config.yaml"]
)

if result.returncode == 0:
    print("Validation passed")
    process_data()
```

---

### Exit Code 1: VALIDATION FAILURE

**Status:** Validation failed

**Conditions:**
- One or more ERROR-severity validations failed
- Data quality issues detected

**Output:**
```
❌ Validation FAILED
Files validated: 2
Checks failed: 3
Errors: 3
Warnings: 2

Failed Validations:
  - customers.csv: MandatoryFieldCheck - 15 rows missing email
  - customers.csv: UniqueKeyCheck - 5 duplicate customer_ids
  - orders.csv: ReferentialIntegrityCheck - 10 invalid customer_ids
```

**Bash Example:**
```bash
python3 -m validation_framework.cli validate config.yaml
if [ $? -eq 1 ]; then
    echo "Validation failed - halting pipeline"
    send_alert "Data quality issues detected"
    exit 1
fi
```

**Python Example:**
```python
result = subprocess.run(
    ["python3", "-m", "validation_framework.cli", "validate", "config.yaml"],
    capture_output=True,
    text=True
)

if result.returncode == 1:
    print("Validation failed")
    print(result.stdout)
    send_alert("Data quality issues detected")
    sys.exit(1)
```

---

### Exit Code 2: CONFIGURATION/RUNTIME ERROR

**Status:** Error in configuration or execution

**Conditions:**
- Configuration file not found
- Invalid YAML syntax
- Missing required parameters
- File read/write errors
- Python exceptions
- Invalid validation type

**Output:**
```
ERROR: [Configuration] Configuration file not found: config.yaml
  Suggestion: Check file path and ensure file exists
```

**Bash Example:**
```bash
python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 2 ]; then
    echo "Configuration error - check DataK9 setup"
    send_alert "DataK9 configuration needs attention"
    exit 2
fi
```

**Python Example:**
```python
result = subprocess.run(
    ["python3", "-m", "validation_framework.cli", "validate", "config.yaml"],
    capture_output=True,
    text=True
)

if result.returncode == 2:
    print("Configuration error")
    print(result.stderr)
    send_alert("DataK9 configuration error")
    sys.exit(2)
```

---

## Configuration Errors

### CFG001: File Not Found

**Error Message:**
```
ERROR: [Configuration] Configuration file not found: config.yaml
```

**Causes:**
- File doesn't exist
- Incorrect path
- Permission denied

**Solutions:**
```bash
# Check file exists
ls -l config.yaml

# Check current directory
pwd

# Use absolute path
python3 -m validation_framework.cli validate /full/path/to/config.yaml

# Check permissions
chmod 644 config.yaml
```

---

### CFG002: Invalid YAML Syntax

**Error Message:**
```
ERROR: [Configuration] Invalid YAML syntax at line 15
  Details: mapping values are not allowed here
  Location: config.yaml:15:3
```

**Causes:**
- Incorrect indentation
- Missing colons
- Mixed tabs and spaces
- Unclosed quotes

**Solutions:**
```bash
# Validate YAML syntax
yamllint config.yaml

# Use Python YAML parser
python3 -c "import yaml; yaml.safe_load(open('config.yaml'))"

# Check indentation
# Correct (2 spaces)
files:
  - path: "data.csv"

# Incorrect (mixed tabs/spaces)
files:
	- path: "data.csv"
```

---

### CFG003: Missing Required Field

**Error Message:**
```
ERROR: [Configuration] Missing required field: 'path' in file configuration
  Location: config.yaml:12
```

**Causes:**
- Required parameter omitted
- Typo in parameter name

**Solution:**
```yaml
# Incorrect
files:
  - name: "customers"
    # Missing 'path'

# Correct
files:
  - name: "customers"
    path: "data/customers.csv"
```

---

### CFG004: Invalid Parameter Value

**Error Message:**
```
ERROR: [Configuration] Invalid value for 'severity': 'CRITICAL'
  Valid values: ERROR, WARNING
  Location: config.yaml:25
```

**Causes:**
- Typo in value
- Invalid enum value
- Wrong data type

**Solution:**
```yaml
# Incorrect
severity: "CRITICAL"
severity: "error"  # Wrong case

# Correct
severity: "ERROR"
severity: "WARNING"
```

---

### CFG005: Unknown Validation Type

**Error Message:**
```
ERROR: [Configuration] Unknown validation type: 'EmailCheck'
  Did you mean: 'RegexCheck'?
  Location: config.yaml:30
```

**Causes:**
- Typo in validation name
- Validation doesn't exist

**Solution:**
```bash
# List available validations
python3 -m validation_framework.cli list-validations

# Use correct name
- type: "RegexCheck"  # Not "EmailCheck"
  params:
    field: "email"
    pattern: "..."
```

---

### CFG006: Invalid File Format

**Error Message:**
```
ERROR: [Configuration] Invalid file format: 'xlsx'
  Valid formats: csv, excel, json, parquet
```

**Cause:**
- Used file extension instead of format name

**Solution:**
```yaml
# Incorrect
format: "xlsx"

# Correct
format: "excel"  # For .xlsx files
```

---

### CFG007: Circular Reference

**Error Message:**
```
ERROR: [Configuration] Circular reference detected in cross-file validation
  File A references File B, which references File A
```

**Cause:**
- Cross-file validations form a loop

**Solution:**
```yaml
# Incorrect: Circular dependency
# File A:
- type: "ReferentialIntegrityCheck"
  params:
    reference_file: "fileB.csv"

# File B:
- type: "ReferentialIntegrityCheck"
  params:
    reference_file: "fileA.csv"

# Correct: One-way dependency
# Only File A references File B
```

---

## Runtime Errors

### RUN001: Data File Not Found

**Error Message:**
```
ERROR: [Runtime] Input file not found: data/customers.csv
  Location: config.yaml:15
```

**Solutions:**
```bash
# Check file exists
ls -l data/customers.csv

# Verify path in config
# Use absolute path
path: "/full/path/to/data/customers.csv"

# Or relative from config location
path: "data/customers.csv"
```

---

### RUN002: Permission Denied (Read)

**Error Message:**
```
ERROR: [Runtime] Permission denied reading file: data/customers.csv
```

**Solutions:**
```bash
# Check file permissions
ls -l data/customers.csv

# Grant read permission
chmod 644 data/customers.csv

# Check directory permissions
ls -ld data/

# Grant directory access
chmod 755 data/
```

---

### RUN003: Permission Denied (Write)

**Error Message:**
```
ERROR: [Runtime] Permission denied writing report: /var/reports/validation_report.html
```

**Solutions:**
```bash
# Check directory permissions
ls -ld /var/reports/

# Use writable directory
python3 -m validation_framework.cli validate config.yaml \
    --output-dir ~/reports/

# Or fix permissions
sudo chmod 755 /var/reports/
```

---

### RUN004: Disk Space Full

**Error Message:**
```
ERROR: [Runtime] No space left on device
  Writing to: /var/reports/validation_report.html
```

**Solutions:**
```bash
# Check disk space
df -h

# Clean up old reports
rm /var/reports/old_*.html

# Use different output directory
python3 -m validation_framework.cli validate config.yaml \
    --output-dir /tmp/reports/
```

---

### RUN005: Invalid File Format

**Error Message:**
```
ERROR: [Runtime] Failed to parse CSV file: data/customers.csv
  Details: Expected 5 columns, found 3 at line 42
```

**Solutions:**
```bash
# Inspect problematic line
head -n 50 data/customers.csv | tail -n 10

# Check for:
# - Unescaped delimiters
# - Unclosed quotes
# - Encoding issues

# Try different encoding
files:
  - path: "data/customers.csv"
    encoding: "latin-1"  # Instead of utf-8
```

---

### RUN006: Encoding Error

**Error Message:**
```
ERROR: [Runtime] Unicode decode error reading file
  Details: 'utf-8' codec can't decode byte 0xff
  Location: data/customers.csv:line 42
```

**Solutions:**
```yaml
# Try different encodings
files:
  - path: "data/customers.csv"
    encoding: "latin-1"   # Western Europe
    # or
    encoding: "cp1252"     # Windows
    # or
    encoding: "iso-8859-1" # ISO Latin-1
```

---

## Validation Errors

### VAL001: Regex Pattern Error

**Error Message:**
```
ERROR: [Validation] Invalid regex pattern: '[a-z+'
  Details: unterminated character set at position 4
  Validation: RegexCheck for field 'email'
```

**Solutions:**
```yaml
# Incorrect pattern
pattern: "[a-z+"  # Unclosed bracket

# Correct pattern
pattern: "[a-z]+"  # Closed bracket

# Escape special characters in YAML
pattern: "\\d{3}-\\d{3}-\\d{4}"  # Use double backslashes
```

---

### VAL002: Date Format Error

**Error Message:**
```
ERROR: [Validation] Invalid date format code: '%Z'
  Validation: DateFormatCheck for field 'created_date'
```

**Solutions:**
```yaml
# Check format codes
# Valid codes: %Y, %m, %d, %H, %M, %S
# See Python strftime documentation

# Correct example
- type: "DateFormatCheck"
  params:
    field: "created_date"
    format: "%Y-%m-%d %H:%M:%S"
```

---

### VAL003: Field Not Found

**Error Message:**
```
ERROR: [Validation] Field 'email' not found in data
  Available fields: customer_id, name, e_mail
  Validation: MandatoryFieldCheck
```

**Solutions:**
```yaml
# Check field name spelling
# Incorrect
fields: ["email"]

# Correct (based on actual column name)
fields: ["e_mail"]

# Or use ColumnPresenceCheck first to validate schema
- type: "ColumnPresenceCheck"
  severity: "ERROR"
  params:
    columns: ["customer_id", "email"]
```

---

### VAL004: Type Mismatch

**Error Message:**
```
ERROR: [Validation] Type mismatch for field 'age'
  Expected: numeric
  Found: string
  Validation: RangeCheck
```

**Solutions:**
```yaml
# Data has non-numeric values in 'age' field
# Options:
# 1. Fix source data
# 2. Use data profiler to understand actual types
python3 -m validation_framework.cli profile data.csv

# 3. Add type validation first
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "age"
    pattern: "^[0-9]+$"  # Must be numeric
```

---

## File I/O Errors

### IO001: File Locked

**Error Message:**
```
ERROR: [IO] File is locked by another process: data/customers.csv
```

**Solutions:**
```bash
# Check processes using file
lsof | grep customers.csv

# Wait for process to release file
# Or close other applications using the file

# Copy file to different location
cp data/customers.csv /tmp/customers.csv
# Validate the copy
```

---

### IO002: File Too Large

**Error Message:**
```
WARNING: File size (150 GB) exceeds recommended limit for CSV format
  Recommendation: Convert to Parquet format for 10x faster processing
```

**Solutions:**
```bash
# Convert CSV to Parquet
python3 -c "
import pandas as pd
df = pd.read_csv('data.csv')
df.to_parquet('data.parquet', compression='snappy')
"

# Update config
files:
  - path: "data.parquet"
    format: "parquet"
```

---

### IO003: Corrupted File

**Error Message:**
```
ERROR: [IO] File appears to be corrupted: data/customers.csv
  Details: Unexpected end of file
```

**Solutions:**
```bash
# Verify file integrity
md5sum data/customers.csv

# Re-download or re-extract file

# Check file size
ls -lh data/customers.csv

# Try opening in text editor to inspect
head -n 100 data/customers.csv
tail -n 100 data/customers.csv
```

---

## Database Errors

### DB001: Connection Failed

**Error Message:**
```
ERROR: [Database] Failed to connect to database
  Connection string: postgresql://user@localhost/db
  Details: could not connect to server: Connection refused
```

**Solutions:**
```bash
# Check database is running
pg_isready -h localhost

# Check connection string
# Format: postgresql://user:password@host:port/database
connection_string: "postgresql://user:pass@localhost:5432/mydb"

# Test connection
psql "postgresql://user:pass@localhost:5432/mydb"

# Check firewall
sudo ufw status
```

---

### DB002: Authentication Failed

**Error Message:**
```
ERROR: [Database] Authentication failed
  User: datavalidation
  Database: production
```

**Solutions:**
```bash
# Verify credentials
psql -U datavalidation -d production

# Check user permissions
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datavalidation;

# Update connection string with correct password
connection_string: "postgresql://user:correct_password@host/db"
```

---

### DB003: Table Not Found

**Error Message:**
```
ERROR: [Database] Table 'customers' does not exist
  Database: production
  Validation: DatabaseReferentialIntegrityCheck
```

**Solutions:**
```bash
# List available tables
psql -d production -c "\dt"

# Check table name spelling
# Correct case and schema
foreign_key_table: "public.customers"  # Not "Customers"
```

---

### DB004: SQL Syntax Error

**Error Message:**
```
ERROR: [Database] SQL syntax error
  Query: SELECT * FORM customers WHERE age > 18
  Details: syntax error at or near "FORM"
```

**Solutions:**
```yaml
# Fix SQL syntax
# Incorrect
sql_query: "SELECT * FORM customers WHERE age > 18"

# Correct
sql_query: "SELECT * FROM customers WHERE age > 18"
```

---

## Memory Errors

### MEM001: Out of Memory

**Error Message:**
```
ERROR: [Memory] Out of memory
  Current chunk size: 100000
  Memory usage: 3.8 GB
```

**Solutions:**
```yaml
# Reduce chunk size
settings:
  chunk_size: 10000  # Reduce from 100000

# Limit sample failures
settings:
  max_sample_failures: 10  # Reduce from 100

# Use Parquet format (more memory-efficient)
files:
  - path: "data.parquet"
    format: "parquet"
```

```bash
# Disable HTML report (saves memory)
python3 -m validation_framework.cli validate config.yaml --no-html
```

---

### MEM002: Memory Warning

**Error Message:**
```
WARNING: High memory usage detected: 2.5 GB
  Recommendation: Reduce chunk_size or use Parquet format
```

**Solutions:**
```yaml
# Already a warning, not an error
# But consider optimizing:

settings:
  chunk_size: 25000  # Reduce from 50000

# Or convert to Parquet
```

---

## Troubleshooting Guide

### Debug Mode

Enable detailed logging:

```bash
# Set debug log level
export DATAK9_LOG_LEVEL=DEBUG

# Run with verbose output
python3 -m validation_framework.cli validate config.yaml --verbose
```

### Validate Configuration

Before full run:

```bash
# Check config syntax only
python3 -m validation_framework.cli validate config.yaml --config-only

# Test with fail-fast
python3 -m validation_framework.cli validate config.yaml --fail-fast
```

### Inspect Data

Profile data first:

```bash
# Profile with sample
python3 -m validation_framework.cli profile data.csv --sample-size 10000

# Review profile report
open data_profile_report.html
```

### Check System Resources

```bash
# Check available memory
free -h

# Check disk space
df -h

# Check file permissions
ls -l data/

# Check Python version
python3 --version  # Need 3.7+

# Check dependencies
pip list | grep -i pandas
```

### Common Issues Checklist

- [ ] YAML syntax valid (no tabs, correct indentation)
- [ ] File paths correct (absolute or relative to config)
- [ ] File permissions allow read access
- [ ] Output directory writable
- [ ] Sufficient disk space
- [ ] Sufficient memory
- [ ] Field names match data (case-sensitive)
- [ ] Validation type names correct
- [ ] Parameter names correct
- [ ] Regex patterns properly escaped
- [ ] Database connection string correct

---

## Error Handling in Scripts

### Bash Script Example

```bash
#!/bin/bash

set -euo pipefail  # Exit on error, undefined vars, pipe failures

CONFIG="config.yaml"
LOG_DIR="/var/log/datak9"
ALERT_EMAIL="data-team@company.com"

# Run validation
python3 -m validation_framework.cli validate "$CONFIG" \
    --verbose \
    --output-dir "$LOG_DIR" \
    2>&1 | tee "$LOG_DIR/execution.log"

EXIT_CODE=$?

# Handle exit code
case $EXIT_CODE in
    0)
        echo "✅ Validation passed"
        # Continue with data processing
        /opt/scripts/load_data.sh
        ;;
    1)
        echo "❌ Validation failed - data quality issues"
        # Send alert
        mail -s "DataK9 Validation Failed" "$ALERT_EMAIL" < "$LOG_DIR/execution.log"
        # Don't process data
        exit 1
        ;;
    2)
        echo "⚠️ Configuration or runtime error"
        # Send alert
        mail -s "DataK9 Configuration Error" "$ALERT_EMAIL" < "$LOG_DIR/execution.log"
        # Don't process data
        exit 2
        ;;
    *)
        echo "❓ Unexpected exit code: $EXIT_CODE"
        exit 3
        ;;
esac
```

### Python Script Example

```python
#!/usr/bin/env python3

import subprocess
import sys
import json
import smtplib
from email.message import EmailMessage

def send_alert(subject, body):
    """Send email alert"""
    msg = EmailMessage()
    msg.set_content(body)
    msg['Subject'] = subject
    msg['From'] = 'datak9@company.com'
    msg['To'] = 'data-team@company.com'

    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)

def run_validation(config_path):
    """Run DataK9 validation and handle results"""

    # Run validation
    result = subprocess.run(
        [
            "python3", "-m", "validation_framework.cli",
            "validate", config_path,
            "--verbose"
        ],
        capture_output=True,
        text=True
    )

    # Log output
    print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    # Handle exit code
    if result.returncode == 0:
        print("✅ Validation passed")
        # Continue processing
        return True

    elif result.returncode == 1:
        print("❌ Validation failed")

        # Parse JSON report for details
        try:
            with open('validation_summary.json') as f:
                report = json.load(f)

            error_count = report.get('total_errors', 0)
            warning_count = report.get('total_warnings', 0)

            # Send alert
            send_alert(
                "DataK9 Validation Failed",
                f"Validation failed with {error_count} errors and {warning_count} warnings\n\n"
                f"{result.stdout}"
            )
        except Exception as e:
            print(f"Failed to parse report: {e}")

        return False

    elif result.returncode == 2:
        print("⚠️ Configuration error")

        # Send alert
        send_alert(
            "DataK9 Configuration Error",
            f"Configuration or runtime error\n\n{result.stderr}"
        )

        return False

    else:
        print(f"❓ Unexpected exit code: {result.returncode}")
        return False

if __name__ == "__main__":
    config = "config.yaml"

    if run_validation(config):
        # Validation passed - continue processing
        print("Proceeding with data load...")
        # call load_data() or similar
    else:
        # Validation failed - halt
        print("Halting pipeline due to validation failure")
        sys.exit(1)
```

---

## Next Steps

**You've mastered error handling! Now:**

1. **[CLI Reference](cli-reference.md)** - Command-line usage
2. **[Troubleshooting](../using-datak9/troubleshooting.md)** - Common issues
3. **[Best Practices](../using-datak9/best-practices.md)** - Production patterns
4. **[CI/CD Integration](../using-datak9/cicd-integration.md)** - Pipeline integration

---

**🐕 DataK9 errors - Your K9 barks when something's wrong, helping you fix it fast**
