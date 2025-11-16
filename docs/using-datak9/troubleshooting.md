# DataK9 Troubleshooting Guide

Solutions to common issues when using **DataK9** - your K9 guardian for data quality.

---

## 📋 Quick Diagnosis

**Symptom → Solution**

| Issue | Go To |
|-------|-------|
| File not found error | [File Path Issues](#file-path-issues) |
| Field/column not found | [Field Name Issues](#field-name-issues) |
| YAML parsing error | [YAML Syntax Errors](#yaml-syntax-errors) |
| Validation running slowly | [Performance Issues](#performance-issues) |
| Out of memory error | [Memory Issues](#memory-issues) |
| HTML report won't open | [Large Report Issues](#large-report-issues) |
| Regex not matching | [Regex Issues](#regex-issues) |
| Import/module errors | [Installation Issues](#installation-issues) |

---

## 🔧 Common Errors

### File Path Issues

#### Error: "FileNotFoundError: [Errno 2] No such file or directory"

**Cause:** The file path in your YAML config is incorrect.

**Solutions:**

1. **Use absolute paths:**
   ```yaml
   path: "/full/path/to/customers.csv"
   ```

2. **Check current working directory:**
   ```bash
   pwd  # See where you are
   ls data/  # Verify file exists
   ```

3. **Use relative paths correctly:**
   ```yaml
   # If running from project root
   path: "data/customers.csv"

   # If running from different directory
   path: "../data/customers.csv"
   ```

4. **Check file permissions:**
   ```bash
   ls -l data/customers.csv  # Verify read permissions
   ```

**Prevention:** Always use absolute paths in production configs.

---

### Field Name Issues

#### Error: "ValidationError: Field 'email_address' not found in data"

**Cause:** Column name in validation doesn't match actual column name in data.

**Solutions:**

1. **Check exact column names** (case-sensitive!):
   ```bash
   # View actual column names
   head -1 customers.csv
   ```

2. **Common mismatches:**
   ```yaml
   # Wrong
   field: "email_address"

   # Correct (matches actual column)
   field: "email"
   ```

3. **Check for extra spaces:**
   ```csv
   # CSV might have: "email "  (note trailing space)
   # Your YAML has: "email"
   ```

4. **Use ColumnPresenceCheck first:**
   ```yaml
   # This will catch missing columns early
   - type: "ColumnPresenceCheck"
     severity: "ERROR"
     params:
       required_columns: ["customer_id", "email", "name"]
   ```

**Prevention:** Use DataK9 Profiler to see exact column names:
```bash
python3 -m validation_framework.cli profile customers.csv
```

---

### YAML Syntax Errors

#### Error: "yaml.scanner.ScannerError: mapping values are not allowed here"

**Cause:** YAML syntax error (usually indentation or special characters).

**Common YAML Mistakes:**

1. **Wrong indentation (use 2 spaces, not tabs):**
   ```yaml
   # Wrong (tabs or 4 spaces)
   validations:
       - type: "EmptyFileCheck"

   # Correct (2 spaces)
   validations:
     - type: "EmptyFileCheck"
   ```

2. **Missing colon:**
   ```yaml
   # Wrong
   severity "ERROR"

   # Correct
   severity: "ERROR"
   ```

3. **Unquoted strings with special characters:**
   ```yaml
   # Wrong (colon in unquoted string)
   description: Email should be: user@domain.com

   # Correct
   description: "Email should be: user@domain.com"
   ```

4. **Wrong list syntax:**
   ```yaml
   # Wrong
   fields: customer_id, email, name

   # Correct
   fields: ["customer_id", "email", "name"]
   # Or
   fields:
     - "customer_id"
     - "email"
     - "name"
   ```

**Solutions:**

1. **Validate YAML online:** Use [yamllint.com](http://www.yamllint.com/)
2. **Use DataK9 Studio:** Generates valid YAML automatically
3. **Check indentation:** Use spaces, not tabs
4. **Quote strings:** When in doubt, use quotes

**Prevention:** Use DataK9 Studio to avoid syntax errors entirely.

---

## ⚡ Performance Issues

### Validation Running Very Slowly

**Symptom:** Validation takes much longer than expected.

**Diagnosis:**

Run with `--verbose` to see which validations are slow:
```bash
python3 -m validation_framework.cli validate config.yaml --verbose
```

**Solutions:**

1. **Use Parquet format (10x faster than CSV):**
   ```bash
   # Convert CSV to Parquet first
   import pandas as pd
   df = pd.read_csv('data.csv')
   df.to_parquet('data.parquet')
   ```

2. **Increase chunk_size:**
   ```yaml
   settings:
     chunk_size: 100000  # Default is 50000
   ```

3. **Reduce max_sample_failures:**
   ```yaml
   settings:
     max_sample_failures: 50  # Default is 100
   ```

4. **Order validations efficiently:**
   ```yaml
   # Put fast checks first (fail fast)
   validations:
     - type: "EmptyFileCheck"  # Very fast
     - type: "ColumnPresenceCheck"  # Fast
     - type: "MandatoryFieldCheck"  # Fast
     - type: "StatisticalOutlierCheck"  # Slower (multiple passes)
   ```

5. **Use conditions to skip unnecessary validations:**
   ```yaml
   - type: "MandatoryFieldCheck"
     params:
       fields: ["company_name"]
     condition: "account_type == 'BUSINESS'"  # Only check when needed
   ```

**See Also:** [Performance Tuning Guide](performance-tuning.md)

---

### Memory Issues

#### Error: "MemoryError: Unable to allocate array"

**Cause:** Chunk size too large for available RAM.

**Solutions:**

1. **Reduce chunk_size:**
   ```yaml
   settings:
     chunk_size: 10000  # Reduce from default 50000
   ```

2. **Use Parquet format:**
   - More memory-efficient than CSV
   - Built-in compression

3. **Reduce max_sample_failures:**
   ```yaml
   settings:
     max_sample_failures: 10  # Don't store too many samples
   ```

4. **Process in batches:**
   - Split large file into smaller files
   - Validate each separately

**Prevention:** For 200GB+ files, use Parquet with chunk_size=50000.

**See Also:** [Handling Large Files](large-files.md)

---

## 📊 Report Issues

### Large Report Issues

#### Problem: HTML report too large to open in browser

**Symptom:** Browser crashes or freezes when opening report.

**Cause:** Too many sample failures stored in report.

**Solutions:**

1. **Reduce max_sample_failures:**
   ```yaml
   settings:
     max_sample_failures: 10  # Default is 100
   ```

2. **Use JSON output instead:**
   ```bash
   python3 -m validation_framework.cli validate config.yaml --json results.json
   ```

3. **Fix data issues and re-run:**
   - Large reports usually mean lots of failures
   - Fix the data quality issues
   - Re-run for cleaner report

4. **Split validations:**
   - Run critical validations first
   - Run detailed validations separately

**See Also:** [Handling Large Files: Reports](large-files.md#large-reports)

---

### Report Not Generated

#### Problem: No HTML report created

**Cause:** Missing output configuration or command-line flag.

**Solutions:**

1. **Add output to YAML:**
   ```yaml
   output:
     html_report: "validation_report.html"
   ```

2. **Or use command-line flag:**
   ```bash
   python3 -m validation_framework.cli validate config.yaml --html report.html
   ```

---

## 🔍 Validation Issues

### Regex Issues

#### Problem: RegexCheck not matching expected values

**Common Regex Mistakes:**

1. **Forgetting to escape backslashes:**
   ```yaml
   # Wrong
   pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"  # Will fail

   # Correct (escape backslashes in YAML)
   pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"
   ```

2. **Not quoting the pattern:**
   ```yaml
   # Wrong
   pattern: ^test@.*\.com$

   # Correct
   pattern: "^test@.*\\.com$"
   ```

3. **Pattern too restrictive:**
   - Test your regex with online tools first
   - Use [regex101.com](https://regex101.com/)

**Email Validation Example:**
```yaml
# Comprehensive email pattern
- type: "RegexCheck"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

**Phone Number Examples:**
```yaml
# US phone: (555) 123-4567
pattern: "^\\([0-9]{3}\\) [0-9]{3}-[0-9]{4}$"

# UK phone: +44 20 1234 5678
pattern: "^\\+44 [0-9]{2} [0-9]{4} [0-9]{4}$"

# International: +1-555-123-4567
pattern: "^\\+[0-9]{1,3}-[0-9]{3}-[0-9]{3}-[0-9]{4}$"
```

---

### Date Validation Issues

#### Problem: DateFormatCheck failing on valid dates

**Solutions:**

1. **Check format string matches data:**
   ```yaml
   # Data format: 2024-01-15
   format: "%Y-%m-%d"

   # Data format: 01/15/2024
   format: "%m/%d/%Y"

   # Data format: 15-Jan-2024
   format: "%d-%b-%Y"
   ```

2. **Common format codes:**
   ```
   %Y = 4-digit year (2024)
   %y = 2-digit year (24)
   %m = Month (01-12)
   %d = Day (01-31)
   %H = Hour (00-23)
   %M = Minute (00-59)
   %S = Second (00-59)
   ```

3. **Handle mixed formats:**
   - Use multiple DateFormatCheck validations
   - Or clean data first to single format

---

### Conditional Validation Not Working

#### Problem: Condition not being evaluated correctly

**Solutions:**

1. **Check condition syntax:**
   ```yaml
   # Correct SQL-like syntax
   condition: "account_type == 'BUSINESS'"
   condition: "age >= 18"
   condition: "status IN ('ACTIVE', 'PENDING')"
   condition: "balance > 0 AND verified == True"
   ```

2. **Quote string values:**
   ```yaml
   # Wrong
   condition: "status == ACTIVE"

   # Correct
   condition: "status == 'ACTIVE'"
   ```

3. **Check field names:**
   ```yaml
   # Field names must match exactly (case-sensitive)
   condition: "AccountType == 'BUSINESS'"  # Wrong if column is "account_type"
   ```

---

## 🔧 Installation Issues

### ModuleNotFoundError

#### Error: "ModuleNotFoundError: No module named 'validation_framework'"

**Cause:** DataK9 not installed or not in Python path.

**Solutions:**

1. **Verify installation:**
   ```bash
   pip list | grep validation
   ```

2. **Re-install:**
   ```bash
   cd data-validation-tool
   pip install -e .
   ```

3. **Check Python version:**
   ```bash
   python3 --version  # Must be 3.8+
   ```

4. **Use correct Python:**
   ```bash
   # If multiple Python versions
   python3 -m validation_framework.cli validate config.yaml
   ```

---

### Import Errors for Optional Dependencies

#### Error: "ModuleNotFoundError: No module named 'pyarrow'"

**Cause:** Optional dependency not installed.

**Solutions:**

**For Parquet support:**
```bash
pip install pyarrow
```

**For Excel support:**
```bash
pip install openpyxl
```

**For PostgreSQL:**
```bash
pip install psycopg2-binary
```

**For MySQL:**
```bash
pip install pymysql
```

---

## 🎯 Debugging Tips

### Enable Verbose Logging

See detailed execution information:

```bash
python3 -m validation_framework.cli validate config.yaml --verbose
```

### Check Configuration Parsing

Verify your YAML is parsed correctly:

```bash
python3 -c "import yaml; print(yaml.safe_load(open('config.yaml')))"
```

### Test with Minimal Config

Create a minimal test config to isolate issues:

```yaml
validation_job:
  name: "Test"

files:
  - name: "test"
    path: "test.csv"
    format: "csv"
    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"
```

### Profile Your Data First

Understand your data before validating:

```bash
python3 -m validation_framework.cli profile data.csv
```

This shows:
- Actual column names
- Data types
- Sample values
- Statistics

---

## 🆘 Still Stuck?

If you're still experiencing issues:

1. **Check the [FAQ](faq.md)** - Common questions answered
2. **Review [Examples](../examples/)** - See working configurations
3. **Search [GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)** - Someone may have had the same issue
4. **Open a new issue** - Provide:
   - DataK9 version
   - Error message (full traceback)
   - Minimal config that reproduces issue
   - Sample data (if possible)

---

## 📚 Related Guides

- **[FAQ](faq.md)** - Frequently asked questions
- **[Configuration Guide](configuration-guide.md)** - Complete YAML reference
- **[Performance Tuning](performance-tuning.md)** - Optimization tips
- **[Handling Large Files](large-files.md)** - 200GB+ file processing

---

**🐕 Let DataK9 guard your data quality!**
