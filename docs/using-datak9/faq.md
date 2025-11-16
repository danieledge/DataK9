# DataK9 Frequently Asked Questions

Common questions about **DataK9** - your K9 guardian for data quality.

---

## General Questions

### Q: What is DataK9?

**A:** DataK9 is a production-grade data quality framework that validates data files before they enter your data warehouse or analytics platform. It catches data quality issues early, preventing bad data from reaching production.

**See also:** [What is DataK9?](what-is-datak9.md)

---

### Q: Do I need to know Python to use DataK9?

**A:** No! You can use DataK9 in three ways:
1. **DataK9 Studio** - Visual IDE with drag-and-drop (no coding!)
2. **YAML Configuration** - Simple configuration files (like writing a recipe)
3. **Python API** - For developers who want programmatic control

Most users work with YAML configs or DataK9 Studio and never write Python code.

**See also:** [DataK9 Studio Guide](studio-guide.md)

---

### Q: How much does DataK9 cost?

**A:** DataK9 is **completely free** and open source (MIT License). No hidden costs, no premium tiers, no vendor lock-in.

---

### Q: Is DataK9 production-ready?

**A:** Yes! DataK9 is used in production environments for:
- Validating 200GB+ files daily
- AutoSys batch job orchestration
- CI/CD pipeline integration
- Enterprise data warehouses (Snowflake, Redshift, BigQuery)

It has 115+ unit tests and has been battle-tested with massive datasets.

---

## Installation & Setup

### Q: What are the system requirements?

**A:** Minimum requirements:
- Python 3.8 or higher
- 2GB RAM (4GB+ recommended for large files)
- 500MB disk space

DataK9 works on Linux, macOS, and Windows.

**See also:** [Installation Guide](../getting-started/installation.md)

---

### Q: Can I run DataK9 in a Docker container?

**A:** Docker support is planned for a future release. Currently, you install DataK9 via pip in a Python environment or virtual environment.

---

### Q: Do I need a database to use DataK9?

**A:** No! DataK9 validates **files** (CSV, Excel, JSON, Parquet). You don't need a database.

However, DataK9 *can* validate data directly in databases if you want (PostgreSQL, MySQL, SQL Server, Oracle).

---

## File Formats

### Q: What file formats does DataK9 support?

**A:** DataK9 supports:
- **CSV** - Any delimiter, headers, quoted fields
- **Excel** - XLS, XLSX, multiple sheets
- **Parquet** - Columnar format (recommended for large files!)
- **JSON** - Standard arrays and JSON Lines (JSONL/NDJSON)
- **Databases** - PostgreSQL, MySQL, SQL Server, Oracle (via SQL queries)

**See also:** [Configuration Guide](configuration-guide.md#file-formats)

---

### Q: Can DataK9 handle compressed files (gzip, zip)?

**A:** Currently, you need to decompress files before validation. Support for compressed files is planned for a future release.

**Workaround:**
```bash
# Decompress first
gunzip data.csv.gz

# Then validate
python3 -m validation_framework.cli validate config.yaml
```

---

### Q: Which format is fastest for large files?

**A:** **Parquet** is 10x faster than CSV for large files!

| Format | 1GB File | 10GB File |
|--------|----------|-----------|
| CSV | ~2 minutes | ~20 minutes |
| Parquet | ~12 seconds | ~2 minutes |
| Excel | ~3 minutes | Not recommended |

**Recommendation:** Convert large CSV files to Parquet for best performance.

---

## Configuration

### Q: How do I choose between ERROR and WARNING severity?

**A:** Use this guideline:

**ERROR** - Use when:
- Data MUST be fixed before loading
- Issue will cause downstream system failures
- Business logic absolutely requires it
- Example: Missing primary key, invalid foreign key

**WARNING** - Use when:
- Issue should be reviewed but not block processing
- Data is suspicious but might be valid
- You want to flag for manual review
- Example: Unusually high values, missing optional fields

**See also:** [Best Practices: ERROR vs WARNING](best-practices.md#error-vs-warning)

---

### Q: Can I validate multiple files in one config?

**A:** Yes! Just add multiple file entries:

```yaml
files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"
    validations: [...]

  - name: "orders"
    path: "data/orders.csv"
    format: "csv"
    validations: [...]

  - name: "products"
    path: "data/products.parquet"
    format: "parquet"
    validations: [...]
```

**See also:** [Configuration Guide](configuration-guide.md#multiple-files)

---

### Q: Can I use variables or parameters in my YAML config?

**A:** Not directly in the YAML, but you can:
1. Use environment variables via command-line substitution
2. Generate YAML configs programmatically with templates
3. Use DataK9's profiler to auto-generate configs

**See also:** [Data Profiling](data-profiling.md#auto-generate-configs)

---

## Performance

### Q: Can DataK9 handle very large files (200GB+)?

**A:** Yes! DataK9 uses **chunked processing**:
- Reads data in chunks (default: 50,000 rows)
- Only one chunk in memory at a time
- Uses ~400MB RAM regardless of file size

We've tested with 200GB+ files successfully.

**Tips for large files:**
1. Use Parquet format (10x faster)
2. Increase chunk_size if you have more RAM
3. Reduce max_sample_failures to save memory

**See also:** [Handling Large Files](large-files.md)

---

### Q: How can I make validation faster?

**A:** Performance optimization tips:

1. **Use Parquet format** - 10x faster than CSV
2. **Increase chunk_size** - Process more rows at once
   ```yaml
   settings:
     chunk_size: 100000  # Default is 50000
   ```
3. **Reduce sample failures** - Don't collect every failure
   ```yaml
   settings:
     max_sample_failures: 50  # Default is 100
   ```
4. **Order validations efficiently** - Put fast checks first
5. **Use conditions** - Skip validations when not needed

**See also:** [Performance Tuning](performance-tuning.md)

---

### Q: Why is my validation taking so long?

**A:** Common causes:

1. **Large CSV files** - Use Parquet instead
2. **Complex regex patterns** - Simplify patterns or use `ValidValuesCheck`
3. **Too many validations** - Reduce to only essential checks
4. **Small chunk_size** - Increase chunk_size setting
5. **Statistical validations** - These require multiple passes

**Solution:** Run with `--verbose` to see which validations are slow.

---

## Validation Rules

### Q: What validation types are available?

**A:** DataK9 has 35 built-in validation rules in 10 categories:

- **File-Level** (3): EmptyFileCheck, RowCountRangeCheck, FileSizeCheck
- **Schema** (2): SchemaMatchCheck, ColumnPresenceCheck
- **Field-Level** (5): MandatoryFieldCheck, RegexCheck, ValidValuesCheck, RangeCheck, DateFormatCheck
- **Record-Level** (3): DuplicateRowCheck, BlankRecordCheck, UniqueKeyCheck
- **Advanced** (9): StatisticalOutlierCheck, CrossFieldComparisonCheck, FreshnessCheck, CompletenessCheck, StringLengthCheck, NumericPrecisionCheck, InlineRegexCheck, InlineBusinessRuleCheck, InlineLookupCheck
- **Conditional** (1): ConditionalValidation
- **Cross-File** (4): ReferentialIntegrityCheck, CrossFileComparisonCheck, CrossFileDuplicateCheck, CrossFileKeyCheck
- **Database** (3): SQLCustomCheck, DatabaseReferentialIntegrityCheck, DatabaseConstraintCheck
- **Temporal** (2): BaselineComparisonCheck, TrendDetectionCheck
- **Statistical** (3): DistributionCheck, CorrelationCheck, AdvancedAnomalyDetectionCheck

**See also:** [Complete Validation Catalog](../reference/validation-reference.md)

---

### Q: Can I create custom validation rules?

**A:** Yes! DataK9 has a plugin architecture for custom validations.

**See also:** [Creating Custom Validations](../for-developers/custom-validations.md)

---

### Q: How do I validate email addresses?

**A:** Use RegexCheck:

```yaml
- type: "RegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

**See also:** [Validation Catalog: RegexCheck](../reference/validation-reference.md#regexcheck)

---

### Q: How do I check if values are in an allowed list?

**A:** Use ValidValuesCheck:

```yaml
- type: "ValidValuesCheck"
  severity: "ERROR"
  params:
    field: "country_code"
    allowed_values: ["US", "UK", "CA", "AU", "DE", "FR"]
```

**See also:** [Validation Catalog: ValidValuesCheck](../reference/validation-reference.md#validvaluescheck)

---

### Q: How do I validate dates?

**A:** Use DateFormatCheck or FreshnessCheck:

```yaml
# Check date format
- type: "DateFormatCheck"
  severity: "ERROR"
  params:
    field: "transaction_date"
    format: "%Y-%m-%d"

# Check date isn't too old
- type: "FreshnessCheck"
  severity: "WARNING"
  params:
    date_field: "transaction_date"
    max_age_days: 30
```

**See also:** [Validation Catalog](../reference/validation-reference.md)

---

## Reports

### Q: What output formats does DataK9 support?

**A:** DataK9 generates:
- **HTML reports** - Beautiful, interactive, dark-themed
- **JSON output** - Machine-readable for CI/CD
- **Console output** - Real-time validation progress

**See also:** [Reading Reports](reading-reports.md)

---

### Q: Can I customize the HTML report theme?

**A:** The HTML reports use a professional dark theme (Tokyo Night colors). Custom themes are not currently supported, but you can modify the HTML template in the source code.

---

### Q: My HTML report is too large to open. What do I do?

**A:** For very large reports:

1. **Reduce max_sample_failures:**
   ```yaml
   settings:
     max_sample_failures: 10  # Default is 100
   ```

2. **Use JSON output** for programmatic analysis
3. **Split validations** across multiple configs

**See also:** [Handling Large Reports](large-files.md#large-reports)

---

## Integration

### Q: Can I use DataK9 with AutoSys?

**A:** Yes! DataK9 is designed for AutoSys integration:

- Exit code 0 = All validations passed
- Exit code 1 = Validation errors found
- Exit code 2 = Configuration or runtime error

**See also:** [AutoSys Integration](autosys-integration.md)

---

### Q: How do I integrate DataK9 with CI/CD pipelines?

**A:** Use JSON output for machine parsing:

```bash
python3 -m validation_framework.cli validate config.yaml --json results.json

# Check exit code
if [ $? -ne 0 ]; then
  echo "Validation failed!"
  exit 1
fi
```

**See also:** [CI/CD Integration](cicd-integration.md)

---

### Q: Can DataK9 run in Airflow?

**A:** Yes! Use the BashOperator:

```python
from airflow.operators.bash import BashOperator

validate_data = BashOperator(
    task_id='validate_customer_data',
    bash_command='python3 -m validation_framework.cli validate /path/to/config.yaml',
    dag=dag
)
```

**See also:** [CI/CD Integration](cicd-integration.md#airflow)

---

## Troubleshooting

### Q: Why is my validation failing with "FileNotFoundError"?

**A:** Check that:
1. The file path in your YAML is correct
2. The file exists at that location
3. You have read permissions
4. Use absolute paths if needed: `/full/path/to/file.csv`

**See also:** [Troubleshooting Guide](troubleshooting.md)

---

### Q: Why is my validation failing with "Field 'X' not found"?

**A:** This means the column name in your validation doesn't match the data file.

**Solutions:**
1. Check column names are exact match (case-sensitive!)
2. Verify there are no extra spaces in column names
3. Use `ColumnPresenceCheck` first to verify columns exist

**See also:** [Troubleshooting Guide](troubleshooting.md#field-not-found)

---

### Q: Why am I getting "YAML parsing error"?

**A:** Common YAML errors:

1. **Incorrect indentation** - Use 2 spaces (not tabs!)
2. **Missing colons** - `field:` not `field`
3. **Unquoted strings** with special characters
4. **Mismatched brackets** - Check `[]` and `{}`

**Tip:** Use an online YAML validator or DataK9 Studio to avoid syntax errors.

**See also:** [Troubleshooting Guide](troubleshooting.md#yaml-errors)

---

### Q: Why is my RegexCheck not working?

**A:** Common regex issues:

1. **Escape backslashes** - Use `\\` not `\`
2. **YAML string escaping** - Quote your patterns
3. **Pattern too restrictive** - Test with regex tester first

**Example:**
```yaml
# Correct
pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"

# Wrong
pattern: ^[0-9]{3}-[0-9]{2}-[0-9]{4}$
```

**See also:** [Troubleshooting Guide](troubleshooting.md#regex-issues)

---

## Data Profiling

### Q: What is data profiling?

**A:** DataK9 Profiler analyzes your data files and generates:
- Statistical summaries (min, max, mean, median)
- Data quality metrics (completeness, uniqueness)
- Type inference
- **Auto-generated validation configs!**

**See also:** [Data Profiling Guide](data-profiling.md)

---

### Q: How do I auto-generate a validation config?

**A:** Run the profiler with `--auto-config`:

```bash
python3 -m validation_framework.cli profile data/customers.csv --auto-config
```

This generates `customers_validation.yaml` with suggested validations!

**See also:** [Data Profiling Guide](data-profiling.md#auto-generate-configs)

---

## Licensing & Support

### Q: What license is DataK9 under?

**A:** MIT License - free for commercial and personal use.

---

### Q: How do I get support?

**A:** Several options:
- **[Troubleshooting Guide](troubleshooting.md)** - Common issues and solutions
- **[GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)** - Report bugs or request features
- **[Examples](../examples/)** - Real-world scenarios
- **This FAQ** - Common questions

---

### Q: How do I contribute to DataK9?

**A:** We welcome contributions!

**See also:** [Contributing Guide](../for-developers/contributing.md)

---

### Q: Can I use DataK9 in my commercial project?

**A:** Yes! DataK9 is MIT licensed, which allows commercial use without restrictions.

---

## Still Have Questions?

- **[Troubleshooting Guide](troubleshooting.md)** - Detailed problem solutions
- **[GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)** - Ask the community
- **[Examples](../examples/)** - See real-world scenarios

---

**🐕 Let DataK9 guard your data quality!**
