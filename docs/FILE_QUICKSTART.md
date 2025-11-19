# File Validation Quick Start Guide

## Choose Your Path

DataK9 validates both **files** (CSV, Excel, JSON, Parquet) and **databases** (PostgreSQL, MySQL, SQL Server, Oracle, SQLite).

### 📁 File Validation → **You're in the right place!**

### 🗄️ Database Validation → [Database Quick Start](./DATABASE_QUICKSTART.md)

---

## File Validation in 3 Steps

### Step 1: Basic CSV Validation

```yaml
validation_job:
  name: "Customer Data Validation"

  files:
    - name: customers
      path: "data/customers.csv"
      format: csv
      delimiter: ","
      encoding: "utf-8"

      validations:
        - type: MandatoryFieldCheck
          params:
            fields: [customer_id, email]
          severity: ERROR

        - type: RegexCheck
          params:
            field: email
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          severity: ERROR

  output:
    html_report: "customer_validation_report.html"
    json_summary: "customer_validation_summary.json"
    fail_on_error: true
```

**Run:**
```bash
python3 -m validation_framework.cli validate config.yaml
```

### Step 2: Profile Your Data

Get data quality insights and validation suggestions:

```bash
python3 -m validation_framework.cli profile data/customers.csv -o profile.html -j profile.json
```

### Step 3: Multiple Files & Formats

Validate CSV, Excel, JSON, and Parquet in one job:

```yaml
files:
  - name: customers_csv
    path: "data/customers.csv"
    format: csv

  - name: orders_excel
    path: "data/orders.xlsx"
    format: excel

  - name: products_json
    path: "data/products.json"
    format: json

  - name: transactions_parquet
    path: "data/transactions.parquet"
    format: parquet
```

---

## Supported File Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| **CSV** | `.csv` | Comma-separated values (default delimiter: `,`) |
| **Excel** | `.xlsx`, `.xls` | Microsoft Excel workbooks |
| **JSON** | `.json` | JSON arrays or newline-delimited JSON |
| **Parquet** | `.parquet` | Apache Parquet columnar format |

---

## File vs Database Configuration

### File Source
```yaml
files:
  - name: sales_data
    path: "data/sales.csv"      # File path
    format: csv                 # File format
    delimiter: ","              # CSV-specific
    encoding: "utf-8"           # File encoding
    header: 0                   # Header row number
```

### Database Source
```yaml
files:
  - name: sales_data
    format: database                                    # Database format
    connection_string: "postgresql://user:pass@host/db" # Connection
    table: "sales"                                      # Table name
    max_rows: 50000                                     # Safety limit
```

**Key Differences:**
- Files use `path`, databases use `connection_string`
- Files have format-specific options (delimiter, encoding, header)
- Databases have table/query and max_rows
- Both support all 35 validation types

---

## Format-Specific Options

### CSV Options
```yaml
- name: custom_csv
  path: "data/pipe_delimited.csv"
  format: csv
  delimiter: "|"          # Custom delimiter
  encoding: "latin-1"     # Custom encoding
  header: 0               # Row number with column names
```

### Excel Options
```yaml
- name: excel_data
  path: "data/report.xlsx"
  format: excel
  # Automatically reads first sheet
  # Multi-sheet support coming soon
```

### JSON Options
```yaml
- name: json_data
  path: "data/records.json"
  format: json
  # Supports: JSON arrays, newline-delimited JSON (NDJSON)
```

### Parquet Options
```yaml
- name: parquet_data
  path: "data/large_dataset.parquet"
  format: parquet
  # Best for large files (10x faster than CSV)
  # Preserves data types
```

---

## All Validations Work with Files

All 35 validation types work with file sources. Here are common ones:

### Field Validations
- **MandatoryFieldCheck** - Required fields not null/empty
- **UniqueKeyCheck** - Unique values across column(s)
- **RegexCheck** - Pattern matching (emails, phones, etc.)
- **RangeCheck** - Numeric ranges
- **ValidValuesCheck** - Enum/categorical validation
- **StringLengthCheck** - Min/max string length
- **DateFormatCheck** - Date format validation
- **NumericPrecisionCheck** - Decimal precision

### Schema Validations
- **SchemaMatchCheck** - Expected columns and types
- **ColumnPresenceCheck** - Required columns exist
- **BlankRecordCheck** - Detect completely empty rows
- **DuplicateRowCheck** - Find duplicate records

### Statistical Validations
- **StatisticalOutlierCheck** - Z-score or IQR outliers
- **AdvancedAnomalyDetectionCheck** - ML-based anomaly detection
- **CorrelationCheck** - Minimum correlation between fields
- **DistributionCheck** - Expected statistical distribution

### Cross-Field Validations
- **CrossFieldComparisonCheck** - Compare two fields (price > cost)
- **CompletenessCheck** - Minimum completeness percentage

### Cross-File Validations
- **ReferentialIntegrityCheck** - Foreign key validation across files
- **CrossFileComparisonCheck** - Aggregate comparisons
- **CrossFileDuplicateCheck** - Duplicates across multiple files

**See:** [Complete Validation Catalog](./VALIDATION_CATALOG.md)

---

## Common Patterns

### Pattern 1: CSV with Custom Delimiter
```yaml
files:
  - name: pipe_delimited
    path: "data/export.txt"
    format: csv
    delimiter: "|"
    encoding: "utf-8"

    validations:
      - type: MandatoryFieldCheck
        params:
          fields: [id, name, email]
        severity: ERROR
```

### Pattern 2: Large Parquet File
```yaml
files:
  - name: transactions
    path: "data/transactions.parquet"
    format: parquet
    # Parquet is 10x faster than CSV for large files
    # Automatically chunked for memory efficiency

    validations:
      - type: RangeCheck
        params:
          field: amount
          min: 0
        severity: ERROR
```

### Pattern 3: Multi-File Validation
```yaml
files:
  - name: customers
    path: "data/customers.csv"
    format: csv
    validations:
      - type: UniqueKeyCheck
        params:
          key_fields: [customer_id]
        severity: ERROR

  - name: orders
    path: "data/orders.csv"
    format: csv
    validations:
      - type: ReferentialIntegrityCheck
        params:
          foreign_key: customer_id
          reference_file: customers
          reference_key: customer_id
        severity: ERROR
```

### Pattern 4: Profile Then Validate
```bash
# Step 1: Profile to discover issues
python3 -m validation_framework.cli profile data/sales.csv -o profile.html

# Step 2: Review profile.html for validation suggestions

# Step 3: Create config based on insights
# Step 4: Run validation
python3 -m validation_framework.cli validate sales_validation.yaml
```

---

## Performance Tips

### For Large Files

**Use Parquet Format:**
```bash
# Convert CSV to Parquet (10x faster reads)
python3 -c "
import pandas as pd
df = pd.read_csv('large_file.csv')
df.to_parquet('large_file.parquet')
"
```

**Adjust Chunk Size:**
```yaml
processing:
  chunk_size: 50000  # Default: 10,000 rows per chunk
```

**Sample Large Files:**
```yaml
files:
  - name: sample_validation
    path: "data/huge_file.csv"
    format: csv
    # Only validate first 100K rows
validations:
  - type: RowCountRangeCheck
    params:
      max_rows: 100000
    severity: WARNING
```

### Memory Optimization

- **Chunk Processing**: Automatic (processes 10K rows at a time)
- **Sample Failures**: Limited to 100 per validation (configurable)
- **Format**: Use Parquet for files >100 MB

---

## Troubleshooting

### "File not found" Error

**Check path:**
```yaml
# Relative path (from where you run command)
path: "data/file.csv"

# Absolute path
path: "/home/user/data/file.csv"
```

### "Encoding" Error

**Try different encoding:**
```yaml
encoding: "utf-8"      # Most common
encoding: "latin-1"    # Western European
encoding: "cp1252"     # Windows
encoding: "iso-8859-1" # ISO Latin-1
```

### "Delimiter" Error

**Specify correct delimiter:**
```yaml
delimiter: ","   # Comma
delimiter: ";"   # Semicolon
delimiter: "\t"  # Tab
delimiter: "|"   # Pipe
```

### Large File Timeout

**Solutions:**
1. Convert to Parquet format
2. Increase chunk_size
3. Validate sample instead of entire file

---

## Next Steps

1. **Try the examples** in `examples/` directory
2. **Explore validation catalog**: [VALIDATION_CATALOG.md](./VALIDATION_CATALOG.md)
3. **Use DataK9 Studio** for visual configuration: `datak9-studio.html`
4. **Learn database validation**: [DATABASE_QUICKSTART.md](./DATABASE_QUICKSTART.md)

## Examples

**Quick test with sample data:**
```bash
# Generate sample CSV
python3 scripts/generate_test_data.py

# Profile the data
python3 -m validation_framework.cli profile sample_data.csv -o profile.html

# Run validation
python3 -m validation_framework.cli validate examples/sample_config.yaml
```

---

## Help & Support

- 📖 [Complete Documentation](./README.md)
- 📋 [Validation Catalog](./VALIDATION_CATALOG.md)
- 🗄️ [Database Validation](./DATABASE_QUICKSTART.md)
- 💡 [DataK9 Studio](../datak9-studio.html) - Visual configuration tool
- 🐛 [Report Issues](https://github.com/anthropics/claude-code/issues)
