# Getting Started with DataK9
## 🐕 Get up and running in 5 minutes!

This guide will walk you through creating your first data validation from scratch with **DataK9** - your K9 guardian for data quality.

---

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)

---

## Step 1: Installation (1 minute)

Clone the repository and install dependencies:

```bash
# Clone the repository
git clone https://github.com/danieledge/data-validation-tool.git
cd data-validation-tool

# Install dependencies
pip install -r requirements.txt

# Install DataK9
pip install -e .
```

**That's it!** No complex setup required.

---

## Step 2: Prepare Your Data (1 minute)

For this quickstart, we'll use a sample customer data file. Create a file called `customers.csv`:

```csv
customer_id,first_name,last_name,email,age,account_balance
1,John,Doe,john.doe@example.com,34,1500.00
2,Jane,Smith,jane.smith@example.com,28,2300.50
3,Bob,Johnson,bob@example.com,45,500.00
4,Alice,Williams,,31,1200.00
5,Charlie,Brown,charlie.brown@example.com,150,-500.00
```

**Notice the issues:**
- Row 4: Missing email
- Row 5: Age is 150 (unrealistic)
- Row 5: Negative balance

DataK9 will sniff these out! 🐕

---

## Step 3: Create Your Validation Config (2 minutes)

Create a file called `my_first_validation.yaml`:

```yaml
validation_job:
  name: "Customer Data Validation"
  description: "Validate customer data quality"

settings:
  chunk_size: 1000
  max_sample_failures: 100

files:
  - name: "customers"
    path: "customers.csv"
    format: "csv"
    validations:
      # Check file is not empty
      - type: "EmptyFileCheck"
        severity: "ERROR"

      # Check required fields are present
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "first_name", "last_name", "email"]

      # Validate email format
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      # Check age is reasonable
      - type: "RangeCheck"
        severity: "WARNING"
        params:
          field: "age"
          min_value: 18
          max_value: 120

      # Check balance is not negative
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "account_balance"
          min_value: 0
```

**What this does:**
- Checks the file isn't empty
- Ensures required fields (customer_id, name, email) are filled
- Validates email addresses match proper format
- Warns if age is outside 18-120 range
- Errors if balance is negative

---

## Step 4: Run Your Validation (1 minute)

Let DataK9 guard your data:

```bash
[DataK9] 🐕
python3 -m validation_framework.cli validate my_first_validation.yaml
```

You'll see output like this:

```
================================================================================
DataK9 - Your K9 guardian for data quality
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

File Results:
  ✗ customers: 3 errors, 0 warnings
```

DataK9 found the issues! 🎯

---

## Step 5: View the Report (1 minute)

Generate an HTML report:

```bash
python3 -m validation_framework.cli validate my_first_validation.yaml --html report.html
```

Open `report.html` in your browser to see:
- ✅ Overall validation status
- 📊 Summary statistics
- 📝 Detailed failure information
- 🔍 Sample failing rows with specific issues
- 🌙 Beautiful dark theme

---

## What You've Learned

In just 5 minutes, you've:
1. ✅ Installed DataK9
2. ✅ Created a validation configuration
3. ✅ Validated data and found issues
4. ✅ Generated a professional report

**DataK9 is now guarding your data pipeline!** 🛡️

---

## Next Steps

### Continue Learning:
- **[What is DataK9?](../using-datak9/what-is-datak9.md)** - Understand DataK9's capabilities
- **[Configuration Guide](../using-datak9/configuration-guide.md)** - Complete guide to writing validations
- **[Validation Catalog](../reference/validation-reference.md)** - All 35+ validation types
- **[Examples](../examples/)** - Real-world validation scenarios

### For Developers:
- **[Architecture](../for-developers/architecture.md)** - How DataK9 works internally
- **[Custom Validations](../for-developers/custom-validations.md)** - Extend the framework
- **[API Reference](../for-developers/api-reference.md)** - Detailed API documentation

---

## Common Next Tasks

### Validate Multiple Files

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
```

### Add Conditional Validations

Only check company name for business accounts:

```yaml
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  params:
    fields: ["company_name"]
  condition: "account_type == 'BUSINESS'"
```

### Validate Different File Formats

```yaml
# CSV file
- name: "customers"
  path: "customers.csv"
  format: "csv"

# Excel file
- name: "sales"
  path: "sales.xlsx"
  format: "excel"
  sheet_name: "Q1_Sales"

# JSON file
- name: "api_data"
  path: "response.json"
  format: "json"

# Parquet file (10x faster!)
- name: "analytics"
  path: "analytics.parquet"
  format: "parquet"
```

### Generate JSON Report for CI/CD Integration

```bash
python3 -m validation_framework.cli validate config.yaml --json results.json
```

Perfect for AutoSys, Airflow, and other orchestration tools!

---

## Troubleshooting

### "ModuleNotFoundError"
- Make sure you ran `pip install -r requirements.txt`
- Check you're in the correct directory

### "FileNotFoundError"
- Verify the `path` in your YAML points to the correct file
- Use absolute paths if needed: `/full/path/to/file.csv`

### Validation Not Working
- Check YAML syntax (indentation matters - use 2 spaces!)
- Verify field names match your data exactly (case-sensitive)
- Use `--verbose` flag for detailed output

### Need Help?
- Check the **[FAQ](../using-datak9/faq.md)** for common questions
- See **[Troubleshooting Guide](../using-datak9/troubleshooting.md)** for solutions
- Review **[Validation Catalog](../reference/validation-reference.md)** for all validation types
- Open an issue on [GitHub](https://github.com/danieledge/data-validation-tool/issues)

---

## Ready for More?

Continue to the **[Using DataK9 Guide](../using-datak9/README.md)** for comprehensive documentation on all features and capabilities.

**🐕 Let DataK9 guard your data quality!**
