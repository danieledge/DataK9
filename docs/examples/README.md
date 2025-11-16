# DataK9 Examples & Recipes

Real-world validation scenarios using **DataK9** - your K9 guardian for data quality.

---

## 📚 Example Categories

### By Industry

Comprehensive industry-specific validation examples:

- **[Finance & Banking](finance.md)**
  - Transaction validation
  - Account data quality
  - Regulatory compliance (AML, KYC, BSA, CTR)
  - Trading data validation (SEC, FINRA)
  - Fraud detection
  - Credit card data (PCI-DSS)

- **[Healthcare](healthcare.md)**
  - Patient data validation
  - HIPAA compliance & de-identification
  - Clinical data quality
  - Lab results validation (LOINC codes)
  - Insurance claims (ICD-10, CPT)
  - EHR integration (HL7)

- **[E-Commerce](ecommerce.md)**
  - Customer data validation
  - Order processing
  - Inventory management
  - Product catalog quality
  - Payment transactions
  - Shopping cart validation

---

## 🚀 Quick Examples

### Example 1: Customer Data Validation

**Scenario:** Validate customer CSV before loading to CRM

```yaml
validation_job:
  name: "Customer Data Quality Check"
  description: "Ensure customer data meets quality standards"

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

      # Required columns exist
      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        params:
          required_columns:
            - "customer_id"
            - "email"
            - "first_name"
            - "last_name"
            - "registration_date"

      # Customer ID is mandatory and unique
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id"]

      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_fields: ["customer_id"]

      # Email format validation
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      # Names must not be empty
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["first_name", "last_name"]

      # Registration date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "registration_date"
          format: "%Y-%m-%d"

      # Age should be reasonable (WARNING, not ERROR)
      - type: "RangeCheck"
        severity: "WARNING"
        params:
          field: "age"
          min_value: 18
          max_value: 120

output:
  html_report: "customer_validation_report.html"
  json_summary: "customer_validation_summary.json"
  fail_on_error: true
```

---

### Example 2: Financial Transaction Validation

**Scenario:** Validate transaction data for fraud detection

```yaml
validation_job:
  name: "Transaction Validation"
  description: "Validate transaction data quality and detect anomalies"

files:
  - name: "transactions"
    path: "data/transactions.csv"
    format: "csv"

    validations:
      # Transaction amount must be positive
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "amount"
          min_value: 0.01

      # Transaction date cannot be in future
      - type: "FreshnessCheck"
        severity: "ERROR"
        params:
          date_field: "transaction_date"
          max_age_days: 0  # No future dates

      # Detect statistical outliers in amounts
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        params:
          field: "amount"
          method: "zscore"
          threshold: 3.0

      # Account ID must exist
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["account_id", "transaction_id"]

      # No duplicate transaction IDs
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_fields: ["transaction_id"]

      # Transaction type must be valid
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "transaction_type"
          allowed_values:
            - "DEBIT"
            - "CREDIT"
            - "TRANSFER"
            - "WITHDRAWAL"
            - "DEPOSIT"

output:
  html_report: "transaction_validation_report.html"
  json_summary: "transaction_summary.json"
```

---

### Example 3: Multi-File Validation with Cross-File Checks

**Scenario:** Validate orders and customers, ensuring referential integrity

```yaml
validation_job:
  name: "E-Commerce Data Validation"
  description: "Validate orders and customers with foreign key checks"

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"
    validations:
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_fields: ["customer_id"]

  - name: "orders"
    path: "data/orders.csv"
    format: "csv"
    validations:
      # Order amount must be positive
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "order_total"
          min_value: 0.01

      # Customer ID must exist in customers file
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "customer_id"
          reference_file: "customers"
          reference_key: "customer_id"

output:
  html_report: "ecommerce_validation_report.html"
```

---

### Example 4: Conditional Validation

**Scenario:** Different validation rules based on account type

```yaml
validation_job:
  name: "Account Validation with Conditional Logic"

files:
  - name: "accounts"
    path: "data/accounts.csv"
    format: "csv"

    validations:
      # All accounts must have account_id
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["account_id", "account_type"]

      # Business accounts must have company name
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["company_name", "tax_id"]
        condition: "account_type == 'BUSINESS'"

      # Personal accounts must have first/last name
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["first_name", "last_name"]
        condition: "account_type == 'PERSONAL'"

      # Premium accounts must have high balance
      - type: "RangeCheck"
        severity: "WARNING"
        params:
          field: "balance"
          min_value: 10000
        condition: "account_tier == 'PREMIUM'"

output:
  html_report: "account_validation_report.html"
```

---

### Example 5: Large File Processing (Parquet)

**Scenario:** Validate 100GB+ file efficiently

```yaml
validation_job:
  name: "Large File Validation"
  description: "Efficiently validate massive dataset"

settings:
  chunk_size: 100000  # Larger chunks for better performance
  max_sample_failures: 50  # Reduce memory usage

files:
  - name: "large_dataset"
    path: "data/large_dataset.parquet"  # Parquet is 10x faster!
    format: "parquet"

    validations:
      # Keep validations fast
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["id", "timestamp"]

      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "value"
          min_value: 0
          max_value: 1000

output:
  # JSON for faster reporting
  json_summary: "large_file_results.json"
  fail_on_error: true
```

---

## 📂 Example Data Files

Sample data files for testing are available in:
```
examples/
└── sample-data/
    ├── customers.csv
    ├── orders.csv
    ├── transactions.csv
    └── invalid_data.csv
```

---

## 🎯 Common Patterns

### Pattern 1: ERROR vs WARNING

```yaml
# Use ERROR for must-fix issues
- type: "MandatoryFieldCheck"
  severity: "ERROR"  # Data cannot proceed without this
  params:
    fields: ["customer_id"]

# Use WARNING for should-review issues
- type: "RangeCheck"
  severity: "WARNING"  # Suspicious but might be valid
  params:
    field: "age"
    min_value: 18
    max_value: 120
```

### Pattern 2: Fast-Fail Ordering

```yaml
# Order validations from fastest to slowest
validations:
  - type: "EmptyFileCheck"           # Fastest
  - type: "ColumnPresenceCheck"      # Fast
  - type: "MandatoryFieldCheck"      # Fast
  - type: "RegexCheck"               # Medium
  - type: "StatisticalOutlierCheck"  # Slowest (multiple passes)
```

### Pattern 3: Progressive Validation

```yaml
# Stage 1: Structure checks
- name: "stage1_structure"
  validations:
    - type: "EmptyFileCheck"
    - type: "SchemaMatchCheck"

# Stage 2: Data quality (only if stage 1 passes)
- name: "stage2_quality"
  validations:
    - type: "MandatoryFieldCheck"
    - type: "RegexCheck"
    - type: "RangeCheck"
```

---

## 📖 Related Documentation

- **[Configuration Guide](../using-datak9/configuration-guide.md)** - Complete YAML reference
- **[Validation Catalog](../using-datak9/validation-catalog.md)** - All validation types
- **[Best Practices](../using-datak9/best-practices.md)** - Production tips
- **[Performance Tuning](../using-datak9/performance-tuning.md)** - Optimization guide

---

**🐕 Use these examples to guard your data with DataK9!**
