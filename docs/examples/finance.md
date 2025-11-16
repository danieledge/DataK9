# Financial Services: Data Validation Examples

**DataK9 for Banking, Trading, and Financial Data**

This guide provides production-ready validation examples for financial services data. DataK9 guards your financial data with K9 vigilance!

---

## Table of Contents

1. [Overview](#overview)
2. [Customer Account Data](#customer-account-data)
3. [Transaction Data](#transaction-data)
4. [Trading Data](#trading-data)
5. [Loan Application Data](#loan-application-data)
6. [Anti-Money Laundering (AML)](#anti-money-laundering-aml)
7. [Regulatory Reporting](#regulatory-reporting)
8. [Credit Card Data](#credit-card-data)
9. [Best Practices](#best-practices)

---

## Overview

### Common Financial Data Challenges

- **Regulatory compliance** (PCI-DSS, SOX, Basel III)
- **Data precision** (currency amounts, interest rates)
- **Audit requirements** (complete transaction history)
- **Fraud detection** (anomaly detection)
- **Referential integrity** (accounts, customers, transactions)
- **Data freshness** (real-time or near-real-time)

### Financial Validation Priorities

1. **ERROR Severity:**
   - Missing account numbers
   - Invalid transaction amounts
   - Failed referential integrity
   - Regulatory field violations

2. **WARNING Severity:**
   - Unusual transaction patterns
   - Missing optional fields
   - Statistical outliers
   - Data completeness issues

---

## Customer Account Data

### Scenario: Retail Banking Customer Accounts

**Data:** Customer master data from core banking system

**Compliance:** KYC (Know Your Customer), CIP (Customer Identification Program)

### Configuration

```yaml
validation_job:
  name: "Customer Account Validation"
  description: "Daily customer master data validation for retail banking"

settings:
  chunk_size: 50000
  max_sample_failures: 50

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"

    validations:
      # File-level checks
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      - type: "RowCountRangeCheck"
        severity: "WARNING"
        description: "Expected customer count: 100k - 1M"
        params:
          min_rows: 100000
          max_rows: 1000000

      # Schema validation
      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        description: "Required columns for regulatory compliance"
        params:
          columns:
            - "customer_id"
            - "account_number"
            - "ssn_last4"
            - "first_name"
            - "last_name"
            - "date_of_birth"
            - "email"
            - "phone"
            - "address_line1"
            - "city"
            - "state"
            - "zip_code"
            - "account_type"
            - "account_status"
            - "account_open_date"
            - "kyc_verified"

      # Mandatory fields (KYC requirements)
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "KYC mandatory fields"
        params:
          fields:
            - "customer_id"
            - "account_number"
            - "ssn_last4"
            - "first_name"
            - "last_name"
            - "date_of_birth"

      # Account number uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        description: "Account numbers must be unique"
        params:
          fields: ["account_number"]

      # SSN last 4 format
      - type: "RegexCheck"
        severity: "ERROR"
        description: "SSN last 4 digits format"
        params:
          field: "ssn_last4"
          pattern: "^[0-9]{4}$"
          message: "SSN last 4 must be exactly 4 digits"

      # Email format
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid email format"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          message: "Invalid email format"

      # Phone format (US)
      - type: "RegexCheck"
        severity: "WARNING"
        description: "US phone number format"
        params:
          field: "phone"
          pattern: "^\\(?[0-9]{3}\\)?[-\\s\\.]?[0-9]{3}[-\\s\\.]?[0-9]{4}$"

      # ZIP code format
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid US ZIP code"
        params:
          field: "zip_code"
          pattern: "^[0-9]{5}(-[0-9]{4})?$"

      # State code validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid US state codes"
        params:
          field: "state"
          valid_values:
            - "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA"
            - "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD"
            - "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ"
            - "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC"
            - "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
            - "DC"
          case_sensitive: false

      # Account type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid account types"
        params:
          field: "account_type"
          valid_values:
            - "CHECKING"
            - "SAVINGS"
            - "MONEY_MARKET"
            - "CD"
            - "IRA"
            - "401K"

      # Account status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid account statuses"
        params:
          field: "account_status"
          valid_values:
            - "ACTIVE"
            - "INACTIVE"
            - "CLOSED"
            - "SUSPENDED"
            - "PENDING"

      # Date of birth format
      - type: "DateFormatCheck"
        severity: "ERROR"
        description: "Valid date of birth"
        params:
          field: "date_of_birth"
          format: "%Y-%m-%d"
          allow_null: false

      # Age range (must be 18+)
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Customer must be at least 18 years old"
        params:
          field: "age"
          min_value: 18
          max_value: 120

      # KYC verification boolean
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "KYC verified flag"
        params:
          field: "kyc_verified"
          valid_values: ["YES", "NO"]

      # Data freshness - accounts updated within 24 hours
      - type: "FreshnessCheck"
        severity: "WARNING"
        description: "Customer data updated within 24 hours"
        params:
          check_type: "file_modified"
          max_age_hours: 24
```

---

## Transaction Data

### Scenario: Daily Transaction Validation

**Data:** Daily transaction feed from core banking system

**Compliance:** BSA (Bank Secrecy Act), CTR (Currency Transaction Report)

### Configuration

```yaml
validation_job:
  name: "Transaction Data Validation"
  description: "Daily transaction validation for fraud and compliance"

settings:
  chunk_size: 100000
  max_sample_failures: 100

files:
  - name: "transactions"
    path: "data/transactions.parquet"
    format: "parquet"

    validations:
      # File checks
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Required transaction fields"
        params:
          fields:
            - "transaction_id"
            - "account_number"
            - "transaction_date"
            - "transaction_time"
            - "transaction_type"
            - "amount"
            - "currency"
            - "status"

      # Transaction ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        description: "Transaction IDs must be unique"
        params:
          fields: ["transaction_id"]

      # Referential integrity - account must exist
      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        description: "Account number must exist in customer file"
        params:
          foreign_key: "account_number"
          reference_file: "data/customers.csv"
          reference_key: "account_number"
          allow_null: false

      # Transaction type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid transaction types"
        params:
          field: "transaction_type"
          valid_values:
            - "DEPOSIT"
            - "WITHDRAWAL"
            - "TRANSFER"
            - "PAYMENT"
            - "FEE"
            - "INTEREST"
            - "ADJUSTMENT"

      # Transaction status validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid transaction statuses"
        params:
          field: "status"
          valid_values:
            - "PENDING"
            - "COMPLETED"
            - "FAILED"
            - "REVERSED"
            - "CANCELLED"

      # Currency code validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid ISO currency codes"
        params:
          field: "currency"
          valid_values: ["USD", "EUR", "GBP", "JPY", "CAD"]

      # Amount precision (2 decimal places)
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        description: "Transaction amounts must have max 2 decimal places"
        params:
          field: "amount"
          max_decimal_places: 2

      # Amount range (positive for most types)
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Transaction amount must be positive"
        params:
          field: "amount"
          min_value: 0.01
          max_value: 999999999.99
        condition: "transaction_type != 'FEE' AND transaction_type != 'ADJUSTMENT'"

      # Date format validation
      - type: "DateFormatCheck"
        severity: "ERROR"
        description: "Valid transaction date"
        params:
          field: "transaction_date"
          format: "%Y-%m-%d"

      # Large transaction detection (CTR compliance)
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Large transactions need CTR fields"
        params:
          fields: ["ctr_filed", "ctr_number"]
        condition: "amount >= 10000"

      # Suspicious activity flag for high amounts
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect unusual transaction amounts"
        params:
          field: "amount"
          method: "zscore"
          threshold: 3.0

      # Data freshness - transactions within 2 hours
      - type: "FreshnessCheck"
        severity: "ERROR"
        description: "Transaction data must be within 2 hours"
        params:
          check_type: "field_value"
          field: "transaction_timestamp"
          max_age_hours: 2
```

---

## Trading Data

### Scenario: Securities Trading Validation

**Data:** Trade execution data from trading platform

**Compliance:** SEC, FINRA, MiFID II

### Configuration

```yaml
validation_job:
  name: "Trade Execution Validation"
  description: "Real-time trade validation for compliance"

settings:
  chunk_size: 50000
  max_sample_failures: 50

files:
  - name: "trades"
    path: "data/trades.parquet"
    format: "parquet"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Required trade fields"
        params:
          fields:
            - "trade_id"
            - "order_id"
            - "symbol"
            - "side"
            - "quantity"
            - "price"
            - "execution_timestamp"
            - "account_id"
            - "trader_id"

      # Trade ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["trade_id"]

      # Symbol format (US stocks)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "Valid stock symbol format"
        params:
          field: "symbol"
          pattern: "^[A-Z]{1,5}$"
          message: "Symbol must be 1-5 uppercase letters"

      # Trade side validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        description: "Valid trade sides"
        params:
          field: "side"
          valid_values: ["BUY", "SELL", "SHORT", "COVER"]

      # Quantity must be positive integer
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Quantity must be positive"
        params:
          field: "quantity"
          min_value: 1
          max_value: 10000000

      # Price precision (4 decimal places for stocks)
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        description: "Price precision max 4 decimals"
        params:
          field: "price"
          max_decimal_places: 4

      # Price must be positive
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Price must be positive"
        params:
          field: "price"
          min_value: 0.0001
          max_value: 100000.00

      # Execution timestamp format
      - type: "DateFormatCheck"
        severity: "ERROR"
        description: "Valid execution timestamp"
        params:
          field: "execution_timestamp"
          format: "%Y-%m-%d %H:%M:%S.%f"

      # Trade must be within market hours
      - type: "ConditionalValidation"
        severity: "WARNING"
        description: "After-hours trades need justification"
        params:
          condition: "execution_hour < 9 OR execution_hour >= 16"
          then_validate:
            - type: "MandatoryFieldCheck"
              params:
                fields: ["after_hours_justification"]

      # Large trade detection
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Large trades need approval"
        params:
          fields: ["large_trade_approval", "approver_id"]
        condition: "quantity * price > 1000000"

      # Price outlier detection
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect unusual prices"
        params:
          field: "price"
          method: "iqr"
          threshold: 1.5
```

---

## Loan Application Data

### Scenario: Mortgage Loan Applications

**Data:** Mortgage application data

**Compliance:** TILA (Truth in Lending Act), RESPA, HMDA

### Configuration

```yaml
validation_job:
  name: "Mortgage Application Validation"
  description: "Mortgage application data validation for compliance"

settings:
  chunk_size: 10000

files:
  - name: "loan_applications"
    path: "data/loan_applications.csv"

    validations:
      # Mandatory HMDA fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "HMDA required fields"
        params:
          fields:
            - "application_id"
            - "applicant_name"
            - "applicant_ssn_last4"
            - "property_address"
            - "loan_amount"
            - "loan_purpose"
            - "loan_type"
            - "occupancy_type"
            - "income"
            - "credit_score"
            - "application_date"

      # Application ID uniqueness
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          fields: ["application_id"]

      # Loan amount range
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Loan amount must be realistic"
        params:
          field: "loan_amount"
          min_value: 50000
          max_value: 5000000

      # Loan amount precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "loan_amount"
          max_decimal_places: 2

      # Loan purpose validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "loan_purpose"
          valid_values:
            - "PURCHASE"
            - "REFINANCE"
            - "HOME_IMPROVEMENT"
            - "CASH_OUT_REFINANCE"

      # Loan type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "loan_type"
          valid_values:
            - "CONVENTIONAL"
            - "FHA"
            - "VA"
            - "USDA"
            - "JUMBO"

      # Occupancy type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "occupancy_type"
          valid_values:
            - "PRIMARY_RESIDENCE"
            - "SECOND_HOME"
            - "INVESTMENT_PROPERTY"

      # Income range
      - type: "RangeCheck"
        severity: "WARNING"
        description: "Annual income validation"
        params:
          field: "income"
          min_value: 10000
          max_value: 10000000

      # Credit score range
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Valid credit score range"
        params:
          field: "credit_score"
          min_value: 300
          max_value: 850

      # DTI (Debt-to-Income) ratio
      - type: "RangeCheck"
        severity: "WARNING"
        description: "DTI ratio should be under 43%"
        params:
          field: "dti_ratio"
          min_value: 0
          max_value: 50

      # LTV (Loan-to-Value) ratio
      - type: "RangeCheck"
        severity: "WARNING"
        description: "LTV ratio validation"
        params:
          field: "ltv_ratio"
          min_value: 0
          max_value: 97  # Max for FHA

      # Conditional: FHA loans have stricter requirements
      - type: "ConditionalValidation"
        severity: "ERROR"
        params:
          condition: "loan_type == 'FHA'"
          then_validate:
            - type: "RangeCheck"
              description: "FHA credit score minimum"
              params:
                field: "credit_score"
                min_value: 580

            - type: "RangeCheck"
              description: "FHA LTV maximum"
              params:
                field: "ltv_ratio"
                max_value: 96.5

      # Date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "application_date"
          format: "%Y-%m-%d"
```

---

## Anti-Money Laundering (AML)

### Scenario: AML Transaction Monitoring

**Data:** Aggregated daily transactions for AML screening

**Compliance:** BSA, Patriot Act, OFAC

### Configuration

```yaml
validation_job:
  name: "AML Transaction Monitoring"
  description: "Daily AML screening validation"

files:
  - name: "aml_transactions"
    path: "data/aml_daily.csv"

    validations:
      # Mandatory AML fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "customer_id"
            - "account_number"
            - "transaction_date"
            - "daily_total_amount"
            - "transaction_count"
            - "high_risk_country_flag"
            - "structuring_flag"
            - "unusual_activity_flag"

      # Daily total amount precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "daily_total_amount"
          max_decimal_places: 2

      # Large cash transactions require SAR fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "SAR filing required for large transactions"
        params:
          fields: ["sar_filed", "sar_number", "sar_date"]
        condition: "daily_total_amount >= 10000"

      # Structuring detection
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Structuring requires investigation"
        params:
          fields: ["investigation_id", "investigator"]
        condition: "structuring_flag == 'YES'"

      # High-risk country transactions
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "High-risk country transactions need enhanced due diligence"
        params:
          fields: ["edd_completed", "edd_date", "edd_approver"]
        condition: "high_risk_country_flag == 'YES'"

      # Flag validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "structuring_flag"
          valid_values: ["YES", "NO"]

      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "unusual_activity_flag"
          valid_values: ["YES", "NO"]

      # Outlier detection for amounts
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect unusual transaction amounts"
        params:
          field: "daily_total_amount"
          method: "modified_zscore"
          threshold: 3.5
```

---

## Regulatory Reporting

### Scenario: Call Report (FFIEC 031/041)

**Data:** Quarterly regulatory reporting data

**Compliance:** Federal Reserve, FDIC, OCC

### Configuration

```yaml
validation_job:
  name: "Call Report Validation"
  description: "Quarterly FFIEC Call Report validation"

files:
  - name: "call_report"
    path: "data/call_report_Q1.csv"

    validations:
      # Mandatory fields
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        description: "Required Call Report fields"
        params:
          fields:
            - "institution_id"
            - "rssd_id"
            - "report_date"
            - "total_assets"
            - "total_liabilities"
            - "total_equity"
            - "net_income"
            - "total_loans"
            - "total_deposits"
            - "tier1_capital"
            - "risk_weighted_assets"

      # All monetary fields must have 2 decimal precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "total_assets"
          max_decimal_places: 2

      # Balance sheet must balance
      - type: "CrossFieldComparisonCheck"
        severity: "ERROR"
        description: "Assets must equal Liabilities + Equity"
        params:
          field1: "total_assets"
          field2: "total_liabilities_and_equity"
          operator: "=="

      # Tier 1 capital ratio
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Tier 1 capital ratio must be at least 4%"
        params:
          field: "tier1_capital_ratio"
          min_value: 4.0
          max_value: 100.0

      # Report date format
      - type: "DateFormatCheck"
        severity: "ERROR"
        params:
          field: "report_date"
          format: "%Y-%m-%d"

      # Numeric range validations
      - type: "RangeCheck"
        severity: "ERROR"
        description: "Total assets must be positive"
        params:
          field: "total_assets"
          min_value: 0
```

---

## Credit Card Data

### Scenario: Credit Card Transaction Processing

**Data:** Credit card transactions

**Compliance:** PCI-DSS

### Configuration

```yaml
validation_job:
  name: "Credit Card Transaction Validation"
  description: "Credit card processing validation (PCI-compliant)"

files:
  - name: "card_transactions"
    path: "data/cc_transactions.parquet"
    format: "parquet"

    validations:
      # Mandatory fields (NO FULL CARD NUMBERS - PCI violation)
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "transaction_id"
            - "card_token"  # Tokenized, not actual card number
            - "last_four"
            - "card_type"
            - "amount"
            - "merchant_id"
            - "merchant_category"
            - "transaction_timestamp"
            - "status"

      # Verify NO full card numbers present (PCI compliance)
      - type: "RegexCheck"
        severity: "ERROR"
        description: "CRITICAL: No full card numbers allowed"
        params:
          field: "card_token"
          pattern: "^[0-9]{16}$"
          invert: true  # Fail if matches (16-digit number)
          message: "PCI VIOLATION: Full card number detected"

      # Last 4 digits format
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "last_four"
          pattern: "^[0-9]{4}$"

      # Card type validation
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "card_type"
          valid_values:
            - "VISA"
            - "MASTERCARD"
            - "AMEX"
            - "DISCOVER"

      # Amount precision
      - type: "NumericPrecisionCheck"
        severity: "ERROR"
        params:
          field: "amount"
          max_decimal_places: 2

      # Amount range
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "amount"
          min_value: 0.01
          max_value: 50000.00

      # Transaction status
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "status"
          valid_values:
            - "AUTHORIZED"
            - "DECLINED"
            - "PENDING"
            - "SETTLED"
            - "REFUNDED"
            - "CHARGEBACK"

      # Fraud detection - outlier amounts
      - type: "StatisticalOutlierCheck"
        severity: "WARNING"
        description: "Detect potentially fraudulent amounts"
        params:
          field: "amount"
          method: "zscore"
          threshold: 4.0

      # Real-time freshness
      - type: "FreshnessCheck"
        severity: "ERROR"
        description: "Transactions must be within 15 minutes"
        params:
          check_type: "field_value"
          field: "transaction_timestamp"
          max_age_hours: 0.25  # 15 minutes
```

---

## Best Practices

### 1. Security & Compliance

**Never store sensitive data in plain text:**

```yaml
# ❌ WRONG - PCI VIOLATION
params:
  fields: ["card_number"]  # Never validate full card numbers

# ✅ CORRECT - PCI COMPLIANT
params:
  fields: ["card_token", "last_four"]  # Use tokens
```

**Always encrypt at rest:**
- Use encrypted file systems
- Encrypt Parquet files
- Use database encryption

### 2. Regulatory Compliance

**Document compliance mapping:**

```yaml
validation_job:
  name: "KYC Validation"
  description: |
    Customer validation for KYC/AML compliance
    Regulatory requirements:
    - BSA: Customer identification
    - Patriot Act: Enhanced due diligence
    - OFAC: Sanctions screening
```

**Maintain audit trails:**
- Store validation reports
- Archive failed validations
- Track configuration changes

### 3. Financial Data Precision

**Always use NumericPrecisionCheck:**

```yaml
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "amount"
    max_decimal_places: 2  # Monetary amounts
```

**Validate precision for interest rates:**

```yaml
- type: "NumericPrecisionCheck"
  severity: "ERROR"
  params:
    field: "interest_rate"
    max_decimal_places: 4  # e.g., 5.2500%
```

### 4. Performance for Large Datasets

**Use Parquet for transactions:**

```yaml
files:
  - path: "transactions.parquet"
    format: "parquet"
    columns:  # Only load needed columns
      - "transaction_id"
      - "amount"
      - "account_id"
```

**Optimize chunk size:**

```yaml
settings:
  chunk_size: 100000  # Larger for transaction data
```

### 5. Fraud Detection

**Combine multiple checks:**

```yaml
# Statistical outliers
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "amount"
    method: "modified_zscore"

# Unusual patterns
- type: "MandatoryFieldCheck"
  severity: "ERROR"
  description: "Large transactions need fraud review"
  params:
    fields: ["fraud_review_completed"]
  condition: "amount > 10000"
```

---

## Next Steps

1. **[Healthcare Examples](healthcare.md)** - HIPAA compliance
2. **[E-Commerce Examples](ecommerce.md)** - Customer and order validation
3. **[Best Practices](../using-datak9/best-practices.md)** - Production patterns

---

**🐕 DataK9 for Finance - Your K9 guardian for financial data compliance and quality**
