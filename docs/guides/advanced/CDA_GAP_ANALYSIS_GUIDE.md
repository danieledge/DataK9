# Critical Data Attribute (CDA) Gap Analysis Guide

## Overview

DataK9's CDA Gap Analysis feature helps you track and ensure validation coverage for Critical Data Attributes - fields that are essential for regulatory compliance, financial accuracy, or business operations.

## Why CDA Tracking?

Organizations face audit requirements to demonstrate that critical data has appropriate validation controls. CDA tracking:

- **Identifies gaps** - Detects critical fields lacking validation coverage
- **Supports audits** - Provides evidence of data quality controls
- **Prioritizes work** - Shows which critical fields need validation
- **Generates reports** - Creates audit-ready HTML reports

## Quick Start

### 1. Add CDAs to Your YAML Config

Define Critical Data Attributes **inline** with each file configuration:

```yaml
validation_job:
  name: "Customer Data Validation"

  files:
    - name: "customers"
      path: "customers.csv"

      # Define Critical Data Attributes inline
      critical_data_attributes:
        - field: "customer_id"
          description: "Unique customer identifier for regulatory reporting"
          owner: "Data Team"

        - field: "email"
          description: "Customer contact for compliance notifications"
          owner: "Compliance Team"

        - field: "account_balance"
          description: "Current balance for risk calculations"
          owner: "Finance Team"

      validations:
        - type: "MandatoryFieldCheck"
          params:
            fields: ["customer_id", "email"]
```

### 2. Run CDA Analysis

```bash
python3 -m validation_framework.cli cda-analysis config.yaml
```

### 3. Review Results

The command outputs:
- Console summary showing coverage by file
- List of gaps (uncovered CDAs)
- HTML report for detailed review

## YAML Schema

### Critical Data Attribute Definition (Inline - Recommended)

```yaml
files:
  - name: "<file_name>"
    path: "path/to/file.csv"

    # Define CDAs inline with the file
    critical_data_attributes:
      - field: "field_name"            # Required: Column/field name
        description: "text"             # Optional: Human-readable description
        owner: "Team Name"              # Optional: Business owner
        data_steward: "Person"          # Optional: Data steward contact
        regulatory_reference: "ref"     # Optional: Regulatory requirement reference

    validations:
      # Your validations here
```

### Complete Example

```yaml
validation_job:
  name: "Financial Data Validation"
  version: "1.0"

  files:
    - name: "customers"
      path: "customers.csv"

      critical_data_attributes:
        - field: "customer_id"
          description: "Primary customer identifier"
          owner: "Compliance Team"
          regulatory_reference: "KYC Regulation 4.2.1"

        - field: "tax_id"
          description: "Tax identification number"
          owner: "Finance Team"
          regulatory_reference: "IRS Form 1099"

        - field: "balance"
          description: "Account balance"
          owner: "Risk Team"

      validations:
        - type: "MandatoryFieldCheck"
          params:
            fields: ["customer_id"]  # tax_id will be flagged as gap!

    - name: "transactions"
      path: "transactions.csv"

      critical_data_attributes:
        - field: "transaction_id"
          description: "Unique transaction reference"
          regulatory_reference: "SOX Section 404"

      validations:
        - type: "UniqueKeyCheck"
          params:
            fields: ["transaction_id"]
```

## CLI Command Reference

### Basic Usage

```bash
# Run CDA gap analysis
python3 -m validation_framework.cli cda-analysis config.yaml

# Custom HTML output path
python3 -m validation_framework.cli cda-analysis config.yaml -o gaps.html

# Generate JSON for automation
python3 -m validation_framework.cli cda-analysis config.yaml -j gaps.json
```

### CI/CD Integration

```bash
# Fail build if any CDA gaps detected
python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps

# In GitHub Actions
- name: CDA Gap Analysis
  run: |
    python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps
    if [ $? -ne 0 ]; then
      echo "CDA gaps detected - blocking deployment"
      exit 1
    fi
```

### Exit Codes

- `0` - No gaps detected (all CDAs covered)
- `1` - Gaps detected (when using `--fail-on-gaps`)
- `2` - Command error (bad config, file not found)

## Understanding the Report

### Console Output

```
╔══════════════════════════════════════════════════════════════╗
║         CDA Gap Analysis: Financial Data Validation          ║
╚══════════════════════════════════════════════════════════════╝

📊 File: customers
   Total CDAs: 3
   ✓ Covered: 1 (33%)
   ✗ Gaps: 2 (67%)

   ✓ COVERED FIELDS:
     • customer_id - Primary customer identifier
       Validated by: MandatoryFieldCheck

   ✗ GAP FIELDS (need validation):
     • tax_id - Tax identification number
     • balance - Account balance

📊 File: transactions
   Total CDAs: 1
   ✓ Covered: 1 (100%)
   ✗ Gaps: 0 (0%)

╔══════════════════════════════════════════════════════════════╗
║                        Overall Summary                        ║
╠══════════════════════════════════════════════════════════════╣
║  Total CDAs:      4                                          ║
║  Covered:         2 (50%)                                    ║
║  Gaps:            2 (50%)                                    ║
╚══════════════════════════════════════════════════════════════╝

⚠️  ATTENTION: 2 critical fields lack validation coverage
```

### HTML Report Features

- **Coverage summary** - Visual progress bars and percentages
- **Gap details** - Uncovered fields with descriptions and owners
- **Validation coverage** - Which validations cover each CDA
- **Recommendations** - Suggested validations for gaps
- **Audit-ready** - Formatted for compliance documentation

## CDA Field Metadata

### Required Fields

- `field` - The column/field name (required)

### Optional Fields

- `description` - Human-readable description of field importance
- `owner` - Business team or person responsible for the field
- `data_steward` - Data quality steward contact
- `regulatory_reference` - Regulatory requirement (e.g., "GDPR Article 15", "SOX Section 404")

### Example with Full Metadata

```yaml
critical_data_attributes:
  - field: "ssn"
    description: "Social Security Number for tax reporting"
    owner: "HR Department"
    data_steward: "Jane Smith (jane@company.com)"
    regulatory_reference: "IRS Publication 1179"
```

## Legacy Syntax (Still Supported)

For backwards compatibility, the old top-level syntax is still supported:

```yaml
validation_job:
  name: "Customer Data Validation"

  # Legacy: Top-level CDAs keyed by file name
  critical_data_attributes:
    customers:  # Must match file name below
      - field: "customer_id"
        description: "Primary identifier"

  files:
    - name: "customers"  # Must match key above
      path: "customers.csv"
      validations: [...]
```

**Note:** The inline syntax (recommended above) is preferred as it's more intuitive and keeps CDAs with their files.

## Best Practices

### 1. Focus on High-Impact Fields

Prioritize CDAs that are:
- Required for regulatory compliance (KYC, GDPR, SOX, HIPAA)
- Used in financial calculations
- Critical for business operations
- Subject to audit requirements

### 2. Document Clearly

Always include:
- **Description** - Why this field is critical
- **Owner** - Who is responsible for data quality
- **Regulatory reference** - Specific compliance requirement

### 3. Start Small

Begin with regulatory-required fields, then expand to financial and operational CDAs.

### 4. Integrate with CI/CD

Use `--fail-on-gaps` to prevent deployments when critical fields lack validation:

```yaml
# .github/workflows/validation.yml
- name: CDA Gap Analysis
  run: |
    python3 -m validation_framework.cli cda-analysis config.yaml \
      --fail-on-gaps \
      -o cda_gaps.html
```

### 5. Regular Reviews

Review CDA coverage quarterly:
- Are all critical fields still relevant?
- Do new regulatory requirements add CDAs?
- Are validations comprehensive enough?

## Common Validation Types for CDAs

| CDA Type | Recommended Validations |
|----------|------------------------|
| Identifiers (customer_id, transaction_id) | MandatoryFieldCheck, UniqueKeyCheck |
| Email addresses | MandatoryFieldCheck, RegexCheck |
| Dates (transaction_date, registration_date) | MandatoryFieldCheck, DateFormatCheck, FreshnessCheck |
| Amounts (balance, transaction_amount) | MandatoryFieldCheck, RangeCheck, NumericPrecisionCheck |
| Tax IDs, SSNs | MandatoryFieldCheck, RegexCheck, StringLengthCheck |
| Status fields | MandatoryFieldCheck, ValidValuesCheck |
| Phone numbers | RegexCheck |

## Troubleshooting

### "No critical_data_attributes defined"

**Cause:** No CDAs found in configuration

**Solution:** Add `critical_data_attributes` list to at least one file config

### Field showing as gap but is validated

**Cause:** Field name mismatch between CDA and validation

**Solution:** Ensure exact field name match (case-sensitive):

```yaml
critical_data_attributes:
  - field: "customer_id"  # Exact name

validations:
  - type: "MandatoryFieldCheck"
    params:
      fields: ["customer_id"]  # Must match exactly
```

### Want to exclude some files from CDA tracking

**Solution:** Simply don't define `critical_data_attributes` for files that don't need tracking

## Example: Full Financial Compliance Config

```yaml
validation_job:
  name: "Financial Compliance Validation"
  version: "1.0"

  files:
    - name: "customer_accounts"
      path: "data/accounts.csv"

      critical_data_attributes:
        # Regulatory - KYC Requirements
        - field: "customer_id"
          description: "Unique customer identifier"
          owner: "Compliance Team"
          regulatory_reference: "KYC Regulation 4.2.1"

        - field: "tax_id"
          description: "Tax identification number"
          owner: "Finance Team"
          regulatory_reference: "IRS Form 1099"

        # Financial - Risk Calculations
        - field: "account_balance"
          description: "Current account balance"
          owner: "Risk Management"
          regulatory_reference: "Basel III Capital Requirements"

        - field: "credit_limit"
          description: "Approved credit limit"
          owner: "Credit Department"

        # Operational - Contact Requirements
        - field: "email"
          description: "Primary contact email"
          owner: "Customer Service"
          regulatory_reference: "GDPR Article 13"

      validations:
        # Identity validation
        - type: "MandatoryFieldCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id", "tax_id", "email"]

        - type: "UniqueKeyCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id"]

        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "tax_id"
            pattern: "^\\d{2}-\\d{7}$"

        # Financial validation
        - type: "RangeCheck"
          severity: "WARNING"
          params:
            field: "account_balance"
            min_value: 0
            max_value: 10000000

        - type: "RangeCheck"
          severity: "WARNING"
          params:
            field: "credit_limit"
            min_value: 0
            max_value: 1000000

        # Contact validation
        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "email"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
```

---

## Summary

CDA Gap Analysis helps you:
1. **Define** critical fields inline with each file
2. **Track** which critical fields have validation coverage
3. **Report** gaps for audit and compliance
4. **Automate** gap detection in CI/CD pipelines

**Next Steps:**
- Review your data files and identify critical fields
- Add `critical_data_attributes` to your configs
- Run `cda-analysis` to detect gaps
- Add validations to close gaps
- Integrate with CI/CD using `--fail-on-gaps`

---

**See also:**
- [Validation Reference](../../reference/validation-reference.md) - All validation types
- [Configuration Guide](../../using-datak9/configuration-guide.md) - YAML syntax
- [CI/CD Integration](../../using-datak9/cicd-integration.md) - Pipeline integration
