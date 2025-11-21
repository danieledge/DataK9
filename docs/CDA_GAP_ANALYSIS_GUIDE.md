# Critical Data Attribute (CDA) Gap Analysis Guide

## Overview

DataK9's CDA Gap Analysis feature helps you track and ensure validation coverage for Critical Data Attributes - fields that are essential for regulatory compliance, financial accuracy, or business operations.

## Why CDA Tracking?

Organizations face audit requirements to demonstrate that critical data has appropriate validation controls. CDA tracking:

- **Identifies gaps** - Detects critical fields lacking validation coverage
- **Supports audits** - Provides evidence of data quality controls
- **Prioritizes by tier** - Focuses attention on regulatory (TIER_1) fields first
- **Generates reports** - Creates audit-ready HTML reports

## CDA Tiers

Critical Data Attributes are classified into three tiers:

| Tier | Name | Priority | Description |
|------|------|----------|-------------|
| **TIER_1** | Regulatory | Highest | Fields required for regulatory compliance (KYC, GDPR, SOX, etc.) |
| **TIER_2** | Financial | High | Fields used in financial calculations and reporting |
| **TIER_3** | Operational | Normal | Fields important for business operations |

## Quick Start

### 1. Add CDAs to Your YAML Config

Add a `critical_data_attributes` section at the top level of your validation config:

```yaml
validation_job:
  name: "Customer Data Validation"

  # Define Critical Data Attributes at top level
  critical_data_attributes:
    customers:  # Maps to file name
      - field: "customer_id"
        tier: "TIER_1"
        description: "Unique customer identifier for regulatory reporting"

      - field: "email"
        tier: "TIER_1"
        description: "Customer contact for compliance notifications"

      - field: "account_balance"
        tier: "TIER_2"
        description: "Current balance for risk calculations"

  files:
    - name: "customers"
      path: "customers.csv"
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

### Critical Data Attribute Definition

```yaml
critical_data_attributes:
  <file_name>:                    # Must match file name in files section
    - field: "field_name"         # Required: Column/field name
      tier: "TIER_1"              # Required: TIER_1, TIER_2, or TIER_3
      description: "text"         # Optional: Human-readable description
      owner: "Team Name"          # Optional: Business owner
      data_steward: "Person"      # Optional: Data steward contact
      regulatory_reference: "ref" # Optional: Regulatory requirement reference
```

### Complete Example

```yaml
validation_job:
  name: "Financial Data Validation"
  version: "1.0"

  critical_data_attributes:
    customers:
      - field: "customer_id"
        tier: "TIER_1"
        description: "Primary customer identifier"
        owner: "Compliance Team"
        regulatory_reference: "KYC Regulation 4.2.1"

      - field: "tax_id"
        tier: "TIER_1"
        description: "Tax identification number"
        owner: "Finance Team"
        regulatory_reference: "IRS Form 1099"

      - field: "balance"
        tier: "TIER_2"
        description: "Account balance"
        owner: "Risk Team"

    transactions:
      - field: "transaction_id"
        tier: "TIER_1"
        description: "Unique transaction reference"
        regulatory_reference: "SOX Section 404"

  files:
    - name: "customers"
      path: "customers.csv"
      validations:
        - type: "MandatoryFieldCheck"
          params:
            fields: ["customer_id"]  # tax_id will be flagged as gap!

    - name: "transactions"
      path: "transactions.csv"
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

### Options

| Option | Description |
|--------|-------------|
| `-o, --output FILE` | HTML report output path (default: `cda_gap_analysis.html`) |
| `-j, --json-output FILE` | Generate JSON report for automation |
| `--fail-on-gaps` | Exit with error code if any gaps detected |
| `--fail-on-tier1` | Exit with error if TIER_1 gaps detected (default: true) |

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (or gaps exist but not failing on them) |
| 1 | TIER_1 gaps detected (with `--fail-on-tier1`) or any gaps (with `--fail-on-gaps`) |

## CI/CD Integration

### GitHub Actions

```yaml
- name: CDA Gap Analysis
  run: |
    python3 -m validation_framework.cli cda-analysis config.yaml \
      -o cda_report.html \
      -j cda_results.json

- name: Upload CDA Report
  uses: actions/upload-artifact@v3
  with:
    name: cda-gap-report
    path: cda_report.html
```

### Fail Pipeline on Gaps

```yaml
- name: CDA Compliance Check
  run: |
    python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps
```

## Report Output

### HTML Report Includes

- **Summary metrics** - Total CDAs, covered, gaps, coverage percentage
- **Tier breakdown** - Coverage by tier with visual indicators
- **Audit alerts** - Prominent warnings for TIER_1 gaps
- **Field details** - Each CDA with coverage status and validations
- **Printable** - Clean print layout for audit evidence

### JSON Output Structure

```json
{
  "job_name": "Financial Data Validation",
  "timestamp": "2024-01-15T10:30:00",
  "summary": {
    "total_cdas": 10,
    "covered": 8,
    "gaps": 2,
    "coverage_percentage": 80.0,
    "tier1_at_risk": true
  },
  "files": [
    {
      "name": "customers",
      "total_cdas": 5,
      "covered": 4,
      "gaps": 1,
      "fields": [
        {
          "field": "customer_id",
          "tier": "TIER_1",
          "is_covered": true,
          "validations": ["MandatoryFieldCheck", "UniqueKeyCheck"]
        }
      ]
    }
  ]
}
```

## Best Practices

### 1. Define CDAs First

Define critical data attributes before writing validations. This ensures you design validations to cover all critical fields.

### 2. Prioritize TIER_1 Coverage

Focus on achieving 100% coverage for TIER_1 (regulatory) fields first. These represent the highest compliance risk.

### 3. Document Business Owners

Include `owner` and `regulatory_reference` fields to create an audit trail showing who is responsible for each critical field.

### 4. Run in CI/CD

Integrate CDA analysis into your CI/CD pipeline to catch coverage gaps before deployment.

### 5. Review Regularly

Run CDA analysis when:
- Adding new data sources
- Modifying validation configs
- Preparing for audits
- Onboarding new data contracts

## Backwards Compatibility

The `critical_data_attributes` section is optional. Existing YAML configs without CDAs continue to work normally. CDA analysis simply reports "No CDAs defined" if the section is absent.

## Example Files

- `examples/cda_validation_example.yaml` - Complete CDA configuration example
- `examples/sample_config.yaml` - Standard validation (no CDAs)

## See Also

- [CLI Guide](../CLI_GUIDE.md) - Complete CLI reference
- [Configuration Guide](using-datak9/configuration-guide.md) - YAML syntax
- [Validation Reference](../VALIDATION_REFERENCE.md) - All validation types
