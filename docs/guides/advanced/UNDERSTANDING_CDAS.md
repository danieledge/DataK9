# Understanding Critical Data Attributes (CDAs)

## What are Critical Data Attributes?

**Critical Data Attributes (CDAs)** are data fields that are essential to your business operations, regulatory compliance, or financial accuracy. They're the fields that, if incorrect or missing, could cause:

- ❌ Regulatory violations and fines
- ❌ Financial miscalculations
- ❌ Failed audits
- ❌ Business process failures
- ❌ Legal liability

Think of CDAs as your "VIP fields" - the data that absolutely must be correct.

---

## Why Track CDAs?

### 1. Audit Requirements

Regulators and auditors want proof that critical data has appropriate validation controls:

- **SOX (Sarbanes-Oxley)**: Financial transaction fields
- **GDPR**: Personal identifiable information
- **HIPAA**: Protected health information
- **KYC/AML**: Customer identification data
- **PCI DSS**: Payment card data

### 2. Risk Management

Identify where validation gaps create business risk:

- Which critical fields lack validation coverage?
- Are regulatory-required fields properly validated?
- Where should quality assurance efforts focus?

### 3. Quality Assurance

Demonstrate data quality controls to stakeholders:

- Generate audit-ready coverage reports
- Track validation improvements over time
- Show compliance with data governance policies

### 4. Prioritization

Focus validation efforts on what matters most:

- Start with regulatory-required fields
- Expand to financially-critical fields
- Cover operational-critical fields

---

## What Makes a Field "Critical"?

A data field should be marked as a CDA if it meets one or more of these criteria:

### Regulatory Compliance
- Required for regulatory reporting (10-K, 10-Q, tax forms)
- Subject to data privacy laws (GDPR, CCPA)
- Mandated by industry regulations (HIPAA, SOX, Basel III)
- Necessary for KYC/AML compliance

**Examples:**
- Social Security Numbers (SSN)
- Tax IDs (TIN, EIN)
- Customer identification numbers
- Transaction IDs for audit trails

### Financial Impact
- Used in financial calculations
- Affects revenue recognition
- Impacts risk assessments
- Drives pricing or billing

**Examples:**
- Account balances
- Transaction amounts
- Interest rates
- Credit limits

### Operational Criticality
- Required for core business processes
- Necessary for customer communication
- Drives automated workflows
- Affects service delivery

**Examples:**
- Customer email addresses
- Order status codes
- Inventory quantities
- Delivery addresses

### Legal Obligations
- Required in contracts
- Necessary for legal proceedings
- Subject to retention policies
- Used in legal reporting

**Examples:**
- Contract dates
- Agreement signatures
- Party identifications
- Legal entity names

---

## Benefits of CDA Tracking

### ✅ Audit Readiness

Generate reports showing:
- All critical fields are identified
- Validation coverage for each field
- Gaps requiring attention
- Controls in place

### ✅ Risk Visibility

Identify high-risk areas:
- Critical fields without validation
- Incomplete validation coverage
- Fields needing additional controls

### ✅ Focused Effort

Prioritize quality work:
- Add validations where they matter most
- Avoid over-validating non-critical fields
- Balance effort vs. impact

### ✅ Compliance Documentation

Demonstrate to auditors:
- Documented critical data inventory
- Validation controls in place
- Gap remediation tracking
- Continuous monitoring

### ✅ Quality Improvement

Track progress over time:
- Measure coverage improvements
- Show reduction in gaps
- Demonstrate commitment to quality

---

## How DataK9 Handles CDAs

### 1. Define CDAs Inline

Mark critical fields directly in your validation configuration:

```yaml
files:
  - name: "customer_accounts"
    path: "accounts.csv"

    # Define CDAs inline with the file
    critical_data_attributes:
      - field: "customer_id"
        description: "Unique customer identifier"
        owner: "Compliance Team"
        regulatory_reference: "KYC Regulation 4.2.1"

      - field: "tax_id"
        description: "Tax identification number"
        owner: "Finance Team"
        regulatory_reference: "IRS Form 1099"

    validations:
      # Your validations here
```

### 2. Run Gap Analysis

Detect which CDAs lack validation coverage:

```bash
python3 -m validation_framework.cli cda-analysis config.yaml
```

### 3. Review Results

Get detailed reports showing:
- Total CDAs defined
- Coverage percentage
- Gap list (uncovered fields)
- Recommendations

### 4. Add Missing Validations

Close gaps by adding appropriate validations for uncovered CDAs.

### 5. Automate in CI/CD

Prevent deployments with CDA gaps:

```bash
python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps
```

---

## CDA Metadata Fields

### Required

**`field`** - The column/field name (required)

```yaml
- field: "customer_id"
```

### Recommended

**`description`** - Why this field is critical

```yaml
- field: "customer_id"
  description: "Unique customer identifier required for regulatory reporting"
```

**`owner`** - Team or person responsible for data quality

```yaml
- field: "customer_id"
  description: "Unique customer identifier"
  owner: "Compliance Team"
```

**`regulatory_reference`** - Specific regulatory requirement

```yaml
- field: "tax_id"
  description: "Tax identification number"
  regulatory_reference: "IRS Publication 1179, Form 1099 reporting"
```

### Optional

**`data_steward`** - Contact person for data quality issues

```yaml
- field: "account_balance"
  description: "Current account balance"
  owner: "Finance Team"
  data_steward: "jane.smith@company.com"
```

---

## Real-World Examples

### Financial Services

```yaml
files:
  - name: "transactions"
    path: "transactions.csv"

    critical_data_attributes:
      # Regulatory - AML/KYC Requirements
      - field: "transaction_id"
        description: "Unique transaction identifier"
        owner: "Compliance Team"
        regulatory_reference: "Bank Secrecy Act - 31 CFR 1020.410"

      - field: "customer_id"
        description: "Customer identifier for KYC"
        owner: "Compliance Team"
        regulatory_reference: "KYC Regulation 4.2.1"

      # Financial - Risk Calculations
      - field: "amount"
        description: "Transaction amount for risk scoring"
        owner: "Risk Management"
        regulatory_reference: "Basel III Capital Requirements"

      - field: "currency_code"
        description: "ISO currency code for FX calculations"
        owner: "Finance Team"

    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["transaction_id", "customer_id", "amount", "currency_code"]

      - type: "UniqueKeyCheck"
        params:
          fields: ["transaction_id"]

      - type: "RangeCheck"
        params:
          field: "amount"
          min_value: 0.01
          max_value: 10000000

      - type: "ValidValuesCheck"
        params:
          field: "currency_code"
          allowed_values: ["USD", "EUR", "GBP", "JPY", "CHF"]
```

### Healthcare

```yaml
files:
  - name: "patient_records"
    path: "patients.csv"

    critical_data_attributes:
      # Regulatory - HIPAA Requirements
      - field: "patient_id"
        description: "Protected health information identifier"
        owner: "Privacy Officer"
        regulatory_reference: "HIPAA 45 CFR 164.514"

      - field: "medical_record_number"
        description: "Medical record number (PHI)"
        owner: "Medical Records"
        regulatory_reference: "HIPAA Privacy Rule"

      - field: "date_of_birth"
        description: "Patient DOB (PHI)"
        owner: "Patient Registration"
        regulatory_reference: "HIPAA 45 CFR 164.514(b)"

      # Operational - Care Delivery
      - field: "insurance_id"
        description: "Insurance policy number for billing"
        owner: "Billing Department"

    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["patient_id", "medical_record_number", "date_of_birth"]

      - type: "DateFormatCheck"
        params:
          field: "date_of_birth"
          format: "%Y-%m-%d"

      - type: "UniqueKeyCheck"
        params:
          fields: ["patient_id"]
```

### E-Commerce

```yaml
files:
  - name: "orders"
    path: "orders.csv"

    critical_data_attributes:
      # Operational - Order Processing
      - field: "order_id"
        description: "Unique order identifier"
        owner: "Operations Team"

      - field: "customer_email"
        description: "Customer contact for order updates"
        owner: "Customer Service"
        regulatory_reference: "GDPR Article 13 (communication requirement)"

      # Financial - Revenue Recognition
      - field: "order_total"
        description: "Total order amount for revenue"
        owner: "Finance Team"

      - field: "payment_status"
        description: "Payment status for accounting"
        owner: "Finance Team"

    validations:
      - type: "MandatoryFieldCheck"
        params:
          fields: ["order_id", "customer_email", "order_total", "payment_status"]

      - type: "RegexCheck"
        params:
          field: "customer_email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      - type: "RangeCheck"
        params:
          field: "order_total"
          min_value: 0

      - type: "ValidValuesCheck"
        params:
          field: "payment_status"
          allowed_values: ["pending", "paid", "failed", "refunded"]
```

---

## Best Practices

### 1. Start with Regulatory Fields

Begin by identifying fields required for regulatory compliance:

```yaml
critical_data_attributes:
  # Start here - regulatory fields first
  - field: "ssn"
    regulatory_reference: "IRS Form W-2"

  - field: "transaction_id"
    regulatory_reference: "SOX Section 404"
```

### 2. Add Financial Fields

Next, include fields used in financial calculations:

```yaml
  # Add financial fields
  - field: "revenue_amount"
    description: "Monthly recurring revenue"

  - field: "account_balance"
    description: "Current balance for risk assessment"
```

### 3. Include Operational Fields

Finally, add fields critical for business operations:

```yaml
  # Include operational fields
  - field: "order_status"
    description: "Order processing state"

  - field: "customer_email"
    description: "Primary contact method"
```

### 4. Document Everything

Always include description and owner:

```yaml
- field: "tax_id"
  description: "Tax identification number for 1099 reporting"
  owner: "Finance Team"
  regulatory_reference: "IRS Form 1099"
  data_steward: "tax-team@company.com"
```

### 5. Review Quarterly

Update CDAs quarterly:
- Are all critical fields still relevant?
- Do new regulations add requirements?
- Have business processes changed?
- Are validations still appropriate?

### 6. Integrate with CI/CD

Prevent gaps from reaching production:

```yaml
# .github/workflows/validation.yml
- name: CDA Gap Analysis
  run: |
    python3 -m validation_framework.cli cda-analysis config.yaml --fail-on-gaps
```

---

## When NOT to Use CDAs

CDAs are for critical fields only. Don't mark these as CDAs:

❌ **Non-critical descriptive fields** (comments, notes, labels)
❌ **Internal metadata** (created_by, updated_at)
❌ **Derived fields** (calculated values, aggregations)
❌ **Nice-to-have fields** (optional preferences, settings)

**Remember:** If every field is critical, no field is critical. Be selective.

---

## FAQ

### Q: How many CDAs should I have?

**A:** Typically 10-30% of your fields. If you have 50 fields, expect 5-15 CDAs. Quality over quantity.

### Q: Do all CDAs need validations?

**A:** Yes! That's the point of CDA tracking - ensuring critical fields have appropriate validation coverage.

### Q: Can CDAs change over time?

**A:** Absolutely. Add new CDAs when regulations change or business needs evolve. Remove CDAs that are no longer critical.

### Q: What if I have hundreds of critical fields?

**A:** Consider splitting into multiple files or using a data catalog tool for enterprise-scale tracking.

### Q: Should I include the same CDA in multiple files?

**A:** Yes, if the field appears in multiple datasets and is critical in each context.

### Q: What's the difference between mandatory fields and CDAs?

**A:**
- **Mandatory** = field must have a value (technical requirement)
- **CDA** = field is business/regulatory-critical (business requirement)

A field can be both, one, or neither.

---

## Next Steps

1. **Identify your CDAs** - Review your data and list regulatory, financial, and operational-critical fields
2. **Add CDA definitions** - Update your YAML configs with inline `critical_data_attributes`
3. **Run gap analysis** - Use `cda-analysis` command to detect coverage gaps
4. **Add missing validations** - Close gaps by adding appropriate validations
5. **Automate** - Integrate CDA checking into your CI/CD pipeline

**→ [CDA Gap Analysis Guide](CDA_GAP_ANALYSIS_GUIDE.md)** - Technical implementation guide

**→ [Configuration Guide](../../using-datak9/configuration-guide.md)** - YAML syntax reference

**→ [Validation Reference](../../reference/validation-reference.md)** - All validation types

---

## Summary

**Critical Data Attributes (CDAs)** help you:
- ✅ Identify your most important data fields
- ✅ Ensure critical fields have validation coverage
- ✅ Generate audit-ready compliance reports
- ✅ Focus quality efforts where they matter most
- ✅ Demonstrate data governance controls

**Define CDAs inline, run gap analysis, add missing validations, and automate in CI/CD.**

Your data quality starts with knowing what's critical. 🐕
