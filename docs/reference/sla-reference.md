# SLA Reference

**Service Level Agreement (SLA) Compliance for Critical Data Attributes**

This reference describes the SLA framework for tracking data quality compliance on Critical Data Attributes (CDAs).

---

## Table of Contents

1. [Overview](#overview)
2. [Traffic Light Status](#traffic-light-status)
3. [Built-in Tiers](#built-in-tiers)
4. [Configuration](#configuration)
5. [How It Works](#how-it-works)
6. [Output Formats](#output-formats)
7. [Examples](#examples)

---

## Overview

The SLA framework monitors data quality independently of validation severity. While validations use ERROR/WARNING levels to control job exit codes, SLA tracking provides:

- **Tolerance-based monitoring** - Track acceptable failure rates per field
- **Traffic light status** - GREEN/AMBER/RED for quick visibility
- **Aggregated metrics** - Union of failures across multiple validations
- **Explicit field coverage** - No guessing which validations cover which fields

### Key Principles

1. **SLA is orthogonal to validation severity** - A validation can be a WARNING but still breach an SLA
2. **Explicit coverage** - Validators declare which fields they cover via `covered_fields`
3. **Union-based aggregation** - When row IDs are available, failures are deduplicated
4. **Simple configuration** - One tier OR tolerance per field, not both

---

## Traffic Light Status

| Status | Meaning | Condition |
|--------|---------|-----------|
| GREEN | Within tolerance | `failure_rate < warning_threshold` |
| AMBER | Approaching tolerance | `failure_rate >= warning_threshold AND < tolerance` |
| RED | Breached | `failure_rate >= tolerance` |
| N/A | Not evaluated | No validations cover this field |

### Warning Threshold

The warning threshold (AMBER zone) is calculated as:

```
warning_threshold = tolerance * warning_at
```

Default `warning_at` is 0.8 (80%), meaning AMBER triggers at 80% of tolerance.

### Zero Tolerance Special Case

For `critical` tier (0% tolerance):
- GREEN if `bad_records == 0`
- RED otherwise (no AMBER possible)

---

## Built-in Tiers

| Tier | Tolerance | Accuracy | Use Case |
|------|-----------|----------|----------|
| `critical` | 0% | 100% | Primary keys, regulated fields |
| `high` | 0.1% | 99.9% | Financial data, audit fields |
| `standard` | 1% | 99% | Most business fields |
| `low` | 5% | 95% | Non-critical, supplementary data |

---

## Configuration

### Global SLA Defaults

Set job-level defaults in `validation_job`:

```yaml
validation_job:
  name: "Transaction Validation"

  sla_defaults:
    warning_at: 0.8        # Amber at 80% of tolerance
    default_tier: standard # Default tier when not specified
    tiers:                 # Override built-in tier values
      critical: 0.0
      high: 0.001
      standard: 0.01
      low: 0.05
```

### Per-File CDA Definitions

Define CDAs for each file:

```yaml
files:
  - name: "transactions"
    path: "data/transactions.csv"

    critical_data_attributes:
      # Option 1: Named tier
      - field: transaction_id
        tier: critical

      # Option 2: Explicit tolerance
      - field: email
        tolerance: 0.02

      # Option 3: Uses default_tier
      - field: phone

    validations:
      - type: MandatoryFieldCheck
        severity: ERROR
        params:
          fields: [transaction_id, email, phone]
```

### Configuration Rules

- Use `tier: <name>` OR `tolerance: <float>`, not both
- If neither specified, uses `default_tier` from `sla_defaults`
- `tolerance` must be between 0.0 and 1.0

---

## How It Works

### 1. Validators Declare Coverage

Each validator explicitly declares which fields it covers:

```python
# MandatoryFieldCheck declares covered_fields
covered_fields = ["transaction_id", "email", "phone"]
```

### 2. Failures Are Tracked

Validators track which rows failed:

```python
failed_row_ids = {102, 5043, 49821}  # Row IDs that failed
```

### 3. Aggregation Strategy

For each CDA field:

1. Find all validations that cover this field
2. If ANY validation has `failed_row_ids`: union all row IDs (accurate)
3. Otherwise: use max failure rate across validations (conservative)

### 4. SLA Evaluation

```
failure_rate = bad_records / evaluated_records

if failure_rate >= tolerance:
    status = RED
elif failure_rate >= warning_threshold:
    status = AMBER
else:
    status = GREEN
```

---

## Output Formats

### CLI Output

```
================================================================================
SLA COMPLIANCE — transactions.csv
================================================================================

Field                Tier       Tolerance  Actual     Status
--------------------------------------------------------------
transaction_id       critical   0.00%      0.01%      RED
amount               high       0.10%      0.01%      GREEN
customer_email       custom     2.00%      1.75%      GREEN
merchant_category    low        5.00%      3.20%      GREEN
--------------------------------------------------------------
Summary: 3 GREEN  0 AMBER  1 RED

================================================================================
```

### JSON Output

The SLA report is included in the JSON validation report:

```json
{
  "sla_report": {
    "file_name": "transactions.csv",
    "dataset_row_count": 50000,
    "green_count": 3,
    "amber_count": 0,
    "red_count": 1,
    "results": [
      {
        "field": "transaction_id",
        "status": "red",
        "tier": "critical",
        "tolerance": "0.00%",
        "failure_rate": "0.01%",
        "accuracy": "99.99%",
        "bad_records": 3,
        "evaluated_records": 50000,
        "aggregation_method": "union",
        "contributing_validations": ["MandatoryFieldCheck"]
      }
    ]
  }
}
```

---

## Examples

### Basic SLA Configuration

```yaml
validation_job:
  name: "Customer Data Validation"

files:
  - name: "customers"
    path: "data/customers.csv"

    critical_data_attributes:
      - field: customer_id
        tier: critical
      - field: email
        tier: high
      - field: phone
        tier: standard

    validations:
      - type: MandatoryFieldCheck
        severity: ERROR
        params:
          fields: [customer_id, email]

      - type: RegexCheck
        severity: WARNING
        params:
          field: email
          pattern: "^[^@]+@[^@]+\\.[^@]+$"
```

### Custom Tolerances

```yaml
critical_data_attributes:
  - field: account_balance
    tolerance: 0.001  # 0.1% - custom, stricter than standard

  - field: notes
    tolerance: 0.10   # 10% - custom, more lenient than low
```

### Multiple Validations Per Field

When multiple validations cover the same field, failures are aggregated:

```yaml
critical_data_attributes:
  - field: email
    tier: high  # 0.1% tolerance

validations:
  # Both validations cover 'email'
  - type: MandatoryFieldCheck
    severity: ERROR
    params:
      fields: [email]

  - type: RegexCheck
    severity: WARNING
    params:
      field: email
      pattern: "^[^@]+@[^@]+\\.[^@]+$"
```

The SLA evaluator will:
1. Find both validations that cover `email`
2. Union their `failed_row_ids` (deduplicated)
3. Calculate a single failure rate for the field

---

## Best Practices

1. **Start with built-in tiers** - Use `critical`, `high`, `standard`, `low` before custom tolerances
2. **Define CDAs for all business-critical fields** - Don't skip fields that matter
3. **Monitor AMBER status** - AMBER means you're approaching a breach
4. **Review aggregation method** - `union` is more accurate than `max_rate`
5. **Update validators** - Ensure all field-level validations declare `covered_fields`

---

## Related

- [YAML Reference](yaml-reference.md) - Full configuration syntax
- [Validation Reference](validation-reference.md) - Available validations
- [CDA Gap Analysis](../guides/cda-gap-analysis.md) - Finding coverage gaps
