# Creating Custom Validations for DataK9

**Extend DataK9 with your own validation rules** 🐕

This guide shows you how to create custom validation rules for DataK9. Like training a K9 unit with specialized skills, you can teach DataK9 new ways to detect data quality issues specific to your business needs.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Understanding the Base Class](#understanding-the-base-class)
3. [Step-by-Step Tutorial](#step-by-step-tutorial)
4. [Advanced Features](#advanced-features)
5. [Testing Custom Validations](#testing-custom-validations)
6. [Real-World Examples](#real-world-examples)
7. [Best Practices](#best-practices)

---

## Quick Start

### 30-Second Custom Validation

```python
from validation_framework.validations.base import DataValidationRule
from validation_framework.core.registry import register_validation

class EmailDomainCheck(DataValidationRule):
    """Validates email domains match approved list"""

    def get_description(self) -> str:
        return f"Checks email domains against approved list"

    def validate(self, data_iterator, context):
        approved_domains = self.params.get("approved_domains", [])
        failures = []

        for chunk in data_iterator:
            invalid_emails = chunk[
                ~chunk['email'].str.contains('|'.join(approved_domains))
            ]
            failures.extend(invalid_emails.to_dict('records'))

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Found {len(failures)} emails with unapproved domains",
            failed_count=len(failures),
            sample_failures=failures[:100]
        )

# Register it!
register_validation("EmailDomainCheck", EmailDomainCheck)
```

**Use in YAML:**
```yaml
validations:
  - type: "EmailDomainCheck"
    severity: "ERROR"
    params:
      approved_domains: ["company.com", "partner.com"]
```

That's it! DataK9 can now sniff out unapproved email domains! 🐕

---

## Understanding the Base Class

### The DataValidationRule Base Class

All custom validations inherit from `DataValidationRule`:

```python
from validation_framework.validations.base import DataValidationRule

class MyValidation(DataValidationRule):
    """
    Base class provides:
    - self.name: Validation name
    - self.severity: ERROR or WARNING
    - self.params: Configuration parameters
    - self.condition: Optional condition expression
    """

    def get_description(self) -> str:
        """Return human-readable description"""
        pass

    def validate(self, data_iterator, context) -> ValidationResult:
        """Execute validation logic"""
        pass
```

### Key Attributes Available

| Attribute | Type | Description |
|-----------|------|-------------|
| `self.name` | str | Validation name from config |
| `self.severity` | str | "ERROR" or "WARNING" |
| `self.params` | dict | Parameters from YAML config |
| `self.condition` | str | Optional condition expression |

### Helper Methods Available

| Method | Purpose |
|--------|---------|
| `_evaluate_condition(df)` | Evaluate condition on DataFrame |
| `_create_result(...)` | Create ValidationResult object |
| `_format_failures(rows, chunk)` | Format failure samples |

---

## Step-by-Step Tutorial

### Step 1: Create the Validation File

Create: `validation_framework/validations/custom/revenue_threshold.py`

```python
"""
Revenue threshold validation for high-value accounts.

Validates that platinum tier customers have minimum revenue.
"""
from typing import Iterator, Dict, Any
import pandas as pd
from validation_framework.validations.base import DataValidationRule, ValidationResult
import logging

logger = logging.getLogger(__name__)


class RevenueThresholdCheck(DataValidationRule):
    """
    Validates revenue meets minimum threshold.

    Configuration:
        params:
            revenue_field (str): Name of revenue field
            min_revenue (float): Minimum acceptable revenue

    Example:
        - type: "RevenueThresholdCheck"
          severity: "WARNING"
          params:
            revenue_field: "annual_revenue"
            min_revenue: 1000000
          condition: "customer_tier == 'PLATINUM'"
    """

    def get_description(self) -> str:
        """Human-readable description."""
        field = self.params.get("revenue_field", "revenue")
        min_rev = self.params.get("min_revenue", 0)
        return f"Validates {field} >= ${min_rev:,.0f}"
```

### Step 2: Implement the validate() Method

```python
    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Execute validation logic.

        Args:
            data_iterator: Iterator yielding DataFrame chunks
            context: Validation context (file info, settings)

        Returns:
            ValidationResult with pass/fail status
        """
        # Extract parameters
        revenue_field = self.params.get("revenue_field", "revenue")
        min_revenue = self.params.get("min_revenue", 0)

        # Validate parameters
        if not revenue_field:
            return self._create_result(
                passed=False,
                message="Missing required parameter 'revenue_field'",
                failed_count=1
            )

        # Initialize tracking
        total_rows = 0
        failed_rows = []
        max_samples = context.get("max_sample_failures", 100)

        # Process each chunk
        for chunk_idx, chunk in enumerate(data_iterator):
            logger.debug(f"Processing chunk {chunk_idx}")

            # Verify field exists
            if revenue_field not in chunk.columns:
                return self._create_result(
                    passed=False,
                    message=f"Field '{revenue_field}' not found",
                    failed_count=1
                )

            # Apply condition if specified
            if self.condition:
                condition_mask = self._evaluate_condition(chunk)
                rows_to_check = chunk[condition_mask]
            else:
                rows_to_check = chunk

            # Find rows below threshold
            below_threshold = rows_to_check[
                rows_to_check[revenue_field] < min_revenue
            ]

            # Collect failures (limit to max_samples)
            if len(below_threshold) > 0:
                for idx, row in below_threshold.iterrows():
                    if len(failed_rows) >= max_samples:
                        break

                    failed_rows.append({
                        "row": int(total_rows + idx),
                        "revenue_field": revenue_field,
                        "actual_revenue": float(row[revenue_field]),
                        "min_revenue": min_revenue,
                        "issue": f"Revenue ${row[revenue_field]:,.0f} < ${min_revenue:,.0f}"
                    })

            total_rows += len(chunk)

        # Create result
        passed = len(failed_rows) == 0

        if passed:
            message = f"All rows have {revenue_field} >= ${min_revenue:,.0f}"
        else:
            message = (
                f"Found {len(failed_rows)} rows with {revenue_field} "
                f"below ${min_revenue:,.0f}"
            )

        return self._create_result(
            passed=passed,
            message=message,
            failed_count=len(failed_rows),
            total_count=total_rows,
            sample_failures=failed_rows
        )
```

### Step 3: Register the Validation

Add to `validation_framework/validations/custom/__init__.py`:

```python
from validation_framework.core.registry import register_validation
from .revenue_threshold import RevenueThresholdCheck

# Register custom validation
register_validation("RevenueThresholdCheck", RevenueThresholdCheck)
```

### Step 4: Use in Configuration

```yaml
validation_job:
  name: "Account Validation"

files:
  - name: "accounts"
    path: "accounts.csv"
    format: "csv"
    validations:
      # Use your custom validation!
      - type: "RevenueThresholdCheck"
        severity: "WARNING"
        params:
          revenue_field: "annual_revenue"
          min_revenue: 1000000
        condition: "customer_tier == 'PLATINUM'"
```

---

## Advanced Features

### 1. Conditional Logic

Your validation automatically supports conditions:

```yaml
- type: "RevenueThresholdCheck"
  severity: "ERROR"
  params:
    revenue_field: "revenue"
    min_revenue: 500000
  condition: "account_type == 'ENTERPRISE' AND region == 'US'"
```

**In your code:**
```python
# Condition is automatically evaluated
if self.condition:
    condition_mask = self._evaluate_condition(chunk)
    rows_to_check = chunk[condition_mask]
else:
    rows_to_check = chunk
```

### 2. Multiple Field Validation

Validate relationships between fields:

```python
class PricingConsistencyCheck(DataValidationRule):
    """Validates list_price >= sale_price"""

    def validate(self, data_iterator, context):
        failures = []

        for chunk in data_iterator:
            # Find inconsistent pricing
            invalid_pricing = chunk[
                chunk['sale_price'] > chunk['list_price']
            ]

            for idx, row in invalid_pricing.iterrows():
                failures.append({
                    "row": idx,
                    "list_price": row['list_price'],
                    "sale_price": row['sale_price'],
                    "issue": f"Sale price ${row['sale_price']} > list price ${row['list_price']}"
                })

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Found {len(failures)} pricing inconsistencies",
            failed_count=len(failures),
            sample_failures=failures[:100]
        )
```

### 3. Statistical Validation

Perform statistical analysis:

```python
class OutlierDetectionCheck(DataValidationRule):
    """Detects outliers using IQR method"""

    def validate(self, data_iterator, context):
        field = self.params.get("field")
        multiplier = self.params.get("iqr_multiplier", 1.5)

        # Collect all values first
        all_values = []
        for chunk in data_iterator:
            all_values.extend(chunk[field].dropna().tolist())

        # Calculate IQR
        q1 = pd.Series(all_values).quantile(0.25)
        q3 = pd.Series(all_values).quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - (multiplier * iqr)
        upper_bound = q3 + (multiplier * iqr)

        # Find outliers
        outliers = [v for v in all_values if v < lower_bound or v > upper_bound]

        return self._create_result(
            passed=len(outliers) == 0,
            message=f"Found {len(outliers)} outliers (IQR method)",
            failed_count=len(outliers),
            sample_failures=[{"value": v} for v in outliers[:100]]
        )
```

### 4. Cross-File Validation

Access other files during validation:

```python
class CrossFileReferenceCheck(DataValidationRule):
    """Validates references exist in another file"""

    def validate(self, data_iterator, context):
        # Load reference data
        reference_file = self.params.get("reference_file")
        reference_key = self.params.get("reference_key")
        foreign_key = self.params.get("foreign_key")

        # Read reference file
        ref_data = pd.read_csv(reference_file)
        valid_keys = set(ref_data[reference_key].unique())

        # Check foreign keys
        failures = []
        for chunk in data_iterator:
            invalid_refs = chunk[
                ~chunk[foreign_key].isin(valid_keys)
            ]

            failures.extend(invalid_refs.to_dict('records'))

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Found {len(failures)} invalid references",
            failed_count=len(failures),
            sample_failures=failures[:100]
        )
```

---

## Testing Custom Validations

### Unit Test Template

Create `tests/test_custom_validations.py`:

```python
import pytest
import pandas as pd
from validation_framework.validations.custom.revenue_threshold import RevenueThresholdCheck


def test_revenue_threshold_passes():
    """Test RevenueThresholdCheck with valid data"""
    validation = RevenueThresholdCheck(
        name="test",
        severity="ERROR",
        params={
            "revenue_field": "revenue",
            "min_revenue": 100000
        }
    )

    # Create test data - all above threshold
    data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "revenue": [150000, 200000, 180000]
    })

    # Execute validation
    result = validation.validate(iter([data]), context={})

    # Assert passed
    assert result.passed is True
    assert result.failed_count == 0


def test_revenue_threshold_fails():
    """Test RevenueThresholdCheck with invalid data"""
    validation = RevenueThresholdCheck(
        name="test",
        severity="ERROR",
        params={
            "revenue_field": "revenue",
            "min_revenue": 100000
        }
    )

    # Create test data - one below threshold
    data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "revenue": [150000, 80000, 200000]  # row 2 is below
    })

    # Execute validation
    result = validation.validate(iter([data]), context={})

    # Assert failed
    assert result.passed is False
    assert result.failed_count == 1
    assert len(result.sample_failures) == 1


def test_revenue_threshold_with_condition():
    """Test RevenueThresholdCheck with conditional logic"""
    validation = RevenueThresholdCheck(
        name="test",
        severity="ERROR",
        params={
            "revenue_field": "revenue",
            "min_revenue": 1000000
        },
        condition="tier == 'PLATINUM'"
    )

    # Create test data
    data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "revenue": [500000, 1500000, 600000],
        "tier": ["GOLD", "PLATINUM", "SILVER"]
    })

    # Execute validation
    result = validation.validate(iter([data]), context={})

    # Assert passed (only PLATINUM checked, and it passes)
    assert result.passed is True


def test_revenue_threshold_missing_field():
    """Test RevenueThresholdCheck with missing field"""
    validation = RevenueThresholdCheck(
        name="test",
        severity="ERROR",
        params={
            "revenue_field": "nonexistent_field",
            "min_revenue": 100000
        }
    )

    data = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "revenue": [150000, 200000, 180000]
    })

    # Execute validation
    result = validation.validate(iter([data]), context={})

    # Assert failed with error message
    assert result.passed is False
    assert "not found" in result.message.lower()
```

### Run Tests

```bash
# Run all tests
pytest tests/test_custom_validations.py

# Run with coverage
pytest tests/test_custom_validations.py --cov=validation_framework

# Run specific test
pytest tests/test_custom_validations.py::test_revenue_threshold_passes -v
```

---

## Real-World Examples

### Example 1: Business Hours Validation

Ensure timestamps fall within business hours:

```python
class BusinessHoursCheck(DataValidationRule):
    """Validates timestamps are within business hours"""

    def get_description(self):
        return "Validates timestamps are within business hours (9 AM - 5 PM)"

    def validate(self, data_iterator, context):
        timestamp_field = self.params.get("timestamp_field", "created_at")
        failures = []

        for chunk in data_iterator:
            # Convert to datetime
            chunk[timestamp_field] = pd.to_datetime(chunk[timestamp_field])

            # Extract hour
            chunk['hour'] = chunk[timestamp_field].dt.hour

            # Find outside business hours (9 AM - 5 PM)
            outside_hours = chunk[
                (chunk['hour'] < 9) | (chunk['hour'] >= 17)
            ]

            for idx, row in outside_hours.iterrows():
                failures.append({
                    "row": idx,
                    "timestamp": str(row[timestamp_field]),
                    "hour": row['hour'],
                    "issue": f"Created at {row['hour']:02d}:00 (outside 9AM-5PM)"
                })

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Found {len(failures)} records outside business hours",
            failed_count=len(failures),
            sample_failures=failures[:100]
        )

register_validation("BusinessHoursCheck", BusinessHoursCheck)
```

### Example 2: Fraud Detection

Flag suspicious transaction patterns:

```python
class FraudPatternCheck(DataValidationRule):
    """Detects suspicious transaction patterns"""

    def get_description(self):
        return "Detects suspicious transaction patterns"

    def validate(self, data_iterator, context):
        amount_field = self.params.get("amount_field", "amount")
        velocity_threshold = self.params.get("velocity_threshold", 10)
        amount_threshold = self.params.get("amount_threshold", 10000)

        suspicious = []

        # Group by account and count transactions
        all_chunks = []
        for chunk in data_iterator:
            all_chunks.append(chunk)

        full_data = pd.concat(all_chunks, ignore_index=True)

        # Pattern 1: High velocity (many transactions)
        account_counts = full_data.groupby('account_id').size()
        high_velocity_accounts = account_counts[
            account_counts > velocity_threshold
        ].index.tolist()

        # Pattern 2: High amounts
        high_amounts = full_data[
            full_data[amount_field] > amount_threshold
        ]

        # Combine patterns
        for idx, row in full_data.iterrows():
            flags = []

            if row['account_id'] in high_velocity_accounts:
                flags.append(f"High velocity ({account_counts[row['account_id']]} txns)")

            if row[amount_field] > amount_threshold:
                flags.append(f"High amount (${row[amount_field]:,.2f})")

            if flags:
                suspicious.append({
                    "row": idx,
                    "account_id": row['account_id'],
                    "amount": row[amount_field],
                    "flags": ", ".join(flags)
                })

        return self._create_result(
            passed=len(suspicious) == 0,
            message=f"Found {len(suspicious)} suspicious transactions",
            failed_count=len(suspicious),
            sample_failures=suspicious[:100]
        )

register_validation("FraudPatternCheck", FraudPatternCheck)
```

### Example 3: Data Completeness Score

Calculate completeness percentage:

```python
class CompletenessScoreCheck(DataValidationRule):
    """Validates data completeness percentage"""

    def get_description(self):
        min_score = self.params.get("min_completeness_pct", 80)
        return f"Validates completeness >= {min_score}%"

    def validate(self, data_iterator, context):
        required_fields = self.params.get("required_fields", [])
        min_completeness_pct = self.params.get("min_completeness_pct", 80)

        total_rows = 0
        field_completeness = {field: 0 for field in required_fields}

        for chunk in data_iterator:
            total_rows += len(chunk)

            for field in required_fields:
                if field in chunk.columns:
                    non_null_count = chunk[field].notna().sum()
                    field_completeness[field] += non_null_count

        # Calculate percentages
        results = {}
        failures = []

        for field in required_fields:
            pct = (field_completeness[field] / total_rows * 100) if total_rows > 0 else 0
            results[field] = pct

            if pct < min_completeness_pct:
                failures.append({
                    "field": field,
                    "completeness_pct": round(pct, 2),
                    "min_required_pct": min_completeness_pct,
                    "issue": f"{field} only {pct:.1f}% complete (need {min_completeness_pct}%)"
                })

        passed = len(failures) == 0

        if passed:
            message = f"All fields meet {min_completeness_pct}% completeness"
        else:
            message = f"{len(failures)} fields below {min_completeness_pct}% completeness"

        return self._create_result(
            passed=passed,
            message=message,
            failed_count=len(failures),
            sample_failures=failures,
            metadata={"field_completeness": results}
        )

register_validation("CompletenessScoreCheck", CompletenessScoreCheck)
```

---

## Best Practices

### 1. Parameter Validation

Always validate parameters at the start:

```python
def validate(self, data_iterator, context):
    # Validate required parameters
    required_field = self.params.get("field")
    if not required_field:
        return self._create_result(
            passed=False,
            message="Missing required parameter 'field'",
            failed_count=1
        )
```

### 2. Field Existence Checks

Check fields exist before using them:

```python
for chunk in data_iterator:
    if field_name not in chunk.columns:
        return self._create_result(
            passed=False,
            message=f"Field '{field_name}' not found in data",
            failed_count=1
        )
```

### 3. Memory Efficiency

Respect the chunked processing pattern:

```python
# Good: Process chunk by chunk
for chunk in data_iterator:
    process_chunk(chunk)

# Avoid: Loading all data at once (only if absolutely necessary)
all_data = pd.concat([chunk for chunk in data_iterator])
```

### 4. Limit Sample Failures

Always limit the number of failures collected:

```python
max_samples = context.get("max_sample_failures", 100)
failures = []

for chunk in data_iterator:
    invalid_rows = chunk[chunk['value'] < threshold]

    for idx, row in invalid_rows.iterrows():
        if len(failures) >= max_samples:
            break  # Stop collecting
        failures.append({...})
```

### 5. Logging

Use logging for debugging:

```python
import logging
logger = logging.getLogger(__name__)

def validate(self, data_iterator, context):
    logger.debug("Starting validation")
    logger.info(f"Processing {context.get('file_name')}")
```

### 6. Error Handling

Handle exceptions gracefully:

```python
def validate(self, data_iterator, context):
    try:
        # Validation logic
        pass
    except Exception as e:
        logger.exception("Validation failed with error")
        return self._create_result(
            passed=False,
            message=f"Validation error: {str(e)}",
            failed_count=1
        )
```

### 7. Clear Messages

Provide clear, actionable messages:

```python
# Good
message = f"Found {count} emails with invalid domains. Check approved_domains list."

# Avoid
message = "Validation failed"
```

---

## Next Steps

- **[Custom Loaders](custom-loaders.md)** - Add support for new file formats
- **[Custom Reporters](custom-reporters.md)** - Create custom report formats
- **[API Reference](api-reference.md)** - Complete Python API documentation
- **[Testing Guide](testing-guide.md)** - Comprehensive testing strategies
- **[Contributing](contributing.md)** - Contribute your validations to DataK9

---

**🐕 Train DataK9 with your custom validations - teach the K9 new tricks for data quality!**
