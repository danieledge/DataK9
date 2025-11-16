# DataK9 Sampling Quick Reference Card

## TL;DR - Copy-Paste Solutions

### Sample Size Cheat Sheet

```python
# Standard sample sizes for different confidence levels
SAMPLE_SIZES = {
    "critical": 1_000_000,    # 99.9% confidence, financial/compliance
    "high": 385_000,          # 95% confidence, ±0.5% margin (RECOMMENDED)
    "medium": 100_000,        # 90% confidence, ±1.0% margin
    "low": 10_000,            # Development/exploratory
}

# Use 385K for most validations - optimal balance of speed vs accuracy
```

### Formula Reference

```python
# Sample size for proportion estimation
def calculate_sample_size(confidence=0.95, margin_of_error=0.005):
    """
    Calculate required sample size.

    Args:
        confidence: Confidence level (0.90, 0.95, 0.99)
        margin_of_error: Margin of error (0.005 = ±0.5%)

    Returns:
        Required sample size
    """
    from scipy.stats import norm

    z = norm.ppf(1 - (1 - confidence) / 2)  # Z-score
    p = 0.5  # Maximum variance assumption

    n = (z**2 * p * (1-p)) / margin_of_error**2

    return int(np.ceil(n))

# Examples:
# calculate_sample_size(0.95, 0.005) → 38,416
# calculate_sample_size(0.99, 0.005) → 66,304
# calculate_sample_size(0.95, 0.01)  → 9,604
```

### Confidence Interval Calculation

```python
def calculate_confidence_interval(sample_proportion, sample_size, confidence=0.95):
    """
    Calculate confidence interval for a proportion.

    Args:
        sample_proportion: Proportion in sample (e.g., 0.187 for 18.7%)
        sample_size: Number of samples
        confidence: Confidence level

    Returns:
        (lower_bound, upper_bound)
    """
    from scipy.stats import norm

    z = norm.ppf(1 - (1 - confidence) / 2)
    se = np.sqrt(sample_proportion * (1 - sample_proportion) / sample_size)
    margin = z * se

    return (sample_proportion - margin, sample_proportion + margin)

# Example:
# CI for 18.7% outliers in 385K sample:
# calculate_confidence_interval(0.187, 385000, 0.95)
# → (0.1858, 0.1882) i.e., 18.58% - 18.82%
```

---

## Code Templates

### Template 1: Add Sampling to Validation

```python
from validation_framework.profiler.sampling_utils import ReservoirSampler
import numpy as np

class YourValidation(DataValidationRule):
    """Your validation with sampling support."""

    def validate(self, data_iterator, context):
        field = self.params.get("field")

        # Check if sampling enabled
        use_sampling = self.params.get("use_sampling", True)
        sample_size = self.params.get("sample_size", 385000)
        confidence = self.params.get("confidence_level", 0.95)

        if use_sampling:
            # STEP 1: Collect sample using reservoir sampler
            sampler = ReservoirSampler(reservoir_size=sample_size)
            total_rows = 0

            for chunk in data_iterator:
                if field in chunk.columns:
                    # Extract values and add to reservoir
                    values = chunk[field].dropna().tolist()
                    sampler.add_batch(values)
                    total_rows += len(chunk)

            # STEP 2: Get sample and perform calculation
            sample_values = np.array(sampler.get_sample())

            # Your validation logic on sample_values
            # e.g., calculate outliers, check ranges, etc.
            violations_in_sample = ...  # Your logic here

            # STEP 3: Calculate confidence interval
            sample_proportion = violations_in_sample / len(sample_values)
            z = 1.96  # 95% confidence
            se = np.sqrt(sample_proportion * (1 - sample_proportion) / len(sample_values))
            ci_lower = sample_proportion - z * se
            ci_upper = sample_proportion + z * se

            # STEP 4: Estimate total violations
            estimated_total = int(sample_proportion * total_rows)
            ci_lower_count = int(ci_lower * total_rows)
            ci_upper_count = int(ci_upper * total_rows)

            # STEP 5: Return result with metadata
            return self._create_result(
                passed=(sample_proportion < threshold),
                message=(
                    f"Estimated {estimated_total:,} violations "
                    f"({sample_proportion*100:.1f}%, "
                    f"95% CI: {ci_lower*100:.1f}%-{ci_upper*100:.1f}%) "
                    f"based on sample of {len(sample_values):,} from {total_rows:,} rows"
                ),
                failed_count=estimated_total,
                total_count=total_rows,
                metadata={
                    "sampling_used": True,
                    "sample_size": len(sample_values),
                    "total_rows": total_rows,
                    "violation_rate": sample_proportion,
                    "confidence_interval": [ci_lower, ci_upper],
                    "confidence_level": confidence
                }
            )
        else:
            # Original full-scan implementation
            # ... your existing code ...
```

### Template 2: Stratified Sampling

```python
from validation_framework.profiler.sampling_utils import StratifiedSampler

def validate_with_stratified_sampling(data_iterator, stratify_by_field,
                                       target_field, sample_size=385000):
    """
    Perform stratified sampling by a categorical field.

    Args:
        data_iterator: Data iterator
        stratify_by_field: Field to stratify on (e.g., "currency")
        target_field: Field to validate
        sample_size: Total sample size to collect
    """
    sampler = StratifiedSampler(
        max_per_category=sample_size // 10,  # Adjust based on expected categories
        max_total=sample_size
    )

    total_rows = 0
    for chunk in data_iterator:
        for idx, row in chunk.iterrows():
            category = row[stratify_by_field]
            value = row[target_field]
            sampler.add(value, category)
            total_rows += 1

    # Get all samples
    all_samples = sampler.get_sample()

    # Get samples by category
    category_samples = sampler.get_category_samples()

    return all_samples, category_samples, total_rows
```

### Template 3: Use Profile Statistics

```python
import json
from pathlib import Path

def validate_with_profile(profile_path, field_name, validation_type):
    """
    Use pre-calculated profile statistics for validation.

    Args:
        profile_path: Path to profile JSON
        field_name: Field to validate
        validation_type: "range" or "outlier"

    Returns:
        Validation bounds/thresholds
    """
    # Load profile
    with open(profile_path, 'r') as f:
        profile = json.load(f)

    # Extract field statistics
    field_stats = profile['fields'][field_name]

    if validation_type == "range":
        # Use P1/P99 for range bounds
        bounds = {
            "min": field_stats['percentiles']['p1'],
            "max": field_stats['percentiles']['p99']
        }
        return bounds

    elif validation_type == "outlier":
        # Use Q1/Q3 for IQR outlier detection
        q1 = field_stats['percentiles']['p25']
        q3 = field_stats['percentiles']['p75']
        iqr = q3 - q1

        bounds = {
            "lower": q1 - 1.5 * iqr,
            "upper": q3 + 1.5 * iqr,
            "q1": q1,
            "q3": q3,
            "iqr": iqr
        }
        return bounds

# Usage:
# bounds = validate_with_profile("profile.json", "Amount Received", "outlier")
# Then scan data for violations outside bounds
```

### Template 4: Early Stopping with SPRT

```python
def validate_with_early_stopping(data_iterator, field, threshold=0.001,
                                  alpha=0.05, beta=0.05):
    """
    Validate with early stopping using Sequential Probability Ratio Test.

    Args:
        data_iterator: Data iterator
        field: Field to validate
        threshold: Acceptable failure rate (e.g., 0.001 = 0.1%)
        alpha: Type I error (false positive rate)
        beta: Type II error (false negative rate)

    Returns:
        Validation result with early stopping information
    """
    p0 = threshold  # Acceptable failure rate
    p1 = threshold * 10  # Unacceptable failure rate (10x worse)

    # Calculate acceptance/rejection bounds
    accept_bound = beta / (1 - alpha)
    reject_bound = (1 - beta) / alpha

    total_checked = 0
    total_failures = 0
    stopped_early = False
    decision = None

    for chunk in data_iterator:
        if field not in chunk.columns:
            break

        # Check violations in this chunk
        violations = ...  # Your validation logic

        total_checked += len(chunk)
        total_failures += violations

        # Calculate likelihood ratio
        if total_failures > 0:
            likelihood_ratio = (
                (p1 / p0) ** total_failures *
                ((1 - p1) / (1 - p0)) ** (total_checked - total_failures)
            )

            # Check stopping criteria
            if likelihood_ratio <= accept_bound:
                stopped_early = True
                decision = "PASS"
                break
            elif likelihood_ratio >= reject_bound:
                stopped_early = True
                decision = "FAIL"
                break

    failure_rate = total_failures / total_checked if total_checked > 0 else 0

    return {
        "stopped_early": stopped_early,
        "decision": decision,
        "rows_checked": total_checked,
        "failures": total_failures,
        "failure_rate": failure_rate,
        "message": (
            f"Stopped early at {total_checked:,} rows "
            f"(decision: {decision}, failure rate: {failure_rate*100:.2f}%)"
            if stopped_early else
            f"Full scan completed: {total_checked:,} rows, {failure_rate*100:.2f}% failures"
        )
    }
```

---

## YAML Configuration Examples

### Example 1: Enable Sampling for All Validations

```yaml
validation_job:
  name: "Transaction Validation with Sampling"

  # Global sampling settings
  sampling_strategy: "stratified"
  sample_size: 385000
  confidence_level: 0.95

  files:
    - name: "transactions"
      path: "transaction_data.parquet"
      format: "parquet"

      validations:
        - type: "StatisticalOutlierCheck"
          severity: "WARNING"
          params:
            field: "Amount Received"
            method: "iqr"
            use_sampling: true  # Explicitly enable
```

### Example 2: Mixed Sampling Strategies

```yaml
validation_job:
  name: "Risk-Based Validation"

  files:
    - name: "transactions"
      path: "transaction_data.parquet"

      validations:
        # CRITICAL: Full scan, no sampling
        - type: "RangeCheck"
          severity: "ERROR"
          params:
            field: "Amount Received"
            min_value: 0
            use_sampling: false  # Full scan

        # HIGH: Large sample with high confidence
        - type: "StatisticalOutlierCheck"
          severity: "WARNING"
          params:
            field: "Amount Paid"
            method: "iqr"
            use_sampling: true
            sample_size: 385000
            confidence_level: 0.95

        # MEDIUM: Smaller sample
        - type: "StringLengthCheck"
          severity: "WARNING"
          params:
            field: "From Account"
            exact_length: 16
            use_sampling: true
            sample_size: 100000
            confidence_level: 0.90
```

### Example 3: Use Profile Statistics

```yaml
validation_job:
  name: "Profile-Based Validation"

  # Reference pre-calculated profile
  use_profile: "transaction_profile.json"
  sampling_strategy: "smart"

  files:
    - name: "transactions"
      path: "transaction_data.parquet"

      validations:
        # Range bounds from profile P1/P99
        - type: "RangeCheck"
          severity: "ERROR"
          params:
            field: "Amount Received"
            use_profile_bounds: true  # Use profile stats

        # Outlier detection from profile Q1/Q3
        - type: "StatisticalOutlierCheck"
          severity: "WARNING"
          params:
            field: "Amount Paid"
            use_profile_stats: true  # Use profile stats
```

---

## Decision Tree: Should You Sample?

```
START
  |
  ├─ Is this a uniqueness check? (e.g., UniqueFieldCheck)
  |    YES → Use FULL SCAN (cannot sample)
  |    NO  → Continue
  |
  ├─ Is this regulatory/compliance validation?
  |    YES → Use FULL SCAN (audit requirements)
  |    NO  → Continue
  |
  ├─ Is the violation rate < 0.01% (rare events)?
  |    YES → Use LARGE SAMPLE (1M rows) or FULL SCAN
  |    NO  → Continue
  |
  ├─ Is this a critical financial field?
  |    YES → Use LARGE SAMPLE (1M rows, 99% confidence)
  |    NO  → Continue
  |
  ├─ Is this a low-cardinality enumeration (<100 unique values)?
  |    YES → Use FULL SCAN (already fast)
  |    NO  → Continue
  |
  └─ DEFAULT → Use SAMPLING (385K rows, 95% confidence)
```

---

## Performance Benchmarks

### Expected Speedups by Validation Type

| Validation | Current Time | With Sampling | Speedup |
|------------|--------------|---------------|---------|
| StatisticalOutlierCheck | 120 sec | 2 sec | 60x |
| RangeCheck (bounds calc) | 30 sec | 0.5 sec | 60x |
| RangeCheck (violation scan) | 30 sec | 3 sec* | 10x |
| MandatoryFieldCheck | 40 sec | 0.3 sec | 140x |
| StringLengthCheck | 30 sec | 0.2 sec | 150x |
| ValidValuesCheck | 25 sec | 25 sec | 1x (no change) |

*With early stopping, can be 30-100x faster

### Overall Job Performance

**Baseline (Current):**
- 15 validations on 54.5M rows
- Runtime: 7-10 minutes

**Optimized (95% confidence sampling):**
- Same 15 validations on 385K sample
- Runtime: 45-60 seconds
- **Speedup: 8-10x**

**Optimized (With profile + early stopping):**
- Runtime: 20-30 seconds
- **Speedup: 15-20x**

---

## Common Pitfalls & Solutions

### Pitfall 1: Sample Too Small for Rare Events

**Problem:** Looking for violations that occur <0.01% of the time
**Symptom:** Wide confidence intervals, unreliable estimates
**Solution:**
```python
# For rare events, use larger sample
rare_event_sample_size = max(385000, int(1 / expected_rate * 1000))
# If event rate is 0.0001 (0.01%), sample = 10M rows
```

### Pitfall 2: Not Reporting Confidence Intervals

**Problem:** Users don't know estimate uncertainty
**Symptom:** Users treat estimates as exact counts
**Solution:**
```python
# Always report CI in message
message = (
    f"Estimated {estimated_count:,} violations "
    f"({rate*100:.1f}%, 95% CI: {ci_lower*100:.1f}%-{ci_upper*100:.1f}%)"
)
```

### Pitfall 3: Sampling with Replacement

**Problem:** Using random sampling with replacement
**Symptom:** Duplicate rows in sample, biased estimates
**Solution:**
```python
# Use ReservoirSampler (samples without replacement)
sampler = ReservoirSampler(reservoir_size=385000)
# NOT: random.choices() which samples with replacement
```

### Pitfall 4: Ignoring Stratification for Skewed Data

**Problem:** Simple random sampling on highly skewed categorical data
**Symptom:** Rare categories under-represented or missing
**Solution:**
```python
# Use StratifiedSampler for categorical fields
sampler = StratifiedSampler(max_per_category=10000, max_total=385000)
for chunk in data_iterator:
    for _, row in chunk.iterrows():
        sampler.add(row[target_field], row[category_field])
```

---

## Testing Your Sampling Implementation

### Unit Test Template

```python
import pytest
import numpy as np
from validation_framework.profiler.sampling_utils import ReservoirSampler

def test_reservoir_sampling_uniformity():
    """Test that reservoir sampler produces uniform distribution."""
    # Create 1M items
    population = list(range(1_000_000))
    sample_size = 10_000

    # Sample multiple times
    samples = []
    for _ in range(100):
        sampler = ReservoirSampler(reservoir_size=sample_size)
        for item in population:
            sampler.add(item)
        samples.append(np.mean(sampler.get_sample()))

    # Mean of samples should be close to population mean
    population_mean = np.mean(population)
    sample_means = np.array(samples)

    assert abs(np.mean(sample_means) - population_mean) / population_mean < 0.01

def test_confidence_interval_coverage():
    """Test that 95% CI has 95% coverage."""
    true_proportion = 0.3
    sample_size = 10000
    num_trials = 1000

    coverage_count = 0

    for _ in range(num_trials):
        # Simulate sample
        sample = np.random.binomial(1, true_proportion, sample_size)
        sample_prop = np.mean(sample)

        # Calculate CI
        z = 1.96
        se = np.sqrt(sample_prop * (1 - sample_prop) / sample_size)
        ci_lower = sample_prop - z * se
        ci_upper = sample_prop + z * se

        # Check if true proportion in CI
        if ci_lower <= true_proportion <= ci_upper:
            coverage_count += 1

    coverage_rate = coverage_count / num_trials

    # Should be approximately 95% (allow ±2%)
    assert 0.93 <= coverage_rate <= 0.97
```

---

## Monitoring & Logging

### Log Sampling Metadata

```python
import logging

logger = logging.getLogger(__name__)

def log_sampling_info(sampler, total_rows, field_name):
    """Log sampling information for debugging."""
    sample_size = sampler.get_sample_size()
    sampling_rate = sample_size / total_rows

    logger.info(
        f"Sampling stats for field '{field_name}': "
        f"sampled {sample_size:,} from {total_rows:,} rows "
        f"({sampling_rate*100:.2f}% sampling rate)"
    )
```

### Add Validation Metadata

```python
# Include in validation result
metadata = {
    "sampling_used": True,
    "sample_size": sample_size,
    "total_rows": total_rows,
    "sampling_rate": sample_size / total_rows,
    "confidence_level": 0.95,
    "margin_of_error": 0.005,
    "reservoir_algorithm": "Vitter's Algorithm R",
}
```

---

## Quick Reference: Statistical Formulas

### Sample Size (Proportion Estimation)
```
n = (Z² × p × (1-p)) / E²

Where:
  Z = 1.96 for 95% confidence
  p = 0.5 (maximum variance)
  E = 0.005 (±0.5% margin)

Result: n = 38,416
```

### Confidence Interval (Proportion)
```
CI = p ± Z × √(p(1-p)/n)

Example: 18.7% outliers in 385K sample
  CI = 0.187 ± 1.96 × √(0.187 × 0.813 / 385000)
  CI = 0.187 ± 0.0012
  CI = [18.58%, 18.82%]
```

### Standard Error (Percentile)
```
SE(p-th percentile) = √(p(1-p) / (n × f(F⁻¹(p))²))

For Q1 (p=0.25) with large n:
  SE ≈ √(0.25 × 0.75 / n)
  SE ≈ 0.433 / √n
```

### Early Stopping (SPRT)
```
Λ = (p₁/p₀)^k × ((1-p₁)/(1-p₀))^(n-k)

Accept if: Λ ≤ β/(1-α)
Reject if: Λ ≥ (1-β)/α

Where:
  k = failures observed
  n = samples checked
  α = 0.05 (Type I error)
  β = 0.05 (Type II error)
```

---

**Last Updated:** November 16, 2025
**DataK9 Version:** 1.0+
**Author:** Daniel Edge
