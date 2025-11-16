# DataK9 Profiler Enhanced Features

**Last Updated:** November 2025
**Author:** Daniel Edge

## Overview

The DataK9 profiler has been significantly enhanced with advanced data science capabilities that provide deep insights into data quality, patterns, and relationships. These enhancements enable the profiler to generate more intelligent and comprehensive validation recommendations automatically.

## Table of Contents

1. [Distribution Analysis](#distribution-analysis)
2. [Anomaly Detection](#anomaly-detection)
3. [Temporal Pattern Analysis](#temporal-pattern-analysis)
4. [Enhanced Pattern Detection](#enhanced-pattern-detection)
5. [Functional Dependency Discovery](#functional-dependency-discovery)
6. [Enhanced Validation Suggestions](#enhanced-validation-suggestions)
7. [Usage Examples](#usage-examples)
8. [API Reference](#api-reference)

---

## Distribution Analysis

### Overview

For numeric columns, the profiler now performs comprehensive statistical distribution analysis to understand data characteristics and detect outliers.

### Features

- **Distribution Classification:** Automatically identifies distribution types
  - Normal (Gaussian)
  - Uniform
  - Right-skewed / Left-skewed
  - Heavy-tailed / Light-tailed
  - Moderately skewed

- **Statistical Metrics:**
  - **Skewness:** Measure of asymmetry (-∞ to +∞, 0 = symmetric)
  - **Kurtosis:** Measure of tailedness (-∞ to +∞, 3 = normal)
  - **Advanced Percentiles:** P1, P5, P95, P99 for robust statistics

- **Outlier Detection:**
  - **IQR Method:** Identifies outliers using Interquartile Range (Q1 - 1.5×IQR, Q3 + 1.5×IQR)
  - **Z-Score Method:** Flags values with |z-score| > 3
  - **Combined Analysis:** Provides count and percentage of outliers

### Output Example

```json
{
  "distribution": {
    "distribution_type": "normal",
    "skewness": 0.123,
    "kurtosis": 2.987,
    "is_normal": true,
    "outlier_count": 5,
    "outlier_percentage": 0.5,
    "outliers_iqr_sample": [1250.5, 1280.3, 1290.7],
    "outliers_zscore_sample": [1285.2, 1292.1],
    "percentile_95": 1150.25,
    "percentile_99": 1210.50,
    "percentile_1": 850.10,
    "percentile_5": 890.75
  }
}
```

### Validation Recommendations

When outliers are detected (>5% of data), the profiler automatically suggests:

```yaml
- type: StatisticalOutlierCheck
  severity: WARNING
  params:
    field: amount
    method: iqr
    threshold: 1.5
  # Detected 50 outliers (5.2%) - distribution is right_skewed
```

When percentile data is available, range checks use P1-P99 instead of min/max for more robust thresholds:

```yaml
- type: RangeCheck
  severity: WARNING
  params:
    field: amount
    min_value: 850.10  # P1
    max_value: 1210.50  # P99
  # Values typically range from P1 (850.10) to P99 (1210.50)
```

---

## Anomaly Detection

### Overview

Multi-method anomaly detection identifies unusual values, patterns, and data quality issues across all column types.

### Detection Methods

1. **Statistical Outliers (Numeric)**
   - Modified Z-score using Median Absolute Deviation (MAD)
   - More robust than standard Z-score, less sensitive to extreme outliers
   - Threshold: |modified z-score| > 3.5

2. **Length Anomalies (String)**
   - Detects strings with unusual lengths
   - Uses IQR method with 3× threshold for aggressive detection
   - Identifies both too-short and too-long values

3. **Type Inconsistency**
   - Flags minority type values (<5% of data)
   - Identifies data entry errors and format inconsistencies

4. **Pattern Violations**
   - Detects values that don't match dominant pattern (>50% prevalence)
   - Useful for identifying malformed entries

### Output Example

```json
{
  "anomalies": {
    "has_anomalies": true,
    "anomaly_count": 12,
    "anomaly_percentage": 1.2,
    "anomaly_methods": ["statistical_outlier", "length_outlier"],
    "anomaly_samples": ["value1", "value2", "value3"],
    "anomaly_details": [
      {
        "value": 1285.50,
        "modified_z_score": 3.8,
        "method": "modified_z_score"
      },
      {
        "value": "ABC",
        "length": 3,
        "expected_range": "10-15",
        "method": "length_outlier"
      }
    ]
  }
}
```

### Validation Recommendations

When anomalies exceed 2% of data:

```yaml
- type: CompletenessCheck
  severity: WARNING
  params:
    field: customer_code
    min_percentage: 98.0
  # Detected 45 anomalies (2.3%) using methods: statistical_outlier, pattern_violation
```

---

## Temporal Pattern Analysis

### Overview

For date/time columns, comprehensive temporal analysis detects patterns, gaps, freshness issues, and data quality problems.

### Features

- **Date Range Analysis:**
  - Earliest and latest dates
  - Total date range in days
  - Average interval between dates

- **Gap Detection:**
  - Identifies missing dates in sequences
  - Detects gaps larger than 2× expected interval
  - Reports gap count and largest gap size

- **Freshness Monitoring:**
  - Checks if latest date is recent (within 30 days)
  - Reports days since latest entry
  - Flags stale data

- **Future Date Detection:**
  - Identifies dates in the future (data entry errors)
  - Counts future date instances
  - Critical for data quality validation

- **Pattern Recognition:**
  - Daily, Weekly, Monthly, Quarterly, Yearly
  - Regular vs. Irregular intervals
  - Based on interval consistency analysis

### Output Example

```json
{
  "temporal": {
    "earliest_date": "2024-01-01",
    "latest_date": "2024-10-15",
    "date_range_days": 288,
    "has_gaps": true,
    "gap_count": 3,
    "largest_gap_days": 14,
    "is_fresh": false,
    "days_since_latest": 45,
    "has_future_dates": true,
    "future_date_count": 2,
    "temporal_pattern": "daily",
    "avg_interval_days": 1.05
  }
}
```

### Validation Recommendations

**For stale data:**

```yaml
- type: FreshnessCheck
  severity: WARNING
  params:
    field: transaction_date
    max_age_days: 30
  # Latest date is 45 days old - data may be stale
```

**For future dates (data quality issue):**

```yaml
- type: RangeCheck
  severity: ERROR
  params:
    field: transaction_date
    max_value: "2025-11-15"  # Today's date
  # Detected 2 future dates - possible data entry error
```

---

## Enhanced Pattern Detection

### Overview

Advanced pattern recognition identifies semantic types, detects PII (Personally Identifiable Information), and discovers regex patterns automatically.

### Semantic Type Detection

Automatically recognizes common data types:

- **Email Addresses** - `user@example.com`
- **Phone Numbers** (US and International) - `(555) 123-4567`, `+1-555-123-4567`
- **Social Security Numbers** - `123-45-6789`
- **Credit Card Numbers** - `1234-5678-9012-3456`
- **ZIP Codes** (US) - `12345`, `12345-6789`
- **URLs** - `https://example.com`
- **IPv4 Addresses** - `192.168.1.1`
- **UUIDs** - `550e8400-e29b-41d4-a716-446655440000`
- **Currency Values** - `$1,234.56`

### PII Detection

Automatically flags columns containing Personally Identifiable Information:

- Email addresses
- Phone numbers
- Social Security Numbers
- Credit card numbers

**Security Note:** When PII is detected, consider additional validation and encryption requirements.

### Regex Pattern Discovery

Automatically generates regex patterns from data:

- Analyzes sample values for consistent patterns
- Converts pattern templates (A=letter, 9=digit) to regex
- Examples:
  - `AAA-999` → `^[a-zA-Z]{3}-\d{3}$`
  - `999.99` → `^\d{3}\.\d{2}$`

### Output Example

```json
{
  "patterns": {
    "semantic_type": "email",
    "semantic_confidence": 98.5,
    "regex_pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
    "format_examples": ["user@example.com", "admin@test.org"],
    "pii_detected": true,
    "pii_types": ["email"]
  }
}
```

### Validation Recommendations

**For detected email addresses:**

```yaml
- type: RegexCheck
  severity: ERROR
  params:
    field: customer_email
    pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
  # Detected email addresses (99% confidence)
```

**For phone numbers:**

```yaml
- type: RegexCheck
  severity: WARNING
  params:
    field: phone
    pattern: '^\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}$'
  # Detected phone numbers (95% confidence)
```

**For discovered patterns:**

```yaml
- type: RegexCheck
  severity: WARNING
  params:
    field: product_code
    pattern: '^[A-Z]{3}-\d{5}$'
  # Detected consistent pattern: ABC-12345
```

**For fixed-length strings:**

```yaml
- type: StringLengthCheck
  severity: ERROR
  params:
    field: account_number
    exact_length: 12
  # Field has fixed length of 12 characters
```

---

## Functional Dependency Discovery

### Overview

Automatically discovers functional dependencies between columns (X → Y, where X determines Y), enabling detection of data integrity rules and relationships.

### Analysis Method

- Groups data by source column
- Counts unique target values per group
- Calculates dependency strength (% of groups with 1:1 mapping)
- Identifies strong dependencies (>95% strength)

### Use Cases

1. **Primary Key Detection:** Columns that uniquely determine others
2. **Foreign Key Relationships:** Referential integrity patterns
3. **Derived Columns:** Fields calculated from others
4. **Data Redundancy:** Unnecessary duplicate information
5. **Normalization Opportunities:** Database design improvements

### Output Example

```json
{
  "dependencies": {
    "depends_on": ["customer_id", "order_id"],
    "determines": ["shipping_address", "total_amount"],
    "dependency_strength": {
      "customer_id": 98.5,
      "order_id": 100.0,
      "customer_email->customer_name": 99.2
    },
    "is_determined_by": [
      ["customer_id"],
      ["order_id", "line_item_id"]
    ]
  }
}
```

### Validation Recommendations

When strong dependencies are found (>95% strength):

```yaml
- type: CrossFieldComparisonCheck
  severity: WARNING
  params:
    field1: customer_id
    field2: customer_name
    operation: determine
  # customer_id functionally determines customer_name (100% strength)
```

**Benefits:**

- Detects when one-to-one relationships are violated
- Identifies potential data integrity issues
- Suggests cross-field validations automatically
- Helps identify primary/foreign key candidates

---

## Enhanced Validation Suggestions

### Overview

The profiler now generates significantly more intelligent and context-aware validation suggestions by leveraging all enhanced analysis features.

### Suggestion Categories

#### 1. File-Level Validations

Always generated:

- **EmptyFileCheck** - Prevents empty file processing
- **RowCountRangeCheck** - Detects significant row count changes (50%-200% of profiled data)

#### 2. Numeric Column Validations

- **RangeCheck** - Uses P1/P99 percentiles for robust ranges (not min/max)
- **StatisticalOutlierCheck** - When >5% outliers detected
- **NumericPrecisionCheck** - For decimal precision requirements

#### 3. String Column Validations

- **ValidValuesCheck** - Low cardinality fields (<20 unique values, <5% cardinality)
- **StringLengthCheck** - Fixed-length or range-based length validation
- **RegexCheck** - Pattern-based validation for emails, phones, codes, etc.

#### 4. Date/Time Validations

- **DateFormatCheck** - Auto-detected date formats
- **FreshnessCheck** - For stale data detection
- **RangeCheck** - Prevents future dates (data entry errors)

#### 5. Uniqueness Validations

- **UniqueKeyCheck** - For fields with >99% unique values

#### 6. Completeness Validations

- **MandatoryFieldCheck** - Fields with >95% completeness
- **CompletenessCheck** - When anomaly percentage is high

#### 7. Relationship Validations

- **CrossFieldComparisonCheck** - For functional dependencies

### Confidence Scores

All suggestions include confidence scores (0-100):

- **95-100%** - High confidence (should be implemented)
- **85-94%** - Good confidence (review parameters)
- **75-84%** - Moderate confidence (consider context)
- **<75%** - Lower confidence (validate before use)

Suggestions are sorted by confidence (highest first).

### Example Output

```yaml
# Top 10 auto-generated validations (sorted by confidence)

- type: EmptyFileCheck
  severity: ERROR
  params: {}
  # Prevent empty file loads
  confidence: 100%

- type: RegexCheck
  severity: ERROR
  params:
    field: email
    pattern: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
  # Detected email addresses (100% confidence)
  confidence: 100%

- type: UniqueKeyCheck
  severity: ERROR
  params:
    fields: [customer_id]
  # Field appears to be a unique identifier (99%+ unique values)
  confidence: 95%

- type: MandatoryFieldCheck
  severity: ERROR
  params:
    fields: [customer_name, email, signup_date]
  # 3 fields have >95% completeness
  confidence: 95%

- type: RangeCheck
  severity: WARNING
  params:
    field: amount
    min_value: 150.25  # P1
    max_value: 1850.75  # P99
  # Values typically range from P1 (150.25) to P99 (1850.75)
  confidence: 92%

- type: StatisticalOutlierCheck
  severity: WARNING
  params:
    field: transaction_amount
    method: iqr
    threshold: 1.5
  # Detected 125 outliers (6.3%) - distribution is right_skewed
  confidence: 90%

- type: ValidValuesCheck
  severity: ERROR
  params:
    field: status
    valid_values: [active, inactive, pending, suspended]
  # Low cardinality field with 4 unique values
  confidence: 90%

- type: DateFormatCheck
  severity: ERROR
  params:
    field: signup_date
    format: "%Y-%m-%d"
  # Detected date format: %Y-%m-%d
  confidence: 85%

- type: FreshnessCheck
  severity: WARNING
  params:
    field: last_login
    max_age_days: 30
  # Latest date is 45 days old - data may be stale
  confidence: 85%

- type: StringLengthCheck
  severity: ERROR
  params:
    field: account_code
    exact_length: 10
  # Field has fixed length of 10 characters
  confidence: 95%
```

---

## Usage Examples

### Basic Profiling with Enhanced Features

```python
from validation_framework.profiler import DataProfiler

# Create profiler
profiler = DataProfiler(chunk_size=50000)

# Profile file
result = profiler.profile_file(
    file_path="customers.csv",
    file_format="csv"
)

# Access enhanced features
for column in result.columns:
    print(f"\n=== {column.name} ===")

    # Distribution analysis (numeric columns)
    if column.distribution:
        print(f"Distribution: {column.distribution.distribution_type}")
        print(f"Skewness: {column.distribution.skewness:.2f}")
        print(f"Outliers: {column.distribution.outlier_count} ({column.distribution.outlier_percentage:.1f}%)")
        print(f"P95: {column.distribution.percentile_95:.2f}")

    # Anomaly detection (all columns)
    if column.anomalies and column.anomalies.has_anomalies:
        print(f"Anomalies: {column.anomalies.anomaly_count} detected")
        print(f"Methods: {', '.join(column.anomalies.anomaly_methods)}")

    # Temporal analysis (date columns)
    if column.temporal:
        print(f"Date range: {column.temporal.earliest_date} to {column.temporal.latest_date}")
        print(f"Pattern: {column.temporal.temporal_pattern}")
        if column.temporal.has_gaps:
            print(f"Gaps detected: {column.temporal.gap_count}")
        if column.temporal.has_future_dates:
            print(f"Future dates: {column.temporal.future_date_count}")

    # Pattern detection (string columns)
    if column.patterns and column.patterns.semantic_type:
        print(f"Semantic type: {column.patterns.semantic_type} ({column.patterns.semantic_confidence:.0f}%)")
        if column.patterns.pii_detected:
            print(f"⚠️  PII detected: {', '.join(column.patterns.pii_types)}")

    # Functional dependencies
    if column.dependencies:
        if column.dependencies.depends_on:
            print(f"Depends on: {', '.join(column.dependencies.depends_on)}")
        if column.dependencies.determines:
            print(f"Determines: {', '.join(column.dependencies.determines)}")

# Access validation suggestions
print(f"\n=== Validation Suggestions ({len(result.suggested_validations)}) ===")
for i, suggestion in enumerate(result.suggested_validations[:10], 1):
    print(f"{i}. {suggestion.validation_type} ({suggestion.confidence:.0f}%)")
    print(f"   {suggestion.reason}")
    print(f"   Params: {suggestion.params}")
```

### Export Profile Results to JSON

```python
import json

# Convert to dictionary
profile_dict = result.to_dict()

# Save to JSON file
with open('profile_results.json', 'w') as f:
    json.dump(profile_dict, f, indent=2)

# Import into DataK9 Studio
# 1. Click "Import Profile Results" in Studio
# 2. Upload profile_results.json
# 3. Review suggested validations
# 4. Select and apply validations to your config
```

### Generate Validation Config from Profile

```python
# Auto-generated YAML configuration
print(result.generated_config_yaml)

# Save to file
with open('auto_validation.yaml', 'w') as f:
    f.write(result.generated_config_yaml)

# Run validation with generated config
# python3 -m validation_framework.cli validate auto_validation.yaml
```

### CLI Usage

```bash
# Profile a file with JSON output
python3 -m validation_framework.cli profile customers.csv \
    -j profile_output.json \
    -o profile_report.html \
    -c auto_validation.yaml

# The JSON output includes all enhanced features
cat profile_output.json | jq '.columns[] | select(.patterns.pii_detected == true)'

# Import JSON into DataK9 Studio for visual review
```

---

## API Reference

### New Data Structures

#### DistributionMetrics

```python
@dataclass
class DistributionMetrics:
    distribution_type: Optional[str]  # normal, uniform, skewed, etc.
    skewness: Optional[float]         # Asymmetry measure
    kurtosis: Optional[float]         # Tailedness measure
    is_normal: Optional[bool]         # Normal distribution test
    outliers_iqr: List[float]         # Outliers by IQR method
    outliers_zscore: List[float]      # Outliers by Z-score method
    outlier_count: int                # Total outliers
    outlier_percentage: float         # Percentage outliers
    percentile_95: Optional[float]    # 95th percentile
    percentile_99: Optional[float]    # 99th percentile
    percentile_1: Optional[float]     # 1st percentile
    percentile_5: Optional[float]     # 5th percentile
```

#### AnomalyInfo

```python
@dataclass
class AnomalyInfo:
    has_anomalies: bool               # True if anomalies detected
    anomaly_count: int                # Number of anomalies
    anomaly_percentage: float         # Percentage anomalies
    anomaly_methods: List[str]        # Detection methods used
    anomaly_samples: List[Any]        # Sample anomalous values
    anomaly_details: List[Dict]       # Detailed anomaly info
```

#### TemporalMetrics

```python
@dataclass
class TemporalMetrics:
    earliest_date: Optional[str]      # Earliest date
    latest_date: Optional[str]        # Latest date
    date_range_days: Optional[int]    # Date range in days
    has_gaps: bool                    # Gaps detected
    gap_count: int                    # Number of gaps
    largest_gap_days: Optional[int]   # Largest gap size
    is_fresh: Optional[bool]          # Data freshness
    days_since_latest: Optional[int]  # Days since latest
    has_future_dates: bool            # Future dates detected
    future_date_count: int            # Number of future dates
    temporal_pattern: Optional[str]   # Detected pattern
    avg_interval_days: Optional[float] # Average interval
```

#### PatternInfo

```python
@dataclass
class PatternInfo:
    semantic_type: Optional[str]      # Detected type (email, phone, etc.)
    semantic_confidence: float        # Confidence (0-100)
    regex_pattern: Optional[str]      # Discovered regex pattern
    format_examples: List[str]        # Example formats
    pii_detected: bool                # PII flag
    pii_types: List[str]              # Types of PII
```

#### DependencyInfo

```python
@dataclass
class DependencyInfo:
    depends_on: List[str]             # Columns this depends on
    determines: List[str]             # Columns this determines
    dependency_strength: Dict[str, float]  # Dependency strengths
    is_determined_by: List[List[str]] # Column groups that determine this
```

### Analysis Utilities

#### DistributionAnalyzer

```python
from validation_framework.profiler.analysis_utils import DistributionAnalyzer

# Analyze numeric distribution
distribution = DistributionAnalyzer.analyze(
    numeric_values=[1.0, 2.0, 3.0, ...],
    total_count=1000
)
```

#### AnomalyDetector

```python
from validation_framework.profiler.analysis_utils import AnomalyDetector

# Detect anomalies
anomalies = AnomalyDetector.detect(
    column_data=profile_data,
    inferred_type="float",
    statistics=column_statistics
)
```

#### TemporalAnalyzer

```python
from validation_framework.profiler.analysis_utils import TemporalAnalyzer

# Analyze temporal patterns
temporal = TemporalAnalyzer.analyze(
    column_data=profile_data,
    sample_values=date_samples
)
```

#### PatternDetector

```python
from validation_framework.profiler.analysis_utils import PatternDetector

# Detect patterns and semantic types
patterns = PatternDetector.detect(
    sample_values=sample_strings,
    patterns=pattern_frequency_dict
)
```

#### DependencyDiscoverer

```python
from validation_framework.profiler.analysis_utils import DependencyDiscoverer

# Discover functional dependencies
dependencies = DependencyDiscoverer.discover(
    columns=column_list,
    data_sample=sample_dataframe
)
```

---

## Performance Considerations

### Memory Usage

- **Distribution Analysis:** Minimal overhead, O(n) memory for numeric values
- **Anomaly Detection:** Processes in-place, minimal additional memory
- **Temporal Analysis:** Parses sample dates only (~100 samples)
- **Pattern Detection:** Analyzes samples, not full dataset
- **Dependency Discovery:** Uses first chunk only (10,000 rows max)

### Processing Time

For a 1 GB CSV file with 10 columns:

- **Without Enhanced Features:** ~2 minutes
- **With Enhanced Features:** ~2.5 minutes (+25% overhead)

The overhead is minimal because most analysis uses already-collected data.

### Optimization Tips

1. **Adjust chunk_size** for memory/speed tradeoff (default: 50,000)
2. **Sample size** for dependencies auto-limited to 10,000 rows
3. **Pattern detection** uses first 100 samples per column
4. **Correlation analysis** limited to first 20 numeric columns

---

## Troubleshooting

### Common Issues

**Issue:** Semantic type detection is incorrect

**Solution:** Semantic types require >80% confidence. If misdetected, the regex pattern may still be useful. Check `semantic_confidence` score.

**Issue:** Too many validation suggestions generated

**Solution:** Filter by confidence score. Focus on suggestions with >90% confidence.

**Issue:** Functional dependencies not detected

**Solution:** Dependencies require >95% strength in first 10,000 rows. Ensure sample is representative.

**Issue:** Distribution classified as "unknown"

**Solution:** Requires numeric data with sufficient variety. Check for constant values or very few unique values.

---

## Future Enhancements

Planned features for future releases:

1. **Data Drift Detection:** Compare profiles over time to detect schema/distribution changes
2. **Advanced Anomaly Detection:** Isolation Forest, DBSCAN clustering methods
3. **Multivariate Analysis:** Detect relationships between multiple columns
4. **Seasonality Detection:** Time-series seasonal pattern recognition
5. **Correlation Networks:** Visualize relationships between all columns
6. **Bayesian Inference:** Probabilistic type and pattern detection
7. **Custom Pattern Libraries:** User-defined semantic type patterns
8. **Profile Comparison:** Side-by-side comparison of multiple profiles

---

## References

- **Statistical Methods:** Using scipy.stats, numpy statistical functions
- **Outlier Detection:** IQR method (Tukey, 1977), Modified Z-score (Iglewicz & Hoaglin, 1993)
- **Pattern Detection:** Regular expressions, semantic type inference
- **Functional Dependencies:** Relational database theory (Codd, 1970)

---

**Need Help?**

- Report issues: https://github.com/your-repo/data-validation-tool/issues
- Documentation: See docs/ folder
- Examples: See examples/ folder

---

**Document Version:** 1.0
**DataK9 Version:** Compatible with DataK9 v2.0+
