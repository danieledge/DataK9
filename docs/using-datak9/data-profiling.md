# Data Profiling Guide

**Know Your Data Before You Guard It**

DataK9's profiler analyzes your data files to understand their structure, quality, and characteristics. Like a K9 unit surveying the territory before patrol, profiling gives you the intelligence you need to build effective validations.

---

## Table of Contents

1. [What is Data Profiling?](#what-is-data-profiling)
2. [Quick Start](#quick-start)
3. [What Gets Analyzed](#what-gets-analyzed)
4. [Understanding Profile Reports](#understanding-profile-reports)
5. [Auto-Generated Validations](#auto-generated-validations)
6. [Quality Metrics Explained](#quality-metrics-explained)
7. [Type Inference](#type-inference)
8. [Profiling Large Files](#profiling-large-files)
9. [Best Practices](#best-practices)
10. [Command Reference](#command-reference)

---

## What is Data Profiling?

### Overview

Data profiling provides comprehensive analysis of your datasets:

- 📊 **Statistical Analysis** - Distributions, ranges, correlations
- 🔍 **Schema Discovery** - Automatic type detection with confidence levels
- ✅ **Quality Assessment** - Completeness, validity, uniqueness scores
- 🎯 **Pattern Detection** - Common patterns in string data
- 💡 **Validation Suggestions** - Recommended checks based on data
- ⚙️ **Config Generation** - Ready-to-use YAML configuration

### When to Use Profiling

**Before Building Validations:**
```
1. Profile your data
2. Understand patterns and quality
3. Build targeted validations
4. Deploy with confidence
```

**Use Cases:**

✅ **New Dataset** - Understand before validating
✅ **Documentation** - Generate data dictionaries
✅ **Data Analysis** - Discover patterns and anomalies
✅ **Quality Assessment** - Measure quality objectively
✅ **Troubleshooting** - Investigate quality issues
✅ **Migration** - Analyze source before transformation

**Example Workflow:**

```bash
# Step 1: Profile the data
python3 -m validation_framework.cli profile data/customers.csv

# Step 2: Review HTML report
open customers_profile_report.html

# Step 3: Use auto-generated config
python3 -m validation_framework.cli validate customers_validation.yaml

# 🐕 DataK9 is now guarding your data with data-driven rules!
```

---

## Quick Start

### 30-Second Example

```bash
# Profile a CSV file
python3 -m validation_framework.cli profile data/customers.csv

# DataK9 generates:
# ✅ customers_profile_report.html    (Interactive visual report)
# ✅ customers_validation.yaml         (Ready-to-use validation config)

# Open the report
open customers_profile_report.html
```

### What You Get

**1. Interactive HTML Report:**
- Beautiful dark theme
- Statistical summaries
- Interactive charts
- Quality scores
- Pattern analysis
- Correlation matrices

**2. Auto-Generated YAML Config:**
```yaml
validation_job:
  name: "Customers Data Validation"
  version: "1.0"

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"

    validations:
      # Auto-generated based on profiling results

      # File must not be empty
      - type: "EmptyFileCheck"
        severity: "ERROR"

      # Expected row count (based on profiled data)
      - type: "RowCountRangeCheck"
        severity: "WARNING"
        params:
          min_rows: 900      # 10% below profiled count
          max_rows: 1100     # 10% above profiled count

      # Mandatory fields (fields with >95% completeness)
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields:
            - "customer_id"
            - "email"
            - "registration_date"

      # Email format (detected from pattern analysis)
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      # Age range (based on min/max from profile)
      - type: "RangeCheck"
        severity: "WARNING"
        params:
          field: "age"
          min_value: 18
          max_value: 120

      # Unique customer IDs (detected 100% uniqueness)
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_fields:
            - "customer_id"
```

**3. JSON Export (Optional):**
- Machine-readable profile data
- For automated processing
- Integration with other tools

---

## What Gets Analyzed

### File-Level Metrics

DataK9 analyzes your entire file:

```
File Properties:
├── File size (bytes, KB, MB, GB)
├── Format (CSV, Excel, JSON, Parquet)
├── Total rows
├── Total columns
├── Processing time
└── Overall quality score (0-100%)
```

**Example Output:**

```
File: customers.csv
Size: 2.4 MB
Format: CSV
Rows: 10,000
Columns: 12
Quality Score: 87.5%
Processing Time: 3.2 seconds
```

### Column-Level Analysis

For each column, DataK9 provides:

#### Basic Statistics

**All Fields:**
- Total count
- Null count and percentage
- Unique count
- Cardinality (unique/total ratio)
- Most common value (mode)
- Top 10 values with frequencies

**Numeric Fields:**
- Minimum value
- Maximum value
- Mean (average)
- Median (50th percentile)
- Standard deviation
- Quartiles (Q1, Q2, Q3)
- Outlier detection

**String Fields:**
- Minimum length
- Maximum length
- Average length
- Common patterns
- Format detection (email, phone, date, etc.)

**Date/DateTime Fields:**
- Earliest date
- Latest date
- Date range
- Format detection
- Temporal patterns

#### Quality Metrics

DataK9 calculates quality scores for each column:

**Completeness (0-100%):**
```
Completeness = (Non-null values / Total values) × 100

Example:
- Total rows: 1000
- Null values: 50
- Completeness: 95%
```

**Validity (0-100%):**
```
Validity = (Values matching inferred type / Non-null values) × 100

Example:
- Non-null values: 950
- Valid integers: 940
- Validity: 98.9%
```

**Uniqueness (0-100%):**
```
Uniqueness = (Unique values / Total values) × 100

Example:
- Total values: 1000
- Unique values: 1000
- Uniqueness: 100% (perfect for primary keys)
```

**Consistency (0-100%):**
```
Consistency = Pattern match percentage

Example:
- Total emails: 1000
- Matching email pattern: 985
- Consistency: 98.5%
```

#### Type Inference

DataK9 intelligently infers data types:

**Inferred Types:**
- `integer` - Whole numbers
- `float` - Decimal numbers
- `string` - Text data
- `boolean` - True/False values
- `date` - Date values
- `datetime` - Date and time values
- `email` - Email addresses (pattern detected)
- `phone` - Phone numbers (pattern detected)
- `url` - URLs (pattern detected)
- `mixed` - Multiple types detected

**Confidence Levels:**
- `HIGH` (>95%) - Very confident in type
- `MEDIUM` (80-95%) - Reasonably confident
- `LOW` (<80%) - Uncertain, multiple types

**Example:**

```
Field: customer_id
Declared Type: Unknown
Inferred Type: integer
Confidence: HIGH (100%)
Reasoning: All 10,000 values are valid integers

Field: email
Declared Type: Unknown
Inferred Type: email
Confidence: HIGH (98.5%)
Reasoning: 9,850/10,000 values match email pattern

Field: notes
Declared Type: Unknown
Inferred Type: mixed
Confidence: LOW (45%)
Reasoning: Mix of strings, numbers, nulls
```

### Correlation Analysis

For numeric fields, DataK9 calculates correlations:

**Correlation Matrix:**
```
              age    income    spend
age          1.00     0.65     0.42
income       0.65     1.00     0.78
spend        0.42     0.78     1.00
```

**Interpretation:**
- `1.0` = Perfect positive correlation
- `0.0` = No correlation
- `-1.0` = Perfect negative correlation

**Use Cases:**
- Detect related fields
- Find data quality issues (unexpected correlations)
- Identify derived fields

---

## Understanding Profile Reports

### Report Sections

DataK9's HTML report has 6 main sections:

#### 1. Executive Summary

**Overview Card:**
```
┌─────────────────────────────────────┐
│  📊 Dataset Overview                │
├─────────────────────────────────────┤
│  File: customers.csv                │
│  Size: 2.4 MB                       │
│  Rows: 10,000                       │
│  Columns: 12                        │
│  Quality Score: 87.5%               │
└─────────────────────────────────────┘
```

**Quality Gauge:**
- Visual gauge showing overall quality (0-100%)
- Color-coded: Red (<60%), Yellow (60-80%), Green (>80%)
- Aggregates all column quality scores

**Key Findings:**
- Top quality issues detected
- Recommended actions
- High-impact validations to implement

#### 2. Column Summary Table

Interactive table with all columns:

| Column | Type | Completeness | Validity | Uniqueness | Quality |
|--------|------|--------------|----------|------------|---------|
| customer_id | integer | 100% | 100% | 100% | ✅ 100% |
| email | email | 98% | 99% | 95% | ✅ 97% |
| age | integer | 95% | 100% | 45% | ⚠️ 80% |
| notes | mixed | 60% | 45% | 88% | ❌ 64% |

**Features:**
- Sortable columns
- Filterable by quality score
- Click column for details
- Color-coded quality indicators

#### 3. Detailed Column Statistics

For each column, an expandable panel with:

**Statistics:**
```
customer_id (integer)
├── Count: 10,000
├── Null: 0 (0%)
├── Unique: 10,000 (100%)
├── Min: 1
├── Max: 10,000
├── Mean: 5,000.5
├── Median: 5,000
├── Std Dev: 2,886.9
├── Q1: 2,500
├── Q3: 7,500
└── Quality: 100%
```

**Top Values:**
```
Value          Count    Percentage
────────────────────────────────
active         6,500    65.0%
inactive       2,800    28.0%
pending          700     7.0%
```

**Validation Recommendations:**
```
Suggested Validations:
✓ UniqueKeyCheck (100% unique)
✓ MandatoryFieldCheck (0% null)
✓ RangeCheck (min: 1, max: 10000)
```

#### 4. Data Quality Issues

Lists detected issues by severity:

**Critical Issues (RED):**
- High null percentage (>20%)
- Low validity (<80%)
- Type conflicts
- Missing expected patterns

**Warnings (YELLOW):**
- Moderate null percentage (5-20%)
- Moderate validity (80-95%)
- Outliers detected
- Low cardinality for unique fields

**Info (BLUE):**
- Pattern variations
- Statistical observations
- Correlation insights

#### 5. Visualizations

**Distribution Charts:**
- Histograms for numeric fields
- Bar charts for categorical fields
- Box plots for outlier detection

**Quality Charts:**
- Quality score comparison
- Completeness heatmap
- Correlation matrix (for numeric fields)

**Trend Charts:**
- Temporal trends (if date fields present)
- Pattern frequency over time

#### 6. Auto-Generated Configuration

Preview of the YAML config:

```yaml
# Copy this configuration or download the generated file

validation_job:
  name: "Customers Data Validation"

files:
  - name: "customers"
    validations:
      # ... auto-generated validations
```

**Download Button:**
Click to download the complete YAML file.

---

## Auto-Generated Validations

DataK9 automatically suggests validations based on profiling results:

### Rules for Generation

**EmptyFileCheck:**
- Always generated
- Severity: ERROR
- Ensures file has data

**RowCountRangeCheck:**
- Generated if row count > 0
- Severity: WARNING
- Range: ±10% of profiled count

**MandatoryFieldCheck:**
- Generated for fields with >95% completeness
- Severity: ERROR
- Treats near-complete fields as mandatory

**RegexCheck:**
- Generated for email, phone, URL patterns
- Severity: ERROR
- Uses detected pattern

**ValidValuesCheck:**
- Generated for low-cardinality fields (<10 unique values)
- Severity: ERROR
- Uses detected valid values

**RangeCheck:**
- Generated for numeric fields
- Severity: WARNING
- Range: min to max from profile

**UniqueKeyCheck:**
- Generated for fields with >99% uniqueness
- Severity: ERROR
- Assumes primary key

**DateFormatCheck:**
- Generated for date fields
- Severity: ERROR
- Uses detected format

### Customizing Generated Config

The auto-generated config is a starting point:

```yaml
# Generated config
validations:
  - type: "RangeCheck"
    severity: "WARNING"    # ← Change to ERROR if critical
    params:
      field: "age"
      min_value: 18        # ← Adjust based on business rules
      max_value: 120       # ← Adjust based on business rules
```

**Recommended Workflow:**

1. Generate initial config with profiler
2. Review and adjust severities
3. Refine ranges and patterns
4. Add business-specific rules
5. Test with sample data
6. Deploy to production

---

## Quality Metrics Explained

### Overall Quality Score

DataK9 calculates an aggregate quality score:

```
Overall Quality = Average of all column quality scores

Column Quality = (Completeness + Validity + Uniqueness + Consistency) / 4
```

**Interpretation:**

| Score | Grade | Meaning |
|-------|-------|---------|
| 90-100% | A | Excellent quality |
| 80-89% | B | Good quality, minor issues |
| 70-79% | C | Acceptable, needs improvement |
| 60-69% | D | Poor quality, significant issues |
| <60% | F | Critical quality problems |

### Completeness

**Definition:** Percentage of non-null values

**Calculation:**
```python
completeness = (total_values - null_count) / total_values * 100
```

**Interpretation:**
- `100%` - Perfect, no nulls
- `95-99%` - High quality, few nulls
- `80-94%` - Acceptable, some nulls
- `<80%` - Poor, many nulls

**Actions:**
- `>95%` → MandatoryFieldCheck
- `80-95%` → CompletenessCheck with threshold
- `<80%` → Investigate why field is mostly null

### Validity

**Definition:** Percentage of values matching inferred type

**Calculation:**
```python
validity = values_matching_type / non_null_values * 100
```

**Interpretation:**
- `100%` - Perfect type consistency
- `95-99%` - High validity, few anomalies
- `80-94%` - Moderate validity, some type conflicts
- `<80%` - Poor validity, many type mismatches

**Actions:**
- `>95%` → Trust inferred type, add type validation
- `80-95%` → Investigate anomalies
- `<80%` → Field may be mixed type or dirty

### Uniqueness

**Definition:** Cardinality - ratio of unique to total values

**Calculation:**
```python
uniqueness = unique_count / total_count * 100
```

**Interpretation:**
- `100%` - Every value unique (primary key candidate)
- `50-99%` - High diversity
- `10-49%` - Moderate diversity
- `<10%` - Low diversity (categorical field)

**Actions:**
- `>99%` → UniqueKeyCheck
- `<10%` → ValidValuesCheck with enumeration
- `1 value` → Field is constant, may be redundant

### Consistency

**Definition:** Pattern match percentage for string fields

**Calculation:**
```python
consistency = values_matching_pattern / non_null_values * 100
```

**Interpretation:**
- `100%` - Perfect pattern consistency
- `95-99%` - High consistency, few variations
- `80-94%` - Moderate consistency
- `<80%` - Low consistency, many pattern variations

**Actions:**
- `>95%` → RegexCheck with detected pattern
- `80-95%` → Review pattern variations
- `<80%` → Field may need standardization

---

## How Profiling Works

### Statistical Analysis Algorithms

DataK9's profiler uses advanced statistical methods to analyze your data deeply:

**Distribution Analysis:**
- Calculates standard statistical measures (mean, median, mode, std dev, quartiles)
- Detects distribution shape using Kolmogorov-Smirnov tests
- Identifies outliers using IQR (Interquartile Range) and Z-score methods
- Analyzes value frequency distributions for categorical data

**Anomaly Detection Methods:**
- **IQR Method**: Detects values outside Q1-1.5×IQR to Q3+1.5×IQR range
- **Z-Score Method**: Flags values >3 standard deviations from mean
- **Modified Z-Score**: Uses median absolute deviation for robustness to outliers
- **Pattern-Based**: Detects format inconsistencies in string data

**Temporal Pattern Recognition:**
- Analyzes time-series data for trends and seasonality
- Detects date gaps and irregular intervals
- Identifies temporal drift in data characteristics
- Calculates growth rates and change patterns

**Pattern Detection:**
DataK9 recognizes common data patterns using regex and heuristics:
- **Email patterns**: RFC 5322 compliant detection with 30% minimum match threshold
- **Phone numbers**: International and domestic formats with column name filtering
- **URLs**: HTTP/HTTPS detection with protocol validation
- **Dates**: Multiple format detection (ISO, US, EU) with confidence scoring
- **IDs**: Sequential, UUID, and custom identifier patterns

**PII Detection Safeguards:**
DataK9 uses intelligent PII detection to minimize false positives:
- **30% Match Threshold**: At least 30% of values must match PII pattern (prevents false alarms on numeric columns)
- **Column Name Filtering**: Excludes unlikely PII columns like 'id', 'amount', 'count', 'total', 'quantity', 'price'
- **Tightened Regex Patterns**: Phone patterns require specific formatting (e.g., `+` prefix for international)
- **Confidence Scoring**: Reports detection confidence so you can validate results

**Dependency Discovery:**
DataK9 identifies functional dependencies between columns:
- **Cardinality Check**: Rejects if source_unique > target_unique × 0.8 (prevents coincidental 1:1 mappings)
- **Minimum Occurrence Threshold**: Source values must appear at least 5 times (statistical significance)
- **Sample Size Penalty**: Reduces confidence by 30% for samples <100 rows (small sample warning)
- **Dependency Strength**: Requires 98% consistency for true functional dependency (was 95%, now more strict)
- **Result**: Eliminates false positive correlation suggestions (e.g., "zip_code determines customer_id")

This prevents the profiler from suggesting bogus validations that would immediately fail on real data.

**Why These Algorithms:**
- **User-Friendly Output**: Statistical complexity hidden behind plain-language interpretations
- **Accuracy**: Multiple validation layers prevent false positives (e.g., zip_code flagged as PII)
- **Context-Aware**: Smart suggestions based on actual data patterns, not just statistics
- **Actionable**: Every insight directly translates to a specific validation rule you can implement

---

## Type Inference

### How DataK9 Infers Types

DataK9 samples your data and applies heuristics:

**1. Pattern Matching:**
```python
# Email detection
if matches_regex(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'):
    type = "email"

# Phone detection
if matches_regex(r'^\+?[\d\s\-\(\)]{10,}$'):
    type = "phone"

# URL detection
if matches_regex(r'^https?://'):
    type = "url"
```

**2. Value Parsing:**
```python
# Try integer
if all(value.isdigit() for value in sample):
    type = "integer"

# Try float
elif all(is_numeric(value) for value in sample):
    type = "float"

# Try date
elif all(is_date(value) for value in sample):
    type = "date"
```

**3. Confidence Calculation:**
```python
confidence = (matching_values / total_values) * 100

if confidence > 95:
    confidence_level = "HIGH"
elif confidence > 80:
    confidence_level = "MEDIUM"
else:
    confidence_level = "LOW"
```

### Known vs Inferred Types

**Known Types:**
- Explicitly declared in schema
- From database column types
- From Parquet schema
- From typed data sources

**Inferred Types:**
- Detected from data analysis
- No schema provided
- CSV/JSON without type info

**Indicator in Report:**
```
Field: price
Type: float (Inferred ✓)    ← Detected from data
Confidence: HIGH (100%)

Field: quantity
Type: integer (Known ✓)      ← From schema
```

### Type Conflicts

When known ≠ inferred:

```
Field: customer_id
Known Type: string
Inferred Type: integer
Conflict: Yes ⚠️

Recommendation:
- All values are integers
- Consider changing type to integer
- Or add validation to ensure numeric format
```

---

## Profiling Large Files

### Memory-Efficient Profiling

DataK9 uses chunked processing for large files:

```bash
# Profile 200GB file
python3 -m validation_framework.cli profile huge_data.parquet

# DataK9 processes in chunks:
# - Reads 50,000 rows at a time
# - Aggregates statistics
# - Never loads entire file into memory
```

**Memory Usage:**
- 1 GB file → ~200 MB RAM
- 10 GB file → ~400 MB RAM
- 200 GB file → ~400 MB RAM

**Performance:**
| File Size | Format | Processing Time | Memory |
|-----------|--------|-----------------|--------|
| 100 MB | CSV | ~10 sec | ~100 MB |
| 1 GB | CSV | ~2 min | ~200 MB |
| 10 GB | Parquet | ~5 min | ~300 MB |
| 100 GB | Parquet | ~45 min | ~400 MB |

### Sampling for Very Large Files

For files >100 GB, consider sampling:

```bash
# Profile first 1 million rows
python3 -m validation_framework.cli profile huge.parquet \
  --sample-rows 1000000

# Profile random 10% sample
python3 -m validation_framework.cli profile huge.parquet \
  --sample-percent 10
```

**When to Sample:**
- Exploratory profiling (quick insights)
- Files >100 GB
- Time constraints
- Approximate statistics acceptable

**When to Profile Fully:**
- Critical datasets
- Accurate statistics needed
- Detecting rare patterns
- Regulatory compliance

---

## Best Practices

### 1. Profile Before Validating

**Workflow:**
```
📊 Profile Data
    ↓
📖 Review Report
    ↓
💡 Understand Patterns
    ↓
⚙️ Build Validations
    ↓
✅ Deploy with Confidence
```

**Benefits:**
- Data-driven validation rules
- Understand quality baseline
- Identify priority issues
- Generate initial config

### 2. Profile Regularly

**Schedule:**
- Initial deployment
- After data source changes
- Quarterly quality assessment
- When investigating issues
- Before major migrations

**Track Changes:**
```bash
# Profile monthly
python3 -m validation_framework.cli profile data.csv \
  -o profile_2024_01.html

python3 -m validation_framework.cli profile data.csv \
  -o profile_2024_02.html

# Compare reports to detect drift
```

### 3. Use Profiling for Documentation

Generate data dictionaries:

**From Profile Report:**
- Field names and types
- Value ranges
- Top values
- Nullability
- Uniqueness

**Example Data Dictionary:**
```markdown
# Customer Data Dictionary

## customer_id
- Type: Integer
- Required: Yes (100% complete)
- Unique: Yes (100% unique)
- Range: 1 - 50,000
- Primary Key: Yes

## email
- Type: String (Email format)
- Required: Yes (98% complete)
- Format: RFC 5322 compliant
- Example: customer@example.com

## age
- Type: Integer
- Required: No (85% complete)
- Range: 18 - 95
- Mean: 42.3
- Median: 39
```

### 4. Combine with Domain Knowledge

**Profile provides:**
- What the data looks like
- Statistical patterns
- Quality baseline

**You provide:**
- What the data should look like
- Business rules
- Regulatory requirements

**Example:**

```yaml
# Profile detected: age range 0-150
# Business rule: customers must be 18+

- type: "RangeCheck"
  severity: "ERROR"
  params:
    field: "age"
    min_value: 18       # Business rule, not profiled min
    max_value: 120      # Realistic, not profiled max (150)
```

### 5. Review Auto-Generated Config

Don't blindly use generated config:

**Review:**
- ✅ Severities appropriate?
- ✅ Ranges realistic?
- ✅ Patterns correct?
- ✅ Missing business rules?

**Refine:**
```yaml
# Generated
- type: "RangeCheck"
  severity: "WARNING"          # ← Change to ERROR
  params:
    field: "discount_percent"
    min_value: 0
    max_value: 95              # ← Change to 50 (business max)

# Add business rule not in profile
- type: "ConditionalValidation"
  severity: "ERROR"
  params:
    condition: "total_price == quantity * unit_price"
```

---

## Command Reference

### Basic Command

```bash
python3 -m validation_framework.cli profile <file_path>
```

### Options

| Option | Description | Example |
|--------|-------------|---------|
| `-o`, `--output` | HTML report path | `-o my_profile.html` |
| `-c`, `--config` | YAML config path | `-c my_validation.yaml` |
| `-j`, `--json` | JSON export path | `-j profile_data.json` |
| `--format` | Explicit format | `--format csv` |
| `--sample-rows` | Sample N rows | `--sample-rows 100000` |
| `--sample-percent` | Sample N% rows | `--sample-percent 10` |
| `--chunk-size` | Rows per chunk | `--chunk-size 50000` |

### Examples

```bash
# Basic profiling
python3 -m validation_framework.cli profile data.csv

# Custom output paths
python3 -m validation_framework.cli profile data.csv \
  -o reports/profile.html \
  -c configs/validation.yaml

# Explicit format
python3 -m validation_framework.cli profile data.txt --format csv

# Sample large file
python3 -m validation_framework.cli profile huge.parquet \
  --sample-rows 1000000

# All outputs
python3 -m validation_framework.cli profile data.csv \
  -o profile.html \
  -c validation.yaml \
  -j profile.json
```

---

## Next Steps

**You've learned data profiling! Now:**

1. **[Configuration Guide](configuration-guide.md)** - Refine your validation config
2. **[Validation Catalog](validation-catalog.md)** - Explore all 35+ validations
3. **[Best Practices](best-practices.md)** - Production deployment guidance
4. **[Reading Reports](reading-reports.md)** - Understanding validation results

---

**🐕 Profile first, validate confidently - DataK9 guards with data-driven intelligence**
