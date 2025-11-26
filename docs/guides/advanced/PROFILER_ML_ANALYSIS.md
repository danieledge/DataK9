# DataK9 ML-Based Anomaly Detection (Beta)

**Last Updated:** November 2025
**Author:** Daniel Edge
**Status:** Experimental Beta Feature

## Overview

The DataK9 profiler includes an experimental machine learning-based anomaly detection system that identifies data quality issues that traditional profiling might miss. This feature uses Isolation Forest, pattern detection algorithms, and statistical methods to analyze sampled data and detect outliers, format inconsistencies, and suspicious patterns.

> **Note:** This is a beta feature. Enable it with the `--beta-ml` flag.

## Table of Contents

1. [Quick Start](#quick-start)
2. [How It Works](#how-it-works)
3. [Detection Techniques](#detection-techniques)
4. [Understanding Results](#understanding-results)
5. [Performance Considerations](#performance-considerations)
6. [Configuration Options](#configuration-options)
7. [Interpreting the Report](#interpreting-the-report)

---

## Quick Start

Enable ML analysis when profiling:

```bash
# Basic usage
data-validate profile data.csv --beta-ml

# With output options
data-validate profile data.parquet --beta-ml -o report.html -j profile.json

# Sampling for large files (recommended for files > 1GB)
data-validate profile large_data.parquet --beta-ml --sample 500000
```

## How It Works

### Sampling Strategy

The ML analyzer automatically samples your data for efficient analysis:

- **Default sample size:** 250,000 rows
- **Sampling method:** Random sampling with fixed seed for reproducibility
- **Why sampling:** ML algorithms like Isolation Forest work effectively on samples while maintaining statistical validity

### Analysis Pipeline

1. **Load sample** - Extracts representative sample from data
2. **Numeric analysis** - Isolation Forest outlier detection on all numeric columns
3. **Format analysis** - Pattern extraction and consistency checking on string columns
4. **Category analysis** - Rare value detection in categorical fields
5. **Cross-column analysis** - Ratio and relationship checks between related columns
6. **Temporal analysis** - Time distribution pattern detection

---

## Detection Techniques

### 1. Numeric Outlier Detection (Isolation Forest)

Uses the Isolation Forest algorithm to detect outliers in numeric columns without assuming any particular distribution.

**How it works:**
- Builds random trees that isolate observations
- Anomalies are isolated in fewer splits (shorter path length)
- Contamination parameter set to 0.1% (expects 0.1% outliers)

**What it catches:**
- Extreme values that IQR/Z-score might miss
- Outliers in multi-modal or skewed distributions
- Values that are anomalous relative to the overall distribution

**Example output:**
```json
{
  "Amount Paid": {
    "method": "isolation_forest",
    "anomaly_count": 245,
    "anomaly_percentage": 0.098,
    "normal_range": {"min": 10.50, "max": 49500.00},
    "anomaly_range": {"min": 1250000.00, "max": 6000000000.00},
    "top_anomalies": [1500000.00, 2500000.00, 6000000000.00],
    "interpretation": "Found 245 extreme outliers - largest is 12000x the median value"
  }
}
```

### 2. Format Consistency Detection

Extracts format patterns from string values and identifies values that don't match the dominant pattern.

**Pattern encoding:**
- `A` = alphabetic character
- `9` = digit
- `_` = space
- Other characters preserved literally
- Consecutive same characters compressed (e.g., `AAA999` becomes `A3_9_3`)

**What it catches:**
- Inconsistent ID formats (e.g., `ACC123456` vs `ACCT-123456`)
- Typos in structured fields
- Missing or extra characters
- Case inconsistencies

**Example output:**
```json
{
  "Account": {
    "dominant_pattern": "A3_9_6",
    "dominant_percentage": 99.95,
    "anomaly_count": 125,
    "anomaly_patterns": {"A4_9_6": 50, "A3-9_6": 45, "A2_9_6": 30},
    "sample_anomalies": ["ACCT001234", "AC-123456", "ACC12345"],
    "interpretation": "Format inconsistency: 125 values (0.050%) don't match the dominant 'A3_9_6' pattern"
  }
}
```

### 3. Rare Category Detection

Identifies suspiciously rare values in categorical columns that may indicate data entry errors or typos.

**Detection criteria:**
- Values appearing in less than 0.1% of records
- Only analyzes columns with 2-100 unique values (categorical range)

**What it catches:**
- Typos in category values (e.g., "USDD" instead of "USD")
- Placeholder values ("N/A", "UNKNOWN", "???")
- Trailing/leading spaces (" EUR" vs "EUR")
- Case variations ("usd" vs "USD")

**Example output:**
```json
{
  "Payment Currency": {
    "threshold_percentage": 0.1,
    "rare_values": [
      {"value": "USDD", "count": 3, "percentage": 0.0012},
      {"value": " EUR", "count": 5, "percentage": 0.002},
      {"value": "N/A", "count": 12, "percentage": 0.0048}
    ],
    "total_rare_count": 20,
    "interpretation": "Found 3 rare values appearing in <0.1% of records - may indicate typos or data errors"
  }
}
```

### 4. Cross-Column Consistency

Checks relationships between potentially related columns (e.g., Amount Paid vs Amount Received).

**Detection logic:**
- Identifies column pairs with related names (paid/received, debit/credit, in/out)
- Calculates ratios between values
- Flags extreme ratios (>10x or <0.1x)

**What it catches:**
- Currency conversion errors
- Data entry mistakes where related fields are inconsistent
- ETL issues where transformations failed

**Example output:**
```json
{
  "columns": ["Amount Paid", "Amount Received"],
  "issue_type": "extreme_ratio",
  "extreme_high_count": 156,
  "extreme_low_count": 89,
  "total_issues": 245,
  "median_ratio": 1.02,
  "interpretation": "Found 245 records with extreme ratios between Amount Paid and Amount Received"
}
```

### 5. Temporal Pattern Analysis

Analyzes datetime columns for suspicious time distributions.

**What it checks:**
- Hour distribution of timestamps
- Concentration at specific times (e.g., midnight)
- Weekend vs weekday distribution

**What it catches:**
- Missing time components (all timestamps at 00:00)
- Data generation artifacts (unrealistic time patterns)
- System-generated data masquerading as real transactions

**Example output:**
```json
{
  "Timestamp": {
    "date_range": {"min": "2022-01-01 00:00:00", "max": "2023-12-31 23:59:00"},
    "hour_distribution": {"0": 247500, "1": 125, "2": 118},
    "warning": "suspicious_midnight_concentration",
    "midnight_percentage": 99.02,
    "interpretation": "99.0% of timestamps are at midnight - may indicate missing time component or data generation artifact"
  }
}
```

---

## Understanding Results

### Severity Levels

The ML analysis assigns an overall severity based on total issues found:

| Severity | Issue Count | Badge Color |
|----------|-------------|-------------|
| High     | > 1,000     | Red         |
| Medium   | 100 - 1,000 | Yellow      |
| Low      | 1 - 99      | Green       |
| None     | 0           | Green       |

### Key Findings Summary

The summary includes:
- **Total issues found** - Aggregate count across all detection methods
- **Key findings** - Top 3-5 most significant issues with descriptions
- **Analysis time** - How long the ML analysis took
- **Sample coverage** - What percentage of the data was analyzed

---

## Performance Considerations

### Execution Time

| Dataset Size | Sample Size | Typical Analysis Time |
|--------------|-------------|----------------------|
| < 100K rows  | Full data   | 2-5 seconds          |
| 100K - 1M    | 250K sample | 15-30 seconds        |
| 1M - 10M     | 250K sample | 20-40 seconds        |
| 10M - 100M   | 250K sample | 25-45 seconds        |
| > 100M       | 250K sample | 30-60 seconds        |

### Memory Usage

The ML analyzer is memory-efficient:
- Only loads sample into memory (not full dataset)
- Cleans up after analysis completes
- Typical overhead: 200-500 MB for 250K row sample

### Dependencies

The ML analysis requires scikit-learn:

```bash
pip install scikit-learn
```

If scikit-learn is not installed, the `--beta-ml` flag will display a warning and skip ML analysis.

---

## Configuration Options

Currently, the ML analyzer uses sensible defaults. Future versions may expose:

- **Sample size** - Number of rows to sample (default: 250,000)
- **Contamination** - Expected outlier percentage (default: 0.1%)
- **Rare threshold** - Percentage threshold for rare values (default: 0.1%)
- **Ratio thresholds** - Extreme ratio bounds (default: 10x)

---

## Interpreting the Report

### HTML Report Section

When `--beta-ml` is enabled, the HTML report includes an "ML-Based Anomaly Detection" section with:

1. **Summary Card** - Total issues found and key findings list
2. **Issue Breakdown Chart** - Doughnut chart showing distribution by category
3. **Outlier Column Chart** - Bar chart showing top columns by outlier count
4. **Detailed Findings** - Expandable sections for each detection type

### JSON Output

The `ml_findings` key in JSON output contains:

```json
{
  "ml_findings": {
    "sample_info": {
      "original_rows": 179000000,
      "analyzed_rows": 250000,
      "sampled": true,
      "sample_percentage": 0.14
    },
    "numeric_outliers": { ... },
    "format_anomalies": { ... },
    "rare_categories": { ... },
    "cross_column_issues": [ ... ],
    "temporal_patterns": { ... },
    "summary": {
      "total_issues": 2775,
      "severity": "high",
      "key_findings": [ ... ]
    },
    "analysis_time_seconds": 27.4
  }
}
```

---

## Best Practices

1. **Use with sampling for large files** - The `--sample` flag reduces profiling time while ML analysis automatically samples anyway

2. **Review findings in context** - Not all flagged issues are problems; some may be legitimate data characteristics

3. **Combine with validation rules** - Use ML findings to inform which validation rules to add

4. **Re-run after fixes** - After addressing issues, re-run ML analysis to verify improvements

5. **Consider domain knowledge** - Some "anomalies" may be expected in your domain (e.g., legitimate large transactions)

---

## Limitations

- **Beta status** - Feature is experimental and may change
- **Sampling bias** - Large datasets are sampled, which may miss localized issues
- **No model persistence** - Analysis runs fresh each time (no learning from history)
- **Fixed parameters** - Detection thresholds are not currently configurable

---

## Feedback

This is a beta feature. Please report issues and suggestions at:
https://github.com/anthropics/data-validation-tool/issues
