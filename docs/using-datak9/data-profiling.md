# Data Profiling Guide

**Know Your Data Before You Guard It**

DataK9's profiler analyzes your data files to understand their structure, quality, and characteristics. Like a K9 unit surveying the territory before patrol, profiling gives you the intelligence you need to build effective validations.

---

## 📋 Table of Contents

- [Quick Start](#-quick-start) - Get profiling in 30 seconds
- [Why Profile First?](#-why-profile-first) - The compelling reason
- [Key Features](#-key-features) - What makes DataK9's profiler unique
  - [FIBO Semantic Intelligence](#-fibo-semantic-intelligence-new)
  - [Auto-Generated Validations](#-auto-generated-validations)
  - [Comprehensive Analysis](#-comprehensive-analysis)
  - [ML-Based Anomaly Detection](#-ml-based-anomaly-detection-beta) - NEW!
- [Understanding Reports](#-understanding-reports) - Read your profile results
- [How It Works](#-how-it-works) - Under the hood (10 stages)
- [Command Reference](#-command-reference) - All options and examples
- [Best Practices](#-best-practices) - Tips for success

---

## ⚡ Quick Start

**30 seconds to your first profile:**

```bash
# Basic profiling (all enhancements enabled by default)
python3 -m validation_framework.cli profile data.csv

# Financial data profiling (FIBO semantic tagging enabled by default)
python3 -m validation_framework.cli profile transactions.csv -o profile.html

# With ML-based anomaly detection (beta)
python3 -m validation_framework.cli profile transactions.csv --beta-ml -o report.html

# Generated files:
# ✓ data_profile_report.html    (Interactive visual report)
# ✓ data_validation.yaml         (Auto-generated validation config)
```

**Note:** Since v1.54, all profiler enhancements are **enabled by default**:
- FIBO semantic tagging
- PII detection
- Temporal analysis
- Enhanced correlation analysis

**[View Example Reports →](../../examples/sample_reports/)**
- [Small Dataset Profile](../../examples/sample_reports/profiler_report_example.html) - 500 rows, accounts data
- [Large Dataset Profile](../../examples/sample_reports/large_dataset_profile.html) - 179M rows, transactions

---

## 🎯 Why Profile First?

### The Problem: Blind Validation

Most teams write validations **without understanding their data:**
- ❌ Guessing at value ranges → validations fail on real data
- ❌ Missing unexpected patterns → issues slip through
- ❌ Over-validating clean fields → false positives everywhere
- ❌ Under-validating messy fields → quality problems persist

### The Solution: Evidence-Based Validation

**DataK9's profiler shows you what your data actually looks like:**

```
Before Profiling:
"I think customer_age should be between 0-120"
→ Validation fails because data includes -1 for "unknown"

After Profiling:
"Profiler shows: min=-1, max=95, mode=-1 (30% of records)"
→ Create targeted validation: age >= -1 AND (age = -1 OR age BETWEEN 18 AND 95)
```

**Result:** Validations based on reality, not assumptions.

---

## ✨ Key Features

### 🧠 FIBO Semantic Intelligence (NEW!)

**What is FIBO?**
FIBO (Financial Industry Business Ontology) is an **industry-standard ontology** maintained by the EDM Council. It defines financial concepts like "MonetaryAmount", "Currency", and "Account" with precise definitions.

**Why It Matters:**
Instead of just knowing a column is "numeric", DataK9 understands it represents a **monetary amount** that must be non-negative, expressed in a currency, and follows financial rules.

**In Action:**
```
Column: transaction_amount
├── Type Detection: "decimal"                    ← Basic profiler
├── FIBO Semantic Tag: "money.amount"            ← DataK9 profiler
├── FIBO Definition: "A monetary measure"
├── Smart Validation: NonNegativeCheck           ← Auto-suggested
└── FIBO Reference: fibo-fnd-acc-cur:MonetaryAmount
```

**The Benefit:**
- ✓ Context-aware validation suggestions
- ✓ Industry-standard terminology
- ✓ Financial best practices built-in
- ✓ Plain-language explanations

**[Learn More: FIBO Ontology](https://spec.edmcouncil.org/fibo/)** (MIT License)

---

### 🎯 Auto-Generated Validations

**The Killer Feature: DataK9 writes your validation config for you.**

After profiling, you get a **ready-to-use YAML config** with intelligent suggestions:

```yaml
# Auto-generated based on your actual data

validations:
  # FIBO-based (from semantic understanding)
  - type: "RangeCheck"
    severity: "ERROR"
    params:
      field: "transaction_amount"
      min_value: 0  # FIBO: money.amount must be non-negative

  # Pattern-based (from format detection)
  - type: "RegexCheck"
    severity: "ERROR"
    params:
      field: "email"
      pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
      # Detected: 98.5% match email pattern

  # Statistical-based (from uniqueness analysis)
  - type: "UniqueKeyCheck"
    severity: "ERROR"
    params:
      key_fields: ["customer_id"]
      # Detected: 100% unique values

  # Completeness-based (from null analysis)
  - type: "MandatoryFieldCheck"
    severity: "ERROR"
    params:
      fields: ["customer_id", "email", "registration_date"]
      # Detected: >95% completeness
```

**No more starting from scratch.** Review, customize, deploy.

---

### 📊 Comprehensive Analysis

What DataK9 profiles:

| Category | What It Analyzes | Example Output |
|----------|------------------|----------------|
| **📈 Statistics** | Min, max, mean, median, quartiles, std dev | Range: 0-50,000 |
| **🔍 Quality** | Completeness, validity, uniqueness, consistency | Quality: 87% |
| **🎨 Patterns** | Email, phone, URL, date formats | 98.5% email format |
| **🧠 Semantics** | FIBO tags, meaning, context | money.amount |
| **🔗 Relationships** | Correlations, dependencies | income ↔ spend (r=0.78) |
| **🚨 Anomalies** | Outliers, unusual patterns | 5 outliers detected |
| **🔐 Privacy** | PII detection (email, phone, SSN) | 2 PII fields flagged |
| **⏱️ Temporal** | Date ranges, gaps, trends | Jan 2024 - Dec 2024 |

---

### 🧠 ML-Based Anomaly Detection (Beta)

**Enable with `--beta-ml` flag for machine learning-powered anomaly detection.**

DataK9's ML analyzer uses industry-standard algorithms to find patterns that traditional profiling might miss:

```bash
# Enable ML analysis
python3 -m validation_framework.cli profile data.csv --beta-ml -o report.html
```

#### What ML Analysis Detects

| Analysis Type | What It Finds | Plain English |
|---------------|---------------|---------------|
| **🔢 Univariate Outliers** | Individual values that deviate significantly | "This $50,000 transaction stands out among mostly $50 ones" |
| **🎯 Multivariate Outliers** | Unusual combinations of values | "Small amount + high-risk country together is suspicious" |
| **🔮 Cluster Analysis** | Natural groupings and noise points | "Most data falls into 3 groups; these 500 records don't fit any" |
| **📈 Correlation Anomalies** | Broken relationships between columns | "Amount Paid and Amount Received should match but don't here" |
| **📝 Format Inconsistencies** | Values that don't match the dominant pattern | "99% use format XXX-1234, but these 50 records don't" |
| **⚠️ Rare Categories** | Suspiciously infrequent values | "This category appears only 3 times - could be a typo" |
| **🔗 Cross-Column Issues** | Violated business rules between columns | "End Date is before Start Date in 12 records" |
| **⏰ Temporal Anomalies** | Suspicious time patterns | "Too many transactions at midnight - batch processing artifact?" |

#### Algorithms Used

| Algorithm | Purpose | When It's Used |
|-----------|---------|----------------|
| **Isolation Forest** | Outlier detection | Numeric columns with >500 rows |
| **DBSCAN** | Clustering & noise detection | Finding natural groupings |
| **IQR Statistical** | Fallback outlier detection | When sklearn unavailable |
| **Pearson Correlation** | Relationship analysis | Pairs of numeric columns |

#### Smart Features

**Adaptive Contamination:** The ML analyzer automatically estimates the expected outlier rate based on your data's characteristics, using IQR-based calculation with dataset size adjustments.

**Binary Column Skip:** Binary/boolean columns (like flags with only 0/1 values) are automatically excluded from outlier detection - a rare value in a flag isn't an anomaly.

**FIBO-Based Semantic Intelligence:** The ML analyzer integrates with FIBO (Financial Industry Business Ontology) semantic tags to intelligently handle different column types:

**For Rare Category Detection:**

| Semantic Type | Behavior | Example |
|---------------|----------|---------|
| **Identifiers** (`banking.account`, `banking.transaction`, `party.customer_id`) | Skip rare detection | Account IDs are expected to be diverse |
| **Reference Domains** (`money.currency`, `category.payment_method`) | Validate against reference list | Only flag unknown values like "FAKE_XYZ" |
| **Counterparties** (`party.counterparty`) | Use strict threshold (10x stricter) | Rare banks are normal in international trade |
| **Categories** (`category.transaction_type`) | Default rare detection | Rare types may indicate data issues |

**For Numeric Outlier Detection (Isolation Forest):**

Columns that are numeric but semantically categorical are excluded:

| Semantic Type | Behavior | Why |
|---------------|----------|-----|
| **`party.counterparty`** | Skip outlier detection | Bank ID 1099 isn't an "outlier" just because most use 1-100 |
| **`banking.account`** | Skip outlier detection | Account numbers are identifiers, not measurements |
| **`category`** | Skip outlier detection | Category codes stored as numbers aren't continuous data |
| **`flag.binary`** | Skip outlier detection | Binary flags have only 2 values by design |

**For Correlation & Multivariate Analysis:**

The same semantic filtering applies to correlation detection and multivariate outlier analysis. Correlations between numeric IDs (like Bank ID and Account Number) are meaningless statistically, so these columns are excluded.

This means:
- ✅ Account number `ACC-00047839` → NOT flagged (identifier, high cardinality expected)
- ✅ Currency `NOK` (Norwegian Krone) → NOT flagged (valid ISO currency code)
- ✅ Counterparty `BANK-0001` appearing once → NOT flagged (strict threshold for entities)
- ✅ Bank ID `1099` → NOT flagged for outliers (numeric but categorical)
- ✅ Bank ID / Account correlation → NOT analyzed (meaningless for IDs)
- ⚠️ Currency `FAKE_XYZ` → FLAGGED (not a valid reference code)
- ⚠️ Payment type `Typo` → FLAGGED (not in known payment methods)

**Known Domain Detection:** For columns named "currency", "country", etc., the analyzer recognizes valid but rare values:
- ✅ UK Pound, Yen, Bitcoin → Valid currencies, not flagged
- ⚠️ "FAKE_XYZ" → Unknown, flagged for review

**Confidence Scoring:** Each finding includes a confidence level (Very High, High, Medium, Low) based on:
- Detection method reliability
- Sample size adequacy
- Anomaly percentage reasonableness

#### Sample Output

```
🧠 Running ML-based anomaly detection (beta)...
  🔴 ML Analysis: 2,847 potential issues (medium severity)
    • Univariate outliers: 1,250 detected (worst: transaction_amount)
    • Multivariate outliers: 500 records with unusual value combinations
    • Cluster analysis: 5 clusters found, 847 noise points (3.4%)
    • Rare values: 12 potentially suspicious categorical values
  → Analyzed 250,000 rows in 45.2s
```

#### Interpreting Results

**High Issue Count ≠ Bad Data**

The ML analyzer flags values for human review. High counts may indicate:
- Legitimate but unusual business transactions
- Data from multiple sources with different patterns
- Historical data with different formats
- Genuinely problematic records

**Review the sample rows** provided in each section to determine which findings need action.

#### Configuration & Limits

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ML_SAMPLE_SIZE` | 250,000 | Max rows analyzed (larger files sampled) |
| `MIN_ROWS_FOR_ML` | 500 | Minimum rows required for reliable ML |
| `Max Contamination` | 5% | Upper limit for outlier detection rate |
| `DBSCAN min_samples` | 10 | Minimum cluster size |

---

### ⚡ Memory-Efficient Processing

**Profile massive files without massive RAM:**

```
File Size    Memory Usage    Processing
---------    ------------    ----------
100 MB       ~100 MB         ~10 sec
1 GB         ~200 MB         ~2 min
10 GB        ~300 MB         ~5 min
200 GB       ~400 MB         ~45 min
```

**How:** Chunked processing - only one chunk in memory at a time.

**Tested:** 357 million rows, no memory leaks.

---

## 📄 Understanding Reports

### Interactive HTML Report

**What you get:**

<details>
<summary><b>1. 📊 Executive Summary</b></summary>

```
Dataset Overview
├── File: customers.csv
├── Size: 2.4 MB
├── Rows: 10,000
├── Columns: 12
└── Quality Score: 87.5%

Key Findings
├── 3 PII fields detected (email, phone, address)
├── 2 fields with missing data (>5% null)
├── 1 potential primary key (customer_id: 100% unique)
└── 15 validation suggestions generated
```
</details>

<details>
<summary><b>2. 🧠 Semantic Understanding (FIBO)</b></summary>

For each column, see what it **means**:

```
transaction_amount
├── Primary Tag: money.amount
├── Confidence: 80%
├── FIBO Class: fibo-fnd-acc-cur:MonetaryAmount
├── Definition: "A monetary measure, expressed in some currency"
├── Evidence:
│   ├── Column name contains "amount"
│   ├── All values are numeric
│   └── All values ≥ 0 (monetary property)
└── Suggested Validations:
    ├── NonNegativeCheck (FIBO: money must be ≥ 0)
    └── OutlierDetectionCheck
```

**28 FIBO Tags Available:**
- `money.amount`, `money.currency`, `money.price`
- `banking.account`, `banking.transaction`, `banking.payment`
- `temporal.transaction_date`, `identifier.code`
- And 20 more...
</details>

<details>
<summary><b>3. 📈 Detailed Statistics</b></summary>

Per column:
- Count, null %, unique %, cardinality
- Min/max/mean/median (numeric)
- Length stats (strings)
- Date ranges (temporal)
- Top 10 values with frequencies
- Distribution charts
</details>

<details>
<summary><b>4. ✅ Quality Metrics</b></summary>

**Four quality dimensions:**

```
Completeness = (non-null / total) × 100
Validity = (matching type / non-null) × 100
Uniqueness = (unique / total) × 100
Consistency = (matching pattern / non-null) × 100

Column Quality = average of all four
```

**Color-coded:** 🟢 >80% | 🟡 60-80% | 🔴 <60%
</details>

<details>
<summary><b>5. 💡 Validation Suggestions</b></summary>

**Organized by category:**
- File-Level (EmptyFileCheck, RowCountRangeCheck)
- Field-Level (MandatoryFieldCheck, UniqueKeyCheck)
- Format-Level (RegexCheck, DateFormatCheck)
- Range-Level (RangeCheck, StatisticalOutlierCheck)
- **FIBO-Level** (Semantic intelligence)

**Each suggestion includes:**
- Validation type
- **Field name** - clearly shows which field the validation applies to
- Severity recommendation
- Parameters (auto-filled from data)
- Confidence score
- Reasoning
- **Visible YAML snippet** - see the full config inline
- Copy button for individual snippets

**Full Configuration YAML:**
The report includes a complete, ready-to-use validation configuration with all suggestions combined. Copy the entire config and save as a `.yaml` file to run validations immediately.
</details>

---

## ⚙️ How It Works

### The 10-Stage Profiling Process

<details>
<summary><b>Stage 1: Intelligent Chunk Size Determination</b></summary>

**Before processing, DataK9 calculates optimal chunk size:**

```python
Analyzes:
- File size (MB/GB)
- Available system memory
- Column count (more columns = smaller chunks)
- Data type mix (complex types need more memory)

Result:
- Small files (<100 MB): Process entire file
- Medium files (100MB-10GB): 25K-50K row chunks
- Large files (>10GB): 10K row chunks
```

**Why it matters:** Prevents out-of-memory errors, maximizes performance.
</details>

<details>
<summary><b>Stage 2: Schema Detection</b></summary>

```
🔍 Loading and Inspecting Data...
├── Detect file format (CSV, Excel, Parquet, JSON)
├── Read column names from header
├── Count total rows
├── Sample first 1000 rows for type inference
└── Initialize statistics collectors
```
</details>

<details>
<summary><b>Stage 3: Chunked Statistical Analysis</b></summary>

```
📊 Processing Chunks (50,000 rows per chunk)...
├── Chunk 1/20
│   ├── Update null counts
│   ├── Update unique value sets
│   ├── Collect min/max values
│   ├── Accumulate for mean calculation
│   └── Track value frequencies
├── Chunk 2/20
│   └── (aggregate statistics)
└── Chunk 20/20
    └── (finalize aggregations)
```

**Key:** Only ONE chunk in memory at a time.
</details>

<details>
<summary><b>Stage 4: Type Inference & Pattern Detection</b></summary>

```
🧠 Inferring Types and Detecting Patterns...
├── Apply pattern matchers (email, phone, URL, date)
├── Calculate type confidence scores
├── Detect format consistency
├── Flag PII (with 30% threshold to prevent false positives)
└── Classify: integer, float, string, date, email, etc.
```
</details>

<details>
<summary><b>Stage 5: FIBO Semantic Tagging</b></summary>

```
🏦 FIBO Semantic Analysis...
├── Stage 1: Map from Visions type detection
├── Stage 2: Match against FIBO taxonomy patterns
│   ├── "transaction_amount" → money.amount
│   ├── "currency_code" → money.currency
│   └── "payment_method" → banking.payment
├── Stage 3: Refine with data properties
│   └── All values ≥ 0 → confirms money.amount
└── Assign confidence score (0-100%)
```

**Result:** Understand what data **means**, not just its type.
</details>

<details>
<summary><b>Stage 6: Quality Scoring</b></summary>

Calculate comprehensive metrics:
- Completeness (non-null %)
- Validity (type match %)
- Uniqueness (cardinality)
- Consistency (pattern match %)
- Overall quality (0-100 scale)
</details>

<details>
<summary><b>Stage 7: Correlation Analysis</b></summary>

For numeric columns:
- Calculate Pearson correlation
- Identify strong correlations (|r| > 0.7)
- Detect functional dependencies
- Apply statistical significance thresholds
</details>

<details>
<summary><b>Stage 8: Intelligent Validation Suggestions</b></summary>

```
💡 Generating Validation Suggestions...
├── FIBO-based (semantic intelligence)
│   ├── money.amount → NonNegativeCheck
│   ├── money.currency → CurrencyCodeCheck
│   └── banking.payment → ValidValuesCheck
├── Statistical (from data analysis)
│   ├── 100% unique → UniqueKeyCheck
│   └── Outliers detected → OutlierDetectionCheck
├── Pattern-based (from format detection)
│   ├── Email pattern → RegexCheck
│   └── Date format → DateFormatCheck
└── Completeness-based
    └── >95% complete → MandatoryFieldCheck
```
</details>

<details>
<summary><b>Stage 9: Report Generation</b></summary>

Create interactive HTML with:
- Executive summary
- FIBO semantic cards
- Statistical charts
- Quality scores
- Validation suggestions
- Auto-generated YAML
</details>

<details>
<summary><b>Stage 10: Configuration Export</b></summary>

Generate ready-to-use YAML config with all suggested validations.
</details>

---

## 🔧 Command Reference

### Basic Command

```bash
python3 -m validation_framework.cli profile <file_path>
```

### All Options

| Option | Description | Example |
|--------|-------------|---------|
| `-o`, `--output` | HTML report path | `-o profile.html` |
| `-c`, `--config` | YAML config path | `-c validation.yaml` |
| `-j`, `--json` | JSON export path | `-j profile.json` |
| `--beta-ml` | Enable ML anomaly detection | `--beta-ml` |
| `--full-analysis` | Disable internal sampling for ML (slower, more accurate) | `--full-analysis` |
| `--no-memory-check` | Disable memory safety termination (use with caution) | `--no-memory-check` |
| `--format` | Explicit format | `--format csv` |
| `--sample-rows` | Sample N rows | `--sample-rows 1000000` |
| `--sample-percent` | Sample N% rows | `--sample-percent 10` |
| `--chunk-size` | Rows per chunk (auto-calculated if omitted) | `--chunk-size 50000` |
| `--disable-pii` | Disable PII detection | `--disable-pii` |
| `--disable-temporal` | Disable temporal analysis | `--disable-temporal` |
| `--disable-correlation` | Disable correlation analysis | `--disable-correlation` |
| `--disable-all-enhancements` | Minimal profiling (fastest) | `--disable-all-enhancements` |

**Note:** Semantic tagging, PII detection, temporal analysis, and correlation are all enabled by default since v1.54.

### Common Examples

```bash
# 1. Basic profiling (all enhancements enabled by default)
python3 -m validation_framework.cli profile data.csv -o profile.html

# 2. Large file profiling (auto-optimized chunk size)
python3 -m validation_framework.cli profile huge.parquet -o profile.html

# 3. Large file with sampling (profile subset for quick overview)
python3 -m validation_framework.cli profile huge.parquet \
  --sample-rows 1000000 \
  -o quick_profile.html

# 4. Complete output (HTML + YAML + JSON)
python3 -m validation_framework.cli profile data.csv \
  -o profile.html \
  -c validation.yaml \
  -j profile.json

# 5. Custom chunk size for memory control
python3 -m validation_framework.cli profile large.csv \
  --chunk-size 25000 \
  -o profile.html

# 6. ML-based anomaly detection (beta)
python3 -m validation_framework.cli profile transactions.csv \
  --beta-ml \
  -o profile_with_ml.html

# 7. Full analysis mode (slower but more accurate ML)
python3 -m validation_framework.cli profile financial_data.parquet \
  --beta-ml \
  --full-analysis \
  -o full_analysis.html \
  -j analysis.json

# 8. Minimal profiling (fastest, disable all enhancements)
python3 -m validation_framework.cli profile data.csv \
  --disable-all-enhancements \
  -o minimal.html
```

**💡 Tips:**
- All enhancements (FIBO, PII, temporal, correlation) are **enabled by default** - no flags needed!
- Use `--beta-ml` when you want to detect outliers, clusters, and anomalies that basic profiling might miss
- Use `--full-analysis` with `--beta-ml` for comprehensive anomaly detection on large datasets
- Use `--disable-all-enhancements` when you only need basic statistics quickly

---

## 💎 Best Practices

### 1. Profile Before Validating

**Workflow:**
```
1. Profile data → Understand patterns
2. Review report → Identify issues
3. Customize config → Refine suggestions
4. Test validations → Validate with sample
5. Deploy → Run in production
```

### 2. FIBO Semantic Tagging is Automatic

For financial data, semantic tagging provides:
- ✓ Industry-standard validation rules
- ✓ Context-aware suggestions
- ✓ Better validation quality
- ✓ Plain-language explanations

```bash
# FIBO semantic tagging is enabled by default (v1.54+)
python3 -m validation_framework.cli profile transactions.csv -o profile.html

# To disable if not needed:
python3 -m validation_framework.cli profile non_financial_data.csv --disable-all-enhancements
```

### 3. Review and Customize Auto-Generated Configs

**Don't blindly use generated configs:**

```yaml
# Generated (from profiler)
- type: "RangeCheck"
  severity: "WARNING"          # ← Review: Should this be ERROR?
  params:
    field: "age"
    min_value: 0               # ← Review: Should be 18 for customers
    max_value: 150             # ← Review: Too high, use 120

# Customized (after review)
- type: "RangeCheck"
  severity: "ERROR"            # ← Changed to ERROR
  params:
    field: "age"
    min_value: 18              # ← Business rule
    max_value: 120             # ← Realistic maximum
```

### 4. Profile Regularly

**Schedule:**
- Initial deployment
- After data source changes
- Quarterly quality assessment
- When investigating issues
- Before major migrations

**Track changes over time** to detect data drift.

### 5. Combine Profiler Intelligence with Domain Knowledge

**Profiler provides:** Data reality
**You provide:** Business rules

**Best validations = Both combined.**

---

## 📚 Next Steps

**You've learned data profiling! Now:**

1. **[Configuration Guide](configuration-guide.md)** - Customize your validation config
2. **[Validation Catalog](validation-catalog.md)** - Explore all 35+ validations
3. **[Best Practices](best-practices.md)** - Production deployment tips
4. **[DEPENDENCIES.md](../for-developers/DEPENDENCIES.md)** - Licensing and FIBO details

---

## 🔗 Quick Links

- **[Example Reports](../../examples/sample_reports/)** - See FIBO semantic tagging in action
- **[FIBO Ontology](https://spec.edmcouncil.org/fibo/)** - Learn about FIBO (MIT License)
- **[Performance Guide](../guides/performance/profiler-memory-optimization.md)** - Optimize for large files

---

**🐕 Profile first, validate confidently - DataK9 guards with FIBO-powered intelligence**

**About FIBO:** DataK9 uses semantic concepts from FIBO (Financial Industry Business Ontology), an industry-standard maintained by the EDM Council under the MIT License.
