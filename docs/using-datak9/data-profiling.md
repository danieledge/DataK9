# Data Profiling Guide

**Know Your Data Before You Guard It**

DataK9's profiler analyzes your data files to understand their structure, quality, and characteristics. Like a K9 unit surveying the territory before patrol, profiling gives you the intelligence you need to build effective validations.

---

## 🎯 Live Demo Report

**See the profiler in action:** [View Titanic Dataset Profile Report](../samples/titanic_profile_demo.html)

This demo shows profiling of the classic Titanic passenger dataset (891 rows, 12 columns):

| Feature | What You'll See |
|---------|-----------------|
| **Executive Summary** | 78% overall quality score, key findings at a glance |
| **Semantic Classification** | `PassengerId` → identifier, `Survived` → Boolean flag, `Age` → Integer |
| **Smart Validations** | Age range 0-120 (not observed 0.42-80), BooleanCheck for Survived |
| **ML Anomaly Detection** | 64 outliers in Fare, Benford's Law analysis, autoencoder findings |
| **PII Detection** | No PII detected (historical dataset) |
| **Correlation Analysis** | Fare-Pclass correlation, survival patterns |
| **Sampling Explanation** | Full analysis (no sampling needed for 891 rows) |

**To generate your own:**
```bash
python3 -m validation_framework.cli profile your_data.csv -o profile.html
```

---

## 🆕 Recent Enhancements (v1.55+)

The DataK9 Profiler has received a significant overhaul with new capabilities:

- **Dual Semantic Classification** - FIBO (financial) + Schema.org (general) ontologies
- **Smart Validation Suggestions** - Semantic-aware rules that work across datasets
- **Binary Flag Detection** - Automatic Boolean classification for 0/1 columns
- **Intelligent Range Calculation** - Domain-aware bounds (e.g., Age: 0-120)
- **Executive HTML Reports** - Redesigned with plain-English explanations
- **Consolidated Sampling Banner** - Clear data analysis methodology

---

## 📋 Table of Contents

- [Quick Start](#-quick-start) - Get profiling in 30 seconds
- [Why Profile First?](#-why-profile-first) - The compelling reason
- [Key Features](#-key-features) - What makes DataK9's profiler unique
  - [Dual Semantic Classification](#-dual-semantic-classification-fibo--schemaorg) - NEW!
  - [FIBO Semantic Intelligence](#-fibo-semantic-intelligence)
  - [Schema.org General Semantics](#-schemaorg-general-semantics) - NEW!
  - [Smart Validation Suggestions](#-smart-validation-suggestions) - ENHANCED!
  - [Auto-Generated Validations](#-auto-generated-validations)
  - [Comprehensive Analysis](#-comprehensive-analysis)
  - [ML-Based Anomaly Detection](#-ml-based-anomaly-detection-beta)
  - [Local LLM Summarization](#-local-llm-summarization-experimental) - EXPERIMENTAL
  - [Intelligent Sampling](#-intelligent-sampling) - NEW!
  - [Memory-Efficient Processing](#-memory-efficient-processing)
- [Analytics Reference](#-analytics-reference) - All analysis types explained
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

### 🔄 Dual Semantic Classification (FIBO + Schema.org)

**What's New:** DataK9 now uses TWO complementary ontologies to understand your data:

| Ontology | Purpose | Best For |
|----------|---------|----------|
| **FIBO** | Financial Industry Business Ontology | Banking, transactions, currencies, accounts |
| **Schema.org** | General-purpose web vocabulary | Names, dates, emails, addresses, quantities |

**How It Works:**

```
Column: "Age"
├── Step 1: FIBO Analysis → No match (not financial)
├── Step 2: Schema.org Analysis → schema:Integer (85% confidence)
├── Resolution: Use Schema.org classification
└── Smart Validation: RangeCheck 0-120 (human-sensible bounds)

Column: "transaction_amount"
├── Step 1: FIBO Analysis → money.amount (95% confidence)
├── Step 2: Schema.org Analysis → schema:Number
├── Resolution: Use FIBO (domain-specific wins)
└── Smart Validation: Non-negative check (monetary rule)
```

**The Benefit:**
- Financial data gets FIBO's specialized rules
- Non-financial data gets Schema.org's broad coverage
- No columns left unclassified

---

### 🧠 FIBO Semantic Intelligence

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

### 🌐 Schema.org General Semantics

**What is Schema.org?**
Schema.org is a collaborative vocabulary used by Google, Microsoft, Yahoo, and Yandex. It provides standardized types for common data concepts.

**Schema.org Types DataK9 Detects:**

| Type | Description | Example Columns |
|------|-------------|-----------------|
| `schema:name` | Person or entity names | Name, CustomerName, FullName |
| `schema:email` | Email addresses | Email, ContactEmail |
| `schema:Integer` | Whole numbers | Age, Count, Quantity |
| `schema:Number` | Decimal numbers | Score, Rate, Percentage |
| `schema:Boolean` | True/false or 0/1 flags | Active, Survived, IsValid |
| `schema:Date` | Date values | BirthDate, CreatedAt |
| `schema:identifier` | Unique identifiers | PassengerId, CustomerID |

**Binary Flag Detection:**
Columns with only 0/1 or True/False values are automatically classified as `schema:Boolean`:
```
Column: "Survived" (values: 0, 1)
├── Detected: Binary flag (2 unique values)
├── Classification: schema:Boolean (85% confidence)
└── Suggested Validation: BooleanCheck (not ValidValuesCheck)
```

---

### 💡 Smart Validation Suggestions

**Semantic-Aware Rules That Work Across Datasets**

The profiler now generates validation suggestions based on semantic understanding, not just observed values:

| Field Type | Old Approach | New Approach |
|------------|--------------|--------------|
| **Age** | RangeCheck 0.42-80.0 (exact observed) | RangeCheck 0-120 (human-sensible) |
| **Fare/Price** | RangeCheck 0-512.33 (restrictive) | Non-negative only (no upper bound) |
| **Survived (0/1)** | ValidValuesCheck ['0', '1'] | BooleanCheck (semantic) |
| **Name** | UniqueKeyCheck (wrong!) | Excluded (names can duplicate) |
| **PassengerId** | UniqueKeyCheck (lucky guess) | UniqueKeyCheck (identifier type) |

**How Smart Range Calculation Works:**

```python
# Age fields: Human-sensible bounds
"Age" → RangeCheck(0, 120)  # Not the observed 0.42-80.0

# Monetary fields: Non-negative only
"Fare" → RangeCheck(0, None)  # No upper bound

# Percentage fields: Detect scale
"rate" → RangeCheck(0, 100)   # or (0, 1) if decimal

# Count fields: Non-negative with margin
"num_items" → RangeCheck(0, observed_max * 1.5)
```

**UniqueKeyCheck Intelligence:**

The profiler uses semantic classification to avoid false positives:

```
✅ Suggested: PassengerId (schema:identifier)
✅ Suggested: TransactionId (identifier type)
❌ Excluded: Name (schema:name - names can duplicate)
❌ Excluded: Amount (schema:MonetaryAmount - not an identifier)
❌ Excluded: Timestamp (temporal - not unique by nature)
```

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

### 🧪 Local LLM Summarization (Experimental)

**Status: BETA - Untested, Mixed Results**

DataK9 includes an experimental feature to generate AI-powered executive summaries using small local LLMs. This feature is **disabled by default** and should be considered experimental.

#### What It Does

When enabled, the profiler uses a local LLM (via `llama-cpp-python`) to generate a plain-English summary of the profile findings:

```
🤖 AI-Generated Summary [LOCAL LLM]

This dataset shows 87% overall quality with notable missing data in
the Cabin field (77%). The strong correlation between Fare and Pclass
suggests pricing tiers. Recommend adding MandatoryFieldCheck for
passenger identification and RangeCheck for Age validation.

⚠️ AI-generated using qwen2.5-1.5b • May contain inaccuracies • Review manually
```

#### Why It's Experimental

| Issue | Description |
|-------|-------------|
| **Mixed quality** | Small LLMs (0.5B-3B parameters) produce inconsistent summaries |
| **Hallucinations** | May generate plausible-sounding but incorrect insights |
| **Slow on CPU** | 15-90 seconds generation time depending on model |
| **No GPU acceleration** | Currently CPU-only for compatibility |
| **Model dependency** | Requires downloading 400MB-2GB GGUF model files |

**Our testing found:** Larger models (Phi-3, Qwen 1.5B) produce reasonable summaries ~70% of the time, but smaller models often generate generic or incorrect analysis.

#### Enabling the Feature

```bash
# Enable LLM summary (requires setup first)
python3 -m validation_framework.cli profile data.csv --beta-llm -o report.html
```

#### Setup Requirements

1. Install llama-cpp-python:
   ```bash
   pip install llama-cpp-python
   ```

2. Download a GGUF model (recommended: Qwen2.5-1.5B-Instruct):
   ```bash
   # Models are auto-discovered from common locations:
   # ~/.cache/huggingface/hub/
   # ~/.local/share/models/
   # ~/models/
   ```

3. Or set explicit path:
   ```bash
   export DATAK9_LLM_MODEL=/path/to/model.gguf
   ```

#### Recommended Models

| Model | Size | Speed | Quality |
|-------|------|-------|---------|
| Qwen2.5-1.5B-Instruct | ~1GB | ~18s | Best balance |
| Qwen2.5-0.5B-Instruct | ~400MB | ~8s | Faster, lower quality |
| Phi-3-mini-4k-instruct | ~2GB | ~90s | Best quality, slow |

#### When to Use

✅ **Use when:**
- You want automated summary drafts to review
- Processing many files and need quick overviews
- You have a suitable local model already installed

❌ **Don't use when:**
- You need accurate, reliable summaries
- Processing sensitive data (LLM may leak patterns)
- You don't have time to verify LLM output

**Bottom line:** This feature is provided as-is for experimentation. The rule-based analysis (FIBO, Schema.org, ML anomaly detection) provides more reliable insights.

---

### 📊 Intelligent Sampling

**Large datasets are profiled efficiently using statistical sampling.**

DataK9 uses smart sampling strategies that provide statistically equivalent results with significantly faster processing:

#### How Sampling Works

| File Size | Sampling Strategy | What It Means |
|-----------|-------------------|---------------|
| **< 100,000 rows** | Full analysis | Every row analyzed - no sampling |
| **100K - 1M rows** | 100K sample | Random sample provides reliable statistics |
| **> 1M rows** | Stratified sampling | Representative sample from across the file |
| **Large Parquet** | Row group stratification | Samples from each row group for coverage |

#### Why Sampling is Statistically Sound

**The math behind it:**

```
For a population of any size, a sample of 100,000 rows provides:
├── 99% confidence level
├── ±0.5% margin of error
└── Reliable detection of patterns appearing in ≥0.1% of data

Example: 10 million row file
├── Without sampling: ~15 minutes analysis time
├── With 100K sample: ~30 seconds
└── Statistical difference: < 0.5% on key metrics
```

**What gets sampled vs. counted:**

| Metric | Sampled | Full Count | Why |
|--------|---------|------------|-----|
| Row count | No | Yes | Exact count always provided |
| Null percentages | Yes | Extrapolated | Sample-based is reliable |
| Min/Max values | Yes | Yes | Tracked across all chunks |
| Unique count | Yes | Capped | Memory-efficient approximation |
| Distribution patterns | Yes | N/A | Statistical sampling is standard |
| ML anomaly detection | Yes | N/A | 100K sample is sufficient |

#### Report Banner

The HTML report includes a clear sampling banner at the top:

```
📊 Analysis Coverage
├── Full Dataset: 10,000,000 rows
├── Analyzed Sample: 100,000 rows (1.0%)
├── Method: Stratified random sampling
└── Confidence: Results provide reliable insights into the full dataset
```

**Expand "How was this data analyzed?" for detailed methodology.**

#### Controlling Sampling

```bash
# Force full analysis (slower, but no sampling)
python3 -m validation_framework.cli profile data.csv --full-analysis

# Custom sample size for ML
python3 -m validation_framework.cli profile data.csv --sample-rows 500000

# Percentage-based sampling
python3 -m validation_framework.cli profile data.csv --sample-percent 10
```

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

## 📊 Analytics Reference

**Complete reference for all analysis types performed by the DataK9 Profiler.**

### Core Analytics (Always Enabled)

| Analysis | What It Does | Output |
|----------|--------------|--------|
| **Type Inference** | Detects data types (integer, float, string, date, boolean) using pattern matching and statistical analysis | Inferred type, confidence score |
| **Basic Statistics** | Calculates count, null%, min, max, mean, median, std dev, quartiles | Per-column statistics |
| **Cardinality Analysis** | Counts unique values, calculates uniqueness ratio | Unique count, cardinality % |
| **Top Values** | Identifies most frequent values and their counts | Top 10 values with frequencies |
| **Quality Scoring** | Computes completeness, validity, uniqueness, consistency metrics | Overall quality score (0-100) |
| **Pattern Detection** | Detects common patterns: email, phone, URL, date formats, SSN, credit card | Pattern type, match percentage |

### Semantic Analysis (Default: Enabled)

| Analysis | What It Does | CLI Flag to Disable |
|----------|--------------|---------------------|
| **FIBO Tagging** | Maps columns to Financial Industry Business Ontology concepts | `--disable-all-enhancements` |
| **Schema.org Classification** | Maps columns to Schema.org vocabulary types | `--disable-all-enhancements` |
| **Semantic Resolution** | Combines FIBO + Schema.org with confidence-based priority | `--disable-all-enhancements` |
| **Binary Flag Detection** | Identifies 0/1, True/False, Y/N columns as Boolean | `--disable-all-enhancements` |

**FIBO Semantic Tags (28 available):**
- **Money:** `money.amount`, `money.currency`, `money.price`, `money.balance`
- **Banking:** `banking.account`, `banking.transaction`, `banking.payment`, `banking.routing`
- **Party:** `party.customer_id`, `party.counterparty`, `party.name`
- **Temporal:** `temporal.transaction_date`, `temporal.timestamp`
- **Category:** `category.transaction_type`, `category.payment_method`
- **Identifier:** `identifier.code`, `identifier.reference`

**Schema.org Types:**
- `schema:name`, `schema:email`, `schema:Integer`, `schema:Number`, `schema:Boolean`
- `schema:Date`, `schema:DateTime`, `schema:identifier`, `schema:Text`

### PII Detection (Default: Enabled)

| PII Type | Detection Method | Threshold |
|----------|------------------|-----------|
| **Email** | Regex pattern matching | >30% of values match |
| **Phone** | Multiple format patterns (US, intl) | >30% of values match |
| **SSN** | XXX-XX-XXXX pattern | >10% of values match |
| **Credit Card** | Luhn algorithm + format | >10% of values match |
| **IP Address** | IPv4/IPv6 patterns | >30% of values match |

```bash
# Disable PII detection
python3 -m validation_framework.cli profile data.csv --disable-pii
```

### Correlation Analysis (Default: Enabled)

| Analysis | What It Does | When Applied |
|----------|--------------|--------------|
| **Pearson Correlation** | Measures linear relationship between numeric columns | Pairs of numeric columns |
| **Strong Correlation Detection** | Flags pairs with \|r\| > 0.7 | All numeric pairs |
| **Correlation Matrix** | Full pairwise correlation matrix | Up to 20 columns |

```bash
# Disable correlation analysis
python3 -m validation_framework.cli profile data.csv --disable-correlation
```

### Temporal Analysis (Default: Enabled)

| Analysis | What It Does | Output |
|----------|--------------|--------|
| **Date Range Detection** | Finds earliest and latest dates | Min/max dates |
| **Gap Analysis** | Identifies missing time periods | Gap count, duration |
| **Trend Detection** | Identifies increasing/decreasing patterns | Trend direction |
| **Seasonality** | Detects recurring patterns | Seasonal indicators |

```bash
# Disable temporal analysis
python3 -m validation_framework.cli profile data.csv --disable-temporal
```

### ML-Based Analysis (Default: Disabled)

**Enable with `--beta-ml` flag.**

| Analysis | Algorithm | What It Detects |
|----------|-----------|-----------------|
| **Univariate Outliers** | Isolation Forest | Individual values that deviate significantly |
| **Multivariate Outliers** | Isolation Forest (multi-dim) | Unusual combinations of values |
| **Cluster Analysis** | DBSCAN | Natural groupings and noise points |
| **Benford's Law** | Chi-square test | Potentially fabricated/synthetic numeric data |
| **Autoencoder Anomalies** | Neural network reconstruction | Records with unusual patterns |
| **Rare Category Detection** | Frequency analysis | Suspiciously infrequent categorical values |

```bash
# Enable ML analysis
python3 -m validation_framework.cli profile data.csv --beta-ml

# Full ML analysis (no internal sampling)
python3 -m validation_framework.cli profile data.csv --beta-ml --full-analysis
```

### Validation Suggestion Generation

| Source | What It Generates | Example |
|--------|-------------------|---------|
| **Completeness** | MandatoryFieldCheck for >95% complete fields | `fields: [customer_id, email]` |
| **Uniqueness** | UniqueKeyCheck for 100% unique identifier columns | `fields: [transaction_id]` |
| **Cardinality** | ValidValuesCheck for low cardinality (<20 values) | `valid_values: [A, B, C]` |
| **Semantic (FIBO)** | Domain-specific checks from ontology | NonNegativeCheck for money.amount |
| **Semantic (Schema.org)** | Type-appropriate checks | BooleanCheck for schema:Boolean |
| **Range (Smart)** | Semantic-aware range bounds | Age: 0-120, not observed 0.42-80.0 |
| **Pattern** | RegexCheck for detected formats | Email regex pattern |
| **ML Findings** | OutlierCheck, StatisticalOutlierCheck | Based on detected anomalies |

### Analysis Flags Summary

| Flag | Effect |
|------|--------|
| `--disable-pii` | Skip PII detection |
| `--disable-temporal` | Skip temporal analysis |
| `--disable-correlation` | Skip correlation analysis |
| `--disable-all-enhancements` | Minimal profiling (basic stats only) |
| `--beta-ml` | Enable ML-based anomaly detection |
| `--beta-llm` | Enable experimental LLM summaries |
| `--full-analysis` | Disable internal sampling for ML |

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
