# Data Profiling Guide

**Know Your Data Before You Guard It**

DataK9's profiler analyzes your data files to understand their structure, quality, and characteristics. Like a K9 unit surveying the territory before patrol, profiling gives you the intelligence you need to build effective validations.

---

## 📋 Table of Contents

- [Quick Start](#-quick-start) - Get profiling in 30 seconds
- [Why Profile First?](#-why-profile-first) - The compelling reason
- [Key Features](#-key-features) - What makes DataK9's profiler unique
- [Understanding Reports](#-understanding-reports) - Read your profile results
- [How It Works](#-how-it-works) - Under the hood (10 stages)
- [Command Reference](#-command-reference) - All options and examples
- [Best Practices](#-best-practices) - Tips for success

---

## ⚡ Quick Start

**30 seconds to your first profile:**

```bash
# Basic profiling
python3 -m validation_framework.cli profile data.csv

# With FIBO semantic intelligence (recommended for financial data)
python3 -m validation_framework.cli profile transactions.csv --enable-semantic-tagging

# Generated files:
# ✓ data_profile_report.html    (Interactive visual report)
# ✓ data_validation.yaml         (Auto-generated validation config)
```

**[View Example Report →](../../examples/reports/profiler_example_with_semantic_tagging.html)**

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
- Range-Level (RangeCheck, OutlierDetectionCheck)
- **FIBO-Level** (Semantic intelligence)

**Each suggestion includes:**
- Validation type
- Severity recommendation (not a badge!)
- Parameters (auto-filled from data)
- Confidence score
- Reasoning
- Copy-paste YAML snippet
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
| `--enable-semantic-tagging` | Enable FIBO analysis | `--enable-semantic-tagging` |
| `--format` | Explicit format | `--format csv` |
| `--sample-rows` | Sample N rows | `--sample-rows 1000000` |
| `--sample-percent` | Sample N% rows | `--sample-percent 10` |
| `--chunk-size` | Rows per chunk | `--chunk-size 50000` |

### Common Examples

```bash
# 1. Basic profiling
python3 -m validation_framework.cli profile data.csv

# 2. Financial data with FIBO semantic tagging (recommended)
python3 -m validation_framework.cli profile transactions.csv \
  --enable-semantic-tagging \
  -o profile.html

# 3. Large file with sampling
python3 -m validation_framework.cli profile huge.parquet \
  --sample-rows 1000000 \
  --enable-semantic-tagging

# 4. Complete output (HTML + YAML + JSON)
python3 -m validation_framework.cli profile data.csv \
  --enable-semantic-tagging \
  -o profile.html \
  -c validation.yaml \
  -j profile.json

# 5. Custom chunk size for memory control
python3 -m validation_framework.cli profile large.csv \
  --chunk-size 25000
```

**💡 Tip:** Always use `--enable-semantic-tagging` for financial datasets to get FIBO intelligence!

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

### 2. Start with FIBO Semantic Tagging

For financial data, semantic tagging provides:
- ✓ Industry-standard validation rules
- ✓ Context-aware suggestions
- ✓ Better validation quality
- ✓ Plain-language explanations

```bash
# Always use --enable-semantic-tagging for financial data
python3 -m validation_framework.cli profile transactions.csv --enable-semantic-tagging
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

- **[Example Report](../../examples/reports/profiler_example_with_semantic_tagging.html)** - See FIBO semantic tagging in action
- **[FIBO Ontology](https://spec.edmcouncil.org/fibo/)** - Learn about FIBO (MIT License)
- **[Performance Guide](../guides/performance/profiler-memory-optimization.md)** - Optimize for large files

---

**🐕 Profile first, validate confidently - DataK9 guards with FIBO-powered intelligence**

**About FIBO:** DataK9 uses semantic concepts from FIBO (Financial Industry Business Ontology), an industry-standard maintained by the EDM Council under the MIT License.
