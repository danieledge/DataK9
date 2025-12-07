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

## 🆕 Recent Enhancements (v1.56+)

The DataK9 Profiler has received a significant overhaul with new capabilities:

### Semantic Intelligence
- **Four-Layer Semantic Classification** ⭐ NEW - FIBO (finance) + Schema.org (general) + Wikidata (knowledge) + Science (QUDT/ChEBI/UO)
- **Configurable Semantic Resolution** - Priority-based resolution with confidence thresholds in `semantic_config.yaml`
- **Identifier Detection** - Automatic classification of ID columns using semantic metadata

### Analysis Enhancements
- **PCA/Dimensionality Reduction** ⭐ NEW - 2D visualization with explained variance and feature loadings
- **Column Family Detection** ⭐ NEW - Groups similar columns in wide datasets (50+ columns)
- **Categorical Association Analysis** ⭐ NEW - Cramér's V for categorical×categorical, point-biserial for binary×numeric
- **Target/Outcome Detection** - Automatic identification of likely prediction targets
- **Correlation Insight Synthesis** - Data-driven headlines with actual values and multipliers

### Report Quality
- **Field Descriptions (Context YAML)** - Provide friendly names and value labels for human-readable reports
- **Context-Aware Anomaly Detection** - Explains outliers in context of subgroups
- **Soft Language for Weak Correlations** - Uses appropriate language for |r| < 0.3
- **Benford's Law Context** - Appropriate risk language for transactional vs non-transactional data
- **Identifier Filtering** - Correlations involving ID columns automatically suppressed

### Infrastructure
- **Data Lineage Tracking** - Full provenance with file hash, timestamps, environment info
- **Configurable Memory Safety** - Adjustable thresholds with graceful termination
- **Chunked ML Accumulation** - Memory-efficient streaming for large datasets

---

## 📋 Table of Contents

- [Quick Start](#-quick-start) - Get profiling in 30 seconds
- [Why Profile First?](#-why-profile-first) - The compelling reason
- [Key Features](#-key-features) - What makes DataK9's profiler unique
  - [Four-Layer Semantic Classification](#-four-layer-semantic-classification) - NEW!
  - [FIBO Semantic Intelligence](#-fibo-semantic-intelligence)
  - [Schema.org General Semantics](#-schemaorg-general-semantics)
  - [Wikidata Knowledge Semantics](#-wikidata-knowledge-semantics) - NEW!
  - [Science Ontology Semantics](#-science-ontology-semantics-qudt-chebi-uo) - NEW!
  - [PCA/Dimensionality Reduction](#-pcadimensionality-reduction) - NEW!
  - [Column Family Detection](#-column-family-detection) - NEW!
  - [Categorical Association Analysis](#-categorical-association-analysis) - NEW!
  - [Correlation Insight Synthesis](#-correlation-insight-synthesis) - NEW!
  - [Smart Validation Suggestions](#-smart-validation-suggestions) - ENHANCED!
  - [Auto-Generated Validations](#-auto-generated-validations)
  - [Comprehensive Analysis](#-comprehensive-analysis)
  - [ML-Based Anomaly Detection](#-ml-based-anomaly-detection-beta)
  - [Intelligent Sampling](#-intelligent-sampling)
  - [Memory-Efficient Processing](#-memory-efficient-processing)
- [Analysis Decision Logic](#-analysis-decision-logic) - When each analysis runs - NEW!
- [Analytics Reference](#-analytics-reference) - All analysis types explained
- [Understanding Reports](#-understanding-reports) - Read your profile results
- [How It Works](#-how-it-works) - Under the hood
- [Command Reference](#-command-reference) - All options and examples
  - [Field Descriptions (Context YAML)](#field-descriptions-file-context-yaml-) - Human-readable insights
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

### 🔄 Four-Layer Semantic Classification

**What's New:** DataK9 uses FOUR complementary ontologies to understand your data with configurable priority resolution:

| Layer | Ontology | Purpose | Best For |
|-------|----------|---------|----------|
| 1 (Highest) | **FIBO** | Financial Industry Business Ontology | Banking, transactions, currencies, accounts |
| 2 | **Science** | QUDT, ChEBI, UO ontologies | Measurements, chemicals, scientific units |
| 3 | **Wikidata** | General knowledge types | Geographic codes, reference data, entities |
| 4 (Baseline) | **Schema.org** | General-purpose web vocabulary | Names, dates, emails, addresses, quantities |

**Resolution Priority:**

The `SemanticResolver` combines all four layers using configurable confidence thresholds:

```yaml
# semantic_config.yaml (configurable thresholds)
resolution:
  fibo_min_conf: 0.7      # FIBO wins at 70%+ confidence
  science_min_conf: 0.6   # Science wins at 60%+ (if no strong FIBO)
  wikidata_min_conf: 0.6  # Wikidata wins at 60%+ (if no strong FIBO/Science)
  schema_min_conf: 0.5    # Schema.org baseline
```

**How Resolution Works:**

```
Column: "transaction_amount"
├── Layer 1: FIBO → money.amount (95% confidence) ✓ WINS
├── Layer 2: Science → No match
├── Layer 3: Wikidata → No match
├── Layer 4: Schema.org → schema:Number (70%)
├── Resolution: FIBO (highest priority with 95% > 70%)
└── Validation Driver: fibo (monetary rules apply)

Column: "temperature_celsius"
├── Layer 1: FIBO → No match
├── Layer 2: Science → qudt:Temperature (85% confidence) ✓ WINS
├── Layer 3: Wikidata → No match
├── Layer 4: Schema.org → schema:Number (60%)
├── Resolution: Science (no FIBO, science > threshold)
└── Validation Driver: science (unit-aware rules apply)

Column: "country_code"
├── Layer 1: FIBO → No match
├── Layer 2: Science → No match
├── Layer 3: Wikidata → wd:Country (80% confidence) ✓ WINS
├── Layer 4: Schema.org → schema:Text (50%)
├── Resolution: Wikidata (no FIBO/Science, wikidata > threshold)
└── Validation Driver: wikidata (ISO code validation applies)
```

**The Benefit:**
- Financial data gets FIBO's specialized rules (highest priority)
- Scientific measurements get QUDT/ChEBI precision
- Geographic/reference data gets Wikidata knowledge
- Everything else gets Schema.org's broad coverage
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

### 🌐 Wikidata Knowledge Semantics

**What is Wikidata?**
Wikidata is a free knowledge base maintained by the Wikimedia Foundation, containing structured data for millions of entities, places, and concepts.

**Wikidata Types DataK9 Detects:**

| Type | Description | Example Columns |
|------|-------------|-----------------|
| `wd:Country` | ISO country codes and names | country, country_code, nationality |
| `wd:Region` | Geographic regions/states | state, province, region |
| `wd:City` | City/municipality names | city, town, municipality |
| `wd:Language` | Language codes and names | language, locale |
| `wd:Organization` | Company/organization names | company, employer, organization |

**When Wikidata Takes Priority:**

Wikidata wins when:
- FIBO has no match or low confidence
- Science has no match
- Wikidata confidence ≥ 60% (configurable)

---

### 🔬 Science Ontology Semantics (QUDT, ChEBI, UO)

**What are these ontologies?**

| Ontology | Purpose | Examples |
|----------|---------|----------|
| **QUDT** | Quantities, Units, Dimensions | Temperature, pressure, velocity, mass |
| **ChEBI** | Chemical Entities of Biological Interest | Alcohol content, pH, chemical concentrations |
| **UO** | Units Ontology | Measurement units and conversions |

**Science Types DataK9 Detects:**

| Type | Description | Example Columns |
|------|-------------|-----------------|
| `qudt:Temperature` | Temperature measurements | temperature, temp_celsius, temp_f |
| `qudt:Pressure` | Pressure readings | pressure, psi, bar |
| `qudt:Mass` | Mass/weight values | weight, mass, kg |
| `qudt:Length` | Length/distance | height, width, distance |
| `qudt:Velocity` | Speed measurements | speed, velocity, mph |
| `chebi:Alcohol` | Alcohol content | alcohol, abv, alcohol_content |
| `chebi:pH` | Acidity/basicity | ph, ph_level, acidity |

**When Science Takes Priority:**

Science wins when:
- FIBO has no match or low confidence
- Science confidence ≥ 60% (configurable)
- Column patterns match scientific measurement naming

---

### 📊 PCA/Dimensionality Reduction

**What is PCA Analysis?**
Principal Component Analysis reduces high-dimensional data to 2D for visualization, showing how records cluster and which features drive the most variation.

**When PCA Runs:**
- Dataset has ≥ 10 rows
- Dataset has ≥ 3 numeric columns
- ML analysis is enabled

**What PCA Provides:**

| Output | Description |
|--------|-------------|
| **2D Projection** | Scatter plot showing record positions in reduced space |
| **Explained Variance** | How much information each component captures (e.g., PC1: 45%, PC2: 23%) |
| **Feature Loadings** | Which columns contribute most to each component |
| **Top Contributors** | Top 5 features for each principal component |
| **Outlier Separation** | Whether outliers cluster separately in reduced space |

**Example Output:**
```
PCA Analysis Results
├── Explained Variance: PC1 (45.2%), PC2 (23.1%) = 68.3% total
├── Top Contributors to PC1:
│   ├── income: 0.82
│   ├── age: 0.71
│   └── credit_score: 0.68
├── Top Contributors to PC2:
│   ├── account_tenure: 0.76
│   └── transaction_count: 0.54
└── Outlier Separation: Detected (outliers cluster in lower-left quadrant)
```

---

### 📁 Column Family Detection

**What is Column Family Detection?**
For wide datasets (50+ columns), the profiler automatically groups similar columns into "families" based on naming patterns.

**When It Runs:**
- Dataset has > 50 columns (configurable threshold)
- Pattern detection identifies ≥ 5 columns per family

**Pattern Types Detected:**

| Pattern Type | Description | Example |
|--------------|-------------|---------|
| **Date Columns** | M/D/YY, YYYY-MM-DD formats in column names | 1/22/20, 2020-01-22 (COVID time series) |
| **Numeric Sequences** | col_1, col_2, col_3... or feature_001, feature_002... | Sensor readings, questionnaire items |
| **Prefix Groups** | Common prefixes | sales_Q1, sales_Q2, sales_Q3, sales_Q4 |
| **Suffix Groups** | Common suffixes | revenue_2020, revenue_2021, revenue_2022 |
| **Dtype Families** | Columns with identical types and similar statistics | All float columns with similar ranges |

**Benefits:**
- Reduces report clutter for wide datasets
- Identifies time series date columns automatically
- Provides aggregate statistics per family
- Flags anomalous columns within families

---

### 📈 Categorical Association Analysis

**Beyond Pearson Correlation**
Standard Pearson correlation only works for numeric×numeric relationships. Categorical analysis extends this to all column type combinations.

**Methods Used:**

| Method | Relationship Type | What It Measures |
|--------|-------------------|------------------|
| **Cramér's V** | Categorical × Categorical | Association strength (0 = independent, 1 = perfectly associated) |
| **Point-Biserial** | Binary × Numeric | Correlation between a 0/1 flag and numeric column |
| **Variance Explained** | Categorical → Numeric | How much a categorical grouping explains numeric variation |

**When It Runs:**
- At least 2 categorical columns (for Cramér's V)
- At least 1 binary column and 1 numeric column (for point-biserial)
- Maximum 50 columns analyzed pairwise (configurable limit)

**Target/Outcome Detection:**
The analyzer automatically identifies likely target columns for ML using:
- Past-tense verb patterns (e.g., "Survived", "Churned")
- Keyword matching (e.g., "class", "target", "outcome", "label")
- Suffix patterns (e.g., "_class", "_target", "_label")
- Binary flag patterns in combination with predictive associations

---

### 🔗 Correlation Insight Synthesis

**Data-Driven Insights, Not Generic Descriptions**

The correlation insight synthesizer transforms raw statistics into actionable, value-based insights.

**What Gets Synthesized:**

| Input | Output |
|-------|--------|
| r = 0.72 between Age and Income | "Records with Age above median tend to have 2.3x higher Income" |
| Cramér's V = 0.55 between Class and Outcome | "1st Class records show 67% positive outcome rate vs 24% for 3rd Class" |
| Variance explained = 0.42 | "Knowing the categorical group explains 42% of the variation in the numeric column" |

**Identifier Filtering:**
Correlations involving identifier columns (detected via semantic metadata) are automatically suppressed from insights, as they represent structural relationships, not predictive signals.

**Weak Correlation Language:**
For correlations with |r| < 0.3, softer language is used:
- "Weak relationship" instead of "Strong correlation"
- "slightly higher" instead of definitive statements
- Focus on statistical uncertainty

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

## 🧭 Analysis Decision Logic

**How the Profiler Decides What to Run**

The profiler makes intelligent decisions about which analyses to apply based on data characteristics. This section documents the triggers, thresholds, and conditions for each analysis type.

### Semantic Analysis Decisions

| Analysis | Trigger Condition | Skip Condition |
|----------|-------------------|----------------|
| **FIBO Tagging** | Always runs | Disabled via `--disable-all-enhancements` |
| **Schema.org Tagging** | Always runs (baseline) | Disabled via `--disable-all-enhancements` |
| **Wikidata Tagging** | Always runs | Module not installed |
| **Science Tagging** | Always runs | Module not installed |
| **Semantic Resolution** | All taggers complete | Any tagger unavailable |

### Correlation Analysis Decisions

| Analysis | Trigger Condition | Skip Condition |
|----------|-------------------|----------------|
| **Pearson Correlation** | ≥ 2 numeric columns | `--disable-correlation` or > 20 columns (uses sampling) |
| **Cramér's V** | ≥ 2 categorical columns (cardinality ≤ 20) | scipy unavailable or > 50 columns |
| **Point-Biserial** | ≥ 1 binary + ≥ 1 numeric column | scipy unavailable |
| **Correlation Insights** | Correlation results available | No significant correlations (|r| < threshold) |

**Identifier Column Filtering:**
Correlations involving columns classified as identifiers (via semantic metadata) are automatically suppressed from insight generation. Detection uses:
- `resolved.primary_type` containing "identifier" or "id"
- `semantic_tags` containing "identifier"

### ML Analysis Decisions

| Analysis | Trigger Condition | Skip Condition |
|----------|-------------------|----------------|
| **Isolation Forest (Univariate)** | ≥ 500 rows, numeric column | `--no-ml` or binary column |
| **Isolation Forest (Multivariate)** | ≥ 500 rows, ≥ 2 numeric columns | `--no-ml` or all columns are identifiers |
| **DBSCAN Clustering** | ≥ 500 rows, ≥ 2 numeric columns | `--no-ml` |
| **Benford's Law** | Numeric column with ≥ 100 values | Column is identifier, bounded (0-100), binary, or negative |
| **Rare Category Detection** | Categorical with ≥ 5 unique values | Column is identifier (semantic) |
| **Autoencoder** | ≥ 500 rows, mixed columns | tensorflow/keras unavailable |

**Benford's Law Eligibility:**
A column is **excluded** from Benford analysis if:
- Classified as identifier (semantic_type contains "id" or "identifier")
- Has bounded range (0-100 like percentages or ages)
- Contains significant negative values
- Is binary (≤ 3 unique values)
- Has average/ratio naming patterns (avg_, mean_, ratio_, per_)

### PCA Analysis Decisions

| Condition | Threshold | Behavior |
|-----------|-----------|----------|
| Minimum rows | 10 | Skip if fewer rows |
| Minimum numeric columns | 3 | Skip if fewer columns |
| Maximum features | 50 | Select top 50 by variance |
| Missing values | Drops rows with NaN | Must have ≥ 10 complete rows |

### Column Family Detection Decisions

| Condition | Threshold | Behavior |
|-----------|-----------|----------|
| Wide dataset trigger | > 50 columns | Activate family detection |
| Minimum family size | 5 columns | Only report families with ≥ 5 members |
| Sample size | 10 columns | Profile representative sample per family |

**Pattern Priority Order:**
1. Date-formatted column names (M/D/YY, YYYY-MM-DD)
2. Numeric sequences (col_1, col_2...)
3. Prefix/suffix groups
4. Dtype similarity families

### Memory Safety Decisions

| Metric | Warning Threshold | Critical Threshold | Behavior |
|--------|-------------------|-------------------|----------|
| System memory usage | 70% | 80% | Warn → Terminate |
| Check interval | Every 5 chunks | - | Log memory status |
| Disable safety | `--no-memory-check` | - | Continue despite high memory |

### Sampling Decisions

| File Size | Analysis Sampling | ML Sampling |
|-----------|-------------------|-------------|
| < 100K rows | Full analysis | Full analysis |
| 100K - 1M rows | Full analysis | 100K sample (configurable) |
| > 1M rows | Stratified sample | 100K sample (configurable) |
| `--full-analysis` | Full analysis | Full analysis (slower) |

**Configurable via:**
- `--analysis-sample-size N` - Set sample size threshold
- `--full-analysis` - Disable all internal sampling

### Validation Suggestion Decisions

| Suggestion Type | Trigger Condition | Severity |
|-----------------|-------------------|----------|
| **MandatoryFieldCheck** | Completeness > 95% | ERROR |
| **UniqueKeyCheck** | 100% unique + identifier semantic type | ERROR |
| **RangeCheck** | Numeric column with defined bounds | WARNING |
| **BooleanCheck** | Binary column (schema:Boolean) | ERROR |
| **RegexCheck** | Pattern detected in > 90% of values | WARNING |
| **ValidValuesCheck** | Categorical with ≤ 20 values | WARNING |

**Identifier Exclusions:**
- UniqueKeyCheck is suggested only for columns with identifier semantic type
- Names (schema:name) are excluded even if 100% unique
- Timestamps are excluded even if unique

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
| `--no-ml` | Disable ML-based anomaly detection |
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
| `--field-descriptions` | YAML file with friendly names and value labels | `--field-descriptions fields.yaml` |
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

### Field Descriptions File (Context YAML) ⭐

The `--field-descriptions` option enables **context-aware profiling** by providing friendly names, descriptions, and value labels for your columns. This transforms cryptic column names and codes into human-readable insights.

#### Why Use Field Descriptions?

| Challenge | Solution |
|-----------|----------|
| Cryptic column names like `SibSp`, `Parch` | Friendly names: "Siblings/Spouses", "Parents/Children" |
| Coded values like `1`, `2`, `3` | Value labels: "1st Class", "2nd Class", "3rd Class" |
| Confusing anomaly explanations | Context-aware insights that explain *why* values are unusual |
| Technical correlation outputs | Business-friendly relationship descriptions |

#### YAML Format

```yaml
# field_descriptions.yaml
field_descriptions:
  # Simple field with friendly name only
  PassengerId:
    friendly_name: "Passenger ID"

  # Field with name and description
  Age:
    friendly_name: "Age"
    description: "Age of passenger in years"

  # Field with coded values (categorical)
  Pclass:
    friendly_name: "Passenger Class"
    description: "Ticket class indicating travel accommodation level"
    value_labels:
      "1": "1st Class"
      "2": "2nd Class"
      "3": "3rd Class"

  # Binary/flag field
  Survived:
    friendly_name: "Survival Status"
    value_labels:
      "0": "Did Not Survive"
      "1": "Survived"

  # Field with abbreviated name
  SibSp:
    friendly_name: "Siblings/Spouses"
    description: "Number of siblings and spouses aboard"

  Parch:
    friendly_name: "Parents/Children"
    description: "Number of parents and children aboard"

  # Location codes
  Embarked:
    friendly_name: "Port of Embarkation"
    description: "Port where passenger boarded the ship"
    value_labels:
      "S": "Southampton"
      "C": "Cherbourg"
      "Q": "Queenstown"

  # Monetary field
  Fare:
    friendly_name: "Ticket Fare"
    description: "Price paid for ticket in British pounds"
```

#### Usage

```bash
python3 -m validation_framework.cli profile data.csv \
  --field-descriptions field_descriptions.yaml \
  -o profile.html
```

#### How Reports Improve

**Correlation Insights:**

| Without Field Descriptions | With Field Descriptions |
|---------------------------|------------------------|
| "Pclass = 1 shows 7.5x higher Fare than Pclass = 3" | "1st Class shows 7.5x higher Ticket Fare than 3rd Class" |
| "Strong correlation between Pclass and Fare" | "Strong correlation between Passenger Class and Ticket Fare" |
| "SibSp negatively correlates with Age" | "Siblings/Spouses negatively correlates with Age" |

**Anomaly Explanations:**

| Without Context | With Context |
|-----------------|--------------|
| "Fare value 512.33 is 3.8σ above mean" | "Ticket Fare of £512.33 is unusually high but **normal for 1st Class passengers** (avg Fare for Passenger Class=1st Class is £84.15)" |
| "Age outlier detected: 0.42" | "Age of 0.42 years detected - infant passenger" |

#### Context-Aware Anomaly Detection

When you provide field descriptions, the profiler performs **subgroup pattern discovery**:

1. **Identifies categorical segmentation** - Discovers how categorical columns (like Pclass) segment numeric columns (like Fare)
2. **Calculates subgroup statistics** - Computes mean, std, quartiles for each segment
3. **Contextualizes outliers** - A £500 fare is an outlier overall, but expected for 1st Class

**Example Output:**
```
Value Analysis: Fare = £512.33

Without context:
  ⚠️ Outlier: 3.8 standard deviations above mean (£32.20)

With context (Pclass = 1st Class):
  ✓ Normal for segment: Within 2.0σ of 1st Class mean (£84.15)
  → Explanation: "Normal for Passenger Class=1st Class (avg Ticket Fare=£84.15 for this group)"
```

#### Correlation Insight Synthesis

Field descriptions also improve correlation insights by:
- Using friendly names in all explanations
- Translating value codes to labels in breakdowns
- Making statistical findings accessible to non-technical stakeholders

```
Without: "Pearson r=-0.55 between Pclass and Fare (p<0.001)"

With: "Passenger Class strongly correlates with Ticket Fare:
       1st Class passengers paid 7.5x more on average than 3rd Class"
```

#### Example File

See `examples/titanic_field_descriptions.yaml` for a complete working example:

```bash
# Profile Titanic dataset with context
python3 -m validation_framework.cli profile titanic.csv \
  --field-descriptions examples/titanic_field_descriptions.yaml \
  -o titanic_profile.html
```

#### Best Practices

1. **Start with key columns** - Focus on columns with cryptic names or coded values
2. **Include value_labels for categoricals** - Especially important for numeric codes (1, 2, 3)
3. **Add descriptions for domain context** - Help explain what the data represents
4. **Reuse across datasets** - Create standard context files for common data structures
5. **Update when schema changes** - Keep field descriptions in sync with your data

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
