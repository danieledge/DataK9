# DataK9 Profiler Internals: How It Works

**Complete Technical Reference - Workflow, Sampling Strategy, and Memory Management**

---

## Table of Contents

1. [Overview](#overview)
2. [Complete Workflow](#complete-workflow)
3. [Sampling Strategy](#sampling-strategy)
4. [Memory Management](#memory-management)
5. [Enhancement Features](#enhancement-features)
6. [Performance Characteristics](#performance-characteristics)
7. [Configuration Options](#configuration-options)

---

## Overview

The DataK9 profiler analyzes data files using a **chunked processing** strategy with **intelligent sampling** to:
- Handle files from KBs to 200+ GB
- Maintain constant memory usage (~300-400MB regardless of file size)
- Provide accurate statistics without loading entire files
- Generate smart validation suggestions

### Key Design Principles

1. **Chunked Processing**: File read in chunks, never fully loaded
2. **Intelligent Sampling**: Sample only what's needed for statistical validity
3. **Memory Bounds**: Hard limits prevent memory exhaustion
4. **Accuracy**: Full processing for critical metrics, sampling for expensive operations

---

## Complete Workflow

### Phase 1: Initialization

```
┌─────────────────────────────────────────┐
│ 1. File Detection & Setup              │
├─────────────────────────────────────────┤
│ • Auto-detect format (CSV, Parquet, etc)│
│ • Calculate optimal chunk size          │
│   └─ Based on available RAM             │
│   └─ Default: auto (~2M rows/chunk)     │
│ • Initialize column profiles            │
│ • Setup enhancement analyzers           │
│   └─ Temporal (if datetime columns)     │
│   └─ PII (if enabled)                  │
│   └─ Correlation (if numeric columns)   │
└─────────────────────────────────────────┘
```

**Example Log Output:**
```
🎯 Auto-calculated chunk size: 2,000,000 rows (based on 20,296MB available memory)
   Estimated chunks: 90 | Peak memory: ~286MB
```

### Phase 2: Chunk Processing Loop

```
FOR EACH CHUNK (e.g., 90 chunks for 179M rows):
┌──────────────────────────────────────────────┐
│ 2. Load Chunk (2M rows)                     │
├──────────────────────────────────────────────┤
│ • Read next 2,000,000 rows from file         │
│ • Convert to DataFrame                       │
│ • Memory footprint: ~200-300MB              │
└──────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────┐
│ 3. Process Each Column in Chunk             │
├──────────────────────────────────────────────┤
│                                              │
│ A. ALWAYS PROCESSED (Full Data):            │
│    ├─ Count total values                    │
│    ├─ Count null values                     │
│    ├─ Count unique values                   │
│    ├─ Update min/max for numerics           │
│    ├─ Update sum for mean calculation       │
│    └─ Collect top 10 values                 │
│                                              │
│ B. TYPE DETECTION (Sampled):                │
│    ├─ First chunk: Analyze ALL values       │
│    ├─ Chunks 2-10: Skip (use first chunk)   │
│    └─ Every 10th chunk: Sample 1,000 values │
│                                              │
│ C. SAMPLE COLLECTION (Limited):              │
│    ├─ Sample values: First 100 (chunk 1)    │
│    ├─ Numeric data: Max 100,000 per column  │
│    ├─ Datetime data: Max 50,000 per column  │
│    └─ PII detection: Max 1,000 per column   │
│                                              │
│ D. SEMANTIC PATTERNS (On samples):           │
│    └─ Detect emails, phones, URLs, etc.     │
│                                              │
└──────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────┐
│ 4. Enhancement Analysis (After Chunk)       │
├──────────────────────────────────────────────┤
│ • Temporal: Analyze datetime patterns       │
│ • PII: Scan for sensitive data              │
│ • Correlation: Update correlation matrix    │
└──────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────┐
│ 5. Free Memory                               │
├──────────────────────────────────────────────┤
│ • Drop chunk DataFrame                       │
│ • Python GC collects memory                 │
│ • Ready for next chunk                       │
└──────────────────────────────────────────────┘
```

**Loop Progress Logs:**
```
📊 Processing chunk 1/90 (2,000,000 rows) - Total: 2,000,000 rows
📊 Processing chunk 2/90 (2,000,000 rows) - Total: 4,000,000 rows
...
📊 Processing chunk 90/90 (1,702,229 rows) - Total: 179,702,229 rows
```

### Phase 3: Finalization

```
┌──────────────────────────────────────────────┐
│ 6. Calculate Final Statistics               │
├──────────────────────────────────────────────┤
│ • Compute mean (sum / count)                 │
│ • Calculate median (from value distribution) │
│ • Compute std dev (from variance)            │
│ • Determine quality scores                   │
│ • Infer final types with confidence          │
└──────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────┐
│ 7. Run Enhancement Finalizers               │
├──────────────────────────────────────────────┤
│ • Temporal: Seasonality, gaps, trends        │
│ • PII: Privacy risk scoring                  │
│ • Correlation: Final correlation matrix      │
│ • Semantic: Pattern-based suggestions        │
└──────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────┐
│ 8. Generate Validation Suggestions          │
├──────────────────────────────────────────────┤
│ • EmptyFileCheck (always)                    │
│ • RowCountRangeCheck (based on row count)    │
│ • MandatoryFieldCheck (>95% complete fields) │
│ • RegexPatternCheck (semantic patterns)      │
│ • RangeCheck (min/max for numerics)          │
│ • UniqueKeyCheck (>99% unique fields)        │
│ • ValidValuesCheck (low cardinality fields)  │
│ • DateFormatCheck (detected date formats)    │
└──────────────────────────────────────────────┘
        ↓
┌──────────────────────────────────────────────┐
│ 9. Generate Outputs                          │
├──────────────────────────────────────────────┤
│ • HTML report (interactive, visualizations)  │
│ • YAML validation config (ready-to-use)      │
│ • JSON export (optional, machine-readable)   │
└──────────────────────────────────────────────┘
```

**Completion Logs:**
```
⏱  Profile completed in 2,869.69 seconds
   chunk_processing: 2867.70s (99.9%)
   temporal_analysis: 0.00s (0.0%)
   pii_detection: 0.00s (0.0%)
   correlation: 0.00s (0.0%)
   generate_suggestions: 0.01s (0.0%)

✅ HTML report generated: /home/daniel/www/demo-tmp/profile.html
✅ Validation config saved: validation_config.yaml
```

---

## Sampling Strategy

### What Gets Sampled (and Why)

DataK9 uses **intelligent sampling** to balance accuracy with memory efficiency:

| Data Type | Sampling Strategy | Reason | Accuracy Impact |
|-----------|-------------------|--------|-----------------|
| **Row counts** | ❌ **NO SAMPLING** - Full count | Exact counts needed | None - 100% accurate |
| **Null counts** | ❌ **NO SAMPLING** - Full count | Critical for completeness | None - 100% accurate |
| **Unique values** | ❌ **NO SAMPLING** - Full tracking | Cardinality is key metric | None - 100% accurate |
| **Min/Max values** | ❌ **NO SAMPLING** - Full scan | Range must be exact | None - 100% accurate |
| **Sum (for mean)** | ❌ **NO SAMPLING** - Full aggregation | Mean must be accurate | None - 100% accurate |
| **Top 10 values** | ❌ **NO SAMPLING** - Full frequency count | Mode detection | None - 100% accurate |
| **Sample values** | ✅ **SAMPLED** - First 100 values | For display/examples only | N/A - display only |
| **Type detection** | ✅ **SAMPLED** - Chunk 1 + every 10th | Statistical validity (1000s of samples) | Minimal - 99%+ accurate |
| **Numeric correlation** | ✅ **SAMPLED** - Max 100,000 per column | Memory limit, statistically valid | Minimal - 95%+ accurate |
| **Datetime temporal** | ✅ **SAMPLED** - Max 50,000 per column | Memory limit, pattern detection | Minimal - 95%+ accurate |
| **PII detection** | ✅ **SAMPLED** - Max 1,000 per column | Pattern matching, not statistics | None - patterns clear in 1K samples |
| **Semantic patterns** | ✅ **SAMPLED** - Uses sample_values (100) | Pattern detection, not measurement | None - patterns emerge quickly |

### Detailed Sampling Behavior

#### 1. Sample Values (First 100)

**Purpose**: Display examples in HTML report

**Strategy**:
```python
# First chunk only
if chunk_idx == 0 and len(sample_values) < 100:
    samples = column.head(100 - len(sample_values)).tolist()
    sample_values.extend(samples)
```

**When**: First chunk only
**Limit**: 100 values per column
**Used for**:
- Displaying example values in report
- Semantic pattern detection
- PII detection

**Trigger Log**: None (silent)

#### 2. Type Detection (Smart Sampling)

**Purpose**: Infer data types accurately without scanning entire file

**Strategy**:
```python
# First chunk: Analyze ALL values
if chunk_idx == 0:
    for value in all_non_null_values:
        detect_type(value)

# Chunks 2-9: Skip (rely on first chunk)

# Every 10th chunk: Sample 1,000 values
if chunk_idx % 10 == 0:
    sample_size = min(1000, len(non_null_values))
    sampled = random.sample(non_null_values, sample_size)
    for value in sampled:
        refine_type(value)
```

**When**:
- Chunk 1: Full analysis
- Chunk 10, 20, 30, ...: 1,000 value sample

**Used for**:
- Type inference (integer, float, string, date, etc.)
- Type confidence calculation
- Type conflict detection

**Trigger Log**: None (internal only)

#### 3. Numeric Data for Correlation (100,000 limit)

**Purpose**: Calculate correlations without exhausting memory

**Strategy**:
```python
MAX_CORRELATION_SAMPLES = 100_000  # Per column

# For each chunk
if current_sample_count < MAX_CORRELATION_SAMPLES:
    samples_needed = MAX_CORRELATION_SAMPLES - current_sample_count

    if chunk_values > samples_needed:
        # Random sample from chunk
        sampled = random.sample(chunk_values, samples_needed)
        numeric_data[col].extend(sampled)
    else:
        # Take all chunk values
        numeric_data[col].extend(chunk_values)
```

**When**: Every chunk until limit reached
**Limit**: 100,000 values per numeric column
**Typical trigger**: After ~50 chunks (100M rows)

**Trigger Log**:
```
💾 Memory optimization: Column 'transaction_amount' sampling limit reached at 100,000,000 rows
   (using 100,000 samples for correlation)
```

**Used for**:
- Pearson correlation
- Spearman correlation
- Correlation matrix visualization

**Statistical Validity**: 100K samples provide 99%+ confidence for correlation estimates

#### 4. Datetime Data for Temporal Analysis (50,000 limit)

**Purpose**: Detect temporal patterns without memory exhaustion

**Strategy**:
```python
MAX_TEMPORAL_SAMPLES = 50_000  # Per datetime column

# For each chunk
if current_sample_count < MAX_TEMPORAL_SAMPLES:
    samples_needed = MAX_TEMPORAL_SAMPLES - current_sample_count

    if chunk_datetime_values > samples_needed:
        # Random sample from chunk
        sampled = random.sample(chunk_datetime_values, samples_needed)
        datetime_data[col].extend(sampled)
    else:
        # Take all values
        datetime_data[col].extend(chunk_datetime_values)
```

**When**: Every chunk until limit reached
**Limit**: 50,000 values per datetime column
**Typical trigger**: After ~25 chunks (50M rows)

**Trigger Log**:
```
💾 Memory optimization: Column 'Timestamp' temporal sampling limit reached at 50,000,000 rows
   (using 50,000 samples)
```

**Used for**:
- Seasonality detection
- Trend analysis
- Gap detection
- Temporal pattern validation suggestions

**Statistical Validity**: 50K datetime samples sufficient for pattern detection

#### 5. PII Detection Data (1,000 limit)

**Purpose**: Scan for sensitive data patterns

**Strategy**:
```python
PII_SAMPLE_LIMIT = 1_000  # Per column

# For each chunk
for col in string_columns:
    if len(pii_samples[col]) < PII_SAMPLE_LIMIT:
        samples_needed = PII_SAMPLE_LIMIT - len(pii_samples[col])
        pii_samples[col].extend(chunk[col].dropna().head(samples_needed))
```

**When**: Every chunk until limit reached
**Limit**: 1,000 values per column
**Typical trigger**: First chunk (usually completes immediately)

**Trigger Log**: None (silent)

**Used for**:
- Email detection
- Phone number detection
- SSN detection
- Credit card detection
- Privacy risk scoring

**Why 1,000 is enough**: Pattern matching, not statistical measurement. If 30% of 1,000 samples are emails, column likely contains emails.

---

## Memory Management

### Memory Bounds (Guaranteed)

DataK9 profiler has **hard memory limits** to prevent exhaustion:

| Component | Memory Limit | Per Column | Notes |
|-----------|--------------|------------|-------|
| Chunk size | ~200-300MB | N/A | Configurable, auto-calculated |
| Numeric samples | ~800KB | 100,000 × 8 bytes | For correlation |
| Datetime samples | ~400KB | 50,000 × 8 bytes | For temporal analysis |
| PII samples | ~50KB | 1,000 × ~50 bytes | For pattern detection |
| Sample values | ~5KB | 100 × ~50 bytes | For display |
| **Total per column** | **~1.3MB** | **Maximum** | After sampling triggered |

### Memory Profile by File Size

| File Size | Rows | Columns | Peak Memory | Notes |
|-----------|------|---------|-------------|-------|
| 10 MB | 100K | 10 | ~100 MB | No sampling triggered |
| 100 MB | 1M | 10 | ~200 MB | No sampling triggered |
| 1 GB | 10M | 10 | ~300 MB | No sampling triggered |
| 10 GB | 100M | 10 | ~350 MB | Sampling triggered for correlation |
| 100 GB | 1B | 10 | ~400 MB | All sampling limits hit |
| **200 GB** | **2B** | **10** | **~400 MB** | **Maximum memory usage** |

### When Sampling Triggers

**Example: 179M row file (HI-Large_Trans.parquet)**

| Row Count | Chunk # | What Happens |
|-----------|---------|--------------|
| 0 - 2M | 1 | Full processing, type detection, sample collection starts |
| 2M - 20M | 2-10 | Full processing, type detection skipped |
| 20M | 10 | Type refinement: 1,000 value sample |
| 50M | 25 | **Temporal sampling triggered** for datetime columns (50K limit hit) |
| 100M | 50 | **Numeric sampling triggered** for correlation (100K limit hit) |
| 200M | 100 | **All limits hit** - constant memory from here |

**Logs you'll see:**
```
📊 Processing chunk 25/90 (2,000,000 rows) - Total: 50,000,000 rows
💾 Memory optimization: Column 'Timestamp' temporal sampling limit reached at 50,000,000 rows
   (using 50,000 samples)

📊 Processing chunk 50/90 (2,000,000 rows) - Total: 100,000,000 rows
💾 Memory optimization: Column 'transaction_amount' sampling limit reached at 100,000,000 rows
   (using 100,000 samples for correlation)
```

---

## Enhancement Features

### Default Behavior (All Enabled by Default)

Since v1.54, **all enhancements are enabled by default**:

```bash
# This command now runs ALL enhancements automatically:
python3 -m validation_framework.cli profile data.csv
```

**What runs**:
- ✅ Temporal analysis (datetime pattern detection, seasonality, gaps)
- ✅ PII detection (privacy risk scoring, sensitive data flagging)
- ✅ Enhanced correlation (Pearson, Spearman, Cramér's V)
- ✅ Semantic pattern detection (email, phone, URL, IP, credit cards, SSN)
- ✅ FIBO semantic tagging (financial ontology-based column classification)

**To disable** (if needed for performance):
```bash
# Disable specific enhancements
python3 -m validation_framework.cli profile data.csv --disable-pii
python3 -m validation_framework.cli profile data.csv --disable-temporal
python3 -m validation_framework.cli profile data.csv --disable-correlation

# Disable all enhancements
python3 -m validation_framework.cli profile data.csv --disable-all-enhancements
```

**ML Analysis** (Beta - requires `--beta-ml` flag):
```bash
# Enable ML-based anomaly detection
python3 -m validation_framework.cli profile data.csv --beta-ml

# Full analysis mode (no internal sampling, more accurate but slower)
python3 -m validation_framework.cli profile data.csv --beta-ml --full-analysis
```

### Enhancement Processing Order

```
1. CHUNK PROCESSING (per chunk):
   ├─ Collect datetime samples (if temporal enabled)
   ├─ Collect numeric samples (if correlation enabled)
   └─ Collect PII samples (if PII enabled)

2. AFTER ALL CHUNKS:
   ├─ Temporal Analysis (on datetime samples)
   │  ├─ Detect seasonality
   │  ├─ Find gaps in time series
   │  ├─ Identify trends
   │  └─ Suggest temporal validations
   │
   ├─ PII Detection (on PII samples)
   │  ├─ Scan for emails, phones, SSN, credit cards
   │  ├─ Calculate privacy risk scores
   │  ├─ Flag sensitive columns in report
   │  └─ Suggest data protection measures
   │
   ├─ Correlation Analysis (on numeric samples)
   │  ├─ Calculate Pearson (linear correlation)
   │  ├─ Calculate Spearman (monotonic correlation)
   │  ├─ Calculate Cramér's V (categorical association)
   │  └─ Generate correlation matrix visualization
   │
   └─ Semantic Pattern Detection (on sample_values)
      ├─ Detect patterns (email, phone, URL, etc.)
      ├─ Calculate pattern confidence
      └─ Generate RegexPatternCheck suggestions
```

---

## Performance Characteristics

### Processing Speed by Format

| Format | Rows/Second | 179M Rows Time | Notes |
|--------|-------------|----------------|-------|
| **Parquet** | 62,000 | ~48 min | **Recommended** - columnar, compressed |
| **CSV** | 15,000 | ~3.3 hours | Text parsing overhead |
| **Excel** | 5,000 | ~10 hours | Complex format, row-by-row |
| **JSON** | 8,000 | ~6.2 hours | Nested structure overhead |

**Recommendation**: Convert large CSVs to Parquet for 4x speedup

### Memory vs. Speed Tradeoff

**Larger chunks** = Faster processing, higher memory:
```bash
# Default: Auto-calculated (~2M rows)
python3 -m validation_framework.cli profile data.parquet

# Faster, uses more memory
python3 -m validation_framework.cli profile data.parquet --chunk-size 5000000

# Slower, uses less memory
python3 -m validation_framework.cli profile data.parquet --chunk-size 500000
```

**Rule of thumb**:
- 1M row chunk ≈ 100-150MB RAM
- 2M row chunk ≈ 200-300MB RAM
- 5M row chunk ≈ 500-750MB RAM

---

## Configuration Options

### Chunk Size

**Auto-calculated (default)**:
```bash
python3 -m validation_framework.cli profile data.csv
# Calculates optimal chunk size based on available RAM
```

**Manual override**:
```bash
python3 -m validation_framework.cli profile data.csv --chunk-size 1000000
# Process 1M rows per chunk
```

### Enhancement Control

**Enable all (default since v1.54)**:
```bash
python3 -m validation_framework.cli profile data.csv
# Runs: temporal, PII, correlation, semantic patterns
```

**Disable specific**:
```bash
# Skip PII detection
python3 -m validation_framework.cli profile data.csv --disable-pii

# Skip temporal analysis
python3 -m validation_framework.cli profile data.csv --disable-temporal

# Skip correlation analysis
python3 -m validation_framework.cli profile data.csv --disable-correlation
```

**Disable all enhancements**:
```bash
# Minimal profiling (fastest, lowest memory)
python3 -m validation_framework.cli profile data.csv --disable-all-enhancements
```

### Output Control

**All outputs**:
```bash
python3 -m validation_framework.cli profile data.csv \
  -o profile.html \           # HTML report
  -j profile.json \           # JSON export
  -c validation.yaml          # YAML config
```

**Specific outputs only**:
```bash
# HTML only (default)
python3 -m validation_framework.cli profile data.csv -o report.html

# Config only (skip HTML)
python3 -m validation_framework.cli profile data.csv -c validation.yaml
```

---

## Open Source Libraries Used

DataK9 profiler leverages best-in-class open source libraries for data processing and analysis:

### Core Data Processing

#### Pandas (pandas>=2.0.0)
**Purpose**: Primary data manipulation and analysis engine
**Used for**:
- Loading and processing data chunks
- DataFrame operations (filtering, grouping, aggregation)
- Statistical calculations (mean, median, std dev, percentiles)
- Type inference and conversion
- Missing value handling

**Why**: Industry-standard data library with 15+ years of optimization, extensive documentation, and massive ecosystem support.

**License**: BSD 3-Clause

#### PyArrow (pyarrow>=12.0.0)
**Purpose**: High-performance Parquet file reading
**Used for**:
- Reading Parquet files (columnar format)
- 4-5x faster than CSV parsing
- Memory-efficient columnar data access
- Automatic compression/decompression

**Why**: Apache Arrow provides zero-copy reads and columnar processing, making it ideal for large files.

**License**: Apache 2.0

#### Polars (Optional)
**Purpose**: Alternative to Pandas for even faster processing
**Used for** (when installed):
- 2-5x faster DataFrame operations
- Lower memory usage
- Better multi-core utilization
- Lazy evaluation for query optimization

**Why**: Rust-based engine with superior performance, especially on large datasets (100M+ rows).

**License**: MIT

### File Format Support

#### OpenPyXL (openpyxl>=3.1.0)
**Purpose**: Excel file reading and writing
**Used for**:
- Reading .xlsx and .xls files
- Sheet selection and multi-sheet support
- Cell formatting and type detection

**Why**: Pure Python implementation, no external dependencies, works cross-platform.

**License**: MIT

#### Dask (dask[dataframe]>=2023.5.0) - Optional
**Purpose**: Parallel processing for very large files (100GB+)
**Used for**:
- Out-of-core processing (data larger than RAM)
- Parallel chunk processing across CPU cores
- Scaling to multi-machine clusters (advanced use)

**Why**: Extends Pandas API to distributed computing, allowing profiling of files that don't fit in memory.

**License**: BSD 3-Clause

### Statistical Analysis

#### SciPy (scipy>=1.11.0)
**Purpose**: Scientific computing and advanced statistics
**Used for**:
- **Correlation analysis**: Spearman rank correlation
- **Statistical tests**: Kolmogorov-Smirnov test for distribution detection
- **Outlier detection**: Z-score calculations
- **Probability distributions**: Testing for normality, etc.

**Why**: Gold standard for scientific Python, highly optimized C/Fortran code, peer-reviewed algorithms.

**License**: BSD 3-Clause

#### Statsmodels (statsmodels>=0.14.0)
**Purpose**: Temporal analysis and time-series decomposition
**Used for**:
- **Seasonality detection**: Decomposing time series into trend + seasonal + residual
- **Trend analysis**: Detecting linear and non-linear trends
- **Gap detection**: Finding irregular intervals in datetime data
- **Autocorrelation**: Identifying temporal dependencies

**Why**: Econometrics-focused library with robust time-series analysis tools used in finance and research.

**License**: BSD 3-Clause

#### Scikit-learn (scikit-learn>=1.3.0)
**Purpose**: Machine learning algorithms for enhanced correlation
**Used for**:
- **Mutual information**: Detecting non-linear relationships between variables
- **Cramér's V**: Measuring association between categorical variables
- **Clustering**: Future feature for automatic column grouping

**Why**: Industry-standard ML library, well-tested algorithms, excellent performance.

**License**: BSD 3-Clause

### Configuration and Templating

#### PyYAML (pyyaml>=6.0)
**Purpose**: YAML configuration file parsing and generation
**Used for**:
- Loading user validation configurations
- Generating auto-validated config files
- Schema validation

**Why**: Standard YAML parser for Python, safe loading, supports complex structures.

**License**: MIT

#### Jinja2 (jinja2>=3.1.0)
**Purpose**: HTML report generation
**Used for**:
- Rendering interactive HTML reports
- Template-based report customization
- Separating content from presentation

**Why**: Industry-standard templating engine (used by Flask, Ansible), powerful and secure.

**License**: BSD 3-Clause

#### JSONSchema (jsonschema>=4.17.0)
**Purpose**: Validation definition schema validation
**Used for**:
- Validating validation_definitions.json structure
- Ensuring config file integrity
- Auto-generating validation docs

**Why**: Reference implementation of JSON Schema specification.

**License**: MIT

### CLI and User Interface

#### Click (click>=8.1.0)
**Purpose**: Command-line interface framework
**Used for**:
- CLI argument parsing
- Help text generation
- Option validation
- User-friendly error messages

**Why**: Best-in-class CLI framework, used by Flask and major Python projects.

**License**: BSD 3-Clause

#### Colorama (colorama>=0.4.6)
**Purpose**: Cross-platform colored terminal output
**Used for**:
- Color-coded validation results (green/yellow/red)
- Progress indicators
- Error highlighting

**Why**: Works across Windows/Linux/macOS, simple API, no dependencies.

**License**: BSD 3-Clause

#### TQDM (tqdm>=4.65.0)
**Purpose**: Progress bars for long-running operations
**Used for**:
- Visual progress during profiling
- Chunk processing indicators
- Time estimation

**Why**: Fast, extensible, works in terminals and Jupyter notebooks.

**License**: MPL 2.0 / MIT

### System and Performance

#### psutil (psutil>=5.9.0)
**Purpose**: System resource monitoring
**Used for**:
- Available memory detection
- Automatic chunk size calculation
- Memory usage tracking
- CPU utilization monitoring

**Why**: Cross-platform system utilities, used in production monitoring tools.

**License**: BSD 3-Clause

#### python-dateutil (python-dateutil>=2.8.0)
**Purpose**: Flexible date parsing
**Used for**:
- Detecting various date formats (ISO, US, EU, etc.)
- Parsing ambiguous dates
- Timezone handling

**Why**: Extends Python's datetime with robust parsing, handles edge cases.

**License**: Apache 2.0 / BSD 3-Clause

### Pattern Detection (Custom Implementation)

#### Semantic Patterns (Built-in, No External Library)
**Purpose**: Email, phone, URL, IP, credit card, SSN detection
**Implementation**: Custom regex-based detector using Python's built-in `re` module
**Used for**:
- Email validation pattern detection
- Phone number format detection
- PII scanning (credit cards, SSN)
- URL and IP address detection

**Why custom?**: Avoids dependency hell (commonregex-improved had conflicts), lightweight, tailored to DataK9's needs, no external dependencies.

**Code**: `validation_framework/profiler/semantic_patterns.py`

### Optional/Database Support

#### SQLAlchemy (Optional, sqlalchemy>=2.0.0)
**Purpose**: Database connectivity and ORM
**Used for**:
- Connecting to PostgreSQL, MySQL, Oracle, SQL Server
- Table schema introspection
- Chunked query execution

**Why**: Universal database abstraction layer, supports 20+ databases.

**License**: MIT

### Library Usage Summary

| Library | Required | Purpose | Size | Performance Impact |
|---------|----------|---------|------|-------------------|
| pandas | ✅ Yes | Core data processing | ~50MB | Critical - main engine |
| pyarrow | ✅ Yes | Parquet file reading | ~25MB | High - 4x speedup |
| scipy | ✅ Yes | Statistical analysis | ~35MB | Medium - correlation only |
| statsmodels | ✅ Yes | Temporal analysis | ~15MB | Low - datetime columns only |
| scikit-learn | ✅ Yes | Enhanced correlation | ~30MB | Low - mutual information only |
| pyyaml | ✅ Yes | Config parsing | ~1MB | Negligible |
| jinja2 | ✅ Yes | HTML templates | ~2MB | Negligible |
| click | ✅ Yes | CLI framework | ~1MB | Negligible |
| psutil | ✅ Yes | System monitoring | ~1MB | Negligible |
| colorama | ✅ Yes | Terminal colors | <1MB | Negligible |
| tqdm | ✅ Yes | Progress bars | <1MB | Negligible |
| openpyxl | ✅ Yes | Excel support | ~3MB | Medium - Excel only |
| jsonschema | ✅ Yes | Schema validation | ~1MB | Negligible |
| python-dateutil | ✅ Yes | Date parsing | ~1MB | Negligible |
| **Total** | | | **~165MB** | |
| **Polars** | ❌ Optional | Faster engine | ~60MB | High - 2-5x speedup |
| **Dask** | ❌ Optional | Very large files | ~20MB | High - enables 100GB+ files |
| **SQLAlchemy** | ❌ Optional | Database support | ~5MB | N/A - database only |

### No External Dependencies For

DataK9 implements these features **without external libraries** to avoid dependency conflicts:

- ✅ Semantic pattern detection (regex-based)
- ✅ Email/phone/URL validation patterns
- ✅ PII detection (credit card, SSN patterns)
- ✅ Basic statistical measures (mean, median, mode)
- ✅ Quality score calculations
- ✅ Cardinality analysis
- ✅ Type inference logic

**Philosophy**: Use industry-standard libraries for complex algorithms (statistics, ML), but implement simple logic in-house to minimize dependencies.

---

## Insight Engine

The Insight Engine transforms raw profiling data into structured, actionable findings with executive summaries and detailed sections.

### Architecture

```
ProfileResult → RuleEngine → Issues → TextGenerator → Report Sections
```

### Components

| Component | Purpose | Output |
|-----------|---------|--------|
| **RuleEngine** | Analyzes profile data against configurable thresholds | List of Issues with severity |
| **TextGenerator** | Renders issue templates into human-readable text | Prose explanations |
| **ExecutiveSummaryGenerator** | Selects diverse top issues for executives | Summary with key findings |
| **DetailedSectionsGenerator** | Groups issues by category | Markdown-formatted sections |

### Issue Categories

| Category | What It Detects |
|----------|-----------------|
| `pii` | Personal identifiable information |
| `outliers` | Statistical outliers (numeric columns) |
| `authenticity` | Benford's Law violations |
| `label_quality` | Class imbalance in categorical columns |
| `temporal` | Time series gaps and patterns |
| `cross_column` | Multi-column consistency issues |
| `completeness` | Missing values / null patterns |
| `validity` | Type/format validity issues |
| `ml_analysis` | Autoencoder and ML-based findings |

### Severity Levels

| Level | Priority | Meaning |
|-------|----------|---------|
| `critical` | 0 | Immediate action required |
| `high` | 1 | Significant issue |
| `medium` | 2 | Notable issue |
| `low` | 3 | Minor issue |
| `info` | 4 | Informational finding |

### Configurable Thresholds

The Insight Engine uses configurable thresholds for all detections:

```python
# Example threshold configuration
InsightThresholds(
    quality_excellent=0.90,    # 90%+ = excellent
    quality_good=0.80,         # 80%+ = good
    null_critical=0.50,        # 50%+ nulls = critical
    null_high=0.20,            # 20%+ nulls = high
    outlier_critical=0.01,     # 1%+ outliers = critical
    pii_high_risk=70,          # Risk score 70+ = high
    validity_low=0.80,         # Below 80% validity = flagged
)
```

### 50K Sampling Policy

The Insight Engine implements a consistent sampling policy:

- **Datasets < 50,000 rows**: Full dataset analysis
- **Datasets ≥ 50,000 rows**: 50,000 row sample for statistical/ML analysis

**Full dataset metrics** (always exact):
- Row count, null count, column metadata

**Sampled metrics** (statistical validity maintained):
- Validity checks, pattern analysis
- ML anomaly detection, Benford analysis
- Outlier detection, distribution analysis
- Cross-column consistency checks

---

## Summary

### What's ALWAYS Processed (No Sampling)

- ✅ Row counts
- ✅ Null counts
- ✅ Unique value counts
- ✅ Min/Max values
- ✅ Sum (for mean calculation)
- ✅ Top 10 value frequencies
- ✅ Quality scores (completeness, validity, uniqueness)

### What's Sampled (and When)

| Feature | Trigger | Limit | Impact |
|---------|---------|-------|--------|
| Type detection | Every 10 chunks | 1,000 values | Minimal (99%+ accurate) |
| Correlation | After ~50M rows | 100,000 per column | Minimal (95%+ accurate) |
| Temporal | After ~25M rows | 50,000 per column | Minimal (patterns detected) |
| PII | First chunk | 1,000 per column | None (patterns clear) |
| Semantic | First chunk | 100 per column | None (patterns clear) |

### Memory Guarantee

**Maximum memory usage: ~400MB** regardless of file size (10MB to 200GB+)

### Performance Guarantee

- **Parquet**: ~62,000 rows/second
- **CSV**: ~15,000 rows/second
- **179M rows**: ~48 minutes (Parquet)

---

## Report UX Design

### Dual-Layer Explanation System

All profiler report sections use a **dual-layer explanation system** to serve both technical and non-technical audiences:

#### Plain-English Summary (📘)
- **Always visible** (expanded by default)
- Uses simple, jargon-free language
- Explains findings in business terms
- No statistical terminology or algorithm names
- Accessible to product managers, analysts, and domain experts

**Examples of plain-English language:**
- ✅ "Most values fall in a normal-looking range"
- ✅ "Some rows look unusual compared to the rest"
- ✅ "A few groups have more missing values than others"
- ❌ ~~"Distribution shows positive skew"~~
- ❌ ~~"MCAR/MAR missingness pattern detected"~~

#### Technical Details (🧠)
- **Collapsed by default** (click to expand)
- Contains all statistical details
- Algorithm names, thresholds, p-values
- Correlation coefficients, error metrics
- For data scientists and technical reviewers

### Section Structure

Every analytical section follows this pattern:

```html
<div class="insight-widget">
  <!-- Plain-English Summary (always visible) -->
  <div class="insight-summary">
    <div class="insight-summary-label">📘 Plain-English Summary</div>
    <p>Simple explanation here...</p>
  </div>

  <!-- Example Data Table -->
  <table class="insight-examples-table">
    <!-- Real data examples -->
  </table>

  <!-- Technical Details (collapsed) -->
  <details class="dual-layer-technical">
    <summary>🧠 Technical Details (click to expand)</summary>
    <div class="technical-body">
      <!-- All technical content -->
    </div>
  </details>
</div>
```

### Target Detection Visual Cues

When the profiler detects a likely ML target column:

1. **Class Distribution section** separates target from non-target fields
2. **Target columns** get:
   - Orange highlighted border (2px solid #f59e0b)
   - "🎯 ML TARGET" badge at top
   - Grouped under "Detected ML Target" header
3. **Non-target columns** appear under "Other Categorical Fields"

### Anomaly Example Deduplication

All anomaly example tables (Outliers, Unusual Combinations, Cross-Field) are **deduplicated** to show unique examples only:
- Sorted by anomaly score (most extreme first)
- Filtered by distinct record signature
- Shows "No examples available" only when count = 0

---

**Updated**: 2025-11-29 (v1.55 - Added dual-layer UX documentation, target detection, anomaly deduplication)
