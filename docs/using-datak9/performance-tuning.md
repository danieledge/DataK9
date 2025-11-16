# Performance Tuning Guide

**Optimize DataK9 for Speed and Scale**

DataK9 is built for performance, but you can tune it further for your specific workloads. This guide shows you how to optimize validation speed and memory usage.

---

## Table of Contents

1. [Performance Overview](#performance-overview)
2. [Backend Selection](#backend-selection) ⚡ Critical Performance Factor
3. [Chunk Size Optimization](#chunk-size-optimization)
4. [File Format Selection](#file-format-selection)
5. [Validation Ordering](#validation-ordering)
6. [Memory Management](#memory-management)
7. [Parallel Processing](#parallel-processing)
8. [Database Optimization](#database-optimization)
9. [Benchmarking](#benchmarking)
10. [Troubleshooting Slow Validations](#troubleshooting-slow-validations)

---

## Performance Overview

### DataK9 Performance Characteristics

**With Polars Backend (Recommended):**

| File Size | Format | Rows | Backend | Processing Time | Memory Usage | Platform |
|-----------|--------|------|---------|-----------------|--------------|----------|
| 1 MB | CSV | 10K | Polars | < 1 second | ~10 MB | Any |
| 100 MB | CSV | 1M | Polars | ~10 seconds | ~100 MB | Any |
| 1 GB | Parquet | 10M | Polars | ~30 seconds | ~200 MB | Desktop |
| 5.1 GB | Parquet | 179M | Polars | ~5 min 21 sec | ~10.2 GB | Desktop (15 validations) |
| 10.1 GB | Parquet | 357M | Polars | ~54 min 48 sec | ~3.5 GB | **Raspberry Pi 4** (30 validations) |

**Real-World Benchmarks:**
- **179M rows (HI-Large_Trans)**: 15 validations, 5:21 runtime, 10.2GB peak memory
- **357M rows (HI+LI-Large)**: 30 comprehensive validations including cross-file referential integrity, statistical outliers, ML anomaly detection on Raspberry Pi 4 (4GB RAM)

**With pandas Backend (Legacy):**

| File Size | Format | Rows | Backend | Processing Time | Memory Usage | Result |
|-----------|--------|------|---------|-----------------|--------------|--------|
| 1 MB | CSV | 10K | pandas | < 1 second | ~50 MB | ✅ OK |
| 100 MB | CSV | 1M | pandas | ~15 seconds | ~150 MB | ✅ OK |
| 1 GB | Parquet | 10M | pandas | ~2 minutes | ~500 MB | ⚠️ Use Polars |
| 5.1 GB | Parquet | 179M | pandas | ~42 min 43 sec | 15.2+ GB | ❌ 12/15 validations (OOM) |
| 200 GB | Parquet | N/A | pandas | N/A | N/A | ❌ OOM crash |

**Real-World Benchmark (179M rows):**
- **Runtime**: 42:43
- **Memory**: 15.2GB+
- **Completions**: 12/15 validations (OOM on 3)

**Key Insights:**
- **Polars delivers high performance** on large datasets
- **Polars is memory efficient**
- **Memory stays constant** regardless of file size (chunked processing)
- **Handles 200GB+ files** with chunked processing

### Performance Factors

**1. Backend Selection** (BIGGEST IMPACT!)
- **Polars**: High performance, memory efficient (default)
- **pandas**: Excel support, broad compatibility

**2. File Format** (large impact)
- Parquet: 10x faster than CSV
- CSV with compression: Slower
- JSON: Moderate

**3. Chunk Size** (memory/speed tradeoff)
- Larger chunks: Faster, more memory
- Smaller chunks: Slower, less memory
- Polars default: 200K rows
- pandas default: 50K rows

**4. Validation Complexity** (varies)
- Simple checks: Fast (RegexCheck, RangeCheck)
- Complex checks: Slower (StatisticalOutlierCheck, CrossFileCheck)
- Vectorized operations: 60-500x faster with Polars

**5. Data Characteristics**
- Row count
- Column count
- Value distributions
- Null percentages

---

## Backend Selection

⚡ **The single most important performance decision!**

### How the Polars Backend Works

DataK9's Polars backend achieves high performance through optimized data processing architecture:

**Why Polars is Fast:**

1. **Vectorized Operations**: Polars uses SIMD (Single Instruction Multiple Data) instructions for parallel processing
   - Example: Checking 100,000 email formats happens in one vectorized operation instead of 100,000 individual checks
   - Result: 60-500x speedup for operations like `StringLengthCheck`, `ValidValuesCheck`, `RangeCheck`

2. **Lazy Evaluation with Query Optimization**: Polars builds an execution plan before running queries
   - Combines multiple operations into single pass through data
   - Eliminates redundant work automatically
   - Example: `filter().select().aggregate()` becomes one optimized operation

3. **Zero-Copy Reads**: Polars uses Apache Arrow memory format
   - No data copying between operations
   - Direct memory access for Parquet files
   - Result: 10x faster Parquet reads vs pandas

4. **Parallel Processing**: Polars automatically parallelizes operations across CPU cores
   - Multi-threaded aggregations, joins, and transformations
   - Scales with available cores

5. **Memory Efficiency**: Columnar memory layout and compression
   - Only loads columns actually needed for validation
   - Native support for data types (no object overhead)
   - Efficient memory usage

**When pandas Might Still Be Needed:**

While Polars is faster, pandas has broader ecosystem support:
- **Excel files**: Polars Excel support is experimental (use pandas for .xlsx, .xls)
- **Complex datetime operations**: pandas has more datetime manipulation functions
- **Third-party integrations**: Some libraries only work with pandas DataFrames
- **Edge cases**: Certain data type conversions may behave differently

DataK9 automatically handles backend differences transparently, so you get correct results regardless of which backend you choose.

**Performance Trade-offs:**

| Feature | Polars Backend | pandas Backend |
|---------|---------------|----------------|
| **Speed** | High performance | Standard |
| **Memory** | Efficient | Standard |
| **Excel support** | Limited (coming soon) | Full support |
| **Chunk size** | 200K rows (larger) | 50K rows |
| **SIMD vectorization** | Yes (60-500x operations) | No |
| **Lazy evaluation** | Yes (query optimization) | No |
| **Multi-threading** | Automatic | Manual only |

**Backend Auto-Selection:**

DataK9 intelligently selects the optimal backend:
```python
# Auto-selection logic:
if backend_arg == "polars":
    use_polars()
elif backend_arg == "pandas":
    use_pandas()
else:  # Auto mode
    if polars_installed and not excel_file:
        use_polars()  # Faster for most cases
    else:
        use_pandas()  # Fallback for compatibility
```

---

### Overview

DataK9 now supports two backends:

| Backend | Speed | Memory | Best For |
|---------|-------|--------|----------|
| **Polars** | **High performance** | **Efficient** | Large files, Parquet, production |
| pandas | Standard | Standard | Excel, small files, compatibility |

**Recommendation:** Use Polars for files > 1GB or 10M+ rows.

### How to Select Backend

**CLI:**
```bash
# Use Polars (recommended for large files)
python3 -m validation_framework.cli validate config.yaml --backend polars

# Use pandas (for Excel or compatibility)
python3 -m validation_framework.cli validate config.yaml --backend pandas

# Auto-select (defaults to Polars if installed)
python3 -m validation_framework.cli validate config.yaml
```

**Python API:**
```python
from validation_framework.core.engine import ValidationEngine
from validation_framework.core.backend import DataFrameBackend

# Polars backend
engine = ValidationEngine.from_config(
    "config.yaml",
    backend=DataFrameBackend.POLARS
)

# pandas backend
engine = ValidationEngine.from_config(
    "config.yaml",
    backend=DataFrameBackend.PANDAS
)
```

**Environment Variable:**
```bash
# Set default backend
export DATAK9_BACKEND=polars
```

### Performance Comparison

**Real-World Benchmark: 179M rows, 5.1GB Parquet file**

| Metric | pandas | Polars |
|--------|--------|--------|
| **Runtime** | 42 min 43 sec | 5 min 21 sec |
| **Memory** | 15.2 GB+ | 10.2 GB |
| **Completions** | 12/15 (OOM) | 15/15 |

**Validation-Level Performance:**

| Validation Type | pandas | Polars |
|----------------|--------|--------|
| StringLengthCheck | ~5 hours | ~10 sec |
| RangeCheck | ~2.5 hours | ~3 sec |
| ValidValuesCheck | ~2.5 hours | ~2 sec |
| StatisticalOutlierCheck | OOM | 180 sec |

### When to Use Polars

✅ **Large CSV or Parquet files** (> 1GB)
```bash
python3 -m validation_framework.cli validate large_data.yaml --backend polars
```

✅ **High row count datasets** (> 10M rows)
```bash
python3 -m validation_framework.cli validate big_table.yaml --backend polars
```

✅ **Memory-constrained environments**
```bash
# Polars is memory efficient
python3 -m validation_framework.cli validate config.yaml --backend polars
```

✅ **Production validation workloads**
```bash
# Fast validation for production
python3 -m validation_framework.cli validate prod_data.yaml --backend polars
```

✅ **Parquet files** (native support, very fast)
```bash
python3 -m validation_framework.cli validate transactions.yaml --backend polars
```

### When to Use pandas

✅ **Excel files** (.xlsx, .xls)
```bash
# Polars Excel support coming soon
python3 -m validation_framework.cli validate excel_config.yaml --backend pandas
```

✅ **Small datasets** (< 100MB)
```bash
# Either backend works, pandas is fine
python3 -m validation_framework.cli validate small_file.yaml --backend pandas
```

✅ **Polars not available**
```bash
# Automatic fallback
python3 -m validation_framework.cli validate config.yaml
```

✅ **Compatibility requirements**
```bash
# Existing pandas-specific code
python3 -m validation_framework.cli validate config.yaml --backend pandas
```

### Feature Matrix

| Feature | Polars | pandas |
|---------|--------|--------|
| **CSV support** | ✅ Fast | ✅ |
| **Parquet support** | ✅ Very fast | ✅ |
| **JSON support** | ✅ Fast | ✅ |
| **Excel support** | ❌ Coming | ✅ |
| **Memory efficiency** | ✅ Efficient | ✅ |
| **Vectorized ops** | ✅ 60-500x | ❌ |
| **Large files (200GB+)** | ✅ Handles well | ⚠️ May OOM |
| **Chunk size** | 200K rows | 50K rows |

### Decision Flowchart

```
START: Need to validate data
│
├─ Is it an Excel file (.xlsx, .xls)?
│  ├─ YES → Use pandas (--backend pandas)
│  └─ NO → Continue
│
├─ Is file > 1GB or > 10M rows?
│  ├─ YES → Use Polars (--backend polars) ⚡ RECOMMENDED
│  └─ NO → Continue
│
├─ Is performance critical?
│  ├─ YES → Use Polars (high performance) ⚡
│  └─ NO → Use pandas (broad compatibility)
│
└─ Default: Polars (if installed), else pandas
```

### Installation

**Install Polars:**
```bash
pip install polars
```

**Verify:**
```bash
python3 -c "import polars; print(f'Polars {polars.__version__} installed')"
```

**Optional: PyArrow for Parquet**
```bash
pip install pyarrow
```

### Troubleshooting

**"Polars backend not available" warning:**
```bash
# Install Polars
pip install polars
```

**Excel validation fails with Polars:**
```bash
# Use pandas for Excel files
python3 -m validation_framework.cli validate config.yaml --backend pandas
```

**Slower than expected:**
```bash
# Check Polars is installed and being used
python3 -m validation_framework.cli validate config.yaml --backend polars --verbose
```

---

## Chunk Size Optimization

### What is Chunk Size?

DataK9 processes data in chunks to manage memory:

```python
# Default chunk sizes (v0.2.0):
# - Polars backend: 200,000 rows (4x larger, more efficient)
# - pandas backend: 50,000 rows

# Example with Polars (200K chunks):
# File has 10 million rows
# Processed in 50 chunks (10M / 200K)

# Example with pandas (50K chunks):
# File has 10 million rows
# Processed in 200 chunks (10M / 50K)
```

### Choosing Chunk Size

**Polars Defaults (v0.2.0):**

**Default (200,000 rows):**
- Optimized for Polars backend
- 4x larger chunks = faster processing
- Lower memory overhead
- Recommended for most use cases

**Larger Chunks (500,000 - 1,000,000):**
- Even faster processing
- Requires more RAM (16GB+)
- Best for:
  - Very large RAM systems (32GB+)
  - Simple validations
  - Fewer columns

**Smaller Chunks (50,000 - 100,000):**
- More conservative memory usage
- Still faster than pandas
- Better for:
  - Memory-constrained (8-16GB)
  - Complex validations

**pandas Defaults (Legacy):**

**Default (50,000 rows):**
- Good for most pandas use cases
- Balances speed and memory
- Works with files up to 50GB

**Larger Chunks (100,000 - 250,000):**
- Faster processing
- More memory required
- Better for:
  - High RAM systems (16GB+)
  - Fewer columns

**Smaller Chunks (10,000 - 25,000):**
- Lower memory usage
- Slower processing
- Better for:
  - Low RAM systems (<8GB)
  - Many columns

**Formula:**

```python
# Estimate memory per chunk
memory_mb = chunk_size × column_count × 8 bytes / 1MB

# Example: 50,000 rows × 50 columns × 8 bytes
memory_mb = 50000 × 50 × 8 / 1024 / 1024 ≈ 19 MB
```

### Setting Chunk Size

**In YAML config:**

```yaml
settings:
  chunk_size: 100000  # Increase for speed

  # or

  chunk_size: 25000   # Decrease for memory
```

**Impact Example:**

```
File: 10GB, 10 million rows, 50 columns

Chunk Size 25,000:
- Chunks: 400
- Memory: ~10 MB per chunk
- Time: ~15 minutes

Chunk Size 50,000 (default):
- Chunks: 200
- Memory: ~20 MB per chunk
- Time: ~10 minutes

Chunk Size 100,000:
- Chunks: 100
- Memory: ~40 MB per chunk
- Time: ~7 minutes
```

### Finding Optimal Chunk Size

**Test with your data:**

```bash
# Test different chunk sizes
python3 -m validation_framework.cli validate config.yaml  # Default 50K

# Edit config, set chunk_size: 100000
python3 -m validation_framework.cli validate config.yaml  # 100K

# Edit config, set chunk_size: 25000
python3 -m validation_framework.cli validate config.yaml  # 25K

# Compare times and memory usage
```

**Monitor memory:**

```bash
# On Linux
htop  # Watch memory usage during validation

# On Mac
Activity Monitor

# On Windows
Task Manager → Performance → Memory
```

---

## File Format Selection

### Format Performance Comparison

| Format | Read Speed | Write Speed | Compression | Schema | Best For |
|--------|------------|-------------|-------------|--------|----------|
| **Parquet** | ⚡⚡⚡ Fastest | ⚡⚡ Fast | ✅ Built-in | ✅ Yes | Large files, analytics |
| **CSV** | ⚡ Slow | ⚡⚡⚡ Fastest | ❌ External | ❌ No | Small files, compatibility |
| **JSON** | ⚡ Slow | ⚡⚡ Fast | ❌ External | ✅ Yes | APIs, nested data |
| **Excel** | ⚡ Slowest | ⚡ Slow | ✅ Built-in | ⚡ Partial | Business users, small files |

### When to Use Parquet

**Convert CSV to Parquet for:**
- Files > 1 GB
- Repeated validations
- Many columns
- Numeric-heavy data
- Analytics workloads

**Speed Improvement:**

```
CSV (10GB):  ~20 minutes
Parquet (10GB): ~5 minutes

CSV (100GB): ~3 hours
Parquet (100GB): ~45 minutes
```

**Conversion:**

```python
import pandas as pd

# Convert CSV to Parquet
df = pd.read_csv('large_file.csv')
df.to_parquet('large_file.parquet', compression='snappy')

# Then validate Parquet
python3 -m validation_framework.cli validate config.yaml
```

### CSV Optimization

If you must use CSV:

**1. Use Appropriate Delimiter:**
```yaml
files:
  - name: "data"
    path: "data.csv"
    delimiter: "|"  # Pipe-delimited faster than comma for numeric data
```

**2. Skip Compression:**
```bash
# Uncompressed CSV faster than gzipped
gunzip large_file.csv.gz
python3 -m validation_framework.cli validate config.yaml
```

**3. Remove Unnecessary Columns:**
```python
# Only include columns you're validating
df = pd.read_csv('large_file.csv', usecols=['id', 'email', 'age'])
df.to_csv('filtered_file.csv', index=False)
```

---

## Validation Ordering

### Order Matters

**Fail Fast Principle:**

Run cheap, high-impact validations first:

```yaml
validations:
  # 1. File-level (milliseconds)
  - type: "EmptyFileCheck"
    severity: "ERROR"

  # 2. Schema (seconds)
  - type: "ColumnPresenceCheck"
    severity: "ERROR"

  # 3. Mandatory fields (seconds)
  - type: "MandatoryFieldCheck"
    severity: "ERROR"

  # 4. Field formats (seconds-minutes)
  - type: "RegexCheck"
    severity: "ERROR"

  # 5. Statistical (minutes)
  - type: "StatisticalOutlierCheck"
    severity: "WARNING"

  # 6. Cross-file (minutes)
  - type: "ReferentialIntegrityCheck"
    severity: "ERROR"
```

**Why?**

If file is empty, no point running expensive statistical checks.

### Validation Performance

**Fast Validations** (milliseconds-seconds):
- EmptyFileCheck
- RowCountRangeCheck
- ColumnPresenceCheck
- MandatoryFieldCheck (if few fields)

**Medium Validations** (seconds-minutes):
- RegexCheck
- ValidValuesCheck
- RangeCheck
- UniqueKeyCheck
- DuplicateRowCheck

**Slow Validations** (minutes):
- StatisticalOutlierCheck
- DistributionCheck
- CorrelationCheck
- CrossFileComparisonCheck
- BaselineComparisonCheck

**Order Example:**

```yaml
# ❌ BAD ORDER
validations:
  - type: "StatisticalOutlierCheck"  # Slow - runs even if file empty
  - type: "CrossFileComparisonCheck" # Slow
  - type: "EmptyFileCheck"           # Fast - should be first!

# ✅ GOOD ORDER
validations:
  - type: "EmptyFileCheck"           # Fast - fail immediately if empty
  - type: "MandatoryFieldCheck"      # Fast - check critical fields
  - type: "StatisticalOutlierCheck"  # Slow - run only if basics pass
  - type: "CrossFileComparisonCheck" # Slow - run last
```

---

## Memory Management

### Memory Architecture

DataK9 uses memory efficiently:

```
Total Memory =
  Base overhead (~100 MB) +
  Chunk memory (chunk_size × columns × 8 bytes) +
  Validation state (~50 MB) +
  Sample failures (max_sample_failures × failure_size)
```

### Reducing Memory Usage

**1. Reduce Chunk Size:**

```yaml
settings:
  chunk_size: 25000  # Half default → half memory
```

**2. Limit Sample Failures:**

```yaml
settings:
  max_sample_failures: 50  # Default 100
```

**3. Process Files Sequentially:**

Instead of validating multiple files in one job:

```bash
# ❌ High memory: Validates all 10 files together
python3 -m validation_framework.cli validate all_files.yaml

# ✅ Low memory: Validates files one at a time
python3 -m validation_framework.cli validate file1.yaml
python3 -m validation_framework.cli validate file2.yaml
# ... repeat for each file
```

**4. Use Parquet:**

Parquet's columnar format allows reading only needed columns:

```yaml
files:
  - name: "data"
    path: "data.parquet"
    columns:  # Only load these columns
      - "customer_id"
      - "email"
      - "age"
```

### Memory Monitoring

**Check current usage:**

```python
import psutil

# During validation
process = psutil.Process()
print(f"Memory: {process.memory_info().rss / 1024 / 1024:.2f} MB")
```

**Set memory limits (Linux):**

```bash
# Limit to 2GB
ulimit -v 2097152  # 2GB in KB

python3 -m validation_framework.cli validate config.yaml
```

---

## Parallel Processing

### Current Limitations

DataK9 v1.x processes validations sequentially:

```
File 1: Validation A → Validation B → Validation C
File 2: Validation A → Validation B → Validation C
```

### Workarounds for Parallelism

**1. Parallel File Processing:**

```bash
# Process 4 files in parallel
python3 -m validation_framework.cli validate file1.yaml &
python3 -m validation_framework.cli validate file2.yaml &
python3 -m validation_framework.cli validate file3.yaml &
python3 -m validation_framework.cli validate file4.yaml &

wait  # Wait for all to complete
```

**2. GNU Parallel:**

```bash
# Create file list
ls *.yaml > configs.txt

# Process in parallel (4 jobs at once)
cat configs.txt | parallel -j 4 'python3 -m validation_framework.cli validate {}'
```

**3. Task Schedulers:**

```python
# Airflow DAG with parallel tasks
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG('data_validation', schedule_interval='@daily')

validate_customers = BashOperator(
    task_id='validate_customers',
    bash_command='python3 -m validation_framework.cli validate customers.yaml',
    dag=dag
)

validate_orders = BashOperator(
    task_id='validate_orders',
    bash_command='python3 -m validation_framework.cli validate orders.yaml',
    dag=dag
)

# Run in parallel
[validate_customers, validate_orders]
```

### Future Enhancement

DataK9 v2.x will support built-in parallelism:

```yaml
# Future feature
settings:
  parallel_validations: true
  max_workers: 4
```

---

## Database Optimization

### Database Performance Tips

**1. Use Indexes:**

```sql
-- Create indexes on join columns
CREATE INDEX idx_customer_id ON customers(customer_id);
CREATE INDEX idx_order_customer ON orders(customer_id);
```

**2. Optimize Queries:**

```yaml
# ❌ BAD: Scans entire table
validations:
  - type: "SQLCustomCheck"
    params:
      query: "SELECT * FROM customers WHERE status = 'active'"

# ✅ GOOD: Uses index, limits columns
validations:
  - type: "SQLCustomCheck"
    params:
      query: "SELECT customer_id, email FROM customers WHERE status = 'active' INDEX(idx_status)"
```

**3. Use Connection Pooling:**

```yaml
files:
  - name: "customer_data"
    format: "database"
    connection_string: "postgresql://user:pass@localhost/db?pool_size=10"
```

**4. Batch Referential Checks:**

```yaml
# Instead of checking each row individually
# DataK9 batches lookups for efficiency
validations:
  - type: "ReferentialIntegrityCheck"
    params:
      field: "customer_id"
      reference_file: "customers"  # Loaded once, cached
      reference_field: "customer_id"
```

---

## Benchmarking

### Measure Performance

**1. Time Individual Validations:**

```bash
# Run with verbose logging
python3 -m validation_framework.cli validate config.yaml --verbose

# Output shows time per validation:
# ✅ EmptyFileCheck: 0.01s
# ✅ MandatoryFieldCheck: 1.2s
# ❌ RegexCheck: 15.3s  ← Slow!
# ✅ RangeCheck: 2.1s
```

**2. Profile with Time:**

```bash
time python3 -m validation_framework.cli validate config.yaml

# Output:
# real    2m15.432s
# user    2m10.123s
# sys     0m3.245s
```

**3. Compare Configurations:**

```bash
# Baseline
time python3 -m validation_framework.cli validate baseline.yaml

# Optimized (larger chunks)
time python3 -m validation_framework.cli validate optimized.yaml

# Compare times
```

### Benchmark Script

```bash
#!/bin/bash
# benchmark.sh - Compare different configurations

echo "=== DataK9 Performance Benchmark ==="

for CHUNK_SIZE in 25000 50000 100000 200000; do
    echo "\nTesting chunk_size=$CHUNK_SIZE"

    # Update config
    sed -i "s/chunk_size:.*/chunk_size: $CHUNK_SIZE/" config.yaml

    # Measure time
    START=$(date +%s)
    python3 -m validation_framework.cli validate config.yaml > /dev/null
    END=$(date +%s)

    DURATION=$((END - START))
    echo "Duration: ${DURATION}s"
done
```

---

## Troubleshooting Slow Validations

### Diagnostic Steps

**1. Identify Slow Validations:**

```bash
# Run with verbose logging
python3 -m validation_framework.cli validate config.yaml --verbose

# Look for validations taking >10s
```

**2. Profile Specific Validation:**

```python
import cProfile
import pstats

# Profile validation
profiler = cProfile.Profile()
profiler.enable()

# Run validation
result = validation.validate(data_iterator, context)

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(10)  # Top 10 slow functions
```

**3. Check Data Characteristics:**

```bash
# Profile data to understand characteristics
python3 -m validation_framework.cli profile data.csv

# Look for:
# - High row count
# - Many columns
# - High cardinality fields
# - Large string fields
```

### Common Slow Validations

**RegexCheck on Large Strings:**

```yaml
# ❌ SLOW: Regex on 10,000-character text fields
- type: "RegexCheck"
  params:
    field: "description"  # Long text field
    pattern: ".*keyword.*"

# ✅ FASTER: Limit to shorter fields or use simpler check
- type: "StringLengthCheck"
  params:
    field: "description"
    max_length: 1000
```

**UniqueKeyCheck on High Cardinality:**

```yaml
# ❌ SLOW: Checking uniqueness of 10 million rows
- type: "UniqueKeyCheck"
  params:
    key_fields: ["customer_id"]

# ✅ FASTER: Sample-based uniqueness check
- type: "UniqueKeyCheck"
  params:
    key_fields: ["customer_id"]
    sample_size: 100000  # Check sample instead
```

**CrossFileComparisonCheck:**

```yaml
# ❌ SLOW: Comparing every row across files
- type: "CrossFileComparisonCheck"

# ✅ FASTER: Use database join or indexed lookup
# Pre-join files before validation
```

### Optimization Strategies

**Strategy 1: Sampling**

For statistical validations on very large files:

```yaml
validations:
  - type: "StatisticalOutlierCheck"
    params:
      field: "amount"
      sample_size: 100000  # Sample instead of full dataset
```

**Strategy 2: Caching**

For repeated validations:

```python
# Cache lookup data
@lru_cache(maxsize=10000)
def lookup_customer(customer_id):
    return customer_db.get(customer_id)
```

**Strategy 3: Conditional Validation**

Only validate when needed:

```yaml
# Don't check inactive customers
validations:
  - type: "EmailCheck"
    condition: "status == 'active'"  # Skip 70% of rows
```

---

## Performance Checklist

### Before Optimizing

- [ ] Profile your data
- [ ] Identify slow validations
- [ ] Measure baseline performance
- [ ] Set performance goals

### Quick Wins

- [ ] Use Parquet instead of CSV (10x faster)
- [ ] Order validations (fail fast)
- [ ] Increase chunk_size if memory available
- [ ] Remove unnecessary validations
- [ ] Use conditions to skip rows

### Advanced Optimization

- [ ] Parallel file processing
- [ ] Database indexing
- [ ] Sampling for statistical checks
- [ ] Custom validation optimization
- [ ] Infrastructure upgrades (SSD, RAM, CPU)

### Monitoring

- [ ] Track validation times
- [ ] Monitor memory usage
- [ ] Set up alerting for slow runs
- [ ] Regular benchmarking

---

---

## Real-World Performance: Ultimate Validation Showcase

**Production-scale validation on modest hardware:** 357M rows, 30 validation types on Raspberry Pi 4

### Benchmark Configuration

- **Dataset**: IBM AML Banking Transactions (HI-Large + LI-Large)
- **Total Size**: 10.1 GB Parquet (357 million rows)
- **Hardware**: Raspberry Pi 4 (ARM64, 4GB RAM)
- **Runtime**: 54 minutes 48 seconds
- **Peak Memory**: ~3.5 GB (with disk spillover optimization)
- **Validations**: 30 comprehensive checks across 10 categories
- **Success Rate**: 100% completion (no OOM errors)

### Why This Matters

This real-world test proves DataK9 can handle enterprise-scale data on budget hardware:

✅ **Budget Hardware**: $35 Raspberry Pi validates 357M rows
✅ **Memory Efficient**: 3.5GB RAM for 10GB dataset (disk spillover magic)
✅ **Production Ready**: 30 different validation types including ML and cross-file
✅ **Reliable**: 100% completion rate, no crashes

### Validation Breakdown

**File 1: HI-Large (179.7M rows) - 27 validations in ~30 minutes**
- File-level checks (3): EmptyFileCheck, RowCountRangeCheck, FileSizeCheck
- Schema checks (2): ColumnPresenceCheck, SchemaMatchCheck
- Field-level (5): MandatoryFieldCheck, DateFormatCheck, RangeCheck, ValidValuesCheck, RegexCheck
- Record-level (3): UniqueKeyCheck, DuplicateRowCheck, BlankRecordCheck
- Advanced (9): StringLengthCheck, CompletenessCheck, CrossFieldComparisonCheck, NumericPrecisionCheck, StatisticalOutlierCheck, FreshnessCheck, plus 3 inline checks
- Statistical (3): DistributionCheck, CorrelationCheck, AdvancedAnomalyDetectionCheck
- Conditional (1): ConditionalValidation
- Cross-file (1): CrossFileKeyCheck (validated against 2M reference keys from LI-Large)

**File 2: LI-Large (177M rows) - 3 validations in ~25 minutes**
- EmptyFileCheck, MandatoryFieldCheck
- CrossFileKeyCheck (validated all 177M rows against HI-Large reference set)

### Performance Insights

**Most Complex Validations:**
1. **CrossFileKeyCheck**: 55% of runtime, processes 357M rows across both files with disk spillover
2. **StatisticalOutlierCheck**: Identified 25.5M outliers (14.2%) using IQR method
3. **AdvancedAnomalyDetectionCheck**: ML-based Isolation Forest on 179M rows
4. **NumericPrecisionCheck**: Optimized 100x faster with Polars vectorization

**Memory Management:**
- Chunked processing: 200K rows/chunk (Polars optimized)
- Disk spillover: Automatic for deduplication and cross-file checks
- Peak usage: 3.5GB (would need 15GB+ without optimization)

**See full details:** [Ultimate Validation Showcase](../../README.md#-ultimate-validation-showcase) in main README

---

## Next Steps

**You've learned performance tuning! Now:**

1. **[Large Files Guide](large-files.md)** - Handle 200GB+ files
2. **[Configuration Guide](configuration-guide.md)** - Optimize your config
3. **[Best Practices](best-practices.md)** - Production deployment
4. **[Troubleshooting](troubleshooting.md)** - Solve performance issues
5. **[Polars Backend Guide](../../POLARS_BACKEND_GUIDE.md)** - Get 8x faster validation

---

**🐕 Fast validation, reliable results - DataK9 guards efficiently**
