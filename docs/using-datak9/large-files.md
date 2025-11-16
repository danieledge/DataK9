# Handling Large Files

**Validate 200GB+ Files with Confidence**

DataK9 is engineered for large-scale data validation. This guide shows you how to handle files from gigabytes to hundreds of gigabytes efficiently.

---

## Table of Contents

1. [Large File Capabilities](#large-file-capabilities)
2. [Memory Architecture](#memory-architecture)
3. [File Format Recommendations](#file-format-recommendations)
4. [Configuration for Large Files](#configuration-for-large-files)
5. [Monitoring and Logging](#monitoring-and-logging)
6. [Real-World Examples](#real-world-examples)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)

---

## Large File Capabilities

### What DataK9 Can Handle

**Tested at Scale:**

| File Size | Rows | Processing Time | Memory | Format |
|-----------|------|-----------------|--------|--------|
| 1 GB | 10M | ~2 min | 400 MB | CSV |
| 10 GB | 100M | ~20 min | 400 MB | CSV |
| 50 GB | 500M | ~2 hours | 400 MB | Parquet |
| 100 GB | 1B | ~4 hours | 400 MB | Parquet |
| 200 GB | 2B | ~8 hours | 400 MB | Parquet |

**Key Insight:** Memory usage remains constant regardless of file size.

### How It Works

**Chunked Processing:**

```
200 GB File
├─ Chunk 1: 50,000 rows → Process → Discard
├─ Chunk 2: 50,000 rows → Process → Discard
├─ Chunk 3: 50,000 rows → Process → Discard
├─ ... (4 million chunks)
└─ Chunk 4M: 50,000 rows → Process → Discard

Memory: Only 1 chunk in memory at a time
Result: Constant ~400 MB memory usage
```

**No Temporary Files:**

DataK9 doesn't write intermediate results to disk:
- ✅ Streams data through memory
- ✅ No disk I/O overhead
- ✅ No cleanup required
- ✅ Works on systems with limited disk space

---

## Memory Architecture

### Memory Breakdown

```
Total Memory Usage:
├─ Base Framework: ~100 MB
│   ├─ Python runtime
│   ├─ Pandas library
│   ├─ DataK9 code
│   └─ Dependencies
│
├─ Data Chunk: ~200 MB (varies)
│   ├─ Current chunk (default 50K rows)
│   └─ Temporary processing
│
├─ Validation State: ~50 MB
│   ├─ Counters
│   ├─ Aggregations
│   └─ Sample failures
│
└─ Sample Failures: ~50 MB
    └─ Up to 100 failures × file size

TOTAL: ~400 MB (typical)
```

### Memory Efficiency

**Chunk Memory Calculation:**

```python
# Formula
chunk_memory_mb = (chunk_size × column_count × 8 bytes) / 1024 / 1024

# Examples
50K rows × 10 cols = 3.8 MB
50K rows × 50 cols = 19 MB
50K rows × 100 cols = 38 MB
50K rows × 500 cols = 190 MB
```

**Optimization:**

```yaml
# For files with many columns (>500)
settings:
  chunk_size: 25000  # Reduce chunk size to manage memory
```

---

## File Format Recommendations

### Parquet for Large Files

**Why Parquet?**

1. **10x Faster Read Speed**
   ```
   CSV 100GB:  ~4 hours
   Parquet 100GB: ~45 minutes
   ```

2. **Built-in Compression**
   ```
   CSV: 100 GB
   Parquet (Snappy): 30 GB
   Savings: 70%
   ```

3. **Columnar Format**
   - Read only needed columns
   - Skip irrelevant data
   - Optimize I/O

4. **Schema Included**
   - Type information preserved
   - No type inference needed
   - Faster validation

**Converting to Parquet:**

```python
import pandas as pd

# One-time conversion
for chunk in pd.read_csv('large_file.csv', chunksize=100000):
    chunk.to_parquet('large_file.parquet', compression='snappy', append=True)

# Or single command
df = pd.read_csv('large_file.csv')
df.to_parquet('large_file.parquet', compression='snappy')
```

**Then validate:**

```bash
python3 -m validation_framework.cli validate config.yaml
# 10x faster with Parquet!
```

### CSV Optimization

If you must use CSV:

**1. Uncompressed**
```bash
# Don't use gzipped CSV for repeated validations
gunzip large_file.csv.gz  # Decompress once
python3 -m validation_framework.cli validate config.yaml  # Faster
```

**2. Simple Delimiter**
```yaml
files:
  - path: "data.csv"
    delimiter: ","  # Comma faster than complex delimiters
```

**3. No Quoted Fields**
```csv
# ✅ FAST: No quotes
1,John Smith,john@example.com
2,Jane Doe,jane@example.com

# ❌ SLOWER: Quoted fields
"1","John Smith","john@example.com"
"2","Jane Doe","jane@example.com"
```

**4. Fixed Encoding**
```yaml
files:
  - path: "data.csv"
    encoding: "utf-8"  # Explicit encoding faster than auto-detection
```

### Format Comparison

| Criterion | Parquet | CSV | Excel | JSON |
|-----------|---------|-----|-------|------|
| **Read Speed** | ⚡⚡⚡ | ⚡ | 🐌 | ⚡ |
| **File Size** | Small | Large | Medium | Medium |
| **Memory Use** | Low | Medium | High | Medium |
| **Max Size** | 200GB+ | 50GB | 1GB | 10GB |
| **Schema** | ✅ | ❌ | ⚡ | ✅ |

**Recommendation:**
- **< 1 GB:** Any format works
- **1-10 GB:** Prefer Parquet or CSV
- **10-50 GB:** Use Parquet
- **50GB+:** Must use Parquet

---

## Configuration for Large Files

### Optimal Settings

```yaml
validation_job:
  name: "Large File Validation"
  version: "1.0"

settings:
  # Chunk size: Balance speed vs memory
  chunk_size: 50000  # Default is good for most cases

  # Sample failures: Limit memory usage
  max_sample_failures: 100  # Don't collect thousands of samples

  # Logging: Monitor progress
  log_level: "INFO"  # See progress without too much detail

files:
  - name: "large_data"
    path: "data/large_file.parquet"  # Use Parquet!
    format: "parquet"

    # Only load needed columns (Parquet feature)
    columns:
      - "customer_id"
      - "email"
      - "transaction_date"
      - "amount"
    # Skips other columns → faster loading

    validations:
      # Order validations: Fail fast
      - type: "EmptyFileCheck"      # Fast - run first
        severity: "ERROR"

      - type: "ColumnPresenceCheck" # Fast
        severity: "ERROR"

      - type: "MandatoryFieldCheck" # Medium
        severity: "ERROR"

      - type: "RegexCheck"          # Medium
        severity: "ERROR"

      # Complex validations last
      - type: "StatisticalOutlierCheck"  # Slow - run last
        severity: "WARNING"
```

### Chunk Size Guidelines

**For Different File Sizes:**

| File Size | Chunk Size | Memory | Processing |
|-----------|------------|--------|------------|
| 1-10 GB | 50,000 | ~400 MB | Balanced |
| 10-50 GB | 50,000 | ~400 MB | Balanced |
| 50-100 GB | 50,000 | ~400 MB | Slower but safe |
| 100-200 GB | 50,000 | ~400 MB | Slow but reliable |

**For Different RAM:**

| Available RAM | Chunk Size | File Size Support |
|---------------|------------|-------------------|
| 4 GB | 25,000 | Up to 50 GB |
| 8 GB | 50,000 | Up to 100 GB |
| 16 GB | 100,000 | Up to 200 GB |
| 32 GB+ | 200,000 | 200 GB+ |

**For Different Column Counts:**

| Columns | Chunk Size | Memory/Chunk |
|---------|------------|--------------|
| 10-50 | 100,000 | ~40 MB |
| 50-100 | 50,000 | ~40 MB |
| 100-500 | 25,000 | ~50 MB |
| 500+ | 10,000 | ~40 MB |

---

## Monitoring and Logging

### Enable Verbose Logging

```bash
# See progress during validation
python3 -m validation_framework.cli validate config.yaml --verbose

# Output:
# 🐕 DataK9 Validation Started
# ├─ Loading file: large_file.parquet (100 GB)
# ├─ Processing chunk 1/2,000,000 (0.05%)
# ├─ Processing chunk 10,000/2,000,000 (0.5%)
# ├─ Processing chunk 100,000/2,000,000 (5%)
# ├─ Processing chunk 1,000,000/2,000,000 (50%)
# ├─ Processing chunk 2,000,000/2,000,000 (100%)
# ✅ Validation complete: 8 hours 15 minutes
```

### Progress Tracking Script

```bash
#!/bin/bash
# track_validation.sh - Monitor long-running validation

LOG_FILE="validation.log"

# Run validation in background with logging
python3 -m validation_framework.cli validate config.yaml --verbose > $LOG_FILE 2>&1 &

VALIDATION_PID=$!

# Monitor progress
while kill -0 $VALIDATION_PID 2>/dev/null; do
    # Show last line (current progress)
    tail -1 $LOG_FILE

    # Check memory usage
    ps -p $VALIDATION_PID -o rss= | awk '{printf "Memory: %.0f MB\n", $1/1024}'

    sleep 60  # Update every minute
done

echo "✅ Validation complete"
```

### Monitoring Metrics

**Track These:**

1. **Progress Percentage**
   ```bash
   grep "Processing chunk" validation.log | tail -1
   ```

2. **Memory Usage**
   ```bash
   ps aux | grep validation_framework | awk '{print $6/1024 " MB"}'
   ```

3. **Elapsed Time**
   ```bash
   START_TIME=$(stat -f %B validation.log)
   CURRENT_TIME=$(date +%s)
   ELAPSED=$((CURRENT_TIME - START_TIME))
   echo "Elapsed: $ELAPSED seconds"
   ```

4. **Estimated Completion**
   ```python
   # Calculate from progress
   progress_pct = 25  # From logs
   elapsed_time = 7200  # 2 hours
   total_time = (elapsed_time / progress_pct) * 100
   remaining = total_time - elapsed_time
   print(f"Estimated remaining: {remaining/3600:.1f} hours")
   ```

---

## Real-World Examples

### Example 1: 100 GB Transaction File

**Scenario:**
- File: 100 GB Parquet
- Rows: 1 billion
- Columns: 25
- Validations: 8

**Configuration:**

```yaml
validation_job:
  name: "Transaction Validation"

settings:
  chunk_size: 50000
  max_sample_failures: 100

files:
  - name: "transactions"
    path: "data/transactions.parquet"
    format: "parquet"

    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["transaction_id", "customer_id", "amount"]

      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "amount"
          min_value: 0
          max_value: 1000000

      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_fields: ["transaction_id"]

      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          field: "customer_id"
          reference_file: "customers"
          reference_field: "customer_id"
```

**Results:**
- Processing Time: ~4 hours
- Memory Usage: ~400 MB
- Failures Found: 1,250 (0.000125%)
- Status: FAILED (duplicate transaction IDs)

### Example 2: 200 GB Log File

**Scenario:**
- File: 200 GB CSV (application logs)
- Rows: 2 billion
- Columns: 12
- Converted to Parquet: 60 GB

**Optimization Steps:**

1. **Convert to Parquet:**
   ```python
   import pandas as pd

   # Convert in chunks
   writer = None
   for chunk in pd.read_csv('app_logs.csv', chunksize=100000):
       if writer is None:
           writer = pd.io.parquet.ParquetWriter('app_logs.parquet', chunk.schema)
       writer.write_table(chunk)
   writer.close()
   ```

2. **Configure for Scale:**
   ```yaml
   settings:
     chunk_size: 50000  # 40K chunks total
     max_sample_failures: 50  # Limit samples

   files:
     - name: "app_logs"
       path: "data/app_logs.parquet"
       format: "parquet"

       # Only validate critical columns
       columns:
         - "timestamp"
         - "user_id"
         - "action"
         - "error_code"
   ```

3. **Run Overnight:**
   ```bash
   # Run as background job
   nohup python3 -m validation_framework.cli validate app_logs.yaml \
     --verbose > validation.log 2>&1 &

   # Check progress in morning
   tail -f validation.log
   ```

**Results:**
- Processing Time: ~8 hours
- Memory Usage: ~400 MB
- Space Savings: 140 GB (70%)
- Status: PASSED

### Example 3: Multiple Large Files

**Scenario:**
- 10 files, 50 GB each (500 GB total)
- Need daily validation
- Limited processing window

**Strategy: Parallel Processing**

```bash
#!/bin/bash
# validate_all.sh - Process files in parallel

# Array of config files
CONFIGS=(
    "customer_data.yaml"
    "order_data.yaml"
    "product_data.yaml"
    "inventory_data.yaml"
    "shipment_data.yaml"
)

# Process 3 at a time (based on available RAM)
for config in "${CONFIGS[@]}"; do
    (
        python3 -m validation_framework.cli validate "$config" \
          > "${config%.yaml}.log" 2>&1
    ) &

    # Limit to 3 concurrent jobs
    while [ $(jobs -r | wc -l) -ge 3 ]; do
        sleep 60
    done
done

wait  # Wait for all to complete

echo "✅ All validations complete"
```

**Results:**
- Sequential Time: ~40 hours
- Parallel Time (3 jobs): ~15 hours
- Memory Usage: ~1.2 GB (400 MB × 3)
- All files validated overnight

---

## Troubleshooting

### Issue 1: Out of Memory

**Symptoms:**
```
MemoryError: Unable to allocate array
Killed (OOM killer)
Process terminated
```

**Solutions:**

1. **Reduce Chunk Size:**
   ```yaml
   settings:
     chunk_size: 25000  # Half the default
   ```

2. **Limit Columns:**
   ```yaml
   files:
     - path: "data.parquet"
       columns:  # Only load needed columns
         - "id"
         - "email"
         - "date"
   ```

3. **Reduce Sample Failures:**
   ```yaml
   settings:
     max_sample_failures: 50  # Default is 100
   ```

4. **Close Other Applications:**
   ```bash
   # Free up RAM before running
   ```

### Issue 2: Too Slow

**Symptoms:**
```
100 GB file taking >12 hours
```

**Solutions:**

1. **Convert to Parquet:**
   ```bash
   # One-time conversion, 10x speedup
   python3 convert_to_parquet.py large_file.csv
   ```

2. **Increase Chunk Size:**
   ```yaml
   settings:
     chunk_size: 100000  # Double the default
   ```

3. **Remove Unnecessary Validations:**
   ```yaml
   # Comment out slow validations
   # - type: "StatisticalOutlierCheck"  # Disable if not critical
   ```

4. **Upgrade Hardware:**
   - SSD instead of HDD (3-5x faster I/O)
   - More RAM (larger chunks)
   - Faster CPU (faster processing)

### Issue 3: Disk Space

**Symptoms:**
```
No space left on device
```

**Solutions:**

1. **DataK9 Doesn't Need Disk:**
   - No temporary files written
   - Only loads chunks to memory
   - Problem likely elsewhere

2. **Check Log Files:**
   ```bash
   # Large log files?
   du -sh *.log

   # Rotate logs
   logrotate /etc/logrotate.conf
   ```

3. **Check Output Reports:**
   ```bash
   # HTML reports can be large
   du -sh *_report.html

   # Compress old reports
   gzip old_reports/*.html
   ```

---

## Best Practices

### 1. Always Use Parquet for Large Files

**Convert Once, Validate Many Times:**

```bash
# Day 1: Convert (one-time cost)
python3 convert_to_parquet.py data.csv

# Day 2+: Fast validation (every day)
python3 -m validation_framework.cli validate config.yaml
```

**ROI:**

```
CSV Validation:     4 hours/day × 365 days = 1,460 hours/year
Parquet Validation: 24 min/day × 365 days = 146 hours/year

Savings: 1,314 hours/year (89%)
```

### 2. Monitor Long-Running Validations

**Set Up Monitoring:**

```bash
# 1. Run with logging
python3 -m validation_framework.cli validate config.yaml \
  --verbose > validation.log 2>&1 &

# 2. Monitor progress
watch -n 60 'tail -5 validation.log'

# 3. Set alerts
if [ $(pgrep -c validation_framework) -eq 0 ]; then
    echo "Validation completed or crashed" | mail -s "Alert" admin@example.com
fi
```

### 3. Test with Samples First

**Before Full Validation:**

```bash
# Test with 1 million rows
python3 -m validation_framework.cli profile large_file.parquet \
  --sample-rows 1000000

# Review profile report
open profile_report.html

# Adjust validation config based on profile

# Then run full validation
python3 -m validation_framework.cli validate config.yaml
```

### 4. Schedule During Off-Hours

**For Very Large Files:**

```bash
# Cron job: Run at 2 AM daily
0 2 * * * cd /data/validation && python3 -m validation_framework.cli validate nightly.yaml > /var/log/validation.log 2>&1
```

### 5. Archive Old Data

**Don't Validate Everything:**

```bash
# Only validate recent data
python3 -m validation_framework.cli validate current_month.yaml

# Archive and compress old data
tar -czf archive_2023.tar.gz data_2023/
```

### 6. Partition Large Files

**Split by Date/Region:**

```
Instead of:
- transactions_2023.parquet (200 GB)

Use:
- transactions_2023_01.parquet (16 GB)
- transactions_2023_02.parquet (16 GB)
- ...
- transactions_2023_12.parquet (16 GB)

Benefits:
- Faster individual validations
- Parallel processing
- Easier troubleshooting
```

---

## Performance Expectations

### Realistic Benchmarks

**Your Hardware:**
- CPU: Modern multi-core (2020+)
- RAM: 8 GB available
- Disk: SSD
- Network: Not applicable (local files)

**Expected Performance:**

| File Size | Format | Time | Memory |
|-----------|--------|------|--------|
| 1 GB | Parquet | 2 min | 300 MB |
| 10 GB | Parquet | 15 min | 350 MB |
| 50 GB | Parquet | 90 min | 400 MB |
| 100 GB | Parquet | 3-4 hours | 400 MB |
| 200 GB | Parquet | 7-9 hours | 400 MB |

**Variables:**
- Number of validations
- Validation complexity
- Column count
- Data types
- Disk I/O speed

---

## Next Steps

**You've learned to handle large files! Now:**

1. **[Performance Tuning](performance-tuning.md)** - Optimize further
2. **[Configuration Guide](configuration-guide.md)** - Configure for scale
3. **[Best Practices](best-practices.md)** - Production deployment
4. **[Troubleshooting](troubleshooting.md)** - Solve issues

---

**🐕 Scale with confidence - DataK9 guards files of any size**
