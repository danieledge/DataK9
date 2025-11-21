# DataK9 Performance Optimization Guide

## Overview

This guide documents performance optimizations for DataK9 validations, focusing on large-scale datasets (10M+ rows). The optimizations achieve 8-20x performance improvements through smart sampling, bloom filters, vectorized processing, and early termination strategies.

## Benchmarks (179M Row Dataset - 5.1 GB Parquet)

**Before Optimization**: 48 minutes
**After Optimization**: 3 minutes (~16x faster)

| Validation | Before | After | Speedup | Technique |
|------------|--------|-------|---------|-----------|
| StatisticalOutlierCheck (IQR) | 20 min | 1 min | 20x | Smart sampling (10M rows) |
| UniqueKeyCheck | 16 min | 12 sec | 80x | Bloom filter + vectorization + early termination |
| DuplicateRowCheck | 12 min | 17 sec | 42x | Bloom filter + vectorization + early termination |

---

## 1. StatisticalOutlierCheck Optimization

### Smart Sampling for IQR Method

**What It Does**: For large datasets (10M+ rows), samples a statistically significant subset instead of processing all data for IQR outlier detection.

**When To Use**:
- **Default behavior**: Automatically enabled for IQR method on files >10M rows
- **Recommended**: For any dataset >5M rows with IQR method
- **Not needed**: Z-score method already uses streaming algorithm (no sampling needed)

### Configuration Options

#### Default (Smart Sampling Enabled)
```yaml
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "Amount Received"
    method: "iqr"
    threshold: 5.0
    # Sampling auto-enabled for files >10M rows
    # Samples 10M rows with stratified method by default
```

#### Custom Sampling
```yaml
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "transaction_amount"
    method: "iqr"
    threshold: 1.5
    enable_sampling: true
    sample_size: 5000000              # Sample 5M rows instead of default 10M
    sampling_method: "random"         # Or "stratified" (default)
    min_sample_size: 100000           # Minimum for statistical validity
    confidence_level: 0.95            # Statistical confidence (default 95%)
```

#### Disable Sampling (100% Accurate but Slower)
```yaml
- type: "StatisticalOutlierCheck"
  severity: "WARNING"
  params:
    field: "amount"
    method: "iqr"
    threshold: 1.5
    enable_sampling: false  # Process all rows (slower but 100% accurate)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_sampling` | bool | true (auto) | Enable smart sampling (auto-enabled >10M rows) |
| `sample_size` | int | 10000000 | Target sample size for statistical analysis |
| `sampling_method` | str | "stratified" | Sampling method: "stratified" or "random" |
| `min_sample_size` | int | 100000 | Minimum sample size for statistical validity |
| `confidence_level` | float | 0.95 | Statistical confidence level (0.0-1.0) |

### Trade-offs

**Advantages**:
- 20x faster on 179M rows (20 min → 1 min)
- Maintains statistical validity with large sample (10M rows)
- Stratified sampling preserves data distribution
- Automatically enabled for large files

**Considerations**:
- May miss rare outliers in the unsampled portion
- For 100% accuracy, disable sampling (but expect longer runtime)
- Z-score method doesn't need sampling (uses streaming algorithm)

### When To Disable Sampling

1. **Critical financial data**: Need 100% accuracy for compliance
2. **Small datasets**: <5M rows, sampling overhead not worth it
3. **Rare event detection**: Looking for very rare outliers
4. **Sufficient time available**: Runtime not a constraint

---

## 2. UniqueKeyCheck Optimization

### Bloom Filter + Vectorization + Early Termination

**What It Does**: Uses bloom filter for fast pre-filtering, vectorized pandas operations instead of row-by-row processing, and early termination after finding duplicates.

**Performance**: 80x faster (16 min → 12 sec on 179M rows)

### Configuration Options

#### Default (All Optimizations Enabled)
```yaml
- type: "UniqueKeyCheck"
  severity: "WARNING"
  params:
    fields:
      - "Timestamp"
      - "From Bank"
      - "Account"
    # Bloom filter enabled by default
    # Early termination enabled by default (max_duplicates: 100)
```

#### Custom Configuration
```yaml
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    fields:
      - "user_id"
      - "order_id"
    # Bloom Filter Settings
    use_bloom_filter: true              # Fast duplicate pre-filtering
    bloom_false_positive_rate: 0.01     # 1% FP rate (lower = more memory)

    # Hash Table Settings
    hash_table_size: 50000000           # 50M keys (avoid disk spillover)

    # Early Termination (Fast Validation Mode)
    enable_early_termination: true      # Stop after finding N duplicates
    max_duplicates: 100                 # Find first 100 duplicates
```

#### Exhaustive Mode (Find All Duplicates)
```yaml
- type: "UniqueKeyCheck"
  severity: "ERROR"
  params:
    fields: ["transaction_id"]
    use_bloom_filter: true
    hash_table_size: 100000000          # Larger hash table for all data
    enable_early_termination: false     # Process all rows
    # Warning: Much slower on large datasets
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `use_bloom_filter` | bool | true | Enable bloom filter for fast pre-filtering |
| `bloom_false_positive_rate` | float | 0.01 | Bloom filter FP rate (0.001-0.1) |
| `hash_table_size` | int | 10000000 | In-memory hash table size before disk spillover |
| `enable_early_termination` | bool | true | Stop after finding max_duplicates |
| `max_duplicates` | int | 100 | Maximum duplicates to find (with early termination) |

### Trade-offs

**Advantages**:
- 80x faster with early termination (16 min → 12 sec)
- Bloom filter reduces memory usage
- Vectorized processing (100x faster than row-by-row)
- Large hash table (50M) avoids slow disk spillover
- Early termination perfect for validation (just need to know if duplicates exist)

**Considerations**:
- Early termination doesn't find all duplicates (only first 100)
- Bloom filter has 1% false positive rate (keys may be checked unnecessarily)
- Large hash table uses more memory (50M keys ≈ 400MB RAM)

### When To Use Exhaustive Mode

1. **Data cleanup**: Need complete list of all duplicates for deduplication
2. **Reporting**: Need exact duplicate count for audits
3. **Small datasets**: <1M rows, exhaustive check is fast enough

### When To Use Early Termination (Default)

1. **Validation**: Just need to know if duplicates exist (validation pass/fail)
2. **Large datasets**: >10M rows where exhaustive check is slow
3. **Quick feedback**: Development/testing where fast results matter

---

## 3. DuplicateRowCheck Optimization

### Bloom Filter + Vectorization + Early Termination

**What It Does**: Same optimizations as UniqueKeyCheck but for detecting duplicate rows across multiple fields.

**Performance**: 42x faster (12 min → 17 sec on 179M rows)

### Configuration Options

#### Default (All Optimizations Enabled)
```yaml
- type: "DuplicateRowCheck"
  severity: "WARNING"
  params:
    key_fields:
      - "Timestamp"
      - "From Bank"
      - "To Bank"
      - "Amount Received"
    # Bloom filter enabled by default
    # Early termination enabled by default (max_duplicates: 100)
```

#### Custom Configuration
```yaml
- type: "DuplicateRowCheck"
  severity: "WARNING"
  params:
    key_fields:
      - "date"
      - "product_id"
      - "quantity"
      - "price"
    # Bloom Filter Settings
    use_bloom_filter: true
    bloom_false_positive_rate: 0.01

    # Hash Table Settings
    hash_table_size: 50000000           # 50M rows

    # Early Termination
    enable_early_termination: true
    max_duplicates: 100
```

#### Exhaustive Mode (Find All Duplicates)
```yaml
- type: "DuplicateRowCheck"
  severity: "ERROR"
  params:
    key_fields: ["order_id", "line_item"]
    use_bloom_filter: true
    hash_table_size: 100000000
    enable_early_termination: false     # Find all duplicates
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `use_bloom_filter` | bool | true | Enable bloom filter for fast pre-filtering |
| `bloom_false_positive_rate` | float | 0.01 | Bloom filter FP rate (0.001-0.1) |
| `hash_table_size` | int | 10000000 | In-memory hash table size |
| `enable_early_termination` | bool | true | Stop after finding max_duplicates |
| `max_duplicates` | int | 100 | Maximum duplicates to find |

### Trade-offs

Same trade-offs as UniqueKeyCheck - see section above.

---

## General Optimization Guidelines

### 1. File Format Optimization

**Use Parquet for Large Files**: Parquet is 10x faster than CSV for large datasets due to columnar storage and compression.

```yaml
files:
  - name: "Large Dataset"
    path: "data.parquet"      # Much faster than data.csv
    format: "parquet"
```

**Benchmark (179M rows)**:
- CSV: ~30 minutes to process
- Parquet: ~3 minutes to process

### 2. Chunk Size Tuning

**Default**: 50,000 rows per chunk (good for most cases)

```yaml
processing:
  chunk_size: 1000000  # 1M rows per chunk for very large files
```

**Guidelines**:
- Small files (<1GB): Use default (50,000)
- Large files (>5GB): Increase to 1,000,000 for fewer I/O operations
- Memory-constrained: Decrease to 10,000

### 3. Validation Selection

**Run Only Necessary Validations**: Each validation takes time. Only enable what you need.

```yaml
validations:
  # Essential validations only
  - type: "NotNullCheck"
    enabled: true

  # Disable expensive validations if not needed
  - type: "StatisticalOutlierCheck"
    enabled: false  # Skip if not needed
```

### 4. Early Termination Strategy

**When To Use**:
- ✅ Development/testing: Quick feedback
- ✅ Validation workflows: Just need pass/fail
- ✅ Large datasets: >10M rows where exhaustive check is slow
- ✅ CI/CD pipelines: Fast validation checks

**When Not To Use**:
- ❌ Data cleanup: Need complete list of duplicates
- ❌ Reporting/auditing: Need exact counts
- ❌ Compliance: Must find all issues

---

## Memory Management

### Hash Table Sizing

**Default**: 10M keys (~80MB RAM)
**Recommended for large datasets**: 50M-100M keys (~400-800MB RAM)

```yaml
params:
  hash_table_size: 50000000  # 50M keys = ~400MB RAM
```

**Guidelines**:
- 10M rows: 10M-20M hash table
- 100M rows: 50M-100M hash table
- 1B rows: 100M-200M hash table

**Disk Spillover**: When hash table fills up, DataK9 spills to SQLite disk storage (slower but unlimited capacity).

### Bloom Filter Sizing

Bloom filter size is automatically calculated based on false positive rate and expected number of keys.

**Memory Usage**:
- 1M keys @ 1% FP rate: ~1.2 MB
- 10M keys @ 1% FP rate: ~12 MB
- 100M keys @ 1% FP rate: ~120 MB

---

## Performance Checklist

- [ ] Use Parquet format for files >1GB
- [ ] Enable smart sampling for StatisticalOutlierCheck on >10M rows
- [ ] Use early termination for UniqueKeyCheck/DuplicateRowCheck in validation workflows
- [ ] Increase hash_table_size to 50M for files >50M rows
- [ ] Increase chunk_size to 1M for files >5GB
- [ ] Run only necessary validations (disable others)
- [ ] Use Polars backend for extreme performance (10M+ rows)
- [ ] Monitor memory usage with large hash tables (50M+ keys)

---

## Troubleshooting

### "Out of Memory" Errors

**Symptoms**: Process killed, system hangs, OOM errors

**Solutions**:
1. Enable sampling for StatisticalOutlierCheck
2. Enable early termination for UniqueKeyCheck/DuplicateRowCheck
3. Reduce hash_table_size (e.g., from 50M to 10M)
4. Reduce chunk_size (e.g., from 1M to 100K)
5. Run fewer validations concurrently
6. Use Parquet format (lower memory footprint)

### Slow Performance

**Symptoms**: Validations taking >10 minutes

**Solutions**:
1. Convert CSV to Parquet (10x faster)
2. Enable sampling for StatisticalOutlierCheck
3. Enable early termination for duplicate checks
4. Increase hash_table_size to avoid disk spillover
5. Increase chunk_size for fewer I/O operations
6. Use Polars backend instead of pandas

### Disk Spillover

**Symptoms**: Logs show "Spilling to disk", slow performance after initial chunk

**Solutions**:
1. Increase hash_table_size (50M-100M)
2. Enable early termination
3. Add more RAM to system
4. Process data in smaller batches

---

## Examples

### Example 1: Fast Validation (Development)
```yaml
validation_job:
  name: "Quick Validation"
  files:
    - name: "Large Dataset"
      path: "data.parquet"
      format: "parquet"
      validations:
        # Fast statistical check (sampled)
        - type: "StatisticalOutlierCheck"
          params:
            field: "amount"
            method: "iqr"
            threshold: 3.0
            # Default sampling enabled

        # Fast uniqueness check (early termination)
        - type: "UniqueKeyCheck"
          params:
            fields: ["transaction_id"]
            enable_early_termination: true
            max_duplicates: 10  # Stop after finding 10
```

### Example 2: Production Validation (Comprehensive)
```yaml
validation_job:
  name: "Production Validation"
  files:
    - name: "Critical Data"
      path: "transactions.parquet"
      format: "parquet"
      validations:
        # 100% accurate statistical check
        - type: "StatisticalOutlierCheck"
          params:
            field: "transaction_amount"
            method: "iqr"
            threshold: 5.0
            enable_sampling: false  # Process all data

        # Find all duplicates (slower but complete)
        - type: "UniqueKeyCheck"
          params:
            fields: ["transaction_id"]
            use_bloom_filter: true
            hash_table_size: 100000000
            enable_early_termination: false  # Find all
```

### Example 3: Extreme Scale (1B+ Rows)
```yaml
validation_job:
  name: "Extreme Scale Validation"
  files:
    - name: "Massive Dataset"
      path: "data_1billion_rows.parquet"
      format: "parquet"
      validations:
        # Sampled statistical check (20x faster)
        - type: "StatisticalOutlierCheck"
          params:
            field: "value"
            method: "iqr"
            threshold: 3.0
            enable_sampling: true
            sample_size: 20000000  # 20M sample

        # Early termination for duplicates
        - type: "UniqueKeyCheck"
          params:
            fields: ["id"]
            use_bloom_filter: true
            hash_table_size: 200000000  # 200M keys
            enable_early_termination: true
            max_duplicates: 1000

  processing:
    chunk_size: 2000000  # 2M rows per chunk
```

---

## Summary

DataK9's performance optimizations enable validation of massive datasets (100M+ rows) in minutes instead of hours:

1. **StatisticalOutlierCheck**: Smart sampling (20x faster)
2. **UniqueKeyCheck**: Bloom filter + early termination (80x faster)
3. **DuplicateRowCheck**: Bloom filter + early termination (42x faster)

**Key Takeaways**:
- Use Parquet for large files (10x faster than CSV)
- Enable optimizations by default (sampling, early termination)
- Disable optimizations only when 100% accuracy is critical
- Tune hash_table_size to avoid disk spillover
- Monitor memory usage with large datasets

For questions or issues, see docs/TROUBLESHOOTING.md or file an issue on GitHub.
