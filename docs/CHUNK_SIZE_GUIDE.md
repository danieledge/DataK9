# DataK9 Chunk Size Optimization Guide

## Overview

Chunk size is critical for memory-efficient processing of large datasets. This guide helps you choose the optimal chunk size for your validation workloads.

## Quick Reference

| File Size | Recommended Chunk Size | Use Case |
|-----------|----------------------|----------|
| < 10 MB | 50,000 rows | Small files, testing |
| 10-100 MB | 100,000 rows | Medium files, development |
| 100 MB - 1 GB | 500,000 rows | Large files, production |
| 1-5 GB | 1,000,000 rows | Very large files |
| > 5 GB | 2,000,000 rows | Extreme scale |

## Chunk Size Calculator

DataK9 includes an intelligent chunk size calculator that considers:

- **File size and format** (CSV, Parquet, JSON)
- **Available system memory**
- **Number of validations**
- **Validation complexity** (simple to heavy)

### Using the CLI Helper

```bash
# Interactive calculator
python3 -m validation_framework.utils.chunk_size_calculator

# With file path
python3 -m validation_framework.utils.chunk_size_calculator /path/to/data.csv
```

### Example Output

```
╔════════════════════════════════════════════════════════════════╗
║           DataK9 Chunk Size Recommendation                     ║
╚════════════════════════════════════════════════════════════════╝

File Information:
  Path: HI-Large_Trans.parquet
  Format: PARQUET
  Size: 5,100 MB
  Estimated Rows: 179,000,000

System Resources:
  Available Memory: 5,600 MB
  Validation Count: 31
  Validation Complexity: moderate

Recommendation:
  Chunk Size: 1,000,000 rows/chunk
  Estimated Chunks: 179
  Peak Memory Usage: ~1,200 MB

Rationale:
  Calculated based on available memory and validation complexity.

YAML Configuration:
  processing:
    chunk_size: 1000000
    max_sample_failures: 100
```

## Factors Affecting Chunk Size

### 1. File Format

**CSV (Text-Based)**
- Larger on disk
- Slower to parse
- 3x expansion when loaded into memory
- **Recommendation:** Use smaller chunks (50K-500K)

**Parquet (Columnar)**
- Highly compressed on disk
- Fast to read
- Efficient memory usage
- **Recommendation:** Use larger chunks (500K-2M)

**JSON**
- Text-based with overhead
- Variable structure
- Memory intensive
- **Recommendation:** Use smaller chunks (25K-250K)

### 2. Available Memory

```
Target Memory = Available RAM × 70% (safety margin)
Chunk Size = Target Memory / (bytes_per_row × expansion_factor × complexity_factor)
```

**Example Calculations:**

**8 GB RAM System:**
- Available: 8,000 MB × 70% = 5,600 MB
- CSV file: 150 bytes/row × 3x expansion = 450 bytes/row in memory
- Simple validations: 1x complexity
- **Optimal chunk: ~1.2M rows**

**16 GB RAM System:**
- Available: 16,000 MB × 70% = 11,200 MB
- Parquet file: 50 bytes/row × 3x expansion = 150 bytes/row in memory
- Moderate validations (31 checks): 2x complexity
- **Optimal chunk: ~2M rows (capped at max)**

### 3. Validation Complexity

**Simple (1x multiplier)**
- EmptyFileCheck
- SchemaValidation
- RowCountCheck
- **Memory:** Minimal state

**Moderate (2x multiplier)**
- RegexCheck
- RangeCheck
- DateFormatCheck
- **Memory:** Per-row validation, some state

**Complex (4x multiplier)**
- DuplicateRowCheck
- UniqueCheck
- Statistical checks
- **Memory:** Tracks unique values, history

**Heavy (8x multiplier)**
- DistributionCheck
- CorrelationCheck
- CrossFileChecks
- **Memory:** Collects all data for analysis

### 4. Number of Validations

Multiple validations increase memory pressure:
- **1-5 validations:** Minimal overhead
- **6-15 validations:** 10% overhead per validation
- **16-30 validations:** Consider reducing chunk size by 25%
- **31+ validations:** Reduce chunk size by 50%

## Configuration Examples

### Small Dataset (100K rows, 10 MB)

```yaml
processing:
  chunk_size: 50000  # Process in 2 chunks
  max_sample_failures: 100
```

### Medium Dataset (10M rows, 500 MB CSV)

```yaml
processing:
  chunk_size: 500000  # Process in 20 chunks
  max_sample_failures: 100
```

### Large Dataset (179M rows, 5 GB Parquet)

```yaml
processing:
  chunk_size: 1000000  # Process in 179 chunks
  max_sample_failures: 1000
```

### Extreme Dataset (1B rows, 50 GB)

```yaml
processing:
  chunk_size: 2000000  # Process in 500 chunks
  max_sample_failures: 1000
```

## Memory Usage Patterns

### Chunked Processing Memory Pattern

```
Memory │
Usage  │     ╭─╮     ╭─╮     ╭─╮
       │     │ │     │ │     │ │
       │ ╭─╮ │ │ ╭─╮ │ │ ╭─╮ │ │
       │ │ │ │ │ │ │ │ │ │ │ │ │
       └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─► Time
         Chunk Chunk Chunk Chunk
           1     2     3     4
```

- Memory peaks during chunk processing
- Drops between chunks (garbage collection)
- Stable, predictable pattern

### Too Large Chunks (Memory Issue)

```
Memory │          ╭─────────╮
Usage  │          │ DANGER! │
       │      ╭───┤ OOM     │
       │  ╭───┤   └─────────┘
       │╭─┤   │
       └┴─┴───┴──────────────► Time
         System crashes
```

### Too Small Chunks (Performance Issue)

```
Memory │ ││││││││││││││││││
Usage  │ ││││││││││││││││││
       │ ││││││││││││││││││
       │ ││││││││││││││││││
       └─────────────────────► Time
         Too much overhead
```

## Performance vs Memory Trade-offs

### Larger Chunks

**Pros:**
- ✓ Faster processing (less overhead)
- ✓ Better I/O efficiency
- ✓ Fewer iterations

**Cons:**
- ✗ Higher memory usage
- ✗ Risk of OOM errors
- ✗ Slower garbage collection

### Smaller Chunks

**Pros:**
- ✓ Lower memory footprint
- ✓ More stable memory usage
- ✓ Better for constrained systems

**Cons:**
- ✗ Slower overall processing
- ✗ More I/O overhead
- ✗ More iterations

## Troubleshooting

### Out of Memory (OOM) Errors

**Symptoms:**
- Process killed by OS
- "MemoryError" exceptions
- System becomes unresponsive

**Solutions:**
1. **Reduce chunk size by 50%**
   ```yaml
   chunk_size: 500000  # was 1000000
   ```

2. **Use Parquet instead of CSV**
   - 3-10x smaller memory footprint
   - Much faster to read

3. **Close other applications**
   - Free up system RAM
   - Stop unnecessary services

4. **Reduce validation complexity**
   - Disable heavy validations (distributions, correlations)
   - Run in multiple passes

### Slow Processing

**Symptoms:**
- Hours to process moderate files
- High CPU but low memory usage
- Too many log messages

**Solutions:**
1. **Increase chunk size by 2x**
   ```yaml
   chunk_size: 2000000  # was 1000000
   ```

2. **Use Parquet format**
   - 10x faster than CSV
   - Better compression

3. **Reduce logging verbosity**
   ```bash
   --log-level WARNING  # instead of INFO
   ```

4. **Disable unnecessary validations**
   - Focus on critical checks
   - Run optional checks separately

## Best Practices

### 1. Start Conservative

Begin with recommended chunk sizes and adjust based on actual performance:

```yaml
# Start here
processing:
  chunk_size: 1000000
```

Monitor memory usage and adjust:
- Memory < 50% used → **increase** chunk size
- Memory > 80% used → **decrease** chunk size

### 2. Format Matters

Always prefer Parquet for large files:

```bash
# Convert CSV to Parquet (pandas)
python3 -c "
import pandas as pd
df = pd.read_csv('large_file.csv')
df.to_parquet('large_file.parquet', compression='snappy')
"
```

### 3. Profile First

Run a profile to understand your data:

```bash
python3 -m validation_framework.cli profile data.csv \
  -o profile.html -j profile.json
```

Use profile insights to adjust chunk size.

### 4. Test Incrementally

Test with progressively larger chunks:

```
50K → 100K → 500K → 1M → 2M
```

Stop when you hit memory limits or diminishing returns.

### 5. Monitor System Resources

```bash
# Monitor memory during validation
watch -n 1 free -h

# Monitor process memory
watch -n 1 'ps aux | grep python | grep validation'
```

## Advanced: Custom Chunk Size Calculation

### Python API

```python
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

calculator = ChunkSizeCalculator()

recommendation = calculator.calculate_optimal_chunk_size(
    file_path="data/large_file.parquet",
    file_format="parquet",
    num_validations=31,
    validation_complexity="moderate",
    target_memory_mb=8000  # Optional: override available memory
)

print(f"Recommended chunk size: {recommendation['recommended_chunk_size']:,}")
print(f"Estimated memory: {recommendation['estimated_memory_mb']} MB")
print(f"Rationale: {recommendation['rationale']}")
```

### Integration in Code

```python
from validation_framework.core.engine import ValidationEngine
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

# Auto-calculate chunk size
calculator = ChunkSizeCalculator()
result = calculator.calculate_optimal_chunk_size(
    file_path="mydata.csv",
    file_format="csv",
    num_validations=10,
    validation_complexity="moderate"
)

# Use in configuration
config = {
    'processing': {
        'chunk_size': result['recommended_chunk_size']
    }
}
```

## Summary

**Key Takeaways:**

1. **Chunk size directly affects memory usage and performance**
2. **Use the calculator for intelligent recommendations**
3. **Parquet format enables larger chunks and better performance**
4. **Monitor and adjust based on actual system behavior**
5. **Conservative starting point: 1M rows for large Parquet files**

**Quick Decision Tree:**

```
File Size < 100 MB?
  └─ Yes → chunk_size: 100,000
  └─ No  → Format is Parquet?
            └─ Yes → chunk_size: 1,000,000
            └─ No  → chunk_size: 500,000
```

---

**For interactive help:**
```bash
python3 -m validation_framework.utils.chunk_size_calculator
```

**For questions or issues:**
- GitHub Issues: https://github.com/danieledge/DataK9/issues
- Documentation: docs/USER_GUIDE.md
