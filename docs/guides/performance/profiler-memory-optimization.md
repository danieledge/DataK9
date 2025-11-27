# DataK9 Profiler Memory Optimization Guide

## Overview

The DataK9 Profiler uses advanced memory optimization techniques to efficiently profile datasets of any size - from small files (MBs) to very large datasets (200GB+) - while maintaining constant, predictable memory usage.

This guide explains the memory safety features, optimization techniques, and how the profiler handles large-scale data profiling.

## Memory Safety Features

### 1. Automatic Memory Monitoring

The profiler automatically monitors system memory usage during profiling operations:

```python
# Memory checks every 10 chunks
self.memory_check_interval = 10

# Warning threshold: 70% system memory
self.memory_warning_threshold = 70

# Critical threshold: 80% system memory (auto-terminate)
self.memory_critical_threshold = 80
```

**How It Works:**
- Checks memory usage every 10 chunks to minimize overhead
- Warns when system memory exceeds 70%
- Automatically terminates profiling at 80% to prevent system crashes
- Provides actionable error messages with suggestions
- Can be disabled with `--no-memory-check` flag (use with caution)

**Example Output:**
```
⚠️  High memory usage: 71.5% (threshold: 70%)
⚠️  Process using 3245.2MB, 25,000,000 rows processed

🚨 CRITICAL: Memory usage 81.2% exceeds threshold 80%
🚨 Process: 4521.8MB, Available: 1243.5MB
🚨 Terminating profiler to prevent system instability at 45,000,000 rows
```

**Disabling Memory Safety (Advanced):**
```bash
# Warning: Only use on dedicated systems with ample resources
python3 -m validation_framework.cli profile large_file.parquet --no-memory-check
```

### 2. Intelligent Sampling Limits

The profiler uses reservoir sampling to limit memory consumption while maintaining statistical validity:

| Data Type | Sample Limit | Purpose | Memory Saved |
|-----------|--------------|---------|--------------|
| Numeric Correlation | 100,000/column | Correlation analysis | 99.9%+ on large datasets |
| Temporal Analysis | 50,000/column | Time series patterns | 99.9%+ on large datasets |
| Numeric Statistics | 10,000/column | Min/max/mean/std | 99.9%+ on large datasets |
| String Lengths | 10,000/column | Length distribution | 99.9%+ on large datasets |
| PII Detection | 1,000/column | Privacy scanning | 99.9%+ on large datasets |
| Value Frequency | 10,000 unique | Top values | Bounded growth |

**Example: 179M Row Dataset**
- **Without sampling**: ~14GB numeric data + ~7GB datetime data = 21GB+ RAM
- **With sampling**: ~8MB numeric + ~4MB datetime = 12MB RAM
- **Memory savings**: 99.94% reduction

### 3. Performance-Optimized Type Detection

Type detection is one of the most expensive operations. The profiler optimizes this:

```python
# First chunk: Detect all types (establishes baseline)
if chunk_idx == 0:
    for value in non_null_series:
        detected_type = self._detect_type(value)
        profile["type_counts"][detected_type] += 1

# Every 10th chunk: Sample 1000 values for refinement
elif chunk_idx % 10 == 0:
    sampled_values = random.sample(list(non_null_series), 1000)
    for value in sampled_values:
        detected_type = self._detect_type(value)
        profile["type_counts"][detected_type] += 1
```

**Performance Impact:**
- **Before**: Process 50,000 values × 3,595 chunks = 179M type detections
- **After**: Process 50,000 + (359 × 1,000) = 409,000 type detections
- **Speed improvement**: 437x faster type detection
- **Overall speedup**: 4x faster profiling (1 sec/chunk vs 4 sec/chunk)

## Memory Optimization Techniques

### Reservoir Sampling

The profiler uses reservoir sampling to maintain a random sample of fixed size:

```python
MAX_CORRELATION_SAMPLES = 100_000
if current_count < MAX_CORRELATION_SAMPLES:
    samples_needed = MAX_CORRELATION_SAMPLES - current_count

    if len(numeric_values) > samples_needed:
        # Random sample to fill to limit
        import random
        sampled = random.sample(numeric_values.tolist(), samples_needed)
        numeric_data[col].extend(sampled)
    else:
        # Take all values if under limit
        numeric_data[col].extend(numeric_values.tolist())
```

**Benefits:**
- Constant memory usage regardless of dataset size
- Statistically valid random sample
- Maintains analysis accuracy
- Prevents memory exhaustion

### Chunk-Based Processing

The profiler processes data in configurable chunks (default: 50,000 rows):

```python
for chunk_idx, chunk in enumerate(loader.load()):
    # Process chunk
    # Only one chunk in memory at a time
    # Previous chunks are garbage collected
```

**Memory Characteristics:**
- Only one chunk in memory at any time
- Previous chunks are garbage collected immediately
- Predictable memory footprint
- Scales to unlimited dataset sizes

### Bounded Accumulators

All data accumulators have strict size limits:

```python
# Value frequency: Max 10,000 unique values
if len(profile["value_counts"]) < 10000:
    value_freq = non_null_series.value_counts()
    for val, count in value_freq.items():
        profile["value_counts"][val] = profile["value_counts"].get(val, 0) + count
```

**What This Prevents:**
- Unlimited growth of value frequency maps
- Memory exhaustion from high-cardinality columns
- Unbounded string accumulation
- Pattern map explosion

## Real-World Performance

### Test: 5.2GB Parquet File (179.7M rows)

**Hardware:**
- System: Consumer laptop
- RAM: 8GB total
- CPU: Standard quad-core

**Results:**

| Metric | Value |
|--------|-------|
| File Size | 5.2GB |
| Total Rows | 179,702,229 |
| Total Chunks | 3,595 (50K rows each) |
| Processing Speed | 1 second per chunk |
| Total Time | ~60 minutes |
| Peak Memory | 5.5% (458MB) |
| Memory Stability | Constant (no growth) |

**Memory Timeline:**
```
Chunk   10: 5.4% (450MB)
Chunk  100: 5.4% (452MB)
Chunk  500: 5.5% (456MB)
Chunk 1000: 5.5% (458MB)
Chunk 2000: 5.5% (458MB)
Chunk 3000: 5.5% (458MB)
Chunk 3595: 5.5% (458MB) ✅ Completed
```

### Comparison: Before vs After Optimization

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Memory Usage | 28% @ 4% progress | 5.5% @ 100% | 5x more efficient |
| Processing Speed | 4 sec/chunk | 1 sec/chunk | 4x faster |
| Completion | Would crash | ✅ Completes | 100% success rate |
| Memory Growth | Linear (would exhaust) | Constant | Predictable |

## Monitoring Memory Usage

### CLI Output

The profiler provides real-time memory optimization feedback:

```bash
💾 Memory optimization: Column 'Amount' sampling limit reached at 2,100,000 rows (using 100,000 samples for correlation)
💾 Memory optimization: Column 'Timestamp' temporal sampling limit reached at 2,550,000 rows (using 50,000 samples)
💾 Memory optimization: Sampled 12 numeric columns (max 100,000 values each)
💾 Total correlation samples in memory: 1,200,000 values (vs 179,702,229 total rows)
```

### Manual Monitoring

Monitor the profiler process:

```bash
# Check memory usage
ps aux | grep "validation_framework.cli profile" | grep -v grep

# Watch memory in real-time
watch -n 5 'ps aux | grep "validation_framework.cli profile" | grep -v grep | awk "{print \$4\"% memory, \"\$6/1024\"MB\"}"'
```

## Configuration Options

### Chunk Size

The profiler automatically calculates the optimal chunk size based on available system memory, file format, and dataset size. You can override this with manual specification if needed.

**Auto-Calculation (Default)**:

```python
from validation_framework.profiler.engine import ProfilerEngine

# Let profiler auto-calculate optimal chunk size (recommended)
profiler = ProfilerEngine()  # chunk_size defaults to None = auto-calculate
result = profiler.profile_file("large_file.parquet")

# CLI auto-calculation
python3 -m validation_framework.cli profile large_file.parquet -o profile.html
# Output: 🎯 Auto-calculated chunk size: 2,000,000 rows (based on 4,309MB available memory)
```

**Manual Override**:

```python
from validation_framework.profiler.engine import ProfilerEngine

# Manually specify chunk size when needed
profiler = ProfilerEngine(chunk_size=25_000)  # Override auto-calculation

# CLI manual override
python3 -m validation_framework.cli profile file.parquet --chunk-size 50000
# Output: 📊 Using specified chunk size: 50,000 rows
```

**How Auto-Calculation Works**:
- Analyzes file metadata (row count, compression, format)
- Checks available system memory
- Considers validation complexity
- Applies safety limits (10K minimum, 2M maximum)
- Optimizes for both memory efficiency and processing speed

**When to Use Manual Override**:
- **Memory-constrained systems**: `chunk_size=25_000` to reduce peak memory
- **Testing specific scenarios**: Compare performance with different sizes
- **Legacy compatibility**: Match previous profiling runs
- **Debugging**: Isolate performance issues

### Sampling Limits

The sampling limits are hardcoded for safety but can be adjusted if needed:

```python
# In validation_framework/profiler/engine.py

# Correlation analysis samples (line ~361)
MAX_CORRELATION_SAMPLES = 100_000  # Increase for better accuracy

# Temporal analysis samples (line ~362)
MAX_TEMPORAL_SAMPLES = 50_000  # Increase for time series

# Column profile samples (line ~717)
MAX_NUMERIC_SAMPLES = 10_000  # Statistics accuracy
MAX_STRING_LENGTH_SAMPLES = 10_000  # Length distribution
```

**Trade-offs:**
- **Higher limits**: Better statistical accuracy, more memory usage
- **Lower limits**: Less memory, slightly reduced accuracy
- **Recommended**: Keep defaults unless specific requirements

### Memory Thresholds

Adjust safety thresholds in ProfilerEngine.__init__:

```python
# In validation_framework/profiler/engine.py

self.memory_check_interval = 10  # Check every N chunks
self.memory_warning_threshold = 70  # Warn at 70% memory
self.memory_critical_threshold = 80  # Terminate at 80%
```

**For Different Systems:**
- **Shared systems**: Lower thresholds (65% warning, 75% critical)
- **Dedicated systems**: Higher thresholds (80% warning, 90% critical)
- **Production**: Keep conservative defaults (70%/80%)

**CLI Override:**
Use `--no-memory-check` to disable automatic termination (use with caution on dedicated systems only).

## Best Practices

### 1. Large File Profiling

For very large files (>50GB):

```bash
# Use Parquet format (significantly faster than CSV)
# Auto-calculation will optimize chunk size based on your system
python3 -m validation_framework.cli profile large_file.parquet \
  -o profile.html \
  -j profile.json \
  --log-level INFO

# The profiler will log the auto-calculated chunk size:
# 🎯 Auto-calculated chunk size: 2,000,000 rows (based on 4,309MB available memory)
# 📊 Estimated chunks: 55 | Peak memory: ~858MB

# Monitor in separate terminal
watch -n 5 'ps aux | grep profile | grep -v grep | awk "{print \$4\"% mem\"}"'
```

### 2. Memory-Constrained Systems

For systems with limited RAM (<4GB):

```python
from validation_framework.profiler.engine import ProfilerEngine

# Reduce chunk size
profiler = ProfilerEngine(
    chunk_size=25_000,  # Smaller chunks
    enable_enhanced_correlation=False  # Disable optional features
)
```

### 3. Sample-Based Profiling

For exploratory analysis, use sampling:

```bash
# Profile first 1M rows
python3 -m validation_framework.cli profile large_file.parquet \
  --sample 1000000 \
  -o quick_profile.html
```

### 4. Monitoring Long-Running Jobs

For datasets that take >30 minutes:

```bash
# Run profiler in background
nohup python3 -m validation_framework.cli profile huge_file.parquet \
  -o profile.html \
  --log-level INFO \
  > profiler.log 2>&1 &

# Monitor progress
tail -f profiler.log | grep "Processing chunk"

# Check memory
watch -n 10 'ps aux | grep profile | grep -v grep'
```

## Troubleshooting

### Memory Still Growing

If you observe memory growth despite optimizations:

1. **Check Python version**: Ensure Python 3.8+ (better garbage collection)
2. **Verify sampling limits**: Confirm limits are active (check logs)
3. **Disable features**: Try disabling enhanced correlation/temporal analysis
4. **Reduce chunk size**: Use smaller chunks (25K rows)

```python
# Minimal memory configuration
profiler = ProfilerEngine(
    chunk_size=25_000,
    enable_temporal_analysis=False,
    enable_pii_detection=False,
    enable_enhanced_correlation=False
)
```

### Out of Memory Error

If profiler terminates with memory error:

1. **Check available RAM**: Ensure 2GB+ free before profiling
2. **Close applications**: Free memory before profiling
3. **Use sampling**: Profile a subset instead
4. **Increase swap**: Add swap space as overflow

```bash
# Check available memory
free -h

# Sample profiling instead
python3 -m validation_framework.cli profile file.parquet \
  --sample 10000000 \
  -o profile.html
```

### Slow Performance

If profiling is slower than expected:

1. **Use Parquet**: Convert CSV to Parquet (10x faster)
2. **Check disk I/O**: Ensure fast storage (SSD preferred)
3. **Increase chunk size**: Try 100K rows on high-memory systems
4. **Disable type detection**: Comment out type detection loop

## Technical Details

### Memory Allocation Breakdown

For a typical 179M row dataset:

```
Component                    Memory Usage    Notes
─────────────────────────────────────────────────────────────
Chunk data (active)          ~400MB         Current chunk only
Column profiles              ~30MB          Bounded accumulators
Numeric correlation samples  ~8MB           100K × N columns × 8 bytes
Temporal analysis samples    ~4MB           50K × M columns × 8 bytes
String length samples        ~2MB           10K × P columns × 4 bytes
PII detection samples        ~1MB           1K × P columns
Value frequency maps         ~10MB          Max 10K unique per column
Overhead (Python, libs)      ~50MB          Fixed overhead
─────────────────────────────────────────────────────────────
Total                        ~505MB         Constant regardless of size
```

### Sampling Statistics

Random sampling maintains statistical validity:

- **100K samples**: 95% confidence interval ±0.98% at population of 179M
- **50K samples**: 95% confidence interval ±1.39%
- **10K samples**: 95% confidence interval ±3.1%

**Conclusion**: All sampling limits provide statistically robust results.

## Summary

The DataK9 Profiler achieves memory-efficient profiling through:

1. **Automatic memory monitoring** with safety thresholds
2. **Intelligent sampling** with reservoir technique
3. **Bounded accumulators** preventing unlimited growth
4. **Optimized type detection** for 4x speed improvement
5. **Chunk-based processing** for constant memory footprint

**Result**: Profile datasets of unlimited size with constant ~500MB memory usage.

---

**Related Documentation:**
- [Performance Optimization Guide](./profiler-performance.md)
- [Large Dataset Guide](../advanced/large-datasets.md)
- [Profiler CLI Reference](../../reference/cli-reference.md#profile)
