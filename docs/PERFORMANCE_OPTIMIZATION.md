# DataK9 Performance Optimization Features

## Overview

DataK9 includes intelligent performance optimization features that automatically analyze your data files and provide non-blocking recommendations to improve processing speed and reduce memory usage.

## Features

### 1. Intelligent Chunk Size Calculator

**Location**: `validation_framework/utils/chunk_size_calculator.py`

The ChunkSizeCalculator intelligently determines optimal chunk sizes based on:

- **File size and format** (CSV, Parquet, JSON, Excel)
- **Available system RAM** (uses 70% safety margin)
- **Number of validations** to be executed
- **Validation complexity** (simple, moderate, complex, heavy)

#### How It Works

```python
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

calculator = ChunkSizeCalculator()
result = calculator.calculate_optimal_chunk_size(
    file_path="data/large_file.parquet",
    file_format="parquet",
    num_validations=31,
    validation_complexity="moderate"
)

print(f"Recommended chunk size: {result['recommended_chunk_size']:,} rows")
print(f"Estimated memory: {result['estimated_memory_mb']} MB")
print(f"Rationale: {result['rationale']}")
```

#### Calculation Formula

```
Target Memory = Available RAM × 70% (safety margin)
Memory per Row = bytes_per_row × expansion_factor × complexity_factor × validation_multiplier
Chunk Size = Target Memory / Memory per Row
```

**Memory Expansion Factors**:
- CSV/Parquet expand ~3x when loaded into pandas DataFrames
- JSON expands ~3-4x due to overhead
- Excel expands ~2.5x

**Complexity Multipliers**:
- **Simple** (1.0x): EmptyFileCheck, SchemaValidation, RowCountCheck
- **Moderate** (2.0x): RegexCheck, RangeCheck, DateFormatCheck
- **Complex** (4.0x): DuplicateRowCheck, UniqueCheck, Statistical checks
- **Heavy** (8.0x): DistributionCheck, CorrelationCheck, CrossFileChecks

#### CLI Tool

```bash
# Interactive calculator
python3 -m validation_framework.utils.chunk_size_calculator

# With file path
python3 -m validation_framework.utils.chunk_size_calculator /path/to/data.csv
```

**Example Output**:
```
╔════════════════════════════════════════════════════════════════╗
║           DataK9 Chunk Size Recommendation                     ║
╚════════════════════════════════════════════════════════════════╝

File Information:
  Path: HI-Large_Trans.parquet
  Format: PARQUET
  Size: 5,195 MB
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

### 2. Performance Advisor

**Location**: `validation_framework/utils/performance_advisor.py`

The PerformanceAdvisor provides non-blocking recommendations for format optimization and memory management.

#### Features

**Format Recommendations**:
- Analyzes CSV file sizes
- Recommends Parquet conversion at three levels:
  - **100MB - 1GB**: Optional recommendation (💡 Tip)
  - **1GB - 5GB**: Strong recommendation (ℹ INFO)
  - **5GB+**: Urgent recommendation (⚠ WARNING)

**Memory Warnings**:
- Alerts for files >5GB that require significant RAM
- Recommends closing applications to free memory
- Never blocks execution - all warnings are advisory

#### Example Advisory Output

**For 158 MB CSV**:
```
💡 Tip: CSV file is 157.6 MB
   Parquet format recommended for better performance
   Benefit: Faster processing and lower memory usage
```

**For 3.5 GB CSV**:
```
ℹ  Large CSV file detected (3521.4 MB)
   Recommendation: Consider converting to Parquet format
   Benefit: 3-10x faster processing, significant memory savings
   Action:
# Convert to Parquet (requires pandas and pyarrow):
python3 -c "
import pandas as pd
df = pd.read_csv('data.csv')
df.to_parquet('data.parquet', compression='snappy', index=False)
print(f'Converted: data.parquet')
"
```

**For 15 GB CSV**:
```
⚠  Very large CSV file detected (15360.0 MB)
   Recommendation: Strongly recommend converting to Parquet format
   Benefit: 10x faster processing, 70% less memory usage
   Action: [conversion command]

⚠  Processing 15360.0 MB file will require significant memory
   Recommendation: Ensure sufficient RAM available (8GB+ recommended)
   Action: Close unnecessary applications to free memory
```

### 3. CLI Integration

Performance advisories are automatically shown in both `validate` and `profile` commands.

#### Validate Command

```bash
python3 -m validation_framework.cli validate config.yaml
```

Before validation starts, if any files are large CSV files, the advisory is displayed:

```
💡 Tip: CSV file is 157.6 MB
   Parquet format recommended for better performance
   Benefit: Faster processing and lower memory usage

Starting validation...
```

#### Profile Command

```bash
python3 -m validation_framework.cli profile data.csv -o profile.html
```

Advisory shown before profiling begins:

```
ℹ  Large CSV file detected (1234.5 MB)
   Recommendation: Consider converting to Parquet format
   Benefit: 3-10x faster processing, significant memory savings
   Action: [conversion command]

🔍 Profiling data.csv...
```

### 4. Intelligent Config Generation

When the profiler generates validation configuration files, it now calculates intelligent chunk sizes based on:

- File size and format
- Number of suggested validations
- Validation complexity (analyzed from suggestion types)
- Available system memory

**Example Generated Config**:

```yaml
# Auto-generated validation configuration
# Generated from profile of: large_data.parquet
# Generated at: 2025-11-18 09:22:56
# File size: 5195.0 MB, Rows: 179,000,000
# Recommended chunk size: 1,000,000 rows (Calculated based on available memory and validation complexity.)

validation_job:
  name: "Validation for large_data.parquet"
  description: "Auto-generated from data profile"

settings:
  chunk_size: 1000000  # Optimized for parquet format, 15 validations (moderate complexity)
  max_sample_failures: 100

files:
  - name: "large_data"
    path: "data/large_data.parquet"
    format: "parquet"

    validations:
      # ... auto-generated validations
```

**Before** (hardcoded):
```yaml
settings:
  chunk_size: 50000  # Fixed value
```

**After** (intelligent):
```yaml
settings:
  chunk_size: 1000000  # Optimized for parquet format, 15 validations (moderate complexity)
```

## Performance Benefits

### CSV vs Parquet

| Metric | CSV | Parquet | Improvement |
|--------|-----|---------|-------------|
| File Size | 10 GB | 3.5 GB | 65% smaller |
| Load Time | 180s | 18s | 10x faster |
| Memory Usage | 30 GB | 9 GB | 70% less |
| Processing Speed | 45 min | 6 min | 7.5x faster |

### Chunk Size Optimization

**Too Small (50K rows)**:
- File: 179M rows
- Chunks: 3,580 chunks
- Problem: Excessive overhead, slow processing

**Optimized (1M rows)**:
- File: 179M rows
- Chunks: 179 chunks
- Result: 20x fewer chunks, stable memory, faster processing

## Best Practices

### 1. Always Use Parquet for Large Files

If you have CSV files >100MB, convert to Parquet before processing:

```bash
# Using pandas
python3 -c "
import pandas as pd
df = pd.read_csv('large_file.csv')
df.to_parquet('large_file.parquet', compression='snappy', index=False)
"
```

**Benefits**:
- 3-10x faster processing
- 50-70% smaller file size
- Lower memory usage
- Better compression
- Columnar storage for efficient querying

### 2. Let DataK9 Calculate Chunk Size

Don't hardcode chunk sizes. Use the profiler to generate configs:

```bash
# Profile data file
python3 -m validation_framework.cli profile data.csv -c validation.yaml

# The generated config will have optimal chunk size
python3 -m validation_framework.cli validate validation.yaml
```

### 3. Monitor System Resources

```bash
# Monitor memory during validation
watch -n 1 free -h

# Monitor process memory
watch -n 1 'ps aux | grep python | grep validation'
```

### 4. Adjust Based on Results

If you encounter OOM errors:
- Reduce chunk size by 50%
- Convert to Parquet format
- Close other applications
- Disable heavy validations (distributions, correlations)

If processing is slow:
- Increase chunk size by 2x
- Use Parquet format
- Reduce logging verbosity

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
  chunk_size: 2000000  # Process in 500 chunks (max)
  max_sample_failures: 1000
```

## Troubleshooting

### Out of Memory (OOM) Errors

**Symptoms**:
- Process killed by OS
- "MemoryError" exceptions
- System becomes unresponsive

**Solutions**:
1. Reduce chunk size by 50%
2. Convert CSV to Parquet
3. Close other applications
4. Disable heavy validations

### Slow Processing

**Symptoms**:
- Hours to process moderate files
- High CPU but low memory usage
- Too many log messages

**Solutions**:
1. Increase chunk size by 2x
2. Use Parquet format
3. Reduce logging verbosity (`--log-level WARNING`)
4. Disable unnecessary validations

## Technical Details

### Memory Safety Margin

DataK9 uses only **70% of available RAM** to prevent OOM crashes:

```python
available_memory = psutil.virtual_memory().available
target_memory = int(available_memory * 0.7)
```

This leaves 30% headroom for:
- Operating system
- Other applications
- Memory spikes during processing
- Garbage collection overhead

### Format-Specific Byte Estimates

```python
BYTES_PER_ROW = {
    'csv': 150,      # Text-based, larger
    'parquet': 50,   # Compressed, much smaller
    'json': 200,     # JSON overhead
    'excel': 100,    # Binary format
}
```

These are conservative estimates. Actual bytes per row vary by:
- Number of columns
- Data types
- String lengths
- Compression level

### Validation Overhead

Multiple validations increase memory:

```python
# Account for validation accumulation
memory_per_row *= (1 + (num_validations * 0.1))
```

Examples:
- 5 validations: 1.5x memory
- 15 validations: 2.5x memory
- 31 validations: 4.1x memory

## API Reference

### ChunkSizeCalculator

```python
from validation_framework.utils.chunk_size_calculator import ChunkSizeCalculator

calculator = ChunkSizeCalculator()

# Calculate optimal chunk size
result = calculator.calculate_optimal_chunk_size(
    file_path: str,
    file_format: str = 'csv',
    num_validations: int = 1,
    validation_complexity: str = 'simple',
    target_memory_mb: Optional[int] = None
) -> Dict[str, any]

# Returns:
{
    'recommended_chunk_size': 1000000,
    'estimated_rows': 179000000,
    'estimated_chunks': 179,
    'estimated_memory_mb': 1200,
    'file_size_mb': 5195,
    'available_memory_mb': 5600,
    'rationale': 'Calculated based on...',
    'warnings': [...]
}

# Get preset recommendation
chunk_size, description = calculator.get_preset_recommendation(file_size_mb=1000)

# Generate report
report = calculator.generate_recommendation_report(
    file_path="data.csv",
    file_format="csv",
    num_validations=10,
    validation_complexity="moderate"
)
print(report)
```

### PerformanceAdvisor

```python
from validation_framework.utils.performance_advisor import get_performance_advisor

advisor = get_performance_advisor()

# Analyze file
analysis = advisor.analyze_file(
    file_path: str,
    operation: str = 'validation'  # or 'profile'
) -> Dict[str, any]

# Returns:
{
    'file_size_mb': 3521.4,
    'file_format': 'CSV',
    'warnings': [...],  # High-priority warnings
    'recommendations': [...],  # Lower-priority tips
    'should_proceed': True,  # Never blocks
    'optimal_format': 'PARQUET'
}

# Format for CLI
warnings_text = advisor.format_warnings_for_cli(analysis)
for line in warnings_text:
    print(line)

# Check if Parquet recommended
should_convert = advisor.should_recommend_parquet(file_path)
```

## Related Documentation

- **[Chunk Size Guide](CHUNK_SIZE_GUIDE.md)** - Detailed guide for chunk size optimization
- **[User Guide](USER_GUIDE.md)** - General usage instructions
- **[Validation Catalog](VALIDATION_CATALOG.md)** - Complete list of validations

## Questions or Issues

- GitHub Issues: https://github.com/danieledge/DataK9/issues
- Documentation: docs/USER_GUIDE.md
