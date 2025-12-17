# Streaming Correlation

## Overview

The DataK9 profiler now includes streaming correlation calculation for very large datasets. This feature enables memory-efficient correlation analysis by computing correlations incrementally using Welford's online algorithm, avoiding the need to load entire datasets into memory.

## Features

- **Memory Efficient**: O(n²) memory where n = number of columns (NOT number of rows)
- **Numerically Stable**: Uses Welford's algorithm with compensated summation
- **Automatic Selection**: Automatically chooses between streaming and standard correlation based on dataset size
- **Chunked Processing**: Processes data in configurable chunks
- **Dual Backend Support**: Works with both pandas and Polars backends
- **Accurate**: Matches standard correlation to machine precision (< 1e-10 error)

## Algorithm

The implementation uses Welford's online algorithm for computing covariance and correlation:

For correlation between variables X and Y, we track:
- **n**: Number of samples processed
- **mean_x, mean_y**: Running means
- **M2_x, M2_y**: Sum of squared differences from mean (for variance)
- **C**: Co-moment (for covariance)

### Update Formulas

For each new pair (x, y):

```
n += 1
delta_x = x - mean_x
mean_x += delta_x / n
delta_y = y - mean_y
mean_y += delta_y / n
M2_x += delta_x * (x - mean_x)
M2_y += delta_y * (y - mean_y)
C += delta_x * (y - mean_y)
```

### Final Correlation

```
correlation = C / sqrt(M2_x * M2_y)
```

## Usage

### Automatic Mode (Recommended)

The profiler automatically selects streaming correlation for large files:

```python
from validation_framework.profiler.polars_engine import PolarsDataProfiler

profiler = PolarsDataProfiler(
    chunk_size=200000,
    enable_correlations=True
)

result = profiler.profile_file('large_dataset.csv')

# Check which method was used
print(result.correlations['method'])  # 'streaming' or 'standard'

# Get correlations
correlations = result.correlations['correlations']
for pair, corr in correlations.items():
    print(f"{pair}: {corr:.4f}")
```

### Direct Usage

You can also use the streaming correlation classes directly:

```python
from validation_framework.profiler.streaming_correlation import (
    StreamingCorrelation,
    StreamingCorrelationMatrix
)
import pandas as pd

# Single pair correlation
corr = StreamingCorrelation()
for chunk in pd.read_csv('data.csv', chunksize=100000):
    corr.update_batch(chunk['x'].values, chunk['y'].values)

correlation = corr.get_correlation()
covariance = corr.get_covariance()
```

### Multiple Column Correlation Matrix

```python
# Full correlation matrix
matrix = StreamingCorrelationMatrix(['col1', 'col2', 'col3'])

for chunk in pd.read_csv('data.csv', chunksize=100000):
    matrix.update_from_dataframe(chunk)

# Get results
correlations = matrix.get_correlation_dict()
corr_matrix = matrix.get_correlation_matrix()  # As numpy array
summary = matrix.get_summary()  # With strong correlations highlighted
```

## Configuration

### Threshold Settings

The profiler uses the following thresholds:

- **Streaming threshold**: 500,000 rows
  - Files with > 500K rows use streaming correlation
  - Files with ≤ 500K rows use standard correlation

- **Maximum rows**: Files larger than the streaming threshold are limited to the first million rows for correlation calculation (configurable)

### Customizing Chunk Size

```python
profiler = PolarsDataProfiler(
    chunk_size=100000,  # Process 100K rows per chunk
    enable_correlations=True
)
```

## Performance

### Memory Usage

- **Standard correlation**: O(rows * columns) - full dataset in memory
- **Streaming correlation**: O(columns²) - only correlation state in memory

### Accuracy

Streaming correlation matches standard correlation to machine precision:
- Typical error: < 1e-10
- Handles numerical edge cases (large/small values, mixed magnitudes)

### Example Performance

For a dataset with 1M rows and 10 numeric columns:

- **Standard**: ~500 MB memory, loads entire dataset
- **Streaming**: ~1 KB memory (tracking 45 correlation pairs), processes in chunks

## Edge Cases

The implementation handles several edge cases:

1. **Missing Values**: Automatically skips pairs with NaN values
2. **Constant Columns**: Returns `None` for correlations involving constant variables
3. **Insufficient Data**: Returns `None` if fewer than 2 valid samples
4. **Numerical Stability**: Uses compensated summation for accuracy with extreme values

## API Reference

### StreamingCorrelation

Main class for computing correlation between two variables.

**Methods:**
- `update(x, y)`: Add a single pair of values
- `update_batch(x_values, y_values)`: Add arrays of values (faster)
- `get_correlation()`: Get final correlation coefficient
- `get_covariance()`: Get covariance
- `reset()`: Reset all statistics

### StreamingCorrelationMatrix

Class for computing correlations between multiple columns.

**Methods:**
- `update_from_dataframe(df)`: Update from DataFrame chunk
- `update_from_dict(data)`: Update from dict of arrays
- `get_correlation_dict()`: Get correlations as dictionary
- `get_correlation_matrix()`: Get as 2D numpy array
- `get_sample_counts()`: Get sample counts per pair
- `get_summary()`: Get summary with strong correlations
- `reset()`: Reset all correlations

## Examples

### Example 1: Profile Large CSV with Correlations

```python
profiler = PolarsDataProfiler(
    chunk_size=200000,
    backend='polars',
    enable_correlations=True
)

result = profiler.profile_file('large_dataset.csv')

print(f"Method: {result.correlations['method']}")
print(f"Columns: {result.correlations['numeric_columns']}")
print(f"Rows processed: {result.correlations['sample_size']:,}")

# Strong correlations
for pair, corr in result.correlations['correlations'].items():
    if abs(corr) > 0.7:
        print(f"{pair}: {corr:.3f}")
```

### Example 2: Monitor Memory-Efficient Processing

```python
from validation_framework.profiler.streaming_correlation import StreamingCorrelationMatrix

matrix = StreamingCorrelationMatrix(['feature1', 'feature2', 'feature3'])

for chunk_num, chunk in enumerate(pd.read_csv('huge_file.csv', chunksize=50000)):
    matrix.update_from_dataframe(chunk)

    if chunk_num % 10 == 0:
        counts = matrix.get_sample_counts()
        print(f"Chunk {chunk_num}: {counts['feature1|feature2']} samples processed")

final_corrs = matrix.get_correlation_dict()
```

### Example 3: Compare Streaming vs Standard

```python
import time
import pandas as pd
from validation_framework.profiler.streaming_correlation import StreamingCorrelationMatrix

df = pd.read_csv('data.csv')

# Standard correlation
start = time.time()
standard_corr = df.corr()
standard_time = time.time() - start

# Streaming correlation
matrix = StreamingCorrelationMatrix(df.columns.tolist())
start = time.time()
for i in range(0, len(df), 10000):
    chunk = df.iloc[i:i+10000]
    matrix.update_from_dataframe(chunk)
streaming_corr = matrix.get_correlation_dict()
streaming_time = time.time() - start

print(f"Standard: {standard_time:.2f}s")
print(f"Streaming: {streaming_time:.2f}s")

# Verify accuracy
for i, col1 in enumerate(df.columns):
    for col2 in df.columns[i+1:]:
        pair_key = f"{col1}|{col2}"
        diff = abs(standard_corr.loc[col1, col2] - streaming_corr[pair_key])
        assert diff < 1e-10, f"Mismatch for {pair_key}: {diff}"
print("✓ Results match within tolerance")
```

## Testing

Comprehensive tests are available in `tests/test_streaming_correlation.py`:

```bash
pytest tests/test_streaming_correlation.py -v
```

Test coverage includes:
- Perfect correlations (positive/negative)
- Numerical accuracy vs NumPy
- Incremental vs batch updates
- Chunked processing
- Missing value handling
- Edge cases (constant variables, insufficient data)
- Numerical stability (large/small values)
- pandas and Polars backend compatibility

## References

- Welford, B. P. (1962). "Note on a method for calculating corrected sums of squares and products"
- Pébay, P. (2008). "Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order Statistical Moments"
