# Cross-File Validation Quick Reference

**DataK9 Framework - Production-Ready Designs**

---

## Overview

Three new cross-file validation types designed for DataK9's architecture:
- Memory-efficient (handles 200GB+ files)
- Backend-aware (Polars + pandas)
- Chunked streaming (50K rows/chunk)
- Vectorized operations (10-100x faster)

**Full Design:** See `CROSS_FILE_VALIDATION_DESIGN.html`

---

## 1. CrossFileKeyCheck - Referential Integrity

### Purpose
Validate foreign key relationships between files (File A keys exist in File B)

### Quick YAML
```yaml
- type: "CrossFileKeyCheck"
  severity: "ERROR"
  params:
    foreign_key: "customer_id"          # Column in current file
    reference_file: "customers.csv"     # Reference file path
    reference_key: "id"                  # Column in reference file
    match_type: "exact_match"            # See match types below
    allow_null: false                    # Allow NULL in foreign key?
    reference_file_format: "csv"         # csv|parquet|excel|json
```

### Match Types

| Type | Behavior | Use Case |
|------|----------|----------|
| `exact_match` | Every key in A must exist in B | Strict foreign keys |
| `overlap` | X% of keys in A exist in B | Partial matches OK |
| `subset` | All A keys ⊆ B keys | Filtered dataset |
| `superset` | All B keys ⊆ A keys | Coverage check |

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| foreign_key | string | ✓ | - | Column in current file |
| reference_file | string | ✓ | - | Path to reference file |
| reference_key | string | ✓ | - | Column in reference file |
| match_type | enum | ✗ | exact_match | Match behavior |
| allow_null | boolean | ✗ | false | Allow NULL values |
| min_overlap_pct | float | ✗ | 95.0 | Min overlap % (for overlap mode) |
| reference_file_format | string | ✗ | csv | File format |

### Memory & Performance

| Scenario | Files | Memory | Time (Polars) | Time (Pandas) |
|----------|-------|--------|---------------|---------------|
| Small | 10 MB each | <50 MB | <2 sec | <5 sec |
| Medium | 1 GB each | <200 MB | ~30 sec | ~3 min |
| Large | 50 GB each | <500 MB | ~5 min | ~30 min |
| Huge | 200 GB each | <2 GB + disk | ~20 min | ~2 hours |

### Algorithm
1. Load reference keys into MemoryBoundedTracker (1M in-memory, spillover to SQLite)
2. Stream current file in chunks
3. Vectorized set membership check per chunk
4. Apply match_type logic

---

## 2. CrossFileConsistencyCheck - Distribution Comparison

### Purpose
Compare statistical distributions and data characteristics between files

### Quick YAML
```yaml
# Similar distributions (A/B testing baseline)
- type: "CrossFileConsistencyCheck"
  severity: "ERROR"
  params:
    column: "user_age"
    reference_file: "control_group.csv"
    reference_column: "user_age"           # Defaults to column
    check_type: "similar_distribution"     # See check types
    statistical_test: "ks_test"            # ks_test|chi_squared|t_test
    similarity_threshold: 0.95             # 0.0-1.0 (higher = more similar)

# Similar statistics (monitoring)
- type: "CrossFileConsistencyCheck"
  severity: "WARNING"
  params:
    column: "order_amount"
    reference_file: "yesterday_sales.parquet"
    check_type: "similar_statistics"
    tolerance_pct: 10.0                    # Allow 10% deviation

# Schema match (migration validation)
- type: "CrossFileConsistencyCheck"
  severity: "ERROR"
  params:
    column: "customer_age"
    reference_file: "old_system.csv"
    check_type: "schema_match"
```

### Check Types

| Type | Method | Memory | Use Case |
|------|--------|--------|----------|
| `similar_statistics` | Compare mean/std/quantiles | <1 MB | Quick monitoring |
| `similar_distribution` | KS test / Chi-squared | <10 MB | Full distribution comparison |
| `schema_match` | Type + range comparison | <100 KB | Schema drift detection |

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| column | string | ✓ | - | Column to compare |
| reference_file | string | ✓ | - | Reference file path |
| reference_column | string | ✗ | same as column | Column in reference file |
| check_type | enum | ✓ | - | Comparison type |
| similarity_threshold | float | ✗ | 0.95 | 0.0-1.0 similarity |
| statistical_test | enum | ✗ | ks_test | Test type |
| tolerance_pct | float | ✗ | 5.0 | % tolerance for stats |
| reference_file_format | string | ✗ | csv | File format |

### Algorithm (similar_statistics)
1. Compute streaming statistics for File A (Welford's algorithm)
2. Compute streaming statistics for File B
3. Compare mean, std, min, max, quantiles with tolerance
4. O(1) memory, single pass per file

### Algorithm (similar_distribution)
1. Build streaming histogram for File A (100 bins, t-digest)
2. Build streaming histogram for File B
3. Run statistical test (KS test or Chi-squared)
4. ~10MB memory, single pass per file

---

## 3. CrossFileAggregationCheck - Aggregate Relationships

### Purpose
Validate business rules between aggregate values across files

### Quick YAML
```yaml
# Order reconciliation (totals must match)
- type: "CrossFileAggregationCheck"
  severity: "ERROR"
  params:
    aggregation: "sum"
    column: "total_amount"
    operator: "=="                         # ==|!=|>|<|>=|<=
    reference_file: "line_items.csv"
    reference_aggregation: "sum"
    reference_column: "item_amount"
    tolerance_pct: 0.01                    # Allow 0.01% rounding

# Fraud detection (HI amounts should be higher)
- type: "CrossFileAggregationCheck"
  severity: "WARNING"
  params:
    aggregation: "avg"
    column: "Amount"
    operator: ">"
    reference_file: "LI-Large_Trans.csv"
    reference_aggregation: "avg"
    reference_column: "Amount"

# Count validation
- type: "CrossFileAggregationCheck"
  severity: "WARNING"
  params:
    aggregation: "count"                   # Column not needed for count
    operator: ">="
    reference_file: "accounts.parquet"
    reference_aggregation: "count"
    reference_file_format: "parquet"
```

### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| aggregation | enum | ✓ | - | sum\|count\|avg\|min\|max |
| column | string | ✗ | - | Column to aggregate (not for count) |
| operator | enum | ✓ | - | Comparison operator |
| reference_file | string | ✓ | - | Reference file path |
| reference_aggregation | enum | ✓ | - | Aggregation for reference |
| reference_column | string | ✗ | - | Column in reference file |
| tolerance | float | ✗ | 0 | Absolute tolerance |
| tolerance_pct | float | ✗ | 0 | Percentage tolerance |
| reference_file_format | string | ✗ | csv | File format |

### Aggregation Types

| Aggregation | Memory | Algorithm | Notes |
|-------------|--------|-----------|-------|
| count | O(1) | Simple counter | Fastest |
| sum | O(1) | Accumulator | Numerically stable |
| avg | O(1) | Welford's algorithm | Stable mean/variance |
| min | O(1) | Comparison per chunk | Single pass |
| max | O(1) | Comparison per chunk | Single pass |

### Algorithm
1. Stream File A in chunks, compute aggregate incrementally
2. Stream File B in chunks, compute aggregate incrementally
3. Apply operator comparison with tolerance
4. O(1) memory, single pass per file

### Performance

| Files (each) | Aggregation | Memory | Time (Polars) | Time (Pandas) |
|--------------|-------------|--------|---------------|---------------|
| 10 GB | count | <10 MB | ~10 sec | ~30 sec |
| 10 GB | sum/avg/min/max | <50 MB | ~10 sec | ~1 min |
| 100 GB | any | <100 MB | ~2 min | ~10 min |

---

## Implementation Patterns

### 1. Inherit from BackendAwareValidationRule

```python
from validation_framework.validations.backend_aware_base import BackendAwareValidationRule

class CrossFileKeyCheck(BackendAwareValidationRule):
    def get_description(self) -> str:
        return "Validates referential integrity..."

    def validate(self, data_iterator, context):
        # Implementation
        pass
```

### 2. Use Helper Methods (Backend Agnostic)

```python
# Works with both Polars and pandas
columns = self.get_columns(chunk)
null_mask = self.get_null_mask(chunk, 'customer_id')
filtered = self.filter_df(chunk, mask)
unique_vals = self.get_unique_values(chunk, 'id')
total = self.get_column_sum(chunk, 'amount')
```

### 3. Secure Path Resolution

```python
from validation_framework.validations.builtin.cross_file_checks import SecurePathResolver

reference_path = SecurePathResolver.safe_resolve_reference_path(
    reference_file=self.params['reference_file'],
    current_file=context.get('file_path'),
    base_path=context.get('base_path')
)
```

### 4. Memory-Bounded Key Tracking

```python
from validation_framework.core.memory_bounded_tracker import MemoryBoundedTracker

# Automatically spills to SQLite at 1M keys
tracker = MemoryBoundedTracker(max_memory_keys=1_000_000)

for key in keys:
    tracker.add(key)

if tracker.has_seen(key):
    # Handle duplicate

tracker.close()  # Cleanup
```

### 5. Streaming Statistics

```python
# Welford's algorithm for stable mean/variance
stats = StreamingStatistics()

for chunk in data_iterator:
    values = self.get_column_values(chunk, column)
    stats.update(values)

mean = stats.mean()
std = stats.std()
quantiles = [stats.quantile(q) for q in [0.25, 0.50, 0.75]]
```

### 6. Error Handling Pattern

```python
def validate(self, data_iterator, context):
    try:
        # Validation logic
        pass
    except ValueError as e:
        # Configuration errors
        return self._create_result(passed=False, message=f"Config error: {e}")
    except (IOError, OSError) as e:
        # File access errors
        return self._create_result(passed=False, message=f"File error: {e}")
    except Exception as e:
        # Unexpected errors
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return self._create_result(passed=False, message="Unexpected error")
```

---

## Edge Cases Handled

1. **File Not Found** → Fail with clear error message
2. **Column Not Found** → Validate early, fail fast
3. **Empty Files** → Type-specific handling (pass/fail/zero)
4. **Type Mismatches** → Attempt conversion, fail if incompatible
5. **NULL Values** → Controlled by allow_null parameter
6. **Large Key Sets** → MemoryBoundedTracker spills to SQLite
7. **Path Traversal** → SecurePathResolver blocks malicious paths
8. **Float Precision** → Always use tolerance for numeric comparisons
9. **Encoding Issues** → UTF-8 with latin-1 fallback
10. **Out of Memory** → Reduce chunk size, enable spillover

---

## Performance Optimization Checklist

- [ ] Use Parquet format for large files (10x faster than CSV)
- [ ] Enable Polars backend when available (5-10x faster)
- [ ] Load only required columns (column projection)
- [ ] Adjust chunk size based on operation complexity
- [ ] Use MemoryBoundedTracker for large key sets
- [ ] Enable lazy evaluation with Polars scan_* methods
- [ ] Vectorize all operations (avoid row loops)
- [ ] Monitor memory usage and enable disk spillover

---

## Testing Strategy

### Unit Tests
```python
def test_crossfile_key_check_exact_match():
    # Test exact_match with all keys valid
    # Test exact_match with some keys invalid
    # Test with NULL values (allow_null=True/False)
    # Test with empty files
    pass

def test_crossfile_consistency_check_statistics():
    # Test similar statistics within tolerance
    # Test statistics outside tolerance
    # Test with different data types
    pass

def test_crossfile_aggregation_check():
    # Test each operator (==, !=, >, <, >=, <=)
    # Test with tolerance (absolute and percentage)
    # Test each aggregation type (sum, count, avg, min, max)
    pass
```

### Integration Tests
- Small datasets (10 MB): Validate correctness
- Medium datasets (1 GB): Validate performance
- Large datasets (50 GB): Validate memory efficiency
- Huge datasets (200 GB): Validate disk spillover

### Performance Benchmarks
- Measure time and memory for each validation type
- Compare Polars vs pandas performance
- Test with CSV, Parquet, Excel formats
- Validate <2GB memory limit maintained

---

## Registration in DataK9

### 1. Add to validation_definitions.json

```json
"CrossFileKeyCheck": {
  "category": "Cross-File",
  "icon": "🔗",
  "description": "Validates referential integrity between files",
  "params": [
    {
      "name": "foreign_key",
      "label": "Foreign Key Column",
      "type": "text",
      "required": true,
      "help": "Column in current file"
    },
    // ... other parameters
  ],
  "examples": "Ensure all customer_id in transactions exist in customers table",
  "tips": "Use exact_match for strict foreign keys, overlap for partial matches",
  "severity_recommendation": "ERROR",
  "python_module": "validation_framework.validations.builtin.cross_file_checks"
}
```

### 2. Register in Python

```python
# In validation_framework/validations/builtin/cross_file_checks.py
from validation_framework.core.registry import register_validation

register_validation("CrossFileKeyCheck", CrossFileKeyCheck)
register_validation("CrossFileConsistencyCheck", CrossFileConsistencyCheck)
register_validation("CrossFileAggregationCheck", CrossFileAggregationCheck)
```

### 3. Update Documentation
- Add to VALIDATION_CATALOG.md
- Generate HTML docs with DocumentGenerator
- Update USER_GUIDE.md with examples

---

## Real-World Use Cases

### E-Commerce
```yaml
# Validate order totals match line items
- type: "CrossFileAggregationCheck"
  params:
    aggregation: "sum"
    column: "total_amount"
    operator: "=="
    reference_file: "order_line_items.csv"
    reference_aggregation: "sum"
    reference_column: "line_amount"
    tolerance_pct: 0.01
```

### AML Banking
```yaml
# Verify HI transactions have higher amounts than LI
- type: "CrossFileAggregationCheck"
  params:
    aggregation: "avg"
    column: "Amount"
    operator: ">"
    reference_file: "LI-Large_Trans.csv"
    reference_aggregation: "avg"
    reference_column: "Amount"
```

### Data Migration
```yaml
# Ensure all customer IDs migrated
- type: "CrossFileKeyCheck"
  params:
    foreign_key: "customer_id"
    reference_file: "legacy_customers.csv"
    reference_key: "cust_id"
    match_type: "exact_match"
```

### A/B Testing
```yaml
# Validate control and treatment groups are similar
- type: "CrossFileConsistencyCheck"
  params:
    column: "age"
    reference_file: "control_group.csv"
    check_type: "similar_distribution"
    statistical_test: "ks_test"
    similarity_threshold: 0.95
```

---

## Summary

Three production-ready cross-file validation types designed for DataK9:

1. **CrossFileKeyCheck** - Referential integrity (foreign keys)
2. **CrossFileConsistencyCheck** - Distribution comparison (statistics, schema)
3. **CrossFileAggregationCheck** - Aggregate relationships (business rules)

**Key Features:**
- Memory-efficient (handles 200GB+ files)
- Backend-aware (Polars + pandas)
- Chunked streaming (constant memory)
- Vectorized operations (10-100x faster)
- Secure (path validation, encoding handling)
- Robust (comprehensive error handling)

**Full Design Document:** `CROSS_FILE_VALIDATION_DESIGN.html`

---

**Author:** Daniel Edge
**Date:** 2025-11-16
**Framework:** DataK9 Data Quality Framework
