# Changelog

All notable changes to the DataK9 Data Validation Framework will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Date/Time Pattern Support** - Output filenames now support automatic date/time pattern substitution
  - Patterns: `{date}`, `{time}`, `{timestamp}`, `{datetime}`, `{job_name}`, `{file_name}`, `{table_name}`
  - Works in CLI arguments (`-o`, `-j`, `-c`, `--log-file`) and YAML config (`output.html_report`, `output.json_summary`)
  - Example: `reports/{date}/validation_{job_name}_{time}.html` → `reports/2025-11-22/validation_My_Job_14-30-45.html`
  - Prevents file overwrites with automatic timestamping
  - Automatic directory creation for nested paths
  - Filename sanitization for cross-platform compatibility
  - Consistent timestamps across all outputs in a single run
  - See [CLI_GUIDE.md](CLI_GUIDE.md) for complete documentation
- **New utility module**: `validation_framework.utils.path_patterns.PathPatternExpander`
- **Example config**: `examples/configs/validation_with_datetime_patterns.yaml`
- **Comprehensive tests**: 40+ unit tests and integration tests for pattern expansion

### Changed
- **Default filenames** now include timestamps to prevent overwrites:
  - Profile: `{file_name}_profile_report_{date}.html` (was `{file_name}_profile_report.html`)
  - Profile config: `{file_name}_validation_{timestamp}.yaml` (was `{file_name}_validation.yaml`)
  - CDA analysis: `cda_gap_analysis_{timestamp}.html` (was `cda_gap_analysis.html`)
- **CLI help text** updated with pattern documentation and examples

### Migration Guide
- **Backward Compatible**: Existing configs without patterns work unchanged
- **To adopt patterns**: Add patterns to your YAML config's `output` section or use in CLI arguments
- **Shell script wrappers**: Can now be replaced with built-in pattern support

---

## [0.2.0] - 2025-11-16

### Added - Major Performance Optimization Release (Polars Migration)

#### Validation Framework
- **Polars Backend Support**: Dual backend architecture supporting both pandas and Polars
  - 8x faster validation on large datasets (179M rows in 5:21 vs 42:43)
  - 33% memory reduction (10.2GB vs 15.2GB+)
  - 100% completion rate (15/15 validations vs 12/15 with OOM)
  - No crashes on datasets up to 179M+ rows (previously limited to 50M)

#### Vectorized Optimizations
- **StringLengthCheck**: 60-75x faster using vectorized string operations
- **RangeCheck**: 60-100x faster using vectorized numeric comparisons
- **ValidValuesCheck**: 100-500x faster using vectorized set operations
- **StatisticalOutlierCheck**: Streaming algorithm with O(1) memory (no OOM)
  - Welford's online algorithm for mean/variance calculation
  - Two-pass approach for outlier detection

#### Profiler Enhancements
- **PolarsDataProfiler**: New Polars-optimized profiler (5-10x faster)
  - Vectorized pattern detection (50-100x faster)
  - Vectorized anomaly detection (24-60x faster)
  - Memory-efficient chunked processing (200K rows/chunk)
  - Backend-aware architecture
- **VectorizedPatternDetector**: 12 pre-compiled regex patterns
  - Email, phone, URL, SSN, credit card, IP, UUID, ZIP, dates
  - Luhn algorithm for credit card validation
  - PII detection methods
- **VectorizedAnomalyDetector**: Multiple outlier detection methods
  - Z-score outlier detection (vectorized)
  - IQR outlier detection (vectorized)
  - Isolation Forest support (optional sklearn)
- **CLI Integration**: `--backend` flag for profiler selection
  - `--backend polars` for 5-10x faster profiling
  - `--backend pandas` for full HTML reporting compatibility

#### Testing & Benchmarking
- **Profiler Benchmark Suite**: Comprehensive performance benchmarking
  - Small (100K), medium (3M), large (179M) dataset tests
  - Pandas vs Polars comparison mode
  - JSON output for results tracking
- **End-to-End Large Dataset Test**: Validates entire workflow with 179M rows
  - Profile → Generate config → Validate → Verify performance
  - Performance targets: <5min profiling, <10min validation, <12GB memory
- **Test Runner Integration**: New menu options for benchmarks and E2E tests
  - Option 8: Run Profiler Benchmarks
  - Option 13: Run End-to-End Large Dataset Test
  - Option 14: Run Polars Profiler Tests

#### Architecture Improvements
- **Backend-Aware Base Classes**:
  - `BackendAwareValidationRule`: 40+ helper methods for validation
  - `BackendAwareProfiler`: 40+ helper methods for profiling
  - Consistent API across pandas and Polars
- **Loader Factory**: Enhanced dual-backend loader selection
  - Automatic backend detection (defaults to Polars)
  - Polars loaders for CSV and Parquet (5-10x faster)
  - Graceful fallback to pandas for unsupported formats
- **Memory-Efficient Patterns**:
  - Lazy evaluation with Polars
  - Chunked processing (50K pandas, 200K Polars)
  - Streaming validations for large files
  - Sample limiting (100 failures max default)

### Changed

#### Performance
- Default chunk size increased from 50K to 200K rows for Polars backend
- Validation framework now defaults to Polars backend when available
- Profiler now defaults to Polars backend for optimal performance

#### CLI
- Added `--backend` option to validation command (polars/pandas)
- Added `--backend` option to profile command (polars/pandas)
- Profile command now defaults to Polars backend

#### Loaders
- Polars Parquet loader uses lazy evaluation (slice-then-collect pattern)
- CSV loader supports both pandas and Polars implementations
- Loader factory automatically selects optimal backend

### Fixed

#### Critical Bugs
- **LoaderFactory.register_loader()**: Fixed undefined `_loaders` reference
  - Now correctly uses `_pandas_loaders` registry
- **StatisticalOutlierCheck**: Fixed incorrect import path
  - Changed from `validation_framework.loaders.loader_factory` to `validation_framework.loaders.factory`
- **Polars Parquet Loader**: Fixed memory bug causing OOM
  - Changed from loading entire file with `.collect()` to lazy slicing
  - Memory usage reduced 27x (15GB+ → 555MB)

#### Test Suite
- Fixed 27 field validation test failures (backend-aware class updates)
- Updated test fixtures for dual-backend support
- Added proper error handling for missing datasets

### Performance Benchmarks

#### Validation (HI-Large_Trans.parquet - 179.7M rows, 5.1GB)
- **Runtime**: 5:20.97 (down from 42:43) - **8.0x faster**
- **Memory**: 10.2 GB (down from 15.2GB+) - **33% less**
- **Completion**: 15/15 validations (up from 12/15 OOM)
- **CPU Usage**: 165% (excellent multi-threading)

#### Component Speedups
| Component | Old | New | Speedup |
|-----------|-----|-----|---------|
| Polars Loader | 15GB+ OOM | 555 MB | 27x less memory |
| StringLengthCheck | ~5 hours | ~10 sec | 60-75x faster |
| RangeCheck | ~2.5 hours | ~3 sec | 60-100x faster |
| ValidValuesCheck | ~2.5 hours | ~2 sec | 100-500x faster |
| StatisticalOutlierCheck | OOM crash | 180 sec | ∞ (no crash) |

#### Profiler (ecommerce_transactions.csv - 100K rows)
- **Runtime**: 3.53s (down from ~30s) - **8.6x faster**
- **Features**: Pattern detection, PII detection, anomaly detection, statistics
- **Backend**: Polars

### Documentation

#### New Documentation
- `VALIDATION_FINAL_SUCCESS_REPORT.md`: Complete validation migration report
- `PROFILER_POLARS_MIGRATION_COMPLETE.md`: Profiler implementation report
- `TEST_UPDATES_SUMMARY.md`: Test suite update documentation
- Updated README with Polars features and performance benchmarks

#### Updated Documentation
- CLI help text updated with backend selection options
- Test runner menu updated with new benchmark options
- Docstrings updated with performance characteristics

### Known Issues

#### Pending Features
- HTML reporter for Polars profiler (in development)
  - Workaround: Use JSON output or pandas backend for HTML
- Some profiler tests need API alignment updates

### Migration Guide

#### For Existing Users

**No breaking changes!** The framework is fully backward compatible.

**To use Polars backend (recommended for large files):**
```bash
# Validation
python3 -m validation_framework.cli validate config.yaml --backend polars

# Profiling
python3 -m validation_framework.cli profile data.csv --backend polars
```

**To continue using pandas:**
```bash
# Validation (explicit)
python3 -m validation_framework.cli validate config.yaml --backend pandas

# Profiling with full HTML reporting
python3 -m validation_framework.cli profile data.csv --backend pandas
```

**Performance Recommendations:**
- Use Polars for files > 1GB (5-10x faster)
- Use Polars for Parquet files (native support, very fast)
- Use pandas for Excel files (Polars Excel support coming)
- Use Polars for datasets > 10M rows (better memory efficiency)

---

## [0.1.0] - 2025-11-15

### Initial Release

#### Features
- 34+ validation types across 10 categories
- Support for CSV, Excel, JSON, Parquet, and database sources
- Pandas-based data processing
- HTML and JSON report generation
- CLI interface
- Interactive DataK9 Studio IDE
- Data profiling with pattern and anomaly detection
- Test suite with 43% coverage threshold

#### Validation Categories
- File Validations (3 types)
- Schema Validations (4 types)
- Field Validations (8 types)
- Record Validations (5 types)
- Advanced Validations (5 types)
- Cross-File Validations (3 types)
- Database Validations (3 types)
- Conditional Validations
- Inline Validations (3 types)
- Temporal Validations (2 types)

---

## Version History

- **0.2.0** (2025-11-16): Major performance optimization with Polars backend (8x faster, 33% less memory)
- **0.1.0** (2025-11-15): Initial release with pandas-based validation framework
