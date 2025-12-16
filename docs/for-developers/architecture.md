# DataK9 Architecture

**Internal system design and implementation details** 🐕

This document explains how DataK9 works internally, its architecture, design patterns, and technical implementation. Like a well-trained K9 unit, DataK9's architecture is disciplined, efficient, and purpose-built for data quality vigilance.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Design Patterns](#design-patterns)
6. [Module Reference](#module-reference)
7. [Extension Points](#extension-points)
8. [Performance Characteristics](#performance-characteristics)
9. [Error Handling](#error-handling)
10. [Testing Strategy](#testing-strategy)

---

## System Overview

DataK9 is a modular, extensible Python-based system for validating data quality across multiple file formats. Like a K9 unit's systematic approach to detection, DataK9 methodically sniffs out data quality issues.

### Key Design Principles

1. **Separation of Concerns**: Each component has a single, well-defined responsibility
2. **Extensibility**: Plugin architecture allows custom validations and loaders
3. **Performance**: Chunked processing handles large files efficiently (200GB+)
4. **Configuration-Driven**: No code required for standard validations
5. **Type Safety**: Leverages Python type hints throughout
6. **Testability**: Components are loosely coupled and easily testable

### Technology Stack

- **Python 3.9+**: Core language
- **Polars / pandas**: Dual backend for data processing (Polars default, high performance)
- **PyYAML**: Configuration parsing
- **pytest**: Testing framework (900+ tests, 48% coverage)
- **Jinja2**: HTML report generation
- **Optional**: PyArrow (Parquet support), openpyxl (Excel), scikit-learn (ML anomaly detection)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      DataK9 CLI Entry Point                      │
│                  validation_framework.cli                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Validation Engine                           │
│                  (core/engine.py)                                │
│  • Orchestrates validation workflow                              │
│  • Loads configuration                                           │
│  • Creates loaders and validations                               │
│  • Generates reports                                             │
└─────┬───────────────────────────────────────────┬───────────────┘
      │                                           │
      ▼                                           ▼
┌─────────────────────┐              ┌─────────────────────────────┐
│  Configuration      │              │   Validation Registry       │
│  (core/config.py)   │              │   (core/registry.py)        │
│  • Parses YAML      │              │   • Stores validation types │
│  • Validates config │              │   • Plugin registration     │
└─────────────────────┘              └─────────────────────────────┘
      │                                           │
      ▼                                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Loader Factory                              │
│                  (loaders/factory.py)                            │
│  • Creates appropriate loader based on file format               │
└────────────┬────────────────────────────────────────────────────┘
             │
    ┌────────┴────────┬────────────┬──────────────┬──────────────┐
    ▼                 ▼            ▼              ▼              ▼
┌─────────┐    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│CSV      │    │Excel     │  │JSON      │  │Parquet   │  │Custom    │
│Loader   │    │Loader    │  │Loader    │  │Loader    │  │Loaders   │
└────┬────┘    └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     │              │             │             │             │
     └──────────────┴─────────────┴─────────────┴─────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   Data Iterator             │
                    │   (yields DataFrame chunks) │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Validation Rules                                │
│                  (validations/base.py)                           │
│  • Abstract base class                                           │
│  • Condition evaluation                                          │
│  • Result creation                                               │
└────────────┬────────────────────────────────────────────────────┘
             │
    ┌────────┴────────┬────────────┬──────────────┬──────────────┐
    ▼                 ▼            ▼              ▼              ▼
┌─────────┐    ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│File     │    │Schema    │  │Field     │  │Record    │  │Custom    │
│Level    │    │Checks    │  │Checks    │  │Checks    │  │Checks    │
└────┬────┘    └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
     │              │             │             │             │
     └──────────────┴─────────────┴─────────────┴─────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   ValidationResult          │
                    │   (core/results.py)         │
                    └─────────────┬───────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   Report Generation         │
                    │   (reporters/)              │
                    │   • HTML Reporter           │
                    │   • JSON Reporter           │
                    └─────────────────────────────┘
```

---

## Core Components

### 1. Validation Engine (`core/engine.py`)

**Purpose**: Orchestrates the entire validation workflow

**Key Responsibilities**:
- Load and parse configuration
- Create data loaders for each file
- Instantiate validation rules
- Execute validations in order
- Collect and aggregate results
- Generate reports

**Key Methods**:
```python
class ValidationEngine:
    def __init__(self, config: ValidationConfig)
    def run(self, verbose: bool) -> ValidationReport
    def _validate_file(self, file_config, verbose) -> FileValidationReport
    def generate_html_report(self, report, output_path)
    def generate_json_report(self, report, output_path)
```

**Workflow**:
1. Parse configuration from YAML
2. For each file in configuration:
   - Create appropriate loader
   - Get file metadata
   - For each validation rule:
     - Instantiate validation class
     - Create data iterator
     - Execute validation
     - Collect results
3. Aggregate results
4. Generate reports

---

### 2. Configuration System (`core/config.py`)

**Purpose**: Parse and validate YAML configuration

**Key Classes**:
```python
class ValidationConfig:
    job_name: str
    description: str
    chunk_size: int
    max_sample_failures: int
    files: List[Dict[str, Any]]

    @classmethod
    def from_yaml(cls, config_path: str) -> ValidationConfig
    def to_dict(self) -> Dict[str, Any]
```

**Configuration Validation**:
- Validates required fields
- Checks file paths exist
- Validates severity levels
- Infers file formats from extensions
- Parses validation parameters

**Design Pattern**: Builder pattern for configuration construction

---

### 3. Registry System (`core/registry.py`)

**Purpose**: Plugin architecture for validation rules

**Key Features**:
- Singleton pattern for global registry
- Register validation classes by name
- Retrieve validation classes for instantiation
- List available validations

**Key Methods**:
```python
class ValidationRegistry:
    def register(self, name: str, validation_class: Type[DataValidationRule])
    def get(self, name: str) -> Type[DataValidationRule]
    def is_registered(self, name: str) -> bool
    def list_available(self) -> List[str]

# Global functions
def get_registry() -> ValidationRegistry
def register_validation(name: str, validation_class)
```

**Auto-Registration**:
Built-in validations are automatically registered on import:
```python
# validation_framework/validations/builtin/registry.py
def register_all_builtin_validations():
    register_validation("MandatoryFieldCheck", MandatoryFieldCheck)
    register_validation("RangeCheck", RangeCheck)
    # ... all built-in validations
```

---

### 4. Loader System (`loaders/`)

**Purpose**: Abstract file format differences

**Base Class** (`loaders/base.py`):
```python
class DataLoader(ABC):
    @abstractmethod
    def load(self) -> Iterator[pd.DataFrame]:
        """Return iterator yielding data chunks"""

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """Return file metadata (row count, columns, etc.)"""
```

**Loader Factory** (`loaders/factory.py`):
```python
class LoaderFactory:
    _loaders: Dict[str, Type[DataLoader]] = {
        "csv": CSVLoader,
        "excel": ExcelLoader,
        "json": JSONLoader,
        "parquet": ParquetLoader,
    }

    @classmethod
    def create_loader(cls, file_path, file_format, **kwargs) -> DataLoader

    @classmethod
    def register_loader(cls, format_name, loader_class)
```

**Design Pattern**: Factory pattern with registration mechanism

**Implementations**:
- **CSVLoader**: Uses `pd.read_csv()` with chunking
- **ExcelLoader**: Uses `pd.read_excel()` with sheet support
- **JSONLoader**: Handles JSON arrays and JSON Lines with flattening
- **ParquetLoader**: Uses `pd.read_parquet()` with columnar efficiency (10x faster!)

---

### 5. Validation Rule System (`validations/`)

**Base Class** (`validations/base.py`):
```python
class DataValidationRule(ABC):
    def __init__(self, name, severity, params, condition=None):
        self.name = name
        self.severity = severity
        self.params = params
        self.condition = condition

    @abstractmethod
    def validate(self, data_iterator, context) -> ValidationResult:
        """Execute validation logic"""

    @abstractmethod
    def get_description(self) -> str:
        """Human-readable description"""

    def _evaluate_condition(self, df) -> pd.Series:
        """Evaluate condition expression"""

    def _create_result(self, passed, message, **kwargs) -> ValidationResult:
        """Helper to create validation results"""
```

**Validation Categories**:
1. **File-Level**: Operate on entire file (EmptyFileCheck, RowCountRangeCheck)
2. **Schema**: Validate structure (SchemaMatchCheck, ColumnPresenceCheck)
3. **Field-Level**: Per-field validation (MandatoryFieldCheck, RangeCheck, RegexCheck)
4. **Record-Level**: Cross-row validation (DuplicateRowCheck, UniqueKeyCheck)
5. **Conditional**: If-then-else logic (ConditionalValidation)
6. **Advanced**: Statistical and complex (StatisticalOutlierCheck, CrossFieldComparisonCheck)

**Design Pattern**: Template Method pattern - base class defines workflow, subclasses implement specifics

---

### 6. Results System (`core/results.py`)

**Purpose**: Standardized validation results

**Key Classes**:
```python
@dataclass
class ValidationResult:
    rule_name: str
    severity: Severity
    passed: bool
    message: str
    failed_count: int
    total_count: int
    sample_failures: List[Dict]
    execution_time: float

class FileValidationReport:
    file_name: str
    file_path: str
    status: Status
    validations: List[ValidationResult]

    def add_result(self, result: ValidationResult)
    def update_status(self)

class ValidationReport:
    job_name: str
    execution_time: datetime
    overall_status: Status
    file_reports: List[FileValidationReport]

    def add_file_report(self, file_report)
    def update_overall_status(self)
    def to_dict(self) -> Dict
```

**Status Enum**:
```python
class Status(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"

class Severity(Enum):
    ERROR = "ERROR"
    WARNING = "WARNING"
```

---

### 7. Reporter System (`reporters/`)

**Purpose**: Generate output reports

**Base Class** (`reporters/base.py`):
```python
class Reporter(ABC):
    @abstractmethod
    def generate(self, report: ValidationReport, output_path: str)
```

**Implementations**:
- **HTMLReporter** (`reporters/html_reporter.py`):
  - Uses Jinja2 templates
  - Generates styled, interactive HTML with dark theme
  - Includes summary, details, sample failures

- **JSONReporter** (`reporters/json_reporter.py`):
  - Serializes ValidationReport to JSON
  - Machine-readable for CI/CD integration
  - Preserves all validation details

---

## Data Flow

### Detailed Execution Flow

```
1. CLI Entry
   ├─> Parse command line arguments
   └─> Load configuration file

2. Configuration Loading
   ├─> Parse YAML
   ├─> Validate structure
   ├─> Resolve file paths
   └─> Create ValidationConfig object

3. Engine Initialization
   ├─> Create ValidationEngine(config)
   └─> Get validation registry

4. For Each File:
   ├─> Create Loader
   │   ├─> Detect/validate format
   │   ├─> Create appropriate loader instance
   │   └─> Get file metadata
   │
   ├─> For Each Validation:
   │   ├─> Get validation class from registry
   │   ├─> Instantiate with parameters
   │   ├─> Create data iterator (chunked)
   │   │
   │   ├─> Execute Validation:
   │   │   ├─> For each chunk:
   │   │   │   ├─> Evaluate condition (if present)
   │   │   │   ├─> Filter rows by condition
   │   │   │   ├─> Apply validation logic
   │   │   │   ├─> Collect failures
   │   │   │   └─> Continue to next chunk
   │   │   │
   │   │   └─> Create ValidationResult
   │   │
   │   └─> Add result to FileValidationReport
   │
   └─> Add FileValidationReport to ValidationReport

5. Aggregation
   ├─> Calculate overall status
   ├─> Count total errors/warnings
   └─> Calculate execution time

6. Report Generation
   ├─> HTML Report (if requested)
   └─> JSON Report (if requested)

7. Exit
   └─> Return exit code (0=pass, 1=fail, 2=error)
```

### Data Iterator Pattern

**Why Chunked Processing?**
- Memory efficiency for large files (200GB+)
- Consistent interface across file formats
- Enables streaming validation

**Implementation**:
```python
def load(self) -> Iterator[pd.DataFrame]:
    """Yield DataFrame chunks"""
    for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size):
        yield chunk

# Usage in validation
for chunk in data_iterator:
    # Process this chunk
    for idx, row in chunk.iterrows():
        # Validate row
        pass
```

**Memory Characteristics**:
- Only `chunk_size` rows in memory at once (default: 50,000)
- Validations process incrementally
- Sample failures collected up to `max_sample_failures`
- **Example**: 50,000 rows × 50 columns × 8 bytes ≈ 20 MB per chunk

---

## Design Patterns

### 1. Factory Pattern

**Used in**: Loader creation, Validation instantiation

**Purpose**: Create objects without specifying exact class

**Implementation**:
```python
class LoaderFactory:
    _loaders = {"csv": CSVLoader, "excel": ExcelLoader, ...}

    @classmethod
    def create_loader(cls, file_path, file_format, **kwargs):
        loader_class = cls._loaders[file_format]
        return loader_class(file_path, **kwargs)
```

**Benefits**:
- Easy to add new loaders
- Centralized creation logic
- Runtime format selection

---

### 2. Registry Pattern

**Used in**: Validation rule registration

**Purpose**: Plugin architecture for extensibility

**Implementation**:
```python
class ValidationRegistry:
    def __init__(self):
        self._validations: Dict[str, Type[DataValidationRule]] = {}

    def register(self, name, validation_class):
        self._validations[name] = validation_class

    def get(self, name):
        return self._validations[name]
```

**Benefits**:
- Decouple validation discovery from usage
- Enable custom validations
- Auto-registration on import

---

### 3. Template Method Pattern

**Used in**: DataValidationRule base class

**Purpose**: Define workflow skeleton, let subclasses fill in steps

---

### 4. Iterator Pattern

**Used in**: Data loading

**Purpose**: Sequential access without exposing underlying representation

**Benefits**:
- Uniform interface for all file formats
- Memory-efficient streaming
- Lazy evaluation

---

### 5. Strategy Pattern

**Used in**: Conditional validation, severity handling

**Purpose**: Select algorithm at runtime

---

### 6. Singleton Pattern

**Used in**: ValidationRegistry

**Purpose**: Single global registry instance

**Implementation**:
```python
_global_registry = None

def get_registry() -> ValidationRegistry:
    global _global_registry
    if _global_registry is None:
        _global_registry = ValidationRegistry()
    return _global_registry
```

---

### 7. Dual Backend Architecture Pattern

**Used in**: Data processing layer

**Purpose**: Support multiple DataFrame backends with unified interface

**Implementation**:
```python
class DataFrameBackend(Enum):
    POLARS = "polars"
    PANDAS = "pandas"

class UnifiedDataFrame:
    """Abstraction layer for backend-agnostic operations"""

    def __init__(self, df, backend: DataFrameBackend):
        self._df = df
        self._backend = backend

    def filter(self, condition):
        if self._backend == DataFrameBackend.POLARS:
            return self._df.filter(condition)
        else:  # pandas
            return self._df[eval(condition)]

    def select(self, columns):
        if self._backend == DataFrameBackend.POLARS:
            return self._df.select(columns)
        else:  # pandas
            return self._df[columns]
```

**Benefits**:
- Switch backends without code changes
- Optimize performance per use case (Polars for large files, pandas for Excel)
- Backend selection based on requirements
- Backend-specific optimizations (SIMD vectorization in Polars)

---

### 8. Temporal Drift Detection Pattern

**Used in**: BaselineComparisonCheck, TrendDetectionCheck

**Purpose**: Detect changes in data characteristics over time

**How It Works**:

DataK9's temporal validations compare current data against historical baselines to detect drift:

**Baseline Comparison Algorithm:**
```python
def detect_drift(current_value, historical_baseline, tolerance_pct):
    """
    Compare current metric against historical average

    Args:
        current_value: Today's metric (e.g., row count, sum)
        historical_baseline: Average from lookback period (e.g., last 30 days)
        tolerance_pct: Acceptable deviation percentage

    Returns:
        passed: True if within tolerance, False if drift detected
    """
    lower_bound = historical_baseline * (1 - tolerance_pct / 100)
    upper_bound = historical_baseline * (1 + tolerance_pct / 100)

    passed = lower_bound <= current_value <= upper_bound

    if not passed:
        deviation_pct = abs(current_value - historical_baseline) / historical_baseline * 100
        alert(f"Drift detected: {deviation_pct:.1f}% deviation from baseline")

    return passed
```

**Trend Detection Algorithm:**
```python
def detect_anomalous_trend(current_value, previous_value, max_growth_pct, max_decline_pct):
    """
    Detect unusual growth or decline patterns

    Detects:
    - Sudden spikes (potential data quality issues, duplicate loads)
    - Sudden drops (potential data loss, filtering errors)
    - Gradual drift (schema changes, upstream issues)
    """
    if previous_value == 0:
        return True  # Can't calculate trend

    growth_pct = (current_value - previous_value) / previous_value * 100

    if growth_pct > max_growth_pct:
        alert(f"Anomalous growth: {growth_pct:.1f}% (max: {max_growth_pct}%)")
        return False

    if growth_pct < -max_decline_pct:
        alert(f"Anomalous decline: {abs(growth_pct):.1f}% (max: {max_decline_pct}%)")
        return False

    return True
```

**Use Cases:**
- **Data Volume Monitoring**: Detect when daily file sizes deviate >20% from 30-day average
- **Schema Evolution Detection**: Identify when column counts or types drift over time
- **Quality Degradation**: Catch when null percentages increase compared to historical norms
- **Duplicate Load Detection**: Flag when row counts spike unexpectedly (2x overnight = likely duplicate load)

**Design Considerations:**
- **Lookback Windows**: Configurable (7, 30, 90 days) to balance responsiveness vs stability
- **Seasonal Adjustments**: Support for weekly/monthly patterns (e.g., Monday vs Friday volumes)
- **Confidence Intervals**: Statistical significance testing for small sample sizes
- **Gradual Drift**: Detect slow degradation that wouldn't trigger absolute thresholds

---

## Module Reference

### Core Modules

```
validation_framework/
├── __init__.py
├── cli.py                    # Command-line interface
├── core/
│   ├── __init__.py
│   ├── engine.py             # ValidationEngine - main orchestrator
│   ├── optimized_engine.py   # OptimizedValidationEngine - with memory monitoring
│   ├── config.py             # ValidationConfig - YAML parsing
│   ├── registry.py           # ValidationRegistry - plugin system
│   ├── results.py            # ValidationResult, Report classes
│   └── logging_config.py     # Logging configuration
├── shared_analysis/          # Shared analysis modules (profiler + validator)
│   ├── __init__.py           # Exports all shared functions
│   ├── credit_card_detection.py  # Luhn algorithm, semantic exclusion
│   ├── outlier_detection.py      # Z-score, IQR methods
│   └── postal_code_detection.py  # Multi-format with decimal rejection
├── loaders/
│   ├── __init__.py
│   ├── base.py               # DataLoader abstract base
│   ├── factory.py            # LoaderFactory
│   ├── csv_loader.py         # CSV file support
│   ├── excel_loader.py       # Excel file support
│   ├── json_loader.py        # JSON file support
│   └── parquet_loader.py     # Parquet file support
├── validations/
│   ├── __init__.py
│   ├── base.py               # DataValidationRule base class
│   ├── builtin/
│   │   ├── __init__.py
│   │   ├── registry.py       # Auto-registration
│   │   ├── file_checks.py    # File-level validations
│   │   ├── schema_checks.py  # Schema validations
│   │   ├── field_checks.py   # Field-level validations
│   │   ├── record_checks.py  # Record-level validations
│   │   ├── conditional.py    # Conditional validation
│   │   ├── inline_checks.py  # Inline/custom checks
│   │   └── advanced_checks.py # Statistical validations
│   └── custom/
│       └── __init__.py       # Custom validations
├── reference_data/
│   ├── __init__.py           # Exports ReferenceDataLoader
│   ├── loader.py             # ISO standard data loading (pycountry)
│   ├── pii/
│   │   ├── patterns.json     # PII detection patterns
│   │   └── column_indicators.json  # Column name indicators
│   └── patterns/
│       └── validation_patterns.json  # ID/measurement patterns
└── reporters/
    ├── __init__.py
    ├── base.py               # Reporter abstract base
    ├── html_reporter.py      # HTML report generation
    └── json_reporter.py      # JSON report generation
```

### Dependencies

**Core Dependencies**:
- `pandas >= 2.0.0` - Data manipulation
- `polars >= 0.20.0` - High-performance data processing (default backend)
- `pyyaml >= 6.0` - YAML parsing
- `jinja2 >= 3.1.0` - HTML templating
- `pycountry >= 22.3.5` - ISO standards (countries, currencies)

**Optional Dependencies**:
- `openpyxl >= 3.0.0` - Excel support
- `pyarrow >= 5.0.0` - Parquet support (10x faster!)
- `colorama >= 0.4.0` - Colored console output

---

## Memory Safety Architecture

### Memory Monitoring

The `OptimizedValidationEngine` includes built-in memory safety monitoring to prevent out-of-memory (OOM) crashes during validation of large datasets.

**Key Features:**

| Feature | Description |
|---------|-------------|
| **Warning Threshold** | 70% system memory usage - logs warning |
| **Critical Threshold** | 80% system memory usage - raises `MemoryError` |
| **Check Interval** | Every 5 chunks (configurable) |
| **Graceful Termination** | Clean exit with meaningful error message |

**Implementation:**

```python
class OptimizedValidationEngine:
    MEMORY_WARNING_THRESHOLD = 70   # Log warning at 70%
    MEMORY_CRITICAL_THRESHOLD = 80  # Raise error at 80%
    MEMORY_CHECK_INTERVAL = 5       # Check every N chunks

    def _check_memory_safety(self, chunk_idx: int, row_count: int) -> bool:
        """Check memory usage and raise MemoryError if critical."""
        if not PSUTIL_AVAILABLE or self.disable_memory_check:
            return True

        if chunk_idx % self.MEMORY_CHECK_INTERVAL != 0:
            return True

        memory = psutil.virtual_memory()
        if memory.percent >= self.MEMORY_CRITICAL_THRESHOLD:
            gc.collect()  # Try to free memory first
            memory = psutil.virtual_memory()
            if memory.percent >= self.MEMORY_CRITICAL_THRESHOLD:
                raise MemoryError(f"Memory critical threshold ({self.MEMORY_CRITICAL_THRESHOLD}%) exceeded")

        return True
```

**Disable Option:**

Memory monitoring can be disabled via CLI (`--disable-memory-check`) for environments where you want to manage memory externally or trust the system's OOM killer.

---

## Shared Analysis Modules

### Architecture

The `shared_analysis` package provides consistent analysis logic used by both the profiler and validator, eliminating code duplication and ensuring identical behavior.

```
shared_analysis/
├── __init__.py                    # Re-exports all public functions
├── credit_card_detection.py       # Luhn algorithm validation
├── outlier_detection.py           # Statistical outlier methods
└── postal_code_detection.py       # Multi-format postal codes
```

### Credit Card Detection

**Features:**
- Luhn algorithm (modulus 10) validation
- Semantic exclusion based on column names (prevents false positives on account_id, transaction_id, etc.)
- Support for major card formats (Visa, MasterCard, Amex, Discover)
- Configurable Luhn threshold for false positive reduction

**Usage:**
```python
from validation_framework.shared_analysis import (
    luhn_check,
    detect_credit_cards,
    is_valid_credit_card,
)

# Single value validation
is_valid = luhn_check("4532015112830366")  # True

# Series detection with details
results = detect_credit_cards(df['card_number'], return_details=True)
# {'count': 150, 'pattern_matches': 200, 'luhn_valid': 150, 'luhn_ratio': 0.75}
```

### Outlier Detection

**Methods:**
- **Z-score**: Statistical deviation from mean (configurable threshold, default 3.0)
- **IQR**: Interquartile range method (configurable multiplier, default 1.5)
- **Combined**: Both methods for comprehensive detection

**Usage:**
```python
from validation_framework.shared_analysis import (
    detect_outliers_zscore,
    detect_outliers_iqr,
    detect_outliers,
)

# Z-score detection
count = detect_outliers_zscore(series, threshold=3.0)

# IQR with bounds
results = detect_outliers_iqr(series, return_bounds=True)
# {'Q1': 25.0, 'Q3': 75.0, 'IQR': 50.0, 'lower_bound': -50.0, 'upper_bound': 150.0}

# Combined detection
results = detect_outliers(series, method='combined', return_details=True)
```

### Postal Code Detection

**Features:**
- Multi-format support (US, UK, Canada, Germany, France, generic)
- **Decimal rejection**: Prevents false positives on float columns (0.04781 is NOT a postal code)
- Column type awareness

**Usage:**
```python
from validation_framework.shared_analysis import (
    detect_postal_codes,
    is_valid_postal_code,
)

# Single value
is_valid = is_valid_postal_code("90210")  # True (US)
is_valid = is_valid_postal_code("SW1A 1AA", country='uk')  # True

# Series detection
results = detect_postal_codes(series, return_details=True)
# {'count': 45, 'formats': {'us_5digit': 40, 'us_9digit': 5}, 'rejected_decimals': 3}
```

---

## Extension Points

### 1. Custom Data Loaders

**Create a custom loader for new file formats**:

```python
from validation_framework.loaders.base import DataLoader
from validation_framework.loaders.factory import LoaderFactory
import pandas as pd

class XMLLoader(DataLoader):
    def load(self) -> Iterator[pd.DataFrame]:
        # Custom XML parsing logic
        # Yield DataFrame chunks
        pass

    def get_metadata(self) -> Dict[str, Any]:
        # Return file metadata
        pass

# Register the loader
LoaderFactory.register_loader("xml", XMLLoader)
```

**See:** [Custom Loaders Guide](custom-loaders.md)

---

### 2. Custom Validations

**Create custom validation rules**:

```python
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.registry import register_validation

class MyCustomValidation(DataValidationRule):
    def get_description(self) -> str:
        return "My custom validation logic"

    def validate(self, data_iterator, context) -> ValidationResult:
        # Custom validation logic
        # Process chunks
        # Return ValidationResult
        pass

# Register the validation
register_validation("MyCustomValidation", MyCustomValidation)
```

**See:** [Custom Validations Guide](custom-validations.md)

---

### 3. Custom Reporters

**Create custom report formats**:

```python
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport

class PDFReporter(Reporter):
    def generate(self, report: ValidationReport, output_path: str):
        # Generate PDF report
        pass
```

**See:** [Custom Reporters Guide](custom-reporters.md)

---

## Performance Characteristics

### Time Complexity

**Per Validation**:
- File-level checks: O(1) - single pass
- Schema checks: O(1) - metadata only
- Field checks: O(n) - one pass through data
- Duplicate checks: O(n log n) - requires sorting/hashing
- Statistical checks: O(n) to O(2n) - may require multiple passes

**Overall**: O(v * n) where v = number of validations, n = number of rows

### Space Complexity

**Memory Usage**:
- Chunk size: `chunk_size * number_of_columns * 8 bytes` (approximate)
- Sample failures: `max_sample_failures * number_of_failed_validations`
- Metadata: O(1) per file

**Example**:
- 50,000 rows/chunk × 50 columns × 8 bytes ≈ 20 MB per chunk
- 100 sample failures × 10 validations = up to 1000 failure records

### Scalability

**File Size**:
- ✅ 1 MB - 100 MB: Excellent performance (<1-10 seconds)
- ✅ 100 MB - 1 GB: Good performance (10-120 seconds)
- ✅ 1 GB - 10 GB: Acceptable performance with tuning (2-20 minutes)
- ✅ 10 GB - 200 GB: Use Parquet format, increase chunk size (20 minutes - 4 hours)

**Number of Validations**:
- Linear scaling: 10 validations ≈ 10x time of 1 validation
- Consider disabling expensive validations in development

**Optimization Strategies**:
1. Increase chunk_size for more columns/memory (up to 200,000)
2. Decrease chunk_size for less memory (down to 10,000)
3. **Use Parquet for large files (10x faster than CSV)**
4. Limit sample failures for faster reporting (10-50)
5. Order validations by speed (fast first)
6. Disable statistical checks during development

**See:** [Performance Tuning Guide](../using-datak9/performance-tuning.md)

---

## Error Handling

### Error Categories

**1. Configuration Errors** (exit code 2):
- Invalid YAML syntax
- Missing required fields
- File not found
- Invalid parameters

**2. Validation Failures** (exit code 1):
- Data quality issues (DataK9 detected problems!)
- Business rule violations
- Returns ValidationResult with passed=False

**3. Runtime Errors** (exit code 2):
- File read errors
- Memory errors
- Unexpected exceptions

### Error Handling Strategy

```python
try:
    # Validation logic
    result = validation.validate(data_iterator, context)
except FileNotFoundError:
    # Handle missing files
    logger.error("File not found")
    # Return error result
except pd.errors.ParserError:
    # Handle parsing errors
    logger.error("Cannot parse file")
except Exception as e:
    # Catch-all for unexpected errors
    logger.exception("Unexpected error")
    # Return error result
```

**Graceful Degradation**:
- Individual validation failures don't stop execution
- Errors are captured as ValidationResult objects
- Full report generated even with some failures

---

## Testing Strategy

### Test Pyramid

```
       ┌─────────────┐
       │   Manual    │  End-to-end scenarios
       │   Testing   │  (exploratory)
       └─────────────┘
      ┌───────────────┐
      │  Integration  │  Full workflows
      │     Tests     │  (pytest)
      └───────────────┘
    ┌───────────────────┐
    │   Component Tests  │  Individual validations
    │    (Unit Tests)    │  (pytest - 115+ tests)
    └───────────────────┘
  ┌───────────────────────┐
  │     Smoke Tests        │  Quick sanity checks
  │  (Built-in fixtures)   │  (pytest)
  └───────────────────────┘
```

### Test Categories

**1. Unit Tests** (validation_framework/tests/):
- Test individual validation classes
- Mock data iterators
- Verify ValidationResult correctness

**2. Integration Tests**:
- Test full validation workflows
- Use actual test data files
- Verify end-to-end results

**3. Fixture Tests**:
- Test with sample data files
- Verify file format handling
- Test edge cases

**Test Coverage**:
- Current: 48% code coverage
- Target: 80%+ code coverage
- Critical paths: 100% coverage
- Edge cases: Well-documented

**See:** [Testing Guide](testing-guide.md)

---

## Next Steps

- **[Custom Validations](custom-validations.md)** - Create custom validation rules
- **[Custom Loaders](custom-loaders.md)** - Add new file format support
- **[Custom Reporters](custom-reporters.md)** - Generate custom report formats
- **[API Reference](api-reference.md)** - Complete Python API documentation
- **[Contributing](contributing.md)** - Contribution guidelines

---

**🐕 Guard your data with DataK9 - architected for data quality vigilance!**
