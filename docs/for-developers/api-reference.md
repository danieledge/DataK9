# API Reference

**Complete DataK9 Python API Documentation**

This reference documents all public classes, methods, and interfaces in DataK9. Use this when creating custom validations, loaders, reporters, or integrating DataK9 into your applications.

---

## Table of Contents

1. [Core Classes](#core-classes)
2. [Validation Classes](#validation-classes)
3. [Loader Classes](#loader-classes)
4. [Reporter Classes](#reporter-classes)
5. [Results Classes](#results-classes)
6. [Registry Functions](#registry-functions)
7. [Configuration Classes](#configuration-classes)
8. [Utility Functions](#utility-functions)

---

## Core Classes

### ValidationEngine

**Path:** `validation_framework.core.engine`

The main orchestrator for running validation jobs.

```python
class ValidationEngine:
    """
    Main engine for executing validation jobs.

    DataK9's core component that loads config, creates loaders,
    instantiates validations, executes them, and generates reports.
    """

    @classmethod
    def from_config(cls, config_path: str) -> 'ValidationEngine':
        """
        Create engine from YAML configuration file.

        Args:
            config_path: Path to YAML configuration file

        Returns:
            ValidationEngine instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValidationConfigError: If config is invalid

        Example:
            engine = ValidationEngine.from_config("config.yaml")
        """
        pass

    def run(self, verbose: bool = False) -> ValidationReport:
        """
        Execute all validations defined in configuration.

        Args:
            verbose: Print detailed progress information

        Returns:
            ValidationReport with all results

        Example:
            engine = ValidationEngine.from_config("config.yaml")
            report = engine.run(verbose=True)

            if report.overall_status == Status.FAILED:
                print(f"Validation failed: {report.total_errors} errors")
        """
        pass
```

**Key Methods:**

| Method | Description | Returns |
|--------|-------------|---------|
| `from_config(config_path)` | Create engine from YAML file | ValidationEngine |
| `run(verbose=False)` | Execute validation job | ValidationReport |
| `validate_file(file_config, context)` | Validate single file | FileValidationReport |

**Usage Example:**

```python
from validation_framework.core.engine import ValidationEngine

# Create engine from config
engine = ValidationEngine.from_config("config.yaml")

# Run validation
report = engine.run(verbose=True)

# Check results
if report.overall_status.value == "FAILED":
    print(f"❌ Validation failed")
    print(f"Errors: {report.total_errors}")
    print(f"Warnings: {report.total_warnings}")
else:
    print(f"✅ Validation passed")
```

---

### ValidationConfig

**Path:** `validation_framework.core.config`

Parses and validates YAML configuration.

```python
class ValidationConfig:
    """
    Loads and validates YAML configuration.

    Ensures configuration has required fields and valid structure.
    """

    def __init__(self, config_path: str):
        """
        Load configuration from YAML file.

        Args:
            config_path: Path to YAML configuration

        Raises:
            FileNotFoundError: If config doesn't exist
            yaml.YAMLError: If YAML is malformed
        """
        pass

    def validate(self) -> None:
        """
        Validate configuration structure.

        Checks:
        - Required sections present (validation_job, files)
        - File paths exist
        - Validation types are registered
        - Parameters are valid

        Raises:
            ValidationConfigError: If configuration is invalid
        """
        pass

    def get_job_config(self) -> Dict[str, Any]:
        """Get validation_job section"""
        pass

    def get_files_config(self) -> List[Dict[str, Any]]:
        """Get files section"""
        pass

    def get_settings(self) -> Dict[str, Any]:
        """Get settings section with defaults"""
        pass
```

---

## Validation Classes

### ValidationRule (Abstract Base)

**Path:** `validation_framework.validations.base`

Base class for all validation rules.

```python
class ValidationRule(ABC):
    """
    Abstract base class for all validation rules.

    Provides conditional validation support and result creation helpers.
    """

    def __init__(
        self,
        name: str,
        severity: Severity,
        params: Optional[Dict[str, Any]] = None,
        condition: Optional[str] = None
    ):
        """
        Initialize validation rule.

        Args:
            name: Name of validation rule
            severity: ERROR or WARNING
            params: Dictionary of validation parameters
            condition: Optional conditional expression (pandas syntax)
                      Example: "age >= 18" or "status == 'ACTIVE'"
        """
        self.name = name
        self.severity = severity
        self.params = params or {}
        self.condition = condition

    @abstractmethod
    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Execute validation logic.

        Args:
            data_iterator: Iterator yielding DataFrame chunks
            context: Validation context with:
                - file_path: Path to current file
                - file_name: Name of current file
                - metadata: File metadata
                - max_sample_failures: Max failures to collect (default: 100)
                - other_files: Dict of other loaded files (for cross-file)

        Returns:
            ValidationResult object
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        Get human-readable description.

        Returns:
            String description for reports and logs
        """
        pass
```

**Helper Methods:**

```python
def _evaluate_condition(self, df: pd.DataFrame) -> pd.Series:
    """
    Evaluate conditional expression on DataFrame.

    Supports SQL-like syntax:
    - AND → & (pandas and)
    - OR → | (pandas or)
    - NOT → ~ (pandas not)

    Args:
        df: DataFrame to evaluate

    Returns:
        Boolean Series indicating which rows match condition

    Example:
        # Validation only runs on rows where this returns True
        mask = self._evaluate_condition(chunk)
        rows_to_check = chunk[mask]
    """
    pass

def _create_result(
    self,
    passed: bool,
    message: str,
    failed_count: int = 0,
    total_count: int = 0,
    sample_failures: List[Dict] = None
) -> ValidationResult:
    """
    Helper to create ValidationResult.

    Args:
        passed: True if validation passed
        message: Result message
        failed_count: Number of failures found
        total_count: Total rows checked
        sample_failures: List of example failures (max 100)

    Returns:
        ValidationResult object
    """
    pass
```

---

### FileValidationRule

**Path:** `validation_framework.validations.base`

Base class for file-level validations (metadata, not content).

```python
class FileValidationRule(ValidationRule):
    """
    Base for validations that check file properties,
    not data content.

    Examples: EmptyFileCheck, FileSizeCheck, RowCountRangeCheck
    """

    @abstractmethod
    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """
        Validate file-level properties.

        Args:
            context: Contains file metadata:
                - metadata['row_count']: Number of rows
                - metadata['file_size']: Size in bytes
                - metadata['columns']: Column names
                - file_path: Path to file

        Returns:
            ValidationResult object
        """
        pass
```

**Example Implementation:**

```python
class EmptyFileCheck(FileValidationRule):
    """Check if file is empty"""

    def get_description(self) -> str:
        return "Checks if file contains any data"

    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        row_count = context.get('metadata', {}).get('row_count', 0)

        if row_count == 0:
            return self._create_result(
                passed=False,
                message="File is empty (0 rows)",
                failed_count=1
            )

        return self._create_result(
            passed=True,
            message=f"File contains {row_count} rows"
        )
```

---

### DataValidationRule

**Path:** `validation_framework.validations.base`

Base class for data content validations.

```python
class DataValidationRule(ValidationRule):
    """
    Base for validations that check data content.

    Examples: RegexCheck, RangeCheck, MandatoryFieldCheck
    """

    @abstractmethod
    def validate(
        self,
        data_iterator: Iterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate data content.

        Must process data in chunks for memory efficiency.

        Args:
            data_iterator: Yields DataFrame chunks
            context: Validation context

        Returns:
            ValidationResult object
        """
        pass
```

**Example Implementation:**

```python
class MandatoryFieldCheck(DataValidationRule):
    """Check required fields are not null"""

    def get_description(self) -> str:
        fields = self.params.get('fields', [])
        return f"Checks mandatory fields: {', '.join(fields)}"

    def validate(self, data_iterator, context) -> ValidationResult:
        fields = self.params.get('fields', [])
        max_samples = context.get('max_sample_failures', 100)

        failures = []
        total_rows = 0

        for chunk in data_iterator:
            # Apply condition if specified
            if self.condition:
                mask = self._evaluate_condition(chunk)
                chunk = chunk[mask]

            # Check each field
            for field in fields:
                if field not in chunk.columns:
                    return self._create_result(
                        passed=False,
                        message=f"Field '{field}' not found",
                        failed_count=1
                    )

                # Find null values
                null_mask = chunk[field].isnull()
                null_rows = chunk[null_mask]

                for idx, row in null_rows.iterrows():
                    if len(failures) < max_samples:
                        failures.append({
                            'row': int(total_rows + idx),
                            'field': field,
                            'message': f"'{field}' is null"
                        })

            total_rows += len(chunk)

        if failures:
            return self._create_result(
                passed=False,
                message=f"Found {len(failures)} rows with missing mandatory fields",
                failed_count=len(failures),
                total_count=total_rows,
                sample_failures=failures
            )

        return self._create_result(
            passed=True,
            message=f"All mandatory fields present in {total_rows} rows",
            total_count=total_rows
        )
```

---

## Loader Classes

### DataLoader (Abstract Base)

**Path:** `validation_framework.loaders.base`

Base class for all data loaders.

```python
class DataLoader(ABC):
    """
    Abstract base for data loaders.

    Loaders convert file formats to pandas DataFrames
    that DataK9 can validate.
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        **kwargs: Any
    ):
        """
        Initialize data loader.

        Args:
            file_path: Path to data file
            chunk_size: Rows per chunk (for memory efficiency)
            **kwargs: Format-specific parameters

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        self.file_path = Path(file_path)
        self.chunk_size = chunk_size
        self.kwargs = kwargs

    @abstractmethod
    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load data in chunks.

        Must yield DataFrames, not return a single DataFrame.
        Even small files should yield chunks.

        Yields:
            DataFrame chunks

        Example:
            for chunk in loader.load():
                # Process chunk
                print(f"Processing {len(chunk)} rows")
        """
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get file metadata without loading all data.

        Should be fast - scan structure, not content.

        Returns:
            Dictionary with:
                - row_count: Total rows (or None if expensive)
                - columns: List of column names
                - column_count: Number of columns
                - file_format: Format identifier (csv, json, etc.)

        Example:
            metadata = loader.get_metadata()
            print(f"File has {metadata['row_count']} rows")
            print(f"Columns: {metadata['columns']}")
        """
        pass
```

**Utility Methods:**

```python
def get_file_size(self) -> int:
    """
    Get file size in bytes.

    Returns:
        File size in bytes
    """
    return self.file_path.stat().st_size

def is_empty(self) -> bool:
    """
    Check if file is empty (0 bytes).

    Returns:
        True if file is empty
    """
    return self.get_file_size() == 0
```

---

### CSVLoader

**Path:** `validation_framework.loaders.csv_loader`

Loads CSV files with customizable delimiters and encoding.

```python
class CSVLoader(DataLoader):
    """
    CSV file loader with pandas.

    Supports:
    - Custom delimiters (comma, pipe, tab, etc.)
    - Various encodings (utf-8, latin-1, etc.)
    - Quoted fields
    - Header detection
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        delimiter: str = ',',
        encoding: str = 'utf-8',
        **kwargs
    ):
        """
        Initialize CSV loader.

        Args:
            file_path: Path to CSV file
            chunk_size: Rows per chunk
            delimiter: Field delimiter (default: ',')
            encoding: File encoding (default: 'utf-8')
            **kwargs: Additional pandas.read_csv parameters
        """
        pass
```

**YAML Configuration:**

```yaml
files:
  - name: "data"
    path: "data.csv"
    format: "csv"
    delimiter: "|"
    encoding: "latin-1"
    chunk_size: 10000
```

---

### ParquetLoader

**Path:** `validation_framework.loaders.parquet_loader`

Loads Parquet files (10x faster than CSV for large files).

```python
class ParquetLoader(DataLoader):
    """
    Parquet file loader.

    Parquet is a columnar format optimized for:
    - Large datasets
    - Fast reads (10x faster than CSV)
    - Efficient compression
    - Column pruning (only read needed columns)

    Recommended for files > 1GB.
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        columns: List[str] = None,
        **kwargs
    ):
        """
        Initialize Parquet loader.

        Args:
            file_path: Path to Parquet file
            chunk_size: Rows per chunk
            columns: Only load these columns (column pruning)
            **kwargs: Additional pyarrow parameters
        """
        pass
```

---

## Reporter Classes

### Reporter (Abstract Base)

**Path:** `validation_framework.reporters.base`

Base class for all reporters.

```python
class Reporter(ABC):
    """
    Abstract base for reporters.

    Reporters transform ValidationReport objects into
    various output formats (HTML, JSON, PDF, Slack, etc.).
    """

    @abstractmethod
    def generate(
        self,
        report: ValidationReport,
        output_path: str
    ):
        """
        Generate report from validation results.

        Args:
            report: ValidationReport object with all results
            output_path: Where to save report (or None for notifications)

        The report contains:
        - report.job_name: Job name
        - report.overall_status: PASSED/FAILED/WARNING
        - report.total_errors: Count of ERROR failures
        - report.total_warnings: Count of WARNING failures
        - report.file_reports: List of FileValidationReport objects
        - report.execution_time: When validation ran
        - report.duration_seconds: How long it took

        Example:
            def generate(self, report, output_path):
                with open(output_path, 'w') as f:
                    f.write(f"Job: {report.job_name}\n")
                    f.write(f"Status: {report.overall_status.value}\n")
                    f.write(f"Errors: {report.total_errors}\n")
        """
        pass
```

---

### HTMLReporter

**Path:** `validation_framework.reporters.html_reporter`

Generates interactive HTML reports with dark theme.

```python
class HTMLReporter(Reporter):
    """
    Generate beautiful HTML validation reports.

    Features:
    - Professional dark theme
    - Interactive charts
    - Expandable failure details
    - Mobile-responsive
    - Sample failures with syntax highlighting
    """

    def generate(self, report: ValidationReport, output_path: str):
        """
        Generate HTML report.

        Args:
            report: ValidationReport object
            output_path: Path for output HTML file
        """
        pass
```

**Usage:**

```python
from validation_framework.reporters.html_reporter import HTMLReporter

engine = ValidationEngine.from_config("config.yaml")
report = engine.run()

html_reporter = HTMLReporter()
html_reporter.generate(report, "validation_report.html")
```

---

### JSONReporter

**Path:** `validation_framework.reporters.json_reporter`

Generates machine-readable JSON summaries for CI/CD.

```python
class JSONReporter(Reporter):
    """
    Generate JSON validation summaries.

    Perfect for:
    - CI/CD pipelines
    - Monitoring systems
    - Automated processing
    - API responses
    """

    def generate(self, report: ValidationReport, output_path: str):
        """
        Generate JSON report.

        Args:
            report: ValidationReport object
            output_path: Path for output JSON file
        """
        pass
```

**JSON Structure:**

```json
{
  "job_name": "Customer Data Validation",
  "status": "FAILED",
  "execution_time": "2024-01-15T14:30:22",
  "duration_seconds": 12.5,
  "errors": 3,
  "warnings": 7,
  "files": [
    {
      "file_name": "customers.csv",
      "status": "FAILED",
      "errors": 2,
      "warnings": 5,
      "validations": [
        {
          "rule_name": "Email Format Check",
          "severity": "ERROR",
          "passed": false,
          "message": "Found 15 invalid emails",
          "failed_count": 15,
          "total_count": 10000
        }
      ]
    }
  ]
}
```

---

## Results Classes

### ValidationResult

**Path:** `validation_framework.core.results`

Result of a single validation rule execution.

```python
@dataclass
class ValidationResult:
    """
    Result of executing one validation rule.
    """

    rule_name: str                              # Name of validation rule
    severity: Severity                          # ERROR or WARNING
    passed: bool                                # True if validation passed
    message: str                                # Result message
    failed_count: int = 0                       # Number of failures
    total_count: int = 0                        # Total rows checked
    details: List[Dict[str, Any]] = field(default_factory=list)
    sample_failures: List[Dict[str, Any]] = field(default_factory=list)
    execution_time: float = 0.0                 # Seconds to execute

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        pass

    def _calculate_success_rate(self) -> float:
        """Calculate percentage of rows that passed"""
        pass
```

**Example:**

```python
# Creating a validation result
result = ValidationResult(
    rule_name="Email Format Check",
    severity=Severity.ERROR,
    passed=False,
    message="Found 15 invalid email addresses",
    failed_count=15,
    total_count=10000,
    sample_failures=[
        {"row": 42, "field": "email", "value": "invalid-email"},
        {"row": 99, "field": "email", "value": "bad@format"},
    ],
    execution_time=2.5
)

print(f"Success rate: {result._calculate_success_rate()}%")  # 99.85%
```

---

### FileValidationReport

**Path:** `validation_framework.core.results`

Validation report for a single file.

```python
@dataclass
class FileValidationReport:
    """
    Aggregated results for one file.
    """

    file_name: str                                          # Name of file
    file_path: str                                          # Full path
    file_format: str                                        # csv, json, parquet, etc.
    status: Status                                          # PASSED, FAILED, WARNING
    validation_results: List[ValidationResult] = field(default_factory=list)
    execution_time: float = 0.0                             # Seconds to validate file
    error_count: int = 0                                    # Count of ERROR failures
    warning_count: int = 0                                  # Count of WARNING failures
    total_validations: int = 0                              # Total validations run
    metadata: Dict[str, Any] = field(default_factory=dict)  # File metadata

    def add_result(self, result: ValidationResult) -> None:
        """Add validation result and update counts"""
        pass

    def update_status(self) -> None:
        """Update overall status based on results"""
        pass

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON"""
        pass
```

**Properties:**

```python
# Access file report data
file_report.file_name                    # "customers.csv"
file_report.status                       # Status.FAILED
file_report.error_count                  # 2
file_report.warning_count                # 5
file_report.metadata['row_count']        # 10000
file_report.metadata['columns']          # ['id', 'name', 'email']

# Iterate validations
for validation in file_report.validation_results:
    print(f"{validation.rule_name}: {validation.passed}")
```

---

### ValidationReport

**Path:** `validation_framework.core.results`

Overall validation job report.

```python
@dataclass
class ValidationReport:
    """
    Complete validation job report.

    Aggregates results from all files in the job.
    """

    job_name: str                                   # Name of validation job
    execution_time: datetime                        # When validation ran
    duration_seconds: float                         # Total execution time
    overall_status: Status                          # PASSED, FAILED, WARNING
    config: Dict[str, Any]                          # Original configuration
    file_reports: List[FileValidationReport]        # Results for each file
    description: str = ""                           # Job description

    @property
    def total_errors(self) -> int:
        """Total ERROR severity failures across all files"""
        return sum(fr.error_count for fr in self.file_reports)

    @property
    def total_warnings(self) -> int:
        """Total WARNING severity failures across all files"""
        return sum(fr.warning_count for fr in self.file_reports)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON"""
        pass
```

**Example:**

```python
# Access report data
report.job_name                          # "Customer Data Validation"
report.overall_status                    # Status.FAILED
report.total_errors                      # 5
report.total_warnings                    # 12
report.duration_seconds                  # 15.7

# Iterate file reports
for file_report in report.file_reports:
    print(f"{file_report.file_name}: {file_report.status.value}")

# Check if validation passed
if report.overall_status == Status.PASSED:
    print("✅ All validations passed")
elif report.overall_status == Status.FAILED:
    print(f"❌ Validation failed with {report.total_errors} errors")
```

---

### Severity

**Path:** `validation_framework.core.results`

Severity levels for validations.

```python
class Severity(Enum):
    """
    Validation severity levels.

    - ERROR: Critical issues that prevent data processing
    - WARNING: Quality issues that don't prevent processing
    """

    ERROR = "ERROR"      # Validation failure causes overall failure
    WARNING = "WARNING"  # Validation failure is noted but doesn't fail job
```

**Usage:**

```python
from validation_framework.core.results import Severity

# Creating validation with ERROR severity
validation = MyValidation(
    name="Email Check",
    severity=Severity.ERROR,
    params={"field": "email"}
)

# Check severity
if validation.severity == Severity.ERROR:
    print("This is a critical validation")
```

---

### Status

**Path:** `validation_framework.core.results`

Overall validation status values.

```python
class Status(Enum):
    """
    Overall validation status.

    - PASSED: All validations passed
    - FAILED: At least one ERROR severity validation failed
    - WARNING: Only WARNING severity validations failed
    """

    PASSED = "PASSED"    # All validations passed
    FAILED = "FAILED"    # At least one ERROR failed
    WARNING = "WARNING"  # Only WARNING failures
```

---

## Registry Functions

### get_registry()

**Path:** `validation_framework.core.registry`

Get the global validation registry.

```python
def get_registry() -> ValidationRegistry:
    """
    Get global validation registry (Singleton).

    Returns:
        ValidationRegistry instance

    Example:
        from validation_framework.core.registry import get_registry

        registry = get_registry()
        all_validations = registry.list_validations()
        print(f"Available validations: {all_validations}")
    """
    pass
```

---

### register_validation()

**Path:** `validation_framework.core.registry`

Register a custom validation type.

```python
def register_validation(
    name: str,
    validation_class: Type[ValidationRule]
):
    """
    Register custom validation with DataK9.

    Args:
        name: Name to use in YAML configs
        validation_class: Validation class (must inherit from ValidationRule)

    Example:
        from validation_framework.core.registry import register_validation
        from validation_framework.validations.base import DataValidationRule

        class MyCustomCheck(DataValidationRule):
            def get_description(self):
                return "My custom validation"

            def validate(self, data_iterator, context):
                # validation logic
                return self._create_result(passed=True, message="OK")

        # Register so it can be used in YAML
        register_validation("MyCustomCheck", MyCustomCheck)
    """
    pass
```

---

### ValidationRegistry

**Path:** `validation_framework.core.registry`

Registry that manages all validation types.

```python
class ValidationRegistry:
    """
    Singleton registry of validation types.

    Automatically registers all built-in validations on import.
    Custom validations can be registered at runtime.
    """

    def register(
        self,
        name: str,
        validation_class: Type[ValidationRule]
    ):
        """Register a validation type"""
        pass

    def get(self, name: str) -> Type[ValidationRule]:
        """
        Get validation class by name.

        Args:
            name: Validation type name

        Returns:
            Validation class

        Raises:
            KeyError: If validation type not found
        """
        pass

    def list_validations(self) -> List[str]:
        """
        Get list of all registered validation names.

        Returns:
            List of validation type names
        """
        pass

    def is_registered(self, name: str) -> bool:
        """Check if validation type is registered"""
        pass
```

**Example:**

```python
from validation_framework.core.registry import get_registry

registry = get_registry()

# List all available validations
all_validations = registry.list_validations()
print(f"Available: {', '.join(all_validations)}")

# Check if type exists
if registry.is_registered("EmailCheck"):
    validation_class = registry.get("EmailCheck")
```

---

## Configuration Classes

### ValidationConfigError

**Path:** `validation_framework.core.config`

Exception raised for invalid configuration.

```python
class ValidationConfigError(Exception):
    """
    Raised when configuration is invalid.

    Examples:
    - Missing required fields
    - Invalid validation types
    - File paths don't exist
    - Invalid parameter values
    """
    pass
```

**Example:**

```python
from validation_framework.core.config import ValidationConfig, ValidationConfigError

try:
    config = ValidationConfig("config.yaml")
    config.validate()
except ValidationConfigError as e:
    print(f"Configuration error: {str(e)}")
    # Fix configuration and try again
```

---

## Utility Functions

### setup_logging()

**Path:** `validation_framework.utils.logging`

Configure logging for DataK9.

```python
def setup_logging(
    level: str = "INFO",
    log_file: str = None,
    format: str = None
):
    """
    Configure DataK9 logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional file to write logs to
        format: Optional custom log format

    Example:
        from validation_framework.utils.logging import setup_logging

        setup_logging(level="DEBUG", log_file="validation.log")
    """
    pass
```

---

## Type Hints Reference

DataK9 uses type hints throughout. Here are common types:

```python
from typing import Iterator, Dict, Any, List, Optional, Type
import pandas as pd
from validation_framework.core.results import ValidationResult, Severity, Status

# Common type signatures
def validate(
    data_iterator: Iterator[pd.DataFrame],     # Iterator of DataFrames
    context: Dict[str, Any]                    # Context dictionary
) -> ValidationResult:                          # Returns ValidationResult
    pass

# Optional parameters
def __init__(
    self,
    name: str,                                  # Required string
    severity: Severity,                         # Required Severity enum
    params: Optional[Dict[str, Any]] = None,    # Optional dictionary
    condition: Optional[str] = None             # Optional string
):
    pass

# Collections
columns: List[str] = ["id", "name", "email"]
metadata: Dict[str, Any] = {"row_count": 1000}
sample_failures: List[Dict[str, Any]] = [
    {"row": 42, "field": "email", "value": "bad"}
]
```

---

## Next Steps

- **[Custom Validations](custom-validations.md)** - Create your own validation rules
- **[Custom Loaders](custom-loaders.md)** - Support new file formats
- **[Custom Reporters](custom-reporters.md)** - Generate custom reports
- **[Architecture](architecture.md)** - Understand DataK9's internals

---

**🐕 DataK9 API - Build powerful data quality solutions with confidence**
