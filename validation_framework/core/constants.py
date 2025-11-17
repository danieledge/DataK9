"""
DataK9 Framework Constants.

This module defines all magic numbers, configuration defaults, and constants
used throughout the DataK9 framework. Centralizing these values improves
maintainability and provides clear documentation of their purpose.

Author: Daniel Edge
"""

# ============================================================================
# File Processing Constants
# ============================================================================

# Default chunk size for iterative data loading (rows per chunk)
# Rationale: 50K rows × 50 columns × 8 bytes ≈ 20MB per chunk
# This balances memory efficiency with I/O overhead
DEFAULT_CHUNK_SIZE: int = 50_000

# Minimum allowed chunk size (prevents excessive I/O)
MIN_CHUNK_SIZE: int = 1_000

# Maximum allowed chunk size (prevents memory issues)
MAX_CHUNK_SIZE: int = 1_000_000


# ============================================================================
# Configuration Security Limits
# ============================================================================

# Maximum YAML configuration file size (10MB)
# Security measure: Prevents DoS attacks via huge YAML files that could
# consume excessive memory during parsing
MAX_YAML_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB in bytes

# Maximum YAML nesting depth
# Security measure: Prevents stack overflow from deeply nested YAML structures
# Example attack: {a: {b: {c: {d: ... 10,000 levels deep}}}}
MAX_YAML_NESTING_DEPTH: int = 20

# Maximum number of keys in YAML mapping
# Security measure: Prevents memory exhaustion from YAML with millions of keys
# Example attack: {key1: val, key2: val, ... key1000000: val}
MAX_YAML_KEY_COUNT: int = 10_000


# ============================================================================
# Validation Result Limits
# ============================================================================

# Maximum number of sample failures to collect per validation
# Rationale: Collecting all failures for large datasets would consume
# excessive memory. 100 samples provides enough context for debugging
# while keeping memory usage bounded.
MAX_SAMPLE_FAILURES: int = 100

# Maximum failures to collect per chunk (prevents memory spikes)
MAX_FAILURES_PER_CHUNK: int = 1_000


# ============================================================================
# String/Text Processing Constants
# ============================================================================

# Maximum length for SQL identifiers (PostgreSQL standard)
# Rationale: PostgreSQL truncates identifiers at 63 characters
MAX_SQL_IDENTIFIER_LENGTH: int = 63

# Maximum reasonable string length for validation (10MB)
# Prevents issues with extremely long strings in validation checks
MAX_STRING_LENGTH: int = 10 * 1024 * 1024


# ============================================================================
# Performance Tuning Constants
# ============================================================================

# File size threshold for automatic Polars backend selection (100MB)
# Rationale: Polars shows 5-10x performance improvement for files >100MB
# but has slightly higher overhead for small files
POLARS_THRESHOLD_BYTES: int = 100 * 1024 * 1024

# Number of rows to use for data type inference
# Rationale: Sampling too few rows may miss type variations,
# but sampling millions is unnecessary for type detection
TYPE_INFERENCE_SAMPLE_SIZE: int = 10_000

# Maximum number of unique values to track for categorical analysis
# Rationale: Beyond this threshold, treat column as high-cardinality
# and use different profiling strategies
MAX_UNIQUE_VALUES_FOR_CATEGORICAL: int = 100


# ============================================================================
# Profiler Constants
# ============================================================================

# Number of bins for histogram generation
HISTOGRAM_BINS: int = 50

# Percentiles to calculate for numeric columns
PERCENTILES: list = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99]

# Z-score threshold for outlier detection
# Rationale: 3 standard deviations captures 99.7% of normal distribution
OUTLIER_Z_SCORE_THRESHOLD: float = 3.0

# IQR multiplier for outlier detection (Tukey's fence)
# Rationale: 1.5 × IQR is standard statistical practice
# Outliers are values < Q1 - 1.5×IQR or > Q3 + 1.5×IQR
OUTLIER_IQR_MULTIPLIER: float = 1.5

# Minimum sample size for statistical tests
# Rationale: Statistical tests become unreliable with very small samples
MIN_SAMPLE_SIZE_FOR_STATS: int = 30

# Maximum number of pattern examples to collect
MAX_PATTERN_EXAMPLES: int = 10


# ============================================================================
# Logging Constants
# ============================================================================

# Default log message format
LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Log date format
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"


# ============================================================================
# Report Generation Constants
# ============================================================================

# Maximum number of rows to show in HTML report tables
MAX_REPORT_TABLE_ROWS: int = 100

# Maximum length for truncated values in reports
MAX_REPORT_VALUE_LENGTH: int = 100


# ============================================================================
# Database Constants
# ============================================================================

# Default database query timeout (seconds)
DEFAULT_DB_TIMEOUT: int = 300  # 5 minutes

# Maximum number of rows to fetch in single query
MAX_DB_FETCH_SIZE: int = 100_000


# ============================================================================
# Temporal Analysis Constants
# ============================================================================

# Minimum number of data points for trend analysis
MIN_POINTS_FOR_TREND: int = 10

# Number of lags to check for autocorrelation
AUTOCORRELATION_LAGS: int = 40


# ============================================================================
# Validation Severity Levels
# ============================================================================

# Valid severity levels for validations
VALID_SEVERITIES: list = ["ERROR", "WARNING", "INFO"]

# Default severity if not specified
DEFAULT_SEVERITY: str = "ERROR"


# ============================================================================
# File Format Constants
# ============================================================================

# Supported file formats
SUPPORTED_FILE_FORMATS: list = ["csv", "excel", "json", "parquet", "database"]

# Default file format if not specified
DEFAULT_FILE_FORMAT: str = "csv"

# File extension to format mapping
FILE_EXTENSION_MAP: dict = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
    ".json": "json",
    ".parquet": "parquet",
    ".pq": "parquet"
}


# ============================================================================
# Progress Reporting Constants
# ============================================================================

# Update progress every N chunks
PROGRESS_UPDATE_FREQUENCY: int = 10

# Minimum time between progress updates (seconds)
MIN_PROGRESS_UPDATE_INTERVAL: float = 1.0


# ============================================================================
# Memory Management Constants
# ============================================================================

# Target memory usage per operation (MB)
# Rationale: Aim to use no more than 10% of system RAM per operation
TARGET_MEMORY_MB: int = 500

# Memory safety margin (percentage)
# Rationale: Leave 20% headroom to prevent OOM crashes
MEMORY_SAFETY_MARGIN: float = 0.20


# ============================================================================
# Data Quality Thresholds (Defaults)
# ============================================================================

# Default minimum completeness threshold (95%)
# Rationale: Most data quality frameworks consider 95%+ as "good quality"
DEFAULT_MIN_COMPLETENESS: float = 95.0

# Default maximum null percentage (5%)
DEFAULT_MAX_NULL_PERCENTAGE: float = 5.0

# Default minimum uniqueness for ID columns (99%)
DEFAULT_MIN_UNIQUENESS: float = 99.0


# ============================================================================
# Regex Patterns (Common)
# ============================================================================

# Common regex patterns used across validations
EMAIL_PATTERN: str = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_US_PATTERN: str = r'^\+1[0-9]{10}$'
ZIP_US_PATTERN: str = r'^\d{5}(-\d{4})?$'
SSN_PATTERN: str = r'^\d{3}-\d{2}-\d{4}$'
CREDIT_CARD_PATTERN: str = r'^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$'
IP_ADDRESS_PATTERN: str = r'^(\d{1,3}\.){3}\d{1,3}$'
URL_PATTERN: str = r'^https?://[^\s/$.?#].[^\s]*$'
