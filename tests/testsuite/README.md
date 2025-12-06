# DataK9 Test Suite

Centralized test data and fixtures for the DataK9 validation framework test suite.

## Directory Structure

```
testsuite/
├── __init__.py              # Module exports and path constants
├── README.md                # This file
├── data/
│   ├── profiler/            # Profiler test data
│   │   ├── testsuite_pii_samples.csv
│   │   ├── testsuite_correlation_patterns.csv
│   │   └── testsuite_temporal_patterns.csv
│   ├── cross_file/          # Cross-file validation test data
│   │   ├── testsuite_customers.csv
│   │   └── testsuite_orders.csv
│   ├── samples/             # Sample datasets
│   │   ├── testsuite_titanic.csv
│   │   └── testsuite_transactions.csv
│   ├── json/                # JSON test fixtures
│   │   ├── testsuite_customers.json
│   │   ├── testsuite_empty.json
│   │   └── testsuite_nested_data.json
│   └── regression/          # Regression test data
│       └── testsuite_regression_data.csv
└── generators/
    └── generate_test_data.py
```

## Naming Convention

All test data files are prefixed with `testsuite_` to:
- Clearly identify files as test fixtures
- Avoid confusion with production data
- Enable easy cleanup and identification

## Usage

### Importing Path Constants

```python
from tests.testsuite import (
    TESTSUITE_DATA_DIR,
    TESTSUITE_PII_SAMPLES,
    TESTSUITE_CORRELATION_PATTERNS,
    TESTSUITE_TEMPORAL_PATTERNS,
    TESTSUITE_CUSTOMERS,
    TESTSUITE_ORDERS,
)

# Use in tests
import pandas as pd
pii_df = pd.read_csv(TESTSUITE_PII_SAMPLES)
```

### Generating Test Data

To regenerate all test data:

```bash
python tests/testsuite/generators/generate_test_data.py
```

## Test Data Descriptions

### Profiler Data

| File | Description | Rows |
|------|-------------|------|
| `testsuite_pii_samples.csv` | Sample PII data (emails, SSNs, credit cards) | 300 |
| `testsuite_correlation_patterns.csv` | Correlation patterns (linear, monotonic, etc.) | 500 |
| `testsuite_temporal_patterns.csv` | Temporal data with trends and seasonality | 500 |

### Cross-File Validation Data

| File | Description |
|------|-------------|
| `testsuite_customers.csv` | Customer master data (5 records) |
| `testsuite_orders.csv` | Orders with FK to customers (7 records, 2 invalid FKs) |

### Sample Datasets

| File | Description |
|------|-------------|
| `testsuite_titanic.csv` | Titanic passenger data sample (10 records) |
| `testsuite_transactions.csv` | Transaction data sample (10 records) |

### JSON Fixtures

| File | Description |
|------|-------------|
| `testsuite_customers.json` | Customer data in JSON format |
| `testsuite_empty.json` | Empty array for edge case testing |
| `testsuite_nested_data.json` | Nested JSON structure for testing |

### Regression Data

| File | Description |
|------|-------------|
| `testsuite_regression_data.csv` | Employee data with designed quality issues (25 records) |

Quality issues in regression data:
- Missing values in email and phone columns
- Duplicate email addresses
- Salary outliers ($250,000 and $500,000)
