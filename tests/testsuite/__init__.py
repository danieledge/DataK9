"""
DataK9 Test Suite - Centralized test data and fixtures.

This module provides:
- Test data files with testsuite_ prefix for easy identification
- Data generators for creating test datasets
- Shared fixtures and utilities

Directory Structure:
    testsuite/
    ├── data/
    │   ├── profiler/          # Profiler test data (PII, correlation, temporal)
    │   ├── cross_file/        # Cross-file validation test data
    │   ├── samples/           # Sample datasets (titanic, transactions)
    │   ├── json/              # JSON test fixtures
    │   └── regression/        # Regression test data and configs
    ├── generators/            # Data generation scripts
    └── README.md              # Documentation

Usage:
    from tests.testsuite import TESTSUITE_DATA_DIR
    pii_data = TESTSUITE_DATA_DIR / "profiler" / "testsuite_pii_samples.csv"
"""

from pathlib import Path

# Base directory for all test suite data
TESTSUITE_DATA_DIR = Path(__file__).parent / "data"

# Subdirectory paths for convenience
PROFILER_DATA_DIR = TESTSUITE_DATA_DIR / "profiler"
CROSS_FILE_DATA_DIR = TESTSUITE_DATA_DIR / "cross_file"
SAMPLES_DATA_DIR = TESTSUITE_DATA_DIR / "samples"
JSON_DATA_DIR = TESTSUITE_DATA_DIR / "json"
REGRESSION_DATA_DIR = TESTSUITE_DATA_DIR / "regression"

# Common test data files
TESTSUITE_PII_SAMPLES = PROFILER_DATA_DIR / "testsuite_pii_samples.csv"
TESTSUITE_CORRELATION_PATTERNS = PROFILER_DATA_DIR / "testsuite_correlation_patterns.csv"
TESTSUITE_TEMPORAL_PATTERNS = PROFILER_DATA_DIR / "testsuite_temporal_patterns.csv"
TESTSUITE_CUSTOMERS = CROSS_FILE_DATA_DIR / "testsuite_customers.csv"
TESTSUITE_ORDERS = CROSS_FILE_DATA_DIR / "testsuite_orders.csv"
TESTSUITE_TITANIC = SAMPLES_DATA_DIR / "testsuite_titanic.csv"
TESTSUITE_TRANSACTIONS = SAMPLES_DATA_DIR / "testsuite_transactions.csv"
TESTSUITE_REGRESSION_DATA = REGRESSION_DATA_DIR / "testsuite_regression_data.csv"

# JSON fixtures
TESTSUITE_CUSTOMERS_JSON = JSON_DATA_DIR / "testsuite_customers.json"
TESTSUITE_NESTED_DATA_JSON = JSON_DATA_DIR / "testsuite_nested_data.json"
TESTSUITE_EMPTY_JSON = JSON_DATA_DIR / "testsuite_empty.json"
