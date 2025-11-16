"""
Shared test fixtures and configuration for all test modules.

This module provides reusable fixtures, test data generators, and helper
functions used across the test suite. All fixtures are properly documented
and organized by category.
"""

import pytest
import pandas as pd
import tempfile
import yaml
import sqlite3
from pathlib import Path
from typing import Iterator, Dict, Any


# ============================================================================
# PYTEST CONFIGURATION
# ============================================================================

def pytest_configure(config):
    """
    Configure pytest with custom markers and settings.

    This function is called once before any tests run, allowing us to
    register custom markers and configure the test environment.
    """
    # Register custom markers
    config.addinivalue_line("markers", "unit: Unit tests for individual components")
    config.addinivalue_line("markers", "integration: Integration tests for full workflows")
    config.addinivalue_line("markers", "slow: Tests that take significant time to run")
    config.addinivalue_line("markers", "security: Security-related tests (SQL injection, DoS, etc.)")
    config.addinivalue_line("markers", "cli: Command-line interface tests")
    config.addinivalue_line("markers", "performance: Performance and load tests")
    config.addinivalue_line("markers", "requires_data: Tests that require sample data files")


# ============================================================================
# DATA FIXTURES - Basic DataFrames
# ============================================================================

@pytest.fixture
def sample_dataframe():
    """
    Create a standard sample DataFrame for general testing.

    Contains typical customer data with:
    - Numeric columns (id, age, balance)
    - String columns (name, email, status)
    - Missing values in some rows

    Returns:
        pd.DataFrame: 5-row DataFrame with mixed data types
    """
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", None, "Eve"],
        "email": ["alice@test.com", "bob@test.com", "invalid", "david@test.com", None],
        "age": [25, 30, 35, 40, 45],
        "balance": [100.50, 200.75, 300.00, 400.25, 500.50],
        "status": ["active", "active", "inactive", "active", "active"]
    })


@pytest.fixture
def clean_dataframe():
    """
    Create a clean DataFrame with no missing values or invalid data.

    Useful for testing validations that should pass completely.

    Returns:
        pd.DataFrame: 5-row DataFrame with all valid data
    """
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "email": ["alice@test.com", "bob@test.com", "charlie@test.com",
                  "david@test.com", "eve@test.com"],
        "age": [25, 30, 35, 40, 45],
        "balance": [100.50, 200.75, 300.00, 400.25, 500.50]
    })


@pytest.fixture
def large_dataframe():
    """
    Create a larger DataFrame for testing chunking and performance.

    Contains 10,000 rows to test memory-efficient processing.

    Returns:
        pd.DataFrame: 10,000-row DataFrame
    """
    return pd.DataFrame({
        "id": range(10000),
        "value": range(10000, 20000),
        "category": [f"cat_{i % 10}" for i in range(10000)],
        "amount": [i * 1.5 for i in range(10000)]
    })


@pytest.fixture
def dataframe_with_duplicates():
    """
    Create a DataFrame with duplicate records for testing duplicate detection.

    Returns:
        pd.DataFrame: DataFrame with intentional duplicates
    """
    return pd.DataFrame({
        "id": [1, 2, 2, 3, 3, 3],
        "name": ["A", "B", "B", "C", "C", "C"],
        "value": [100, 200, 200, 300, 300, 300]
    })


@pytest.fixture
def dataframe_with_outliers():
    """
    Create a DataFrame with statistical outliers for testing outlier detection.

    Returns:
        pd.DataFrame: DataFrame with normal values and outliers
    """
    import numpy as np
    # Normal distribution with mean=100, std=15
    normal_values = list(np.random.normal(100, 15, 100))
    # Add outliers
    outliers = [200, 250, -50, 300]

    return pd.DataFrame({
        "id": range(len(normal_values) + len(outliers)),
        "value": normal_values + outliers
    })


# ============================================================================
# FILE FIXTURES - Temporary Files
# ============================================================================

@pytest.fixture
def temp_csv_file(sample_dataframe):
    """
    Create a temporary CSV file from sample data.

    The file is automatically cleaned up after the test completes.

    Args:
        sample_dataframe: Fixture providing sample data

    Yields:
        str: Path to temporary CSV file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_dataframe.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_large_csv_file(large_dataframe):
    """
    Create a temporary large CSV file for performance testing.

    Args:
        large_dataframe: Fixture providing large dataset

    Yields:
        str: Path to temporary CSV file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        large_dataframe.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_empty_file():
    """
    Create an empty file (0 bytes) for testing empty file detection.

    Yields:
        str: Path to empty file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        # Don't write anything - creates 0-byte file
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_json_file(sample_dataframe):
    """
    Create a temporary JSON file from sample data.

    Args:
        sample_dataframe: Fixture providing sample data

    Yields:
        str: Path to temporary JSON file
    """
    import json

    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
        data = sample_dataframe.to_dict('records')
        json.dump(data, f, indent=2)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_excel_file(sample_dataframe):
    """
    Create a temporary Excel file from sample data.

    Args:
        sample_dataframe: Fixture providing sample data

    Yields:
        str: Path to temporary Excel file
    """
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
        temp_path = f.name
        sample_dataframe.to_excel(temp_path, index=False, engine='openpyxl')

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def temp_parquet_file(sample_dataframe):
    """
    Create a temporary Parquet file from sample data.

    Args:
        sample_dataframe: Fixture providing sample data

    Yields:
        str: Path to temporary Parquet file
    """
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        temp_path = f.name
        sample_dataframe.to_parquet(temp_path, index=False)

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


# ============================================================================
# CONFIGURATION FIXTURES
# ============================================================================

@pytest.fixture
def valid_config_dict(temp_csv_file):
    """
    Create a valid configuration dictionary for testing.

    This represents a complete, valid YAML configuration with all standard
    options populated.

    Args:
        temp_csv_file: Path to test data file

    Returns:
        dict: Valid configuration dictionary
    """
    return {
        "validation_job": {
            "name": "Test Validation Job",
            "version": "1.0",
            "description": "Test configuration for automated tests",
            "files": [
                {
                    "name": "test_file",
                    "path": temp_csv_file,
                    "format": "csv",
                    "validations": [
                        {
                            "type": "EmptyFileCheck",
                            "severity": "ERROR"
                        },
                        {
                            "type": "MandatoryFieldCheck",
                            "severity": "ERROR",
                            "params": {
                                "fields": ["id", "name"]
                            }
                        }
                    ]
                }
            ],
            "output": {
                "html_report": "test_report.html",
                "json_summary": "test_summary.json",
                "fail_on_error": True,
                "fail_on_warning": False
            },
            "processing": {
                "chunk_size": 50000,
                "parallel_files": False,
                "max_sample_failures": 100
            }
        }
    }


@pytest.fixture
def minimal_config_dict(temp_csv_file):
    """
    Create a minimal valid configuration with only required fields.

    Tests that defaults are properly applied for optional fields.

    Args:
        temp_csv_file: Path to test data file

    Returns:
        dict: Minimal valid configuration
    """
    return {
        "validation_job": {
            "name": "Minimal Test",
            "files": [
                {
                    "path": temp_csv_file
                }
            ]
        }
    }


@pytest.fixture
def temp_config_file(valid_config_dict):
    """
    Create a temporary YAML configuration file.

    Args:
        valid_config_dict: Valid configuration dictionary

    Yields:
        str: Path to temporary config file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(valid_config_dict, f)
        temp_path = f.name

    yield temp_path

    Path(temp_path).unlink(missing_ok=True)


# ============================================================================
# DATABASE FIXTURES
# ============================================================================

@pytest.fixture
def temp_sqlite_db():
    """
    Create a temporary SQLite database for testing database validations.

    The database contains sample tables:
    - customers: id, name, email, status
    - orders: id, customer_id, amount, order_date

    Yields:
        tuple: (connection_string, connection_object)
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    # Create connection
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create customers table
    cursor.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            status TEXT DEFAULT 'active'
        )
    """)

    # Insert sample data
    cursor.executemany(
        "INSERT INTO customers (id, name, email, status) VALUES (?, ?, ?, ?)",
        [
            (1, "Alice", "alice@test.com", "active"),
            (2, "Bob", "bob@test.com", "active"),
            (3, "Charlie", "charlie@test.com", "inactive"),
            (4, "David", "david@test.com", "active"),
            (5, "Eve", "eve@test.com", "active")
        ]
    )

    # Create orders table
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount REAL,
            order_date TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """)

    cursor.executemany(
        "INSERT INTO orders (id, customer_id, amount, order_date) VALUES (?, ?, ?, ?)",
        [
            (1, 1, 100.50, "2025-01-01"),
            (2, 1, 250.00, "2025-01-05"),
            (3, 2, 75.25, "2025-01-03"),
            (4, 3, 500.00, "2025-01-07"),
            (5, 5, 125.75, "2025-01-10")
        ]
    )

    conn.commit()

    connection_string = f"sqlite:///{db_path}"

    yield connection_string, conn

    # Cleanup
    conn.close()
    Path(db_path).unlink(missing_ok=True)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_temp_file_with_content(content: str, suffix: str = '.txt') -> str:
    """
    Create a temporary file with specific content.

    Helper function for creating test files with custom content.
    Caller is responsible for cleanup.

    Args:
        content: String content to write to file
        suffix: File extension (default: .txt)

    Returns:
        str: Path to created file
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False) as f:
        f.write(content)
        return f.name


def assert_validation_passed(result):
    """
    Assert that a validation result passed successfully.

    Provides clear error messages if validation failed.

    Args:
        result: ValidationResult object

    Raises:
        AssertionError: If validation did not pass
    """
    assert result.passed is True, (
        f"Validation failed: {result.message}\n"
        f"Failed count: {result.failed_count}\n"
        f"Sample failures: {result.sample_failures[:5]}"
    )


def assert_validation_failed(result, expected_fail_count=None):
    """
    Assert that a validation result failed as expected.

    Args:
        result: ValidationResult object
        expected_fail_count: Optional expected number of failures

    Raises:
        AssertionError: If validation did not fail properly
    """
    assert result.passed is False, (
        f"Validation unexpectedly passed: {result.message}"
    )

    if expected_fail_count is not None:
        assert result.failed_count == expected_fail_count, (
            f"Expected {expected_fail_count} failures, got {result.failed_count}"
        )


def create_data_iterator(dataframe: pd.DataFrame, chunk_size: int = None) -> Iterator[pd.DataFrame]:
    """
    Create an iterator from a DataFrame for testing validations.

    Args:
        dataframe: DataFrame to iterate over
        chunk_size: Optional chunk size for splitting data

    Returns:
        Iterator[pd.DataFrame]: Iterator yielding data chunks
    """
    if chunk_size is None:
        yield dataframe
    else:
        for start in range(0, len(dataframe), chunk_size):
            yield dataframe.iloc[start:start + chunk_size]


# ============================================================================
# TEST DATA DIRECTORIES
# ============================================================================

@pytest.fixture(scope="session")
def test_data_dir():
    """
    Get the path to the test data directory.

    Returns:
        Path: Path to tests/fixtures directory
    """
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def ensure_test_data_dirs(test_data_dir):
    """
    Ensure all required test data directories exist.

    Creates fixture directories if they don't exist:
    - fixtures/csv/
    - fixtures/json/
    - fixtures/excel/
    - fixtures/cross_file/

    Args:
        test_data_dir: Base test data directory
    """
    directories = [
        test_data_dir,
        test_data_dir / "csv",
        test_data_dir / "json",
        test_data_dir / "excel",
        test_data_dir / "cross_file"
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)

    return test_data_dir
