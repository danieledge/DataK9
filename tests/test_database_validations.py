"""
Tests for database-specific validation checks.

Tests SQLCustomCheck, DatabaseReferentialIntegrityCheck, and DatabaseConstraintCheck
which validate data directly in databases.
"""

import pytest
import pandas as pd
import sqlite3
import tempfile
from pathlib import Path
from validation_framework.validations.builtin.database_checks import (
    SQLCustomCheck,
    DatabaseReferentialIntegrityCheck,
    DatabaseConstraintCheck
)
from validation_framework.core.results import Severity
from tests.conftest import create_data_iterator


@pytest.fixture
def temp_test_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create customers table (WITHOUT CHECK constraint to allow test data)
    cursor.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER,
            country TEXT
        )
    """)

    # Insert sample data (including invalid ages for testing)
    cursor.executemany(
        "INSERT INTO customers (id, name, email, age, country) VALUES (?, ?, ?, ?, ?)",
        [
            (1, "Alice", "alice@test.com", 25, "US"),
            (2, "Bob", "bob@test.com", 30, "UK"),
            (3, "Charlie", "charlie@test.com", 17, "CA"),  # Underage
            (4, "David", "david@test.com", 160, "US"),  # Age violation
            (5, "Eve", "eve@test.com", 45, "FR")
        ]
    )

    # Create orders table
    cursor.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount REAL,
            status TEXT,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """)

    cursor.executemany(
        "INSERT INTO orders (id, customer_id, amount, status) VALUES (?, ?, ?, ?)",
        [
            (1, 1, 100.50, "completed"),
            (2, 1, 250.00, "pending"),
            (3, 2, 75.25, "completed"),
            (4, 99, 500.00, "completed"),  # Invalid customer_id
            (5, 5, 125.75, "completed")
        ]
    )

    conn.commit()

    connection_string = f"sqlite:///{db_path}"

    yield connection_string, conn, db_path

    # Cleanup
    conn.close()
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.unit
class TestSQLCustomCheck:
    """Tests for SQLCustomCheck - execute custom SQL validations."""

    def test_sql_custom_find_violations(self, temp_test_db):
        """Test SQL custom check that finds constraint violations."""
        conn_string, conn, _ = temp_test_db

        # SQL query that finds age violations (age < 18 OR age > 150)
        validation = SQLCustomCheck(
            name="AgeConstraintCheck",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "sql_query": """
                    SELECT id, name, age
                    FROM customers
                    WHERE age < 18 OR age > 150
                """,
                "description": "Find customers with invalid ages"
            }
        )

        # Create empty data iterator (SQL check doesn't use input data)
        data_iterator = create_data_iterator(pd.DataFrame())
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should find 2 violations (age=17 and age=160)
        assert result.passed is False
        assert result.failed_count == 2
        assert len(result.sample_failures) == 2

    def test_sql_custom_no_violations(self, temp_test_db):
        """Test SQL custom check that finds no violations."""
        conn_string, conn, _ = temp_test_db

        # SQL query that looks for negative amounts (none exist)
        validation = SQLCustomCheck(
            name="NegativeAmounts",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "sql_query": """
                    SELECT id, amount
                    FROM orders
                    WHERE amount < 0
                """,
                "description": "Find negative order amounts"
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        # Should pass - no negative amounts
        assert result.passed is True
        assert result.failed_count == 0

    def test_sql_custom_missing_connection_string(self):
        """Test SQL custom check without connection string."""
        validation = SQLCustomCheck(
            name="NoConnection",
            severity=Severity.ERROR,
            params={
                "sql_query": "SELECT * FROM test"
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        assert result.passed is False
        assert "Parameter" in result.message and "is required" in result.message

    def test_sql_custom_invalid_query(self, temp_test_db):
        """Test SQL custom check with invalid SQL query."""
        conn_string, conn, _ = temp_test_db

        validation = SQLCustomCheck(
            name="InvalidSQL",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "sql_query": "SELECT * FROM nonexistent_table",
                "description": "Invalid query"
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        assert result.passed is False
        assert "Error" in result.message


@pytest.mark.unit
class TestDatabaseReferentialIntegrityCheck:
    """Tests for DatabaseReferentialIntegrityCheck - validate foreign keys."""

    def test_referential_integrity_violations(self, temp_test_db):
        """Test detection of referential integrity violations."""
        conn_string, conn, _ = temp_test_db

        validation = DatabaseReferentialIntegrityCheck(
            name="OrderCustomerFK",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "foreign_key_table": "orders",
                "foreign_key_column": "customer_id",
                "reference_table": "customers",
                "reference_key_column": "id",
                "allow_null": False
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should find 1 violation (customer_id=99 doesn't exist)
        assert result.passed is False
        assert result.failed_count == 1
        assert "99" in str(result.sample_failures)

    def test_referential_integrity_all_valid(self, temp_test_db):
        """Test when all foreign keys are valid."""
        conn_string, conn, db_path = temp_test_db

        # Create a clean orders table with only valid customer_ids
        cursor = conn.cursor()
        cursor.execute("DELETE FROM orders")
        cursor.executemany(
            "INSERT INTO orders (id, customer_id, amount, status) VALUES (?, ?, ?, ?)",
            [
                (1, 1, 100.50, "completed"),
                (2, 2, 75.25, "completed"),
                (3, 5, 125.75, "completed")
            ]
        )
        conn.commit()

        validation = DatabaseReferentialIntegrityCheck(
            name="CleanOrdersFK",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "foreign_key_table": "orders",
                "foreign_key_column": "customer_id",
                "reference_table": "customers",
                "reference_key_column": "id"
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        assert result.passed is True
        assert result.failed_count == 0

    def test_referential_integrity_missing_params(self):
        """Test with missing required parameters."""
        validation = DatabaseReferentialIntegrityCheck(
            name="MissingParams",
            severity=Severity.ERROR,
            params={
                "connection_string": "sqlite:///test.db",
                "foreign_key_table": "orders"
                # Missing other required params
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        assert result.passed is False
        assert "Parameter" in result.message and "is required" in result.message


@pytest.mark.unit
class TestDatabaseConstraintCheck:
    """Tests for DatabaseConstraintCheck - validate database constraints."""

    def test_constraint_check_age_violations(self, temp_test_db):
        """Test detection of CHECK constraint violations."""
        conn_string, conn, _ = temp_test_db

        validation = DatabaseConstraintCheck(
            name="AgeConstraint",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "table": "customers",
                "constraint_name": "age_check",
                "constraint_query": """
                    SELECT id, name, age
                    FROM customers
                    WHERE age < 0 OR age > 150
                """
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        context = {"max_sample_failures": 100}

        result = validation.validate(data_iterator, context)

        # Should find 1 violation (age=160)
        assert result.passed is False
        assert result.failed_count >= 1
        assert "160" in str(result.sample_failures) or "David" in str(result.sample_failures)

    def test_constraint_check_no_violations(self, temp_test_db):
        """Test when no constraint violations exist."""
        conn_string, conn, _ = temp_test_db

        validation = DatabaseConstraintCheck(
            name="PositiveAmounts",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "table": "orders",
                "constraint_name": "positive_amount",
                "constraint_query": """
                    SELECT id, amount
                    FROM orders
                    WHERE amount <= 0
                """
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        # Should pass - all amounts are positive
        assert result.passed is True
        assert result.failed_count == 0

    def test_constraint_check_missing_params(self):
        """Test with missing required parameters."""
        validation = DatabaseConstraintCheck(
            name="MissingParams",
            severity=Severity.ERROR,
            params={
                "connection_string": "sqlite:///test.db"
                # Missing table and constraint_query
            }
        )

        data_iterator = create_data_iterator(pd.DataFrame())
        result = validation.validate(data_iterator, {})

        assert result.passed is False
        assert "Parameter" in result.message and "is required" in result.message


@pytest.mark.integration
class TestDatabaseValidationsIntegration:
    """Integration tests for database validations."""

    def test_combined_database_checks(self, temp_test_db):
        """Test multiple database validation checks together."""
        conn_string, conn, _ = temp_test_db

        # SQL custom check for age violations
        sql_check = SQLCustomCheck(
            name="InvalidAges",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "sql_query": "SELECT * FROM customers WHERE age < 18 OR age > 150"
            }
        )

        # Referential integrity check
        fk_check = DatabaseReferentialIntegrityCheck(
            name="OrdersFK",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "foreign_key_table": "orders",
                "foreign_key_column": "customer_id",
                "reference_table": "customers",
                "reference_key_column": "id"
            }
        )

        # Constraint check
        constraint_check = DatabaseConstraintCheck(
            name="AgeConstraint",
            severity=Severity.ERROR,
            params={
                "connection_string": conn_string,
                "table": "customers",
                "constraint_query": "SELECT * FROM customers WHERE age NOT BETWEEN 0 AND 150"
            }
        )

        context = {"max_sample_failures": 100}
        data_iterator = create_data_iterator(pd.DataFrame())

        sql_result = sql_check.validate(data_iterator, context)
        fk_result = fk_check.validate(data_iterator, context)
        constraint_result = constraint_check.validate(data_iterator, context)

        # All should find violations
        assert sql_result.passed is False
        assert fk_result.passed is False
        assert constraint_result.passed is False

        # Each should find at least one issue
        assert sql_result.failed_count > 0
        assert fk_result.failed_count > 0
        assert constraint_result.failed_count > 0
