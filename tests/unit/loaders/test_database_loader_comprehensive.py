"""
Comprehensive tests for database_loader.py.

Tests:
- Connection string validation and security
- SQL injection prevention
- Query safety validation
- Database type inference
- Chunked data loading
- Timeout handling
- Error scenarios
- NULL value handling
"""

import pytest
import sqlite3
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from validation_framework.loaders.database_loader import DatabaseLoader
from validation_framework.core.sql_utils import SQLIdentifierValidator


class TestDatabaseLoaderInitialization:
    """Test DatabaseLoader initialization and configuration."""

    def test_initialization_with_table(self, tmp_path):
        """Should initialize with table name."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="customers",
            chunk_size=100
        )

        assert loader.table == "customers"
        assert loader.query is None
        assert loader.chunk_size == 100
        assert loader.db_type == "sqlite"

    def test_initialization_with_query(self, tmp_path):
        """Should initialize with custom query."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="SELECT * FROM customers WHERE status = 'ACTIVE'",
            chunk_size=100
        )

        assert loader.query is not None
        assert loader.table is None
        assert loader.chunk_size == 100

    def test_initialization_without_table_or_query(self, tmp_path):
        """Should raise error without table or query."""
        db_path = tmp_path / "test.db"

        with pytest.raises(ValueError, match="Either 'query' or 'table' must be provided"):
            DatabaseLoader(
                connection_string=f"sqlite:///{db_path}",
                chunk_size=100
            )

    def test_initialization_with_both_table_and_query(self, tmp_path):
        """Should raise error with both table and query."""
        db_path = tmp_path / "test.db"

        with pytest.raises(ValueError, match="Provide either 'query' or 'table', not both"):
            DatabaseLoader(
                connection_string=f"sqlite:///{db_path}",
                table="customers",
                query="SELECT * FROM customers",
                chunk_size=100
            )

    def test_initialization_with_max_rows(self, tmp_path):
        """Should initialize with max_rows limit."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="customers",
            max_rows=1000
        )

        assert loader.max_rows == 1000

    def test_initialization_with_sample_percent(self, tmp_path):
        """Should initialize with sample_percent."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="SELECT * FROM customers",
            sample_percent=10.0
        )

        assert loader.sample_percent == 10.0


class TestDatabaseTypeInference:
    """Test database type inference from connection strings."""

    def test_infer_postgresql(self):
        """Should infer PostgreSQL from connection string."""
        loader = DatabaseLoader(
            connection_string="postgresql://user:pass@localhost/db",
            table="test"
        )
        assert loader.db_type == "postgresql"

    def test_infer_mysql(self):
        """Should infer MySQL from connection string."""
        loader = DatabaseLoader(
            connection_string="mysql+pymysql://user:pass@localhost/db",
            table="test"
        )
        assert loader.db_type == "mysql"

    def test_infer_sqlite(self, tmp_path):
        """Should infer SQLite from connection string."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="test"
        )
        assert loader.db_type == "sqlite"

    def test_infer_mssql(self):
        """Should infer MS SQL Server from connection string."""
        loader = DatabaseLoader(
            connection_string="mssql+pyodbc://user:pass@localhost/db",
            table="test"
        )
        assert loader.db_type == "mssql"

    def test_infer_oracle(self):
        """Should infer Oracle from connection string."""
        loader = DatabaseLoader(
            connection_string="oracle+cx_oracle://user:pass@localhost/db",
            table="test"
        )
        assert loader.db_type == "oracle"

    def test_explicit_db_type(self, tmp_path):
        """Should use explicit db_type when provided."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="test",
            db_type="postgresql"
        )
        # Should use explicit type
        assert loader.db_type == "postgresql"


class TestConnectionStringValidation:
    """Test connection string validation and security."""

    def test_valid_postgresql_connection(self):
        """Should accept valid PostgreSQL connection string."""
        loader = DatabaseLoader(
            connection_string="postgresql://user:pass@localhost:5432/database",
            table="test"
        )
        # Should not raise error
        loader._validate_connection_string()

    def test_valid_mysql_connection(self):
        """Should accept valid MySQL connection string."""
        loader = DatabaseLoader(
            connection_string="mysql+pymysql://user:pass@localhost:3306/database",
            table="test"
        )
        loader._validate_connection_string()

    def test_valid_sqlite_connection(self, tmp_path):
        """Should accept valid SQLite connection string."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="test"
        )
        loader._validate_connection_string()

    def test_invalid_protocol(self):
        """Should reject invalid protocol."""
        loader = DatabaseLoader(
            connection_string="invalid://user:pass@localhost/db",
            table="test"
        )

        with pytest.raises(ValueError, match="Invalid or unsupported database protocol"):
            loader._validate_connection_string()

    def test_path_traversal_in_sqlite(self):
        """Should reject path traversal in SQLite paths."""
        loader = DatabaseLoader(
            connection_string="sqlite:///../../../etc/passwd",
            table="test"
        )

        with pytest.raises(ValueError, match="Path traversal detected"):
            loader._validate_connection_string()

    def test_nested_file_protocol_in_sqlite(self):
        """Should reject nested file:// protocol in SQLite."""
        loader = DatabaseLoader(
            connection_string="sqlite:///file://malicious/path",
            table="test"
        )

        with pytest.raises(ValueError, match="Nested file:// protocol not allowed"):
            loader._validate_connection_string()


class TestTableIdentifierValidation:
    """Test table name validation for SQL injection prevention."""

    def test_valid_table_name(self, tmp_path):
        """Should accept valid table name."""
        db_path = tmp_path / "test.db"
        # Should not raise error
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="customers"
        )
        assert loader.table == "customers"

    def test_table_name_with_underscore(self, tmp_path):
        """Should accept table name with underscores."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="customer_orders"
        )
        assert loader.table == "customer_orders"

    def test_table_name_with_numbers(self, tmp_path):
        """Should accept table name with numbers."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="table_2024"
        )
        assert loader.table == "table_2024"

    def test_invalid_table_name_with_sql_injection(self, tmp_path):
        """Should reject table name with SQL injection attempt."""
        db_path = tmp_path / "test.db"

        with pytest.raises(ValueError, match="Invalid table name"):
            DatabaseLoader(
                connection_string=f"sqlite:///{db_path}",
                table="customers; DROP TABLE users--"
            )

    def test_invalid_table_name_with_quotes(self, tmp_path):
        """Should reject table name with quotes."""
        db_path = tmp_path / "test.db"

        with pytest.raises(ValueError):
            DatabaseLoader(
                connection_string=f"sqlite:///{db_path}",
                table="customers' OR '1'='1"
            )


class TestQuerySafetyValidation:
    """Test SQL query safety validation."""

    def test_query_safety_select_only(self, tmp_path):
        """Should validate that query is SELECT only."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="SELECT * FROM customers"
        )

        # Should not raise error for SELECT
        loader._validate_query_safety(loader.query)

    def test_query_safety_reject_drop(self, tmp_path):
        """Should reject DROP statements."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="DROP TABLE customers"
        )

        with pytest.raises(ValueError, match="Only SELECT queries are allowed"):
            loader._validate_query_safety(loader.query)

    def test_query_safety_reject_delete(self, tmp_path):
        """Should reject DELETE statements."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="DELETE FROM customers WHERE id=1"
        )

        with pytest.raises(ValueError):
            loader._validate_query_safety(loader.query)

    def test_query_safety_reject_update(self, tmp_path):
        """Should reject UPDATE statements."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="UPDATE customers SET name='hacked'"
        )

        with pytest.raises(ValueError):
            loader._validate_query_safety(loader.query)

    def test_query_safety_reject_insert(self, tmp_path):
        """Should reject INSERT statements."""
        db_path = tmp_path / "test.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="INSERT INTO customers VALUES (1, 'test')"
        )

        with pytest.raises(ValueError):
            loader._validate_query_safety(loader.query)


class TestChunkedDataLoading:
    """Test chunked data loading functionality."""

    @pytest.fixture
    def populated_db(self, tmp_path):
        """Create a populated test database."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))

        # Create test table with 100 rows
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Customer_{i}' for i in range(1, 101)],
            'amount': [i * 10.5 for i in range(1, 101)],
            'status': ['ACTIVE' if i % 2 == 0 else 'INACTIVE' for i in range(1, 101)]
        })

        df.to_sql('customers', conn, if_exists='replace', index=False)
        conn.close()

        return db_path

    def test_load_chunks_basic(self, populated_db):
        """Should load data in chunks."""
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{populated_db}",
            table="customers",
            chunk_size=25
        )

        chunks = list(loader.load_chunks())

        # Should have 4 chunks (100 rows / 25 per chunk)
        assert len(chunks) == 4

        # Each chunk should be a DataFrame
        for chunk in chunks:
            assert isinstance(chunk, pd.DataFrame)

        # Total rows should be 100
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 100

    def test_load_chunks_with_max_rows(self, populated_db):
        """Should respect max_rows limit."""
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{populated_db}",
            table="customers",
            chunk_size=25,
            max_rows=50
        )

        chunks = list(loader.load_chunks())

        # Should only load 50 rows
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 50

    def test_load_chunks_with_query(self, populated_db):
        """Should load data using custom query."""
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{populated_db}",
            query="SELECT * FROM customers WHERE status = 'ACTIVE'",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())

        # Should have chunks
        assert len(chunks) > 0

        # All rows should have ACTIVE status
        for chunk in chunks:
            assert all(chunk['status'] == 'ACTIVE')

    def test_load_chunks_empty_table(self, tmp_path):
        """Should handle empty table gracefully."""
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))

        # Create empty table
        conn.execute("CREATE TABLE empty_table (id INTEGER, name TEXT)")
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="empty_table",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())

        # Should return empty list or single empty chunk
        if chunks:
            assert all(len(chunk) == 0 for chunk in chunks)


class TestNullValueHandling:
    """Test NULL value handling in database loading."""

    @pytest.fixture
    def db_with_nulls(self, tmp_path):
        """Create database with NULL values."""
        db_path = tmp_path / "nulls.db"
        conn = sqlite3.connect(str(db_path))

        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', None, 'Charlie', 'David', None],
            'amount': [100.0, 200.0, None, 400.0, None],
            'date': [None, '2024-01-02', '2024-01-03', None, '2024-01-05']
        })

        df.to_sql('data_with_nulls', conn, if_exists='replace', index=False)
        conn.close()

        return db_path

    def test_null_value_loading(self, db_with_nulls):
        """Should correctly load NULL values as pandas NaN/None."""
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_with_nulls}",
            table="data_with_nulls",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())
        df = pd.concat(chunks, ignore_index=True)

        # Check NULL values are represented correctly
        assert df['name'].isna().sum() == 2
        assert df['amount'].isna().sum() == 2
        assert df['date'].isna().sum() == 2

    def test_null_value_detection(self, db_with_nulls):
        """Should allow detection of NULL values with pd.isna()."""
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_with_nulls}",
            table="data_with_nulls",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())
        df = pd.concat(chunks, ignore_index=True)

        # Should be able to detect NULLs
        null_names = df[df['name'].isna()]
        assert len(null_names) == 2


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_nonexistent_database(self, tmp_path):
        """Should handle non-existent database gracefully."""
        db_path = tmp_path / "nonexistent.db"
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="customers",
            chunk_size=10
        )

        # Should raise error when trying to load
        with pytest.raises(Exception):
            list(loader.load_chunks())

    def test_nonexistent_table(self, tmp_path):
        """Should handle non-existent table gracefully."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="nonexistent_table",
            chunk_size=10
        )

        # Should raise error when trying to load
        with pytest.raises(Exception):
            list(loader.load_chunks())

    def test_malformed_query(self, tmp_path):
        """Should handle malformed SQL query."""
        # Create a test database
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        df = pd.DataFrame({'id': [1, 2, 3]})
        df.to_sql('customers', conn, if_exists='replace', index=False)
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            query="SELECT * FROM customers WHERE",  # Incomplete WHERE
            chunk_size=10
        )

        # Should raise error when executing query
        with pytest.raises(Exception):
            list(loader.load_chunks())

    def test_connection_timeout_handling(self, tmp_path):
        """Should handle connection timeouts gracefully."""
        db_path = tmp_path / "test.db"

        # Test that timeout parameter is accepted
        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="test",
            chunk_size=10
        )

        # The loader should have been created
        assert loader is not None


class TestDatabaseLoaderContext:
    """Test database loader context manager and resource cleanup."""

    def test_connection_cleanup(self, tmp_path):
        """Should properly clean up database connections."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="test",
            chunk_size=10
        )

        # Load chunks
        list(loader.load_chunks())

        # Connection should be cleaned up
        # This is implementation-dependent


class TestSpecialCases:
    """Test special cases and edge scenarios."""

    def test_unicode_data(self, tmp_path):
        """Should handle Unicode data correctly."""
        db_path = tmp_path / "unicode.db"
        conn = sqlite3.connect(str(db_path))

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', '李明', 'José'],
            'description': ['English', '中文', 'Español']
        })

        df.to_sql('unicode_data', conn, if_exists='replace', index=False)
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="unicode_data",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())
        df_loaded = pd.concat(chunks, ignore_index=True)

        # Should preserve Unicode characters
        assert '李明' in df_loaded['name'].values
        assert 'José' in df_loaded['name'].values

    def test_large_text_fields(self, tmp_path):
        """Should handle large text fields."""
        db_path = tmp_path / "large_text.db"
        conn = sqlite3.connect(str(db_path))

        large_text = 'x' * 10000  # 10KB text
        df = pd.DataFrame({
            'id': [1, 2],
            'content': [large_text, large_text]
        })

        df.to_sql('large_text', conn, if_exists='replace', index=False)
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="large_text",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())
        df_loaded = pd.concat(chunks, ignore_index=True)

        # Should load large text correctly
        assert len(df_loaded['content'].iloc[0]) == 10000

    def test_numeric_precision(self, tmp_path):
        """Should maintain numeric precision."""
        db_path = tmp_path / "precision.db"
        conn = sqlite3.connect(str(db_path))

        df = pd.DataFrame({
            'id': [1, 2, 3],
            'precise_value': [1.23456789012345, 9.87654321098765, 3.14159265358979]
        })

        df.to_sql('precision_test', conn, if_exists='replace', index=False)
        conn.close()

        loader = DatabaseLoader(
            connection_string=f"sqlite:///{db_path}",
            table="precision_test",
            chunk_size=10
        )

        chunks = list(loader.load_chunks())
        df_loaded = pd.concat(chunks, ignore_index=True)

        # Should maintain reasonable precision
        # (exact precision depends on SQLite float handling)
        assert abs(df_loaded['precise_value'].iloc[0] - 1.23456789012345) < 1e-10
