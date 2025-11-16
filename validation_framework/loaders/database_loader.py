"""
Database loader for validating data directly from databases.

Supports:
- PostgreSQL
- MySQL
- SQL Server
- Oracle
- SQLite
"""

from typing import Iterator, Dict, Any, Optional
import pandas as pd
from pathlib import Path
from validation_framework.core.sql_utils import SQLIdentifierValidator, create_safe_select_query, create_safe_count_query
import logging

logger = logging.getLogger(__name__)


class DatabaseLoader:
    """
    Loader for reading data from databases in chunks.

    Supports chunked reading for memory-efficient processing of large database tables.
    """

    def __init__(
        self,
        connection_string: str,
        query: str = None,
        table: str = None,
        chunk_size: int = 10000,
        db_type: str = "postgresql"
    ):
        """
        Initialize database loader.

        Args:
            connection_string: Database connection string
                Examples:
                - PostgreSQL: "postgresql://user:password@host:port/database"
                - MySQL: "mysql+pymysql://user:password@host:port/database"
                - SQL Server: "mssql+pyodbc://user:password@host:port/database?driver=ODBC+Driver+17+for+SQL+Server"
                - Oracle: "oracle+cx_oracle://user:password@host:port/?service_name=service"
                - SQLite: "sqlite:///path/to/database.db"
            query: SQL query to execute (alternative to table)
            table: Table name to read (alternative to query)
            chunk_size: Number of rows to read per chunk
            db_type: Database type (postgresql, mysql, mssql, oracle, sqlite)
        """
        self.connection_string = connection_string
        self.query = query
        self.table = table
        self.chunk_size = chunk_size
        self.db_type = db_type.lower()
        self.connection = None

        # Validate that either query or table is provided
        if not query and not table:
            raise ValueError("Either 'query' or 'table' must be provided")

        if query and table:
            raise ValueError("Provide either 'query' or 'table', not both")

        # Validate table identifier if provided to prevent SQL injection
        if table:
            try:
                SQLIdentifierValidator.validate_identifier(table, "table")
            except ValueError as e:
                raise ValueError(f"Invalid table name: {str(e)}")

    def load_chunks(self) -> Iterator[pd.DataFrame]:
        """
        Load data in chunks from database.

        Yields:
            DataFrame chunks

        Raises:
            ImportError: If required database driver is not installed
            Exception: For database connection or query errors
        """
        try:
            # Import SQLAlchemy
            try:
                from sqlalchemy import create_engine, text
            except ImportError:
                raise ImportError(
                    "SQLAlchemy is required for database connectivity. "
                    "Install with: pip install sqlalchemy"
                )

            # Check for database-specific drivers
            self._check_driver_requirements()

            # Create engine
            engine = create_engine(self.connection_string)

            # Build query
            if self.query:
                sql_query = self.query
                logger.info(f"Loading data from database using custom query: {self.db_type}")
            else:
                # Use safe query builder to prevent SQL injection
                sql_query = create_safe_select_query(
                    table=self.table,
                    columns=None,  # SELECT *
                    dialect=self.db_type
                )
                logger.info(f"Loading data from database table: {self.db_type}")

            logger.debug(f"Query: {sql_query}")

            # Read in chunks using pandas
            for chunk in pd.read_sql_query(
                sql_query,
                engine,
                chunksize=self.chunk_size
            ):
                yield chunk

            engine.dispose()

        except ImportError as e:
            logger.error(f"Missing required package for {self.db_type}: {str(e)}")
            raise

        except Exception as e:
            logger.error(f"Error loading data from database: {str(e)}", exc_info=True)
            raise

    def _check_driver_requirements(self):
        """Check if required database driver is installed."""
        driver_requirements = {
            "postgresql": ("psycopg2", "Install with: pip install psycopg2-binary"),
            "mysql": ("pymysql", "Install with: pip install pymysql"),
            "mssql": ("pyodbc", "Install with: pip install pyodbc"),
            "oracle": ("cx_Oracle", "Install with: pip install cx-Oracle"),
            "sqlite": (None, None),  # SQLite is built-in
        }

        if self.db_type in driver_requirements:
            driver, install_msg = driver_requirements[self.db_type]
            if driver:
                try:
                    __import__(driver)
                except ImportError:
                    raise ImportError(
                        f"Database driver '{driver}' not found for {self.db_type}. "
                        f"{install_msg}"
                    )

    def get_row_count(self) -> int:
        """
        Get total row count from database.

        Returns:
            Total number of rows
        """
        try:
            from sqlalchemy import create_engine, text

            engine = create_engine(self.connection_string)

            # Build count query
            if self.query:
                # Wrap query in count (user-provided query, assume trusted)
                count_query = f"SELECT COUNT(*) as count FROM ({self.query}) AS subquery"
            else:
                # Use safe query builder to prevent SQL injection
                count_query = create_safe_count_query(
                    table=self.table,
                    dialect=self.db_type
                )

            with engine.connect() as conn:
                result = conn.execute(text(count_query))
                count = result.scalar()

            engine.dispose()
            return count

        except Exception as e:
            logger.error(f"Error getting row count: {str(e)}")
            return 0

    def get_columns(self) -> list:
        """
        Get column names from query/table.

        Returns:
            List of column names
        """
        try:
            from sqlalchemy import create_engine

            engine = create_engine(self.connection_string)

            # Read first row to get columns
            if self.query:
                sql_query = self.query
            else:
                # Use safe query builder to prevent SQL injection
                sql_query = create_safe_select_query(
                    table=self.table,
                    columns=None,  # SELECT *
                    dialect=self.db_type
                )

            df = pd.read_sql_query(sql_query, engine, chunksize=1)
            first_chunk = next(df)
            columns = list(first_chunk.columns)

            engine.dispose()
            return columns

        except Exception as e:
            logger.error(f"Error getting columns: {str(e)}")
            return []


def create_database_loader(config: Dict[str, Any]) -> DatabaseLoader:
    """
    Factory function to create database loader from configuration.

    Args:
        config: Database configuration dict with keys:
            - connection_string (required)
            - query or table (one required)
            - chunk_size (optional)
            - db_type (optional)

    Returns:
        DatabaseLoader instance

    Example config:
        {
            "connection_string": "postgresql://user:pass@localhost/db",
            "table": "customers",
            "chunk_size": 10000,
            "db_type": "postgresql"
        }
    """
    return DatabaseLoader(
        connection_string=config.get("connection_string"),
        query=config.get("query"),
        table=config.get("table"),
        chunk_size=config.get("chunk_size", 10000),
        db_type=config.get("db_type", "postgresql")
    )
