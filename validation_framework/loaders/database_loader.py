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

    NULL Value Handling:
        Database NULL values are converted to pandas' missing value representation
        during data loading. The framework handles NULL values consistently:

        - Database NULL → pandas NaN (for numeric columns)
        - Database NULL → pandas None/NaN (for object columns)
        - Database NULL → pandas NaT (for datetime columns)

        Validations that check for missing values (like CompletenessCheck,
        MandatoryFieldCheck) will detect these NULL values appropriately.

        Best Practices:
        - Use pd.isna() or pd.isnull() to check for missing values
        - Use df.fillna() to handle missing values before validation if needed
        - Configure validation thresholds to account for expected NULL rates
        - Use MandatoryFieldCheck to enforce non-NULL constraints

        Example:
            # Check for NULL values in a column
            null_count = df['column_name'].isna().sum()

            # Filter rows with NULL values
            rows_with_nulls = df[df['column_name'].isna()]

            # Replace NULLs with default value
            df['column_name'].fillna(0, inplace=True)
    """

    def __init__(
        self,
        connection_string: str,
        query: str = None,
        table: str = None,
        chunk_size: int = 10000,
        db_type: str = None,
        max_rows: Optional[int] = None,
        sample_percent: Optional[float] = None
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
            db_type: Database type (postgresql, mysql, mssql, oracle, sqlite).
                     If not provided, will be inferred from connection_string.
            max_rows: Maximum number of rows to process (safety limit for production)
                     If None, processes all rows (use with caution on large tables)
            sample_percent: Sample percentage (0.0-100.0) for validation on subset
                           Only works with custom queries, not table names

        Production Safety:
            - For large production tables, ALWAYS specify max_rows or sample_percent
            - Use custom queries with WHERE clauses to filter data
            - Consider validating recent data: WHERE created_date >= CURRENT_DATE - 7
        """
        self.connection_string = connection_string
        self.query = query
        self.table = table
        self.chunk_size = chunk_size
        self.max_rows = max_rows
        self.sample_percent = sample_percent

        # Infer db_type from connection string if not provided
        if db_type is None:
            self.db_type = self._infer_db_type(connection_string)
        else:
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

    def _infer_db_type(self, connection_string: str) -> str:
        """
        Infer database type from connection string.

        Args:
            connection_string: Database connection string

        Returns:
            Database type string (postgresql, mysql, sqlite, mssql, oracle)
        """
        conn_lower = connection_string.lower()

        if conn_lower.startswith("sqlite"):
            return "sqlite"
        elif conn_lower.startswith("postgresql") or conn_lower.startswith("postgres"):
            return "postgresql"
        elif conn_lower.startswith("mysql"):
            return "mysql"
        elif conn_lower.startswith("mssql") or conn_lower.startswith("sqlserver"):
            return "mssql"
        elif conn_lower.startswith("oracle"):
            return "oracle"
        else:
            # Default to postgresql if can't determine
            logger.warning(f"Could not infer database type from connection string: {connection_string[:30]}... Defaulting to postgresql")
            return "postgresql"

    def _validate_connection_string(self) -> None:
        """
        Validate connection string for security.

        Prevents:
        - Path traversal attacks in SQLite file paths
        - Dangerous protocols
        - Malformed connection strings

        Raises:
            ValueError: If connection string is invalid or unsafe
        """
        conn_lower = self.connection_string.lower()

        # Whitelist allowed protocols
        allowed_protocols = [
            'postgresql://', 'postgres://',
            'mysql://', 'mysql+pymysql://',
            'mssql://', 'mssql+pyodbc://', 'sqlserver://',
            'oracle://', 'oracle+cx_oracle://',
            'sqlite:///'
        ]

        if not any(conn_lower.startswith(proto) for proto in allowed_protocols):
            raise ValueError(
                f"Invalid or unsupported database protocol. "
                f"Allowed protocols: {', '.join(allowed_protocols)}"
            )

        # Validate SQLite paths for path traversal
        if conn_lower.startswith('sqlite:///'):
            # Extract file path after sqlite:///
            file_path = self.connection_string[10:]  # Remove 'sqlite:///'

            # Check for path traversal attempts
            if '..' in file_path:
                raise ValueError(
                    "Path traversal detected in SQLite connection string. "
                    "Use absolute paths only."
                )

            # Block file:// protocol within path
            if 'file://' in file_path.lower():
                raise ValueError(
                    "Nested file:// protocol not allowed in SQLite path"
                )

        logger.debug("Connection string validation passed")

    def _validate_query_safety(self, query: str) -> None:
        """
        Validate user-provided SQL query for safety.

        Prevents SQL injection by checking for dangerous keywords and patterns.
        This is defense-in-depth - users should use read-only credentials.

        Args:
            query: SQL query to validate

        Raises:
            ValueError: If query contains dangerous keywords or patterns

        Note:
            This does NOT make arbitrary queries safe. Always use:
            - Read-only database credentials
            - Principle of least privilege
            - WHERE clauses to limit data
        """
        query_upper = query.upper()

        # Block dangerous SQL keywords
        dangerous_keywords = [
            'DROP', 'DELETE', 'TRUNCATE', 'INSERT', 'UPDATE',
            'ALTER', 'CREATE', 'GRANT', 'REVOKE',
            'EXEC', 'EXECUTE', 'CALL',
            '--', '/*', '*/',  # Comment markers
        ]

        for keyword in dangerous_keywords:
            if keyword in query_upper:
                raise ValueError(
                    f"Query contains dangerous keyword '{keyword}'. "
                    f"Only SELECT queries are allowed for validation. "
                    f"Use read-only database credentials for additional safety."
                )

        # Block multiple statements (semicolon followed by more SQL)
        # Allow trailing semicolon but not multiple statements
        semicolon_count = query.count(';')
        if semicolon_count > 1:
            raise ValueError(
                "Multiple SQL statements detected (multiple semicolons). "
                "Only single SELECT queries are allowed."
            )
        elif semicolon_count == 1 and not query.strip().endswith(';'):
            raise ValueError(
                "Semicolon detected in middle of query. "
                "Only single SELECT queries are allowed."
            )

        # Query must start with SELECT (after whitespace)
        query_stripped = query.strip()
        if not query_stripped.upper().startswith('SELECT'):
            raise ValueError(
                "Query must be a SELECT statement. "
                f"Found: {query_stripped[:50]}..."
            )

        logger.debug("Query safety validation passed")

    def load_chunks(self) -> Iterator[pd.DataFrame]:
        """
        Load data in chunks from database.

        Yields:
            DataFrame chunks

        Raises:
            ImportError: If required database driver is not installed
            Exception: For database connection or query errors
        """
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

        # Validate connection string for security
        self._validate_connection_string()

        # Create engine with timeouts and connection pool settings
        engine = create_engine(
            self.connection_string,
            pool_pre_ping=True,  # Verify connections before using them
            connect_args={
                'connect_timeout': 30,  # 30 second connection timeout
            }
        )

        try:
            # Build query
            if self.query:
                # Validate user-provided query for SQL injection
                self._validate_query_safety(self.query)
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

            # Count total rows if max_rows is set (production safety check)
            if self.max_rows is not None:
                count_query = f"SELECT COUNT(*) as row_count FROM ({sql_query}) AS subquery"
                total_rows = pd.read_sql_query(count_query, engine).iloc[0]['row_count']

                if total_rows > self.max_rows:
                    logger.warning(
                        f"Table/query has {total_rows:,} rows but max_rows={self.max_rows:,}. "
                        f"Only processing first {self.max_rows:,} rows for safety."
                    )
                    # Add LIMIT to query
                    if self.db_type in ["postgresql", "mysql", "sqlite"]:
                        sql_query = f"{sql_query} LIMIT {self.max_rows}"
                    elif self.db_type == "mssql":
                        sql_query = f"SELECT TOP {self.max_rows} * FROM ({sql_query}) AS limited"
                    elif self.db_type == "oracle":
                        sql_query = f"SELECT * FROM ({sql_query}) WHERE ROWNUM <= {self.max_rows}"
                else:
                    logger.info(f"Processing {total_rows:,} rows (within max_rows limit)")

            # Read in chunks using pandas
            rows_processed = 0
            for chunk in pd.read_sql_query(
                sql_query,
                engine,
                chunksize=self.chunk_size
            ):
                # Enforce max_rows limit strictly (trim last chunk if needed)
                if self.max_rows and rows_processed + len(chunk) > self.max_rows:
                    # Trim the chunk to exact max_rows
                    rows_to_take = self.max_rows - rows_processed
                    chunk = chunk.iloc[:rows_to_take]
                    rows_processed += len(chunk)
                    yield chunk
                    logger.info(f"Reached max_rows limit ({self.max_rows:,}). Stopping data load.")
                    break

                rows_processed += len(chunk)
                yield chunk

            logger.info(f"Processed {rows_processed:,} total rows from database")

        except ImportError as e:
            logger.error(f"Missing required package for {self.db_type}: {str(e)}")
            raise

        except Exception as e:
            logger.error(f"Error loading data from database: {str(e)}", exc_info=True)
            raise

        finally:
            # CRITICAL: Always dispose of engine to prevent connection leaks
            # This runs even if the generator is not fully consumed
            if engine:
                engine.dispose()
                logger.debug("Database engine disposed successfully")

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
            Total number of rows (0 if error occurs)
        """
        from sqlalchemy import create_engine, text

        engine = None
        try:
            engine = create_engine(self.connection_string)

            # Build count query
            if self.query:
                # Validate user query for safety
                self._validate_query_safety(self.query)
                # Wrap query in count
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

            return count

        except Exception as e:
            logger.error(f"Error getting row count: {str(e)}")
            return 0

        finally:
            # Always cleanup engine
            if engine:
                engine.dispose()

    def get_columns(self) -> list:
        """
        Get column names from query/table.

        Returns:
            List of column names

        Raises:
            Exception: If unable to retrieve column information
        """
        from sqlalchemy import create_engine

        engine = None
        try:
            engine = create_engine(self.connection_string)

            # Build query with LIMIT 1 for efficiency
            if self.query:
                # Wrap user query in subquery with LIMIT
                sql_query = f"SELECT * FROM ({self.query}) AS subquery LIMIT 1"
            else:
                # Use safe query builder to prevent SQL injection
                sql_query = create_safe_select_query(
                    table=self.table,
                    columns=None,  # SELECT *
                    dialect=self.db_type
                )
                # Add LIMIT 1 for efficiency
                if self.db_type in ["postgresql", "mysql", "sqlite"]:
                    sql_query = f"{sql_query} LIMIT 1"
                elif self.db_type == "mssql":
                    sql_query = f"SELECT TOP 1 * FROM {self.table}"
                elif self.db_type == "oracle":
                    sql_query = f"SELECT * FROM {self.table} WHERE ROWNUM <= 1"

            # Read single row to get column names
            df = pd.read_sql_query(sql_query, engine)
            columns = list(df.columns)

            return columns

        except Exception as e:
            logger.error(f"Error getting columns from database: {str(e)}")
            raise

        finally:
            # Always cleanup engine
            if engine:
                engine.dispose()

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the database source.

        Returns:
            Dictionary with metadata:
            - row_count: Total number of rows
            - columns: List of column names
            - db_type: Database type
            - source_type: 'database'
            - table: Table name (if applicable)
            - query: Query string (if applicable)
        """
        metadata = {
            "source_type": "database",
            "db_type": self.db_type,
            "row_count": self.get_row_count(),
            "columns": self.get_columns(),
        }

        if self.table:
            metadata["table"] = self.table
        if self.query:
            metadata["query"] = self.query

        return metadata


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
