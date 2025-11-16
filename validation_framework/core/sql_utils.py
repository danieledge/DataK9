"""
SQL utility functions for secure database operations.

This module provides utilities for safely handling SQL identifiers (table names,
column names) to prevent SQL injection attacks.
"""

import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class SQLIdentifierValidator:
    """
    Validates and sanitizes SQL identifiers (table names, column names).

    Prevents SQL injection by ensuring identifiers only contain safe characters
    and properly quoting them for use in SQL queries.
    """

    # Pattern for valid SQL identifiers: alphanumeric, underscore, and dot (for schema.table)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?$')

    # Maximum length for identifiers (most databases support 63-128 characters)
    MAX_IDENTIFIER_LENGTH = 63

    @classmethod
    def validate_identifier(cls, identifier: str, identifier_type: str = "identifier") -> None:
        """
        Validate that an identifier is safe to use in SQL queries.

        Args:
            identifier: The identifier to validate (table name, column name, etc.)
            identifier_type: Type of identifier for error messages (e.g., "table", "column")

        Raises:
            ValueError: If the identifier is invalid or potentially unsafe

        Examples:
            >>> SQLIdentifierValidator.validate_identifier("customers")
            >>> SQLIdentifierValidator.validate_identifier("public.orders")
            >>> SQLIdentifierValidator.validate_identifier("user_data_2024")
            >>> SQLIdentifierValidator.validate_identifier("'; DROP TABLE users; --")  # Raises ValueError
        """
        if not identifier:
            raise ValueError(f"Empty {identifier_type} name is not allowed")

        if len(identifier) > cls.MAX_IDENTIFIER_LENGTH:
            raise ValueError(
                f"{identifier_type} name '{identifier}' exceeds maximum length of "
                f"{cls.MAX_IDENTIFIER_LENGTH} characters"
            )

        # Check for SQL keywords that shouldn't be used as identifiers
        sql_keywords = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'UNION'
        }

        identifier_upper = identifier.upper()
        if identifier_upper in sql_keywords:
            raise ValueError(
                f"{identifier_type} name '{identifier}' is a SQL keyword and cannot be used"
            )

        # Validate pattern
        if not cls.IDENTIFIER_PATTERN.match(identifier):
            raise ValueError(
                f"Invalid {identifier_type} name '{identifier}'. "
                f"Identifiers must start with a letter or underscore, "
                f"and contain only letters, numbers, underscores, and dots (for schema.table)"
            )

        logger.debug(f"Validated {identifier_type}: {identifier}")

    @classmethod
    def validate_multiple_identifiers(cls, identifiers: list, identifier_type: str = "identifier") -> None:
        """
        Validate multiple identifiers at once.

        Args:
            identifiers: List of identifiers to validate
            identifier_type: Type of identifier for error messages

        Raises:
            ValueError: If any identifier is invalid
        """
        for identifier in identifiers:
            cls.validate_identifier(identifier, identifier_type)

    @staticmethod
    def quote_identifier(identifier: str, dialect: str = "postgresql") -> str:
        """
        Quote an identifier for safe use in SQL queries.

        Different database systems use different quoting mechanisms:
        - PostgreSQL, SQLite: double quotes "identifier"
        - MySQL, MariaDB: backticks `identifier`
        - SQL Server: square brackets [identifier]

        Args:
            identifier: The identifier to quote
            dialect: Database dialect (postgresql, mysql, mssql, oracle, sqlite)

        Returns:
            Quoted identifier safe for SQL interpolation

        Note:
            This should be used AFTER validation to prevent injection

        Examples:
            >>> SQLIdentifierValidator.quote_identifier("customers", "postgresql")
            '"customers"'
            >>> SQLIdentifierValidator.quote_identifier("orders", "mysql")
            '`orders`'
        """
        dialect = dialect.lower()

        # Handle schema.table notation
        if '.' in identifier:
            parts = identifier.split('.')
            quoted_parts = [SQLIdentifierValidator.quote_identifier(part, dialect) for part in parts]
            return '.'.join(quoted_parts)

        if dialect in ('postgresql', 'sqlite', 'oracle'):
            # Use double quotes and escape any existing quotes
            escaped = identifier.replace('"', '""')
            return f'"{escaped}"'
        elif dialect == 'mysql':
            # Use backticks and escape any existing backticks
            escaped = identifier.replace('`', '``')
            return f'`{escaped}`'
        elif dialect == 'mssql':
            # Use square brackets and escape any existing brackets
            escaped = identifier.replace(']', ']]')
            return f'[{escaped}]'
        else:
            # Default to PostgreSQL style
            logger.warning(f"Unknown dialect '{dialect}', using PostgreSQL-style quoting")
            escaped = identifier.replace('"', '""')
            return f'"{escaped}"'

    @staticmethod
    def build_safe_query(
        template: str,
        identifiers: dict,
        dialect: str = "postgresql"
    ) -> str:
        """
        Build a SQL query with safely quoted identifiers.

        Args:
            template: SQL query template with {identifier} placeholders
            identifiers: Dict mapping placeholder names to identifier values
            dialect: Database dialect for proper quoting

        Returns:
            SQL query with safely quoted identifiers

        Raises:
            ValueError: If any identifier is invalid

        Example:
            >>> template = "SELECT {col} FROM {table} WHERE {col} IS NOT NULL"
            >>> identifiers = {"table": "customers", "col": "email"}
            >>> query = SQLIdentifierValidator.build_safe_query(template, identifiers, "postgresql")
            >>> print(query)
            SELECT "email" FROM "customers" WHERE "email" IS NOT NULL
        """
        # Validate all identifiers first
        for name, identifier in identifiers.items():
            SQLIdentifierValidator.validate_identifier(identifier, f"{name} identifier")

        # Quote all identifiers
        quoted_identifiers = {
            name: SQLIdentifierValidator.quote_identifier(identifier, dialect)
            for name, identifier in identifiers.items()
        }

        # Build query with quoted identifiers
        return template.format(**quoted_identifiers)


def create_safe_select_query(
    table: str,
    columns: Optional[list] = None,
    where_clause: Optional[str] = None,
    dialect: str = "postgresql"
) -> str:
    """
    Create a safe SELECT query with validated identifiers.

    Args:
        table: Table name (will be validated and quoted)
        columns: List of column names (will be validated and quoted). If None, uses *
        where_clause: Optional WHERE clause (must be parameterized separately)
        dialect: Database dialect

    Returns:
        Safe SELECT query string

    Raises:
        ValueError: If table or column names are invalid

    Examples:
        >>> create_safe_select_query("customers")
        'SELECT * FROM "customers"'
        >>> create_safe_select_query("orders", ["order_id", "total"], dialect="mysql")
        'SELECT `order_id`, `total` FROM `orders`'
    """
    # Validate and quote table name
    SQLIdentifierValidator.validate_identifier(table, "table")
    quoted_table = SQLIdentifierValidator.quote_identifier(table, dialect)

    # Validate and quote column names
    if columns:
        SQLIdentifierValidator.validate_multiple_identifiers(columns, "column")
        quoted_columns = ', '.join(
            SQLIdentifierValidator.quote_identifier(col, dialect) for col in columns
        )
    else:
        quoted_columns = '*'

    # Build query
    query = f"SELECT {quoted_columns} FROM {quoted_table}"

    if where_clause:
        query += f" WHERE {where_clause}"

    return query


def create_safe_count_query(
    table: str,
    where_clause: Optional[str] = None,
    dialect: str = "postgresql"
) -> str:
    """
    Create a safe COUNT query with validated identifiers.

    Args:
        table: Table name (will be validated and quoted)
        where_clause: Optional WHERE clause (must be parameterized separately)
        dialect: Database dialect

    Returns:
        Safe COUNT query string

    Raises:
        ValueError: If table name is invalid

    Example:
        >>> create_safe_count_query("customers")
        'SELECT COUNT(*) as count FROM "customers"'
    """
    # Validate and quote table name
    SQLIdentifierValidator.validate_identifier(table, "table")
    quoted_table = SQLIdentifierValidator.quote_identifier(table, dialect)

    # Build query
    query = f"SELECT COUNT(*) as count FROM {quoted_table}"

    if where_clause:
        query += f" WHERE {where_clause}"

    return query
