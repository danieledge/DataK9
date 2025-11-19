#!/usr/bin/env python3
"""
Create SQLite test database from sample CSV files.

This script creates a test database populated with data from the examples/sample_data
directory. The database is used for testing database validations.

Author: Daniel Edge
Date: 2025-11-19
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_test_database(db_path: str = "test_data.db", sample_data_dir: str = None):
    """
    Create SQLite database from sample CSV files.

    Args:
        db_path: Path where database will be created
        sample_data_dir: Directory containing CSV files (default: examples/sample_data)
    """
    # Default to examples/sample_data
    if sample_data_dir is None:
        script_dir = Path(__file__).parent
        sample_data_dir = script_dir.parent / "examples" / "sample_data"
    else:
        sample_data_dir = Path(sample_data_dir)

    # Output database path
    db_file = Path(db_path)

    # Remove existing database
    if db_file.exists():
        logger.info(f"Removing existing database: {db_file}")
        db_file.unlink()

    # Create connection
    logger.info(f"Creating database: {db_file}")
    conn = sqlite3.connect(str(db_file))

    try:
        # Load customers
        customers_file = sample_data_dir / "customers.csv"
        if customers_file.exists():
            logger.info(f"Loading customers from {customers_file}")
            df_customers = pd.read_csv(customers_file)
            logger.info(f"  Rows: {len(df_customers)}, Columns: {len(df_customers.columns)}")

            # Write to database
            df_customers.to_sql('customers', conn, if_exists='replace', index=False)
            logger.info("  ✓ Created table: customers")

        # Load transactions
        transactions_file = sample_data_dir / "transactions.csv"
        if transactions_file.exists():
            logger.info(f"Loading transactions from {transactions_file}")
            df_transactions = pd.read_csv(transactions_file)
            logger.info(f"  Rows: {len(df_transactions)}, Columns: {len(df_transactions.columns)}")

            # Write to database
            df_transactions.to_sql('transactions', conn, if_exists='replace', index=False)
            logger.info("  ✓ Created table: transactions")

        # Load accounts
        accounts_file = sample_data_dir / "accounts.csv"
        if accounts_file.exists():
            logger.info(f"Loading accounts from {accounts_file}")
            df_accounts = pd.read_csv(accounts_file)
            logger.info(f"  Rows: {len(df_accounts)}, Columns: {len(df_accounts.columns)}")

            # Write to database
            df_accounts.to_sql('accounts', conn, if_exists='replace', index=False)
            logger.info("  ✓ Created table: accounts")

        # Create indexes for better performance
        logger.info("Creating indexes...")
        cursor = conn.cursor()

        # Customers indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_id ON customers(customer_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email)")

        # Transactions indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id)")

        # Accounts indexes (if account_id exists)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_customer ON accounts(customer_id)")

        conn.commit()
        logger.info("  ✓ Indexes created")

        # Print database statistics
        logger.info("\nDatabase Statistics:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            logger.info(f"  {table_name}: {count:,} rows")

        # Get database size
        db_size_bytes = db_file.stat().st_size
        db_size_mb = db_size_bytes / (1024 * 1024)
        logger.info(f"\nDatabase size: {db_size_mb:.2f} MB")
        logger.info(f"Database location: {db_file.absolute()}")

        # Print connection string for use
        logger.info(f"\n✓ Database created successfully!")
        logger.info(f"\nConnection string for validations:")
        logger.info(f'  "sqlite:///{db_file.absolute()}"')

    except Exception as e:
        logger.error(f"Error creating database: {str(e)}", exc_info=True)
        raise

    finally:
        conn.close()

    return str(db_file.absolute())


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Create test database from CSV files")
    parser.add_argument(
        "-o", "--output",
        default="test_data.db",
        help="Output database file path (default: test_data.db)"
    )
    parser.add_argument(
        "-d", "--data-dir",
        help="Directory containing CSV files (default: examples/sample_data)"
    )

    args = parser.parse_args()

    create_test_database(db_path=args.output, sample_data_dir=args.data_dir)


if __name__ == "__main__":
    main()
