#!/usr/bin/env python3
"""
Example: Database Validation with DataK9

This example demonstrates how to validate data directly from a SQLite database
using DataK9's database loader. The same validations work identically whether
your data source is a file or a database.

Author: Daniel Edge
Date: 2025-11-19
"""

from validation_framework.loaders.factory import LoaderFactory
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck, RegexCheck
from validation_framework.validations.builtin.record_checks import UniqueKeyCheck
from validation_framework.validations.builtin.advanced_checks import CompletenessCheck
from validation_framework.core.pretty_output import PrettyOutput as po
from pathlib import Path


def main():
    """Run database validation example."""

    # Path to test database
    db_path = Path(__file__).parent.parent / "test_data.db"

    if not db_path.exists():
        po.error(f"Test database not found: {db_path}")
        po.info("Run: python3 scripts/create_test_database.py")
        return

    po.logo()
    po.header("DataK9 Database Validation Example")

    # Connection string for SQLite
    connection_string = f"sqlite:///{db_path.absolute()}"

    po.section("Database Connection")
    po.info(f"Database: {db_path.name}")
    po.info(f"Connection: {connection_string}")

    # Create database loader for customers table
    po.section("Creating Database Loader")
    loader = LoaderFactory.create_database_loader(
        connection_string=connection_string,
        table="customers",
        chunk_size=100  # Small chunks for demo
    )

    po.success(f"✓ Loader created for table: {loader.table}")
    po.info(f"  Database type: {loader.db_type}")
    po.info(f"  Chunk size: {loader.chunk_size}")

    # Get row count
    row_count = loader.get_row_count()
    po.info(f"  Total rows: {row_count:,}")

    # Define validations
    po.section("Running Validations")

    validations = [
        ("Mandatory Email", MandatoryFieldCheck(
            name="EmailRequired",
            severity="ERROR",
            params={"fields": ["email"]}
        )),

        ("Email Format", RegexCheck(
            name="EmailFormat",
            severity="ERROR",
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            }
        )),

        ("Unique Customer ID", UniqueKeyCheck(
            name="UniqueCustomerID",
            severity="ERROR",
            params={"fields": ["customer_id"]}
        )),

        ("Email Completeness", CompletenessCheck(
            name="EmailCompleteness",
            severity="WARNING",
            params={"field": "email", "min_completeness": 95}
        ))
    ]

    # Run validations
    results = []
    for name, validation in validations:
        po.info(f"Running: {name}")

        # Get fresh data iterator for each validation
        data_iterator = loader.load_chunks()
        result = validation.validate(data_iterator, context={})

        results.append((name, result))

        # Show result
        if result.passed:
            po.success(f"  ✓ {result.message}")
        else:
            po.warning(f"  ✗ {result.message}")
            if result.sample_failures:
                po.info(f"    Sample failures: {len(result.sample_failures)}")

    # Summary
    po.section("Validation Summary")

    passed = sum(1 for _, r in results if r.passed)
    failed = sum(1 for _, r in results if not r.passed)

    # Print summary directly
    po.info(f"Database: {db_path.name}")
    po.info(f"Table: {loader.table}")
    po.info(f"Rows Validated: {row_count:,}")
    po.info(f"Validations Run: {len(validations)}")
    po.info(f"Passed: {passed}")
    po.info(f"Failed: {failed}")

    if failed == 0:
        po.success(f"Status: ✓ PASSED")
    else:
        po.warning(f"Status: ✗ FAILED")

    # Show failed validation details
    if failed > 0:
        po.section("Failed Validation Details")
        for name, result in results:
            if not result.passed:
                po.warning(f"{name}:")
                po.info(f"  Message: {result.message}")
                po.info(f"  Failed count: {result.failed_count}")

                if result.sample_failures:
                    po.info(f"  Sample failures (showing up to 3):")
                    for failure in result.sample_failures[:3]:
                        po.info(f"    • Row {failure.get('row', '?')}: {failure.get('message', '')}")

    po.section("Key Takeaway")
    po.info("These same validations work identically on CSV, Excel, Parquet, and Database sources!")
    po.info("The iterator pattern makes DataK9 source-agnostic.")


if __name__ == "__main__":
    main()
