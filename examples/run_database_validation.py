#!/usr/bin/env python3
"""
Run database validation using DataK9.

This script demonstrates how to validate database tables programmatically.
Once YAML support is complete, this can be replaced with a YAML config.

Author: Daniel Edge
Date: 2025-11-19
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from validation_framework.loaders.factory import LoaderFactory
from validation_framework.core.engine import ValidationEngine
from validation_framework.core.config import ValidationConfig
from validation_framework.core.results import Severity
from validation_framework.core.pretty_output import PrettyOutput as po
from validation_framework.core.registry import get_registry
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck, RegexCheck, RangeCheck
from validation_framework.validations.builtin.record_checks import UniqueKeyCheck
from validation_framework.validations.builtin.advanced_checks import CompletenessCheck


def main():
    """Run database validation example."""

    db_path = Path(__file__).parent.parent / "test_data.db"

    if not db_path.exists():
        po.error(f"Test database not found: {db_path}")
        po.info("Run: python3 scripts/create_test_database.py")
        return 1

    po.logo()
    po.header("Database Validation Example")

    # Database connection details
    connection_string = f"sqlite:///{db_path.absolute()}"
    table_name = "customers"

    po.section("Database Connection")
    po.info(f"Database: {db_path.name}")
    po.info(f"Table: {table_name}")
    po.info(f"Connection: {connection_string}")

    # Create validation configuration programmatically
    # (Future: Load from YAML when database support is complete)
    po.section("Building Validation Configuration")

    config_dict = {
        "validation_job": {
            "name": "Database Quality Check",
            "version": "1.0",
            "description": "Validate customer data from database",
            "files": [
                {
                    "name": "customers_db",
                    "path": str(db_path),  # Database path
                    "format": "csv",  # Placeholder (will override with loader)
                    "validations": [
                        {
                            "type": "MandatoryFieldCheck",
                            "severity": "ERROR",
                            "params": {"fields": ["customer_id", "email"]}
                        },
                        {
                            "type": "RegexCheck",
                            "severity": "ERROR",
                            "params": {
                                "field": "email",
                                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                            }
                        },
                        {
                            "type": "UniqueKeyCheck",
                            "severity": "ERROR",
                            "params": {"fields": ["customer_id"]}
                        },
                        {
                            "type": "CompletenessCheck",
                            "severity": "WARNING",
                            "params": {
                                "field": "email",
                                "min_completeness": 95
                            }
                        },
                        {
                            "type": "RangeCheck",
                            "severity": "WARNING",
                            "params": {
                                "field": "account_balance",
                                "min_value": 0,
                                "max_value": 10000000
                            }
                        }
                    ]
                }
            ]
        }
    }

    po.success(f"✓ Configured {len(config_dict['validation_job']['files'][0]['validations'])} validations")

    # Create database loader
    po.section("Creating Database Loader")
    db_loader = LoaderFactory.create_database_loader(
        connection_string=connection_string,
        table=table_name,
        chunk_size=10000
    )

    row_count = db_loader.get_row_count()
    po.success(f"✓ Connected to database")
    po.info(f"  Table: {table_name}")
    po.info(f"  Rows: {row_count:,}")
    po.info(f"  Chunk size: {db_loader.chunk_size:,}")

    # Run validations
    po.section("Running Validations")

    registry = get_registry()
    validations = config_dict['validation_job']['files'][0]['validations']
    results = []

    for val_config in validations:
        val_type = val_config['type']
        val_class = registry.get(val_type)

        po.info(f"Running: {val_type}")

        # Create validation instance
        validation = val_class(
            name=val_type,
            severity=Severity.ERROR if val_config['severity'] == 'ERROR' else Severity.WARNING,
            params=val_config['params']
        )

        # Run validation on database chunks
        data_iterator = db_loader.load_chunks()
        result = validation.validate(data_iterator, context={})
        results.append((val_type, result))

        # Show result
        if result.passed:
            po.success(f"  ✓ {result.message}")
        else:
            po.warning(f"  ✗ {result.message}")

    # Summary
    po.section("Validation Summary")

    passed = sum(1 for _, r in results if r.passed)
    failed = sum(1 for _, r in results if not r.passed)

    po.info(f"Database: {db_path.name}")
    po.info(f"Table: {table_name}")
    po.info(f"Rows: {row_count:,}")
    po.info(f"Validations: {len(results)}")
    po.info(f"Passed: {passed}")
    po.info(f"Failed: {failed}")

    if failed == 0:
        po.success("Status: ✓ ALL VALIDATIONS PASSED")
    else:
        po.warning(f"Status: ✗ {failed} VALIDATIONS FAILED")

    # Show failures
    if failed > 0:
        po.section("Failed Validations")
        for val_type, result in results:
            if not result.passed:
                po.warning(f"{val_type}:")
                po.info(f"  {result.message}")
                if result.sample_failures:
                    po.info(f"  Sample failures (showing up to 3):")
                    for failure in result.sample_failures[:3]:
                        po.info(f"    • Row {failure.get('row', '?')}: {failure.get('message', '')}")

    po.section("Key Points")
    po.info("• Database validations use the same rules as file validations")
    po.info("• Data is processed in chunks for memory efficiency")
    po.info("• 33/35 DataK9 validations work with databases")
    po.info("• Connection string format: sqlite:///path or postgresql://user:pass@host/db")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit(main())
