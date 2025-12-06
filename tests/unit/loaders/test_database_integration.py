"""
Test database integration with validation framework.

These tests demonstrate that validations work identically whether
the data source is a file or a database.

Author: Daniel Edge
Date: 2025-11-19
"""

import pytest
import sqlite3
import pandas as pd
from pathlib import Path

from validation_framework.loaders.factory import LoaderFactory
from validation_framework.core.registry import get_registry
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck
from validation_framework.validations.builtin.record_checks import UniqueKeyCheck, DuplicateRowCheck
from validation_framework.validations.builtin.advanced_checks import CompletenessCheck
from validation_framework.utils.definition_loader import get_definition_loader


@pytest.fixture(scope="module")
def test_db_path(tmp_path_factory):
    """Create test database with sample data."""
    db_path = tmp_path_factory.mktemp("data") / "test.db"
    conn = sqlite3.connect(str(db_path))

    # Create test table with known data quality issues
    df = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'email': [
            'user1@example.com',
            'user2@example.com',
            None,  # Missing email
            'user4@example.com',
            '',    # Empty email
            'user6@example.com',
            'user7@example.com',
            'user8@example.com',
            None,  # Missing email
            'user10@example.com'
        ],
        'phone': [f'+1555{i:07d}' for i in range(1, 11)],
        'status': ['ACTIVE', 'ACTIVE', 'PENDING', 'SUSPENDED', 'ACTIVE',
                   'ACTIVE', 'INACTIVE', 'ACTIVE', 'PENDING', 'ACTIVE']
    })

    df.to_sql('customers', conn, if_exists='replace', index=False)
    conn.close()

    return str(db_path)


@pytest.fixture(scope="module")
def test_csv_path(tmp_path_factory, test_db_path):
    """Create equivalent CSV file for comparison."""
    csv_path = tmp_path_factory.mktemp("data") / "customers.csv"

    # Read data from database and write to CSV
    conn = sqlite3.connect(test_db_path)
    df = pd.read_sql("SELECT * FROM customers", conn)
    conn.close()

    df.to_csv(csv_path, index=False)
    return str(csv_path)


class TestDatabaseLoaderBasics:
    """Test database loader basic functionality."""

    def test_database_loader_creation(self, test_db_path):
        """Test creating database loader via factory."""
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers",
            chunk_size=5
        )

        assert loader is not None
        assert loader.table == "customers"
        assert loader.chunk_size == 5
        assert loader.db_type == "sqlite"

    def test_database_loader_chunks(self, test_db_path):
        """Test loading data in chunks."""
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers",
            chunk_size=5
        )

        chunks = list(loader.load_chunks())

        # Should have 2 chunks (10 rows / 5 per chunk)
        assert len(chunks) == 2

        # Each chunk should be a DataFrame
        for chunk in chunks:
            assert isinstance(chunk, pd.DataFrame)

        # First chunk should have 5 rows
        assert len(chunks[0]) == 5

        # Second chunk should have 5 rows
        assert len(chunks[1]) == 5

    def test_database_loader_row_count(self, test_db_path):
        """Test getting row count."""
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers"
        )

        count = loader.get_row_count()
        assert count == 10


class TestValidationFileVsDatabase:
    """Test that validations work identically on files and databases."""

    def test_mandatory_field_check_database(self, test_db_path):
        """Test MandatoryFieldCheck on database."""
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers"
        )

        validation = MandatoryFieldCheck(
            name="EmailRequired",
            severity="ERROR",
            params={"fields": ["email"]}
        )

        result = validation.validate(loader.load_chunks(), context={})

        # Should fail (2 missing emails: rows 3 and 9, plus 1 empty at row 5)
        assert not result.passed
        assert result.failed_count == 3

    def test_mandatory_field_check_csv(self, test_csv_path):
        """Test MandatoryFieldCheck on CSV for comparison."""
        loader = LoaderFactory.create_loader(
            test_csv_path,
            file_format="csv"
        )

        validation = MandatoryFieldCheck(
            name="EmailRequired",
            severity="ERROR",
            params={"fields": ["email"]}
        )

        result = validation.validate(loader.load(), context={})

        # Should have same result as database
        assert not result.passed
        assert result.failed_count == 3

    def test_completeness_check_database(self, test_db_path):
        """Test CompletenessCheck on database."""
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers"
        )

        validation = CompletenessCheck(
            name="EmailCompleteness",
            severity="WARNING",
            params={"field": "email", "min_completeness": 90}
        )

        result = validation.validate(loader.load_chunks(), context={})

        # 70% complete (7 out of 10 valid) - should fail
        assert not result.passed

    def test_unique_key_check_database(self, test_db_path):
        """Test UniqueKeyCheck on database."""
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers"
        )

        validation = UniqueKeyCheck(
            name="UniqueCustomerID",
            severity="ERROR",
            params={"fields": ["customer_id"]}  # Use 'fields' parameter
        )

        result = validation.validate(loader.load_chunks(), context={})

        # All customer_ids are unique - should pass
        assert result.passed


class TestSourceCompatibility:
    """Test source compatibility metadata system."""

    def test_loader_knows_source_compatibility(self):
        """Test that loader provides source compatibility info."""
        # Create fresh loader to avoid singleton cache issues
        from pathlib import Path
        from validation_framework.utils.definition_loader import ValidationDefinitionLoader
        def_file = Path(__file__).parent.parent.parent.parent / "validation_framework" / "validation_definitions.json"
        loader = ValidationDefinitionLoader(def_file)

        # MandatoryFieldCheck should work on both
        assert loader.is_compatible_with("MandatoryFieldCheck", "file")
        assert loader.is_compatible_with("MandatoryFieldCheck", "database")

        # FileSizeCheck should only work on files
        assert loader.is_compatible_with("FileSizeCheck", "file")
        assert not loader.is_compatible_with("FileSizeCheck", "database")

        # SQLCustomCheck should only work on databases
        assert not loader.is_compatible_with("SQLCustomCheck", "file")
        assert loader.is_compatible_with("SQLCustomCheck", "database")

    def test_get_database_compatible_validations(self):
        """Test filtering validations by database compatibility."""
        # Create fresh loader
        from pathlib import Path
        from validation_framework.utils.definition_loader import ValidationDefinitionLoader
        def_file = Path(__file__).parent.parent.parent.parent / "validation_framework" / "validation_definitions.json"
        loader = ValidationDefinitionLoader(def_file)

        db_compat = loader.get_by_source_compatibility("database")

        # Should have 33 database-compatible validations
        assert len(db_compat) == 33

        # Should include MandatoryFieldCheck
        assert "MandatoryFieldCheck" in db_compat

        # Should NOT include FileSizeCheck
        assert "FileSizeCheck" not in db_compat

    def test_compatibility_summary(self):
        """Test getting compatibility summary."""
        # Create fresh loader
        from pathlib import Path
        from validation_framework.utils.definition_loader import ValidationDefinitionLoader
        def_file = Path(__file__).parent.parent.parent.parent / "validation_framework" / "validation_definitions.json"
        loader = ValidationDefinitionLoader(def_file)

        summary = loader.get_compatibility_summary()

        assert summary['total'] == 36
        assert summary['file_compatible'] == 33
        assert summary['database_compatible'] == 33
        assert summary['both_compatible'] == 30
        assert summary['file_only'] == 3
        assert summary['database_only'] == 3


class TestDatabaseOnlyValidations:
    """Test database-specific validations."""

    def test_sql_custom_check_exists(self):
        """Test that SQLCustomCheck is registered."""
        registry = get_registry()
        validation_class = registry.get("SQLCustomCheck")
        assert validation_class is not None

    def test_database_referential_integrity_exists(self):
        """Test that DatabaseReferentialIntegrityCheck is registered."""
        registry = get_registry()
        validation_class = registry.get("DatabaseReferentialIntegrityCheck")
        assert validation_class is not None

    def test_database_constraint_check_exists(self):
        """Test that DatabaseConstraintCheck is registered."""
        registry = get_registry()
        validation_class = registry.get("DatabaseConstraintCheck")
        assert validation_class is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
