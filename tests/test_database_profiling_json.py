"""
Test database profiling JSON serialization.

Ensures that profiling results from databases can be serialized to JSON
without numpy/pandas type errors.

Author: Daniel Edge
Date: 2025-11-19
"""

import pytest
import json
import tempfile
from pathlib import Path

from validation_framework.loaders.factory import LoaderFactory
from validation_framework.profiler.engine import DataProfiler


@pytest.fixture
def test_db_path():
    """Path to test database."""
    db_path = Path(__file__).parent.parent / "test_data.db"
    if not db_path.exists():
        pytest.skip("Test database not found. Run scripts/create_test_database.py")
    return db_path


@pytest.mark.integration
class TestDatabaseProfilingJSON:
    """Test JSON serialization of database profiling results."""

    def test_database_profile_json_serializable(self, test_db_path):
        """Test that database profiling results can be serialized to JSON."""
        # Create database loader
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers",
            chunk_size=10000
        )

        # Get sample data
        sample_chunk = next(loader.load_chunks())

        # Create profiler
        profiler = DataProfiler(chunk_size=10000)

        # Profile the dataframe
        profile_result = profiler.profile_dataframe(
            sample_chunk,
            name="customers"
        )

        # Convert to dict
        result_dict = profile_result.to_dict()

        # Test JSON serialization (this would fail with int64 types)
        try:
            json_str = json.dumps(result_dict, indent=2)
            assert len(json_str) > 0
        except TypeError as e:
            pytest.fail(f"JSON serialization failed: {e}")

        # Verify structure
        assert "row_count" in result_dict
        assert "column_count" in result_dict
        assert "columns" in result_dict

        # Verify types are JSON-serializable
        assert isinstance(result_dict["row_count"], int)
        assert isinstance(result_dict["column_count"], int)
        assert isinstance(result_dict["overall_quality_score"], (int, float))

    def test_database_profile_to_json_file(self, test_db_path):
        """Test writing database profile to JSON file."""
        # Create database loader
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers"
        )

        # Get sample data
        sample_chunk = next(loader.load_chunks())

        # Create profiler and profile
        profiler = DataProfiler()
        profile_result = profiler.profile_dataframe(sample_chunk, name="customers")

        # Write to JSON file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json_path = f.name
            try:
                json.dump(profile_result.to_dict(), f, indent=2)
            except TypeError as e:
                pytest.fail(f"Failed to write JSON: {e}")

        # Read it back and verify
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
                assert data["row_count"] > 0
                assert data["column_count"] > 0
        finally:
            # Clean up
            Path(json_path).unlink(missing_ok=True)

    def test_column_statistics_json_serializable(self, test_db_path):
        """Test that column statistics with numeric types are JSON-serializable."""
        # Create database loader
        loader = LoaderFactory.create_database_loader(
            connection_string=f"sqlite:///{test_db_path}",
            table="customers"
        )

        # Get sample data
        sample_chunk = next(loader.load_chunks())

        # Profile
        profiler = DataProfiler()
        profile_result = profiler.profile_dataframe(sample_chunk, name="customers")

        # Get column profiles
        result_dict = profile_result.to_dict()
        columns = result_dict["columns"]

        assert len(columns) > 0

        # Check each column's statistics are JSON-serializable
        for col in columns:
            # This would fail if int64/float64 types weren't converted
            try:
                json.dumps(col)
            except TypeError as e:
                pytest.fail(f"Column {col.get('name')} has non-serializable types: {e}")

            # Verify statistics types
            stats = col.get("statistics", {})
            if "count" in stats:
                assert isinstance(stats["count"], int), f"count is {type(stats['count'])}"
            if "null_count" in stats:
                assert isinstance(stats["null_count"], int), f"null_count is {type(stats['null_count'])}"
            if "unique_count" in stats:
                assert isinstance(stats["unique_count"], int), f"unique_count is {type(stats['unique_count'])}"
            if "mode_frequency" in stats:
                assert isinstance(stats["mode_frequency"], int), f"mode_frequency is {type(stats['mode_frequency'])}"
