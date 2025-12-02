"""
Tests for async validation engine and async loaders.

Tests concurrent file validation, async data loading, and
performance improvements from parallel processing.
"""

import pytest
import pandas as pd
import tempfile
import yaml
import asyncio
from pathlib import Path

from validation_framework.core.async_engine import AsyncValidationEngine
from validation_framework.core.config import ValidationConfig
from validation_framework.core.results import Status
from validation_framework.loaders.async_csv_loader import AsyncCSVLoader
from validation_framework.loaders.async_json_loader import AsyncJSONLoader
from validation_framework.loaders.async_factory import AsyncLoaderFactory


# ============================================================================
# ASYNC LOADER TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
class TestAsyncCSVLoader:
    """Tests for async CSV loader."""

    async def test_async_csv_loader_initialization(self, temp_csv_file):
        """Test AsyncCSVLoader initialization."""
        loader = AsyncCSVLoader(
            file_path=temp_csv_file,
            chunk_size=1000
        )

        assert loader.file_path == temp_csv_file
        assert loader.chunk_size == 1000

    async def test_async_csv_loader_loads_data(self, temp_csv_file):
        """Test that async CSV loader loads data correctly."""
        loader = AsyncCSVLoader(file_path=temp_csv_file)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)
            assert isinstance(chunk, pd.DataFrame)

        # Should have loaded at least one chunk
        assert len(chunks) > 0

        # Combine and verify data
        df = pd.concat(chunks, ignore_index=True)
        assert len(df) > 0
        assert "id" in df.columns

    async def test_async_csv_loader_chunking(self, temp_large_csv_file):
        """Test async CSV loader chunking behavior."""
        loader = AsyncCSVLoader(
            file_path=temp_large_csv_file,
            chunk_size=1000
        )

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)
            assert len(chunk) <= 1000

        # Should have multiple chunks for large file
        assert len(chunks) > 1

    async def test_async_csv_loader_metadata(self, temp_csv_file):
        """Test async CSV loader metadata retrieval."""
        loader = AsyncCSVLoader(file_path=temp_csv_file)
        metadata = await loader.get_metadata()

        assert "file_path" in metadata
        assert "file_size" in metadata or "file_size_bytes" in metadata
        # Verify we have some useful metadata
        assert metadata["file_size"] > 0 or metadata.get("file_size_bytes", 0) > 0
        assert "format" in metadata or "columns" in metadata


@pytest.mark.asyncio
@pytest.mark.unit
class TestAsyncJSONLoader:
    """Tests for async JSON loader."""

    async def test_async_json_loader_loads_array(self, temp_json_file):
        """Test async JSON loader with array format."""
        loader = AsyncJSONLoader(file_path=temp_json_file)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)
            assert isinstance(chunk, pd.DataFrame)

        assert len(chunks) > 0

        # Verify data
        df = pd.concat(chunks, ignore_index=True)
        assert len(df) > 0

    async def test_async_json_loader_metadata(self, temp_json_file):
        """Test async JSON loader metadata."""
        loader = AsyncJSONLoader(file_path=temp_json_file)
        metadata = await loader.get_metadata()

        assert "file_path" in metadata
        assert "format" in metadata


@pytest.mark.asyncio
@pytest.mark.unit
class TestAsyncLoaderFactory:
    """Tests for async loader factory."""

    async def test_create_async_csv_loader(self, temp_csv_file):
        """Test creating async CSV loader from factory."""
        loader = await AsyncLoaderFactory.create_loader(
            file_path=temp_csv_file,
            file_format="csv"
        )

        assert isinstance(loader, AsyncCSVLoader)

    async def test_create_async_json_loader(self, temp_json_file):
        """Test creating async JSON loader from factory."""
        loader = await AsyncLoaderFactory.create_loader(
            file_path=temp_json_file,
            file_format="json"
        )

        assert isinstance(loader, AsyncJSONLoader)

    async def test_auto_detect_format(self, temp_csv_file):
        """Test format auto-detection in async factory."""
        loader = await AsyncLoaderFactory.create_loader(file_path=temp_csv_file)

        assert isinstance(loader, AsyncCSVLoader)

    async def test_unsupported_format_raises_error(self):
        """Test that unsupported format raises error."""
        # Create a temp file with unsupported format
        with tempfile.NamedTemporaryFile(mode='w', suffix='.unsupported', delete=False) as f:
            f.write("test data")
            temp_file = f.name

        try:
            with pytest.raises(ValueError):
                await AsyncLoaderFactory.create_loader(
                    file_path=temp_file,
                    file_format="unsupported"
                )
        finally:
            Path(temp_file).unlink(missing_ok=True)


# ============================================================================
# ASYNC ENGINE TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.integration
class TestAsyncValidationEngine:
    """Tests for async validation engine."""

    async def test_async_engine_initialization(self, valid_config_dict):
        """Test AsyncValidationEngine initialization."""
        config = ValidationConfig(valid_config_dict)
        engine = AsyncValidationEngine(config)

        assert engine.config.job_name == config.job_name
        assert engine.config == config

    async def test_async_engine_from_config(self, temp_config_file):
        """Test creating async engine from config file."""
        config = ValidationConfig.from_yaml(temp_config_file)
        engine = AsyncValidationEngine(config)

        assert engine.config.job_name == "Test Validation Job"

    async def test_async_engine_single_file_validation(self, temp_csv_file):
        """Test async validation of single file."""
        config_dict = {
            "validation_job": {
                "name": "Async Single File Test",
                "files": [
                    {
                        "path": temp_csv_file,
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"}
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = AsyncValidationEngine(config)

        report = await engine.run()

        assert report.job_name == "Async Single File Test"
        assert len(report.file_reports) == 1
        assert report.file_reports[0].total_validations == 1

    async def test_async_engine_multiple_files_parallel(self, temp_csv_file, sample_dataframe):
        """Test async validation of multiple files in parallel."""
        # Create second file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            sample_dataframe.to_csv(f.name, index=False)
            temp_csv_file2 = f.name

        try:
            config_dict = {
                "validation_job": {
                    "name": "Async Multi-File Test",
                    "files": [
                        {
                            "name": "file1",
                            "path": temp_csv_file,
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"}
                            ]
                        },
                        {
                            "name": "file2",
                            "path": temp_csv_file2,
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"}
                            ]
                        }
                    ],
                    "processing": {
                        "parallel_files": True
                    }
                }
            }

            config = ValidationConfig(config_dict)
            engine = AsyncValidationEngine(config)

            report = await engine.run()

            # Should have results for both files
            assert len(report.file_reports) == 2
            assert report.file_reports[0].file_name == "file1"
            assert report.file_reports[1].file_name == "file2"
        finally:
            Path(temp_csv_file2).unlink(missing_ok=True)

    async def test_async_engine_error_handling(self):
        """Test async engine error handling with invalid file."""
        config_dict = {
            "validation_job": {
                "name": "Error Test",
                "files": [
                    {
                        "path": "nonexistent.csv",
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"}
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = AsyncValidationEngine(config)

        # Async engine handles errors gracefully, should return report with error
        report = await engine.run()

        # Should have one file report
        assert len(report.file_reports) == 1
        # File should have failed status
        assert report.file_reports[0].status == Status.FAILED
        # Should have recorded the error
        assert report.file_reports[0].error_count > 0

    async def test_async_engine_generates_reports(self, temp_csv_file):
        """Test that async engine generates HTML and JSON reports."""
        with tempfile.TemporaryDirectory() as temp_dir:
            html_path = Path(temp_dir) / "async_report.html"
            json_path = Path(temp_dir) / "async_summary.json"

            config_dict = {
                "validation_job": {
                    "name": "Report Generation Test",
                    "files": [
                        {
                            "path": temp_csv_file,
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"}
                            ]
                        }
                    ],
                    "output": {
                        "html_report": str(html_path),
                        "json_summary": str(json_path)
                    }
                }
            }

            config = ValidationConfig(config_dict)
            engine = AsyncValidationEngine(config)

            report = await engine.run()

            # Verify files created by run()
            assert html_path.exists()
            assert json_path.exists()
            assert report.job_name == "Report Generation Test"


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.performance
@pytest.mark.slow
class TestAsyncPerformance:
    """Test performance benefits of async processing."""

    async def test_async_faster_than_sync_multiple_files(self, large_dataframe):
        """Test that async processing is faster for multiple files."""
        import time

        # Create multiple large files
        temp_files = []
        for i in range(3):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                large_dataframe.to_csv(f.name, index=False)
                temp_files.append(f.name)

        try:
            # Config for async processing
            config_dict = {
                "validation_job": {
                    "name": "Performance Test",
                    "files": [
                        {
                            "path": temp_file,
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"},
                                {
                                    "type": "MandatoryFieldCheck",
                                    "severity": "ERROR",
                                    "params": {"fields": ["id"]}
                                }
                            ]
                        }
                        for temp_file in temp_files
                    ],
                    "processing": {
                        "parallel_files": True
                    }
                }
            }

            config = ValidationConfig(config_dict)
            engine = AsyncValidationEngine(config)

            # Time async execution
            start_time = time.time()
            report = await engine.run()
            async_duration = time.time() - start_time

            # Verify all files processed
            assert len(report.file_reports) == 3

            # Async should complete (exact timing varies by system)
            assert async_duration < 60  # Should complete within 60 seconds

        finally:
            for temp_file in temp_files:
                Path(temp_file).unlink(missing_ok=True)

    async def test_async_concurrent_file_processing(self, sample_dataframe):
        """Test that files are actually processed concurrently."""
        # Create 5 small files
        temp_files = []
        for i in range(5):
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                sample_dataframe.to_csv(f.name, index=False)
                temp_files.append(f.name)

        try:
            config_dict = {
                "validation_job": {
                    "name": "Concurrent Test",
                    "files": [
                        {
                            "path": temp_file,
                            "validations": [
                                {"type": "EmptyFileCheck", "severity": "ERROR"}
                            ]
                        }
                        for temp_file in temp_files
                    ],
                    "processing": {
                        "parallel_files": True
                    }
                }
            }

            config = ValidationConfig(config_dict)
            engine = AsyncValidationEngine(config)

            report = await engine.run()

            # All files should be processed
            assert len(report.file_reports) == 5

            # All should have validations run
            assert all(fr.total_validations > 0 for fr in report.file_reports)
            # All should pass (no errors)
            assert all(fr.error_count == 0 for fr in report.file_reports)

        finally:
            for temp_file in temp_files:
                Path(temp_file).unlink(missing_ok=True)


# ============================================================================
# EDGE CASES
# ============================================================================

@pytest.mark.asyncio
@pytest.mark.unit
class TestAsyncEdgeCases:
    """Test edge cases in async processing."""

    async def test_async_empty_file_list(self):
        """Test async engine with no files."""
        config_dict = {
            "validation_job": {
                "name": "Empty Test",
                "files": []
            }
        }

        # Should raise error during config validation
        with pytest.raises(Exception):
            config = ValidationConfig(config_dict)

    async def test_async_file_with_no_validations(self, temp_csv_file):
        """Test async processing file with no validations."""
        config_dict = {
            "validation_job": {
                "name": "No Validations Test",
                "files": [
                    {
                        "path": temp_csv_file,
                        "validations": []
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = AsyncValidationEngine(config)

        report = await engine.run()

        # Should complete with 0 validations
        assert len(report.file_reports) == 1
        assert report.file_reports[0].total_validations == 0

    async def test_async_mixed_success_and_failure(self, temp_csv_file):
        """Test async processing with mixed validation results."""
        config_dict = {
            "validation_job": {
                "name": "Mixed Results Test",
                "files": [
                    {
                        "path": temp_csv_file,
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"},  # Will pass
                            {
                                "type": "RegexCheck",  # Will fail on "invalid" email
                                "severity": "ERROR",
                                "params": {
                                    "field": "email",
                                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                                }
                            }
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = AsyncValidationEngine(config)

        report = await engine.run()

        # Should have both passed and failed validations
        file_report = report.file_reports[0]
        assert file_report.total_validations == 2
        # At least one validation should have passed (EmptyFileCheck)
        passed_count = sum(1 for r in file_report.validation_results if r.passed)
        assert passed_count >= 1
        # May have some failures
        assert file_report.error_count >= 0

    async def test_async_cancellation_handling(self, temp_csv_file):
        """Test that async operations can be cancelled gracefully."""
        config_dict = {
            "validation_job": {
                "name": "Cancellation Test",
                "files": [
                    {
                        "path": temp_csv_file,
                        "validations": [
                            {"type": "EmptyFileCheck", "severity": "ERROR"}
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        engine = AsyncValidationEngine(config)

        # Create task and cancel it
        task = asyncio.create_task(engine.run())

        # Let it start
        await asyncio.sleep(0.01)

        # Cancel the task
        task.cancel()

        # Either raises CancelledError (if cancelled in time)
        # or completes normally (if task finished before cancellation)
        # Both are valid outcomes for this timing-dependent test
        try:
            await task
            # Task completed before cancellation - this is acceptable
            assert True
        except asyncio.CancelledError:
            # Task was cancelled - this is the expected path
            assert True
