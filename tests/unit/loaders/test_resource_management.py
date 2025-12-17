"""
Tests for resource management in loaders.

Verifies that file handles are properly closed and resources are cleaned up.
"""

import pytest
import tempfile
import pandas as pd
from pathlib import Path
import gc
import psutil
import os

from validation_framework.loaders.async_csv_loader import AsyncCSVLoader


@pytest.mark.unit
class TestResourceManagement:
    """Test proper resource management in loaders."""

    @pytest.mark.asyncio
    async def test_async_csv_loader_closes_file_handles_in_metadata(self):
        """Test that AsyncCSVLoader properly closes file handle in get_metadata."""
        # Create a temporary CSV file
        df = pd.DataFrame({
            "id": range(100),
            "value": range(100, 200)
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            # Get process for monitoring open files
            process = psutil.Process(os.getpid())
            
            # Get initial open file count
            initial_fds = len(process.open_files())

            # Create loader and get metadata multiple times
            loader = AsyncCSVLoader(file_path=temp_path)
            
            for _ in range(10):
                metadata = await loader.get_metadata()
                assert metadata["row_count"] == 100
                assert "columns" in metadata

            # Force garbage collection
            gc.collect()

            # Get final open file count
            final_fds = len(process.open_files())

            # File descriptor count should not increase significantly
            # (allow small variance for system operations)
            assert final_fds - initial_fds <= 2, \
                f"File descriptor leak detected: {initial_fds} -> {final_fds}"

        finally:
            Path(temp_path).unlink()

    @pytest.mark.asyncio
    async def test_async_csv_loader_metadata_with_large_file(self):
        """Test metadata retrieval with larger file to ensure proper resource cleanup."""
        # Create a larger temporary CSV file
        df = pd.DataFrame({
            "id": range(10000),
            "value": range(10000, 20000)
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            loader = AsyncCSVLoader(file_path=temp_path)
            
            # Get metadata - should handle file properly
            metadata = await loader.get_metadata()

            assert metadata["row_count"] == 10000
            assert "id" in metadata["columns"]
            assert "value" in metadata["columns"]

            # Verify file can be deleted (no file handle leak)
            # In some systems, this may require a small delay
            import asyncio
            await asyncio.sleep(0.1)
            
            # File should be closable/deletable
            Path(temp_path).unlink()

        except Exception as e:
            # Cleanup on failure
            if Path(temp_path).exists():
                Path(temp_path).unlink()
            raise

    @pytest.mark.asyncio  
    async def test_async_csv_loader_load_and_metadata_no_interference(self):
        """Test that load() and get_metadata() don't interfere with each other."""
        df = pd.DataFrame({
            "id": range(1000),
            "value": range(1000, 2000)
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            loader = AsyncCSVLoader(file_path=temp_path, chunk_size=100)

            # Get metadata first
            metadata = await loader.get_metadata()
            assert metadata["row_count"] == 1000

            # Then load data
            chunks = []
            async for chunk in loader.load():
                chunks.append(chunk)
            
            assert len(chunks) == 10  # 1000 / 100 = 10 chunks

            # Get metadata again
            metadata2 = await loader.get_metadata()
            assert metadata2["row_count"] == 1000

            # Both operations should work without interference
            assert metadata == metadata2

        finally:
            Path(temp_path).unlink()
