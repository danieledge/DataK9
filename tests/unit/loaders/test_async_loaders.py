"""
Comprehensive tests for async file loaders.

Author: Daniel Edge
"""

import pytest
import pandas as pd
import asyncio
import os
import tempfile
from pathlib import Path

from validation_framework.loaders.async_parquet_loader import AsyncParquetLoader, create_async_parquet_loader
from validation_framework.loaders.async_excel_loader import AsyncExcelLoader, create_async_excel_loader
from validation_framework.loaders.async_csv_loader import AsyncCSVLoader
from validation_framework.loaders.async_json_loader import AsyncJSONLoader

# Check for optional dependencies
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


class TestAsyncParquetLoader:
    """Test async Parquet file loader."""

    @pytest.fixture
    def temp_parquet_file(self):
        """Create a temporary Parquet file for testing."""
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Name_{i}' for i in range(1, 101)],
            'value': [i * 10.5 for i in range(1, 101)],
            'category': ['A' if i % 2 == 0 else 'B' for i in range(1, 101)]
        })

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name, engine='pyarrow')
            temp_path = f.name

        yield temp_path

        # Cleanup
        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def large_parquet_file(self):
        """Create a large Parquet file for chunked reading tests."""
        df = pd.DataFrame({
            'id': range(1, 10001),
            'value': range(10000, 20000),
            'text': [f'Text_{i}' for i in range(1, 10001)]
        })

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name, engine='pyarrow')
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_load_basic(self, temp_parquet_file):
        """Test basic Parquet file loading."""
        loader = AsyncParquetLoader(temp_parquet_file, chunk_size=50)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        # Should have 2 chunks (100 rows / 50 chunk_size)
        assert len(chunks) == 2
        assert len(chunks[0]) == 50
        assert len(chunks[1]) == 50

        # Verify data
        all_data = pd.concat(chunks, ignore_index=True)
        assert len(all_data) == 100
        assert list(all_data.columns) == ['id', 'name', 'value', 'category']

    @pytest.mark.asyncio
    async def test_load_with_column_selection(self, temp_parquet_file):
        """Test loading specific columns only."""
        loader = AsyncParquetLoader(temp_parquet_file, columns=['id', 'name'])

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        all_data = pd.concat(chunks, ignore_index=True)
        assert list(all_data.columns) == ['id', 'name']
        assert 'value' not in all_data.columns
        assert 'category' not in all_data.columns

    @pytest.mark.asyncio
    async def test_load_large_file_chunked(self, large_parquet_file):
        """Test chunked loading of large file."""
        chunk_size = 2000
        loader = AsyncParquetLoader(large_parquet_file, chunk_size=chunk_size)

        chunks = []
        total_rows = 0

        async for chunk in loader.load():
            chunks.append(chunk)
            total_rows += len(chunk)
            assert len(chunk) <= chunk_size

        # Should have 5 chunks (10000 / 2000)
        assert len(chunks) == 5
        assert total_rows == 10000

    @pytest.mark.asyncio
    async def test_get_metadata(self, temp_parquet_file):
        """Test metadata extraction."""
        loader = AsyncParquetLoader(temp_parquet_file)
        metadata = await loader.get_metadata()

        assert metadata['row_count'] == 100
        assert metadata['column_count'] == 4
        assert 'columns' in metadata
        assert set(metadata['columns']) == {'id', 'name', 'value', 'category'}
        assert 'dtypes' in metadata
        assert 'num_row_groups' in metadata
        assert 'file_size' in metadata

    @pytest.mark.asyncio
    async def test_file_not_found(self):
        """Test handling of non-existent file."""
        loader = AsyncParquetLoader('/nonexistent/file.parquet')

        with pytest.raises(FileNotFoundError):
            async for _ in loader.load():
                pass

    @pytest.mark.asyncio
    async def test_file_exists_check(self, temp_parquet_file):
        """Test file existence check."""
        loader = AsyncParquetLoader(temp_parquet_file)
        assert await loader.file_exists() is True

        loader_missing = AsyncParquetLoader('/nonexistent.parquet')
        assert await loader_missing.file_exists() is False

    @pytest.mark.asyncio
    async def test_factory_function(self, temp_parquet_file):
        """Test factory function for creating loader."""
        loader = await create_async_parquet_loader(temp_parquet_file, chunk_size=25)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        assert len(chunks) == 4  # 100 rows / 25 chunk_size

    @pytest.mark.asyncio
    async def test_concurrent_loading(self, temp_parquet_file, large_parquet_file):
        """Test concurrent loading of multiple files."""
        loader1 = AsyncParquetLoader(temp_parquet_file, chunk_size=50)
        loader2 = AsyncParquetLoader(large_parquet_file, chunk_size=1000)

        async def load_file(loader):
            chunks = []
            async for chunk in loader.load():
                chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)

        # Load both files concurrently
        results = await asyncio.gather(
            load_file(loader1),
            load_file(loader2)
        )

        assert len(results[0]) == 100
        assert len(results[1]) == 10000

    @pytest.mark.asyncio
    async def test_empty_parquet_file(self):
        """Test loading empty Parquet file."""
        df = pd.DataFrame(columns=['id', 'name', 'value'])

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name, engine='pyarrow')
            temp_path = f.name

        try:
            loader = AsyncParquetLoader(temp_path)
            chunks = []
            async for chunk in loader.load():
                chunks.append(chunk)

            # Empty file should yield no chunks or one empty chunk
            if chunks:
                all_data = pd.concat(chunks, ignore_index=True)
                assert len(all_data) == 0
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_metadata_with_compression(self):
        """Test metadata extraction with compression info."""
        df = pd.DataFrame({
            'id': range(1000),
            'data': [f'Data_{i}' for i in range(1000)]
        })

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name, engine='pyarrow', compression='snappy')
            temp_path = f.name

        try:
            loader = AsyncParquetLoader(temp_path)
            metadata = await loader.get_metadata()

            assert 'compression' in metadata
            assert metadata['num_row_groups'] > 0
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)


@pytest.mark.skipif(not HAS_OPENPYXL, reason="openpyxl not installed")
class TestAsyncExcelLoader:
    """Test async Excel file loader."""

    @pytest.fixture
    def temp_excel_file(self):
        """Create a temporary Excel file for testing."""
        df = pd.DataFrame({
            'id': range(1, 51),
            'name': [f'Name_{i}' for i in range(1, 51)],
            'value': [i * 2.5 for i in range(1, 51)]
        })

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as f:
            df.to_excel(f.name, index=False, sheet_name='Sheet1', engine='openpyxl')
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.fixture
    def multi_sheet_excel(self):
        """Create Excel file with multiple sheets."""
        df1 = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})
        df2 = pd.DataFrame({'name': ['A', 'B', 'C'], 'score': [100, 200, 300]})

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.xlsx', delete=False) as f:
            with pd.ExcelWriter(f.name, engine='openpyxl') as writer:
                df1.to_excel(writer, sheet_name='Data', index=False)
                df2.to_excel(writer, sheet_name='Scores', index=False)
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_load_basic(self, temp_excel_file):
        """Test basic Excel file loading."""
        loader = AsyncExcelLoader(temp_excel_file, chunk_size=20)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        # Should have 3 chunks (50 rows / 20 chunk_size)
        assert len(chunks) == 3

        all_data = pd.concat(chunks, ignore_index=True)
        assert len(all_data) == 50
        assert list(all_data.columns) == ['id', 'name', 'value']

    @pytest.mark.asyncio
    async def test_load_specific_sheet(self, multi_sheet_excel):
        """Test loading specific sheet by name."""
        loader = AsyncExcelLoader(multi_sheet_excel, sheet_name='Scores')

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        all_data = pd.concat(chunks, ignore_index=True)
        assert len(all_data) == 3
        assert list(all_data.columns) == ['name', 'score']

    @pytest.mark.asyncio
    async def test_load_sheet_by_index(self, multi_sheet_excel):
        """Test loading specific sheet by index."""
        loader = AsyncExcelLoader(multi_sheet_excel, sheet_name=0)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        all_data = pd.concat(chunks, ignore_index=True)
        assert list(all_data.columns) == ['id', 'value']

    @pytest.mark.asyncio
    async def test_get_metadata(self, temp_excel_file):
        """Test metadata extraction from Excel file."""
        loader = AsyncExcelLoader(temp_excel_file)
        metadata = await loader.get_metadata()

        assert metadata['row_count'] == 50
        assert metadata['column_count'] == 3
        assert 'columns' in metadata
        assert 'dtypes' in metadata
        assert metadata['sheet_name'] == 'Sheet1'

    @pytest.mark.asyncio
    async def test_get_metadata_multi_sheet(self, multi_sheet_excel):
        """Test metadata extraction from multi-sheet Excel."""
        loader = AsyncExcelLoader(multi_sheet_excel)
        metadata = await loader.get_metadata()

        assert 'available_sheets' in metadata
        assert len(metadata['available_sheets']) == 2
        assert 'Data' in metadata['available_sheets']
        assert 'Scores' in metadata['available_sheets']

    @pytest.mark.asyncio
    async def test_file_not_found(self):
        """Test handling of non-existent Excel file."""
        loader = AsyncExcelLoader('/nonexistent/file.xlsx')

        with pytest.raises(FileNotFoundError):
            async for _ in loader.load():
                pass

    @pytest.mark.asyncio
    async def test_factory_function(self, temp_excel_file):
        """Test factory function."""
        loader = await create_async_excel_loader(temp_excel_file, chunk_size=10)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        assert len(chunks) == 5  # 50 rows / 10 chunk_size

    @pytest.mark.asyncio
    async def test_concurrent_excel_loading(self, temp_excel_file, multi_sheet_excel):
        """Test concurrent loading of multiple Excel files."""
        loader1 = AsyncExcelLoader(temp_excel_file)
        loader2 = AsyncExcelLoader(multi_sheet_excel, sheet_name='Data')

        async def load_all(loader):
            chunks = []
            async for chunk in loader.load():
                chunks.append(chunk)
            return pd.concat(chunks, ignore_index=True)

        results = await asyncio.gather(
            load_all(loader1),
            load_all(loader2)
        )

        assert len(results[0]) == 50
        assert len(results[1]) == 3


class TestAsyncCSVLoader:
    """Test async CSV file loader."""

    @pytest.fixture
    def temp_csv_file(self):
        """Create a temporary CSV file for testing."""
        df = pd.DataFrame({
            'id': range(1, 101),
            'name': [f'Name_{i}' for i in range(1, 101)],
            'value': [i * 1.5 for i in range(1, 101)]
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_load_basic(self, temp_csv_file):
        """Test basic CSV loading."""
        loader = AsyncCSVLoader(temp_csv_file, chunk_size=25)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        assert len(chunks) == 4  # 100 rows / 25 chunk_size

        all_data = pd.concat(chunks, ignore_index=True)
        assert len(all_data) == 100

    @pytest.mark.asyncio
    async def test_get_metadata(self, temp_csv_file):
        """Test CSV metadata extraction."""
        loader = AsyncCSVLoader(temp_csv_file)
        metadata = await loader.get_metadata()

        assert 'columns' in metadata
        assert 'column_count' in metadata
        assert metadata['column_count'] == 3

    @pytest.mark.asyncio
    async def test_file_not_found(self):
        """Test CSV file not found."""
        loader = AsyncCSVLoader('/nonexistent/file.csv')

        with pytest.raises(FileNotFoundError):
            async for _ in loader.load():
                pass


class TestAsyncJSONLoader:
    """Test async JSON file loader."""

    @pytest.fixture
    def temp_json_file(self):
        """Create a temporary JSON file for testing."""
        df = pd.DataFrame({
            'id': range(1, 51),
            'name': [f'Name_{i}' for i in range(1, 51)],
            'value': [i * 3.0 for i in range(1, 51)]
        })

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            df.to_json(f.name, orient='records', lines=True)
            temp_path = f.name

        yield temp_path

        if os.path.exists(temp_path):
            os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_load_basic(self, temp_json_file):
        """Test basic JSON loading."""
        loader = AsyncJSONLoader(temp_json_file, chunk_size=20)

        chunks = []
        async for chunk in loader.load():
            chunks.append(chunk)

        all_data = pd.concat(chunks, ignore_index=True)
        assert len(all_data) == 50

    @pytest.mark.asyncio
    async def test_get_metadata(self, temp_json_file):
        """Test JSON metadata extraction."""
        loader = AsyncJSONLoader(temp_json_file)
        metadata = await loader.get_metadata()

        assert 'columns' in metadata
        assert metadata['column_count'] == 3

    @pytest.mark.asyncio
    async def test_file_not_found(self):
        """Test JSON file not found."""
        loader = AsyncJSONLoader('/nonexistent/file.json')

        with pytest.raises(FileNotFoundError):
            async for _ in loader.load():
                pass


class TestAsyncLoaderEdgeCases:
    """Test edge cases and error handling for async loaders."""

    @pytest.mark.asyncio
    async def test_very_small_chunk_size(self):
        """Test with chunk_size=1."""
        df = pd.DataFrame({'id': [1, 2, 3, 4, 5]})

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name, engine='pyarrow')
            temp_path = f.name

        try:
            loader = AsyncParquetLoader(temp_path, chunk_size=1)
            chunks = []
            async for chunk in loader.load():
                chunks.append(chunk)

            assert len(chunks) == 5
            for chunk in chunks:
                assert len(chunk) == 1
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_very_large_chunk_size(self):
        """Test with chunk_size larger than file."""
        df = pd.DataFrame({'id': range(100)})

        with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as f:
            df.to_parquet(f.name, engine='pyarrow')
            temp_path = f.name

        try:
            loader = AsyncParquetLoader(temp_path, chunk_size=10000)
            chunks = []
            async for chunk in loader.load():
                chunks.append(chunk)

            # Should get one chunk with all data
            assert len(chunks) == 1
            assert len(chunks[0]) == 100
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    @pytest.mark.asyncio
    async def test_concurrent_multiple_file_types(self):
        """Test concurrent loading of different file types."""
        # Create test files
        df = pd.DataFrame({'id': range(10), 'value': range(10, 20)})

        csv_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        df.to_csv(csv_file.name, index=False)
        csv_file.close()

        parquet_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False)
        df.to_parquet(parquet_file.name, engine='pyarrow')
        parquet_file.close()

        try:
            csv_loader = AsyncCSVLoader(csv_file.name)
            parquet_loader = AsyncParquetLoader(parquet_file.name)

            async def load_all(loader):
                chunks = []
                async for chunk in loader.load():
                    chunks.append(chunk)
                return pd.concat(chunks, ignore_index=True)

            results = await asyncio.gather(
                load_all(csv_loader),
                load_all(parquet_loader)
            )

            assert len(results[0]) == 10
            assert len(results[1]) == 10
        finally:
            os.unlink(csv_file.name)
            os.unlink(parquet_file.name)
