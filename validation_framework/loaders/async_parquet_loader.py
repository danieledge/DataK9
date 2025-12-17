"""
Async Parquet data loader.

Provides non-blocking Parquet file loading with chunked processing for
memory-efficient validation of large Parquet files.
"""

from typing import AsyncIterator, Dict, Any
import pandas as pd
import asyncio
from validation_framework.loaders.async_base import AsyncFileLoader
import logging

logger = logging.getLogger(__name__)

try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pq = None


class AsyncParquetLoader(AsyncFileLoader):
    """
    Async loader for Parquet files.

    Uses asyncio to enable concurrent file processing and non-blocking I/O.
    PyArrow Parquet operations are executed in a thread pool to avoid blocking
    the event loop.

    Features:
    - Concurrent processing of multiple Parquet files
    - Chunked reading for memory efficiency
    - Efficient columnar storage access
    - Non-blocking I/O operations
    - Fast metadata access without loading data
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        columns: list = None,
        **kwargs
    ):
        """
        Initialize async Parquet loader.

        Args:
            file_path: Path to Parquet file
            chunk_size: Number of rows per chunk (default: 50,000)
            columns: Optional list of columns to read (None reads all)
            **kwargs: Additional parameters passed to PyArrow
        """
        super().__init__(file_path, chunk_size)
        self.columns = columns
        self.kwargs = kwargs

    async def load(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously load Parquet data in chunks.

        Yields DataFrame chunks without blocking the event loop, allowing
        concurrent processing of multiple files.

        Yields:
            DataFrame chunks

        Raises:
            RuntimeError: If PyArrow is not installed
            FileNotFoundError: If the file does not exist

        Example:
            >>> loader = AsyncParquetLoader('data.parquet', chunk_size=10000)
            >>> async for chunk in loader.load():
            ...     print(f"Processing {len(chunk)} rows")
        """
        if not HAS_PYARROW:
            raise RuntimeError(
                "PyArrow is required for Parquet support but is not installed. "
                "Install it with: pip install pyarrow"
            )

        if not await self.file_exists():
            raise FileNotFoundError(f"Parquet file not found: {self.file_path}")

        logger.info(f"Async loading Parquet file: {self.file_path}")

        # Run PyArrow operations in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()

        def _read_parquet_batches():
            """Synchronous function to create Parquet file reader."""
            return pq.ParquetFile(self.file_path)

        # Get the Parquet file object
        parquet_file = await loop.run_in_executor(None, _read_parquet_batches)

        # Iterate through batches asynchronously
        chunk_count = 0

        def _read_batch(batch_iter, batch_num):
            """Read a single batch from the iterator."""
            try:
                batch = next(batch_iter)
                return batch.to_pandas()
            except StopIteration:
                return None

        # Create batch iterator
        batch_iterator = parquet_file.iter_batches(
            batch_size=self.chunk_size,
            columns=self.columns
        )

        while True:
            # Read batch in thread pool
            chunk = await loop.run_in_executor(None, _read_batch, batch_iterator, chunk_count)

            if chunk is None:
                break

            # Yield control to event loop between chunks
            await asyncio.sleep(0)
            chunk_count += 1
            logger.debug(f"Loaded chunk {chunk_count}: {len(chunk)} rows")
            yield chunk

        logger.info(f"Completed loading {chunk_count} chunks from {self.file_path}")

    async def get_metadata(self) -> Dict[str, Any]:
        """
        Asynchronously retrieve Parquet file metadata.

        Parquet files store rich metadata including schema, row count,
        and column statistics, which can be read without loading the data.

        Returns:
            Dictionary containing file metadata including row count,
            column count, columns, file size, data types, and Parquet-specific
            information like row groups and compression
        """
        base_metadata = await self.get_base_metadata()

        if not HAS_PYARROW:
            return {
                **base_metadata,
                "error": "PyArrow not installed - cannot read Parquet metadata"
            }

        # Run PyArrow operations in thread pool
        loop = asyncio.get_event_loop()

        def _get_parquet_info():
            """Synchronous function to get Parquet metadata."""
            # Read Parquet metadata (very fast, doesn't load data)
            parquet_file = pq.ParquetFile(self.file_path)
            schema = parquet_file.schema_arrow

            metadata = {
                "columns": schema.names,
                "column_count": len(schema.names),
                "row_count": parquet_file.metadata.num_rows,
                "dtypes": {
                    name: str(schema.field(name).type)
                    for name in schema.names
                },
                "num_row_groups": parquet_file.metadata.num_row_groups,
                "num_columns": parquet_file.metadata.num_columns,
            }

            # Compression information (from first row group)
            if parquet_file.metadata.num_row_groups > 0:
                first_row_group = parquet_file.metadata.row_group(0)
                if first_row_group.num_columns > 0:
                    compression = first_row_group.column(0).compression
                    metadata["compression"] = compression

            return metadata

        try:
            parquet_info = await loop.run_in_executor(None, _get_parquet_info)
            return {
                **base_metadata,
                **parquet_info,
            }
        except Exception as e:
            logger.error(f"Error reading Parquet metadata: {str(e)}")
            return {
                **base_metadata,
                "error": f"Could not read Parquet metadata: {str(e)}"
            }


async def create_async_parquet_loader(
    file_path: str,
    chunk_size: int = 50000,
    **kwargs
) -> AsyncParquetLoader:
    """
    Factory function to create async Parquet loader.

    Args:
        file_path: Path to Parquet file
        chunk_size: Number of rows per chunk
        **kwargs: Additional parameters for AsyncParquetLoader

    Returns:
        AsyncParquetLoader instance

    Example:
        >>> loader = await create_async_parquet_loader('data.parquet')
        >>> async for chunk in loader.load():
        ...     await process(chunk)
    """
    return AsyncParquetLoader(file_path, chunk_size, **kwargs)
