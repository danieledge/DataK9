"""
Async CSV data loader.

Provides non-blocking CSV file loading with chunked processing for
memory-efficient validation of large CSV files.
"""

from typing import AsyncIterator, Dict, Any
import pandas as pd
import asyncio
from validation_framework.loaders.async_base import AsyncFileLoader
import logging

logger = logging.getLogger(__name__)


class AsyncCSVLoader(AsyncFileLoader):
    """
    Async loader for CSV and delimited text files.

    Uses asyncio to enable concurrent file processing and non-blocking I/O.
    Pandas read_csv operations are executed in a thread pool to avoid blocking
    the event loop.

    Features:
    - Concurrent processing of multiple CSV files
    - Chunked reading for memory efficiency
    - Support for custom delimiters, encodings
    - Non-blocking I/O operations
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        delimiter: str = ",",
        encoding: str = "utf-8",
        header: int = 0,
        **kwargs
    ):
        """
        Initialize async CSV loader.

        Args:
            file_path: Path to CSV file
            chunk_size: Number of rows per chunk (default: 50,000)
            delimiter: Column delimiter character (default: ',')
            encoding: File encoding (default: 'utf-8')
            header: Row number to use as column names (default: 0)
            **kwargs: Additional parameters passed to pandas read_csv
        """
        super().__init__(file_path, chunk_size)
        self.delimiter = delimiter
        self.encoding = encoding
        self.header = header
        self.kwargs = kwargs

    async def load(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously load CSV data in chunks.

        Yields DataFrame chunks without blocking the event loop, allowing
        concurrent processing of multiple files.

        Yields:
            DataFrame chunks

        Example:
            >>> loader = AsyncCSVLoader('data.csv', chunk_size=10000)
            >>> async for chunk in loader.load():
            ...     print(f"Processing {len(chunk)} rows")
        """
        if not await self.file_exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        logger.info(f"Async loading CSV file: {self.file_path}")

        # Run pandas read_csv in thread pool to avoid blocking event loop
        # pandas operations are CPU-bound and synchronous, so we use to_thread
        loop = asyncio.get_event_loop()

        def _read_csv_chunks():
            """Synchronous function to read CSV in chunks."""
            return pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                header=self.header,
                chunksize=self.chunk_size,
                **self.kwargs
            )

        # Get the chunk iterator
        chunk_iterator = await loop.run_in_executor(None, _read_csv_chunks)

        # Yield chunks asynchronously
        chunk_count = 0
        for chunk in chunk_iterator:
            # Yield control to event loop between chunks
            await asyncio.sleep(0)
            chunk_count += 1
            logger.debug(f"Loaded chunk {chunk_count}: {len(chunk)} rows")
            yield chunk

        logger.info(f"Completed loading {chunk_count} chunks from {self.file_path}")

    async def get_metadata(self) -> Dict[str, Any]:
        """
        Asynchronously retrieve CSV file metadata.

        Returns:
            Dictionary containing file metadata including row count,
            column count, columns, file size, and format
        """
        base_metadata = await self.get_base_metadata()

        # Run pandas operations in thread pool
        loop = asyncio.get_event_loop()

        def _get_csv_info():
            """Synchronous function to get CSV metadata."""
            # Read first chunk to get column info
            first_chunk = pd.read_csv(
                self.file_path,
                delimiter=self.delimiter,
                encoding=self.encoding,
                header=self.header,
                nrows=1,
                **self.kwargs
            )

            # Count total rows (reading file sequentially)
            row_count = sum(1 for _ in open(self.file_path, encoding=self.encoding)) - (1 if self.header == 0 else 0)

            return {
                "columns": list(first_chunk.columns),
                "column_count": len(first_chunk.columns),
                "row_count": row_count,
            }

        csv_info = await loop.run_in_executor(None, _get_csv_info)

        return {
            **base_metadata,
            **csv_info,
        }


async def create_async_csv_loader(
    file_path: str,
    chunk_size: int = 50000,
    **kwargs
) -> AsyncCSVLoader:
    """
    Factory function to create async CSV loader.

    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
        **kwargs: Additional parameters for AsyncCSVLoader

    Returns:
        AsyncCSVLoader instance

    Example:
        >>> loader = await create_async_csv_loader('data.csv', delimiter='|')
        >>> async for chunk in loader.load():
        ...     await process(chunk)
    """
    return AsyncCSVLoader(file_path, chunk_size, **kwargs)
