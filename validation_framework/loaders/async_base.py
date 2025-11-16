"""
Async base classes for data loaders.

Provides async/await support for I/O-bound data loading operations,
enabling concurrent processing of multiple files and better performance
for large-scale validation jobs.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, Any
import pandas as pd


class AsyncDataLoader(ABC):
    """
    Abstract base class for async data loaders.

    Async loaders enable non-blocking I/O operations, allowing the validation
    engine to process multiple files concurrently and improve throughput for
    I/O-bound workloads.

    Benefits of async loaders:
    - Concurrent validation of multiple files
    - Better resource utilization during I/O waits
    - Improved performance for network-based data sources (databases, cloud storage)
    - Non-blocking chunk iteration

    All concrete async loader implementations must inherit from this class
    and implement the required abstract methods.
    """

    def __init__(self, file_path: str, chunk_size: int = 50000):
        """
        Initialize async data loader.

        Args:
            file_path: Path to the data file or resource identifier
            chunk_size: Number of rows to load per chunk for memory-efficient processing
        """
        self.file_path = file_path
        self.chunk_size = chunk_size

    @abstractmethod
    async def load(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously load data in chunks.

        This method should yield DataFrame chunks asynchronously, allowing
        other async operations to run during I/O waits.

        Yields:
            DataFrame: Chunks of data as pandas DataFrames

        Example:
            >>> async for chunk in loader.load():
            ...     # Process chunk asynchronously
            ...     await validate_chunk(chunk)
        """
        pass

    @abstractmethod
    async def get_metadata(self) -> Dict[str, Any]:
        """
        Asynchronously retrieve metadata about the data source.

        Returns:
            Dictionary containing metadata such as:
            - row_count: Total number of rows
            - column_count: Number of columns
            - columns: List of column names
            - file_size: Size in bytes
            - format: Data format

        Example:
            >>> metadata = await loader.get_metadata()
            >>> print(f"Total rows: {metadata['row_count']}")
        """
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup resources."""
        return False


class AsyncFileLoader(AsyncDataLoader):
    """
    Base class for async file-based loaders.

    Provides common functionality for async file operations including:
    - Async file existence checking
    - Async file size retrieval
    - Async file opening with proper resource management
    """

    async def file_exists(self) -> bool:
        """
        Check if file exists asynchronously.

        Returns:
            True if file exists, False otherwise
        """
        try:
            import aiofiles.os
            return await aiofiles.os.path.exists(self.file_path)
        except ImportError:
            # Fall back to sync check if aiofiles not available
            from pathlib import Path
            return Path(self.file_path).exists()

    async def get_file_size(self) -> int:
        """
        Get file size asynchronously.

        Returns:
            File size in bytes

        Raises:
            FileNotFoundError: If file does not exist
        """
        if not await self.file_exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")

        try:
            import aiofiles.os
            stat = await aiofiles.os.stat(self.file_path)
            return stat.st_size
        except ImportError:
            # Fall back to sync operation
            from pathlib import Path
            return Path(self.file_path).stat().st_size

    async def get_base_metadata(self) -> Dict[str, Any]:
        """
        Get basic file metadata asynchronously.

        Returns:
            Dictionary with file_path, file_size, format
        """
        file_size = await self.get_file_size()

        return {
            "file_path": self.file_path,
            "file_size": file_size,
            "format": self._get_format(),
        }

    def _get_format(self) -> str:
        """
        Infer file format from extension.

        Returns:
            File format string
        """
        from pathlib import Path
        suffix = Path(self.file_path).suffix.lower()

        format_map = {
            ".csv": "csv",
            ".tsv": "csv",
            ".txt": "csv",
            ".json": "json",
            ".jsonl": "jsonl",
            ".parquet": "parquet",
            ".xlsx": "excel",
            ".xls": "excel",
        }

        return format_map.get(suffix, "unknown")
