"""
Async JSON data loader.

Provides non-blocking JSON file loading with support for both standard JSON
arrays and JSON Lines (JSONL) format.
"""

from typing import AsyncIterator, Dict, Any
import pandas as pd
import asyncio
import json
from validation_framework.loaders.async_base import AsyncFileLoader
import logging

# Optional dependency for async file I/O
try:
    import aiofiles
    import aiofiles.os
    AIOFILES_AVAILABLE = True
except ImportError:
    AIOFILES_AVAILABLE = False
    aiofiles = None

logger = logging.getLogger(__name__)


class AsyncJSONLoader(AsyncFileLoader):
    """
    Async loader for JSON and JSON Lines files.

    Supports:
    - Standard JSON arrays: [{"col1": "val1"}, {"col2": "val2"}]
    - JSON Lines format: one JSON object per line
    - Automatic format detection
    - Nested JSON flattening
    - Concurrent file processing

    Uses aiofiles for true async file I/O when reading JSON Lines format.
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        lines: bool = None,
        flatten: bool = True,
        **kwargs
    ):
        """
        Initialize async JSON loader.

        Args:
            file_path: Path to JSON file
            chunk_size: Number of rows per chunk (default: 50,000)
            lines: True for JSON Lines format, False for standard JSON,
                  None for auto-detection (default: None)
            flatten: Whether to flatten nested JSON structures (default: True)
            **kwargs: Additional parameters passed to pandas read_json
        """
        super().__init__(file_path, chunk_size)
        self.lines = lines
        self.flatten = flatten
        self.kwargs = kwargs

    async def _detect_format(self) -> bool:
        """
        Detect if file is JSON Lines format.

        Returns:
            True if JSON Lines, False if standard JSON array
        """
        if not AIOFILES_AVAILABLE:
            raise ImportError(
                "aiofiles package is required for async JSON loading. "
                "Install with: pip install aiofiles"
            )

        async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
            first_line = await f.readline()
            first_line = first_line.strip()

            # JSON Lines: first line should be a valid JSON object
            # Standard JSON: first line should start with '['
            if first_line.startswith('['):
                return False  # Standard JSON array
            else:
                return True  # JSON Lines

    async def load(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously load JSON data in chunks.

        Yields:
            DataFrame chunks

        Example:
            >>> loader = AsyncJSONLoader('data.jsonl', chunk_size=5000)
            >>> async for chunk in loader.load():
            ...     print(f"Processing {len(chunk)} rows")
        """
        if not await self.file_exists():
            raise FileNotFoundError(f"JSON file not found: {self.file_path}")

        # Auto-detect format if not specified
        is_lines = self.lines
        if is_lines is None:
            is_lines = await self._detect_format()

        logger.info(f"Async loading JSON file: {self.file_path} (lines={is_lines})")

        if is_lines:
            # JSON Lines format - can read line by line asynchronously
            async for chunk in self._load_jsonl_chunks():
                yield chunk
        else:
            # Standard JSON array - load entire array then chunk
            async for chunk in self._load_json_array_chunks():
                yield chunk

    async def _load_jsonl_chunks(self) -> AsyncIterator[pd.DataFrame]:
        """
        Load JSON Lines file in chunks using true async I/O.

        Yields:
            DataFrame chunks
        """
        chunk_data = []
        chunk_count = 0

        async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    chunk_data.append(record)

                    if len(chunk_data) >= self.chunk_size:
                        # Create DataFrame from chunk
                        df = pd.DataFrame(chunk_data)

                        if self.flatten and len(df) > 0:
                            df = self._flatten_dataframe(df)

                        chunk_count += 1
                        logger.debug(f"Loaded JSONL chunk {chunk_count}: {len(df)} rows")

                        yield df

                        # Reset chunk
                        chunk_data = []

                        # Yield control to event loop
                        await asyncio.sleep(0)

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line: {e}")
                    continue

        # Yield remaining data
        if chunk_data:
            df = pd.DataFrame(chunk_data)

            if self.flatten and len(df) > 0:
                df = self._flatten_dataframe(df)

            chunk_count += 1
            logger.debug(f"Loaded final JSONL chunk {chunk_count}: {len(df)} rows")
            yield df

        logger.info(f"Completed loading {chunk_count} chunks from {self.file_path}")

    async def _load_json_array_chunks(self) -> AsyncIterator[pd.DataFrame]:
        """
        Load standard JSON array file in chunks.

        Since pandas read_json doesn't support chunking for arrays,
        we load the entire array and then chunk it.

        Yields:
            DataFrame chunks
        """
        loop = asyncio.get_event_loop()

        def _read_json():
            """Synchronous function to read JSON."""
            df = pd.read_json(self.file_path, **self.kwargs)

            if self.flatten and len(df) > 0:
                df = self._flatten_dataframe(df)

            return df

        # Load entire JSON in thread pool
        df = await loop.run_in_executor(None, _read_json)

        # Yield in chunks
        total_rows = len(df)
        chunk_count = 0

        for start_idx in range(0, total_rows, self.chunk_size):
            end_idx = min(start_idx + self.chunk_size, total_rows)
            chunk = df.iloc[start_idx:end_idx].copy()

            chunk_count += 1
            logger.debug(f"Loaded JSON chunk {chunk_count}: {len(chunk)} rows")

            yield chunk

            # Yield control to event loop
            await asyncio.sleep(0)

        logger.info(f"Completed loading {chunk_count} chunks from {self.file_path}")

    def _flatten_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flatten nested JSON structures in DataFrame.

        Args:
            df: DataFrame with potentially nested structures

        Returns:
            Flattened DataFrame
        """
        return pd.json_normalize(df.to_dict('records'))

    async def get_metadata(self) -> Dict[str, Any]:
        """
        Asynchronously retrieve JSON file metadata.

        Returns:
            Dictionary containing file metadata
        """
        base_metadata = await self.get_base_metadata()

        # Detect format
        is_lines = self.lines
        if is_lines is None:
            is_lines = await self._detect_format()

        loop = asyncio.get_event_loop()

        def _get_json_info():
            """Synchronous function to get JSON metadata."""
            if is_lines:
                # Count lines for JSON Lines
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    row_count = sum(1 for line in f if line.strip())

                # Read first line to get columns
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line:
                        first_record = json.loads(first_line)
                        columns = list(first_record.keys())
                    else:
                        columns = []
            else:
                # Load JSON array
                df = pd.read_json(self.file_path)
                row_count = len(df)
                columns = list(df.columns)

            return {
                "columns": columns,
                "column_count": len(columns),
                "row_count": row_count,
                "is_json_lines": is_lines,
            }

        json_info = await loop.run_in_executor(None, _get_json_info)

        return {
            **base_metadata,
            **json_info,
        }


async def create_async_json_loader(
    file_path: str,
    chunk_size: int = 50000,
    **kwargs
) -> AsyncJSONLoader:
    """
    Factory function to create async JSON loader.

    Args:
        file_path: Path to JSON file
        chunk_size: Number of rows per chunk
        **kwargs: Additional parameters for AsyncJSONLoader

    Returns:
        AsyncJSONLoader instance

    Example:
        >>> loader = await create_async_json_loader('data.jsonl')
        >>> async for chunk in loader.load():
        ...     await process(chunk)
    """
    return AsyncJSONLoader(file_path, chunk_size, **kwargs)
