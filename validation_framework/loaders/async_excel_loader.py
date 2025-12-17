"""
Async Excel data loader.

Provides non-blocking Excel file loading with chunked processing for
memory-efficient validation of Excel files.
"""

from typing import AsyncIterator, Dict, Any
import pandas as pd
import asyncio
from validation_framework.loaders.async_base import AsyncFileLoader
import logging

logger = logging.getLogger(__name__)


class AsyncExcelLoader(AsyncFileLoader):
    """
    Async loader for Excel files (.xls, .xlsx).

    Uses asyncio to enable concurrent file processing and non-blocking I/O.
    Pandas Excel operations are executed in a thread pool to avoid blocking
    the event loop.

    Features:
    - Concurrent processing of multiple Excel files
    - Chunked reading for memory efficiency
    - Support for both .xlsx and .xls formats
    - Non-blocking I/O operations
    - Multi-sheet support
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        sheet_name: int = 0,
        header: int = 0,
        **kwargs
    ):
        """
        Initialize async Excel loader.

        Args:
            file_path: Path to Excel file
            chunk_size: Number of rows per chunk (default: 50,000)
            sheet_name: Sheet name or index to read (default: 0 for first sheet)
            header: Row number to use as column names (default: 0)
            **kwargs: Additional parameters passed to pandas read_excel
        """
        super().__init__(file_path, chunk_size)
        self.sheet_name = sheet_name
        self.header = header
        self.kwargs = kwargs

    async def load(self) -> AsyncIterator[pd.DataFrame]:
        """
        Asynchronously load Excel data in chunks.

        Note: Excel files are typically loaded fully into memory first, then
        yielded in chunks for consistency with other loaders.

        Yields:
            DataFrame chunks

        Raises:
            FileNotFoundError: If the file does not exist
            RuntimeError: If there's an error reading the Excel file

        Example:
            >>> loader = AsyncExcelLoader('data.xlsx', sheet_name='Sheet1')
            >>> async for chunk in loader.load():
            ...     print(f"Processing {len(chunk)} rows")
        """
        if not await self.file_exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")

        logger.info(f"Async loading Excel file: {self.file_path}")

        # Run pandas read_excel in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()

        def _read_excel():
            """Synchronous function to read Excel file."""
            return pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=self.header,
                engine='openpyxl',
                **self.kwargs
            )

        try:
            # Read the full Excel sheet (Excel files are typically smaller)
            df = await loop.run_in_executor(None, _read_excel)

            # Yield in chunks for consistency with other loaders
            chunk_count = 0
            if len(df) == 0:
                chunk_count = 1
                yield df
            else:
                for start in range(0, len(df), self.chunk_size):
                    end = min(start + self.chunk_size, len(df))
                    # Yield control to event loop between chunks
                    await asyncio.sleep(0)
                    chunk_count += 1
                    logger.debug(f"Loaded chunk {chunk_count}: {end - start} rows")
                    yield df.iloc[start:end].copy()

            logger.info(f"Completed loading {chunk_count} chunks from {self.file_path}")

        except Exception as e:
            raise RuntimeError(
                f"Error loading Excel file {self.file_path}: {str(e)}. "
                f"Ensure the file is a valid Excel format (.xlsx or .xls)."
            )

    async def get_metadata(self) -> Dict[str, Any]:
        """
        Asynchronously retrieve Excel file metadata.

        Returns:
            Dictionary containing file metadata including row count,
            column count, columns, file size, data types, and sheet names
        """
        base_metadata = await self.get_base_metadata()

        # Run pandas operations in thread pool
        loop = asyncio.get_event_loop()

        def _get_excel_info():
            """Synchronous function to get Excel metadata."""
            # Read Excel file to get metadata
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=self.header,
                engine='openpyxl',
                **self.kwargs
            )

            # Get sheet names
            xl_file = pd.ExcelFile(self.file_path, engine='openpyxl')
            sheet_names = xl_file.sheet_names

            return {
                "columns": list(df.columns),
                "column_count": len(df.columns),
                "row_count": len(df),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "sheet_names": sheet_names,
                "active_sheet": self.sheet_name if isinstance(self.sheet_name, str) else sheet_names[self.sheet_name],
            }

        try:
            excel_info = await loop.run_in_executor(None, _get_excel_info)
            return {
                **base_metadata,
                **excel_info,
            }
        except Exception as e:
            logger.error(f"Error reading Excel metadata: {str(e)}")
            return {
                **base_metadata,
                "error": f"Could not read Excel metadata: {str(e)}"
            }


async def create_async_excel_loader(
    file_path: str,
    chunk_size: int = 50000,
    **kwargs
) -> AsyncExcelLoader:
    """
    Factory function to create async Excel loader.

    Args:
        file_path: Path to Excel file
        chunk_size: Number of rows per chunk
        **kwargs: Additional parameters for AsyncExcelLoader

    Returns:
        AsyncExcelLoader instance

    Example:
        >>> loader = await create_async_excel_loader('data.xlsx', sheet_name='Data')
        >>> async for chunk in loader.load():
        ...     await process(chunk)
    """
    return AsyncExcelLoader(file_path, chunk_size, **kwargs)
