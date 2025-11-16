"""
Async data loader factory for creating async loaders based on file format.

Enables concurrent validation of multiple files through async/await patterns.
"""

from typing import Dict, Type, Optional
from pathlib import Path
from validation_framework.loaders.async_base import AsyncDataLoader
from validation_framework.loaders.async_csv_loader import AsyncCSVLoader
from validation_framework.loaders.async_json_loader import AsyncJSONLoader


class AsyncLoaderFactory:
    """
    Factory for creating async data loaders based on file format.

    This factory automatically selects the appropriate async loader implementation
    based on the specified file format or file extension, enabling concurrent
    validation of multiple files.

    Supported formats:
        - CSV and delimited text files (csv, tsv, txt)
        - JSON files (json, jsonl)
        - Parquet files (parquet) - coming soon
        - Excel files (xls, xlsx) - coming soon

    Benefits of async loaders:
        - Concurrent validation of multiple files
        - Better resource utilization during I/O waits
        - Improved throughput for I/O-bound workloads
        - Non-blocking chunk iteration
    """

    # Mapping of format names to async loader classes
    _async_loaders: Dict[str, Type[AsyncDataLoader]] = {
        "csv": AsyncCSVLoader,
        "json": AsyncJSONLoader,
        # "parquet": AsyncParquetLoader,  # TODO: Implement
        # "excel": AsyncExcelLoader,  # TODO: Implement
    }

    @classmethod
    async def create_loader(
        cls,
        file_path: str,
        file_format: str = None,
        chunk_size: int = 50000,
        **kwargs
    ) -> AsyncDataLoader:
        """
        Create an appropriate async data loader for the given file.

        Args:
            file_path: Path to the data file
            file_format: File format ('csv', 'json', 'parquet').
                        If None, will be inferred from file extension.
            chunk_size: Number of rows per chunk for memory-efficient processing.
                       Default is 50,000 rows.
            **kwargs: Additional loader-specific parameters:
                - delimiter: Column delimiter for CSV files (default: ',')
                - encoding: File encoding for CSV files (default: 'utf-8')
                - header: Row number to use as column names (default: 0)
                - lines: For JSON files, True for JSON Lines format (default: auto-detect)
                - flatten: For JSON files, flatten nested structures (default: True)

        Returns:
            AsyncDataLoader: An instance of the appropriate async loader class

        Raises:
            ValueError: If the file format is not supported or cannot be inferred
            FileNotFoundError: If the specified file does not exist

        Examples:
            >>> # Create async CSV loader
            >>> loader = await AsyncLoaderFactory.create_loader(
            ...     'data.csv',
            ...     file_format='csv',
            ...     delimiter='|',
            ...     chunk_size=100000
            ... )
            >>>
            >>> # Process chunks concurrently
            >>> async for chunk in loader.load():
            ...     await validate_chunk(chunk)
            >>>
            >>> # Create async JSON loader (format auto-detected)
            >>> loader = await AsyncLoaderFactory.create_loader('data.jsonl')
        """
        # Verify file exists
        file_path_obj = Path(file_path)

        try:
            import aiofiles.os
            file_exists = await aiofiles.os.path.exists(str(file_path_obj))
        except ImportError:
            # Fall back to sync check if aiofiles not available
            file_exists = file_path_obj.exists()

        if not file_exists:
            raise FileNotFoundError(f"File not found: {file_path}")

        # Infer format from extension if not provided
        if file_format is None:
            file_format = cls._infer_format(file_path)

        # Normalize format name
        file_format = file_format.lower().strip()

        # Get the appropriate async loader class
        loader_class = cls._async_loaders.get(file_format)

        if loader_class is None:
            supported_formats = ', '.join(cls._async_loaders.keys())
            raise ValueError(
                f"Unsupported file format for async loading: '{file_format}'. "
                f"Supported formats are: {supported_formats}"
            )

        # Instantiate and return the async loader
        try:
            return loader_class(file_path, chunk_size=chunk_size, **kwargs)
        except Exception as e:
            raise RuntimeError(
                f"Error creating async loader for {file_path}: {str(e)}"
            )

    @classmethod
    def _infer_format(cls, file_path: str) -> str:
        """
        Infer file format from file extension.

        Args:
            file_path: Path to the file

        Returns:
            Inferred format name

        Raises:
            ValueError: If format cannot be inferred from extension
        """
        suffix = Path(file_path).suffix.lower()

        # Map file extensions to format names
        extension_map = {
            ".csv": "csv",
            ".tsv": "csv",
            ".txt": "csv",
            ".json": "json",
            ".jsonl": "json",
            # ".parquet": "parquet",  # TODO: Add when AsyncParquetLoader implemented
            # ".xls": "excel",  # TODO: Add when AsyncExcelLoader implemented
            # ".xlsx": "excel",
        }

        inferred_format = extension_map.get(suffix)

        if inferred_format is None:
            raise ValueError(
                f"Cannot infer format from file extension '{suffix}'. "
                f"Please specify the format explicitly. "
                f"Supported extensions: {', '.join(extension_map.keys())}"
            )

        return inferred_format

    @classmethod
    def register_loader(cls, format_name: str, loader_class: Type[AsyncDataLoader]):
        """
        Register a custom async data loader for a new format.

        This allows users to extend the factory with their own async loader implementations.

        Args:
            format_name: Name to register the format under
            loader_class: Async loader class (must inherit from AsyncDataLoader)

        Raises:
            TypeError: If loader_class is not a subclass of AsyncDataLoader

        Example:
            >>> class CustomAsyncLoader(AsyncDataLoader):
            ...     async def load(self):
            ...         # Custom async implementation
            ...         pass
            ...     async def get_metadata(self):
            ...         return {}
            >>>
            >>> AsyncLoaderFactory.register_loader('custom', CustomAsyncLoader)
        """
        if not issubclass(loader_class, AsyncDataLoader):
            raise TypeError(
                f"{loader_class.__name__} must be a subclass of AsyncDataLoader"
            )

        cls._async_loaders[format_name.lower()] = loader_class

    @classmethod
    def list_supported_formats(cls) -> list:
        """
        Get list of all supported file formats for async loading.

        Returns:
            List of supported format names
        """
        return list(cls._async_loaders.keys())

    @classmethod
    async def create_multiple_loaders(
        cls,
        file_configs: list,
        chunk_size: int = 50000
    ) -> list:
        """
        Create multiple async loaders concurrently.

        This is useful for validation jobs that process multiple files,
        allowing loader initialization to happen concurrently.

        Args:
            file_configs: List of file configuration dictionaries with keys:
                - path: File path (required)
                - format: File format (optional, will be inferred)
                - Additional loader-specific parameters
            chunk_size: Default chunk size for all loaders

        Returns:
            List of AsyncDataLoader instances

        Example:
            >>> file_configs = [
            ...     {"path": "file1.csv", "delimiter": "|"},
            ...     {"path": "file2.json", "lines": True},
            ...     {"path": "file3.csv"},
            ... ]
            >>> loaders = await AsyncLoaderFactory.create_multiple_loaders(file_configs)
            >>> # Process all files concurrently
            >>> await asyncio.gather(*[validate_file(loader) for loader in loaders])
        """
        import asyncio

        async def _create_single_loader(config):
            """Create a single loader from config."""
            path = config.pop("path")
            file_format = config.pop("format", None)
            return await cls.create_loader(
                path,
                file_format=file_format,
                chunk_size=chunk_size,
                **config
            )

        # Create all loaders concurrently
        loaders = await asyncio.gather(
            *[_create_single_loader(config.copy()) for config in file_configs]
        )

        return loaders
