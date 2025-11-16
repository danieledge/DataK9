"""
Polars-based Parquet data loader optimized for ultra-fast large file processing.

Polars provides 5-10x performance improvement over pandas for Parquet files:
- Multi-threaded reading out of the box
- Lazy evaluation with query optimization
- Lower memory footprint
- Native Arrow integration without conversion overhead
"""

from typing import Iterator, Dict, Any, List
from validation_framework.loaders.base import DataLoader
from validation_framework.core.backend import HAS_POLARS, DataFrame

if HAS_POLARS:
    import polars as pl


class PolarsParquetLoader(DataLoader):
    """
    Polars-based loader for Parquet files with superior performance.

    Performance characteristics compared to pandas:
    - 5-10x faster read times
    - 50% less memory usage
    - Multi-threaded by default
    - Optimized for large files (200GB+)

    This loader is automatically selected when backend=POLARS (default).
    """

    def load(self) -> Iterator[DataFrame]:
        """
        Load Parquet data in chunks using Polars' optimized engine.

        Polars reads Parquet files with maximum efficiency:
        - Parallel column reading
        - Predicate pushdown
        - Projection pushdown
        - Direct Arrow memory layout (no conversion)

        Yields:
            pl.DataFrame: Chunks of data from the Parquet file

        Raises:
            RuntimeError: If there's an error reading the Parquet file
        """
        if not HAS_POLARS:
            raise RuntimeError(
                "Polars is required but not installed. "
                "Install it with: pip install polars-lts-cpu"
            )

        try:
            # Polars scan_parquet provides lazy evaluation
            # CRITICAL: Do NOT collect() the entire file - use lazy slicing for chunks
            lazy_df = pl.scan_parquet(str(self.file_path))

            # Get total row count efficiently without loading data
            total_rows = lazy_df.select(pl.len()).collect().item()

            # Yield chunks using lazy evaluation - only materializes one chunk at a time
            for start_row in range(0, total_rows, self.chunk_size):
                end_row = min(start_row + self.chunk_size, total_rows)
                # Use slice on lazy frame, then collect only this chunk
                chunk = lazy_df.slice(start_row, end_row - start_row).collect()
                yield chunk

        except FileNotFoundError:
            raise FileNotFoundError(f"Parquet file not found: {self.file_path}")

        except Exception as e:
            # Provide detailed error message for debugging
            raise RuntimeError(
                f"Error loading Parquet file {self.file_path}: {str(e)}. "
                f"Ensure the file is a valid Parquet format."
            )

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get Parquet file metadata efficiently using Polars.

        Polars can read metadata without loading the full file,
        similar to PyArrow but with better integration.

        Returns:
            Dict[str, Any]: Dictionary containing comprehensive file metadata
        """
        metadata = {
            "file_path": str(self.file_path),
            "file_size_bytes": self.get_file_size(),
            "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
            "file_size_gb": round(self.get_file_size() / (1024 * 1024 * 1024), 2),
            "is_empty": self.is_empty(),
            "backend": "polars"
        }

        if not self.is_empty():
            try:
                # Use scan_parquet to get schema without loading data
                lazy_df = pl.scan_parquet(str(self.file_path))

                # Get schema information (use collect_schema() to avoid PerformanceWarning)
                schema = lazy_df.collect_schema()
                metadata["columns"] = list(schema.keys())
                metadata["column_count"] = len(schema)

                # Data types
                metadata["dtypes"] = {
                    name: str(dtype)
                    for name, dtype in schema.items()
                }

                # Get row count efficiently without loading full data
                # Use Polars select(len()) optimization
                metadata["total_rows"] = lazy_df.select(pl.len()).collect().item()

            except Exception as e:
                # If metadata reading fails, log the error but don't crash
                metadata["error"] = f"Could not read Parquet metadata: {str(e)}"

        return metadata

    def get_columns(self) -> List[str]:
        """
        Get list of column names efficiently from Parquet schema.

        Returns:
            List of column names

        Raises:
            RuntimeError: If columns cannot be read from metadata
        """
        try:
            if self.is_empty():
                return []

            lazy_df = pl.scan_parquet(str(self.file_path))
            return list(lazy_df.schema.keys())

        except Exception as e:
            raise RuntimeError(
                f"Error reading column names from {self.file_path}: {str(e)}"
            )

    def get_row_count(self) -> int:
        """
        Get exact row count from Parquet file.

        Polars can read this efficiently from file metadata.

        Returns:
            int: Total number of rows

        Raises:
            RuntimeError: If row count cannot be read
        """
        try:
            if self.is_empty():
                return 0

            # Scan and count - Polars optimizes this operation
            lazy_df = pl.scan_parquet(str(self.file_path))
            return lazy_df.select(pl.len()).collect().item()

        except Exception as e:
            raise RuntimeError(
                f"Error reading row count from {self.file_path}: {str(e)}"
            )
