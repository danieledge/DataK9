"""
Parquet data loader optimized for large files.

Parquet is a columnar storage format ideal for large datasets (200GB+).
It provides excellent compression and allows for efficient column-based reading.
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
from validation_framework.loaders.base import DataLoader

try:
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False
    pq = None


class ParquetLoader(DataLoader):
    """
    Loader for Parquet files with optimized handling for large datasets.

    Parquet files are particularly well-suited for large data (200GB+) because:
    - Columnar storage allows reading only needed columns
    - Built-in compression reduces I/O
    - Efficient chunked reading without loading entire file
    - Schema is stored in the file metadata
    """

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load Parquet data in chunks using PyArrow for optimal performance.

        This method uses PyArrow's batch reading capabilities to efficiently
        process very large files without loading them entirely into memory.

        Yields:
            pd.DataFrame: Chunks of data from the Parquet file

        Raises:
            RuntimeError: If there's an error reading the Parquet file or pyarrow is not installed
        """
        if not HAS_PYARROW:
            raise RuntimeError(
                "PyArrow is required for Parquet support but is not installed. "
                "Install it with: pip install pyarrow"
            )

        try:
            # Use PyArrow for efficient chunked reading
            parquet_file = pq.ParquetFile(self.file_path)

            # Read in batches for memory efficiency
            # batch_size is in rows, similar to chunk_size for consistency
            for batch in parquet_file.iter_batches(batch_size=self.chunk_size):
                # Convert PyArrow batch to pandas DataFrame
                df = batch.to_pandas()
                yield df

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
        Get Parquet file metadata efficiently without loading data.

        Parquet files store rich metadata including schema, row count,
        and column statistics, which can be read without loading the data.

        Returns:
            Dict[str, Any]: Dictionary containing comprehensive file metadata:
                - file_path: Full path to the file
                - file_size_bytes: File size in bytes
                - file_size_mb: File size in megabytes
                - file_size_gb: File size in gigabytes (useful for large files)
                - is_empty: Whether the file is 0 bytes
                - columns: List of column names
                - column_count: Number of columns
                - dtypes: Dictionary mapping column names to data types
                - total_rows: Exact row count (from metadata, not counted)
                - num_row_groups: Number of row groups (Parquet structure)
                - compression: Compression algorithm used
        """
        if not HAS_PYARROW:
            # Return minimal metadata if pyarrow not available
            return {
                "file_path": str(self.file_path),
                "file_size_bytes": self.get_file_size(),
                "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
                "file_size_gb": round(self.get_file_size() / (1024 * 1024 * 1024), 2),
                "is_empty": self.is_empty(),
                "error": "PyArrow not installed - cannot read Parquet metadata"
            }

        metadata = {
            "file_path": str(self.file_path),
            "file_size_bytes": self.get_file_size(),
            "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
            "file_size_gb": round(self.get_file_size() / (1024 * 1024 * 1024), 2),
            "is_empty": self.is_empty(),
        }

        if not self.is_empty():
            try:
                # Read Parquet metadata (very fast, doesn't load data)
                parquet_file = pq.ParquetFile(self.file_path)
                schema = parquet_file.schema_arrow

                # Column information from schema
                metadata["columns"] = schema.names
                metadata["column_count"] = len(schema.names)

                # Data types (convert PyArrow types to string for readability)
                metadata["dtypes"] = {
                    name: str(schema.field(name).type)
                    for name in schema.names
                }

                # Row count from metadata (fast, no data scan needed)
                metadata["total_rows"] = parquet_file.metadata.num_rows

                # Parquet-specific metadata
                metadata["num_row_groups"] = parquet_file.metadata.num_row_groups
                metadata["num_columns"] = parquet_file.metadata.num_columns

                # Compression information (from first row group)
                if parquet_file.metadata.num_row_groups > 0:
                    first_row_group = parquet_file.metadata.row_group(0)
                    if first_row_group.num_columns > 0:
                        # Get compression from first column of first row group
                        compression = first_row_group.column(0).compression
                        metadata["compression"] = compression

            except Exception as e:
                # If metadata reading fails, log the error but don't crash
                metadata["error"] = f"Could not read Parquet metadata: {str(e)}"

        return metadata

    def get_columns(self) -> List[str]:
        """
        Get list of column names efficiently from Parquet metadata.

        This is a convenience method that doesn't require loading any data.

        Returns:
            List of column names

        Raises:
            RuntimeError: If columns cannot be read from metadata
        """
        try:
            if self.is_empty():
                return []

            parquet_file = pq.ParquetFile(self.file_path)
            return parquet_file.schema_arrow.names

        except Exception as e:
            raise RuntimeError(
                f"Error reading column names from {self.file_path}: {str(e)}"
            )

    def get_row_count(self) -> int:
        """
        Get exact row count efficiently from Parquet metadata.

        Unlike CSV files, Parquet stores row count in metadata,
        so this is very fast even for 200GB+ files.

        Returns:
            int: Total number of rows

        Raises:
            RuntimeError: If row count cannot be read from metadata
        """
        try:
            if self.is_empty():
                return 0

            parquet_file = pq.ParquetFile(self.file_path)
            return parquet_file.metadata.num_rows

        except Exception as e:
            raise RuntimeError(
                f"Error reading row count from {self.file_path}: {str(e)}"
            )
