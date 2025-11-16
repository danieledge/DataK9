"""
Polars-based CSV data loader with superior performance.

Polars CSV parser is significantly faster than pandas:
- 3-5x faster parsing
- Multi-threaded by default
- Better memory efficiency
- Automatic type inference
"""

from typing import Iterator, Dict, Any, Optional
import logging
from validation_framework.loaders.base import DataLoader
from validation_framework.core.backend import HAS_POLARS, DataFrame

logger = logging.getLogger(__name__)

if HAS_POLARS:
    import polars as pl


class PolarsCSVLoader(DataLoader):
    """
    Polars-based loader for CSV and delimited text files.

    Performance advantages over pandas:
    - 3-5x faster CSV parsing
    - Multi-threaded reading
    - Efficient memory usage
    - Better type inference

    This loader is automatically selected when backend=POLARS (default).
    """

    def load(self) -> Iterator[DataFrame]:
        """
        Load CSV data in chunks using Polars' optimized parser.

        Polars CSV parser is multi-threaded and highly optimized,
        providing significant speedup over pandas especially for large files.

        Yields:
            pl.DataFrame: Chunks of data from the CSV file

        Raises:
            RuntimeError: If there's an error reading the CSV file
        """
        if not HAS_POLARS:
            raise RuntimeError(
                "Polars is required but not installed. "
                "Install it with: pip install polars-lts-cpu"
            )

        delimiter = self.kwargs.get("delimiter", ",")
        encoding = self.kwargs.get("encoding", "utf-8")
        # Normalize encoding for Polars (expects 'utf8', not 'utf-8')
        if encoding.lower() == 'utf-8':
            encoding = 'utf8'
        elif encoding.lower() == 'utf-16':
            encoding = 'utf8'  # Polars only supports utf8
        has_header = self.kwargs.get("header", 0) == 0  # 0 means first row is header

        try:
            # Polars scan_csv provides lazy reading with automatic optimization
            lazy_df = pl.scan_csv(
                str(self.file_path),
                separator=delimiter,
                encoding=encoding,
                has_header=has_header,
                ignore_errors=True,  # Similar to pandas on_bad_lines='warn'
                low_memory=False,  # Better performance for large files
            )

            # Collect the data (Polars is memory efficient)
            df = lazy_df.collect()

            # Yield chunks
            total_rows = df.height
            for start_row in range(0, total_rows, self.chunk_size):
                end_row = min(start_row + self.chunk_size, total_rows)
                chunk = df.slice(start_row, end_row - start_row)
                yield chunk

        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        except Exception as e:
            logger.error(f"Error loading CSV file {self.file_path}: {str(e)}", exc_info=True)
            raise RuntimeError(f"Error loading CSV file {self.file_path}: {str(e)}")

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get CSV file metadata using Polars.

        Returns:
            Dictionary with file metadata
        """
        metadata = {
            "file_path": str(self.file_path),
            "file_size_bytes": self.get_file_size(),
            "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
            "is_empty": self.is_empty(),
            "backend": "polars"
        }

        # Try to get column info without loading full file
        if not self.is_empty():
            try:
                delimiter = self.kwargs.get("delimiter", ",")
                encoding = self.kwargs.get("encoding", "utf-8")
                # Normalize encoding for Polars
                if encoding.lower() == 'utf-8':
                    encoding = 'utf8'
                elif encoding.lower() == 'utf-16':
                    encoding = 'utf8'
                has_header = self.kwargs.get("header", 0) == 0

                # Read just first few rows to get schema
                sample_df = pl.read_csv(
                    str(self.file_path),
                    separator=delimiter,
                    encoding=encoding,
                    has_header=has_header,
                    n_rows=1000,  # Sample size
                    ignore_errors=True,
                )

                metadata["columns"] = sample_df.columns
                metadata["column_count"] = len(sample_df.columns)
                metadata["dtypes"] = {
                    col: str(dtype)
                    for col, dtype in sample_df.schema.items()
                }
                metadata["encoding"] = encoding

                # Estimate total rows (Polars is fast enough we can scan the file)
                # For very large files, this is still faster than pandas
                try:
                    full_count = pl.scan_csv(
                        str(self.file_path),
                        separator=delimiter,
                        encoding=encoding,
                        has_header=has_header,
                    ).select(pl.count()).collect().item()
                    metadata["total_rows"] = full_count
                except:
                    # If full scan fails, provide estimate
                    metadata["estimated_rows"] = "unknown (scan failed)"

            except Exception as e:
                logger.error(f"Error reading CSV metadata: {str(e)}", exc_info=True)
                metadata["error"] = f"Could not read metadata: {str(e)}"

        return metadata
