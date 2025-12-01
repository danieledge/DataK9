"""
Polars-based CSV data loader with superior performance.

Polars CSV parser is significantly faster than pandas:
- 3-5x faster parsing
- Multi-threaded by default
- Better memory efficiency
- Automatic type inference
"""

import csv
from typing import Iterator, Dict, Any, Optional
import logging
from validation_framework.loaders.base import DataLoader
from validation_framework.core.backend import HAS_POLARS, DataFrame

logger = logging.getLogger(__name__)

if HAS_POLARS:
    import polars as pl


def detect_delimiter(file_path: str, sample_size: int = 8192) -> str:
    """
    Auto-detect the delimiter used in a CSV file.

    Args:
        file_path: Path to the CSV file
        sample_size: Number of bytes to sample for detection

    Returns:
        Detected delimiter character, defaults to ',' if detection fails
    """
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', newline='', encoding=encoding) as f:
                sample = f.read(sample_size)

            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=',\t|;:')
            return dialect.delimiter
        except (UnicodeDecodeError, csv.Error):
            continue
        except Exception:
            break

    return ','


def detect_encoding(file_path: str) -> str:
    """
    Detect the encoding of a file by trying common encodings.

    Args:
        file_path: Path to the file

    Returns:
        Detected encoding name, defaults to 'utf-8'
    """
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'latin-1', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(8192)
            return encoding
        except UnicodeDecodeError:
            continue

    return 'utf-8'


class PolarsCSVLoader(DataLoader):
    """
    Polars-based loader for CSV and delimited text files with robust error handling.

    Performance advantages over pandas:
    - 3-5x faster CSV parsing
    - Multi-threaded reading
    - Efficient memory usage
    - Better type inference

    This loader is automatically selected when backend=POLARS (default).
    """

    def __init__(self, file_path: str, chunk_size: int = 10000, **kwargs):
        """
        Initialize PolarsCSVLoader with auto-detection capabilities.

        Args:
            file_path: Path to CSV file
            chunk_size: Number of rows per chunk
            **kwargs: Additional options (delimiter, encoding, header)
        """
        super().__init__(file_path, chunk_size, **kwargs)

        # Auto-detect delimiter if not specified
        if 'delimiter' not in kwargs or kwargs.get('delimiter') is None:
            self.kwargs['delimiter'] = detect_delimiter(file_path)
            if self.kwargs['delimiter'] != ',':
                logger.info(f"Auto-detected delimiter: {repr(self.kwargs['delimiter'])}")

        # Auto-detect encoding if not specified
        if 'encoding' not in kwargs or kwargs.get('encoding') is None:
            self.kwargs['encoding'] = detect_encoding(file_path)
            if self.kwargs['encoding'] != 'utf-8':
                logger.info(f"Auto-detected encoding: {self.kwargs['encoding']}")

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

        except pl.exceptions.ComputeError as e:
            error_msg = str(e)
            # Provide helpful error messages for common issues
            if "expected" in error_msg.lower() and "column" in error_msg.lower():
                raise RuntimeError(
                    f"CSV parsing error in {self.file_path}: Row has inconsistent number of columns. "
                    f"This often means the delimiter is incorrect (current: {repr(delimiter)}) "
                    f"or the file contains unquoted delimiters in data fields. "
                    f"Try specifying --delimiter or check the file for formatting issues.\n"
                    f"Original error: {error_msg}"
                )
            raise RuntimeError(f"CSV parsing error in {self.file_path}: {error_msg}")

        except Exception as e:
            error_msg = str(e)
            # Check for encoding errors
            if 'decode' in error_msg.lower() or 'encoding' in error_msg.lower() or 'utf' in error_msg.lower():
                raise RuntimeError(
                    f"Encoding error in {self.file_path}: Cannot decode file. "
                    f"The file may use a different encoding than detected. "
                    f"Try specifying encoding explicitly.\n"
                    f"Original error: {error_msg}"
                )
            logger.error(f"Error loading CSV file {self.file_path}: {error_msg}", exc_info=True)
            raise RuntimeError(f"Error loading CSV file {self.file_path}: {error_msg}")

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
