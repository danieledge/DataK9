"""CSV data loader with chunked reading for large files."""

import csv
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, Optional
import pandas as pd
from validation_framework.loaders.base import DataLoader

logger = logging.getLogger(__name__)


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


class CSVLoader(DataLoader):
    """Loader for CSV and delimited text files with robust error handling."""

    def __init__(self, file_path: str, chunk_size: int = 10000, **kwargs):
        """
        Initialize CSVLoader with auto-detection capabilities.

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

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load CSV data in chunks with robust error handling.

        Yields:
            DataFrames containing chunks of data
        """
        delimiter = self.kwargs.get("delimiter", ",")
        encoding = self.kwargs.get("encoding", "utf-8")
        header = self.kwargs.get("header", 0)

        try:
            # Use chunksize for memory-efficient reading
            for chunk in pd.read_csv(
                self.file_path,
                delimiter=delimiter,
                encoding=encoding,
                header=header,
                chunksize=self.chunk_size,
                low_memory=False,
                on_bad_lines='warn',  # Warn but don't fail on bad lines
            ):
                yield chunk

        except pd.errors.EmptyDataError:
            logger.warning(f"Empty CSV file: {self.file_path}")
            yield pd.DataFrame()

        except pd.errors.ParserError as e:
            error_msg = str(e)
            # Provide helpful error messages for common issues
            if "Expected" in error_msg and "fields" in error_msg:
                raise RuntimeError(
                    f"CSV parsing error in {self.file_path}: Row has inconsistent number of columns. "
                    f"This often means the delimiter is incorrect (current: {repr(delimiter)}) "
                    f"or the file contains unquoted delimiters in data fields. "
                    f"Try specifying --delimiter or check the file for formatting issues.\n"
                    f"Original error: {error_msg}"
                )
            raise RuntimeError(f"CSV parsing error in {self.file_path}: {error_msg}")

        except UnicodeDecodeError as e:
            raise RuntimeError(
                f"Encoding error in {self.file_path}: Cannot decode file with {encoding} encoding. "
                f"Try specifying a different encoding (e.g., cp1252, latin-1, utf-16).\n"
                f"Original error: {str(e)}"
            )

        except Exception as e:
            raise RuntimeError(f"Error loading CSV file {self.file_path}: {str(e)}")

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get CSV file metadata.

        Returns:
            Dictionary with file metadata
        """
        metadata = {
            "file_path": str(self.file_path),
            "file_size_bytes": self.get_file_size(),
            "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
            "is_empty": self.is_empty(),
        }

        # Try to get column info without loading full file
        if not self.is_empty():
            try:
                delimiter = self.kwargs.get("delimiter", ",")
                encoding = self.kwargs.get("encoding", "utf-8")
                header = self.kwargs.get("header", 0)

                # Read just first chunk to get schema
                first_chunk = pd.read_csv(
                    self.file_path,
                    delimiter=delimiter,
                    encoding=encoding,
                    header=header,
                    nrows=1000,
                    low_memory=False,
                )

                metadata["columns"] = list(first_chunk.columns)
                metadata["column_count"] = len(first_chunk.columns)
                metadata["dtypes"] = {col: str(dtype) for col, dtype in first_chunk.dtypes.items()}

                # Estimate total rows (rough estimate based on file size and sample)
                # This is just an estimate, actual count requires reading the full file
                sample_size_bytes = len(first_chunk.to_csv(index=False).encode(encoding))
                estimated_rows = int((self.get_file_size() / sample_size_bytes) * len(first_chunk))
                metadata["estimated_rows"] = estimated_rows

            except Exception as e:
                metadata["error"] = f"Could not read metadata: {str(e)}"

        return metadata
