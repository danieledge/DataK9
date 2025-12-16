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
    Detect the encoding of a file using BOM detection and heuristics.

    Args:
        file_path: Path to the file

    Returns:
        Detected encoding name, defaults to 'utf-8'
    """
    # First, check for BOM (Byte Order Mark)
    try:
        with open(file_path, 'rb') as f:
            raw = f.read(4)

        # Check for common BOMs
        if raw.startswith(b'\xef\xbb\xbf'):
            logger.debug("Detected UTF-8 BOM")
            return 'utf-8-sig'
        elif raw.startswith(b'\xff\xfe\x00\x00'):
            logger.debug("Detected UTF-32 LE BOM")
            return 'utf-32-le'
        elif raw.startswith(b'\x00\x00\xfe\xff'):
            logger.debug("Detected UTF-32 BE BOM")
            return 'utf-32-be'
        elif raw.startswith(b'\xff\xfe'):
            logger.debug("Detected UTF-16 LE BOM")
            return 'utf-16-le'
        elif raw.startswith(b'\xfe\xff'):
            logger.debug("Detected UTF-16 BE BOM")
            return 'utf-16-be'
    except Exception:
        pass

    # Try encodings in order of likelihood, validating each
    # Note: latin-1 never fails (accepts any byte), so it's last as fallback
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'iso-8859-1', 'latin-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='strict') as f:
                # Read a larger sample to catch encoding issues
                content = f.read(32768)

                # For UTF-8, check for common non-UTF-8 patterns that might slip through
                if encoding == 'utf-8':
                    # If we see replacement characters, encoding might be wrong
                    if '\ufffd' in content:
                        continue

                    # Check if content looks reasonable (has printable chars)
                    printable_ratio = sum(1 for c in content[:1000] if c.isprintable() or c in '\n\r\t') / max(len(content[:1000]), 1)
                    if printable_ratio < 0.9:
                        continue

            logger.debug(f"Detected encoding: {encoding}")
            return encoding
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.debug(f"Error testing encoding {encoding}: {e}")
            continue

    # Default to utf-8 if all else fails
    logger.debug("Encoding detection failed, defaulting to utf-8")
    return 'utf-8'


def detect_header_skip_rows(file_path: str, delimiter: str = ',', encoding: str = 'utf-8') -> int:
    """
    Auto-detect the number of rows to skip before the header row.

    Detects common patterns where files have metadata/identifier lines before headers:
    - First line has fewer columns than second line (file identifier)
    - First line matches common metadata patterns (File:, Report:, Generated:, Export:, etc.)
    - First line is a single value (file ID, timestamp, etc.)

    Args:
        file_path: Path to the CSV file
        delimiter: Column delimiter
        encoding: File encoding

    Returns:
        Number of rows to skip (0 if header is on first line)
    """
    import re

    # Metadata patterns that suggest a non-header first line
    metadata_patterns = [
        r'^file[:\s]',
        r'^report[:\s]',
        r'^generated[:\s]',
        r'^export[:\s]',
        r'^date[:\s]',
        r'^created[:\s]',
        r'^source[:\s]',
        r'^data\s*extract',
        r'^\d{4}[-/]\d{2}[-/]\d{2}',  # Date-like: 2024-01-15
        r'^[A-Z]{2,5}[-_]\d+',  # ID-like: FILE-001, EXP_123
    ]

    try:
        with open(file_path, 'r', newline='', encoding=encoding) as f:
            # Read first few lines
            lines = []
            for i, line in enumerate(f):
                if i >= 5:  # Only check first 5 lines
                    break
                lines.append(line.strip())

            if len(lines) < 2:
                return 0

            # Count columns in first few lines
            def count_cols(line):
                # Simple column count (doesn't handle all quoting edge cases)
                return len(line.split(delimiter))

            first_line_cols = count_cols(lines[0])
            second_line_cols = count_cols(lines[1])

            # Check if first line looks like a file identifier
            first_line_lower = lines[0].lower()

            # Pattern 1: First line has significantly fewer columns than second
            if first_line_cols == 1 and second_line_cols > 1:
                logger.info(f"Auto-detected file identifier line: first line has 1 column, second has {second_line_cols}")
                return 1

            # Pattern 2: First line matches metadata patterns
            for pattern in metadata_patterns:
                if re.match(pattern, first_line_lower, re.IGNORECASE):
                    logger.info(f"Auto-detected metadata line matching pattern: {pattern}")
                    return 1

            # Pattern 3: First line has much fewer columns than subsequent lines
            if len(lines) >= 3:
                third_line_cols = count_cols(lines[2])
                if first_line_cols < second_line_cols and second_line_cols == third_line_cols:
                    logger.info(f"Auto-detected header skip: line 1 has {first_line_cols} cols, lines 2-3 have {second_line_cols} cols")
                    return 1

    except Exception as e:
        logger.debug(f"Header skip detection failed: {e}")

    return 0


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

        # Track skipped rows for reporting
        self.skipped_row_count = 0

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

        # Auto-detect skiprows if not specified (detect file identifier lines)
        if 'skiprows' not in kwargs or kwargs.get('skiprows') is None:
            detected_skip = detect_header_skip_rows(
                file_path,
                delimiter=self.kwargs.get('delimiter', ','),
                encoding=self.kwargs.get('encoding', 'utf-8')
            )
            if detected_skip > 0:
                self.kwargs['skiprows'] = detected_skip
                logger.info(f"Auto-detected: skipping first {detected_skip} row(s) before header")

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load CSV data in chunks with robust error handling.

        Yields:
            DataFrames containing chunks of data
        """
        delimiter = self.kwargs.get("delimiter", ",")
        encoding = self.kwargs.get("encoding", "utf-8")
        header = self.kwargs.get("header", 0)
        skiprows = self.kwargs.get("skiprows", None)
        quoting = self.kwargs.get("quoting", 0)  # Default: QUOTE_MINIMAL

        # Log skiprows if set
        if skiprows:
            logger.info(f"Skipping first {skiprows} row(s) before header")

        # Reset skipped rows tracking - just count, line numbers not available with chunked reading
        self.skipped_row_count = 0

        def track_bad_line(bad_line):
            """Track bad lines instead of printing each one."""
            self.skipped_row_count += 1
            return None  # Return None to skip the line

        try:
            # Use chunksize for memory-efficient reading
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', message='Skipping line')
                for chunk in pd.read_csv(
                    self.file_path,
                    delimiter=delimiter,
                    encoding=encoding,
                    header=header,
                    skiprows=skiprows,  # Skip metadata/identifier rows before header
                    chunksize=self.chunk_size,
                    on_bad_lines=track_bad_line,  # Track and skip bad lines
                    engine='python',  # Required for callable on_bad_lines
                    quoting=quoting,
                ):
                    yield chunk

            # Log summary of skipped rows (if any)
            if self.skipped_row_count > 0:
                logger.warning(f"Skipped {self.skipped_row_count} malformed row(s) due to inconsistent column counts")

        except pd.errors.EmptyDataError:
            logger.warning(f"Empty CSV file: {self.file_path}")
            yield pd.DataFrame()

        except pd.errors.ParserError as e:
            error_msg = str(e)
            # Provide helpful error messages for common issues
            if "Expected" in error_msg and "fields" in error_msg:
                # Try to recover by skipping bad lines
                logger.warning(f"CSV has inconsistent columns, attempting recovery with on_bad_lines='skip'")
                try:
                    for chunk in pd.read_csv(
                        self.file_path,
                        delimiter=delimiter,
                        encoding=encoding,
                        header=header,
                        skiprows=skiprows,  # Skip metadata/identifier rows before header
                        chunksize=self.chunk_size,
                        low_memory=False,
                        on_bad_lines='skip',  # Skip problematic rows
                        quoting=quoting,
                    ):
                        yield chunk
                    logger.warning("CSV loaded with some rows skipped due to parsing errors")
                    return
                except Exception:
                    pass  # Recovery failed, raise original error

                raise RuntimeError(
                    f"CSV parsing error in {self.file_path}: Row has inconsistent number of columns. "
                    f"This often means:\n"
                    f"  1. The delimiter is incorrect (current: {repr(delimiter)})\n"
                    f"  2. The file contains unquoted {repr(delimiter)} characters in data fields\n"
                    f"  3. Some rows have missing or extra columns\n\n"
                    f"Solutions:\n"
                    f"  - If fields contain {repr(delimiter)}, they should be quoted: \"field with {delimiter}\"\n"
                    f"  - Try a different delimiter with --delimiter option\n"
                    f"  - Check and fix the source file\n\n"
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
                skiprows = self.kwargs.get("skiprows", None)
                quoting = self.kwargs.get("quoting", 0)

                # Read just first chunk to get schema
                first_chunk = pd.read_csv(
                    self.file_path,
                    delimiter=delimiter,
                    encoding=encoding,
                    header=header,
                    skiprows=skiprows,
                    quoting=quoting,
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
