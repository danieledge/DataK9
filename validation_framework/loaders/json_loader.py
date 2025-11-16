"""
JSON data loader with support for standard JSON arrays and JSON Lines format.

Supports:
- Standard JSON arrays: [{"col1": "val1"}, {"col2": "val2"}]
- JSON Lines (JSONL/NDJSON): One JSON object per line
- Nested JSON structures (automatically flattened)
- Chunked processing for large files

Author: daniel edge
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
import json
from pathlib import Path
from validation_framework.loaders.base import DataLoader


class JSONLoader(DataLoader):
    """
    Loader for JSON and JSON Lines files.

    Supports two formats:
    1. Standard JSON array: [{"id": 1}, {"id": 2}]
    2. JSON Lines (JSONL/NDJSON): {"id": 1}\n{"id": 2}

    Configuration:
        lines (bool): If True, treat as JSON Lines format (default: auto-detect)
        orient (str): Pandas json orientation ('records', 'index', etc.)
        flatten (bool): Flatten nested JSON structures (default: True)
    """

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load JSON data in chunks.

        Auto-detects JSON format (array vs lines) and processes accordingly.

        Yields:
            DataFrames containing chunks of data
        """
        lines = self.kwargs.get("lines", None)  # None = auto-detect
        orient = self.kwargs.get("orient", "records")
        flatten = self.kwargs.get("flatten", True)

        try:
            # Auto-detect format if not specified
            if lines is None:
                lines = self._is_jsonl_format()

            if lines:
                # JSON Lines format - process line by line in chunks
                yield from self._load_jsonl(flatten)
            else:
                # Standard JSON array format
                yield from self._load_json_array(orient, flatten)

        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Invalid JSON in file {self.file_path}: {str(e)}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Error loading JSON file {self.file_path}: {str(e)}"
            )

    def _is_jsonl_format(self) -> bool:
        """
        Auto-detect if file is JSON Lines format.

        Checks first line - if it's a valid JSON object (not array start),
        likely JSON Lines format.

        Returns:
            True if JSON Lines, False if standard JSON array
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()

                # Empty file
                if not first_line:
                    return False

                # Standard JSON array starts with [
                if first_line.startswith('['):
                    return False

                # Try parsing as JSON object
                json.loads(first_line)
                return True  # Valid JSON object = JSON Lines format

        except json.JSONDecodeError:
            return False  # Not valid JSON object = standard JSON

    def _load_jsonl(self, flatten: bool) -> Iterator[pd.DataFrame]:
        """
        Load JSON Lines format in chunks.

        Args:
            flatten: Whether to flatten nested structures

        Yields:
            DataFrames containing chunks of records
        """
        records: List[Dict[str, Any]] = []

        with open(self.file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()

                # Skip empty lines
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    records.append(record)

                    # Yield chunk when we reach chunk_size
                    if len(records) >= self.chunk_size:
                        df = self._records_to_dataframe(records, flatten)
                        yield df
                        records = []

                except json.JSONDecodeError as e:
                    # Log warning but continue processing
                    print(f"Warning: Invalid JSON on line, skipping: {str(e)}")
                    continue

        # Yield remaining records
        if records:
            df = self._records_to_dataframe(records, flatten)
            yield df

    def _load_json_array(self, orient: str, flatten: bool) -> Iterator[pd.DataFrame]:
        """
        Load standard JSON array format in chunks.

        Args:
            orient: Pandas JSON orientation
            flatten: Whether to flatten nested structures

        Yields:
            DataFrames containing chunks of data
        """
        # For standard JSON arrays, pandas can handle it efficiently
        # but we need to chunk it for memory efficiency

        try:
            # Read entire JSON (for arrays, we need to parse the whole structure)
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # If it's a dict, might be oriented data
                records = data
            else:
                raise ValueError(f"Unexpected JSON structure: {type(data)}")

            # Convert to DataFrame
            if isinstance(records, list):
                df = self._records_to_dataframe(records, flatten)
            else:
                df = pd.DataFrame.from_dict(records, orient=orient)
                if flatten:
                    df = self._flatten_dataframe(df)

            # Yield in chunks
            for i in range(0, len(df), self.chunk_size):
                chunk = df.iloc[i:i + self.chunk_size].copy()
                yield chunk

        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON array format: {str(e)}")

    def _records_to_dataframe(self, records: List[Dict[str, Any]], flatten: bool) -> pd.DataFrame:
        """
        Convert list of records to DataFrame, optionally flattening nested structures.

        Args:
            records: List of dictionaries
            flatten: Whether to flatten nested JSON

        Returns:
            DataFrame with records
        """
        if not records:
            return pd.DataFrame()

        if flatten:
            # Use json_normalize for flattening nested structures
            df = pd.json_normalize(records, sep='_')
        else:
            df = pd.DataFrame(records)

        return df

    def _flatten_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flatten nested structures in existing DataFrame.

        Args:
            df: DataFrame with potential nested columns

        Returns:
            Flattened DataFrame
        """
        # Convert back to records and use json_normalize
        records = df.to_dict('records')
        return pd.json_normalize(records, sep='_')

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get JSON file metadata.

        Returns:
            Dictionary with file metadata including format detection
        """
        metadata = {
            "file_path": str(self.file_path),
            "file_size_bytes": self.get_file_size(),
            "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
            "is_empty": self.is_empty(),
        }

        # Detect format and get schema
        if not self.is_empty():
            try:
                is_jsonl = self._is_jsonl_format()
                metadata["format"] = "jsonl" if is_jsonl else "json_array"

                # Read first chunk to get schema
                first_chunk = next(self.load())

                metadata["columns"] = list(first_chunk.columns)
                metadata["column_count"] = len(first_chunk.columns)
                metadata["dtypes"] = {
                    col: str(dtype) for col, dtype in first_chunk.dtypes.items()
                }

                # Estimate total rows
                if is_jsonl:
                    # Count lines for JSONL
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for line in f if line.strip())
                    metadata["estimated_rows"] = line_count
                else:
                    # For JSON arrays, we'd need to parse entire file
                    # Just count from what we have
                    with open(self.file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            metadata["estimated_rows"] = len(data)
                        else:
                            metadata["estimated_rows"] = len(first_chunk)

            except Exception as e:
                metadata["error"] = f"Could not read metadata: {str(e)}"

        return metadata
