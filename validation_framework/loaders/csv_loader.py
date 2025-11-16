"""CSV data loader with chunked reading for large files."""

from typing import Iterator, Dict, Any
import pandas as pd
from validation_framework.loaders.base import DataLoader


class CSVLoader(DataLoader):
    """Loader for CSV and delimited text files."""

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load CSV data in chunks.

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
            # Return empty DataFrame with no columns
            yield pd.DataFrame()

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
