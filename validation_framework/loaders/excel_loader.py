"""Excel data loader."""

from typing import Iterator, Dict, Any
import pandas as pd
from validation_framework.loaders.base import DataLoader


class ExcelLoader(DataLoader):
    """Loader for Excel files (.xls, .xlsx)."""

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load Excel data in chunks.

        Note: Excel files are typically smaller than CSVs. We load the full sheet
        but can still yield in chunks for consistency.

        Yields:
            DataFrames containing chunks of data
        """
        sheet_name = self.kwargs.get("sheet_name", 0)
        header = self.kwargs.get("header", 0)

        try:
            # Read the full Excel sheet (Excel files are typically smaller)
            df = pd.read_excel(
                self.file_path,
                sheet_name=sheet_name,
                header=header,
                engine='openpyxl',  # Use openpyxl for .xlsx files
            )

            # Yield in chunks for consistency with other loaders
            if len(df) == 0:
                yield df
            else:
                for start in range(0, len(df), self.chunk_size):
                    end = min(start + self.chunk_size, len(df))
                    yield df.iloc[start:end].copy()

        except Exception as e:
            raise RuntimeError(f"Error loading Excel file {self.file_path}: {str(e)}")

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get Excel file metadata.

        Returns:
            Dictionary with file metadata
        """
        metadata = {
            "file_path": str(self.file_path),
            "file_size_bytes": self.get_file_size(),
            "file_size_mb": round(self.get_file_size() / (1024 * 1024), 2),
            "is_empty": self.is_empty(),
        }

        if not self.is_empty():
            try:
                sheet_name = self.kwargs.get("sheet_name", 0)
                header = self.kwargs.get("header", 0)

                # Read to get schema (we need to load Excel files fully anyway)
                df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name,
                    header=header,
                    engine='openpyxl',
                )

                metadata["columns"] = list(df.columns)
                metadata["column_count"] = len(df.columns)
                metadata["dtypes"] = {col: str(dtype) for col, dtype in df.dtypes.items()}
                metadata["total_rows"] = len(df)

                # Get sheet names
                xl_file = pd.ExcelFile(self.file_path, engine='openpyxl')
                metadata["sheet_names"] = xl_file.sheet_names

            except Exception as e:
                metadata["error"] = f"Could not read metadata: {str(e)}"

        return metadata
