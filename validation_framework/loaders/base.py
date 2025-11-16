"""Base data loader interface."""

from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, Optional
from pathlib import Path
import pandas as pd


class DataLoader(ABC):
    """Base class for data loaders."""

    def __init__(self, file_path: str, chunk_size: int = 50000, **kwargs: Any) -> None:
        """
        Initialize data loader.

        Args:
            file_path: Path to the data file
            chunk_size: Number of rows per chunk for memory-efficient processing
            **kwargs: Additional loader-specific parameters
        """
        self.file_path: Path = Path(file_path)
        self.chunk_size: int = chunk_size
        self.kwargs: Dict[str, Any] = kwargs

        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

    @abstractmethod
    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load data in chunks.

        Yields:
            DataFrames containing chunks of data
        """
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get file metadata.

        Returns:
            Dictionary containing metadata (size, row count estimate, etc.)
        """
        pass

    def get_file_size(self) -> int:
        """Get file size in bytes."""
        return self.file_path.stat().st_size

    def is_empty(self) -> bool:
        """Check if file is empty (0 bytes)."""
        return self.get_file_size() == 0
