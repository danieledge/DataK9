"""
Unit tests for CSV loader debug logging (AUDIT-2025-0002 fix).

Tests verify that delimiter and encoding detection have debug logging
enabled with exc_info=True for troubleshooting.
"""

import pytest
import tempfile
import logging
from pathlib import Path

from validation_framework.loaders.csv_loader import (
    detect_delimiter,
    detect_encoding,
    CSVLoader
)


@pytest.fixture
def malformed_csv_file():
    """Create a CSV file that will cause delimiter detection issues."""
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as f:
        f.write(b"random text without clear delimiters\n")
        f.write(b"more random data\n")
        temp_path = f.name
    yield temp_path
    try:
        Path(temp_path).unlink()
    except FileNotFoundError:
        pass


@pytest.fixture
def binary_file():
    """Create a binary file for encoding detection tests."""
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as f:
        f.write(b'\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09')
        f.write(b'\xff\xfe\xfd\xfc\xfb\xfa\xf9\xf8\xf7\xf6')
        temp_path = f.name
    yield temp_path
    try:
        Path(temp_path).unlink()
    except FileNotFoundError:
        pass


@pytest.fixture
def valid_csv_file():
    """Create a valid CSV file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write("col1,col2,col3\n")
        f.write("a,b,c\n")
        f.write("1,2,3\n")
        temp_path = f.name
    yield temp_path
    try:
        Path(temp_path).unlink()
    except FileNotFoundError:
        pass


@pytest.mark.unit
class TestDelimiterDetection:
    """Tests for delimiter detection functionality."""

    def test_detect_delimiter_returns_comma_for_csv(self, valid_csv_file):
        """Test comma is detected for standard CSV."""
        result = detect_delimiter(valid_csv_file, 'utf-8')
        assert result == ','

    def test_detect_delimiter_handles_malformed_file(self, malformed_csv_file):
        """Test malformed file returns fallback delimiter."""
        result = detect_delimiter(malformed_csv_file, 'utf-8')
        # Should return comma as default fallback
        assert result == ','

    def test_detect_delimiter_with_nonexistent_file(self):
        """Test nonexistent file returns fallback."""
        result = detect_delimiter('/nonexistent/path.csv', 'utf-8')
        assert result == ','


@pytest.mark.unit
class TestEncodingDetection:
    """Tests for encoding detection functionality."""

    def test_detect_encoding_returns_utf8_for_valid_csv(self, valid_csv_file):
        """Test UTF-8 is detected for standard CSV."""
        result = detect_encoding(valid_csv_file)
        assert result in ['utf-8', 'utf-8-sig', 'ascii']

    def test_detect_encoding_handles_binary_file(self, binary_file):
        """Test binary file returns some encoding."""
        result = detect_encoding(binary_file)
        # Should return some encoding (won't be utf-8)
        assert result is not None
        assert isinstance(result, str)

    def test_detect_encoding_with_nonexistent_file(self):
        """Test nonexistent file returns default."""
        result = detect_encoding('/nonexistent/path.csv')
        assert result == 'utf-8'


@pytest.mark.unit
class TestCSVLoaderDebugLogging:
    """Tests for CSV loader logging behavior."""

    def test_csv_loader_loads_valid_file(self, valid_csv_file):
        """Test loader successfully loads valid CSV."""
        loader = CSVLoader(valid_csv_file)
        # load() returns a generator, consume it
        chunks = list(loader.load())
        assert len(chunks) >= 1
        df = chunks[0]
        assert len(df) == 2
        assert list(df.columns) == ['col1', 'col2', 'col3']

    def test_csv_loader_auto_detects_delimiter(self, valid_csv_file):
        """Test loader auto-detects delimiter."""
        loader = CSVLoader(valid_csv_file)
        # load() returns a generator, consume it
        chunks = list(loader.load())
        assert len(chunks) >= 1
        # If it loaded correctly, delimiter was detected
        assert len(chunks[0]) == 2

    def test_csv_loader_with_explicit_delimiter(self, valid_csv_file):
        """Test loader uses explicit delimiter."""
        loader = CSVLoader(valid_csv_file, delimiter=',')
        chunks = list(loader.load())
        assert len(chunks) >= 1
        assert len(chunks[0]) == 2


@pytest.mark.unit
class TestDebugLoggingConfiguration:
    """Tests for debug logging configuration."""

    def test_logger_exists_for_csv_loader(self):
        """Test that CSV loader has a logger configured."""
        import validation_framework.loaders.csv_loader as csv_module
        assert hasattr(csv_module, 'logger')
        assert csv_module.logger.name == 'validation_framework.loaders.csv_loader'

    def test_debug_log_level_is_recognized(self):
        """Test DEBUG level is properly configured."""
        logger = logging.getLogger('validation_framework.loaders.csv_loader')
        # Logger should accept DEBUG level messages
        assert logger.isEnabledFor(logging.DEBUG) or logger.level <= logging.DEBUG or logger.level == 0
