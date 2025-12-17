"""
Unit tests for file_lock context manager (AUDIT-2025-0001 fix).

Tests verify that the file lock context manager properly:
- Creates lock files
- Cleans up on normal exit
- Cleans up on exception
- Handles concurrent access
"""

import pytest
import tempfile
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

from validation_framework.cli import file_lock


@pytest.fixture
def temp_lock_file():
    """Create a temporary lock file path."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.lock') as f:
        lock_path = f.name
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        pass
    yield lock_path
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def temp_lock_dir():
    """Create a temporary directory for lock files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    import shutil
    try:
        shutil.rmtree(temp_dir)
    except Exception:
        pass


@pytest.mark.unit
class TestFileLock:
    """Unit tests for file_lock context manager."""

    def test_file_lock_creates_lock_file(self, temp_lock_file):
        """Test that entering the context creates the lock file."""
        assert not os.path.exists(temp_lock_file)

        with file_lock(temp_lock_file, is_windows=False):
            assert os.path.exists(temp_lock_file)

    def test_file_lock_removes_lock_on_exit(self, temp_lock_file):
        """Test that exiting the context removes the lock file."""
        with file_lock(temp_lock_file, is_windows=False):
            assert os.path.exists(temp_lock_file)

        assert not os.path.exists(temp_lock_file)

    def test_file_lock_removes_lock_on_exception(self, temp_lock_file):
        """Test that lock is released even when exception occurs."""
        try:
            with file_lock(temp_lock_file, is_windows=False):
                assert os.path.exists(temp_lock_file)
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert not os.path.exists(temp_lock_file)

    @pytest.mark.skipif(sys.platform == 'win32', reason="fcntl not available on Windows")
    def test_file_lock_blocks_concurrent_access(self, temp_lock_file):
        """Test that concurrent lock acquisition fails properly."""
        import fcntl

        lock_acquired = threading.Event()
        second_lock_failed = threading.Event()

        def hold_lock():
            with file_lock(temp_lock_file, is_windows=False):
                lock_acquired.set()
                time.sleep(0.5)

        def try_lock():
            lock_acquired.wait(timeout=2)
            try:
                with file_lock(temp_lock_file, is_windows=False):
                    pass
            except (OSError, BlockingIOError):
                second_lock_failed.set()

        t1 = threading.Thread(target=hold_lock)
        t2 = threading.Thread(target=try_lock)

        t1.start()
        t2.start()
        t1.join(timeout=3)
        t2.join(timeout=3)

        # Second lock should have failed
        assert second_lock_failed.is_set()

    def test_file_lock_windows_noop(self, temp_lock_file):
        """Test that lock is a no-op on Windows."""
        # Should not raise, even on non-Windows when is_windows=True
        with file_lock(temp_lock_file, is_windows=True):
            pass

    def test_file_lock_creates_parent_directory(self, temp_lock_dir):
        """Test that parent directories are created if needed."""
        nested_lock = os.path.join(temp_lock_dir, 'nested', 'dir', 'test.lock')

        with file_lock(nested_lock, is_windows=False):
            assert os.path.exists(nested_lock)

    def test_file_lock_multiple_sequential_acquires(self, temp_lock_file):
        """Test that the same lock can be acquired multiple times sequentially."""
        for _ in range(3):
            with file_lock(temp_lock_file, is_windows=False):
                assert os.path.exists(temp_lock_file)
            assert not os.path.exists(temp_lock_file)


@pytest.mark.unit
class TestFileLockIntegration:
    """Integration tests for file_lock in realistic scenarios."""

    def test_file_lock_exception_cleanup(self, temp_lock_file):
        """Test that exceptions don't leave stale lock files."""
        for i in range(3):
            try:
                with file_lock(temp_lock_file, is_windows=False):
                    if i % 2 == 0:
                        raise RuntimeError(f"Error {i}")
            except RuntimeError:
                pass

            # Lock should always be cleaned up
            assert not os.path.exists(temp_lock_file)
