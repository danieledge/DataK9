"""
Unit tests for file_lock context manager (AUDIT-2025-0001 fix).

Tests verify that the file lock context manager properly:
- Creates lock files
- Cleans up on normal exit
- Cleans up on exception
- Blocks concurrent access
- Handles Windows platform correctly
"""

import pytest
import tempfile
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the file_lock context manager
from validation_framework.cli import file_lock


@pytest.fixture
def temp_lock_file():
    """Create a temporary lock file path."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.lock') as f:
        lock_path = f.name

    # Remove the file - we just want the path
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        pass

    yield lock_path

    # Cleanup
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        pass


@pytest.fixture
def temp_lock_dir():
    """Create a temporary directory for lock files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir

    # Cleanup
    import shutil
    try:
        shutil.rmtree(temp_dir)
    except FileNotFoundError:
        pass


@pytest.mark.unit
class TestFileLock:
    """Tests for file_lock context manager."""

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_creates_lock_file(self, temp_lock_file):
        """Test that lock file is created when entering context."""
        assert not os.path.exists(temp_lock_file), "Lock file should not exist initially"

        with file_lock(temp_lock_file, is_windows=False):
            # Lock file should exist while in context
            assert os.path.exists(temp_lock_file), "Lock file should exist inside context"

            # Verify lock file contains process info
            with open(temp_lock_file, 'r') as f:
                content = f.read()
                assert str(os.getpid()) in content, "Lock file should contain process ID"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_removes_lock_on_exit(self, temp_lock_file):
        """Test that lock file is removed on normal exit."""
        with file_lock(temp_lock_file, is_windows=False):
            assert os.path.exists(temp_lock_file), "Lock file should exist inside context"

        # Lock file should be removed after context exits
        assert not os.path.exists(temp_lock_file), "Lock file should be removed after normal exit"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_removes_lock_on_exception(self, temp_lock_file):
        """Test that lock file is removed when exception is raised inside context."""
        with pytest.raises(ValueError):
            with file_lock(temp_lock_file, is_windows=False):
                assert os.path.exists(temp_lock_file), "Lock file should exist inside context"
                raise ValueError("Test exception")

        # Lock file should still be removed even though exception was raised
        assert not os.path.exists(temp_lock_file), "Lock file should be removed after exception"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_blocks_concurrent_access(self, temp_lock_file):
        """Test that second lock attempt fails with proper error when lock is held."""
        import fcntl

        # Acquire lock in main thread
        with file_lock(temp_lock_file, is_windows=False):
            # Try to acquire lock in separate thread (simulating concurrent process)
            exception_caught = []

            def try_acquire_lock():
                try:
                    with file_lock(temp_lock_file, is_windows=False):
                        pass  # Should not reach here
                except BlockingIOError as e:
                    exception_caught.append(e)

            thread = threading.Thread(target=try_acquire_lock)
            thread.start()
            thread.join(timeout=2)

            # Verify that BlockingIOError was raised
            assert len(exception_caught) == 1, "Second lock attempt should raise BlockingIOError"
            assert isinstance(exception_caught[0], BlockingIOError), "Exception should be BlockingIOError"

    def test_file_lock_windows_noop(self, temp_lock_file, capsys):
        """Test that Windows no-op mode works without creating lock file."""
        # On Windows, file_lock should yield without doing anything
        with file_lock(temp_lock_file, is_windows=True):
            # Should not create lock file on Windows
            pass

        # Verify warning was printed (via po.warning, not logger)
        captured = capsys.readouterr()
        assert "not supported on Windows" in captured.out, "Should warn about Windows limitation"

        # Lock file should not be created
        assert not os.path.exists(temp_lock_file), "Lock file should not be created on Windows"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_creates_parent_directory(self, temp_lock_dir):
        """Test that lock file parent directory is created if it doesn't exist."""
        # Create path with non-existent subdirectory
        nested_lock_path = os.path.join(temp_lock_dir, "subdir1", "subdir2", "test.lock")
        assert not os.path.exists(os.path.dirname(nested_lock_path)), "Parent directory should not exist"

        with file_lock(nested_lock_path, is_windows=False):
            assert os.path.exists(nested_lock_path), "Lock file should be created"
            assert os.path.exists(os.path.dirname(nested_lock_path)), "Parent directory should be created"

        # Verify cleanup
        assert not os.path.exists(nested_lock_path), "Lock file should be removed"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_cleanup_error_handling(self, temp_lock_file, caplog):
        """Test that cleanup errors are logged but don't prevent normal cleanup flow."""
        import logging
        caplog.set_level(logging.DEBUG)

        with file_lock(temp_lock_file, is_windows=False):
            # Manually remove lock file to trigger cleanup error
            os.unlink(temp_lock_file)

        # Check that debug message was logged about removal error
        debug_messages = [record.message for record in caplog.records if record.levelno == logging.DEBUG]
        assert any("Error removing lock file" in msg for msg in debug_messages), \
            "Should log debug message when lock file removal fails"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_multiple_sequential_acquires(self, temp_lock_file):
        """Test that lock can be acquired multiple times sequentially."""
        # First acquisition
        with file_lock(temp_lock_file, is_windows=False):
            assert os.path.exists(temp_lock_file)

        assert not os.path.exists(temp_lock_file), "Lock should be released"

        # Second acquisition - should succeed
        with file_lock(temp_lock_file, is_windows=False):
            assert os.path.exists(temp_lock_file)

        assert not os.path.exists(temp_lock_file), "Lock should be released again"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_write_process_info(self, temp_lock_file):
        """Test that lock file contains process ID and timestamp."""
        with file_lock(temp_lock_file, is_windows=False):
            # Read lock file content
            with open(temp_lock_file, 'r') as f:
                lines = f.readlines()

            assert len(lines) >= 2, "Lock file should contain at least 2 lines"

            # First line should be PID
            pid = lines[0].strip()
            assert pid.isdigit(), "First line should be process ID"
            assert int(pid) == os.getpid(), "PID should match current process"

            # Second line should be ISO timestamp
            timestamp = lines[1].strip()
            assert len(timestamp) > 0, "Second line should contain timestamp"
            # Basic ISO format check (YYYY-MM-DD)
            assert timestamp[4] == '-' and timestamp[7] == '-', "Should be ISO format timestamp"

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_exclusive_lock_type(self, temp_lock_file):
        """Test that LOCK_EX (exclusive) lock is acquired."""
        import fcntl

        with file_lock(temp_lock_file, is_windows=False):
            # Try to acquire shared lock on same file - should fail if exclusive lock is held
            with open(temp_lock_file, 'r') as test_fd:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(test_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_logging_messages(self, temp_lock_file, caplog):
        """Test that appropriate log messages are generated."""
        import logging
        caplog.set_level(logging.INFO)

        with file_lock(temp_lock_file, is_windows=False):
            pass

        # Check for acquisition and release log messages
        log_messages = [record.message for record in caplog.records]

        assert any("Acquired lock file" in msg for msg in log_messages), \
            "Should log when lock is acquired"
        assert any("Released lock file" in msg for msg in log_messages), \
            "Should log when lock is released"


@pytest.mark.unit
class TestFileLockIntegration:
    """Integration tests for file_lock with realistic scenarios."""

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_prevents_data_corruption(self, temp_lock_file):
        """Test that file lock prevents race conditions in concurrent access."""
        shared_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        shared_file.write('0')
        shared_file.close()

        try:
            successful_writes = []
            blocked_attempts = []

            def write_to_file(thread_id):
                """Simulate write operation protected by lock."""
                try:
                    with file_lock(temp_lock_file, is_windows=False):
                        # Read current value with explicit sync
                        with open(shared_file.name, 'r') as f:
                            content = f.read()
                            current_value = int(content) if content else 0

                        # Simulate processing time
                        time.sleep(0.02)

                        # Write incremented value with explicit sync
                        with open(shared_file.name, 'w') as f:
                            f.write(str(current_value + 1))
                            f.flush()
                            os.fsync(f.fileno())  # Force write to disk

                        successful_writes.append(thread_id)
                except BlockingIOError:
                    # Expected for concurrent attempts with LOCK_NB
                    blocked_attempts.append(thread_id)

            # Start multiple threads trying to increment the value
            threads = []
            for i in range(3):
                thread = threading.Thread(target=write_to_file, args=(i,))
                threads.append(thread)

            # Start all threads at once to ensure overlap
            for thread in threads:
                thread.start()

            # Wait for all threads
            for thread in threads:
                thread.join(timeout=10)

            # Verify behavior: lock should either block concurrent access OR serialize it
            total_attempts = len(successful_writes) + len(blocked_attempts)
            assert total_attempts == 3, f"Expected 3 total attempts, got {total_attempts}"

            # Read final value with explicit sync
            time.sleep(0.01)  # Small delay to ensure all writes are flushed
            with open(shared_file.name, 'r') as f:
                final_value = int(f.read())

            # KEY TEST: Value should equal number of successful writes
            # This proves no race conditions occurred - each successful write
            # saw the correct value, incremented it, and wrote it back
            assert final_value == len(successful_writes), \
                f"Race condition detected: final value {final_value} != successful writes {len(successful_writes)}"

            # Verify at least one thread completed successfully
            assert len(successful_writes) > 0, "At least one thread should have completed"

        finally:
            os.unlink(shared_file.name)
            try:
                os.unlink(temp_lock_file)
            except FileNotFoundError:
                pass

    @pytest.mark.skipif(sys.platform.startswith('win'), reason="Test requires Unix-style file locking")
    def test_file_lock_exception_doesnt_leave_stale_lock(self, temp_lock_file):
        """Test that exceptions inside locked section don't leave stale locks."""
        # Verify no lock exists
        assert not os.path.exists(temp_lock_file)

        # Raise exception inside lock
        try:
            with file_lock(temp_lock_file, is_windows=False):
                raise RuntimeError("Simulated processing error")
        except RuntimeError:
            pass  # Expected

        # Lock should be cleaned up
        assert not os.path.exists(temp_lock_file), "Stale lock file should not exist"

        # Subsequent lock attempt should succeed
        with file_lock(temp_lock_file, is_windows=False):
            assert os.path.exists(temp_lock_file)

        assert not os.path.exists(temp_lock_file)
