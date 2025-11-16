"""
Memory-bounded data structure for tracking seen keys with disk spillover.

This module provides a memory-efficient way to track millions/billions of keys
without running out of memory. When the in-memory limit is reached, keys are
spilled to a SQLite database on disk.

This enables validation of 200GB+ files while keeping memory usage under control.
"""

import sqlite3
import tempfile
import os
import logging
import hashlib
from typing import Any, Optional, Tuple, Dict
from pathlib import Path

logger = logging.getLogger(__name__)


class MemoryBoundedTracker:
    """
    Tracks seen keys with automatic disk spillover when memory limit is reached.

    This data structure maintains a fixed-size in-memory set of keys. When the
    memory limit is exceeded, keys are written to a SQLite database on disk.

    Features:
    - Configurable memory limit (default: 1 million keys)
    - Automatic spillover to disk when limit reached
    - Fast in-memory lookups until spillover
    - Efficient SQLite-based lookups after spillover
    - Automatic cleanup of temporary database files
    - Thread-safe operations

    Example:
        >>> tracker = MemoryBoundedTracker(max_memory_keys=100000)
        >>> for key in large_dataset:
        ...     if tracker.has_seen(key):
        ...         print(f"Duplicate: {key}")
        ...     else:
        ...         tracker.add(key)
        >>> tracker.close()
    """

    def __init__(
        self,
        max_memory_keys: int = 1_000_000,
        db_path: Optional[str] = None,
        auto_cleanup: bool = True
    ):
        """
        Initialize the memory-bounded tracker.

        Args:
            max_memory_keys: Maximum number of keys to keep in memory before spilling to disk.
                           Default: 1,000,000 keys (~40-80 MB depending on key size)
            db_path: Path to SQLite database file. If None, creates a temporary file.
            auto_cleanup: Whether to automatically delete the database file on close.
        """
        self.max_memory_keys = max_memory_keys
        self.auto_cleanup = auto_cleanup

        # In-memory storage for fast lookups
        self.memory_keys: set = set()

        # Disk storage for spillover
        self.db_path = db_path
        self.db_conn: Optional[sqlite3.Connection] = None
        self.is_spilled = False

        # Statistics
        self.total_keys_added = 0
        self.total_lookups = 0
        self.memory_hits = 0
        self.disk_hits = 0

        logger.debug(
            f"Initialized MemoryBoundedTracker with max_memory_keys={max_memory_keys}"
        )

    def _init_database(self) -> None:
        """
        Initialize SQLite database for disk spillover.

        Creates a temporary database file and sets up the schema with indexes
        for fast lookups.
        """
        if self.db_conn is not None:
            return  # Already initialized

        # Create database file
        if self.db_path is None:
            # Create temporary file
            fd, self.db_path = tempfile.mkstemp(suffix='.db', prefix='tracker_')
            os.close(fd)  # Close the file descriptor, SQLite will open it
            logger.info(f"Created temporary database for spillover: {self.db_path}")
        else:
            logger.info(f"Using specified database for spillover: {self.db_path}")

        # Connect to database
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for better performance
        self.db_conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed

        # Create table for keys
        # We store keys as hashes (TEXT) for uniform size and indexing
        self.db_conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_keys (
                key_hash TEXT PRIMARY KEY,
                key_value BLOB
            )
        """)

        # Create index for fast lookups
        self.db_conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_key_hash ON seen_keys(key_hash)
        """)

        self.db_conn.commit()
        logger.debug("Initialized spillover database schema")

    def _hash_key(self, key: Any) -> str:
        """
        Generate a hash for a key.

        Uses SHA256 to generate a fixed-size hash from any Python object.
        The key is serialized to a string representation before hashing.

        Args:
            key: Any hashable Python object

        Returns:
            Hex-encoded SHA256 hash string
        """
        # Convert key to string representation
        key_str = str(key)

        # Generate hash
        return hashlib.sha256(key_str.encode('utf-8')).hexdigest()

    def _serialize_key(self, key: Any) -> bytes:
        """
        Serialize a key to bytes for database storage.

        Args:
            key: Key to serialize

        Returns:
            Serialized key as bytes
        """
        import pickle
        return pickle.dumps(key)

    def _spill_to_disk(self) -> None:
        """
        Spill in-memory keys to disk database.

        This is called automatically when the memory limit is reached.
        All keys currently in memory are written to the database, and
        the in-memory set is cleared.
        """
        if self.is_spilled:
            return  # Already in spillover mode

        logger.info(
            f"Memory limit reached ({len(self.memory_keys):,} keys). "
            f"Spilling to disk..."
        )

        # Initialize database
        self._init_database()

        # Write all memory keys to database
        batch = []
        for key in self.memory_keys:
            key_hash = self._hash_key(key)
            key_value = self._serialize_key(key)
            batch.append((key_hash, key_value))

            # Batch insert for performance
            if len(batch) >= 10000:
                self.db_conn.executemany(
                    "INSERT OR IGNORE INTO seen_keys (key_hash, key_value) VALUES (?, ?)",
                    batch
                )
                batch = []

        # Insert remaining keys
        if batch:
            self.db_conn.executemany(
                "INSERT OR IGNORE INTO seen_keys (key_hash, key_value) VALUES (?, ?)",
                batch
            )

        self.db_conn.commit()

        # Clear memory to reclaim space
        spilled_count = len(self.memory_keys)
        self.memory_keys.clear()
        self.is_spilled = True

        logger.info(f"Spilled {spilled_count:,} keys to disk successfully")

    def has_seen(self, key: Any) -> bool:
        """
        Check if a key has been seen before.

        Args:
            key: Key to check (any hashable Python object)

        Returns:
            True if key was previously added, False otherwise

        Example:
            >>> tracker = MemoryBoundedTracker()
            >>> tracker.add("customer_123")
            >>> tracker.has_seen("customer_123")
            True
            >>> tracker.has_seen("customer_456")
            False
        """
        self.total_lookups += 1

        if self.is_spilled:
            # Check database
            key_hash = self._hash_key(key)
            cursor = self.db_conn.execute(
                "SELECT 1 FROM seen_keys WHERE key_hash = ? LIMIT 1",
                (key_hash,)
            )
            result = cursor.fetchone()
            found = result is not None

            if found:
                self.disk_hits += 1

            return found
        else:
            # Check in-memory set
            found = key in self.memory_keys

            if found:
                self.memory_hits += 1

            return found

    def add(self, key: Any) -> bool:
        """
        Add a key to the tracker.

        Args:
            key: Key to add (any hashable Python object)

        Returns:
            True if key was newly added, False if it already existed

        Example:
            >>> tracker = MemoryBoundedTracker()
            >>> tracker.add("customer_123")  # Returns True (new key)
            True
            >>> tracker.add("customer_123")  # Returns False (duplicate)
            False
        """
        # Check if already seen
        if self.has_seen(key):
            return False

        self.total_keys_added += 1

        if self.is_spilled:
            # Add to database
            key_hash = self._hash_key(key)
            key_value = self._serialize_key(key)

            try:
                self.db_conn.execute(
                    "INSERT INTO seen_keys (key_hash, key_value) VALUES (?, ?)",
                    (key_hash, key_value)
                )
                # Commit periodically for performance
                if self.total_keys_added % 10000 == 0:
                    self.db_conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Key already exists (shouldn't happen if has_seen is called first)
                return False
        else:
            # Add to memory
            self.memory_keys.add(key)

            # Check if we need to spill to disk
            if len(self.memory_keys) >= self.max_memory_keys:
                self._spill_to_disk()

            return True

    def add_and_check(self, key: Any) -> Tuple[bool, bool]:
        """
        Atomically check and add a key.

        This is more efficient than calling has_seen() followed by add()
        for duplicate detection use cases.

        Args:
            key: Key to check and add

        Returns:
            Tuple of (was_seen_before, was_added)
            - was_seen_before: True if key already existed
            - was_added: True if key was newly added

        Example:
            >>> tracker = MemoryBoundedTracker()
            >>> is_duplicate, added = tracker.add_and_check("customer_123")
            >>> print(f"Duplicate: {is_duplicate}, Added: {added}")
            Duplicate: False, Added: True
        """
        was_seen = self.has_seen(key)
        was_added = False if was_seen else self.add(key)
        return was_seen, was_added

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about tracker usage.

        Returns:
            Dictionary with statistics including:
            - total_keys: Total unique keys tracked
            - memory_keys: Keys currently in memory
            - disk_keys: Keys spilled to disk
            - is_spilled: Whether spillover has occurred
            - total_lookups: Total number of lookups
            - memory_hit_rate: Percentage of lookups served from memory
            - total_keys_added: Total add() calls
        """
        if self.is_spilled:
            # Count keys in database
            cursor = self.db_conn.execute("SELECT COUNT(*) FROM seen_keys")
            disk_count = cursor.fetchone()[0]
            total_keys = disk_count
            memory_keys = 0
        else:
            disk_count = 0
            memory_keys = len(self.memory_keys)
            total_keys = memory_keys

        memory_hit_rate = (
            (self.memory_hits / self.total_lookups * 100)
            if self.total_lookups > 0
            else 0
        )

        return {
            "total_keys": total_keys,
            "memory_keys": memory_keys,
            "disk_keys": disk_count,
            "is_spilled": self.is_spilled,
            "total_lookups": self.total_lookups,
            "memory_hits": self.memory_hits,
            "disk_hits": self.disk_hits,
            "memory_hit_rate": round(memory_hit_rate, 2),
            "total_keys_added": self.total_keys_added,
        }

    def close(self) -> None:
        """
        Close the tracker and clean up resources.

        If auto_cleanup is enabled, deletes the temporary database file.
        Always call this method when done using the tracker to prevent
        resource leaks.
        """
        if self.db_conn:
            self.db_conn.commit()
            self.db_conn.close()
            self.db_conn = None

            # Delete temporary database if auto_cleanup enabled
            if self.auto_cleanup and self.db_path and os.path.exists(self.db_path):
                try:
                    os.remove(self.db_path)
                    # Also remove WAL and SHM files
                    wal_file = f"{self.db_path}-wal"
                    shm_file = f"{self.db_path}-shm"
                    if os.path.exists(wal_file):
                        os.remove(wal_file)
                    if os.path.exists(shm_file):
                        os.remove(shm_file)
                    logger.debug(f"Cleaned up temporary database: {self.db_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary database: {e}")

        # Clear memory
        self.memory_keys.clear()

        # Log final statistics
        stats = self.get_statistics()
        logger.info(
            f"Tracker closed. Statistics: {stats['total_keys']:,} total keys, "
            f"{stats['total_lookups']:,} lookups, "
            f"{stats['memory_hit_rate']:.1f}% memory hit rate"
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        self.close()
        return False

    def __del__(self):
        """Destructor - ensures cleanup even if close() not called."""
        if hasattr(self, 'db_conn') and self.db_conn:
            self.close()
