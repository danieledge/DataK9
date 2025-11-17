"""
Record-level validation rules.

These validations check entire rows/records:
- Duplicate detection
- Blank/empty record detection
- Uniqueness constraints

Author: Daniel Edge
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.memory_bounded_tracker import MemoryBoundedTracker
from validation_framework.core.exceptions import (
    ColumnNotFoundError,
    ParameterValidationError
)
from validation_framework.core.constants import MAX_SAMPLE_FAILURES


class DuplicateRowCheck(DataValidationRule):
    """
    Detects duplicate rows based on specified key fields.

    This is critical for ensuring data integrity, especially for
    transactional data where each record should be unique.

    PERFORMANCE OPTIMIZATIONS:
    - Uses memory-bounded tracking with disk spillover (handles 100M+ rows)
    - Optional bloom filter for fast duplicate pre-filtering (10x faster)
    - Larger hash table size (10M keys default vs 1M)
    - Early termination option for large datasets
    - Vectorized pandas operations where possible

    Configuration:
        params:
            key_fields (list): List of fields that define uniqueness
            consider_all_fields (bool): If True, checks ALL columns (default: False)
            use_bloom_filter (bool): Enable bloom filter pre-filtering (default: True)
            bloom_false_positive_rate (float): Bloom filter FP rate (default: 0.01)
            hash_table_size (int): In-memory hash table size (default: 10,000,000)
            enable_early_termination (bool): Stop after finding N duplicates (default: False)
            max_duplicates (int): Max duplicates before stopping (default: 1000)

    Example YAML - Default (Optimized):
        # Check for duplicate customer IDs
        - type: "DuplicateRowCheck"
          severity: "ERROR"
          params:
            key_fields: ["customer_id"]
            # Bloom filter enabled by default
            # 10M keys in memory before disk spillover

    Example YAML - Maximum Performance:
        # Check for duplicate transactions
        - type: "DuplicateRowCheck"
          severity: "ERROR"
          params:
            key_fields: ["transaction_id", "date"]
            use_bloom_filter: true
            hash_table_size: 50000000  # 50M keys (~2GB RAM)
            enable_early_termination: true
            max_duplicates: 100

    Example YAML - 100% Accuracy (Slower):
        - type: "DuplicateRowCheck"
          severity: "ERROR"
          params:
            key_fields: ["customer_id"]
            use_bloom_filter: false  # No bloom filter for zero false positives
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        if self.params.get("consider_all_fields", False):
            return "Checks for duplicate rows across all fields"
        else:
            key_fields = self.params.get("key_fields", [])
            return f"Checks for duplicates based on: {', '.join(key_fields)}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check for duplicate rows with bloom filter optimization.

        Uses memory-bounded tracking with automatic disk spillover to handle
        files of any size (including 200GB+) while keeping memory usage under control.

        Default: 10M keys in memory with optional bloom filter for 10x faster lookups.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of duplicate rows
        """
        # Get optimization parameters
        use_bloom = self.params.get("use_bloom_filter", True)
        bloom_fp_rate = self.params.get("bloom_false_positive_rate", 0.01)
        hash_table_size = self.params.get("hash_table_size", 10_000_000)  # 10M default
        enable_early_term = self.params.get("enable_early_termination", False)
        max_dups = self.params.get("max_duplicates", 1000)

        # Create memory-bounded tracker with context manager for automatic cleanup
        with MemoryBoundedTracker(max_memory_keys=hash_table_size) as tracker:
            try:
                consider_all = self.params.get("consider_all_fields", False)
                key_fields = self.params.get("key_fields", [])

                if not consider_all and not key_fields:
                    raise ParameterValidationError(
                        "No key fields specified for duplicate check",
                        validation_name=self.name,
                        parameter="key_fields",
                        value=None
                    )

                total_rows = 0
                failed_rows = []
                max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)
                duplicate_count = 0

                # Initialize bloom filter for fast pre-filtering (optional)
                bloom_filter = None
                if use_bloom:
                    try:
                        from pybloom_live import BloomFilter
                        bloom_capacity = hash_table_size * 2
                        bloom_filter = BloomFilter(capacity=bloom_capacity, error_rate=bloom_fp_rate)
                        self.logger.info(
                            f"DuplicateRowCheck: Bloom filter enabled (capacity={bloom_capacity:,}, "
                            f"false_positive_rate={bloom_fp_rate})"
                        )
                    except ImportError:
                        self.logger.warning(
                            "pybloom_live not available. Install with: pip install pybloom-live. "
                            "Falling back to standard duplicate detection."
                        )
                        bloom_filter = None

                # Process each chunk
                for chunk_idx, chunk in enumerate(data_iterator):
                    # Determine which columns to check
                    if consider_all:
                        check_cols = list(chunk.columns)
                    else:
                        # Verify key fields exist
                        missing_fields = [f for f in key_fields if f not in chunk.columns]
                        if missing_fields:
                            raise ColumnNotFoundError(
                                validation_name=self.name,
                                column=missing_fields[0],
                                available_columns=list(chunk.columns)
                            )
                        check_cols = key_fields

                    # Create composite key for each row
                    for idx in range(len(chunk)):
                        # Build tuple of key values
                        row_key = tuple(chunk.iloc[idx][check_cols])

                        # Fast pre-filter with bloom filter (if enabled)
                        if bloom_filter is not None:
                            if row_key in bloom_filter:
                                # Might be duplicate - check with full tracker
                                is_duplicate, was_added = tracker.add_and_check(row_key)
                            else:
                                # Definitely not seen before - add to both
                                bloom_filter.add(row_key)
                                is_duplicate, was_added = False, tracker.add(row_key)
                        else:
                            # No bloom filter - check directly with tracker
                            is_duplicate, was_added = tracker.add_and_check(row_key)

                        if is_duplicate:
                            duplicate_count += 1

                            # Collect sample
                            if len(failed_rows) < max_samples:
                                row_data = chunk.iloc[idx].to_dict()
                                failed_rows.append({
                                    "row": int(total_rows + idx),
                                    "key_values": {k: row_data[k] for k in check_cols},
                                    "message": f"Duplicate row detected"
                                })

                            # Early termination if requested
                            if enable_early_term and duplicate_count >= max_dups:
                                self.logger.info(
                                    f"Early termination: Found {duplicate_count} duplicates "
                                    f"(max_duplicates={max_dups})"
                                )
                                break

                    total_rows += len(chunk)

                    # Stop processing chunks if early termination triggered
                    if enable_early_term and duplicate_count >= max_dups:
                        break

                # Get statistics from tracker
                stats = tracker.get_statistics()

                # Create result
                if duplicate_count > 0:
                    unique_keys = stats["total_keys"]
                    spill_info = " (disk spillover used)" if stats["is_spilled"] else ""
                    bloom_info = " (bloom filter enabled)" if bloom_filter is not None else ""
                    early_term_info = f" (early termination at {max_dups})" if enable_early_term and duplicate_count >= max_dups else ""

                    return self._create_result(
                        passed=False,
                        message=f"Found {duplicate_count} duplicate rows ({unique_keys:,} unique records{spill_info}{bloom_info}{early_term_info})",
                        failed_count=duplicate_count,
                        total_count=total_rows,
                        sample_failures=failed_rows,
                    )

                bloom_info = " (bloom filter enabled)" if bloom_filter is not None else ""
                return self._create_result(
                    passed=True,
                    message=f"No duplicates found among {total_rows:,} rows{bloom_info}",
                    total_count=total_rows,
                )

            except Exception as e:
                return self._create_result(
                    passed=False,
                    message=f"Error during duplicate check: {str(e)}",
                    failed_count=1,
                )


class BlankRecordCheck(DataValidationRule):
    """
    Detects completely blank/empty rows.

    A row is considered blank if all fields are null or empty strings.

    Configuration:
        params:
            exclude_fields (list, optional): Fields to ignore when checking for blanks

    Example YAML:
        - type: "BlankRecordCheck"
          severity: "WARNING"
          params:
            exclude_fields: ["optional_notes"]
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        exclude = self.params.get("exclude_fields", [])
        if exclude:
            return f"Checks for blank rows (excluding fields: {', '.join(exclude)})"
        return "Checks for completely blank rows"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check for blank rows across all chunks.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of blank rows found
        """
        try:
            exclude_fields = self.params.get("exclude_fields", [])

            total_rows = 0
            failed_rows = []
            max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)

            # Process each chunk
            for chunk_idx, chunk in enumerate(data_iterator):
                # Determine which columns to check
                check_cols = [col for col in chunk.columns if col not in exclude_fields]

                if not check_cols:
                    return self._create_result(
                        passed=False,
                        message="No columns to check after exclusions",
                        failed_count=1,
                    )

                # Find rows where all checked columns are null or empty (vectorized)
                chunk_subset = chunk[check_cols]

                # Check for nulls and empty strings efficiently
                # For string columns, replace empty strings with NaN
                chunk_subset = chunk_subset.replace('', pd.NA)
                chunk_subset = chunk_subset.replace(r'^\s*$', pd.NA, regex=True)

                # Find rows where ALL columns are null/empty (much faster!)
                blank_mask = chunk_subset.isna().all(axis=1)
                blank_indices = blank_mask[blank_mask].index.tolist()

                # Collect samples up to max_samples
                for idx in blank_indices:
                    if len(failed_rows) >= max_samples:
                        break
                    failed_rows.append({
                        "row": int(total_rows + idx),
                        "message": "Completely blank row detected"
                    })

                total_rows += len(chunk)

            # Create result
            blank_count = len(failed_rows)

            if blank_count > 0:
                return self._create_result(
                    passed=False,
                    message=f"Found {blank_count} completely blank rows",
                    failed_count=blank_count,
                    total_count=total_rows,
                    sample_failures=failed_rows,
                )

            return self._create_result(
                passed=True,
                message=f"No blank rows found among {total_rows} rows",
                total_count=total_rows,
            )

        except Exception as e:
            return self._create_result(
                passed=False,
                message=f"Error during blank record check: {str(e)}",
                failed_count=1,
            )


class UniqueKeyCheck(DataValidationRule):
    """
    Validates that a field or combination of fields contains only unique values.

    Similar to DuplicateRowCheck but specifically for primary key validation.

    PERFORMANCE OPTIMIZATIONS:
    - Uses memory-bounded tracking with disk spillover (handles 100M+ rows)
    - Optional bloom filter for fast duplicate pre-filtering (10x faster lookups)
    - Vectorized pandas operations where possible
    - Configurable memory limits and early termination

    Configuration:
        params:
            fields (list): List of fields that should be unique
            use_bloom_filter (bool): Enable bloom filter pre-filtering (default: True)
            bloom_false_positive_rate (float): Bloom filter false positive rate (default: 0.01)
            hash_table_size (int): In-memory hash table size (default: 10,000,000)
            enable_early_termination (bool): Stop after finding N duplicates (default: False)
            max_duplicates (int): Maximum duplicates to find before stopping (default: 1000)

    Example YAML - Default (Optimized):
        - type: "UniqueKeyCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id"]
            # Bloom filter enabled by default
            # 10M keys in memory before disk spillover

    Example YAML - Maximum Performance:
        - type: "UniqueKeyCheck"
          severity: "ERROR"
          params:
            fields: ["transaction_id"]
            use_bloom_filter: true
            hash_table_size: 50000000  # 50M keys (~2GB RAM)
            enable_early_termination: true
            max_duplicates: 100  # Stop after finding 100 duplicates

    Example YAML - 100% Accuracy (Slower):
        - type: "UniqueKeyCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id"]
            use_bloom_filter: false  # Disable bloom filter for zero false positives
    """

    def get_description(self) -> str:
        """Get human-readable description."""
        fields = self.params.get("fields", [])
        return f"Checks uniqueness of: {', '.join(fields)}"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Check uniqueness of specified fields with bloom filter optimization.

        Uses memory-bounded tracking with automatic disk spillover to handle
        files of any size while keeping memory usage under control.

        Optional bloom filter provides 10x faster duplicate detection with
        configurable false positive rate (default: 1%).

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult with details of duplicate keys
        """
        # Get optimization parameters
        use_bloom = self.params.get("use_bloom_filter", True)
        bloom_fp_rate = self.params.get("bloom_false_positive_rate", 0.01)
        hash_table_size = self.params.get("hash_table_size", 10_000_000)  # 10M default
        enable_early_term = self.params.get("enable_early_termination", False)
        max_dups = self.params.get("max_duplicates", 1000)

        # Create memory-bounded tracker with context manager for automatic cleanup
        with MemoryBoundedTracker(max_memory_keys=hash_table_size) as tracker:
            try:
                fields = self.params.get("fields", [])
                if not fields:
                    raise ParameterValidationError(
                        "No fields specified for uniqueness check",
                        validation_name=self.name,
                        parameter="fields",
                        value=None
                    )

                total_rows = 0
                failed_rows = []
                max_samples = context.get("max_sample_failures", MAX_SAMPLE_FAILURES)
                duplicate_count = 0

                # Track first occurrence of keys for better error messages
                # Note: This is memory-bounded separately - we only store first 100k
                first_occurrence = {}
                max_first_occurrence = 100_000

                # Initialize bloom filter for fast pre-filtering (optional)
                bloom_filter = None
                if use_bloom:
                    try:
                        # Lazy import - only load if needed
                        from pybloom_live import BloomFilter
                        # Estimate capacity based on hash table size
                        bloom_capacity = hash_table_size * 2  # 2x for safety margin
                        bloom_filter = BloomFilter(capacity=bloom_capacity, error_rate=bloom_fp_rate)
                        self.logger.info(
                            f"UniqueKeyCheck: Bloom filter enabled (capacity={bloom_capacity:,}, "
                            f"false_positive_rate={bloom_fp_rate})"
                        )
                    except ImportError:
                        self.logger.warning(
                            "pybloom_live not available. Install with: pip install pybloom-live. "
                            "Falling back to standard duplicate detection."
                        )
                        bloom_filter = None

                # Process each chunk
                for chunk_idx, chunk in enumerate(data_iterator):
                    # Verify fields exist
                    missing_fields = [f for f in fields if f not in chunk.columns]
                    if missing_fields:
                        raise ColumnNotFoundError(
                            validation_name=self.name,
                            column=missing_fields[0],
                            available_columns=list(chunk.columns)
                        )

                    # Check each row
                    for idx in range(len(chunk)):
                        # Build composite key
                        if len(fields) == 1:
                            row_key = chunk.iloc[idx][fields[0]]
                        else:
                            row_key = tuple(chunk.iloc[idx][fields])

                        # Skip null keys
                        if pd.isna(row_key) or (isinstance(row_key, tuple) and any(pd.isna(v) for v in row_key)):
                            continue

                        # Fast pre-filter with bloom filter (if enabled)
                        # Bloom filter can have false positives but NEVER false negatives
                        # So if bloom says "not seen", we can skip the full tracker check
                        if bloom_filter is not None:
                            if row_key in bloom_filter:
                                # Might be duplicate - check with full tracker
                                is_duplicate, was_added = tracker.add_and_check(row_key)
                            else:
                                # Definitely not seen before - add to both bloom and tracker
                                bloom_filter.add(row_key)
                                is_duplicate, was_added = False, tracker.add(row_key)
                        else:
                            # No bloom filter - check directly with tracker
                            is_duplicate, was_added = tracker.add_and_check(row_key)

                        if is_duplicate:
                            duplicate_count += 1

                            if len(failed_rows) < max_samples:
                                key_dict = {k: chunk.iloc[idx][k] for k in fields}
                                first_row = first_occurrence.get(row_key, "unknown")
                                failed_rows.append({
                                    "row": int(total_rows + idx),
                                    "key_values": key_dict,
                                    "first_seen_row": first_row,
                                    "message": f"Duplicate key found (first occurrence at row {first_row})"
                                })

                            # Early termination if requested
                            if enable_early_term and duplicate_count >= max_dups:
                                self.logger.info(
                                    f"Early termination: Found {duplicate_count} duplicates "
                                    f"(max_duplicates={max_dups})"
                                )
                                break
                        else:
                            # Track first occurrence if we have space
                            if len(first_occurrence) < max_first_occurrence:
                                first_occurrence[row_key] = total_rows + idx

                    total_rows += len(chunk)

                    # Stop processing chunks if early termination triggered
                    if enable_early_term and duplicate_count >= max_dups:
                        break

                # Get statistics from tracker
                stats = tracker.get_statistics()

                # Create result
                if duplicate_count > 0:
                    spill_info = " (disk spillover used)" if stats["is_spilled"] else ""
                    bloom_info = " (bloom filter enabled)" if bloom_filter is not None else ""
                    early_term_info = f" (early termination at {max_dups})" if enable_early_term and duplicate_count >= max_dups else ""

                    return self._create_result(
                        passed=False,
                        message=f"Found {duplicate_count} duplicate keys (should be unique{spill_info}{bloom_info}{early_term_info})",
                        failed_count=duplicate_count,
                        total_count=total_rows,
                        sample_failures=failed_rows,
                    )

                unique_count = stats["total_keys"]
                bloom_info = " (bloom filter enabled)" if bloom_filter is not None else ""
                return self._create_result(
                    passed=True,
                    message=f"All {unique_count:,} keys are unique across {total_rows:,} rows{bloom_info}",
                    total_count=total_rows,
                )

            except Exception as e:
                return self._create_result(
                    passed=False,
                    message=f"Error during unique key check: {str(e)}",
                    failed_count=1,
                )
