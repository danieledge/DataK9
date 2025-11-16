"""
JSON serialization utilities for profiler results.

Handles numpy types and other non-standard JSON types to ensure
profile results can be properly serialized to JSON.
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, date
from typing import Any


class NumpyJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles numpy types and other non-standard types.

    Converts:
    - numpy int types → Python int
    - numpy float types → Python float
    - numpy bool → Python bool
    - numpy arrays → Python lists
    - pandas Timestamp → ISO format string
    - datetime/date → ISO format string
    - NaN/inf → null
    """

    def default(self, obj: Any) -> Any:
        """
        Convert non-serializable types to JSON-compatible types.

        Args:
            obj: Object to serialize

        Returns:
            JSON-serializable representation
        """
        # Handle numpy integer types
        if isinstance(obj, (np.integer, np.int8, np.int16, np.int32, np.int64)):
            return int(obj)

        # Handle numpy float types
        if isinstance(obj, (np.floating, np.float16, np.float32, np.float64)):
            # Convert NaN and inf to None
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)

        # Handle numpy bool
        if isinstance(obj, np.bool_):
            return bool(obj)

        # Handle numpy arrays
        if isinstance(obj, np.ndarray):
            return obj.tolist()

        # Handle pandas Timestamp
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()

        # Handle datetime/date
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()

        # Handle sets
        if isinstance(obj, set):
            return list(obj)

        # Let the base class handle everything else
        return super().default(obj)


def convert_to_json_serializable(obj: Any) -> Any:
    """
    Recursively convert object to JSON-serializable types.

    Handles nested structures like dicts and lists.

    Args:
        obj: Object to convert

    Returns:
        JSON-serializable version of object
    """
    # Handle None
    if obj is None:
        return None

    # Handle numpy integer types
    if isinstance(obj, (np.integer, np.int8, np.int16, np.int32, np.int64)):
        return int(obj)

    # Handle numpy float types
    if isinstance(obj, (np.floating, np.float16, np.float32, np.float64)):
        # Convert NaN and inf to None
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)

    # Handle numpy bool
    if isinstance(obj, np.bool_):
        return bool(obj)

    # Handle numpy arrays
    if isinstance(obj, np.ndarray):
        return [convert_to_json_serializable(item) for item in obj.tolist()]

    # Handle pandas Timestamp
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    # Handle datetime/date
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    # Handle dictionaries recursively
    if isinstance(obj, dict):
        return {
            key: convert_to_json_serializable(value)
            for key, value in obj.items()
        }

    # Handle lists/tuples recursively
    if isinstance(obj, (list, tuple)):
        return [convert_to_json_serializable(item) for item in obj]

    # Handle sets
    if isinstance(obj, set):
        return [convert_to_json_serializable(item) for item in obj]

    # Return as-is for standard types (str, int, float, bool)
    return obj


def safe_json_dumps(obj: Any, **kwargs) -> str:
    """
    Safely serialize object to JSON string using custom encoder.

    Args:
        obj: Object to serialize
        **kwargs: Additional arguments to pass to json.dumps

    Returns:
        JSON string
    """
    # Set default kwargs
    if 'cls' not in kwargs:
        kwargs['cls'] = NumpyJSONEncoder
    if 'indent' not in kwargs:
        kwargs['indent'] = 2

    return json.dumps(obj, **kwargs)


def safe_json_dump(obj: Any, fp, **kwargs) -> None:
    """
    Safely serialize object to JSON file using custom encoder.

    Args:
        obj: Object to serialize
        fp: File pointer to write to
        **kwargs: Additional arguments to pass to json.dump
    """
    # Set default kwargs
    if 'cls' not in kwargs:
        kwargs['cls'] = NumpyJSONEncoder
    if 'indent' not in kwargs:
        kwargs['indent'] = 2

    json.dump(obj, fp, **kwargs)
