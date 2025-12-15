"""
Parquet/PyArrow Type Handler - Comprehensive handling for all Arrow data types.

This module provides utilities to safely handle all PyArrow data types when
profiling Parquet files. Some Arrow types (nested, binary, etc.) need special
handling to be used in dictionaries, value counts, or serialized to JSON.

PyArrow Data Types Reference:
https://arrow.apache.org/docs/python/api/datatypes.html

Type Categories:
- Primitive: int8-64, uint8-64, float16/32/64, bool
- Temporal: date32/64, time32/64, timestamp, duration, interval
- Binary/String: binary, string, large_binary, large_string, fixed_size_binary
- Decimal: decimal128, decimal256
- Nested: list, large_list, map, struct, union
- Special: dictionary, uuid, json, run_end_encoded
"""

import logging
from typing import Any, Dict, Optional
from decimal import Decimal as PyDecimal

import numpy as np

# Import pyarrow once at module level
try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    pa = None
    HAS_PYARROW = False

logger = logging.getLogger(__name__)

# Track which type warnings we've already logged to avoid spam
_warned_types = set()


def to_hashable(value: Any) -> Any:
    """
    Convert a value to a hashable type suitable for use as dict keys or in sets.

    Handles all PyArrow types that may appear when reading Parquet files.

    Args:
        value: Any value, potentially from a Parquet file

    Returns:
        A hashable representation of the value
    """
    if value is None:
        return None

    # Already hashable primitives
    if isinstance(value, (str, int, float, bool, type(None))):
        return value

    # Bytes -> hex string
    if isinstance(value, bytes):
        return value.hex()

    # Decimal -> float (or string for very large decimals)
    if isinstance(value, PyDecimal):
        try:
            return float(value)
        except (ValueError, OverflowError):
            return str(value)

    # numpy scalar types
    if isinstance(value, np.generic):
        return value.item()  # Convert to Python native type

    # PyArrow scalar types
    if HAS_PYARROW and hasattr(pa, 'Scalar') and isinstance(value, pa.Scalar):
        return to_hashable(value.as_py())

    # Lists, arrays, tuples -> tuple of hashable values (for small ones) or string
    if isinstance(value, (list, tuple)):
        if len(value) <= 10:
            try:
                return tuple(to_hashable(v) for v in value)
            except (TypeError, ValueError):
                return str(value)
        return f"[{len(value)} items]"

    # numpy arrays
    if isinstance(value, np.ndarray):
        if value.size <= 10:
            return tuple(to_hashable(v) for v in value.flat)
        return f"array({value.shape}, {value.dtype})"

    # Dicts/maps -> frozenset of tuples or string
    if isinstance(value, dict):
        if len(value) <= 10:
            try:
                return frozenset((to_hashable(k), to_hashable(v)) for k, v in value.items())
            except (TypeError, ValueError):
                return str(value)
        return f"{{dict with {len(value)} keys}}"

    # UUID objects
    try:
        import uuid
        if isinstance(value, uuid.UUID):
            return str(value)
    except ImportError:
        pass

    # datetime types
    try:
        import datetime
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value.isoformat()
        if isinstance(value, datetime.timedelta):
            return str(value)
    except ImportError:
        pass

    # pandas types
    try:
        import pandas as pd
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, pd.Timedelta):
            return str(value)
        if isinstance(value, pd.Interval):
            return str(value)
        if pd.isna(value):
            return None
    except ImportError:
        pass

    # Last resort: convert to string
    try:
        return str(value)
    except Exception:
        return f"<unhashable {type(value).__name__}>"


def to_serializable(value: Any) -> Any:
    """
    Convert a value to a JSON-serializable type.

    Similar to to_hashable but ensures the result can be serialized to JSON.

    Args:
        value: Any value, potentially from a Parquet file

    Returns:
        A JSON-serializable representation of the value
    """
    if value is None:
        return None

    # JSON-native types
    if isinstance(value, (str, int, float, bool)):
        return value

    # Bytes -> base64 or hex
    if isinstance(value, bytes):
        try:
            import base64
            return base64.b64encode(value).decode('ascii')
        except Exception:
            return value.hex()

    # Decimal -> float or string
    if isinstance(value, PyDecimal):
        try:
            return float(value)
        except (ValueError, OverflowError):
            return str(value)

    # numpy types
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, np.ndarray):
            return value.tolist()
    except ImportError:
        pass

    # PyArrow scalars
    try:
        import pyarrow as pa
        if hasattr(pa, 'Scalar') and isinstance(value, pa.Scalar):
            return to_serializable(value.as_py())
    except ImportError:
        pass

    # Collections
    if isinstance(value, (list, tuple)):
        return [to_serializable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): to_serializable(v) for k, v in value.items()}
    if isinstance(value, set):
        return [to_serializable(v) for v in value]

    # datetime types
    try:
        import datetime
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        if isinstance(value, datetime.time):
            return value.isoformat()
        if isinstance(value, datetime.timedelta):
            return value.total_seconds()
    except ImportError:
        pass

    # pandas types
    try:
        import pandas as pd
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if isinstance(value, pd.Timedelta):
            return value.total_seconds()
        if isinstance(value, pd.Interval):
            return {'left': to_serializable(value.left), 'right': to_serializable(value.right)}
        if pd.isna(value):
            return None
    except ImportError:
        pass

    # UUID
    try:
        import uuid
        if isinstance(value, uuid.UUID):
            return str(value)
    except ImportError:
        pass

    # Fallback to string
    return str(value)


def get_dtype_category(dtype) -> str:
    """
    Categorize a pandas/pyarrow dtype into a semantic category.

    Args:
        dtype: A pandas dtype, pyarrow type, or string type name

    Returns:
        Category string: 'numeric', 'string', 'datetime', 'boolean', 'binary',
                        'decimal', 'nested', 'categorical', 'unknown'
    """
    dtype_str = str(dtype).lower()

    # Numeric types
    if any(t in dtype_str for t in ['int', 'uint', 'float', 'double', 'half']):
        return 'numeric'

    # Decimal types
    if 'decimal' in dtype_str:
        return 'decimal'

    # Boolean
    if 'bool' in dtype_str:
        return 'boolean'

    # String types
    if any(t in dtype_str for t in ['string', 'utf8', 'object', 'str']):
        return 'string'

    # Binary types
    if 'binary' in dtype_str or 'bytes' in dtype_str:
        return 'binary'

    # Datetime/temporal types
    if any(t in dtype_str for t in ['datetime', 'timestamp', 'date', 'time', 'duration', 'interval', 'timedelta']):
        return 'datetime'

    # Nested types
    if any(t in dtype_str for t in ['list', 'struct', 'map', 'union', 'array']):
        return 'nested'

    # Categorical/dictionary
    if any(t in dtype_str for t in ['category', 'dictionary', 'dict']):
        return 'categorical'

    # Extension types
    if any(t in dtype_str for t in ['uuid', 'json']):
        return 'string'  # Treat as string for profiling

    return 'unknown'


def is_complex_type(dtype) -> bool:
    """
    Check if a dtype is a complex/nested type that needs special handling.

    Args:
        dtype: A pandas dtype, pyarrow type, or string type name

    Returns:
        True if the type requires special handling
    """
    category = get_dtype_category(dtype)
    return category in ('nested', 'binary', 'decimal')


def warn_unsupported_type(dtype, column_name: str = None):
    """
    Log a warning for unsupported types (only once per type).

    Args:
        dtype: The dtype that's not fully supported
        column_name: Optional column name for context
    """
    dtype_str = str(dtype)
    if dtype_str not in _warned_types:
        _warned_types.add(dtype_str)
        col_context = f" in column '{column_name}'" if column_name else ""
        logger.debug(f"Complex type '{dtype_str}'{col_context} - values converted to string representation")


def safe_value_counts(series, max_unique: int = 10000) -> Dict[Any, int]:
    """
    Compute value counts safely, handling all Parquet types.

    Args:
        series: A pandas Series
        max_unique: Maximum number of unique values to track

    Returns:
        Dictionary of value -> count
    """
    import pandas as pd

    result = {}
    try:
        value_freq = series.value_counts()
        for val, count in value_freq.items():
            if len(result) >= max_unique:
                break
            try:
                # Try to use value directly
                result[val] = result.get(val, 0) + count
            except TypeError:
                # Value is unhashable - convert it
                hashable_val = to_hashable(val)
                result[hashable_val] = result.get(hashable_val, 0) + count
    except Exception as e:
        logger.debug(f"Error computing value counts: {e}")

    return result
