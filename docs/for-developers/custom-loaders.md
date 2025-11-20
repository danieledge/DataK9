# Creating Custom Loaders

**Extending DataK9 with New File Formats**

DataK9 ships with loaders for CSV, Excel, JSON, Parquet, and databases. But sometimes you need to validate data in formats not yet supported - XML, fixed-width files, proprietary formats, or data from APIs.

This guide shows you how to create custom loaders so DataK9 can guard your data, no matter where it comes from.

---

## Table of Contents

1. [Quick Start: 30-Second Example](#quick-start-30-second-example)
2. [Understanding the BaseLoader](#understanding-the-baseloader)
3. [Step-by-Step Tutorial](#step-by-step-tutorial)
4. [Testing Custom Loaders](#testing-custom-loaders)
5. [Real-World Examples](#real-world-examples)
6. [Best Practices](#best-practices)
7. [Next Steps](#next-steps)

---

## Quick Start: 30-Second Example

**Want to load XML files?** Here's a minimal working loader:

```python
from typing import Iterator, Dict, Any
import pandas as pd
import xml.etree.ElementTree as ET
from validation_framework.loaders.base import DataLoader

class XMLLoader(DataLoader):
    """Load and validate XML files with DataK9"""

    def load(self) -> Iterator[pd.DataFrame]:
        """Parse XML and yield DataFrame chunks"""
        tree = ET.parse(self.file_path)
        root = tree.getroot()

        records = [self._element_to_dict(elem) for elem in root]
        yield pd.DataFrame(records)

    def get_metadata(self) -> Dict[str, Any]:
        """Return file metadata"""
        tree = ET.parse(self.file_path)
        root = tree.getroot()

        return {
            "row_count": len(root),
            "file_format": "xml"
        }

    def _element_to_dict(self, elem) -> dict:
        """Convert XML element to dictionary"""
        return {child.tag: child.text for child in elem}
```

**Register it:**
```python
# In validation_framework/loaders/factory.py
from validation_framework.loaders.xml_loader import XMLLoader

class LoaderFactory:
    _loaders = {
        "csv": CSVLoader,
        "excel": ExcelLoader,
        "json": JSONLoader,
        "parquet": ParquetLoader,
        "xml": XMLLoader,  # ← Add your loader
    }

    extension_map = {
        ".csv": "csv",
        ".xlsx": "excel",
        ".json": "json",
        ".parquet": "parquet",
        ".xml": "xml",  # ← Add file extension
    }
```

**Use it:**
```yaml
files:
  - name: "customer_data"
    path: "data/customers.xml"
    format: "xml"

    validations:
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]
```

That's it! DataK9 can now guard XML data files.

---

## Understanding the BaseLoader

All loaders inherit from `DataLoader` base class. Understanding this contract is essential.

### The Base Class

```python
from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any
import pandas as pd

class DataLoader(ABC):
    """
    Base class for all data loaders in DataK9.

    Loaders convert various file formats into pandas DataFrames
    that DataK9 can validate.
    """

    def __init__(self, file_path: str, chunk_size: int = 1000, **kwargs):
        """
        Initialize loader.

        Args:
            file_path: Path to file to load
            chunk_size: Number of rows per chunk (for memory efficiency)
            **kwargs: Format-specific parameters
        """
        self.file_path = file_path
        self.chunk_size = chunk_size

    @abstractmethod
    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load file and yield DataFrame chunks.

        MUST implement chunked loading for memory efficiency.
        Even small files should be yielded as chunks.

        Returns:
            Iterator yielding DataFrame chunks
        """
        pass

    @abstractmethod
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get file metadata without loading entire file.

        Should return:
            - row_count: Total number of records
            - columns: List of column names
            - column_count: Number of columns
            - file_format: Format identifier (csv, json, etc.)

        Returns:
            Dictionary with file metadata
        """
        pass
```

### What Your Loader Must Do

1. **Implement `load()` method**
   - Return an Iterator that yields pandas DataFrames
   - Process data in chunks (respect `self.chunk_size`)
   - Handle large files without loading everything into memory
   - Yield at least one DataFrame, even for empty files

2. **Implement `get_metadata()` method**
   - Return file information WITHOUT loading all data
   - Should be fast (scan file structure, not content)
   - Return `None` for counts if they're expensive to compute

3. **Accept `**kwargs` in constructor**
   - Allow format-specific configuration
   - Pass through to parent class
   - Store as instance variables for use in `load()`

### Available Attributes

When implementing your loader, you have access to:

```python
self.file_path      # Path to file being loaded
self.chunk_size     # Number of rows per chunk (default: 50,000)
# Plus any custom parameters you accept in __init__
```

---

## Step-by-Step Tutorial

Let's build a robust **Fixed-Width File Loader** that handles legacy mainframe data formats.

### Example File Format

```
NAME          AGE SALARY    DEPARTMENT
John Smith    032 075000.00 Engineering
Jane Doe      028 082500.00 Marketing
Bob Johnson   045 095000.00 Finance
```

Fixed positions: Name (0-13), Age (14-16), Salary (17-26), Department (27-38)

### Step 1: Create the Loader File

Create `validation_framework/loaders/fixed_width_loader.py`:

```python
"""
Fixed-width file loader for DataK9.

Handles legacy mainframe data with fixed column positions.
Supports chunked loading for memory efficiency.

File: validation_framework/loaders/fixed_width_loader.py
Author: Daniel Edge
"""
from typing import Iterator, Dict, Any, List, Tuple
import pandas as pd
from validation_framework.loaders.base import DataLoader
import logging

logger = logging.getLogger(__name__)


class FixedWidthLoader(DataLoader):
    """
    Loader for fixed-width text files (mainframe format).

    DataK9 uses this loader to parse files with fixed column positions.
    Commonly used for legacy system exports and government data files.

    Configuration:
        column_specs (List[Tuple]): List of (name, start, end) tuples
        has_header (bool): Whether file has a header row to skip
        encoding (str): File encoding (default: 'utf-8')

    Example YAML:
        - name: "mainframe_data"
          path: "data/employee.txt"
          format: "fixed_width"
          column_specs:
            - ["name", 0, 14]
            - ["age", 14, 17]
            - ["salary", 17, 27]
            - ["department", 27, 38]
          has_header: true
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        column_specs: List[List] = None,
        has_header: bool = False,
        encoding: str = 'utf-8',
        **kwargs
    ):
        """
        Initialize FixedWidthLoader.

        Args:
            file_path: Path to fixed-width file
            chunk_size: Number of rows per chunk (default: 50,000)
            column_specs: List of [name, start, end] for each column
            has_header: Whether to skip first line
            encoding: File encoding
        """
        super().__init__(file_path, chunk_size, **kwargs)

        # Validate column specs
        if not column_specs:
            raise ValueError("column_specs required for fixed-width files")

        # Convert to list of tuples: (name, start, end)
        self.column_specs = [
            (spec[0], int(spec[1]), int(spec[2]))
            for spec in column_specs
        ]

        self.has_header = has_header
        self.encoding = encoding

        logger.info(f"Initialized FixedWidthLoader: {file_path}")
        logger.debug(f"Column specs: {self.column_specs}")

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Load fixed-width file and yield DataFrame chunks.

        DataK9 calls this method to get data for validation.
        Processes file in chunks for memory efficiency.

        Yields:
            DataFrame chunks with parsed data
        """
        try:
            logger.info(f"🐕 DataK9 loading fixed-width file: {self.file_path}")

            records = []
            line_number = 0

            with open(self.file_path, 'r', encoding=self.encoding) as f:
                for line in f:
                    line_number += 1

                    # Skip header line
                    if self.has_header and line_number == 1:
                        continue

                    # Skip empty lines
                    if not line.strip():
                        continue

                    # Parse line using column specs
                    record = {}
                    for col_name, start, end in self.column_specs:
                        # Extract value and strip whitespace
                        value = line[start:end].strip()

                        # Convert to appropriate type
                        record[col_name] = self._convert_value(value)

                    records.append(record)

                    # Yield chunk when chunk_size reached
                    if len(records) >= self.chunk_size:
                        df = pd.DataFrame(records)
                        logger.debug(f"Yielding chunk: {len(df)} rows")
                        yield df
                        records = []

            # Yield remaining records
            if records:
                df = pd.DataFrame(records)
                logger.debug(f"Yielding final chunk: {len(df)} rows")
                yield df

            logger.info(f"✅ Successfully loaded {line_number} lines")

        except FileNotFoundError:
            logger.error(f"File not found: {self.file_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading fixed-width file: {str(e)}")
            raise

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get file metadata.

        DataK9 uses this for reporting and validation context.

        Returns:
            Dictionary with file metadata
        """
        try:
            # Count total lines (fast operation)
            with open(self.file_path, 'r', encoding=self.encoding) as f:
                total_lines = sum(1 for _ in f)

            # Subtract header if present
            row_count = total_lines - 1 if self.has_header else total_lines

            # Extract column names from specs
            columns = [spec[0] for spec in self.column_specs]

            return {
                "row_count": row_count,
                "columns": columns,
                "column_count": len(columns),
                "file_format": "fixed_width",
                "encoding": self.encoding,
            }

        except Exception as e:
            logger.warning(f"Could not get metadata: {str(e)}")
            return {
                "row_count": None,
                "columns": [spec[0] for spec in self.column_specs],
                "column_count": len(self.column_specs),
                "file_format": "fixed_width",
            }

    def _convert_value(self, value: str) -> Any:
        """
        Convert string value to appropriate Python type.

        Tries int → float → keeps as string.

        Args:
            value: String value from file

        Returns:
            Converted value (int, float, or str)
        """
        if not value:
            return None

        # Try integer
        try:
            return int(value)
        except ValueError:
            pass

        # Try float
        try:
            return float(value)
        except ValueError:
            pass

        # Keep as string
        return value
```

### Step 2: Register the Loader

Add to `validation_framework/loaders/factory.py`:

```python
from validation_framework.loaders.fixed_width_loader import FixedWidthLoader

class LoaderFactory:
    """Factory for creating data loaders"""

    _loaders: Dict[str, Type[DataLoader]] = {
        "csv": CSVLoader,
        "excel": ExcelLoader,
        "json": JSONLoader,
        "parquet": ParquetLoader,
        "database": DatabaseLoader,
        "fixed_width": FixedWidthLoader,  # ← Add your loader
    }

    extension_map = {
        ".csv": "csv",
        ".xlsx": "excel",
        ".xls": "excel",
        ".json": "json",
        ".jsonl": "json",
        ".ndjson": "json",
        ".parquet": "parquet",
        ".txt": "fixed_width",  # ← Add extension mapping
        ".dat": "fixed_width",  # Multiple extensions OK
    }
```

### Step 3: Use in Configuration

Create a validation config using your new loader:

```yaml
validation_job:
  name: "Legacy Employee Data Validation"
  version: "1.0"
  description: "Validate mainframe employee export"

settings:
  chunk_size: 50000
  max_sample_failures: 100

files:
  - name: "employee_data"
    path: "data/employees.txt"
    format: "fixed_width"

    # Fixed-width specific configuration
    column_specs:
      - ["name", 0, 14]
      - ["age", 14, 17]
      - ["salary", 17, 27]
      - ["department", 27, 38]
    has_header: true
    encoding: "utf-8"

    validations:
      # All fields must be present
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["name", "age", "salary", "department"]

      # Age must be reasonable
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "age"
          min_value: 18
          max_value: 75

      # Salary must be positive
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "salary"
          min_value: 0

      # Department must be valid
      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "department"
          allowed_values:
            - "Engineering"
            - "Marketing"
            - "Finance"
            - "HR"
            - "Operations"

output:
  html_report: "employee_validation.html"
  json_summary: "employee_validation.json"
  fail_on_error: true
```

### Step 4: Test Your Loader

Create test file and run validation:

```bash
# Create test data file
cat > employees.txt << 'EOF'
NAME          AGE SALARY    DEPARTMENT
John Smith    032 075000.00 Engineering
Jane Doe      028 082500.00 Marketing
Bob Johnson   045 095000.00 Finance
Alice Brown   029 078000.00 HR
EOF

# Run DataK9 validation
python3 -m validation_framework.cli validate employee_config.yaml

# Output:
# 🐕 DataK9 Validation Started
# ✅ Loaded: employee_data (4 rows, 4 columns)
# ✅ All validations passed
# 📊 Report: employee_validation.html
```

**Success!** DataK9 can now validate fixed-width files.

---

## Testing Custom Loaders

Testing is critical for robust loaders. Here's a comprehensive test suite.

### Test Template

Create `tests/test_fixed_width_loader.py`:

```python
"""
Tests for FixedWidthLoader.

Tests chunked loading, metadata extraction, and error handling.

Author: Daniel Edge
"""
import pytest
import pandas as pd
from pathlib import Path
from validation_framework.loaders.fixed_width_loader import FixedWidthLoader


@pytest.fixture
def sample_file(tmp_path):
    """Create sample fixed-width file for testing."""
    file_path = tmp_path / "test.txt"

    content = """NAME          AGE SALARY    DEPARTMENT
John Smith    032 075000.00 Engineering
Jane Doe      028 082500.00 Marketing
Bob Johnson   045 095000.00 Finance
Alice Brown   029 078000.00 HR
"""

    file_path.write_text(content)
    return file_path


@pytest.fixture
def column_specs():
    """Column specifications for test file."""
    return [
        ["name", 0, 14],
        ["age", 14, 17],
        ["salary", 17, 27],
        ["department", 27, 38]
    ]


class TestFixedWidthLoader:
    """Tests for FixedWidthLoader"""

    def test_loader_initialization(self, sample_file, column_specs):
        """Test loader initializes correctly."""
        loader = FixedWidthLoader(
            file_path=str(sample_file),
            column_specs=column_specs,
            has_header=True
        )

        assert loader.file_path == str(sample_file)
        assert loader.chunk_size == 50000
        assert len(loader.column_specs) == 4
        assert loader.has_header is True

    def test_loader_requires_column_specs(self, sample_file):
        """Test loader fails without column specs."""
        with pytest.raises(ValueError, match="column_specs required"):
            FixedWidthLoader(
                file_path=str(sample_file),
                column_specs=None
            )

    def test_load_returns_dataframes(self, sample_file, column_specs):
        """Test load() yields DataFrames."""
        loader = FixedWidthLoader(
            file_path=str(sample_file),
            column_specs=column_specs,
            has_header=True
        )

        chunks = list(loader.load())

        assert len(chunks) >= 1
        assert all(isinstance(chunk, pd.DataFrame) for chunk in chunks)

    def test_load_parses_data_correctly(self, sample_file, column_specs):
        """Test data is parsed correctly."""
        loader = FixedWidthLoader(
            file_path=str(sample_file),
            column_specs=column_specs,
            has_header=True
        )

        chunks = list(loader.load())
        df = pd.concat(chunks, ignore_index=True)

        # Should have 4 data rows (header skipped)
        assert len(df) == 4

        # Check first row
        assert df.loc[0, 'name'] == "John Smith"
        assert df.loc[0, 'age'] == 32
        assert df.loc[0, 'salary'] == 75000.00
        assert df.loc[0, 'department'] == "Engineering"

    def test_load_handles_chunking(self, tmp_path, column_specs):
        """Test chunked loading with small chunk size."""
        # Create file with many rows
        file_path = tmp_path / "large.txt"

        lines = ["NAME          AGE SALARY    DEPARTMENT\n"]
        for i in range(100):
            lines.append(f"Person {i:<6} {25+i:03d} {50000+i*1000:09.2f} Engineering\n")

        file_path.write_text(''.join(lines))

        # Load with small chunks
        loader = FixedWidthLoader(
            file_path=str(file_path),
            column_specs=column_specs,
            has_header=True,
            chunk_size=25
        )

        chunks = list(loader.load())

        # Should yield multiple chunks
        assert len(chunks) == 4  # 100 rows / 25 = 4 chunks

        # Total rows should be 100
        total_rows = sum(len(chunk) for chunk in chunks)
        assert total_rows == 100

    def test_get_metadata(self, sample_file, column_specs):
        """Test metadata extraction."""
        loader = FixedWidthLoader(
            file_path=str(sample_file),
            column_specs=column_specs,
            has_header=True
        )

        metadata = loader.get_metadata()

        assert metadata['row_count'] == 4  # 5 lines - 1 header
        assert metadata['columns'] == ['name', 'age', 'salary', 'department']
        assert metadata['column_count'] == 4
        assert metadata['file_format'] == 'fixed_width'

    def test_type_conversion(self, tmp_path, column_specs):
        """Test automatic type conversion."""
        file_path = tmp_path / "types.txt"

        content = """NAME          AGE SALARY    DEPARTMENT
John Smith    032 075000.00 Engineering
Jane Doe      abc invalid   Marketing
"""

        file_path.write_text(content)

        loader = FixedWidthLoader(
            file_path=str(file_path),
            column_specs=column_specs,
            has_header=True
        )

        chunks = list(loader.load())
        df = pd.concat(chunks, ignore_index=True)

        # First row: age should be int, salary should be float
        assert isinstance(df.loc[0, 'age'], int)
        assert isinstance(df.loc[0, 'salary'], float)

        # Second row: invalid values should be strings
        assert isinstance(df.loc[1, 'age'], str)
        assert isinstance(df.loc[1, 'salary'], str)

    def test_empty_file(self, tmp_path, column_specs):
        """Test handling of empty file."""
        file_path = tmp_path / "empty.txt"
        file_path.write_text("")

        loader = FixedWidthLoader(
            file_path=str(file_path),
            column_specs=column_specs
        )

        chunks = list(loader.load())

        # Should yield empty list or single empty DataFrame
        assert len(chunks) == 0 or (len(chunks) == 1 and len(chunks[0]) == 0)

    def test_file_not_found(self, column_specs):
        """Test error when file doesn't exist."""
        loader = FixedWidthLoader(
            file_path="/nonexistent/file.txt",
            column_specs=column_specs
        )

        with pytest.raises(FileNotFoundError):
            list(loader.load())
```

### Run Tests

```bash
# Run all loader tests
pytest tests/test_fixed_width_loader.py -v

# Run with coverage
pytest tests/test_fixed_width_loader.py --cov=validation_framework.loaders

# Output:
# tests/test_fixed_width_loader.py::TestFixedWidthLoader::test_loader_initialization PASSED
# tests/test_fixed_width_loader.py::TestFixedWidthLoader::test_load_returns_dataframes PASSED
# tests/test_fixed_width_loader.py::TestFixedWidthLoader::test_load_parses_data_correctly PASSED
# ...
# ✅ All tests passed
```

---

## Real-World Examples

### Example 1: API Loader

Load data directly from REST APIs:

```python
"""
API data loader for DataK9.

Fetches data from REST endpoints and validates responses.

File: validation_framework/loaders/api_loader.py
Author: Daniel Edge
"""
from typing import Iterator, Dict, Any
import pandas as pd
import requests
from validation_framework.loaders.base import DataLoader
import logging

logger = logging.getLogger(__name__)


class APILoader(DataLoader):
    """
    Load data from REST API endpoints.

    DataK9 uses this to validate data returned from APIs
    before processing or storing.

    Configuration:
        url (str): API endpoint URL
        method (str): HTTP method (GET, POST)
        headers (dict): HTTP headers
        params (dict): Query parameters
        json_path (str): JSON path to data array (e.g., "data.records")
        auth_token (str): Bearer token for authentication

    Example YAML:
        - name: "customer_api"
          format: "api"
          url: "https://api.example.com/customers"
          method: "GET"
          headers:
            Accept: "application/json"
          auth_token: "${API_TOKEN}"
          json_path: "data.customers"
    """

    def __init__(
        self,
        file_path: str,  # Used as 'url' for APIs
        chunk_size: int = 50000,
        url: str = None,
        method: str = "GET",
        headers: Dict[str, str] = None,
        params: Dict[str, Any] = None,
        json_path: str = None,
        auth_token: str = None,
        **kwargs
    ):
        """Initialize APILoader."""
        super().__init__(file_path, chunk_size, **kwargs)

        self.url = url or file_path
        self.method = method.upper()
        self.headers = headers or {}
        self.params = params or {}
        self.json_path = json_path

        # Add authentication if provided
        if auth_token:
            self.headers['Authorization'] = f'Bearer {auth_token}'

        logger.info(f"Initialized APILoader: {self.url}")

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Fetch data from API and yield as DataFrame.

        Yields:
            DataFrame with API response data
        """
        try:
            logger.info(f"🐕 DataK9 fetching data from: {self.url}")

            # Make API request
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                params=self.params,
                timeout=30
            )

            # Check response status
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Extract data array using JSON path
            if self.json_path:
                for key in self.json_path.split('.'):
                    data = data.get(key, [])

            # Convert to DataFrame
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unexpected data type: {type(data)}")

            # Yield in chunks if large
            if len(df) <= self.chunk_size:
                yield df
            else:
                for start in range(0, len(df), self.chunk_size):
                    chunk = df.iloc[start:start + self.chunk_size]
                    yield chunk

            logger.info(f"✅ Fetched {len(df)} records from API")

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading API data: {str(e)}")
            raise

    def get_metadata(self) -> Dict[str, Any]:
        """Get API metadata."""
        return {
            "row_count": None,  # Unknown until fetched
            "columns": [],
            "column_count": 0,
            "file_format": "api",
            "url": self.url,
            "method": self.method,
        }
```

**Usage:**

```yaml
files:
  - name: "customer_api"
    format: "api"
    url: "https://api.example.com/v1/customers"
    method: "GET"
    headers:
      Accept: "application/json"
      X-API-Key: "${API_KEY}"
    json_path: "data.customers"

    validations:
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email", "status"]
```

### Example 2: Database Query Loader

Load data from database queries (alternative to built-in DatabaseLoader):

```python
"""
Custom database loader with advanced features.

Extends DataK9's database capabilities with query pagination,
connection pooling, and streaming results.

File: validation_framework/loaders/advanced_db_loader.py
Author: Daniel Edge
"""
from typing import Iterator, Dict, Any
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.pool import QueuePool
from validation_framework.loaders.base import DataLoader
import logging

logger = logging.getLogger(__name__)


class AdvancedDatabaseLoader(DataLoader):
    """
    Advanced database loader with streaming and pagination.

    Features:
    - Connection pooling for performance
    - Server-side cursors for large result sets
    - Query pagination with OFFSET/LIMIT
    - Multiple database support (PostgreSQL, MySQL, Oracle, SQL Server)

    Configuration:
        connection_string (str): SQLAlchemy connection string
        query (str): SQL query to execute
        use_pagination (bool): Use OFFSET/LIMIT for chunking
        pool_size (int): Connection pool size

    Example YAML:
        - name: "customer_data"
          format: "advanced_db"
          connection_string: "postgresql://user:pass@localhost/db"
          query: "SELECT * FROM customers WHERE active = true"
          use_pagination: true
          chunk_size: 10000
    """

    def __init__(
        self,
        file_path: str,  # Not used for DB, but required by base class
        chunk_size: int = 50000,
        connection_string: str = None,
        query: str = None,
        use_pagination: bool = False,
        pool_size: int = 5,
        **kwargs
    ):
        """Initialize AdvancedDatabaseLoader."""
        super().__init__(file_path, chunk_size, **kwargs)

        if not connection_string:
            raise ValueError("connection_string required")
        if not query:
            raise ValueError("query required")

        self.connection_string = connection_string
        self.query = query
        self.use_pagination = use_pagination

        # Create engine with connection pooling
        self.engine = sa.create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=10
        )

        logger.info(f"Initialized AdvancedDatabaseLoader")
        logger.debug(f"Query: {query[:100]}...")

    def load(self) -> Iterator[pd.DataFrame]:
        """
        Execute query and yield results in chunks.

        Uses server-side cursors for memory efficiency.

        Yields:
            DataFrame chunks with query results
        """
        try:
            logger.info("🐕 DataK9 executing database query")

            if self.use_pagination:
                # Use OFFSET/LIMIT pagination
                offset = 0
                while True:
                    paginated_query = f"""
                        {self.query}
                        OFFSET {offset} ROWS
                        FETCH NEXT {self.chunk_size} ROWS ONLY
                    """

                    df = pd.read_sql(paginated_query, self.engine)

                    if len(df) == 0:
                        break

                    yield df
                    offset += self.chunk_size

                    if len(df) < self.chunk_size:
                        break  # Last chunk
            else:
                # Use pandas chunking
                for chunk in pd.read_sql(
                    self.query,
                    self.engine,
                    chunksize=self.chunk_size
                ):
                    yield chunk

            logger.info("✅ Database query completed")

        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            raise
        finally:
            self.engine.dispose()

    def get_metadata(self) -> Dict[str, Any]:
        """Get query metadata."""
        try:
            # Execute count query
            count_query = f"SELECT COUNT(*) as count FROM ({self.query}) AS subquery"
            result = pd.read_sql(count_query, self.engine)
            row_count = int(result['count'].iloc[0])

            # Get columns from first row
            sample_df = pd.read_sql(f"{self.query} LIMIT 1", self.engine)
            columns = list(sample_df.columns)

            return {
                "row_count": row_count,
                "columns": columns,
                "column_count": len(columns),
                "file_format": "database",
            }
        except Exception as e:
            logger.warning(f"Could not get database metadata: {str(e)}")
            return {
                "row_count": None,
                "columns": [],
                "column_count": 0,
                "file_format": "database",
            }
```

### Example 3: S3/Cloud Storage Loader

Load data directly from cloud storage:

```python
"""
AWS S3 loader for DataK9.

Loads files directly from S3 buckets for validation.
Supports CSV, JSON, Parquet in S3.

File: validation_framework/loaders/s3_loader.py
Author: Daniel Edge
"""
from typing import Iterator, Dict, Any
import pandas as pd
import boto3
from io import BytesIO
from validation_framework.loaders.base import DataLoader
from validation_framework.loaders.csv_loader import CSVLoader
from validation_framework.loaders.json_loader import JSONLoader
from validation_framework.loaders.parquet_loader import ParquetLoader
import logging

logger = logging.getLogger(__name__)


class S3Loader(DataLoader):
    """
    Load files from AWS S3 for DataK9 validation.

    Automatically detects file format and uses appropriate loader.

    Configuration:
        bucket (str): S3 bucket name
        key (str): S3 object key (file path)
        file_format (str): File format (csv, json, parquet)
        aws_access_key_id (str): AWS access key
        aws_secret_access_key (str): AWS secret key
        region_name (str): AWS region

    Example YAML:
        - name: "s3_data"
          format: "s3"
          bucket: "my-data-bucket"
          key: "raw/customers/2024-01-01.csv"
          file_format: "csv"
          region_name: "us-east-1"
    """

    def __init__(
        self,
        file_path: str,
        chunk_size: int = 50000,
        bucket: str = None,
        key: str = None,
        file_format: str = "csv",
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = "us-east-1",
        **kwargs
    ):
        """Initialize S3Loader."""
        super().__init__(file_path, chunk_size, **kwargs)

        self.bucket = bucket
        self.key = key or file_path
        self.file_format = file_format

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Download file to memory
        logger.info(f"Downloading s3://{bucket}/{self.key}")
        response = self.s3_client.get_object(Bucket=bucket, Key=self.key)
        file_content = response['Body'].read()

        # Create in-memory file
        self.buffer = BytesIO(file_content)

        # Create appropriate loader based on format
        if file_format == 'csv':
            self.loader = CSVLoader(file_path=self.buffer, chunk_size=chunk_size, **kwargs)
        elif file_format == 'json':
            self.loader = JSONLoader(file_path=self.buffer, chunk_size=chunk_size, **kwargs)
        elif file_format == 'parquet':
            self.loader = ParquetLoader(file_path=self.buffer, chunk_size=chunk_size, **kwargs)
        else:
            raise ValueError(f"Unsupported format: {file_format}")

        logger.info(f"✅ Downloaded {len(file_content)} bytes from S3")

    def load(self) -> Iterator[pd.DataFrame]:
        """Load S3 file via appropriate loader."""
        logger.info(f"🐕 DataK9 loading S3 file: s3://{self.bucket}/{self.key}")
        return self.loader.load()

    def get_metadata(self) -> Dict[str, Any]:
        """Get S3 file metadata."""
        metadata = self.loader.get_metadata()
        metadata['file_format'] = f's3_{self.file_format}'
        metadata['s3_bucket'] = self.bucket
        metadata['s3_key'] = self.key
        return metadata
```

---

## Best Practices

### 1. **Always Implement Chunked Loading**

Even for small files, yield data in chunks:

```python
def load(self) -> Iterator[pd.DataFrame]:
    """Load file in chunks"""
    # ❌ BAD: Load entire file
    df = pd.read_csv(self.file_path)
    yield df

    # ✅ GOOD: Use chunked reading
    for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size):
        yield chunk
```

**Why?** DataK9 may process 200GB+ files. Memory-efficient loading is critical.

### 2. **Validate Parameters in __init__**

Fail fast with clear error messages:

```python
def __init__(self, file_path: str, column_specs: List = None, **kwargs):
    super().__init__(file_path, **kwargs)

    # ✅ Validate required parameters
    if not column_specs:
        raise ValueError("column_specs required for fixed-width files")

    # ✅ Validate parameter types
    if not isinstance(column_specs, list):
        raise TypeError("column_specs must be a list")

    self.column_specs = column_specs
```

### 3. **Implement Robust Error Handling**

Handle file I/O errors gracefully:

```python
def load(self) -> Iterator[pd.DataFrame]:
    try:
        with open(self.file_path, 'r') as f:
            # ... processing logic
            yield df

    except FileNotFoundError:
        logger.error(f"File not found: {self.file_path}")
        raise

    except UnicodeDecodeError as e:
        logger.error(f"Encoding error: {str(e)}")
        logger.info("Try specifying encoding parameter")
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
```

### 4. **Make get_metadata() Fast**

Don't load entire file to get metadata:

```python
def get_metadata(self) -> Dict[str, Any]:
    # ❌ BAD: Load all data
    all_data = list(self.load())
    df = pd.concat(all_data)
    return {"row_count": len(df)}

    # ✅ GOOD: Quick file scan
    with open(self.file_path, 'r') as f:
        row_count = sum(1 for line in f)

    return {
        "row_count": row_count,
        "file_format": "custom"
    }
```

### 5. **Use Logging Liberally**

Help users debug issues:

```python
import logging

logger = logging.getLogger(__name__)

def load(self) -> Iterator[pd.DataFrame]:
    logger.info(f"🐕 DataK9 loading file: {self.file_path}")
    logger.debug(f"Chunk size: {self.chunk_size}")

    # ... processing

    logger.debug(f"Yielding chunk: {len(chunk)} rows")
    yield chunk

    logger.info(f"✅ Successfully loaded {total_rows} rows")
```

### 6. **Accept Format-Specific Parameters**

Allow configuration via YAML:

```python
def __init__(
    self,
    file_path: str,
    chunk_size: int = 50000,
    # Format-specific parameters
    delimiter: str = ',',
    encoding: str = 'utf-8',
    skip_rows: int = 0,
    **kwargs  # ← Always accept **kwargs
):
    super().__init__(file_path, chunk_size, **kwargs)
    self.delimiter = delimiter
    self.encoding = encoding
    self.skip_rows = skip_rows
```

Users can then configure in YAML:

```yaml
files:
  - name: "custom_data"
    path: "data/file.txt"
    format: "custom"
    delimiter: "|"
    encoding: "latin-1"
    skip_rows: 3
```

### 7. **Return Consistent Column Names**

Normalize column names for predictability:

```python
def load(self) -> Iterator[pd.DataFrame]:
    for chunk in self._read_chunks():
        # Normalize column names
        chunk.columns = [
            col.strip().lower().replace(' ', '_')
            for col in chunk.columns
        ]

        yield chunk
```

**Why?** Makes validation rules more reliable across different data sources.

### 8. **Handle Empty Files Gracefully**

Don't crash on empty files:

```python
def load(self) -> Iterator[pd.DataFrame]:
    records = []

    with open(self.file_path, 'r') as f:
        for line in f:
            # ... parse line
            records.append(record)

    # ✅ Handle empty case
    if records:
        yield pd.DataFrame(records)
    else:
        # Yield empty DataFrame with correct columns
        yield pd.DataFrame(columns=self._get_column_names())
```

### 9. **Test with Real-World Data**

Test with actual files, not just synthetic data:

```python
def test_with_production_sample():
    """Test with real production file sample"""
    # Use anonymized production data sample
    loader = MyLoader("tests/fixtures/prod_sample.dat")

    chunks = list(loader.load())
    df = pd.concat(chunks)

    # Verify structure matches production
    assert set(df.columns) == set(EXPECTED_COLUMNS)
    assert len(df) > 0
```

### 10. **Document Configuration in Docstring**

Help users understand parameters:

```python
class MyLoader(DataLoader):
    """
    Custom loader for proprietary format.

    Configuration:
        param1 (str): Description of param1
        param2 (int): Description of param2

    Example YAML:
        - name: "data"
          format: "custom"
          path: "file.dat"
          param1: "value1"
          param2: 100

    Example Python:
        loader = MyLoader(
            file_path="data.dat",
            param1="value1",
            param2=100
        )
    """
```

---

## Next Steps

Now that you understand custom loaders, explore:

1. **[Custom Validations](custom-validations.md)** - Create domain-specific validation rules
2. **[Custom Reporters](custom-reporters.md)** - Generate custom report formats
3. **[API Reference](api-reference.md)** - Complete API documentation
4. **[Testing Guide](testing-guide.md)** - Comprehensive testing strategies

---

## Questions?

**Need help?** Check these resources:

- **Built-in Loaders**: `validation_framework/loaders/` for examples
- **Base Class**: `validation_framework/loaders/base.py` for full interface
- **Factory**: `validation_framework/loaders/factory.py` for registration
- **Tests**: `tests/test_loaders.py` for test examples

**Found a bug?** Report it on [GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)

**Want to contribute?** See **[Contributing Guide](contributing.md)**

---

**🐕 Teach DataK9 to guard any data format - your K9 companion adapts to your needs**
