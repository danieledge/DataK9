# Async Validators

Async validators enable non-blocking validation execution with concurrent processing, significantly improving throughput for I/O-bound operations and multi-file validation jobs.

## Overview

The async validator system provides:

- **Concurrent Execution**: Run multiple independent validations simultaneously
- **Non-Blocking I/O**: Execute database lookups and API calls without blocking
- **Backwards Compatibility**: Existing sync validators work seamlessly via adapter
- **Dependency Management**: Define validation dependencies with automatic resolution
- **Resource Control**: Limit concurrency to prevent resource exhaustion
- **Error Isolation**: One validation failure doesn't stop others

## Architecture

### Core Components

1. **AsyncValidationRule**: Base class for async validators
2. **SyncValidatorAdapter**: Wraps sync validators to work with async infrastructure
3. **AsyncValidationOrchestrator**: Manages concurrent execution with dependencies
4. **AsyncValidationEngine**: Integrates async validators into validation pipeline

## Using Async Validators

### Built-in Async Validators

#### AsyncSchemaMatchCheck

Validates schema asynchronously without blocking.

```yaml
validations:
  - type: "AsyncSchemaMatchCheck"
    severity: "ERROR"
    params:
      expected_schema:
        customer_id: "integer"
        email: "string"
        created_date: "date"
      strict: false
      check_order: false
```

#### AsyncLookupCheck

Performs reference data lookups with async database support.

**In-Memory Lookup:**
```yaml
validations:
  - type: "AsyncLookupCheck"
    severity: "ERROR"
    params:
      field: "country_code"
      reference_values: ["US", "UK", "CA", "AU"]
      check_type: "allow"
      description: "Valid country codes"
```

**Database Lookup (Async):**
```yaml
validations:
  - type: "AsyncLookupCheck"
    severity: "ERROR"
    params:
      field: "customer_id"
      reference_table: "customers"
      reference_column: "id"
      connection_string: "postgresql://user:pass@localhost/db"
      description: "Customer must exist"
```

#### AsyncRegexCheck

Validates fields against regex patterns with concurrent chunk processing.

```yaml
validations:
  - type: "AsyncRegexCheck"
    severity: "ERROR"
    params:
      field: "email"
      pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
      description: "Valid email format"
      should_match: true
```

### Creating Custom Async Validators

#### Basic Async Validator

```python
from validation_framework.validations.async_base import AsyncDataValidationRule
from validation_framework.core.results import ValidationResult, Severity
from typing import AsyncIterator, Dict, Any
import pandas as pd

class MyAsyncValidator(AsyncDataValidationRule):
    """Custom async validator."""

    def get_description(self) -> str:
        return "My custom async validation"

    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """Execute validation asynchronously."""
        total_rows = 0
        failures = []

        async for chunk in data_iterator:
            # Process chunk asynchronously
            chunk_failures = await self._process_chunk(chunk)
            failures.extend(chunk_failures)
            total_rows += len(chunk)

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Validated {total_rows} rows",
            failed_count=len(failures),
            total_count=total_rows,
            sample_failures=failures
        )

    async def _process_chunk(self, chunk: pd.DataFrame):
        """Process chunk with async operations."""
        # Run blocking operations in executor
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._validate_chunk_sync,
            chunk
        )

    def _validate_chunk_sync(self, chunk: pd.DataFrame):
        """Sync validation logic (runs in executor)."""
        failures = []
        # Your validation logic here
        return failures
```

#### Async File Validator

```python
from validation_framework.validations.async_base import AsyncFileValidationRule

class MyAsyncFileValidator(AsyncFileValidationRule):
    """Async file-level validator."""

    def get_description(self) -> str:
        return "My async file validation"

    async def validate_file_async(
        self,
        context: Dict[str, Any]
    ) -> ValidationResult:
        """Validate file metadata asynchronously."""
        columns = context.get("columns", [])

        # Perform async checks
        result = await self._check_metadata(columns)

        return self._create_result(
            passed=result,
            message="File metadata validated"
        )

    async def _check_metadata(self, columns):
        """Async metadata check."""
        # Your async logic here
        return True
```

## Using the Orchestrator

The orchestrator manages concurrent validation execution with dependency resolution.

### Basic Usage

```python
from validation_framework.core.async_orchestrator import AsyncValidationOrchestrator
from validation_framework.validations.async_validators import (
    AsyncSchemaMatchCheck,
    AsyncLookupCheck
)
from validation_framework.core.results import Severity

# Create orchestrator
orchestrator = AsyncValidationOrchestrator(max_concurrency=5)

# Add validators
schema_validator = AsyncSchemaMatchCheck(
    name="schema_check",
    severity=Severity.ERROR,
    params={"expected_schema": {"id": "integer", "name": "string"}}
)

lookup_validator = AsyncLookupCheck(
    name="lookup_check",
    severity=Severity.ERROR,
    params={
        "field": "id",
        "reference_values": ["1", "2", "3"]
    }
)

# Add tasks (lookup depends on schema)
orchestrator.add_task(
    task_id="schema",
    validator=schema_validator,
    dependencies=set()
)

orchestrator.add_task(
    task_id="lookup",
    validator=lookup_validator,
    dependencies={"schema"}  # Wait for schema check
)

# Execute all
async def data_factory():
    # Return fresh data iterator
    return async_data_iterator()

results = await orchestrator.execute_all(data_factory, context)

for task_id, result in results.items():
    print(f"{task_id}: {result.passed}")
```

### Concurrent Execution (No Dependencies)

```python
# Execute specific tasks concurrently
results = await orchestrator.execute_concurrent(
    task_ids=["schema", "lookup"],
    data_iterator_factory=data_factory,
    context=context
)
```

### Timeout Support

```python
# Add validator with timeout
orchestrator.add_task(
    task_id="slow_check",
    validator=slow_validator,
    dependencies=set(),
    timeout=30.0  # 30 second timeout
)
```

## Adapter Pattern

Use `SyncValidatorAdapter` to run existing sync validators asynchronously:

```python
from validation_framework.validations.async_base import SyncValidatorAdapter
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck

# Create sync validator
sync_validator = MandatoryFieldCheck(
    name="mandatory_check",
    severity=Severity.ERROR,
    params={"fields": ["id", "email"]}
)

# Wrap in adapter
async_validator = SyncValidatorAdapter(sync_validator)

# Use like any async validator
result = await async_validator.validate_async(data_iterator, context)
```

Or use the convenience function:

```python
from validation_framework.validations.async_base import create_async_validator

async_validator = create_async_validator(sync_validator)
```

## Performance Optimization

### Concurrency Limits

Control resource usage with concurrency limits:

```python
# Limit to 10 concurrent validations
orchestrator = AsyncValidationOrchestrator(max_concurrency=10)
```

### Chunk Processing

Process chunks in executor to avoid blocking event loop:

```python
async def validate_async(self, data_iterator, context):
    async for chunk in data_iterator:
        # Run CPU-intensive work in executor
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self._process_chunk_cpu_intensive,
            chunk
        )
```

### Database Connections

Use connection pooling for database lookups:

```python
# In your async validator
async def _load_reference_from_db(self, table, column, conn_string):
    loop = asyncio.get_event_loop()

    def load():
        engine = create_engine(conn_string, pool_size=10, max_overflow=20)
        try:
            df = pd.read_sql_query(f"SELECT DISTINCT {column} FROM {table}", engine)
            return set(df[column].tolist())
        finally:
            engine.dispose()

    return await loop.run_in_executor(None, load)
```

## Best Practices

### 1. Run I/O Operations Asynchronously

Use async for database queries, API calls, and file operations:

```python
# Good: Async database lookup
reference_data = await self._load_reference_from_db(...)

# Avoid: Blocking database call in event loop
# reference_data = self._load_reference_from_db_sync(...)  # Blocks!
```

### 2. Use Executor for CPU-Intensive Work

Run CPU-bound operations in thread pool:

```python
# Good: CPU work in executor
loop = asyncio.get_event_loop()
result = await loop.run_in_executor(None, cpu_intensive_func, data)

# Avoid: CPU work in event loop
# result = cpu_intensive_func(data)  # Blocks event loop!
```

### 3. Define Dependencies Explicitly

Declare validation dependencies for correct execution order:

```python
# Schema must run before data validations
orchestrator.add_task("schema", schema_validator, dependencies=set())
orchestrator.add_task("data_check", data_validator, dependencies={"schema"})
```

### 4. Handle Errors Gracefully

Async validators should handle errors without stopping other validations:

```python
try:
    result = await self._validate_data(chunk)
except Exception as e:
    logger.error(f"Validation error: {e}", exc_info=True)
    return self._create_result(
        passed=False,
        message=f"Error: {str(e)}",
        failed_count=1
    )
```

### 5. Use Timeouts for Long Operations

Prevent validations from hanging indefinitely:

```python
orchestrator.add_task(
    task_id="api_check",
    validator=api_validator,
    timeout=60.0  # 1 minute timeout
)
```

## Migration from Sync to Async

Existing sync validators work automatically with async engine via adapter:

```yaml
# Your existing validation config works as-is
validations:
  - type: "MandatoryFieldCheck"  # Sync validator
    severity: "ERROR"
    params:
      fields: ["id", "email"]

  - type: "AsyncLookupCheck"  # Async validator
    severity: "ERROR"
    params:
      field: "country"
      reference_values: ["US", "UK"]
```

The async engine automatically wraps sync validators in `SyncValidatorAdapter`.

## Troubleshooting

### Event Loop Blocking

If validations are slow, check for blocking operations:

```python
# Bad: Blocking I/O in async function
async def validate_async(self, data_iterator, context):
    data = pd.read_csv("large_file.csv")  # Blocks!

# Good: Run in executor
async def validate_async(self, data_iterator, context):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, pd.read_csv, "large_file.csv")
```

### Circular Dependencies

Orchestrator detects circular dependencies:

```python
# This will raise ValueError
orchestrator.add_task("A", validator_a, dependencies={"B"})
orchestrator.add_task("B", validator_b, dependencies={"A"})  # Circular!
```

### Timeout Errors

Increase timeout for slow validations:

```python
orchestrator.add_task(
    task_id="slow_db_check",
    validator=db_validator,
    timeout=300.0  # 5 minutes
)
```
