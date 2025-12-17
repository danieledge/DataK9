# Async Validators Quick Reference

Quick reference for using async validators in DataK9.

## Built-in Async Validators

### AsyncSchemaMatchCheck

Validates schema structure and data types.

```yaml
- type: "AsyncSchemaMatchCheck"
  severity: "ERROR"
  params:
    expected_schema:
      id: "integer"
      name: "string"
      value: "float"
    strict: false          # No extra columns allowed
    check_order: false     # Check column order
```

### AsyncLookupCheck

Validates against reference data (in-memory or database).

```yaml
# In-memory lookup
- type: "AsyncLookupCheck"
  severity: "ERROR"
  params:
    field: "country_code"
    reference_values: ["US", "UK", "CA"]
    check_type: "allow"    # or "deny"
    description: "Valid countries"

# Database lookup
- type: "AsyncLookupCheck"
  severity: "ERROR"
  params:
    field: "customer_id"
    reference_table: "customers"
    reference_column: "id"
    connection_string: "postgresql://user:pass@host/db"
    description: "Customer exists"
```

### AsyncRegexCheck

Validates field values against regex patterns.

```yaml
- type: "AsyncRegexCheck"
  severity: "ERROR"
  params:
    field: "email"
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    should_match: true     # or false for negative matching
    description: "Valid email"
```

## Programmatic Usage

### Basic Async Validator

```python
from validation_framework.validations.async_validators import AsyncRegexCheck
from validation_framework.core.results import Severity

validator = AsyncRegexCheck(
    name="email_check",
    severity=Severity.ERROR,
    params={
        "field": "email",
        "pattern": r".+@.+\..+",
        "should_match": True
    }
)

result = await validator.validate_async(data_iterator, context)
```

### Using Sync Validators Asynchronously

```python
from validation_framework.validations.async_base import SyncValidatorAdapter
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck

sync_validator = MandatoryFieldCheck(
    name="mandatory",
    severity=Severity.ERROR,
    params={"fields": ["id", "email"]}
)

async_validator = SyncValidatorAdapter(sync_validator)
result = await async_validator.validate_async(data_iterator, context)
```

### Orchestrator: Concurrent Execution

```python
from validation_framework.core.async_orchestrator import AsyncValidationOrchestrator

orchestrator = AsyncValidationOrchestrator(max_concurrency=5)

# Add independent validators (run concurrently)
orchestrator.add_task("schema", schema_validator, dependencies=set())
orchestrator.add_task("email", email_validator, dependencies=set())
orchestrator.add_task("country", country_validator, dependencies=set())

# Execute all concurrently
results = await orchestrator.execute_all(data_factory, context)
```

### Orchestrator: With Dependencies

```python
# Schema must run before data checks
orchestrator.add_task("schema", schema_validator, dependencies=set())
orchestrator.add_task("lookup", lookup_validator, dependencies={"schema"})
orchestrator.add_task("regex", regex_validator, dependencies={"schema"})

# Executes: schema -> (lookup, regex in parallel)
results = await orchestrator.execute_all(data_factory, context)
```

### Orchestrator: With Timeouts

```python
orchestrator.add_task(
    task_id="slow_check",
    validator=slow_validator,
    timeout=60.0  # 60 second timeout
)
```

## Custom Async Validators

### Basic Data Validator

```python
from validation_framework.validations.async_base import AsyncDataValidationRule
import asyncio

class MyAsyncValidator(AsyncDataValidationRule):
    def get_description(self) -> str:
        return "My custom async validation"

    async def validate_async(self, data_iterator, context):
        failures = []
        total = 0

        async for chunk in data_iterator:
            # Run CPU-intensive work in executor
            loop = asyncio.get_event_loop()
            chunk_failures = await loop.run_in_executor(
                None, self._process_chunk, chunk
            )
            failures.extend(chunk_failures)
            total += len(chunk)

        return self._create_result(
            passed=len(failures) == 0,
            message=f"Validated {total} rows",
            failed_count=len(failures),
            total_count=total,
            sample_failures=failures
        )

    def _process_chunk(self, chunk):
        # Your sync validation logic
        return []
```

### File-level Validator

```python
from validation_framework.validations.async_base import AsyncFileValidationRule

class MyAsyncFileValidator(AsyncFileValidationRule):
    def get_description(self) -> str:
        return "My file validator"

    async def validate_file_async(self, context):
        columns = context.get("columns", [])
        # Your async logic here
        return self._create_result(
            passed=True,
            message="File validated"
        )
```

## Common Patterns

### Database Lookup

```python
async def _load_reference_from_db(self, table, column, conn_string):
    loop = asyncio.get_event_loop()

    def load():
        from sqlalchemy import create_engine
        engine = create_engine(conn_string)
        try:
            df = pd.read_sql_query(
                f"SELECT DISTINCT {column} FROM {table}",
                engine
            )
            return set(df[column].tolist())
        finally:
            engine.dispose()

    return await loop.run_in_executor(None, load)
```

### API Call

```python
async def _check_api(self, value):
    import aiohttp

    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.example.com/verify?value={value}") as resp:
            result = await resp.json()
            return result["valid"]
```

### CPU-Intensive Work

```python
async def _process_heavy_computation(self, data):
    loop = asyncio.get_event_loop()

    # Run in executor to avoid blocking event loop
    result = await loop.run_in_executor(
        None,
        self._heavy_computation_sync,
        data
    )
    return result

def _heavy_computation_sync(self, data):
    # CPU-intensive synchronous code
    return result
```

## Performance Tips

1. **Use Concurrency Wisely**
   ```python
   # Good for I/O-bound validations
   orchestrator = AsyncValidationOrchestrator(max_concurrency=10)

   # Lower for CPU-bound validations
   orchestrator = AsyncValidationOrchestrator(max_concurrency=3)
   ```

2. **Run CPU Work in Executor**
   ```python
   # Good: Non-blocking
   result = await loop.run_in_executor(None, cpu_func, data)

   # Bad: Blocks event loop
   result = cpu_func(data)  # DON'T DO THIS
   ```

3. **Use Dependencies**
   ```python
   # Schema check must complete before data checks
   orchestrator.add_task("schema", schema_val, dependencies=set())
   orchestrator.add_task("data", data_val, dependencies={"schema"})
   ```

4. **Set Timeouts**
   ```python
   # Prevent hanging on slow operations
   orchestrator.add_task("api_check", api_val, timeout=30.0)
   ```

## Troubleshooting

### "Event loop is blocked"
- Move CPU-intensive work to executor
- Use `await loop.run_in_executor(None, func, args)`

### "Circular dependency detected"
- Check task dependencies
- Remove circular references
- Use `orchestrator.get_task_info(task_id)` to debug

### "Timeout exceeded"
- Increase timeout: `timeout=120.0`
- Or remove timeout: `timeout=None`
- Check for blocking I/O operations

### "Validation runs slowly"
- Increase concurrency: `max_concurrency=10`
- Check for sequential execution
- Profile with logging: `logging.DEBUG`

## Migration from Sync to Async

Existing sync validators work automatically:

```yaml
# This works as-is - no changes needed
validations:
  - type: "MandatoryFieldCheck"  # Sync validator
    severity: "ERROR"
    params:
      fields: ["id", "email"]
```

The async engine automatically wraps sync validators.

To manually wrap:
```python
from validation_framework.validations.async_base import create_async_validator

async_val = create_async_validator(sync_val)
```
