# Database Validation Guide

## Overview

DataK9 supports validating data directly from SQL databases without requiring data export. This guide covers everything you need to know about database validation.

## Supported Databases

DataK9 supports the following database systems:

| Database | Connection String Format | Driver Required |
|----------|-------------------------|-----------------|
| **PostgreSQL** | `postgresql://user:pass@host:port/database` | `psycopg2-binary` |
| **MySQL** | `mysql+pymysql://user:pass@host:port/database` | `pymysql` |
| **SQL Server** | `mssql+pyodbc://user:pass@host:port/database` | `pyodbc` |
| **Oracle** | `oracle+cx_oracle://user:pass@host:port/?service_name=service` | `cx-Oracle` |
| **SQLite** | `sqlite:///path/to/database.db` | Built-in (no driver needed) |

## Quick Start

**Security First:** Before configuring database connections, review the **[Database Credentials Security Guide](DATABASE_CREDENTIALS_SECURITY.md)** for secure credential storage using environment variables, systemd credentials, HashiCorp Vault, or AWS Secrets Manager. Never commit credentials to version control.

### 1. Install Database Drivers

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install pymysql

# SQL Server
pip install pyodbc

# Oracle
pip install cx-Oracle

# SQLite - No installation needed (built into Python)
```

### 2. Create a YAML Configuration

**Option A: Validate a Specific Table**

```yaml
validation_job:
  name: "Customer Database Validation"
  version: "1.0"
  description: "Validate customer data from production database"

  files:
    - name: "customers"
      path: "postgresql://user:password@localhost:5432/production_db"
      format: "database"
      table: "customers"  # Table to validate

      validations:
        - type: "MandatoryFieldCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id", "email", "created_at"]

        - type: "UniqueKeyCheck"
          severity: "ERROR"
          params:
            fields: ["customer_id"]

        - type: "RegexCheck"
          severity: "ERROR"
          params:
            field: "email"
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

  output:
    html_report: "customer_validation_report.html"
    json_summary: "customer_validation_summary.json"

  processing:
    chunk_size: 10000  # Process 10k rows at a time
```

**Option B: Validate with Custom SQL Query**

```yaml
validation_job:
  name: "Active Orders Validation"

  files:
    - name: "active_orders"
      path: "mysql+pymysql://user:password@localhost:3306/ecommerce"
      format: "database"
      query: "SELECT * FROM orders WHERE status = 'ACTIVE' AND created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)"

      validations:
        - type: "RangeCheck"
          severity: "WARNING"
          params:
            field: "order_total"
            min_value: 0
            max_value: 100000
```

### 3. Run the Validation

```bash
python3 -m validation_framework.cli validate database_validation.yaml
```

## CLI Commands

### Profile a Database Table

Profile a database table to understand data characteristics and generate suggested validations:

```bash
# Profile a specific table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/mydb" \
  --table customers \
  -o customer_profile.html \
  -j customer_profile.json

# Profile with custom query
python3 -m validation_framework.cli profile \
  --database "sqlite:///test.db" \
  --query "SELECT * FROM users WHERE active = 1" \
  -o active_users_profile.html
```

### List Database-Compatible Validations

See which validations work with database sources:

```bash
# List all database-compatible validations
python3 -m validation_framework.cli list-validations --source database

# Show compatibility information
python3 -m validation_framework.cli list-validations --show-compatibility

# Filter by category
python3 -m validation_framework.cli list-validations --source database --category "Field Checks"
```

## Programmatic Usage

### Python Script Example

```python
from validation_framework.loaders.factory import LoaderFactory
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck, RangeCheck
from validation_framework.core.results import Severity

# Create database loader
loader = LoaderFactory.create_database_loader(
    connection_string="postgresql://user:pass@localhost/mydb",
    table="customers",
    chunk_size=10000
)

# Get row count
total_rows = loader.get_row_count()
print(f"Total rows: {total_rows:,}")

# Run validation
validation = MandatoryFieldCheck(
    name="Required Fields",
    severity=Severity.ERROR,
    params={"fields": ["customer_id", "email", "first_name", "last_name"]}
)

# Validate data
result = validation.validate(loader.load_chunks(), context={})

# Check results
if result.passed:
    print("✓ All required fields present")
else:
    print(f"✗ Found {result.failed_count} rows with missing fields")
    for failure in result.sample_failures[:5]:
        print(f"  Row {failure['row']}: {failure['message']}")
```

## Validation Compatibility

### ✅ Database-Compatible (33/35 validations)

These validations work identically on files and databases:

**Field Checks:**
- MandatoryFieldCheck
- RegexCheck
- ValidValuesCheck
- RangeCheck
- DateFormatCheck
- And more...

**Record Checks:**
- UniqueKeyCheck
- DuplicateRecordCheck
- BlankRecordCheck
- RecordCountCheck
- And more...

**Advanced Checks:**
- CompletenessCheck
- StatisticalOutlierCheck
- CorrelationCheck
- CrossFieldComparisonCheck
- And more...

### 🗄️ Database-Only Validations (2)

These validations are specifically designed for databases:

1. **SQLCustomCheck** - Run custom SQL queries for validation
2. **DatabaseReferentialIntegrityCheck** - Check foreign key relationships

### 📁 File-Only Validations (2)

These don't apply to databases:

1. **EmptyFileCheck** - Checks if file is empty (not applicable to tables)
2. **FileFormatCheck** - Validates file format (not applicable to databases)

## Connection String Examples

### PostgreSQL

```yaml
# Standard connection
path: "postgresql://username:password@localhost:5432/database_name"

# With SSL
path: "postgresql://user:pass@localhost:5432/db?sslmode=require"

# With schema
path: "postgresql://user:pass@localhost:5432/db?options=-c%20search_path=myschema"
```

### MySQL

```yaml
# Standard connection
path: "mysql+pymysql://username:password@localhost:3306/database_name"

# With charset
path: "mysql+pymysql://user:pass@localhost:3306/db?charset=utf8mb4"
```

### SQL Server

```yaml
# Windows authentication
path: "mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

# SQL Server authentication
path: "mssql+pyodbc://username:password@server:1433/database?driver=ODBC+Driver+17+for+SQL+Server"
```

### Oracle

```yaml
# Service name
path: "oracle+cx_oracle://username:password@hostname:1521/?service_name=ORCL"

# SID
path: "oracle+cx_oracle://username:password@hostname:1521/sid"
```

### SQLite

```yaml
# Absolute path
path: "sqlite:////absolute/path/to/database.db"

# Relative path
path: "sqlite:///relative/path/database.db"

# In-memory
path: "sqlite:///:memory:"
```

## Best Practices

### 1. Use Chunked Processing

Process large tables in chunks to avoid memory issues:

```yaml
processing:
  chunk_size: 10000  # Adjust based on table size and available memory
```

**Recommended chunk sizes:**
- Small tables (<100K rows): 50,000 rows
- Medium tables (100K-10M rows): 10,000 rows
- Large tables (10M+ rows): 5,000 rows

### 2. Use Queries to Pre-Filter Data

Validate only relevant data by using SQL queries:

```yaml
# Good: Only validate recent active orders
query: "SELECT * FROM orders WHERE status = 'ACTIVE' AND created_at > DATE_SUB(NOW(), INTERVAL 7 DAY)"

# Avoid: Validating entire historical table
table: "orders"  # Could be billions of rows!
```

### 3. Index Database Columns

Ensure columns used in queries are indexed for better performance:

```sql
-- Create indexes on frequently validated columns
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_orders_status ON orders(status);
```

### 4. Use Read Replicas

For production databases, consider using read replicas to avoid impacting production:

```yaml
path: "postgresql://readonly_user:pass@read-replica:5432/production_db"
```

### 5. Secure Credentials

Never hard-code credentials in YAML files. Use environment variables:

```yaml
# In YAML
path: "${DB_CONNECTION_STRING}"

# In shell
export DB_CONNECTION_STRING="postgresql://user:pass@host/db"
python3 -m validation_framework.cli validate config.yaml
```

Or use a secrets management system:

```python
import os
from validation_framework.loaders.factory import LoaderFactory

# Get connection string from environment or secrets manager
connection_string = os.environ.get('DATABASE_URL')

loader = LoaderFactory.create_database_loader(
    connection_string=connection_string,
    table="customers"
)
```

## Performance Considerations

### Memory Usage

Database validations use the same chunked processing as file validations:

- **Memory footprint**: ~(chunk_size × row_size) bytes
- **Default chunk size**: 10,000 rows
- **Only one chunk in memory at a time**

### Network Overhead

Database queries incur network latency:

- **Local database**: Minimal overhead (~1-5ms per chunk)
- **Remote database**: Network latency affects performance
- **Large result sets**: Use `chunk_size` to balance network vs memory

### Query Optimization

For custom queries, ensure they're optimized:

```sql
-- Good: Uses index, specific columns
SELECT customer_id, email, created_at
FROM customers
WHERE status = 'ACTIVE'
AND created_at > '2024-01-01'

-- Avoid: Full table scan, SELECT *
SELECT * FROM customers
```

## Troubleshooting

### Connection Errors

**Error: "could not connect to server"**

```bash
# Check connection string format
# Verify host, port, username, password
# Ensure database is running and accessible

# Test connection manually
psql -h localhost -p 5432 -U username -d database_name
```

**Error: "driver not found"**

```bash
# Install required driver
pip install psycopg2-binary  # PostgreSQL
pip install pymysql          # MySQL
pip install pyodbc           # SQL Server
pip install cx-Oracle        # Oracle
```

### Performance Issues

**Validation is slow**

1. **Reduce chunk size** if memory allows larger chunks:
   ```yaml
   processing:
     chunk_size: 50000
   ```

2. **Add indexes** to columns used in queries

3. **Use read replica** to avoid production impact

4. **Pre-filter with query** instead of validating entire table

**Out of memory errors**

1. **Reduce chunk size**:
   ```yaml
   processing:
     chunk_size: 5000  # Smaller chunks
   ```

2. **Check query** - ensure it's not loading too much data per row

### Validation Failures

**Error: "table not found"**

- Verify table name is correct (case-sensitive in some databases)
- Check schema/database name
- Ensure user has SELECT permissions

**Error: "column not found"**

- Verify column names in validation params
- Check for typos
- Ensure columns exist in the table

## Examples

See the `examples/` directory for complete working examples:

- **[database_validation_config.yaml](../examples/database_validation_config.yaml)** - Comprehensive database validation example
- **[database_validation_test.yaml](../examples/database_validation_test.yaml)** - Simple test example
- **[run_database_validation.py](../examples/run_database_validation.py)** - Python script example

## Demo

Run the interactive database demo:

```bash
./demo.sh
# Select option 3: Database Demo
```

The demo includes:
1. Python script validation example
2. YAML configuration example
3. Database profiling example

## Next Steps

- [View all validation types](reference/validation-reference.md)
- [Learn about profiling](using-datak9/data-profiling.md)
- [Integrate with CI/CD](using-datak9/cicd-integration.md)
- [Explore configuration options](using-datak9/configuration-guide.md)
