# Database Validation - Production Safety Guide

## Critical Production Considerations

### The Risk: Uncontrolled SELECT * Queries

**WARNING:** Without proper safeguards, database validation can cause serious production issues:

- **Memory Exhaustion**: Loading multi-million row tables into memory
- **Database Load**: Long-running queries impacting production performance
- **Network Saturation**: Transferring huge result sets
- **Application Crashes**: Out-of-memory errors
- **Cost Overruns**: Cloud database egress charges

## Safety Mechanisms

DataK9 provides multiple layers of protection for production database validation:

### 1. Row Limits (`max_rows`)

**Recommended for all production validations.**

Limits the total number of rows processed, preventing runaway queries:

```yaml
files:
  - name: customers_validation
    format: database
    connection_string: "postgresql://user:pass@prod-db:5432/sales"
    table: "customers"
    max_rows: 100000  # Safety limit: only process first 100K rows
```

**What happens:**
- System counts total rows before processing
- Warns if table exceeds `max_rows`
- Automatically adds `LIMIT` clause to query
- Stops processing after limit reached

**Example Output:**
```
WARNING - Table/query has 5,234,891 rows but max_rows=100,000.
          Only processing first 100,000 rows for safety.
INFO    - Processing 100,000 rows (stopped at safety limit)
```

### 2. Custom SQL Queries (Recommended)

**Best practice: Filter data at the database level.**

Instead of `SELECT *`, use queries with:
- **WHERE clauses** to filter data
- **Time-based filtering** for recent data only
- **Sampling** for statistical validation
- **Aggregations** for summary checks

```yaml
files:
  - name: recent_orders_validation
    format: database
    connection_string: "postgresql://user:pass@prod-db:5432/sales"
    query: |
      SELECT *
      FROM orders
      WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
      AND status IN ('pending', 'processing')
    # No max_rows needed - query naturally limits data
```

**Why this is better:**
- Filtering happens on database (uses indexes)
- Only relevant data transferred over network
- Validates most recent/critical data
- Predictable resource usage

### 3. Chunked Processing (Automatic)

DataK9 always processes data in chunks (default 10,000 rows):

```yaml
processing:
  chunk_size: 10000  # Rows per chunk (default)
```

**Benefits:**
- Never loads entire table into memory
- Constant memory footprint regardless of table size
- Can validate billion-row tables safely
- Progress visible in real-time

## Production Validation Strategies

### Strategy 1: Recent Data Validation (Recommended)

Validate only recent data where quality issues are most likely:

```yaml
files:
  - name: recent_transactions
    format: database
    connection_string: "${DB_PROD_CONNECTION}"  # Use env vars for credentials
    query: |
      SELECT t.*, c.customer_name
      FROM transactions t
      JOIN customers c ON t.customer_id = c.id
      WHERE t.created_date >= CURRENT_DATE - INTERVAL '24 hours'
```

**Use cases:**
- Daily data quality checks
- Real-time validation pipelines
- Monitoring data ingestion quality
- Detecting recent data issues

### Strategy 2: Statistical Sampling

Validate a representative sample for large tables:

```yaml
files:
  - name: customer_sample
    format: database
    connection_string: "${DB_PROD_CONNECTION}"
    query: |
      SELECT *
      FROM customers
      TABLESAMPLE SYSTEM (1)  -- PostgreSQL: random 1% sample
      LIMIT 50000
```

**Database-specific sampling:**

**PostgreSQL:**
```sql
SELECT * FROM large_table TABLESAMPLE SYSTEM (5);  -- 5% sample
```

**MySQL:**
```sql
SELECT * FROM large_table WHERE RAND() < 0.05 LIMIT 50000;  -- ~5% sample
```

**SQL Server:**
```sql
SELECT * FROM large_table TABLESAMPLE (5 PERCENT);  -- 5% sample
```

**Oracle:**
```sql
SELECT * FROM large_table SAMPLE (5);  -- 5% sample
```

### Strategy 3: Critical Columns Only

Select only fields you're validating:

```yaml
files:
  - name: email_validation
    format: database
    connection_string: "${DB_PROD_CONNECTION}"
    query: |
      SELECT customer_id, email, registration_date
      FROM customers
      WHERE registration_date >= '2024-01-01'
    max_rows: 500000

    validations:
      - type: RegexCheck
        params:
          field: email
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        severity: ERROR
```

**Benefits:**
- Reduced network transfer
- Faster query execution
- Lower memory usage
- Focus on specific quality issues

### Strategy 4: Incremental Validation

Validate only new/changed data using watermarks:

```yaml
files:
  - name: incremental_validation
    format: database
    connection_string: "${DB_PROD_CONNECTION}"
    query: |
      SELECT *
      FROM orders
      WHERE updated_at > (
        SELECT MAX(validated_until)
        FROM validation_watermarks
        WHERE table_name = 'orders'
      )
```

**Implementation:**
1. Track last validation timestamp
2. Query only records changed since then
3. Update watermark after successful validation
4. Minimal impact on production

## Read-Only Database Access

**CRITICAL: Always use read-only database users for validation.**

### Creating Read-Only Users

**PostgreSQL:**
```sql
CREATE USER dataq_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE sales_db TO dataq_readonly;
GRANT USAGE ON SCHEMA public TO dataq_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dataq_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dataq_readonly;
```

**MySQL:**
```sql
CREATE USER 'dataq_readonly'@'%' IDENTIFIED BY 'secure_password';
GRANT SELECT ON sales_db.* TO 'dataq_readonly'@'%';
FLUSH PRIVILEGES;
```

**SQL Server:**
```sql
CREATE LOGIN dataq_readonly WITH PASSWORD = 'SecurePassword123!';
CREATE USER dataq_readonly FOR LOGIN dataq_readonly;
ALTER ROLE db_datareader ADD MEMBER dataq_readonly;
```

### Why Read-Only Matters

- **Prevents accidental writes**: No risk of data modification
- **Security**: Limited blast radius if credentials compromised
- **Compliance**: Audit trail shows validation is read-only
- **Confidence**: Run validation on production safely

## Connection String Security

### Never Hardcode Credentials

**❌ BAD:**
```yaml
connection_string: "postgresql://admin:MyPassword123@prod-db:5432/sales"
```

**✅ GOOD:**
```yaml
connection_string: "${DATABASE_URL}"  # Environment variable
```

### Environment Variable Best Practices

```bash
# .env file (git-ignored)
DATABASE_URL="postgresql://dataq_readonly:secure_pass@prod-db:5432/sales"

# Or use secrets management
export DATABASE_URL=$(aws secretsmanager get-secret-value --secret-id prod/dataq/db --query SecretString --output text)
```

### Connection String Formats

```yaml
# PostgreSQL
connection_string: "postgresql://user:pass@host:5432/database"

# MySQL
connection_string: "mysql+pymysql://user:pass@host:3306/database"

# SQL Server
connection_string: "mssql+pyodbc://user:pass@host:1433/database?driver=ODBC+Driver+17+for+SQL+Server"

# Oracle
connection_string: "oracle+cx_oracle://user:pass@host:1521/?service_name=ORCL"

# SQLite (local file)
connection_string: "sqlite:///path/to/database.db"
```

## Performance Optimization

### 1. Use Database Indexes

Ensure WHERE clause columns are indexed:

```sql
-- Add index for time-based filtering
CREATE INDEX idx_orders_created_date ON orders(created_date);

-- Add index for status filtering
CREATE INDEX idx_orders_status ON orders(status);

-- Composite index for common query patterns
CREATE INDEX idx_orders_date_status ON orders(created_date, status);
```

### 2. Query During Off-Peak Hours

```yaml
# Schedule validations during low-traffic periods
# Use cron: 0 2 * * * (2 AM daily)
```

### 3. Read Replicas

**Best practice:** Run validations against read replicas, not primary database:

```yaml
connection_string: "postgresql://dataq_readonly:pass@read-replica-1:5432/sales"
```

**Benefits:**
- Zero impact on production writes
- Can run longer queries
- Multiple replicas for load distribution
- Replica lag acceptable for quality checks

### 4. Connection Pooling

For frequent validations, consider connection pooling:

```python
# Custom validation script with pooling
from sqlalchemy.pool import QueuePool
engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10
)
```

## Production Deployment Checklist

- [ ] Use read-only database credentials
- [ ] Set `max_rows` limit or use filtered queries
- [ ] Store credentials in environment variables / secrets manager
- [ ] Use database read replicas if available
- [ ] Add indexes for query filter columns
- [ ] Test on development database first
- [ ] Schedule during off-peak hours
- [ ] Set up monitoring/alerting for validation job failures
- [ ] Document expected validation runtime
- [ ] Have rollback plan if validation causes issues

## Example Production Configuration

```yaml
validation_job:
  name: "Production Customer Data Quality - Daily"
  description: "Validates recent customer data without impacting production"

  files:
    - name: recent_customers
      format: database
      connection_string: "${PROD_READ_REPLICA_URL}"  # Read replica
      query: |
        SELECT customer_id, email, phone, created_date, status
        FROM customers
        WHERE created_date >= CURRENT_DATE - INTERVAL '7 days'
        OR updated_at >= CURRENT_DATE - INTERVAL '7 days'
      max_rows: 100000  # Safety limit

      validations:
        - type: MandatoryFieldCheck
          params:
            fields: [customer_id, email]
          severity: ERROR

        - type: RegexCheck
          params:
            field: email
            pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
          severity: ERROR

        - type: ValidValuesCheck
          params:
            field: status
            valid_values: [active, inactive, suspended]
          severity: WARNING

  output:
    html_report: "/var/log/dataq/customer_validation_{date}.html"
    json_summary: "/var/log/dataq/customer_validation_{date}.json"
    fail_on_error: true

  processing:
    chunk_size: 10000
    max_sample_failures: 100
```

## Monitoring and Alerting

### Key Metrics to Track

1. **Validation Runtime**: Track execution time trends
2. **Rows Processed**: Monitor data volume
3. **Failure Rates**: Alert on increasing error percentages
4. **Database Impact**: Monitor query execution time on database
5. **Memory Usage**: Ensure chunking is working correctly

### Example Prometheus Metrics

```python
# Track validation metrics
validation_duration_seconds.labels(table="customers").observe(duration)
validation_rows_processed.labels(table="customers").inc(row_count)
validation_failures.labels(table="customers", severity="ERROR").inc(error_count)
```

## Troubleshooting

### "Query timeout" errors

**Solution:** Reduce `max_rows` or add more specific WHERE filters

### "Out of memory" errors

**Solution:** Reduce `chunk_size` in processing configuration

### Slow validation performance

**Solutions:**
- Add database indexes for query filters
- Use read replica
- Reduce `max_rows`
- Select fewer columns
- Run during off-peak hours

### Connection pool exhaustion

**Solution:** Reduce concurrent validation jobs or increase pool size

## Summary: Safe Production Database Validation

**Golden Rules:**
1. **Always use `max_rows` or filtered queries** - Never unbounded SELECT *
2. **Read-only credentials only** - No write access
3. **Environment variables for credentials** - Never hardcode
4. **Use read replicas when possible** - Zero production impact
5. **Test on dev/staging first** - Validate before production
6. **Monitor and alert** - Track performance and failures

With these safety mechanisms, DataK9 can safely validate production databases without risking downtime or performance degradation.
