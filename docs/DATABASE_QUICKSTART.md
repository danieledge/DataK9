# Database Validation Quick Start Guide

## Choose Your Path

DataK9 validates both **files** (CSV, Excel, JSON, Parquet) and **databases** (PostgreSQL, MySQL, SQL Server, Oracle, SQLite).

### 📁 File Validation → [File Quick Start](./FILE_QUICKSTART.md)

### 🗄️ Database Validation → **You're in the right place!**

---

## Database Validation in 3 Steps

### Step 1: Basic Table Validation

Validate an entire database table:

```yaml
validation_job:
  name: "Customer Data Validation"

  files:
    - name: customers
      format: database
      connection_string: "postgresql://user:pass@localhost:5432/mydb"
      table: "customers"
      max_rows: 10000  # Safety limit (recommended)

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

  output:
    html_report: "customer_validation_report.html"
    json_summary: "customer_validation_summary.json"
    fail_on_error: true
```

**Run:**
```bash
python3 -m validation_framework.cli validate config.yaml
```

### Step 2: Filtered Query Validation (Recommended for Production)

Validate only recent or filtered data:

```yaml
files:
  - name: recent_orders
    format: database
    connection_string: "${DATABASE_URL}"  # Use env variable
    query: |
      SELECT * FROM orders
      WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
      AND status IN ('pending', 'processing')

    validations:
      - type: DateFormatCheck
        params:
          field: order_date
          date_format: "%Y-%m-%d"
        severity: ERROR
```

### Step 3: Profile Database Table

Analyze data quality and get validation suggestions:

```bash
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost:5432/mydb" \
  --table customers \
  -o customer_profile.html \
  -j customer_profile.json
```

---

## Database vs File Configuration

### File Source
```yaml
files:
  - name: sales_data
    path: "data/sales.csv"      # File path
    format: csv                 # File format
    delimiter: ","
    encoding: "utf-8"
```

### Database Source
```yaml
files:
  - name: sales_data
    format: database                                    # Database format
    connection_string: "postgresql://user:pass@host/db" # Connection
    table: "sales"                                      # Table name
    max_rows: 50000                                     # Safety limit
```

**Key Differences:**
- **No `path`** - use `connection_string` instead
- **No file options** - no delimiter, encoding, header
- **Add `table` or `query`** - specify what to validate
- **Add `max_rows`** - safety limit (highly recommended)

---

## Supported Databases

| Database | Connection String Example |
|----------|---------------------------|
| **PostgreSQL** | `postgresql://user:password@host:5432/database` |
| **MySQL** | `mysql+pymysql://user:password@host:3306/database` |
| **SQL Server** | `mssql+pyodbc://user:password@host:1433/database?driver=ODBC+Driver+17+for+SQL+Server` |
| **Oracle** | `oracle+cx_oracle://user:password@host:1521/?service_name=ORCL` |
| **SQLite** | `sqlite:///path/to/database.db` |

---

## Database-Compatible Validations

**All 35 validation types work with databases**, but these are database-specific:

### Database-Specific Validations

**DatabaseReferentialIntegrityCheck** - Check foreign key relationships
```yaml
- type: DatabaseReferentialIntegrityCheck
  params:
    foreign_key_field: customer_id
    reference_table: customers
    reference_field: customer_id
  severity: ERROR
```

**DatabaseConstraintCheck** - Verify database constraints
```yaml
- type: DatabaseConstraintCheck
  params:
    constraint: UNIQUE
    field: email
  severity: ERROR
```

**SQLCustomCheck** - Run custom SQL validation queries
```yaml
- type: SQLCustomCheck
  params:
    query: |
      SELECT order_id, total_amount,
             (SELECT SUM(line_total) FROM order_items
              WHERE order_id = orders.order_id) as calculated_total
      FROM orders
      WHERE ABS(total_amount - calculated_total) > 0.01
  severity: WARNING
```

### All Other Validations

All standard validations work with databases:
- Field Validations: MandatoryFieldCheck, UniqueKeyCheck, RegexCheck, RangeCheck, etc.
- Schema Validations: SchemaMatchCheck, ColumnPresenceCheck
- Statistical: StatisticalOutlierCheck, AdvancedAnomalyDetectionCheck
- Cross-Field: CrossFieldComparisonCheck, CompletenessCheck
- And 25+ more...

**See:** [Complete Validation Catalog](./VALIDATION_CATALOG.md)

---

## Production Safety

**⚠️ CRITICAL: Always use production safety features**

### 1. Use max_rows Limit
```yaml
max_rows: 50000  # Process max 50K rows
```

### 2. Use Filtered Queries
```yaml
query: |
  SELECT * FROM large_table
  WHERE created_date >= CURRENT_DATE - 7
```

### 3. Use Read-Only Credentials
```bash
# PostgreSQL read-only user
CREATE USER dataq_readonly WITH PASSWORD 'secure_password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dataq_readonly;
```

### 4. Use Environment Variables
```yaml
connection_string: "${DATABASE_URL}"  # Never hardcode credentials
```

### 5. Use Read Replicas
```yaml
connection_string: "postgresql://readonly@read-replica:5432/db"
```

**See:** [Complete Production Safety Guide](./DATABASE_SAFETY.md)

---

## Common Patterns

### Pattern 1: Daily Data Quality Check
```yaml
validation_job:
  name: "Daily Customer Data Quality"

  files:
    - name: new_customers
      format: database
      connection_string: "${PROD_READ_REPLICA}"
      query: |
        SELECT * FROM customers
        WHERE created_date >= CURRENT_DATE - 1

      validations:
        - type: MandatoryFieldCheck
          params:
            fields: [customer_id, email, status]
          severity: ERROR
```

**Schedule:** `cron: 0 8 * * *` (8 AM daily)

### Pattern 2: Sample Large Table
```yaml
files:
  - name: transaction_sample
    format: database
    connection_string: "${DATABASE_URL}"
    query: |
      SELECT * FROM transactions
      TABLESAMPLE SYSTEM (5)  -- 5% random sample
      LIMIT 100000
    max_rows: 100000
```

### Pattern 3: Critical Columns Only
```yaml
files:
  - name: email_validation
    format: database
    connection_string: "${DATABASE_URL}"
    query: |
      SELECT customer_id, email, email_verified
      FROM customers
      WHERE email_verified = false

    validations:
      - type: RegexCheck
        params:
          field: email
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        severity: ERROR
```

### Pattern 4: Cross-Table Validation
```yaml
files:
  - name: orphaned_orders
    format: database
    connection_string: "${DATABASE_URL}"

    validations:
      - type: DatabaseReferentialIntegrityCheck
        params:
          foreign_key_field: customer_id
          reference_table: customers
          reference_field: customer_id
        severity: ERROR
```

---

## Troubleshooting

### "Missing database driver" Error

**PostgreSQL:**
```bash
pip install psycopg2-binary
```

**MySQL:**
```bash
pip install pymysql
```

**SQL Server:**
```bash
pip install pyodbc
```

**Oracle:**
```bash
pip install cx-Oracle
```

### "Connection refused" Error

1. Check connection string format
2. Verify database is running
3. Check firewall/security groups
4. Verify credentials are correct

### "Query timeout" Error

**Solutions:**
- Reduce `max_rows`
- Add more specific WHERE filters
- Add indexes for query columns
- Use read replica

### Memory Issues

**Solutions:**
- Reduce `chunk_size` in processing config
- Reduce `max_rows`
- Select fewer columns in query

---

## Next Steps

1. **Try the examples** in `examples/` directory
2. **Read production safety guide**: [DATABASE_SAFETY.md](./DATABASE_SAFETY.md)
3. **Explore validation catalog**: [VALIDATION_CATALOG.md](./VALIDATION_CATALOG.md)
4. **Use DataK9 Studio** for visual configuration: `datak9-studio.html`

## Examples

**Quick test with SQLite:**
```bash
# Create test database
python3 scripts/create_test_database.py

# Run simple validation
python3 -m validation_framework.cli validate examples/database_validation_test.yaml

# Profile database table
python3 -m validation_framework.cli profile --database "sqlite:///test_data.db" --table customers -o profile.html
```

**Production PostgreSQL:**
```bash
# Set credentials
export DATABASE_URL="postgresql://dataq_readonly:password@prod-replica:5432/sales"

# Run safe validation
python3 -m validation_framework.cli validate examples/database_validation_safe_production.yaml
```

---

## Help & Support

- 📖 [Complete Documentation](./README.md)
- 🛡️ [Production Safety Guide](./DATABASE_SAFETY.md)
- 📋 [Validation Catalog](./VALIDATION_CATALOG.md)
- 💡 [DataK9 Studio](../datak9-studio.html) - Visual configuration tool
- 🐛 [Report Issues](https://github.com/anthropics/claude-code/issues)
