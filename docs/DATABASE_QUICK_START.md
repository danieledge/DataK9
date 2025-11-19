# Database Validation - Quick Start Guide

## 🚀 Get Started in 3 Minutes

### Step 1: Install Database Driver

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install pymysql

# SQLite - Already installed (built into Python)
```

### Step 2: Create Config File

Create `db_validation.yaml`:

```yaml
validation_job:
  name: "My Database Validation"

  files:
    - name: "my_table"
      path: "postgresql://user:password@localhost/mydb"
      format: "database"
      table: "customers"

      validations:
        - type: "MandatoryFieldCheck"
          severity: "ERROR"
          params:
            fields: ["id", "email"]

        - type: "UniqueKeyCheck"
          severity: "ERROR"
          params:
            fields: ["id"]
```

### Step 3: Run Validation

```bash
python3 -m validation_framework.cli validate db_validation.yaml
```

✅ Done! Check the HTML report.

---

## 📋 Cheat Sheet

### Connection Strings

```bash
# PostgreSQL
postgresql://user:pass@host:5432/database

# MySQL
mysql+pymysql://user:pass@host:3306/database

# SQL Server
mssql+pyodbc://user:pass@host:1433/database?driver=ODBC+Driver+17+for+SQL+Server

# Oracle
oracle+cx_oracle://user:pass@host:1521/?service_name=ORCL

# SQLite
sqlite:///path/to/database.db
```

### CLI Commands

```bash
# Profile a database table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@host/db" \
  --table customers \
  -o profile.html

# Validate with YAML
python3 -m validation_framework.cli validate config.yaml

# List database-compatible validations
python3 -m validation_framework.cli list-validations --source database

# Show compatibility info
python3 -m validation_framework.cli list-validations --show-compatibility
```

### Python API

```python
from validation_framework.loaders.factory import LoaderFactory
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck
from validation_framework.core.results import Severity

# Create loader
loader = LoaderFactory.create_database_loader(
    connection_string="postgresql://user:pass@host/db",
    table="customers"
)

# Create validation
validation = MandatoryFieldCheck(
    name="Required Fields",
    severity=Severity.ERROR,
    params={"fields": ["id", "email"]}
)

# Run validation
result = validation.validate(loader.load_chunks(), {})
print(f"Passed: {result.passed}")
```

---

## 🔍 Common Patterns

### Validate with Custom Query

```yaml
files:
  - name: "active_users"
    path: "postgresql://user:pass@host/db"
    format: "database"
    query: "SELECT * FROM users WHERE active = true AND created_at > '2024-01-01'"

    validations: [...]
```

### Multiple Tables

```yaml
files:
  - name: "customers"
    path: "postgresql://user:pass@host/db"
    format: "database"
    table: "customers"
    validations: [...]

  - name: "orders"
    path: "postgresql://user:pass@host/db"
    format: "database"
    table: "orders"
    validations: [...]
```

### Adjust Chunk Size

```yaml
processing:
  chunk_size: 5000  # Process 5,000 rows at a time (default: 10,000)
```

---

## ✅ Compatible Validations (33/35)

### Field Checks
- MandatoryFieldCheck
- RegexCheck
- ValidValuesCheck
- RangeCheck
- DateFormatCheck
- StringLengthCheck
- NumericPrecisionCheck

### Record Checks
- UniqueKeyCheck
- DuplicateRecordCheck
- BlankRecordCheck
- RecordCountCheck
- RowCountRangeCheck

### Advanced Checks
- CompletenessCheck
- StatisticalOutlierCheck
- CorrelationCheck
- CrossFieldComparisonCheck
- BaselineComparisonCheck
- And 18 more...

**See full list:** `python3 -m validation_framework.cli list-validations --source database`

---

## 🎯 Interactive Demo

Try the built-in demos:

```bash
./demo.sh
# Select option 3: Database Demo
```

Choose from:
1. **Python Script** - See programmatic validation
2. **YAML Config** - See YAML-based validation
3. **Profile Database** - See database profiling

---

## 📖 Full Documentation

**Complete Guide:** `docs/DATABASE_VALIDATION_GUIDE.md`

Topics covered:
- Connection string formats for all databases
- Security best practices
- Performance optimization
- Troubleshooting guide
- Advanced patterns

---

## 🆘 Quick Troubleshooting

**"driver not found"**
```bash
pip install psycopg2-binary  # PostgreSQL
pip install pymysql          # MySQL
```

**"connection refused"**
- Check host/port are correct
- Verify database is running
- Test with: `psql -h localhost -U user -d database`

**"table not found"**
- Verify table name (case-sensitive)
- Check user has SELECT permission
- Try: `SELECT * FROM your_table LIMIT 1;`

**Slow validation**
- Reduce chunk_size in YAML
- Add indexes to columns
- Use read replica for production DBs

---

## 💡 Pro Tips

1. **Use environment variables for credentials**
   ```yaml
   path: "${DATABASE_URL}"
   ```

2. **Pre-filter with queries**
   ```yaml
   query: "SELECT * FROM orders WHERE created_at > NOW() - INTERVAL 7 DAY"
   ```

3. **Validate read replicas**
   ```yaml
   path: "postgresql://readonly@read-replica:5432/db"
   ```

4. **Check compatibility first**
   ```bash
   python3 -m validation_framework.cli list-validations --source database
   ```

---

## 🚀 Next Steps

1. ✅ Read this quick start
2. ✅ Try the interactive demo: `./demo.sh` → option 3
3. ✅ Create your first database YAML config
4. ✅ Run validation: `python3 -m validation_framework.cli validate config.yaml`
5. ✅ Review full guide: `docs/DATABASE_VALIDATION_GUIDE.md`

---

**Questions?** See `docs/DATABASE_VALIDATION_GUIDE.md` for comprehensive documentation.
