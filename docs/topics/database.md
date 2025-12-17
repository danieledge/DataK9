# Database Documentation Index

All documentation related to DataK9 database validation.

---

## Getting Started

- [Database Quick Start](../guides/database/DATABASE_QUICKSTART.md) - Get started in 3 minutes
- [Database Validation Guide](../guides/database/DATABASE_VALIDATION_GUIDE.md) - Complete guide

## Security

- [Database Safety](../guides/database/DATABASE_SAFETY.md) - Production safety features
- [Credentials Security](../guides/database/DATABASE_CREDENTIALS_SECURITY.md) - Secure credential management
- [Security Guidelines](../SECURITY.md) - Overall security best practices

## Reference

- [CLI Reference - Database Options](../reference/cli-reference.md) - Database CLI flags
- [Validation Reference - Database Validations](../reference/validation-reference.md) - SQLCustomCheck, DatabaseReferentialIntegrityCheck, DatabaseConstraintCheck
- [Validation Compatibility](../reference/VALIDATION_COMPATIBILITY.md) - Which validations work with databases

## Supported Databases

| Database | Driver | Install |
|----------|--------|---------|
| PostgreSQL | psycopg2 | `pip install psycopg2-binary` |
| MySQL | pymysql | `pip install pymysql` |
| SQL Server | pyodbc | `pip install pyodbc` |
| Oracle | cx_Oracle | `pip install cx_Oracle` |
| SQLite | Built-in | No install needed |

---

## Database Validation Example

```yaml
database:
  connection_string: "${DB_CONNECTION_STRING}"
  tables:
    - name: "customers"
      validations:
        - type: "MandatoryFieldCheck"
          params:
            fields: ["customer_id", "email"]
        - type: "UniqueKeyCheck"
          params:
            fields: ["customer_id"]
```

**Quick Command:**
```bash
export DB_CONNECTION_STRING="postgresql://user:pass@host/db"
python3 -m validation_framework.cli validate db_config.yaml
```
