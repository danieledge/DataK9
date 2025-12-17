# Validation Compatibility Matrix

## Overview

DataK9 supports **37 validation types** that work with both **file sources** (CSV, Excel, JSON, Parquet) and **database sources** (PostgreSQL, MySQL, SQL Server, Oracle, SQLite).

## Quick Reference

✅ **All validations work with both files and databases**

🔵 **3 validations are database-specific** (require database features)

---

## Compatibility Matrix

| Validation Type | Files | Databases | Notes |
|----------------|:-----:|:---------:|-------|
| **Field Validations** |
| MandatoryFieldCheck | ✅ | ✅ | Required fields not null/empty |
| UniqueKeyCheck | ✅ | ✅ | Uses bloom filter for efficiency |
| RegexCheck | ✅ | ✅ | Pattern matching |
| RangeCheck | ✅ | ✅ | Numeric min/max validation |
| ValidValuesCheck | ✅ | ✅ | Enum/categorical validation |
| StringLengthCheck | ✅ | ✅ | Min/max string length |
| DateFormatCheck | ✅ | ✅ | Date format validation |
| NumericPrecisionCheck | ✅ | ✅ | Decimal precision check |
| InlineRegexCheck | ✅ | ✅ | Inline pattern validation |
| **Schema Validations** |
| SchemaMatchCheck | ✅ | ✅ | Expected columns and types |
| ColumnPresenceCheck | ✅ | ✅ | Required columns exist |
| BlankRecordCheck | ✅ | ✅ | Completely empty rows |
| DuplicateRowCheck | ✅ | ✅ | Duplicate records |
| **Cross-Field Validations** |
| CrossFieldComparisonCheck | ✅ | ✅ | Compare two fields (price > cost) |
| CompletenessCheck | ✅ | ✅ | Minimum completeness % |
| **Statistical Validations** |
| StatisticalOutlierCheck | ✅ | ✅ | Z-score or IQR outliers |
| AdvancedAnomalyDetectionCheck | ✅ | ✅ | ML-based anomaly detection |
| CorrelationCheck | ✅ | ✅ | Field correlation analysis |
| DistributionCheck | ✅ | ✅ | Statistical distribution check |
| **Business Rule Validations** |
| InlineBusinessRuleCheck | ✅ | ✅ | Custom business logic |
| InlineLookupCheck | ✅ | ✅ | Value lookup validation |
| ConditionalValidation | ✅ | ✅ | Conditional logic |
| **Temporal Validations** |
| FreshnessCheck | ✅ | ✅ | Data recency validation |
| TrendDetectionCheck | ✅ | ✅ | Trend anomaly detection |
| BaselineComparisonCheck | ✅ | ✅ | Historical baseline comparison |
| **Cross-File/Table Validations** |
| ReferentialIntegrityCheck | ✅ | ✅ | Foreign key validation |
| CrossFileComparisonCheck | ✅ | ✅ | Aggregate comparisons |
| CrossFileDuplicateCheck | ✅ | ✅ | Cross-file duplicate detection |
| **Metadata Validations** |
| RowCountRangeCheck | ✅ | ✅ | Expected row count range |
| EmptyFileCheck | ✅ | ❌ | File-only (checks file size) |
| **Database-Specific Validations** | | |
| DatabaseConstraintCheck | ❌ | 🔵 | Database constraints (UNIQUE, NOT NULL, etc.) |
| DatabaseReferentialIntegrityCheck | ❌ | 🔵 | Database foreign keys |
| SQLCustomCheck | ❌ | 🔵 | Custom SQL validation queries |

**Legend:**
- ✅ Works with this source type
- 🔵 Database-specific validation
- ❌ Not applicable to this source type

---

## Database-Specific Validations

These 3 validations leverage database features and only work with database sources:

### 1. DatabaseConstraintCheck

Verifies database-level constraints are enforced:

```yaml
- type: DatabaseConstraintCheck
  params:
    constraint: UNIQUE        # UNIQUE, NOT NULL, CHECK, PRIMARY KEY
    field: email
  severity: ERROR
```

**Use cases:**
- Verify UNIQUE constraints are working
- Check NOT NULL enforcement
- Validate CHECK constraints
- Confirm PRIMARY KEY uniqueness

### 2. DatabaseReferentialIntegrityCheck

Validates foreign key relationships between tables:

```yaml
- type: DatabaseReferentialIntegrityCheck
  params:
    foreign_key_field: customer_id
    reference_table: customers
    reference_field: customer_id
  severity: ERROR
```

**Use cases:**
- Check foreign key integrity
- Find orphaned records
- Validate cross-table relationships
- Ensure referential consistency

### 3. SQLCustomCheck

Run custom SQL queries for validation:

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

**Use cases:**
- Complex business logic validation
- Cross-table calculations
- Custom data quality checks
- Database-specific validations

---

## File-Specific Validations

### 1. EmptyFileCheck

Detects empty or zero-byte files:

```yaml
- type: EmptyFileCheck
  severity: ERROR
```

**Use cases:**
- Prevent processing empty data feeds
- Detect failed file transfers
- Validate ETL pipeline outputs

---

## Cross-Source Validations

These validations can validate data **across different source types**:

### ReferentialIntegrityCheck

Works across:
- File → File (CSV customers, CSV orders)
- Database → Database (table to table)
- **File → Database** (CSV file referencing database table)
- **Database → File** (database table referencing CSV file)

```yaml
# Example: CSV file referencing database table
files:
  - name: new_orders_csv
    path: "data/new_orders.csv"
    format: csv

    validations:
      - type: ReferentialIntegrityCheck
        params:
          foreign_key: customer_id
          reference_file: customer_database
          reference_key: customer_id
        severity: ERROR

  - name: customer_database
    format: database
    connection_string: "postgresql://user:pass@localhost/db"
    table: "customers"
```

---

## Validation Selection Guide

### For Files (CSV, Excel, JSON, Parquet)

**Recommended Validations:**
1. **SchemaMatchCheck** - Verify expected columns and types
2. **MandatoryFieldCheck** - Required fields present
3. **UniqueKeyCheck** - Key fields are unique
4. **RegexCheck** - Format validation (emails, phones, etc.)
5. **RangeCheck** - Numeric bounds
6. **DateFormatCheck** - Date consistency
7. **EmptyFileCheck** - Detect empty files
8. **RowCountRangeCheck** - Expected volume

### For Databases (PostgreSQL, MySQL, etc.)

**Recommended Validations:**
1. **DatabaseReferentialIntegrityCheck** - Foreign key integrity
2. **DatabaseConstraintCheck** - Constraint enforcement
3. **MandatoryFieldCheck** - NOT NULL equivalent
4. **UniqueKeyCheck** - Uniqueness validation
5. **RegexCheck** - Pattern validation
6. **RangeCheck** - Value bounds
7. **SQLCustomCheck** - Complex business rules
8. **RowCountRangeCheck** - Data volume checks

---

## Performance Considerations

### For Large Files

**Fastest validations:**
- SchemaMatchCheck (reads 1 row only)
- ColumnPresenceCheck (reads 1 row only)
- RowCountRangeCheck (counts without reading data)

**Slowest validations:**
- UniqueKeyCheck (must process all rows)
- DuplicateRowCheck (must process all rows)
- StatisticalOutlierCheck (requires multiple passes)

**Tip:** Use Parquet format for files >100 MB (10x faster than CSV)

### For Large Database Tables

**Always use:**
- `max_rows` safety limit
- Filtered queries with WHERE clauses
- Indexes on filtered/validated columns
- Read replicas for production

**Example:**
```yaml
connection_string: "postgresql://readonly@read-replica:5432/db"
query: |
  SELECT * FROM large_table
  WHERE created_date >= CURRENT_DATE - 7
max_rows: 100000
```

---

## Combining Validations

You can apply **multiple validations** to the same source:

```yaml
files:
  - name: customer_data
    path: "data/customers.csv"
    format: csv

    validations:
      - type: SchemaMatchCheck
        params:
          expected_schema:
            customer_id: "integer"
            email: "string"
            status: "string"
        severity: ERROR

      - type: MandatoryFieldCheck
        params:
          fields: [customer_id, email]
        severity: ERROR

      - type: UniqueKeyCheck
        params:
          key_fields: [customer_id]
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
```

---

## Next Steps

- **Files:** [File Quick Start](./FILE_QUICKSTART.md)
- **Databases:** [Database Quick Start](./DATABASE_QUICKSTART.md)
- **Complete Catalog:** [Validation Catalog](./VALIDATION_CATALOG.md)
- **Production Safety:** [Database Safety Guide](./DATABASE_SAFETY.md)
