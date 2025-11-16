# DataK9 Best Practices

**A practical guide to implementing effective data quality validations** 🐕

This guide provides battle-tested advice for implementing data validations in production environments. Learn when to use ERROR vs WARNING, what minimum checks every dataset needs, and how to handle failures pragmatically. Let DataK9 guard your data the right way!

---

## Table of Contents

1. [ERROR vs WARNING: Making the Right Choice](#error-vs-warning-making-the-right-choice)
2. [Essential Validations Every Dataset Needs](#essential-validations-every-dataset-needs)
3. [Validation Ordering and Performance](#validation-ordering-and-performance)
4. [Handling Failures in Batch Jobs](#handling-failures-in-batch-jobs)
5. [Managing Warnings and Trending Issues](#managing-warnings-and-trending-issues)
6. [Configuration Patterns](#configuration-patterns)
7. [Production Deployment Checklist](#production-deployment-checklist)

---

## ERROR vs WARNING: Making the Right Choice

### The Golden Rule

> **ERROR**: Data is fundamentally broken and cannot be safely processed
>
> **WARNING**: Data has quality issues but processing can continue

Think of DataK9 like a security K9 unit:
- **ERROR**: The K9 detects a real threat - stop immediately!
- **WARNING**: The K9 senses something unusual - investigate, but continue with caution

### When to Use ERROR

Use `severity: ERROR` when:

1. **Critical Business Keys Are Invalid**
   - Missing customer IDs in transactions
   - Duplicate primary keys
   - Invalid foreign key references

   ```yaml
   - type: "ReferentialIntegrityCheck"
     severity: "ERROR"  # Orphaned records are unacceptable
     params:
       foreign_key: "customer_id"
       reference_file: "customers.csv"
       reference_key: "id"
   ```

2. **Data Cannot Be Processed**
   - Empty files (no data to process)
   - Missing required columns
   - Completely invalid formats

   ```yaml
   - type: "EmptyFileCheck"
     severity: "ERROR"  # Can't process an empty file
     params:
       check_data_rows: true
   ```

3. **Regulatory/Compliance Requirements**
   - Missing mandatory fields for regulations
   - Data that violates legal requirements
   - Failed encryption or security checks

4. **Financial Data Integrity**
   - Negative amounts where impossible
   - Totals don't balance
   - Missing transaction amounts

   ```yaml
   - type: "CrossFileComparisonCheck"
     severity: "ERROR"  # Financial reconciliation must match
     params:
       aggregation: "sum"
       column: "amount"
       comparison: "=="
       reference_file: "transactions.csv"
       reference_aggregation: "sum"
       reference_column: "transaction_amount"
       tolerance_pct: 0.01  # Allow only 0.01% variance
   ```

### When to Use WARNING

Use `severity: WARNING` when:

1. **Data Quality Issues Don't Block Processing**
   - Incomplete address information
   - Missing optional fields
   - Formatting inconsistencies

   ```yaml
   - type: "MandatoryFieldCheck"
     severity: "WARNING"  # Nice to have, not critical
     params:
       fields: ["phone_number", "middle_name"]
   ```

2. **Unusual Patterns That Need Investigation**
   - Unexpected data volume changes
   - Statistical anomalies
   - Unusual distributions

   ```yaml
   - type: "BaselineComparisonCheck"
     severity: "WARNING"  # Alert, but don't stop processing
     params:
       metric: "count"
       baseline_file: "historical_counts.csv"
       lookback_days: 30
       tolerance_pct: 20
   ```

3. **Business Rules with Exceptions**
   - Age restrictions with valid edge cases
   - Category validations with evolving lists
   - Optional enrichment data

4. **Data Freshness Concerns**
   - Delayed data feeds
   - Outdated reference data

   ```yaml
   - type: "FreshnessCheck"
     severity: "WARNING"  # Old data is better than no data
     params:
       date_field: "last_updated"
       max_age_days: 7
   ```

### Common Mistakes

❌ **TOO STRICT**: Making everything ERROR
- Result: Jobs fail constantly, teams ignore validation
- Fix: Reserve ERROR for truly critical issues

❌ **TOO LENIENT**: Making everything WARNING
- Result: Data quality degrades, no one acts on warnings
- Fix: ERROR for data that breaks downstream processes

✅ **BALANCED APPROACH**:
- Start with ERROR for critical path validations
- Add WARNINGs gradually as you understand your data
- Review WARNING trends monthly

---

## Essential Validations Every Dataset Needs

### The Minimum Viable Validation Set

**Every dataset should have these 5 core validations:**

1. **File Is Not Empty**
   ```yaml
   - type: "EmptyFileCheck"
     severity: "ERROR"
     params:
       check_data_rows: true
   ```

2. **Required Columns Exist**
   ```yaml
   - type: "ColumnPresenceCheck"
     severity: "ERROR"
     params:
       columns: ["id", "created_date", "amount"]
   ```

3. **Primary Key Is Unique**
   ```yaml
   - type: "UniqueKeyCheck"
     severity: "ERROR"
     params:
       key_columns: ["id"]
   ```

4. **Required Fields Are Not Null**
   ```yaml
   - type: "MandatoryFieldCheck"
     severity: "ERROR"
     params:
       fields: ["id", "customer_id", "transaction_date"]
   ```

5. **Row Count Is Reasonable**
   ```yaml
   - type: "RowCountRangeCheck"
     severity: "WARNING"
     params:
       min_rows: 1  # At least some data
       max_rows: 10000000  # Catch runaway files
   ```

**🐕 DataK9 Tip:** These 5 validations catch 80% of common data quality issues!

### Industry-Specific Requirements

#### Financial Data
Add these critical validations:
- Referential integrity for account/transaction relationships
- Amount range checks (no negative amounts where invalid)
- Balance reconciliation
- Date range validation

#### Customer/PII Data
Add these validations:
- Email format validation
- Required consent/privacy flags
- Data retention date checks
- PII presence validation

#### Event/Log Data
Add these validations:
- Timestamp is recent (freshness)
- Event types are valid
- Sequence numbers are monotonic

---

## Validation Ordering and Performance

### Optimal Validation Order

**Run validations in this order for best performance:**

1. **File-Level Checks** (Fastest)
   - EmptyFileCheck
   - FileSizeCheck
   - RowCountRangeCheck

2. **Schema Checks** (Fast)
   - ColumnPresenceCheck
   - SchemaMatchCheck

3. **Field-Level Checks** (Medium)
   - MandatoryFieldCheck
   - DataTypeCheck
   - RangeCheck
   - RegexCheck

4. **Record-Level Checks** (Slower)
   - UniqueKeyCheck
   - DuplicateRowCheck

5. **Cross-File Checks** (Slowest)
   - ReferentialIntegrityCheck
   - CrossFileComparisonCheck

6. **Statistical Checks** (Slowest)
   - DistributionCheck
   - CorrelationCheck

**Rationale**: Fail fast on simple checks before spending time on complex validations. Like a K9 unit checking the perimeter before searching inside! 🐕

### Performance Optimization Tips

1. **Use Appropriate Chunk Sizes**
   ```yaml
   settings:
     chunk_size: 10000  # Smaller for complex validations
     # chunk_size: 100000  # Larger for simple validations
   ```

2. **Limit Sample Failures**
   - Don't collect every single failure
   - 10-100 samples is usually sufficient for diagnosis

3. **Use Parquet Format**
   - **10-100x faster than CSV**
   - Built-in compression
   - Columnar storage for field-level checks

4. **Skip Optional Validations in Development**
   - Use conditional execution
   - Skip statistical checks in non-production

---

## Handling Failures in Batch Jobs

### AutoSys/Batch Job Integration

**Pattern 1: Fail on ERROR, Continue on WARNING**

```bash
#!/bin/bash
# validation_gate.sh

# Run DataK9 validation
python3 -m validation_framework.cli validate config.yaml --json results.json

# Check exit code
if [ $? -ne 0 ]; then
  echo "🐕 CRITICAL: DataK9 detected errors in data quality"
  # Parse results.json to send alerts
  python3 /scripts/send_alert.py results.json
  exit 1  # Fail AutoSys job
fi

# Check for warnings
WARNING_COUNT=$(jq '.summary.total_warnings' results.json)
if [ "$WARNING_COUNT" -gt 0 ]; then
  echo "🐕 WARNING: DataK9 found $WARNING_COUNT quality warnings"
  python3 /scripts/send_warning_alert.py results.json
  # Continue processing despite warnings
fi

# Proceed with data load
./load_data.sh
exit 0
```

**Pattern 2: Alert and Continue (Non-Critical Feeds)**

```bash
#!/bin/bash
# validation_best_effort.sh

python3 -m validation_framework.cli validate config.yaml --json results.json

# Always continue, just alert on issues
if [ $? -ne 0 ]; then
  python3 /scripts/send_alert.py results.json
  echo "Validation failed, but continuing with data load"
fi

./load_data.sh
exit 0
```

**Pattern 3: Validation with Retry Logic**

```bash
#!/bin/bash
# validation_with_retry.sh

MAX_RETRIES=3
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  python3 -m validation_framework.cli validate config.yaml --json results.json

  if [ $? -eq 0 ]; then
    echo "✅ DataK9 approved! Validation passed"
    ./load_data.sh
    exit 0
  fi

  RETRY_COUNT=$((RETRY_COUNT + 1))
  echo "🐕 DataK9 detected issues, attempt $RETRY_COUNT of $MAX_RETRIES"

  if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
    echo "Waiting 5 minutes before retry..."
    sleep 300
  fi
done

echo "Validation failed after $MAX_RETRIES attempts"
exit 1
```

### AutoSys JIL Examples

**Standard Validation Gate:**

```
insert_job: VALIDATE_CUSTOMER_DATA
job_type: c
command: /apps/validation/validate_customers.sh
condition: success(EXTRACT_CUSTOMER_DATA)
alarm_if_fail: yes
max_run_alarm: 30

insert_job: LOAD_CUSTOMER_DATA
job_type: c
command: /apps/etl/load_customers.sh
condition: success(VALIDATE_CUSTOMER_DATA)
```

**Validation with Warning Monitoring:**

```
insert_job: VALIDATE_TRANSACTIONS
job_type: c
command: /apps/validation/validate_with_warnings.sh
condition: success(EXTRACT_TRANSACTIONS)
std_out_file: /logs/validation_$AUTO_JOB_NAME_$AUTORUN.out

insert_job: CHECK_VALIDATION_WARNINGS
job_type: c
command: /apps/monitoring/check_warnings.sh /logs/validation_VALIDATE_TRANSACTIONS_$AUTORUN.out
condition: done(VALIDATE_TRANSACTIONS)
```

**See:** [AutoSys Integration Guide](autosys-integration.md) for more details.

---

## Managing Warnings and Trending Issues

### The WARNING Problem

**Challenge**: Warnings get ignored if no one acts on them.

**Solution**: Implement a WARNING Review Process

### Weekly WARNING Review Process

1. **Generate WARNING Summary Report**
   ```bash
   # Run validation and extract warnings
   python3 -m validation_framework.cli validate config.yaml --json results.json
   jq '.validations[] | select(.severity == "WARNING" and .passed == false)' results.json > warnings.json
   ```

2. **Track WARNING Trends**
   - Count warnings over time
   - Identify increasing trends
   - Flag new warning types

3. **Triage Process**
   - **Ignore**: False positive, update validation
   - **Fix Data**: Data source issue, work with provider
   - **Promote to ERROR**: Happening consistently, now critical

### Trending Dashboard Example

Create a simple dashboard that tracks:
- WARNING count by validation type (weekly)
- Top 10 most frequent warnings
- New warnings this week
- Warnings that became errors

### When to Promote WARNING → ERROR

Promote a WARNING to ERROR when:

1. **Consistently Failing** (>80% of runs)
   - No longer an exception, it's the norm
   - Data quality has degraded permanently

2. **Downstream Impact Identified**
   - Reports are now incorrect
   - Business decisions affected
   - Customer complaints received

3. **Regulatory Changes**
   - New compliance requirements
   - Legal obligations

4. **Pattern Becomes Predictable**
   - Can now write precise validation rules
   - Edge cases understood

---

## Configuration Patterns

### Pattern 1: Layered Validations (Recommended)

**Structure your validations in priority order:**

```yaml
validation_job:
  name: "Customer Data Validation"

files:
  - name: "customers"
    path: "customers.csv"
    validations:
      # Layer 1: Critical Structural Checks (ERROR)
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      - type: "ColumnPresenceCheck"
        severity: "ERROR"
        params:
          columns: ["customer_id", "email", "created_date"]

      # Layer 2: Data Integrity Checks (ERROR)
      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_columns: ["customer_id"]

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      # Layer 3: Business Rules (ERROR)
      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

      # Layer 4: Data Quality Checks (WARNING)
      - type: "MandatoryFieldCheck"
        severity: "WARNING"
        params:
          fields: ["phone_number", "address"]

      - type: "BaselineComparisonCheck"
        severity: "WARNING"
        params:
          metric: "count"
          baseline_file: "historical_counts.csv"
          lookback_days: 30
          tolerance_pct: 20

      # Layer 5: Statistical Analysis (WARNING)
      - type: "DistributionCheck"
        severity: "WARNING"
        params:
          column: "age"
          expected_distribution: "normal"
```

### Pattern 2: Environment-Specific Configs

**Maintain separate configs for each environment:**

```yaml
# config_dev.yaml - Lenient for development
validations:
  - type: "RowCountRangeCheck"
    severity: "WARNING"  # Just warn in dev
    params:
      min_rows: 1

# config_prod.yaml - Strict for production
validations:
  - type: "RowCountRangeCheck"
    severity: "ERROR"  # Must have realistic data
    params:
      min_rows: 1000
      max_rows: 10000000
```

### Pattern 3: Conditional Validations for Different Data Types

```yaml
validations:
  # Apply different rules based on account type
  - type: "ConditionalValidation"
    severity: "ERROR"
    params:
      condition: "account_type == 'BUSINESS'"
      then_validate:
        - type: "MandatoryFieldCheck"
          params:
            fields: ["company_name", "tax_id"]
      else_validate:
        - type: "MandatoryFieldCheck"
          params:
            fields: ["first_name", "last_name"]
```

---

## Production Deployment Checklist

### Before Deploying to Production

- [ ] **Run DataK9 Profiler** on production-like data
  ```bash
  python3 -m validation_framework.cli profile prod_sample.csv --format html --output analysis.html
  ```

- [ ] **Test with historical data** (last 30 days)
  - Ensure validations don't fail on known-good data
  - Identify false positives

- [ ] **Set appropriate thresholds**
  - Row count ranges based on actuals
  - Baseline tolerances from historical analysis
  - Date ranges that make sense

- [ ] **Implement monitoring**
  - Alert destination configured
  - Log aggregation set up
  - Dashboard for trends

- [ ] **Document escalation process**
  - Who to contact for validation failures?
  - What's the SLA for fixing data issues?
  - When to bypass validation (emergency process)?

- [ ] **Test failure scenarios**
  - Empty file
  - Missing columns
  - Corrupt data
  - Unexpected data types

- [ ] **Performance test**
  - Run on largest expected file
  - Verify completes within batch window
  - Check memory usage

### Post-Deployment

- [ ] **Monitor first week closely**
  - Daily review of validation results
  - Quick response to false positives

- [ ] **Tune thresholds**
  - Adjust based on actual data patterns
  - Update baseline files

- [ ] **Establish review cadence**
  - Weekly WARNING review
  - Monthly validation effectiveness review
  - Quarterly comprehensive audit

---

## Real-World Examples

### Example 1: E-Commerce Orders

```yaml
validation_job:
  name: "Daily Orders Validation"

settings:
  chunk_size: 10000

files:
  - name: "orders"
    path: "orders_{date}.csv"
    format: "csv"
    validations:
      # Critical: Can't process without these
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_columns: ["order_id"]

      - type: "ReferentialIntegrityCheck"
        severity: "ERROR"
        params:
          foreign_key: "customer_id"
          reference_file: "customers.csv"
          reference_key: "id"

      # Business rules: Required for accounting
      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["order_id", "customer_id", "total_amount", "order_date"]

      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "total_amount"
          min_value: 0  # Can't have negative orders

      # Quality checks: Nice to have
      - type: "MandatoryFieldCheck"
        severity: "WARNING"
        params:
          fields: ["shipping_address", "phone_number"]

      # Anomaly detection: Monitor trends
      - type: "BaselineComparisonCheck"
        severity: "WARNING"
        params:
          metric: "sum"
          column: "total_amount"
          baseline_file: "daily_revenue.csv"
          lookback_days: 30
          tolerance_pct: 30  # 30% variance is concerning

      - type: "TrendDetectionCheck"
        severity: "WARNING"
        params:
          metric: "count"
          baseline_file: "daily_order_counts.csv"
          max_decline_pct: 50  # Alert if orders drop >50%
          comparison_period: 1
```

### Example 2: Financial Transactions

```yaml
validation_job:
  name: "Transaction Validation"

files:
  - name: "transactions"
    path: "transactions.parquet"
    format: "parquet"
    validations:
      # Zero tolerance for errors in financial data
      - type: "EmptyFileCheck"
        severity: "ERROR"
        params:
          check_data_rows: true

      - type: "UniqueKeyCheck"
        severity: "ERROR"
        params:
          key_columns: ["transaction_id"]

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["transaction_id", "account_id", "amount", "transaction_date", "transaction_type"]

      # Financial integrity
      - type: "RangeCheck"
        severity: "ERROR"
        params:
          field: "amount"
          min_value: 0.01  # Must have positive amount

      - type: "ValidValuesCheck"
        severity: "ERROR"
        params:
          field: "transaction_type"
          allowed_values: ["DEBIT", "CREDIT", "TRANSFER", "FEE"]

      # Reconciliation
      - type: "CrossFileComparisonCheck"
        severity: "ERROR"
        params:
          aggregation: "sum"
          column: "amount"
          comparison: "=="
          reference_file: "account_balances.csv"
          reference_aggregation: "sum"
          reference_column: "balance_change"
          tolerance_pct: 0.001  # 0.001% tolerance for rounding

      # Audit trail
      - type: "FreshnessCheck"
        severity: "WARNING"
        params:
          date_field: "transaction_date"
          max_age_days: 1  # Transactions should be recent
```

---

## Summary: Key Takeaways

1. **ERROR = Breaks Processing**: Reserve for truly critical issues
2. **WARNING = Needs Investigation**: For quality issues that don't block processing
3. **Minimum 5 Validations**: Empty, Schema, Unique, Required, RowCount
4. **Fail Fast**: Run simple checks before expensive ones
5. **Review Warnings Weekly**: Don't let them accumulate
6. **Test Before Production**: Use DataK9 Profiler and historical data
7. **Monitor and Tune**: Validations need ongoing maintenance

**🐕 DataK9 Tip:** Like training a K9 unit, data validations need regular care and adjustment to stay effective!

---

## Next Steps

- **[Validation Catalog](validation-catalog.md)** - Complete list of 35+ validations
- **[Configuration Guide](configuration-guide.md)** - Complete YAML reference
- **[Examples](../examples/)** - More real-world scenarios
- **[DataK9 Studio Guide](studio-guide.md)** - Visual configuration builder

---

**🐕 Guard your data with DataK9 - your vigilant K9 for data quality!**
