# AutoSys Integration Guide

**Integrate DataK9 with Enterprise Job Schedulers**

DataK9 integrates seamlessly with AutoSys, Control-M, and other enterprise job schedulers. This guide shows you how to deploy DataK9 as a data quality gate in your batch processing pipelines.

---

## Table of Contents

1. [Overview](#overview)
2. [Exit Codes](#exit-codes)
3. [AutoSys JIL Examples](#autosys-jil-examples)
4. [Validation Gates](#validation-gates)
5. [Error Handling](#error-handling)
6. [Alerting and Notifications](#alerting-and-notifications)
7. [Production Patterns](#production-patterns)
8. [Monitoring](#monitoring)
9. [Troubleshooting](#troubleshooting)

---

## Overview

### Integration Architecture

```
AutoSys Workflow:
┌──────────────────────────────────────────────┐
│  1. Extract Job (Box: DATA_EXTRACT)          │
│     └─ Downloads data from source system     │
├──────────────────────────────────────────────┤
│  2. DataK9 Validation (Box: DATA_QUALITY)    │
│     └─ Validates data before processing      │
│     Exit 0: Data OK → Continue               │
│     Exit 1: Data Bad → Stop, Alert           │
├──────────────────────────────────────────────┤
│  3. Transform Job (Box: DATA_TRANSFORM)      │
│     └─ Only runs if validation passed        │
├──────────────────────────────────────────────┤
│  4. Load Job (Box: DATA_LOAD)                │
│     └─ Loads validated data to warehouse     │
└──────────────────────────────────────────────┘
```

### Why DataK9 + AutoSys?

**Benefits:**

✅ **Data Quality Gate** - Prevent bad data from entering warehouse
✅ **Fail Fast** - Stop pipeline at first quality issue
✅ **Automated Validation** - No manual checks required
✅ **Audit Trail** - Complete validation history
✅ **Standard Integration** - Uses AutoSys exit codes
✅ **Well-Tested** - Reliable integration patterns

---

## Exit Codes

### DataK9 Exit Codes

DataK9 follows Unix/AutoSys conventions with comprehensive exit codes for batch processing:

| Exit Code | Meaning | AutoSys Status | Action |
|-----------|---------|----------------|--------|
| **0** | SUCCESS - All validations passed | SUCCESS | Continue pipeline |
| **1** | FAILURE - ERROR validations failed | FAILURE | Stop pipeline, alert |
| **2** | WARNING AS FAILURE - Warnings with `--fail-on-warning` | FAILURE | Stop pipeline, review |
| **3** | TIMEOUT - Job exceeded `--timeout` limit | FAILURE | Stop, investigate slow job |
| **4** | LOCK CONFLICT - Another instance running (`--lock-file`) | FAILURE | Stop, check for stuck job |
| **5** | ENVIRONMENT ERROR - Missing dependencies or wrong Python version | FAILURE | Stop, fix environment |
| **130** | INTERRUPTED - SIGINT (Ctrl+C) or SIGTERM received | FAILURE | Graceful shutdown |
| **137** | MEMORY - Out of memory error | FAILURE | Stop, increase resources |

### Exit Code Behavior

**Exit 0 (SUCCESS):**
```bash
python3 -m validation_framework.cli validate config.yaml
echo $?
# 0

# AutoSys sees: SUCCESS
# Downstream jobs: Run
```

**Exit 1 (VALIDATION FAILURE):**
```bash
python3 -m validation_framework.cli validate config.yaml
echo $?
# 1

# AutoSys sees: FAILURE
# Downstream jobs: Don't run
# Alert: Triggered
```

**Exit 2 (WARNING AS FAILURE):**
```bash
python3 -m validation_framework.cli validate config.yaml --fail-on-warning
echo $?
# 2

# AutoSys sees: FAILURE (warnings treated as failure)
# Downstream jobs: Don't run
# Alert: Triggered
```

**Exit 5 (ENVIRONMENT ERROR):**
```bash
# If dependencies are missing or wrong Python version
python3 -m validation_framework.cli validate config.yaml
# ERROR: DataK9 Environment Check Failed
# Missing Dependencies: pandas, numpy
echo $?
# 5

# AutoSys sees: FAILURE
# Downstream jobs: Don't run
# Action: Fix Python environment, install dependencies
```

### Controlling Exit Codes

**In Configuration:**

```yaml
validation_job:
  name: "Customer Data Validation"

settings:
  # Exit code 1 if any ERROR-severity validation fails
  fail_on_error: true  # Default: true

  # Exit code 1 if any WARNING-severity validation fails
  fail_on_warning: false  # Default: false
```

**Behavior Examples:**

```yaml
# Scenario 1: Strict (default)
settings:
  fail_on_error: true
  fail_on_warning: false

# Result:
# - ERROR failures → Exit 1 (fail)
# - WARNING failures → Exit 0 (pass)

# Scenario 2: Very Strict
settings:
  fail_on_error: true
  fail_on_warning: true

# Result:
# - ERROR failures → Exit 1 (fail)
# - WARNING failures → Exit 1 (fail)

# Scenario 3: Lenient (not recommended)
settings:
  fail_on_error: false
  fail_on_warning: false

# Result:
# - ERROR failures → Exit 0 (pass!)
# - WARNING failures → Exit 0 (pass)
# Only use for reporting-only validations
```

---

## Platform Support

DataK9 runs on Linux, macOS, and Windows, but some batch processing features have platform-specific behavior:

| Feature | Linux/macOS | Windows |
|---------|-------------|---------|
| Exit codes (0-5, 130, 137) | ✅ Full support | ✅ Full support |
| `--timeout` | ✅ Full support (SIGALRM) | ⚠️ Limited (threading-based) |
| `--lock-file` | ✅ Full support (fcntl) | ❌ Not supported |
| `--exit-file` | ✅ Full support | ✅ Full support |
| SIGTERM handling | ✅ Full support | ⚠️ Limited |
| Ctrl+C / SIGINT | ✅ Full support | ✅ Full support |

**Recommendations for Windows:**
- Use Windows Task Scheduler's built-in job isolation instead of `--lock-file`
- Use Windows Task Scheduler's timeout settings instead of `--timeout`
- `--exit-file` works fully and is recommended for automation
- All exit codes work as documented

**Example: Windows-Friendly Command**
```powershell
# Windows batch command (no lock file or timeout)
python -m validation_framework.cli validate config.yaml --exit-file C:\logs\validation.exit -q
```

---

## Batch Processing Options

### Built-in Defensive Features

DataK9 provides several CLI options specifically designed for batch/headless environments:

| Option | Description | Example |
|--------|-------------|---------|
| `--timeout SECONDS` | Kill job if exceeds time limit (exit code 3) | `--timeout 3600` |
| `--lock-file PATH` | Prevent concurrent runs (exit code 4 if locked) | `--lock-file /tmp/job.lock` |
| `--exit-file PATH` | Write exit code to file for automation | `--exit-file /tmp/job.exit` |
| `--log-file PATH` | Write detailed logs to file | `--log-file /logs/job.log` |
| `--log-level LEVEL` | Set logging verbosity (DEBUG/INFO/WARNING/ERROR) | `--log-level INFO` |
| `-q, --quiet` | Minimal output for headless operation | `-q` |

### Recommended Batch Command

```bash
# Full defensive batch command
data-validate validate config.yaml \
  --timeout 3600 \
  --lock-file /tmp/validation_job.lock \
  --exit-file /tmp/validation_job.exit \
  --log-file /logs/validation_{timestamp}.log \
  --log-level INFO \
  -q
```

### Timeout Protection

Prevent runaway jobs from blocking your pipeline:

```bash
# Kill job if it runs longer than 1 hour
data-validate validate config.yaml --timeout 3600

# Check exit code
if [ $? -eq 3 ]; then
    echo "Job timed out after 1 hour"
fi
```

**AutoSys with Timeout:**

```jil
insert_job: VALIDATE_CUSTOMERS
job_type: CMD
command: data-validate validate /config/customers.yaml --timeout 1800 -q
max_run_alarm: 35  # Set slightly higher than timeout
alarm_if_fail: yes
```

### Lock File Protection

Prevent multiple instances from running simultaneously:

```bash
# Use lock file to prevent concurrent runs
data-validate validate config.yaml --lock-file /tmp/customers_validation.lock
```

**How it works:**
1. DataK9 attempts to acquire exclusive lock on the file
2. If lock acquired: runs validation, releases lock on completion
3. If lock busy: exits immediately with code 4

**AutoSys with Lock File:**

```jil
insert_job: VALIDATE_CUSTOMERS
job_type: CMD
command: data-validate validate /config/customers.yaml --lock-file /tmp/cust_val.lock -q
/* If previous run is stuck, this will exit with code 4 */
alarm_if_fail: yes
```

### Exit File for Automation

Write exit code to a file for downstream automation:

```bash
# Write exit code to file
data-validate validate config.yaml --exit-file /tmp/validation_result.exit

# Read result in downstream job
EXIT_CODE=$(cat /tmp/validation_result.exit)
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "Validation passed"
fi
```

**Use Cases:**
- Control-M event triggers based on file content
- Custom monitoring scripts
- Integration with non-shell schedulers

### Complete Defensive Example

```bash
#!/bin/bash
#===============================================================================
# Defensive Validation Wrapper for AutoSys
# Uses all batch safety features
#===============================================================================

JOB_NAME="customers_daily"
CONFIG="/config/${JOB_NAME}.yaml"
LOG_DIR="/logs/validation"
LOCK_FILE="/tmp/${JOB_NAME}.lock"
EXIT_FILE="/tmp/${JOB_NAME}.exit"
TIMEOUT=1800  # 30 minutes

# Create log directory
mkdir -p "$LOG_DIR"

# Run with all defensive options
data-validate validate "$CONFIG" \
    --timeout $TIMEOUT \
    --lock-file "$LOCK_FILE" \
    --exit-file "$EXIT_FILE" \
    --log-file "$LOG_DIR/${JOB_NAME}_{timestamp}.log" \
    --log-level INFO \
    -q

EXIT_CODE=$?

# Handle exit codes
case $EXIT_CODE in
    0)
        echo "SUCCESS: Validation passed"
        ;;
    1)
        echo "FAILED: Data quality issues detected"
        /scripts/alert_data_team.sh "$JOB_NAME"
        ;;
    2)
        echo "FAILED: Warnings treated as failure"
        /scripts/alert_data_team.sh "$JOB_NAME"
        ;;
    3)
        echo "TIMEOUT: Job exceeded ${TIMEOUT}s limit"
        /scripts/alert_ops_team.sh "$JOB_NAME" "timeout"
        ;;
    4)
        echo "LOCKED: Another instance is running"
        /scripts/alert_ops_team.sh "$JOB_NAME" "locked"
        ;;
    5)
        echo "ENVIRONMENT: Missing dependencies or wrong Python version"
        /scripts/alert_ops_team.sh "$JOB_NAME" "environment"
        ;;
    130)
        echo "INTERRUPTED: Job was killed"
        /scripts/alert_ops_team.sh "$JOB_NAME" "killed"
        ;;
    137)
        echo "MEMORY: Out of memory error"
        /scripts/alert_ops_team.sh "$JOB_NAME" "oom"
        ;;
    *)
        echo "UNKNOWN: Exit code $EXIT_CODE"
        /scripts/alert_ops_team.sh "$JOB_NAME" "unknown"
        ;;
esac

exit $EXIT_CODE
```

---

## AutoSys JIL Examples

### Basic Validation Job

**Single File Validation:**

```jil
/* DataK9 Validation Job */
insert_job: VALIDATE_CUSTOMERS
job_type: CMD
command: python3 -m validation_framework.cli validate /data/configs/customers.yaml
machine: prod_server_01
owner: dataops
permission: gx,wx
date_conditions: yes
days_of_week: mo,tu,we,th,fr
start_times: "06:00"
std_out_file: /logs/autosys/validate_customers.$(AUTOSERV).out
std_err_file: /logs/autosys/validate_customers.$(AUTOSERV).err
alarm_if_fail: yes
max_run_alarm: 30  # Minutes
```

**Key Fields Explained:**

- `command` - Full path to DataK9 CLI
- `std_out_file` - Capture standard output
- `std_err_file` - Capture error messages
- `alarm_if_fail: yes` - Alert on failure
- `max_run_alarm: 30` - Alert if runs >30 min

### Validation Gate Pattern

**Complete Data Pipeline:**

```jil
/* ============================================
   BOX: Customer Data Pipeline
   Description: Daily customer data processing
   ============================================ */

insert_job: CUST_PIPELINE_BOX
job_type: BOX
owner: dataops

/* --------------------------------------------
   JOB 1: Extract Data
   -------------------------------------------- */
insert_job: CUST_EXTRACT
job_type: CMD
box_name: CUST_PIPELINE_BOX
command: /scripts/extract_customers.sh
machine: prod_server_01
owner: dataops
date_conditions: yes
start_times: "02:00"
std_out_file: /logs/autosys/cust_extract.$(AUTOSERV).out
std_err_file: /logs/autosys/cust_extract.$(AUTOSERV).err

/* --------------------------------------------
   JOB 2: DataK9 Validation (GATE)
   -------------------------------------------- */
insert_job: CUST_VALIDATE
job_type: CMD
box_name: CUST_PIPELINE_BOX
command: /scripts/validate_wrapper.sh
machine: prod_server_01
owner: dataops
condition: SUCCESS(CUST_EXTRACT)  # Only run if extract succeeds
std_out_file: /logs/autosys/cust_validate.$(AUTOSERV).out
std_err_file: /logs/autosys/cust_validate.$(AUTOSERV).err
alarm_if_fail: yes  # Alert data quality team
notification_msg: "Customer data validation FAILED - check /logs/autosys/cust_validate.*.err"
max_run_alarm: 15

/* --------------------------------------------
   JOB 3: Transform Data
   -------------------------------------------- */
insert_job: CUST_TRANSFORM
job_type: CMD
box_name: CUST_PIPELINE_BOX
command: /scripts/transform_customers.sh
machine: prod_server_01
owner: dataops
condition: SUCCESS(CUST_VALIDATE)  # Only run if validation passes
std_out_file: /logs/autosys/cust_transform.$(AUTOSERV).out
std_err_file: /logs/autosys/cust_transform.$(AUTOSERV).err

/* --------------------------------------------
   JOB 4: Load to Warehouse
   -------------------------------------------- */
insert_job: CUST_LOAD
job_type: CMD
box_name: CUST_PIPELINE_BOX
command: /scripts/load_customers.sh
machine: prod_server_01
owner: dataops
condition: SUCCESS(CUST_TRANSFORM)  # Only run if transform succeeds
std_out_file: /logs/autosys/cust_load.$(AUTOSERV).out
std_err_file: /logs/autosys/cust_load.$(AUTOSERV).err
```

### Wrapper Script

**validate_wrapper.sh:**

```bash
#!/bin/bash
#===============================================================================
# DataK9 Validation Wrapper for AutoSys
#
# Purpose: Validates data files and generates reports
# Exit:    0 = Validation passed
#          1 = Validation failed (data quality issues)
#          2 = Runtime error (config/system issues)
#===============================================================================

set -e  # Exit on error

# Configuration
CONFIG_FILE="/data/configs/customers_validation.yaml"
REPORT_DIR="/data/reports/$(date +%Y%m%d)"
ALERT_EMAIL="data-quality-team@company.com"

# Logging
LOG_FILE="/logs/validation/customers_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo "======================================"
echo "🐕 DataK9 Validation Started"
echo "Time: $(date)"
echo "Config: $CONFIG_FILE"
echo "======================================"

# Create report directory
mkdir -p "$REPORT_DIR"

# Activate virtual environment (if needed)
source /opt/datak9/venv/bin/activate

# Run DataK9 validation
echo "Running validation..."
python3 -m validation_framework.cli validate "$CONFIG_FILE"

EXIT_CODE=$?

echo "======================================"
echo "Validation Exit Code: $EXIT_CODE"
echo "======================================"

# Handle results based on exit code
case $EXIT_CODE in
    0)
        echo "✅ SUCCESS: All validations passed"
        echo "Report: $REPORT_DIR/validation_report.html"
        exit 0
        ;;
    1)
        echo "❌ FAILURE: Data quality issues detected"
        echo "Report: $REPORT_DIR/validation_report.html"

        # Send alert email
        /scripts/send_alert.sh \
            "$ALERT_EMAIL" \
            "DataK9 Validation Failed: Customers" \
            "Data quality issues found. Review report at $REPORT_DIR/validation_report.html"

        exit 1
        ;;
    2)
        echo "🚨 ERROR: Runtime/configuration error"

        # Send critical alert
        /scripts/send_alert.sh \
            "$ALERT_EMAIL" \
            "DataK9 Error: System Issue" \
            "Validation failed due to runtime error. Check logs at $LOG_FILE"

        exit 2
        ;;
    *)
        echo "⚠️ UNKNOWN: Unexpected exit code $EXIT_CODE"
        exit 2
        ;;
esac
```

---

## Validation Gates

### What is a Validation Gate?

A **validation gate** is a checkpoint in your pipeline that:

1. ✅ Validates data quality
2. 🛑 Stops pipeline if quality issues found
3. ✅ Allows pipeline to continue if data is good
4. 📊 Generates audit trail

### Gate Implementation

**Pattern:**

```
Extract → Validate (GATE) → Transform → Load
          ↓
        If fail:
        - Stop pipeline
        - Alert team
        - Log issue
```

**AutoSys Implementation:**

```jil
/* Transform only runs if validation succeeds */
insert_job: TRANSFORM_DATA
condition: SUCCESS(VALIDATE_DATA)
```

### Multi-File Gates

**Validate Multiple Files:**

```jil
/* Validate all files before any transformation */

insert_job: VALIDATE_CUSTOMERS
job_type: CMD
command: python3 -m validation_framework.cli validate customers.yaml

insert_job: VALIDATE_ORDERS
job_type: CMD
command: python3 -m validation_framework.cli validate orders.yaml

insert_job: VALIDATE_PRODUCTS
job_type: CMD
command: python3 -m validation_framework.cli validate products.yaml

/* Transform only if ALL validations pass */
insert_job: TRANSFORM_ALL
condition: SUCCESS(VALIDATE_CUSTOMERS) AND SUCCESS(VALIDATE_ORDERS) AND SUCCESS(VALIDATE_PRODUCTS)
```

### Conditional Gates

**Different Validation Based on Conditions:**

```bash
#!/bin/bash
# conditional_validate.sh

FILE_SIZE=$(stat -f%z "$DATA_FILE")

if [ $FILE_SIZE -gt 1073741824 ]; then
    # Large file (>1GB): Quick validation only
    python3 -m validation_framework.cli validate quick_checks.yaml
else
    # Small file: Full validation
    python3 -m validation_framework.cli validate full_checks.yaml
fi
```

---

## Error Handling

### Handling Validation Failures

**AutoSys On-Failure Actions:**

```jil
insert_job: VALIDATE_DATA
on_error: email_alert
notification_msg: "Data validation failed - see logs"
notification_email: data-team@company.com

/* Or trigger recovery job */
insert_job: VALIDATE_DATA
on_fail: recover_validation_job
```

**Recovery Job Example:**

```jil
/* Recovery: Re-extract data and retry validation */
insert_job: recover_validation_job
job_type: CMD
command: /scripts/reextract_and_validate.sh
```

### Handling Runtime Errors

**Distinguish Validation Failures from System Errors:**

```bash
#!/bin/bash
# smart_validate.sh

python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 1 ]; then
    # Validation failure (data quality issue)
    echo "DATA QUALITY ISSUE"
    /scripts/notify_data_team.sh
    exit 1
elif [ $EXIT_CODE -eq 2 ]; then
    # Runtime error (system issue)
    echo "SYSTEM ERROR"
    /scripts/notify_ops_team.sh
    exit 2
else
    # Success
    echo "VALIDATION PASSED"
    exit 0
fi
```

### Retry Logic

**Auto-Retry for Transient Failures:**

```bash
#!/bin/bash
# validate_with_retry.sh

MAX_RETRIES=3
RETRY_DELAY=300  # 5 minutes

for i in $(seq 1 $MAX_RETRIES); do
    echo "Attempt $i of $MAX_RETRIES"

    python3 -m validation_framework.cli validate config.yaml
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Validation passed"
        exit 0
    elif [ $EXIT_CODE -eq 2 ]; then
        # Runtime error - retry
        echo "Runtime error, retrying in $RETRY_DELAY seconds..."
        sleep $RETRY_DELAY
    else
        # Validation failure - don't retry
        echo "❌ Validation failed (data quality issue)"
        exit 1
    fi
done

echo "❌ Max retries exceeded"
exit 2
```

---

## Alerting and Notifications

### Email Alerts

**send_alert.sh:**

```bash
#!/bin/bash
# send_alert.sh - Send email alert

TO_EMAIL="$1"
SUBJECT="$2"
BODY="$3"
REPORT_HTML="$4"  # Optional attachment

# Send email
if [ -z "$REPORT_HTML" ]; then
    # Simple email
    echo "$BODY" | mail -s "$SUBJECT" "$TO_EMAIL"
else
    # Email with attachment
    echo "$BODY" | mail -s "$SUBJECT" -a "$REPORT_HTML" "$TO_EMAIL"
fi
```

**Usage in AutoSys:**

```jil
insert_job: VALIDATE_DATA
job_type: CMD
command: /scripts/validate_and_alert.sh
std_out_file: /logs/validate.out
std_err_file: /logs/validate.err
alarm_if_fail: yes
notification_email: data-quality@company.com
notification_msg: "DataK9 validation failed - check std_err_file"
```

### Slack Notifications

**notify_slack.sh:**

```bash
#!/bin/bash
# notify_slack.sh - Send Slack notification

WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
MESSAGE="$1"
STATUS="$2"  # success, warning, error

if [ "$STATUS" == "error" ]; then
    COLOR="danger"
    EMOJI=":x:"
elif [ "$STATUS" == "warning" ]; then
    COLOR="warning"
    EMOJI=":warning:"
else
    COLOR="good"
    EMOJI=":white_check_mark:"
fi

curl -X POST "$WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  -d '{
    "attachments": [{
      "color": "'"$COLOR"'",
      "title": "'"$EMOJI DataK9 Validation"'",
      "text": "'"$MESSAGE"'",
      "footer": "AutoSys Job Scheduler",
      "ts": '$(date +%s)'
    }]
  }'
```

**Integration:**

```bash
#!/bin/bash
# validate_with_slack.sh

python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    /scripts/notify_slack.sh "Customer validation passed" "success"
else
    /scripts/notify_slack.sh "Customer validation FAILED - investigate immediately" "error"
fi

exit $EXIT_CODE
```

---

## Production Patterns

### Pattern 1: Standard Quality Gate

```
Extract → DataK9 Validate → Transform → Load
              ↓ fail
            Alert & Stop
```

**When to Use:**
- Standard ETL pipelines
- Daily batch processing
- Critical data quality requirements

### Pattern 2: Parallel Validation

```
Extract
  ├─ Validate File A ──┐
  ├─ Validate File B ──┼─ All Success? → Transform → Load
  └─ Validate File C ──┘      ↓ Any Fail
                           Alert & Stop
```

**When to Use:**
- Multiple source files
- Independent file validation
- Parallel processing available

### Pattern 3: Staged Validation

```
Extract → Quick Checks → Transform → Full Validation → Load
              ↓ fail          ↓ fail
            Stop Early      Stop Before Load
```

**When to Use:**
- Very large files
- Expensive transformations
- Multiple validation levels

### Pattern 4: Validation with Quarantine

```
Extract → Validate ─┬─ Pass → Load to Prod
                    └─ Fail → Load to Quarantine
                              ↓
                           Alert for Review
```

**When to Use:**
- Can't afford to lose data
- Manual review possible
- Quarantine system available

**Implementation:**

```bash
#!/bin/bash
# validate_with_quarantine.sh

python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    # Load to production
    echo "Loading to production database..."
    /scripts/load_to_prod.sh "$DATA_FILE"
else
    # Load to quarantine
    echo "Loading to quarantine for review..."
    /scripts/load_to_quarantine.sh "$DATA_FILE"

    # Alert team
    /scripts/notify_team.sh "Data moved to quarantine - review required"

    # Exit success (don't stop pipeline)
    exit 0
fi
```

---

## Monitoring

### Key Metrics to Track

**1. Validation Success Rate**
```sql
SELECT
    DATE(execution_time) as date,
    COUNT(*) as total_validations,
    SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM validation_audit_log
GROUP BY DATE(execution_time)
ORDER BY date DESC;
```

**2. Execution Time**
```sql
SELECT
    job_name,
    AVG(duration_seconds) as avg_duration,
    MIN(duration_seconds) as min_duration,
    MAX(duration_seconds) as max_duration
FROM validation_audit_log
WHERE execution_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY job_name;
```

**3. Failure Patterns**
```sql
SELECT
    file_name,
    validation_type,
    COUNT(*) as failure_count
FROM validation_failures
WHERE execution_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY file_name, validation_type
ORDER BY failure_count DESC
LIMIT 10;
```

### AutoSys Monitoring

**Check Job Status:**
```bash
# List recent validation jobs
autorep -J VALIDATE_% -d

# Check specific job
autorep -J VALIDATE_CUSTOMERS -d

# Check box status
autorep -J CUST_PIPELINE_BOX -d
```

**Monitor Logs:**
```bash
# Tail validation log
tail -f /logs/autosys/validate_customers.*.out

# Search for failures
grep "FAILED" /logs/autosys/validate_*.err

# Count successes vs failures today
grep -c "SUCCESS" /logs/autosys/validate_*$(date +%Y%m%d)*.out
grep -c "FAILED" /logs/autosys/validate_*$(date +%Y%m%d)*.err
```

---

## Troubleshooting

### Common Issues

**Issue 1: Job Always Fails**

**Symptoms:**
```
AutoSys shows: FAILURE
But data looks good
```

**Diagnosis:**
```bash
# Run validation manually
python3 -m validation_framework.cli validate config.yaml
echo $?  # Check exit code

# Check config
cat config.yaml | grep fail_on
```

**Fix:**
```yaml
# If fail_on_warning is too strict
settings:
  fail_on_error: true
  fail_on_warning: false  # Change from true
```

**Issue 2: Job Times Out**

**Symptoms:**
```
AutoSys: MAX_RUN_ALARM triggered
Job killed before completion
```

**Diagnosis:**
```bash
# Check file size
du -sh /data/file.csv

# Estimate time needed
# Rule of thumb: 1GB ≈ 2 minutes with Parquet
```

**Fix:**
```jil
/* Increase max_run_alarm */
insert_job: VALIDATE_DATA
max_run_alarm: 120  # Increase to 2 hours
```

**Issue 3: Missing Reports**

**Symptoms:**
```
Validation runs but no HTML report generated
```

**Diagnosis:**
```bash
# Check config for output settings
grep -A 5 "output:" config.yaml

# Check directory permissions
ls -ld /data/reports/
```

**Fix:**
```yaml
# Ensure output configured
output:
  html_report: "/data/reports/validation_report_$(date +%Y%m%d).html"
  json_summary: "/data/reports/validation_summary.json"
```

---

## Next Steps

**You've learned AutoSys integration! Now:**

1. **[CI/CD Integration](cicd-integration.md)** - Jenkins, GitLab CI, GitHub Actions
2. **[Best Practices](best-practices.md)** - Production deployment guidance
3. **[Monitoring](monitoring.md)** - Track validation metrics
4. **[Troubleshooting](troubleshooting.md)** - Solve common issues

---

**🐕 DataK9 guards your enterprise pipelines - integrate with confidence**
