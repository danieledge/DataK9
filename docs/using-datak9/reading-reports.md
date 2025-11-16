# Reading Validation Reports

**Understanding What DataK9 Found**

After DataK9 guards your data, it generates detailed reports showing what it found. This guide helps you understand validation reports and take action on results.

---

## Table of Contents

1. [Report Types](#report-types)
2. [HTML Report Structure](#html-report-structure)
3. [Understanding Status Indicators](#understanding-status-indicators)
4. [Reading Summary Section](#reading-summary-section)
5. [Interpreting Validation Results](#interpreting-validation-results)
6. [Sample Failures](#sample-failures)
7. [JSON Reports](#json-reports)
8. [Taking Action](#taking-action)
9. [Common Patterns](#common-patterns)

---

## Report Types

DataK9 generates two types of reports:

### HTML Report

**Purpose:** Human-readable, visual report

**Features:**
- 🎨 Beautiful dark theme
- 📊 Interactive charts
- 📁 Expandable sections
- 🔍 Search and filter
- 📱 Mobile-responsive
- 🖨️ Print-friendly

**When to Use:**
- Manual review
- Stakeholder presentations
- Documentation
- Troubleshooting

**Example:**
```bash
python3 -m validation_framework.cli validate config.yaml

# Generates: validation_report.html
open validation_report.html
```

### JSON Report

**Purpose:** Machine-readable, programmatic access

**Features:**
- 🤖 Structured data
- 🔗 API-friendly
- 📊 Metric extraction
- 🔄 CI/CD integration
- 📈 Trend analysis

**When to Use:**
- Automated pipelines
- Monitoring systems
- Alerting
- Metrics dashboards
- Historical tracking

**Example:**
```bash
python3 -m validation_framework.cli validate config.yaml

# Generates: validation_summary.json
cat validation_summary.json | jq '.overall_status'
```

---

## HTML Report Structure

### Report Sections

DataK9's HTML report has 5 main sections:

```
┌────────────────────────────────────────┐
│  1. Header & Overall Status            │
├────────────────────────────────────────┤
│  2. Executive Summary                  │
├────────────────────────────────────────┤
│  3. File Reports (expandable)          │
│     ├─ File 1 Results                  │
│     │   ├─ Validation 1                │
│     │   ├─ Validation 2                │
│     │   └─ ...                          │
│     └─ File 2 Results                  │
├────────────────────────────────────────┤
│  4. Detailed Failures (if any)         │
├────────────────────────────────────────┤
│  5. Footer & Metadata                  │
└────────────────────────────────────────┘
```

---

## Understanding Status Indicators

### Overall Status

The top of the report shows overall status:

**✅ PASSED (Green)**
```
All validations passed
No ERROR-severity failures
May have WARNING-severity issues
```

**❌ FAILED (Red)**
```
At least one ERROR-severity validation failed
Critical quality issues detected
Data should not be processed
```

**⚠️ WARNING (Yellow)**
```
Only WARNING-severity validations failed
No ERROR failures
Quality issues to review
Safe to process with caution
```

### Status Badge

```html
┌─────────────────────────────────────┐
│  ✅ PASSED                          │
│  Customer Data Validation           │
│  2024-01-15 14:30:22               │
│  Duration: 12.5 seconds             │
└─────────────────────────────────────┘
```

Or if failed:

```html
┌─────────────────────────────────────┐
│  ❌ FAILED                          │
│  Customer Data Validation           │
│  Errors: 3   Warnings: 7            │
│  2024-01-15 14:30:22               │
│  Duration: 12.5 seconds             │
└─────────────────────────────────────┘
```

---

## Reading Summary Section

### Summary Cards

The executive summary shows high-level metrics:

```
┌──────────────┬──────────────┬──────────────┐
│  📊 Files    │  ✅ Passed   │  ❌ Failed   │
│     2        │     15       │     3        │
├──────────────┼──────────────┼──────────────┤
│  ⚠️ Warnings │  ⏱️ Duration │  📝 Total    │
│     7        │  12.5s       │  25 checks   │
└──────────────┴──────────────┴──────────────┘
```

**Metrics Explained:**

| Metric | Meaning | Interpretation |
|--------|---------|----------------|
| **Files** | Number of files validated | Each file can have multiple validations |
| **Passed** | Validation checks that passed | All rows met requirements |
| **Failed** | Validation checks that failed | Some rows violated rules |
| **Warnings** | WARNING-severity failures | Quality issues, not critical |
| **Duration** | Total execution time | Includes all files and validations |
| **Total** | Total validation checks | Passed + Failed |

### Summary Table

Below the cards, a table lists each file:

| File | Status | Errors | Warnings | Row Count | Validations |
|------|--------|--------|----------|-----------|-------------|
| customers.csv | ✅ PASSED | 0 | 2 | 10,000 | 8 |
| orders.csv | ❌ FAILED | 3 | 5 | 5,000 | 12 |

**Columns:**
- **File** - Click to jump to detailed results
- **Status** - File-level status (worst of all validations)
- **Errors** - COUNT of ERROR-severity failures
- **Warnings** - Count of WARNING-severity failures
- **Row Count** - Total rows in file
- **Validations** - Total checks run on file

---

## Interpreting Validation Results

### Result Cards

Each validation shows a result card:

**Passed Validation:**

```
┌────────────────────────────────────────────┐
│  ✅ Email Format Check                     │
├────────────────────────────────────────────┤
│  Severity: ERROR                           │
│  Status: PASSED                            │
│  Message: All 10,000 emails valid          │
│  Failures: 0 / 10,000                     │
│  Success Rate: 100%                        │
│  Execution Time: 2.3s                      │
└────────────────────────────────────────────┘
```

**Failed Validation:**

```
┌────────────────────────────────────────────┐
│  ❌ Age Range Check                        │
├────────────────────────────────────────────┤
│  Severity: ERROR                           │
│  Status: FAILED                            │
│  Message: Found 15 ages outside range      │
│  Failures: 15 / 10,000                    │
│  Success Rate: 99.85%                      │
│  Execution Time: 1.2s                      │
│                                            │
│  📋 Sample Failures (showing 10 of 15)     │
│  [Expandable section - click to view]     │
└────────────────────────────────────────────┘
```

### Key Fields

**Severity:**
- `ERROR` - Critical, must fix
- `WARNING` - Review recommended

**Status:**
- `PASSED` - All rows valid
- `FAILED` - Some rows invalid

**Message:**
- Human-readable summary
- Count of failures
- Context about issue

**Failures:**
- `15 / 10,000` means:
  - 15 rows failed validation
  - Out of 10,000 total rows checked

**Success Rate:**
- Percentage of rows that passed
- `99.85%` = (10,000 - 15) / 10,000

**Execution Time:**
- Seconds to run this validation
- Helps identify slow validations

---

## Sample Failures

### Understanding Samples

DataK9 shows sample failures to help you understand issues:

**Default:** Shows up to 100 sample failures per validation

**Why Samples?**
- Large datasets may have thousands of failures
- Samples give representative view
- Keeps reports manageable
- Speeds up report generation

### Sample Format

Failures shown as structured data:

```json
Sample Failure 1:
{
  "row": 42,
  "field": "age",
  "value": 150,
  "message": "Age 150 exceeds maximum of 120"
}

Sample Failure 2:
{
  "row": 99,
  "field": "age",
  "value": 5,
  "message": "Age 5 below minimum of 18"
}

Sample Failure 3:
{
  "row": 156,
  "field": "age",
  "value": null,
  "message": "Age is null/missing"
}
```

**Fields in Samples:**

| Field | Description | Example |
|-------|-------------|---------|
| `row` | Row number (0-indexed or 1-indexed) | 42 |
| `field` | Column/field name | "age" |
| `value` | Actual value that failed | 150 |
| `message` | Why it failed | "Exceeds maximum" |
| `expected` | Expected value (if applicable) | 120 |

### Analyzing Samples

**Look for Patterns:**

```
All failures in same field?
→ Field has systematic issue

Failures across many fields?
→ Row-level data quality problem

Failures in specific rows?
→ Source system issue at specific time

Similar values failing?
→ Validation rule may be too strict
```

**Example Analysis:**

```
Validation: Email Format Check
Failures: 50 / 10,000

Sample Failures:
- Row 12: "john.smith@companycom" (missing dot before TLD)
- Row 45: "jane.doe@company" (no TLD)
- Row 78: "bob@.com" (missing domain)
- Row 101: "@company.com" (missing local part)
- Row 234: "alice@" (missing domain)

Pattern: All missing parts of email format
Action: Source system not validating emails
Fix: Implement input validation at source
```

---

## JSON Reports

### JSON Structure

```json
{
  "job_name": "Customer Data Validation",
  "execution_time": "2024-01-15T14:30:22",
  "duration_seconds": 12.5,
  "overall_status": "FAILED",
  "total_errors": 3,
  "total_warnings": 7,

  "files": [
    {
      "file_name": "customers",
      "file_path": "data/customers.csv",
      "file_format": "csv",
      "status": "FAILED",
      "error_count": 3,
      "warning_count": 5,
      "execution_time": 8.2,

      "metadata": {
        "row_count": 10000,
        "columns": ["customer_id", "name", "email", "age"],
        "column_count": 4
      },

      "validations": [
        {
          "rule_name": "Email Format Check",
          "severity": "ERROR",
          "passed": false,
          "message": "Found 15 invalid emails",
          "failed_count": 15,
          "total_count": 10000,
          "success_rate": 99.85,
          "execution_time": 2.3,

          "sample_failures": [
            {
              "row": 42,
              "field": "email",
              "value": "invalid-email",
              "message": "Does not match email pattern"
            }
          ]
        }
      ]
    }
  ]
}
```

### Programmatic Access

**Python Example:**

```python
import json

# Load report
with open('validation_summary.json') as f:
    report = json.load(f)

# Check overall status
if report['overall_status'] == 'FAILED':
    print(f"❌ Validation failed")
    print(f"Errors: {report['total_errors']}")
    print(f"Warnings: {report['total_warnings']}")

    # List failed validations
    for file_report in report['files']:
        for validation in file_report['validations']:
            if not validation['passed']:
                print(f"\n{validation['rule_name']}:")
                print(f"  Failures: {validation['failed_count']}")
                print(f"  Message: {validation['message']}")

    exit(1)  # Exit with error
else:
    print("✅ All validations passed")
    exit(0)
```

**Bash/jq Example:**

```bash
#!/bin/bash

# Check status
STATUS=$(jq -r '.overall_status' validation_summary.json)

if [ "$STATUS" == "FAILED" ]; then
    echo "❌ Validation failed"

    # Count errors
    ERRORS=$(jq -r '.total_errors' validation_summary.json)
    echo "Errors: $ERRORS"

    # List failed checks
    jq -r '.files[].validations[] | select(.passed == false) | "- \(.rule_name): \(.message)"' validation_summary.json

    exit 1
else
    echo "✅ All validations passed"
    exit 0
fi
```

---

## Taking Action

### Action Matrix

Based on status, take appropriate action:

| Status | Errors | Warnings | Action |
|--------|--------|----------|--------|
| PASSED | 0 | 0 | ✅ Proceed with data processing |
| PASSED | 0 | >0 | ⚠️ Review warnings, then proceed |
| FAILED | >0 | any | ❌ Fix errors before processing |

### Workflow by Severity

**ERROR Failures:**

```
1. Stop processing
2. Review failure samples
3. Identify root cause
4. Fix data at source
5. Re-run validation
6. Proceed when PASSED
```

**WARNING Failures:**

```
1. Review failure samples
2. Assess business impact
3. Document decision
4. Proceed with data OR
5. Fix if critical for use case
```

### Common Actions

**For Invalid Formats:**
```
Action: Fix at source
Example: Invalid emails
→ Implement input validation in source system
```

**For Out-of-Range Values:**
```
Action: Investigate anomalies
Example: Age = 150
→ Check if data entry error or system bug
```

**For Missing Mandatory Fields:**
```
Action: Enforce completeness
Example: Missing customer_id
→ Make field required in source database
```

**For Duplicate Keys:**
```
Action: Implement deduplication
Example: Duplicate order_ids
→ Add unique constraint or dedup logic
```

**For Referential Integrity:**
```
Action: Validate relationships
Example: Invalid customer_id
→ Ensure foreign key constraints
```

---

## Common Patterns

### Pattern 1: Systematic Source Issue

**Symptoms:**
- Same validation failing consistently
- Many failures across all rows
- Pattern in failure times/dates

**Example:**
```
Email Format Check: 5,000 / 10,000 failed
All failures: email field is null

Root Cause: Source system not capturing emails
Action: Fix source data collection
```

### Pattern 2: Data Entry Errors

**Symptoms:**
- Random, scattered failures
- Different fields affected
- Human-recognizable mistakes

**Example:**
```
Age Range Check: 5 / 10,000 failed
Row 42: age = 150 (likely typo: 15)
Row 99: age = 5 (below minimum)

Root Cause: Manual data entry errors
Action: Implement input validation, data review
```

### Pattern 3: Validation Too Strict

**Symptoms:**
- Many failures
- Values seem reasonable
- Business users disagree with failures

**Example:**
```
Name Length Check: 500 / 10,000 failed
Failures: Names longer than 50 characters

Root Cause: Validation rule too strict
Action: Increase max_length to 100
```

### Pattern 4: Data Drift

**Symptoms:**
- Validations that used to pass now fail
- New patterns in data
- Changes after source system update

**Example:**
```
Valid Status Check: 100 / 10,000 failed
New value: "on_hold" not in allowed list

Root Cause: Source added new status type
Action: Update allowed_values list
```

### Pattern 5: Incomplete Data Load

**Symptoms:**
- Row count lower than expected
- Unexpected nulls
- Truncated data

**Example:**
```
Row Count Range Check: FAILED
Expected: 9,000 - 11,000
Actual: 5,000

Root Cause: Data extract incomplete
Action: Re-run extract, investigate source
```

---

## Report Tips

### 1. Start with Summary

Don't dive into details first:

```
1. Check overall status (PASSED/FAILED)
2. Review summary counts
3. Identify files with issues
4. Then drill into failures
```

### 2. Prioritize by Severity

Focus on ERRORs first:

```
1. ERROR failures (must fix)
2. WARNING failures (should review)
3. Passed validations (for context)
```

### 3. Look for Patterns

Don't treat each failure independently:

```
Pattern: All email failures in rows 1000-2000
Insight: Issue with specific batch
Action: Re-process that batch
```

### 4. Use Sample Failures

Samples are representative:

```
100 samples from 10,000 failures
→ Shows variety of issues
→ Helps identify root cause
→ No need to review all 10,000
```

### 5. Track Over Time

Compare reports from different runs:

```
Run 1: 50 failures
Run 2: 30 failures (improvement!)
Run 3: 10 failures (getting better)
Run 4: 0 failures (perfect!)
```

---

## Next Steps

**You've learned to read validation reports! Now:**

1. **[Configuration Guide](configuration-guide.md)** - Adjust validation rules
2. **[Best Practices](best-practices.md)** - Production deployment guidance
3. **[Troubleshooting](troubleshooting.md)** - Solve common issues
4. **[Data Profiling](data-profiling.md)** - Understand your data better

---

**🐕 DataK9 reports what it found - now you know how to act on it**
