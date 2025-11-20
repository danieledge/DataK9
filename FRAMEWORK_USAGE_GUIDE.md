# DataK9 Framework - Complete Usage Guide

**How to use the DataK9 Data Quality Framework from start to finish**

---

## 📑 Table of Contents

- [Overview](#overview)
- [The DataK9 Workflow](#the-datak9-workflow)
- [Step 1: Profile Your Data](#step-1-profile-your-data)
- [Step 2: Build Validation Configuration](#step-2-build-validation-configuration)
- [Step 3: Run Validation](#step-3-run-validation)
- [Step 4: Review Results](#step-4-review-results)
- [Advanced Workflows](#advanced-workflows)
- [Best Practices](#best-practices)

---

## Overview

DataK9 provides **three complementary tools** that work together:

1. **Profiler** - Analyzes data to understand structure, quality, and patterns
2. **Studio** - Visual IDE for building validation configurations (no coding)
3. **Validation Engine** - Executes validations and generates reports

**The Complete Framework:**

```
Your Data
    │
    ▼
┌──────────────────────┐
│  DataK9 Profiler     │ ← Analyze data structure & quality
│  python3 -m ... profile
└──────────────────────┘
    │
    ├─► profile_report.html   (Interactive analysis)
    ├─► profile.json          (Machine-readable data)
    └─► validation.yaml       (Auto-generated config)
         │
         ▼
┌──────────────────────┐
│  DataK9 Studio       │ ← Refine config visually (optional)
│  datak9-studio.html  │
└──────────────────────┘
    │
    └─► refined_validation.yaml
         │
         ▼
┌──────────────────────┐
│  DataK9 Validator    │ ← Execute validations
│  python3 -m ... validate
└──────────────────────┘
    │
    ├─► validation_report.html
    └─► validation_summary.json
```

---

## The DataK9 Workflow

### Recommended Approach (First-Time Users)

**For new data sources:**

1. **Profile** → Understand your data
2. **Studio** → Build/refine config visually
3. **Validate** → Test with real data
4. **Iterate** → Refine based on results
5. **Productionize** → Schedule in pipeline

**For recurring validations:**

1. **Validate** → Run existing config
2. **Monitor** → Track quality over time
3. **Adjust** → Update validations as needed

---

## Step 1: Profile Your Data

**Goal:** Understand data structure, quality, and patterns before building validations.

### What the Profiler Does

The profiler analyzes your data and generates:

1. **Interactive HTML Report** - Visual analysis with charts
2. **JSON Profile** - Machine-readable data (optional)
3. **Auto-Generated YAML Config** - Ready-to-use validation configuration

**Analysis Performed:**
- Type inference with confidence scoring
- Statistical distributions (mean, median, quartiles)
- Quality metrics (completeness, validity, uniqueness)
- Pattern detection (emails, phones, dates, etc.)
- Outlier and anomaly detection
- Correlation analysis
- PII detection
- Validation suggestions (sorted by confidence)

### Profile a File

```bash
# Basic profiling
python3 -m validation_framework.cli profile data/customers.csv

# With custom outputs
python3 -m validation_framework.cli profile data/customers.csv \
  -o customer_profile.html \
  -j customer_profile.json \
  -c customer_validation.yaml

# Large file with custom chunk size
python3 -m validation_framework.cli profile large_data.parquet \
  --chunk-size 100000
```

**Outputs:**
- `customer_profile.html` - Open in browser for visual analysis
- `customer_profile.json` - Use for programmatic analysis
- `customer_validation.yaml` - Ready to use with CLI or refine in Studio

### Profile a Database Table

```bash
# PostgreSQL table
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/mydb" \
  --table customers \
  -o db_profile.html

# With custom SQL query
python3 -m validation_framework.cli profile \
  --database "sqlite:///test.db" \
  --query "SELECT * FROM orders WHERE date > '2024-01-01'" \
  -o recent_orders_profile.html
```

### Understanding the Profile Report

Open the HTML profile report in your browser:

**Key Sections:**

1. **Summary Cards**
   - File size, format, rows, columns
   - Overall quality score (0-100%)

2. **Quality Overview Charts**
   - Completeness by column (bar chart)
   - Quality radar chart

3. **Column Profiles** (Expandable cards)
   - Type information (known vs inferred, confidence)
   - Statistics (min, max, mean, median for numbers; lengths for strings)
   - Quality metrics (completeness, validity, uniqueness, consistency)
   - Issues list (quality problems detected)
   - Top values table

4. **Correlations** (if numeric columns exist)
   - Significant correlations (|r| > 0.5)
   - Helps identify field relationships

5. **Suggested Validations**
   - Context-aware suggestions with confidence scores
   - Parameters pre-filled
   - Sorted by confidence (most confident first)

6. **Generated Configuration**
   - Auto-generated YAML config
   - Copy button for easy export
   - CLI command to run validation

**What to Look For:**

✅ **High quality scores (>90%)** - Data is clean, ready for validation
⚠️ **Medium scores (70-90%)** - Some quality issues, review suggestions
❌ **Low scores (<70%)** - Significant issues, investigate before validation

**Key Insights:**

- **Completeness** - Which fields have nulls?
- **Uniqueness** - Which fields are keys/identifiers?
- **Patterns** - Email detection, phone formats, etc.
- **Outliers** - Statistical anomalies (>5% outliers = investigate)
- **Types** - Confidence <80% = mixed types (needs cleanup)

### Using Auto-Generated Config

The profiler creates a ready-to-use YAML config:

```bash
# Profile creates customer_validation.yaml
python3 -m validation_framework.cli profile data/customers.csv

# Immediately run validation
python3 -m validation_framework.cli validate customer_validation.yaml
```

**What's in the Auto-Generated Config:**

1. **File-Level Validations**
   - EmptyFileCheck (always included)
   - RowCountRangeCheck (±50-200% of profiled count)

2. **Mandatory Field Checks**
   - Fields with >95% completeness

3. **Unique Key Checks**
   - Fields with >99% cardinality

4. **Range Checks**
   - Numeric fields using P1-P99 percentiles (robust to outliers)

5. **Pattern Validations**
   - RegexCheck for detected patterns (emails, phones, etc.)

6. **Valid Values Checks**
   - Low cardinality fields (<5% cardinality, <20 unique values)

7. **Statistical Outlier Checks**
   - Fields with >5% outliers detected

8. **Freshness Checks**
   - Date fields with stale data (>30 days old)

**Customization Needed:**

- ✏️ Review severity levels (profiler is conservative)
- ✏️ Adjust ranges based on business rules (profiler shows data as-is)
- ✏️ Add business-specific validations
- ✏️ Refine patterns for specific needs

---

## Step 2: Build Validation Configuration

You have **three options** for building configurations:

### Option A: Use Auto-Generated Config (Fastest)

**When to use:** Quick validation, auto-generated config looks good

```bash
# Profile generates config
python3 -m validation_framework.cli profile data.csv -c config.yaml

# Use directly
python3 -m validation_framework.cli validate config.yaml
```

### Option B: Edit YAML Manually

**When to use:** You understand YAML, want full control

```yaml
validation_job:
  name: "Customer Data Validation"

settings:
  chunk_size: 50000
  max_sample_failures: 100

files:
  - name: "customers"
    path: "data/customers.csv"
    format: "csv"

    validations:
      - type: "EmptyFileCheck"
        severity: "ERROR"

      - type: "MandatoryFieldCheck"
        severity: "ERROR"
        params:
          fields: ["customer_id", "email"]

      - type: "RegexCheck"
        severity: "ERROR"
        params:
          field: "email"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

output:
  html_report: "validation_report.html"
  json_summary: "validation_summary.json"
  fail_on_error: true
```

**See [VALIDATION_REFERENCE.md](VALIDATION_REFERENCE.md) for all 35 validation types**

### Option C: Use DataK9 Studio (Visual, No Coding)

**When to use:** Prefer visual tools, building complex configs, team collaboration

#### Launch Studio

```bash
# Open Studio in browser
cd data-validation-tool
open datak9-studio.html          # macOS
xdg-open datak9-studio.html      # Linux
start datak9-studio.html         # Windows
```

**Or browse to:**
- Local: `file:///path/to/datak9-studio.html`
- Online: https://your-domain.com/datak9-studio.html

#### Studio Workflow

**1. Start New Project**
- Click "🚀 New Project"
- Name: `customer_validation`
- Description: `Daily customer extract validation`

**2. Add File**
- Click "⊕ Add File"
- Name: `customers`
- Path: `data/customers.csv`
- Format: `csv`

**3. Add Validations**
- Click "+ Add Validation" on file card
- Search/browse 35+ validation types
- Click validation card to add
- Configure parameters in center panel
- Click "💾 Save Changes"

**4. Review YAML**
- Bottom panel shows generated YAML
- Updates in real-time as you build

**5. Export Config**
- Click "💾 Export YAML"
- File downloads: `customer_validation.yaml`

**See [Studio Guide](docs/using-datak9/studio-guide.md) for complete reference**

#### Studio Tips

**Visual Form vs YAML:**
- Forms: Beginner-friendly, guided inputs
- YAML: Power users, bulk operations

**Switch Between Views:**
- Click "🔒 Unlock to Edit" to edit YAML directly
- Click "🔄 Sync from YAML" to update visual form

**Keyboard Shortcuts:**
- `Ctrl+S` - Export YAML
- `Ctrl+B` - Toggle sidebar
- `Ctrl+H` - Toggle help panel
- `Ctrl+R` - Sync from YAML

**Use Help Panel:**
- Select validation → Help updates automatically
- Shows parameter descriptions and examples

---

## Step 3: Run Validation

**Goal:** Execute validations and generate results.

### Basic Validation

```bash
# Run validation
python3 -m validation_framework.cli validate config.yaml

# With custom output paths
python3 -m validation_framework.cli validate config.yaml \
  -o reports/validation.html \
  -j reports/summary.json

# Verbose mode (shows progress)
python3 -m validation_framework.cli validate config.yaml --verbose

# Debug mode
python3 -m validation_framework.cli validate config.yaml \
  --log-level DEBUG \
  --log-file validation.log
```

### Understanding Exit Codes

DataK9 uses standard exit codes for automation:

| Exit Code | Status | Description |
|-----------|--------|-------------|
| `0` | SUCCESS | All ERROR validations passed (warnings OK) |
| `1` | VALIDATION_FAILED | ERROR-severity failures found |
| `2` | COMMAND_ERROR | Bad config, file not found, etc. |

**Use in Scripts:**

```bash
# Basic check
python3 -m validation_framework.cli validate config.yaml
if [ $? -ne 0 ]; then
  echo "Validation failed"
  exit 1
fi

# Detailed check
python3 -m validation_framework.cli validate config.yaml
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "✓ Validation passed"
  ./proceed_with_load.sh
elif [ $EXIT_CODE -eq 1 ]; then
  echo "✗ Data quality issues"
  ./send_alert.sh
  exit 1
elif [ $EXIT_CODE -eq 2 ]; then
  echo "✗ Configuration error"
  exit 2
fi
```

### Backend Selection

DataK9 supports two backends:

**Polars Backend** (Default, Recommended)
- 5-10x faster than pandas
- 50-70% less memory
- Best for: Large files (>1GB), memory-limited systems
- Install: `pip install polars`

```bash
python3 -m validation_framework.cli validate config.yaml --backend polars
```

**pandas Backend**
- Excel file support (.xlsx, .xls)
- Best for: Small files (<100MB), Excel formats
- Install: `pip install pandas openpyxl`

```bash
python3 -m validation_framework.cli validate config.yaml --backend pandas
```

**See [CLI_GUIDE.md](CLI_GUIDE.md) for complete reference**

---

## Step 4: Review Results

### HTML Validation Report

Open `validation_report.html` in your browser:

**Report Sections:**

1. **Header**
   - Validation job name
   - Overall status badge (PASSED/FAILED)
   - Timestamp

2. **Summary Cards**
   - Files processed
   - Validations run
   - Errors found
   - Warnings found
   - Processing time

3. **File Results** (Expandable cards for each file)
   - File name and path
   - Row count
   - File size
   - Status badge

4. **Validation Results** (For each validation)
   - ✅ Passed (green) - Validation succeeded
   - ❌ Failed (red) - ERROR severity failure
   - ⚠️ Warning (yellow) - WARNING severity issue
   - Severity badge
   - Message explaining result
   - Failure count (if failed)
   - Sample failures (up to 100 examples)

5. **Footer**
   - Total errors and warnings
   - Exit code explanation
   - Timestamp

### JSON Summary

**File:** `validation_summary.json`

**Structure:**

```json
{
  "job_name": "Customer Data Validation",
  "overall_status": "FAILED",
  "total_files": 1,
  "total_validations": 5,
  "total_errors": 2,
  "total_warnings": 1,
  "processing_time_seconds": 3.24,
  "files": [
    {
      "name": "customers",
      "path": "data/customers.csv",
      "row_count": 10000,
      "status": "FAILED",
      "validations": [
        {
          "type": "EmptyFileCheck",
          "severity": "ERROR",
          "passed": true,
          "message": "File contains 10000 rows"
        },
        {
          "type": "MandatoryFieldCheck",
          "severity": "ERROR",
          "passed": false,
          "message": "Found 200 rows with null values in required fields",
          "failure_count": 200,
          "sample_failures": [
            {"row": 105, "field": "email", "value": null},
            {"row": 287, "field": "customer_id", "value": null}
          ]
        }
      ]
    }
  ]
}
```

**Use Cases:**
- Programmatic analysis
- Dashboard integration
- Trend tracking over time
- Alert systems

### Interpreting Results

**✅ PASSED**
- All ERROR validations succeeded
- Warnings may exist (review recommended)
- Safe to proceed with data processing
- Exit code: 0

**❌ FAILED**
- One or more ERROR validations failed
- Data has quality issues
- DO NOT proceed with processing
- Exit code: 1
- **Action:** Fix data or adjust validations

**⚠️ WARNINGS**
- WARNING-severity issues found
- Data quality concerns but not critical
- Proceed with caution
- Exit code: 0 (unless `fail_on_warning: true`)
- **Action:** Investigate, consider fixing

### Common Failure Patterns

**Empty/Missing Data:**
```
EmptyFileCheck: FAILED
File is empty or contains no data rows
→ Check file generation process
```

**Null Values:**
```
MandatoryFieldCheck: FAILED (200 failures)
Fields [email] have null values in 200 rows
→ Missing data in source, add null handling
```

**Format Violations:**
```
RegexCheck: FAILED (150 failures)
email field: 150 values don't match pattern
Sample: "invalid-email" (row 45)
→ Invalid email formats, clean data
```

**Range Violations:**
```
RangeCheck: FAILED (25 failures)
age field: 25 values outside range [18, 120]
Sample: 150 (row 782)
→ Data entry errors or outliers
```

**Duplicates:**
```
UniqueKeyCheck: FAILED (50 duplicates)
Duplicate values found in key fields
Sample: customer_id=1234 (rows 100, 250, 500)
→ Duplicate records in source
```

---

## Advanced Workflows

### Workflow 1: Production Pipeline

**Goal:** Automated validation in ETL pipeline

```bash
#!/bin/bash
# production_validation.sh

DATE=$(date +%Y%m%d)
CONFIG="config/production_validation.yaml"
REPORT_DIR="reports/${DATE}"
REPORT_HTML="${REPORT_DIR}/validation_report.html"
REPORT_JSON="${REPORT_DIR}/validation_summary.json"
LOG_FILE="${REPORT_DIR}/validation.log"

# Create report directory
mkdir -p ${REPORT_DIR}

# Run validation
python3 -m validation_framework.cli validate ${CONFIG} \
  -o ${REPORT_HTML} \
  -j ${REPORT_JSON} \
  --log-file ${LOG_FILE} \
  --log-level INFO

# Capture exit code
EXIT_CODE=$?

# Log result
if [ $EXIT_CODE -eq 0 ]; then
  echo "$(date): Validation PASSED" >> validation_history.log
  # Proceed with ETL
  ./run_etl_load.sh
  exit 0
else
  echo "$(date): Validation FAILED - Exit code ${EXIT_CODE}" >> validation_history.log
  # Send alert
  ./send_alert_email.sh "${REPORT_HTML}"
  exit $EXIT_CODE
fi
```

**AutoSys JIL:**

```bash
insert_job: VALIDATE_DATA
job_type: c
command: /apps/validation/production_validation.sh
condition: success(EXTRACT_DATA)
alarm_if_fail: yes

insert_job: LOAD_DATA
job_type: c
command: /apps/etl/load_data.sh
condition: success(VALIDATE_DATA)
```

### Workflow 2: Profile-Based Monitoring

**Goal:** Track data quality trends over time

```bash
#!/bin/bash
# continuous_profiling.sh

DATE=$(date +%Y%m%d)
PROFILE_DIR="profiles/${DATE}"
mkdir -p ${PROFILE_DIR}

# Profile all data files
for file in data/*.csv; do
  filename=$(basename "$file" .csv)

  # Generate profile
  python3 -m validation_framework.cli profile "$file" \
    -o "${PROFILE_DIR}/${filename}_profile.html" \
    -j "${PROFILE_DIR}/${filename}_profile.json"

  echo "Profiled: ${filename}"
done

# Compare to yesterday for quality trends
python3 scripts/compare_profiles.py \
  "${PROFILE_DIR}" \
  "profiles/$(date -d yesterday +%Y%m%d)" \
  > "${PROFILE_DIR}/quality_trends.txt"

# Alert if quality dropped >10%
python3 scripts/quality_alert.py "${PROFILE_DIR}/quality_trends.txt"
```

### Workflow 3: Multi-Environment Validation

**Goal:** Different validation rules for dev/staging/production

```
configs/
├── dev_validation.yaml        # Lenient rules for testing
├── staging_validation.yaml    # Strict rules, warnings allowed
└── production_validation.yaml # Strictest rules
```

```bash
# Development
python3 -m validation_framework.cli validate configs/dev_validation.yaml

# Staging
python3 -m validation_framework.cli validate configs/staging_validation.yaml

# Production
python3 -m validation_framework.cli validate configs/production_validation.yaml --fail-on-warning
```

### Workflow 4: Database Pre-Load Validation

**Goal:** Validate data before loading to database

```bash
#!/bin/bash
# db_load_with_validation.sh

DB_TABLE="customers"
DATA_FILE="data/customers.csv"
CONFIG="config/customers_validation.yaml"

# Step 1: Validate file
echo "Validating data..."
python3 -m validation_framework.cli validate ${CONFIG} \
  -o reports/pre_load_validation.html

if [ $? -ne 0 ]; then
  echo "✗ Validation failed - aborting database load"
  exit 1
fi

# Step 2: Load to staging table
echo "✓ Validation passed - loading to staging..."
psql -d mydb -c "TRUNCATE TABLE ${DB_TABLE}_staging;"
psql -d mydb -c "\\copy ${DB_TABLE}_staging FROM '${DATA_FILE}' CSV HEADER;"

# Step 3: Validate in database
echo "Validating in database..."
python3 -m validation_framework.cli profile \
  --database "postgresql://user:pass@localhost/mydb" \
  --table "${DB_TABLE}_staging" \
  -o reports/db_staging_profile.html

# Step 4: Promote to production table
echo "Promoting staging → production..."
psql -d mydb -c "BEGIN; DELETE FROM ${DB_TABLE}; INSERT INTO ${DB_TABLE} SELECT * FROM ${DB_TABLE}_staging; COMMIT;"

echo "✓ Load complete"
```

---

## Best Practices

### 1. Always Profile First

❌ **Don't:**
```bash
# Blind validation without understanding data
python3 -m validation_framework.cli validate config.yaml
# Many unexpected failures due to misunderstood data
```

✅ **Do:**
```bash
# Profile to understand data
python3 -m validation_framework.cli profile data.csv

# Review profile report
# Build targeted validations based on insights
```

**Why:** Profiling reveals:
- Actual data types (not assumed types)
- Completeness percentages (not 100% as expected)
- Pattern variations (email formats differ)
- Outliers and anomalies (realistic ranges)

### 2. Start Simple, Iterate

❌ **Don't:**
```yaml
# 50 validations on first run
validations:
  - type: EmptyFileCheck
  - type: MandatoryFieldCheck
  - type: RegexCheck
  - type: RangeCheck
  # ... 46 more validations
```

✅ **Do:**
```yaml
# Start with 3-5 critical validations
validations:
  - type: EmptyFileCheck        # File exists
  - type: MandatoryFieldCheck   # Keys populated
  - type: UniqueKeyCheck        # No duplicates

# Run, fix issues, add more validations
```

**Why:**
- Fewer initial failures = easier to fix
- Understand data issues incrementally
- Avoid being overwhelmed by 1000+ failures

### 3. Use Appropriate Severity

❌ **Don't:**
```yaml
# Everything is ERROR
- type: CompletenessCheck
  severity: ERROR  # ← Too strict
  params:
    field: phone_number
    min_completeness: 100
```

✅ **Do:**
```yaml
# WARNING for quality metrics
- type: CompletenessCheck
  severity: WARNING  # ← Appropriate
  params:
    field: phone_number
    min_completeness: 80

# ERROR for critical constraints
- type: MandatoryFieldCheck
  severity: ERROR  # ← Critical data
  params:
    fields: [customer_id, email]
```

**Severity Guide:**
- **ERROR**: Data fundamentally broken (missing keys, violated constraints)
- **WARNING**: Quality concerns (low completeness, outliers, pattern variations)

### 4. Layer Validations Logically

❌ **Don't:**
```yaml
# Random order
validations:
  - type: RegexCheck              # Checks email format
  - type: EmptyFileCheck          # Should be first!
  - type: CrossFieldComparison    # Complex check
  - type: MandatoryFieldCheck     # Should be early
```

✅ **Do:**
```yaml
# Logical order: simple → complex, fail fast
validations:
  - type: EmptyFileCheck          # File exists?
  - type: ColumnPresenceCheck     # Columns exist?
  - type: MandatoryFieldCheck     # Keys populated?
  - type: RegexCheck              # Format valid?
  - type: CrossFieldComparison    # Relationships OK?
  - type: StatisticalOutlierCheck # Deep analysis last
```

**Validation Order:**
1. **File-Level** - File exists, row count reasonable
2. **Schema** - Columns present, types correct
3. **Field-Level** - Nulls, formats, ranges
4. **Record-Level** - Duplicates, blanks, uniqueness
5. **Advanced** - Outliers, cross-field logic
6. **Cross-File** - Referential integrity
7. **Statistical** - Distributions, correlations

**Why:** Fail fast on fundamental issues before expensive checks

### 5. Use Percentile-Based Ranges

❌ **Don't:**
```yaml
# Using min/max (sensitive to outliers)
- type: RangeCheck
  params:
    field: age
    min_value: 1    # ← One data error ruins range
    max_value: 150  # ← Unrealistic outlier
```

✅ **Do:**
```yaml
# Using P1-P99 percentiles (robust)
- type: RangeCheck
  params:
    field: age
    min_value: 18   # ← P1 percentile from profile
    max_value: 85   # ← P99 percentile from profile
```

**Why:** Percentiles ignore outliers, more realistic ranges

**Get Percentiles:** Profile report shows P1, P5, P95, P99 for all numeric fields

### 6. Handle Nulls Appropriately

❌ **Don't:**
```yaml
# Mandatory check on optional field
- type: MandatoryFieldCheck
  severity: ERROR
  params:
    fields: [phone_number]  # ← Optional field
```

✅ **Do:**
```yaml
# Completeness check with realistic threshold
- type: CompletenessCheck
  severity: WARNING
  params:
    field: phone_number
    min_completeness: 60  # ← Based on profile
```

**Check Profile:** Completeness percentage tells you realistic thresholds

### 7. Test with Real Data

❌ **Don't:**
```bash
# Deploy without testing
vim production_config.yaml  # Edit config
git commit && git push      # Deploy
# Production failures!
```

✅ **Do:**
```bash
# Test locally first
python3 -m validation_framework.cli validate config.yaml

# Review results
open validation_report.html

# Fix issues, iterate
# THEN deploy to production
```

### 8. Version Control Configs

✅ **Do:**
```bash
# Track validation configs in Git
git add config/production_validation.yaml
git commit -m "Update validation: add email regex check"
git push

# Revert if needed
git revert HEAD
```

**Benefits:**
- Track changes over time
- Collaborate with team
- Rollback if issues
- Code review validation changes

### 9. Monitor Quality Trends

✅ **Do:**
```bash
# Daily profiling
python3 -m validation_framework.cli profile data.csv \
  -j "profiles/$(date +%Y%m%d)_profile.json"

# Track quality over time
python3 scripts/quality_trends.py profiles/*.json > quality_dashboard.html
```

**Track These Metrics:**
- Overall quality score (target: >90%)
- Completeness percentages (watch for drops)
- Outlier percentages (watch for increases)
- Row counts (watch for unusual volumes)

### 10. Document Business Rules

❌ **Don't:**
```yaml
# No explanation
- type: RangeCheck
  params:
    field: discount_rate
    max_value: 0.40
```

✅ **Do:**
```yaml
# Document reasoning
- type: RangeCheck
  params:
    field: discount_rate
    max_value: 0.40  # Company policy: max 40% discount
  message: "Discount exceeds company maximum (40%)"
```

**Add Comments:**
```yaml
validations:
  # Critical validations (ERROR)
  - type: MandatoryFieldCheck
    # Business requirement: All customers must have ID and email

  # Quality checks (WARNING)
  - type: CompletenessCheck
    # Marketing requested 80% phone coverage for campaigns
```

---

## Quick Reference

### Common Commands

```bash
# Profile data
python3 -m validation_framework.cli profile data.csv

# Validate data
python3 -m validation_framework.cli validate config.yaml

# List validations
python3 -m validation_framework.cli list-validations

# List by category
python3 -m validation_framework.cli list-validations --category field

# List for databases
python3 -m validation_framework.cli list-validations --source database

# Generate sample config
python3 -m validation_framework.cli init-config my_config.yaml
```

### File Locations

```
data-validation-tool/
├── datak9-studio.html           # Visual IDE
├── validation_framework/
│   └── cli.py                   # CLI commands
├── config/
│   └── production.yaml          # Your configs
├── reports/
│   ├── validation_report.html   # Validation results
│   └── validation_summary.json  # Machine-readable
└── profiles/
    ├── data_profile.html        # Profile reports
    └── data_profile.json        # Profile data
```

### Next Steps

**Learn More:**
- **[CLI_GUIDE.md](CLI_GUIDE.md)** - Complete CLI reference
- **[VALIDATION_REFERENCE.md](VALIDATION_REFERENCE.md)** - All 35 validations
- **[Best Practices](docs/using-datak9/best-practices.md)** - Production patterns
- **[Performance Tuning](docs/using-datak9/performance-tuning.md)** - Optimize speed/memory

**Get Help:**
- [GitHub Issues](https://github.com/danieledge/data-validation-tool/issues) - Bug reports
- [Error Codes](docs/reference/error-codes.md) - Troubleshooting
- [FAQ](docs/using-datak9/faq.md) - Common questions

---

**🐕 DataK9 - Your K9 guardian for data quality**
