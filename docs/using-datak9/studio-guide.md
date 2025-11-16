<div align="center">
  <img src="../../resources/images/datak9studio-web.png" alt="DataK9 Studio Logo" width="300">

  # DataK9 Studio Guide

  **Your IDE for Data Quality**
</div>

DataK9 Studio is a professional, IDE-style interface for creating validation configurations without writing YAML by hand. Like VS Code for data quality - build powerful validation rules through an intuitive visual interface.

---

## Table of Contents

1. [What is DataK9 Studio?](#what-is-datak9-studio)
2. [Quick Start](#quick-start)
3. [The Interface](#the-interface)
4. [Creating Projects](#creating-projects)
5. [Adding Validations](#adding-validations)
6. [YAML Editor](#yaml-editor)
7. [Export and Import](#export-and-import)
8. [Best Practices](#best-practices)
9. [Keyboard Shortcuts](#keyboard-shortcuts)
10. [Troubleshooting](#troubleshooting)

---

## What is DataK9 Studio?

### Overview

DataK9 Studio brings IDE-level productivity to data validation configuration. Instead of writing YAML manually, use a visual interface with:

- 🎨 **Visual Configuration Builder** - Form-based validation creation
- 💻 **Monaco Code Editor** - VS Code's editor with syntax highlighting
- 🔄 **Two-Way Sync** - Visual ↔ YAML real-time synchronization
- 📦 **35+ Validations** - Complete validation library built-in
- 💾 **Auto-Save** - Never lose your work
- 📖 **Context-Sensitive Help** - Documentation when you need it
- ⌨️ **Keyboard Shortcuts** - Power-user workflows
- 📱 **Mobile-Responsive** - Works on tablets and phones

### Why DataK9?

**K9** (pronounced "canine") represents vigilance, loyalty, and protection - exactly what you need for data quality. DataK9 Studio is your loyal guardian, helping you build bulletproof validation rules.

### No Installation Required

DataK9 Studio is a single HTML file (170KB) that runs entirely in your browser:

- ✅ No server required
- ✅ No installation needed
- ✅ No dependencies to install
- ✅ Works offline (except Monaco CDN)
- ✅ Cross-platform (Windows, Mac, Linux)

---

## Quick Start

### 5-Minute Walkthrough

**Step 1: Open DataK9 Studio**

```bash
# Open datak9-studio.html in any modern browser
cd /home/daniel/www/dqa/data-validation-tool

# On Linux/Mac
open datak9-studio.html

# Or double-click datak9-studio.html in your file explorer
```

**Step 2: Create Your First Project**

1. Click **"🚀 New Project"** on the welcome screen
2. Fill in the wizard:

   **Page 1 - Project Details:**
   - Name: "Customer Data Validation"
   - Description: "Validates daily customer extract"
   - Click "Next"

   **Page 2 - Settings (accept defaults):**
   - Chunk Size: 50,000
   - Max Sample Failures: 100
   - Click "Next"

   **Page 3 - First File:**
   - Name: customer_data
   - Path: data/customers.csv
   - Format: CSV
   - Click "✓ Create Project"

**Step 3: Add Validation Rules**

Click **"+ Add Validation"** in the file card:

1. Select **"MandatoryFieldCheck"** from catalog
2. Configure parameters:
   - Severity: ERROR
   - Fields: customer_id, email, registration_date
3. Click **"💾 Save Changes"**

Add more validations:
- RegexCheck for email format
- ValidValuesCheck for status field
- UniqueKeyCheck for customer_id

**Step 4: Export Configuration**

Click **"💾 Export YAML"** in the bottom panel to download your configuration file.

**Step 5: Use with DataK9**

```bash
# Run validation with exported config
python3 -m validation_framework.cli validate customer_validation.yaml

# 🐕 DataK9 is now guarding your customer data!
```

---

## The Interface

### Layout Overview

```
┌─────────────────────────────────────────────────────┐
│  File  Edit  View  Help        ← Menu Bar           │
├───────────┬─────────────────────────┬───────────────┤
│           │                         │               │
│  Project  │   Visual Editor         │  Help Panel   │
│  Sidebar  │                         │               │
│           │   ───────────────       │               │
│  • Files  │   YAML Preview          │  Context-     │
│  • Vals   │   (Monaco Editor)       │  Sensitive    │
│           │                         │  Docs         │
│           │                         │               │
├───────────┴─────────────────────────┴───────────────┤
│  Export Panel                                       │
│  💾 Export YAML  📋 Copy  ℹ️ Status                 │
└─────────────────────────────────────────────────────┘
```

### Panel Functions

**Left Sidebar (Resizable: 200px - 600px)**
- 📁 Project tree
- 📄 File management
- ✓ Validation list
- Collapsible with `Ctrl+B`

**Center Workspace**
- 🎨 Visual editor (top half)
- 💻 YAML preview (bottom half)
- Resizable vertical split (30%-70%)

**Right Help Panel (Resizable: 250px - 600px)**
- 📖 Documentation
- 💡 Examples
- ⚙️ Parameter reference
- Collapsible with `Ctrl+H`

**Bottom Export Panel (Resizable: 150px - 400px)**
- 💾 Export controls
- 📋 Copy to clipboard
- ℹ️ Status information
- Collapsible with `Ctrl+J`

---

## Creating Projects

### New Project Wizard

The wizard guides you through 3 simple steps:

**Step 1: Project Details**

```yaml
Name: [Required] Your project name
Description: [Optional] What this validates
```

**Example:**
- Name: "E-Commerce Order Validation"
- Description: "Validates daily order extracts from Shopify"

**Step 2: Settings**

DataK9 provides smart defaults:

| Setting | Default | Description |
|---------|---------|-------------|
| chunk_size | 50,000 | Rows per chunk (memory efficiency) |
| max_sample_failures | 100 | Max failure samples to collect |
| fail_on_error | true | Exit code 1 if ERROR validations fail |
| fail_on_warning | false | Exit code 1 if WARNING validations fail |
| log_level | INFO | Logging verbosity |

💡 **Tip:** Accept defaults for most use cases.

**Step 3: First File**

```yaml
Name: [Required] Logical file name (alphanumeric_underscore)
Path: [Required] File path relative to where you'll run DataK9
Format: [Required] csv, excel, json, parquet, database
```

**Example:**
- Name: orders
- Path: data/orders.csv
- Format: csv

Click **"✓ Create Project"** and you're ready to add validations!

### Adding More Files

After creating your project:

1. Click **"+ Add File"** in the sidebar
2. Fill in file details
3. Click **"Add File"**

Each file can have its own set of validations.

---

## Adding Validations

### Validation Catalog

Click **"+ Add Validation"** to open the catalog with 35+ validation types organized into 10 categories:

#### File-Level Validations (3)

**EmptyFileCheck**
- Purpose: Ensure file contains data
- Parameters: None
- Use When: File must not be empty

**RowCountRangeCheck**
- Purpose: Verify expected row count
- Parameters: min_rows, max_rows
- Use When: You expect specific record volumes

**FileSizeCheck**
- Purpose: Check file size limits
- Parameters: min_size_bytes, max_size_bytes
- Use When: Detecting incomplete uploads

#### Schema Validations (2)

**SchemaMatchCheck**
- Purpose: Verify column names and types
- Parameters: expected_schema
- Use When: Ensuring schema consistency

**ColumnPresenceCheck**
- Purpose: Required columns exist
- Parameters: required_columns
- Use When: Validating minimal schema

#### Field-Level Validations (5)

**MandatoryFieldCheck** ⭐ Most Common
- Purpose: Check required fields not null
- Parameters: fields (list)
- Use When: Critical fields must have values

**RegexCheck** ⭐ Most Common
- Purpose: Validate field format with regex
- Parameters: field, pattern
- Use When: Emails, phone numbers, IDs, postcodes

**ValidValuesCheck** ⭐ Most Common
- Purpose: Field values in allowed list
- Parameters: field, allowed_values
- Use When: Status codes, categories, enums

**RangeCheck** ⭐ Most Common
- Purpose: Numeric values within bounds
- Parameters: field, min_value, max_value
- Use When: Ages, amounts, quantities

**DateFormatCheck**
- Purpose: Validate date formats
- Parameters: field, date_format
- Use When: Ensuring consistent date formats

#### Record-Level Validations (3)

**DuplicateRowCheck**
- Purpose: Find duplicate records
- Parameters: key_fields (optional)
- Use When: Ensuring data deduplication

**BlankRecordCheck**
- Purpose: Detect completely blank rows
- Parameters: None
- Use When: Cleaning data quality

**UniqueKeyCheck** ⭐ Most Common
- Purpose: Primary key uniqueness
- Parameters: key_fields
- Use When: Ensuring referential integrity

#### Advanced Validations (9)

**StatisticalOutlierCheck**
- Purpose: Detect statistical anomalies
- Parameters: field, method (zscore/iqr), threshold
- Use When: Finding unusual values

**CrossFieldComparisonCheck**
- Purpose: Compare two fields
- Parameters: field1, field2, operator
- Use When: start_date < end_date, price > cost

**FreshnessCheck**
- Purpose: Data recency validation
- Parameters: date_field, max_age_days
- Use When: Ensuring data is current

**CompletenessCheck**
- Purpose: Field completeness percentage
- Parameters: field, min_completeness_pct
- Use When: Expecting mostly complete data

**StringLengthCheck**
- Purpose: String length constraints
- Parameters: field, min_length, max_length
- Use When: Field length requirements

**NumericPrecisionCheck**
- Purpose: Decimal precision validation
- Parameters: field, max_decimals
- Use When: Financial precision rules

**InlineRegexCheck**
- Purpose: Quick inline regex validation
- Parameters: field, pattern
- Use When: Simple pattern matching inline

**InlineBusinessRuleCheck**
- Purpose: Custom inline business logic
- Parameters: rule_expression
- Use When: Quick custom validation rules

**InlineLookupCheck**
- Purpose: Inline reference data validation
- Parameters: field, lookup_values
- Use When: Small lookup validations

#### Cross-File Validations (4)

**ReferentialIntegrityCheck**
- Purpose: Foreign key validation
- Parameters: field, reference_file, reference_field
- Use When: Maintaining referential integrity

**CrossFileComparisonCheck**
- Purpose: Compare values across files
- Parameters: field, reference_file, reference_field, comparison
- Use When: Consistency across datasets

**CrossFileDuplicateCheck**
- Purpose: Find duplicates across files
- Parameters: field, other_files
- Use When: Global deduplication

#### Database Validations (3)

**SQLCustomCheck**
- Purpose: Custom SQL validation
- Parameters: connection_string, query
- Use When: Complex database logic

**DatabaseReferentialIntegrityCheck**
- Purpose: Check foreign keys in database
- Parameters: connection_string, table, field, ref_table, ref_field
- Use When: Database integrity

**DatabaseConstraintCheck**
- Purpose: Verify database constraints
- Parameters: connection_string, table, constraint_type
- Use When: Ensuring DB integrity

#### Temporal Validations (2)

**BaselineComparisonCheck**
- Purpose: Compare against baseline
- Parameters: baseline_file, field, threshold
- Use When: Detecting data drift

**TrendDetectionCheck**
- Purpose: Detect trend anomalies
- Parameters: field, window_size, threshold
- Use When: Monitoring data patterns

#### Statistical Validations (3)

**DistributionCheck**
- Purpose: Validate data distribution
- Parameters: field, expected_distribution
- Use When: Statistical consistency

**CorrelationCheck**
- Purpose: Check field correlations
- Parameters: field1, field2, expected_correlation
- Use When: Relationship validation

**AdvancedAnomalyDetectionCheck**
- Purpose: ML-based anomaly detection
- Parameters: fields, algorithm, threshold
- Use When: Complex anomaly detection

### Configuring Validations

When you select a validation, a form appears with:

**Required Fields** (marked with *)
- Always must be filled in
- Examples: field name, pattern, threshold

**Optional Fields**
- Enhance functionality
- Examples: message, condition

**Severity Selection**

Choose severity based on impact:

| Severity | Use When | Effect |
|----------|----------|--------|
| ERROR | Data is fundamentally broken | Validation failure causes job failure (exit code 1) |
| WARNING | Quality issues, but data usable | Validation failure is logged but doesn't fail job |

💡 **Rule of Thumb:**
- ERROR: Cannot safely process data (missing primary keys, invalid formats)
- WARNING: Should review but can continue (outliers, low completeness)

**Conditional Validations**

All validations support optional `condition` parameter using pandas syntax:

```yaml
# Only check discount for retail customers
condition: "customer_type == 'retail'"

# Only check adults
condition: "age >= 18"

# Complex conditions
condition: "(status == 'active') & (balance > 0)"
```

---

## YAML Editor

### Monaco Editor Features

DataK9 Studio uses Monaco Editor (VS Code's engine):

**Editing Features:**
- Syntax highlighting
- Auto-indentation
- Line numbers
- Code folding
- Find & replace (Ctrl+F)
- Multi-cursor editing (Alt+Click)
- Bracket matching

**Two-Way Sync:**

Changes sync instantly:

```
Visual Editor → YAML
   ↓                ↓
Update field   →   YAML updates automatically

YAML Editor ← Visual
   ↓              ↓
Edit YAML  →   Visual form updates
```

**Example Workflow:**

1. Add validation in visual form → YAML updates
2. Tweak parameters in YAML → Form updates
3. Copy/paste YAML sections → Form parses and displays
4. Export final YAML → Download .yaml file

---

## Export and Import

### Exporting Projects

**Method 1: Download YAML File**

Click **"💾 Export YAML"** in bottom panel:
- Downloads `.yaml` file
- Ready to use with DataK9 CLI
- Saved to browser's download folder

**Method 2: Copy to Clipboard**

Click **"📋 Copy YAML"**:
- Copies YAML to clipboard
- Paste into editor or terminal
- Quick sharing with team

**Method 3: Manual Copy**

Select text in Monaco editor and copy:
- Full control over what you copy
- Can copy specific sections

### Importing YAML

**Current Capabilities:**

DataK9 Studio v1.0 has basic import:
- Import simple configurations
- May not parse complex YAML perfectly
- Recommended: Use wizard to recreate projects

**Future Enhancement:**

Full YAML parser coming in v2.0:
- Parse any valid DataK9 YAML
- Preserve comments and formatting
- Round-trip editing (import → edit → export)

**Workaround:**

For now, to edit existing configs:
1. Open existing YAML in text editor
2. Use Studio to create new project
3. Recreate validations in visual interface
4. Reference old YAML for parameters

---

## Best Practices

### Choose Appropriate Severity

**ERROR for:**
- Missing primary keys
- Invalid foreign keys
- Malformed required formats (email, phone)
- Schema violations
- Data type mismatches
- Missing mandatory fields

**WARNING for:**
- Statistical outliers
- Low completeness percentages
- Unusual value patterns
- Date freshness issues
- Distribution shifts

**Example:**

```yaml
# ERROR: Critical - can't process without valid email
- type: RegexCheck
  severity: ERROR
  params:
    field: email
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

# WARNING: Quality issue - should review but can continue
- type: CompletenessCheck
  severity: WARNING
  params:
    field: phone
    min_completeness_pct: 80
```

### Layer Validations Logically

Apply validations in order:

```
1. File-Level       → File exists, not empty
2. Schema           → Columns present
3. Field-Level      → Values valid, not null
4. Record-Level     → Row integrity, uniqueness
5. Advanced         → Statistical checks
6. Cross-File       → Relationships valid
```

**Why?** Fail fast. No point checking email formats if file is empty.

### Set Realistic Bounds

Use business limits, not technical limits:

```yaml
# ❌ BAD: Technical limit
max_value: 2147483647

# ✅ GOOD: Business limit
max_value: 120  # for age field
```

### Use Meaningful Names

```yaml
# ❌ BAD: Generic names
- name: file1
- name: check1

# ✅ GOOD: Descriptive names
- name: customer_master
- name: email_format_check
```

### Always Export Your Work

**localStorage can be cleared!**

- Browser settings clear storage
- Private/incognito mode doesn't persist
- Different browsers have separate storage

**Safety:**
1. Export to YAML after major changes
2. Commit YAML to version control
3. Backup important configurations

### Test Incrementally

Build validations step by step:

1. Start with 1-2 critical validations
2. Run against sample data
3. Add more validations
4. Test again
5. Gradually build complete suite

Don't create 20 validations then test - debug incrementally!

---

## Keyboard Shortcuts

### Global Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+N` / `Cmd+N` | New project |
| `Ctrl+O` / `Cmd+O` | Import YAML |
| `Ctrl+S` / `Cmd+S` | Export YAML |
| `Ctrl+E` / `Cmd+E` | Export YAML (alternative) |

### Panel Toggles

| Shortcut | Action |
|----------|--------|
| `Ctrl+B` / `Cmd+B` | Toggle sidebar |
| `Ctrl+H` / `Cmd+H` | Toggle help panel |
| `Ctrl+J` / `Cmd+J` | Toggle bottom panel |

### Monaco Editor

| Shortcut | Action |
|----------|--------|
| `Ctrl+F` / `Cmd+F` | Find |
| `Ctrl+H` / `Cmd+H` | Find & replace |
| `Ctrl+/` / `Cmd+/` | Toggle comment |
| `Alt+Click` | Add cursor |
| `Ctrl+D` / `Cmd+D` | Select next occurrence |

---

## Troubleshooting

### Common Issues

**Problem: Monaco Editor doesn't load**

Causes:
- No internet connection (Monaco loads from CDN)
- Browser blocking CDN
- Firewall blocking external resources

Solutions:
1. Check internet connection
2. Disable ad blockers/extensions
3. Try different browser
4. Check browser console (F12) for errors

**Problem: localStorage full error**

Causes:
- Too many large projects saved
- Browser storage limit reached (usually 5-10MB)

Solutions:
1. Export current project to YAML
2. Clear browser data for DataK9 Studio
3. Split large projects into smaller ones
4. Use external files instead of embedding large schemas

**Problem: Project disappeared**

Causes:
- Browser cleared localStorage
- Private/incognito mode (doesn't persist)
- Different browser/profile

Solutions:
1. Check browser privacy settings
2. Disable "clear on exit" for DataK9 Studio domain
3. Always export important projects to YAML
4. Commit YAMLs to version control

**Problem: Validation not working as expected**

Debugging steps:
1. Check validation parameters in visual form
2. Review generated YAML in Monaco editor
3. Test with small sample dataset first
4. Check help panel for parameter documentation
5. Verify field names match actual data

**Problem: Cannot import YAML**

Current limitation:
- v1.0 has basic YAML parser
- Complex configs may not import perfectly

Workaround:
1. Use wizard to create new project
2. Reference existing YAML for parameters
3. Recreate validations manually
4. Wait for v2.0 with full YAML parser

**Problem: Export not working**

Troubleshooting:
1. Check browser download settings
2. Check disk space
3. Try "Copy YAML" instead
4. Try different browser

---

## Tips & Tricks

### Quick Project Setup

Use templates for common scenarios:

**E-Commerce Orders:**
```
Files: orders, customers, products
Validations:
- Mandatory fields (order_id, customer_id, product_id)
- Referential integrity (customer_id → customers, product_id → products)
- Range check (quantity > 0, price > 0)
- Date validation (order_date not in future)
```

**Financial Transactions:**
```
Files: transactions, accounts
Validations:
- Unique keys (transaction_id)
- Mandatory fields (account_id, amount, date)
- Range check (amount, date range)
- Cross-file reference (account_id → accounts)
```

### Maximize Productivity

**Use Search in Catalog:**
- Type "email" → finds RegexCheck, MandatoryFieldCheck
- Type "unique" → finds UniqueKeyCheck, CrossFileDuplicateCheck
- Faster than scrolling

**Use Help Panel:**
- Select validation → help updates automatically
- See examples without leaving Studio
- Copy/paste parameter examples

**Use YAML for Bulk Operations:**
- Need 10 similar validations?
- Create one in visual form
- Copy YAML block 10 times
- Edit field names in YAML
- Visual form updates automatically

---

## Next Steps

**You've learned DataK9 Studio! Now:**

1. **[Configuration Guide](configuration-guide.md)** - Deep dive into YAML configuration
2. **[Validation Catalog](validation-catalog.md)** - Complete reference of all 35+ validations
3. **[Best Practices](best-practices.md)** - Production deployment guidance
4. **[Reading Reports](reading-reports.md)** - Understanding validation results

---

## Browser Requirements

| Browser | Min Version | Status |
|---------|-------------|--------|
| Chrome | 90+ | ✅ Fully Supported |
| Firefox | 88+ | ✅ Fully Supported |
| Edge | 90+ | ✅ Fully Supported |
| Safari | 14+ | ✅ Fully Supported |
| IE | Any | ❌ Not Supported |

---

## System Requirements

- **OS**: Windows 10+, macOS 10.14+, Linux (modern distro)
- **RAM**: 4GB min, 8GB recommended
- **Screen**: 1280x720 min, 1920x1080 recommended
- **Internet**: Required for Monaco CDN
- **JavaScript**: Must be enabled
- **localStorage**: Must be enabled (5-10MB available)

---

**🐕 Build robust validations with DataK9 Studio - Your IDE for data quality**
