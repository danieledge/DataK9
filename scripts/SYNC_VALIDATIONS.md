# Validation Synchronization Guide

## Problem

DataK9 has validation definitions in two places:
1. **Python** (`validation_framework/validations/`) - 34 validation types
2. **JavaScript** (`datak9-studio.html` validationLibrary) - 22 validation types

This creates a maintenance burden and risk of drift.

## Solution

Automated synchronization script that reads Python validation registry and generates JavaScript code.

## Usage

### Run Sync Script

```bash
cd /home/daniel/www/dqa/data-validation-tool
python3 scripts/sync_validations.py
```

**Output:**
- Summary report showing all 34 validations by category
- Generated JavaScript file: `scripts/generated_validations.js`

### Update Studio

1. Review `scripts/generated_validations.js`
2. Add parameter definitions for new validations (see examples below)
3. Copy to `datak9-studio.html` replacing the `validationLibrary` object
4. Test all validations in Studio UI

## Parameter Definition Format

Each validation needs parameter definitions for the UI form builder.

**Example: RangeCheck**

```javascript
params: [
    {
        name: 'field',
        label: 'Field Name',
        type: 'text',
        required: true,
        help: 'Field to validate'
    },
    {
        name: 'min_value',
        label: 'Minimum Value',
        type: 'number',
        required: false,
        help: 'Minimum acceptable value'
    },
    {
        name: 'max_value',
        label: 'Maximum Value',
        type: 'number',
        required: false,
        help: 'Maximum acceptable value'
    }
]
```

**Parameter Types:**
- `text` - Text input
- `number` - Number input
- `checkbox` - Boolean checkbox
- `textarea` - Multi-line text
- `select` - Dropdown (requires `options` array)

## Finding Parameter Definitions

Check the Python validation class for parameters:

```bash
# Find validation class
grep -r "class ConditionalValidation" validation_framework/

# Read the class to understand parameters
cat validation_framework/validations/conditional.py
```

Look for:
- `__init__` method parameters
- `validate()` method usage
- Class docstrings
- Examples in `docs/VALIDATION_CATALOG.md`

## Automation Strategy

### Current (Semi-Automated)

```
Python Registry → Sync Script → generated_validations.js → Manual Param Entry → Studio
```

**Pros:**
- Works with existing code
- No changes to validator
- Full control over UI

**Cons:**
- Still requires manual parameter definition
- Parameters must be maintained separately

### Future (Fully Automated)

**Option A: Python Decorators**

Add metadata decorators to validation classes:

```python
@validation_metadata(
    icon='📊',
    category='Statistical',
    params=[
        ParamDef('field', 'Field Name', 'text', required=True),
        ParamDef('threshold', 'Threshold', 'number', required=False)
    ]
)
class StatisticalOutlierCheck(DataValidationRule):
    ...
```

**Option B: JSON Schema**

Create `validation_definitions.json` as single source of truth:

```json
{
  "StatisticalOutlierCheck": {
    "category": "Statistical",
    "icon": "📊",
    "description": "Detects statistical outliers",
    "params": [
      {
        "name": "field",
        "type": "text",
        "required": true,
        "label": "Field Name"
      }
    ]
  }
}
```

Both Python and JavaScript read from this file.

**Option C: OpenAPI/JSON Schema**

Use standard schema formats that can be validated and shared.

## Recommended Workflow

### Weekly Sync (Maintenance)

```bash
# 1. Run sync script
python3 scripts/sync_validations.py

# 2. Check for new validations
git diff scripts/generated_validations.js

# 3. If new validations found:
#    - Add parameter definitions
#    - Update Studio
#    - Test in UI
#    - Commit changes
```

### New Validation Checklist

When adding a new validation to the framework:

- [ ] Create Python validation class
- [ ] Add to registry (auto-registers)
- [ ] Add unit tests
- [ ] Document in VALIDATION_CATALOG.md
- [ ] Run `sync_validations.py`
- [ ] Add parameter definitions to generated JS
- [ ] Update Studio validationLibrary
- [ ] Test in Studio UI

## Missing Validations (as of Nov 2025)

These 12 validations are in the Python validator but missing from Studio UI:

1. AdvancedAnomalyDetectionCheck - ML-based anomaly detection
2. BaselineComparisonCheck - Compare against baseline
3. ConditionalValidation - If-then-else logic
4. CorrelationCheck - Statistical correlation
5. DatabaseConstraintCheck - Database constraints
6. DatabaseReferentialIntegrityCheck - Foreign keys
7. DistributionCheck - Data distribution
8. InlineBusinessRuleCheck - Custom business rules
9. InlineLookupCheck - Inline lookups
10. InlineRegexCheck - Inline regex
11. SQLCustomCheck - Custom SQL
12. TrendDetectionCheck - Time-series trends

## Long-Term Goal

**Single Source of Truth:**

```
validation_definitions.json
    ↓
    ├→ Python: Auto-generate base classes
    ├→ JavaScript: Auto-generate Studio UI
    ├→ Docs: Auto-generate catalog
    └→ CLI: Auto-generate help text
```

All validation metadata lives in one place, everything else is generated.

## Author

Daniel Edge
November 15, 2025
