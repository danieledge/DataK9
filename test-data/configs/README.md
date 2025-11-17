# DataK9 Test Configurations

Pre-built validation configurations for testing and demonstration purposes.

## Available Configs

### comprehensive_test_config.yaml
**Purpose**: Test 25+ validation types on e-commerce dataset
**Dataset**: E-commerce transactions (100K rows, 18 MB)
**Validations**: 25+ types across all categories
**Use Case**: Quick comprehensive test of framework capabilities

```bash
python3 -m validation_framework.cli validate test-data/configs/comprehensive_test_config.yaml
```

---

### comprehensive_large_test_config.yaml
**Purpose**: Test 25+ validations on large banking dataset
**Dataset**: HI-Large (179M rows, 16 GB CSV / 5.1 GB Parquet)
**Validations**: 25+ types including performance-optimized checks
**Use Case**: Large-scale validation testing, performance benchmarks

```bash
python3 -m validation_framework.cli validate test-data/configs/comprehensive_large_test_config.yaml
```

---

### optimized_validations_test.yaml
**Purpose**: Performance testing of 3 optimized validations
**Dataset**: HI-Large (179M rows)
**Optimizations**:
- StatisticalOutlierCheck: Smart IQR sampling (20x faster)
- UniqueKeyCheck: Bloom filter + 10M hash table (8x faster)
- DuplicateRowCheck: Bloom filter + 10M hash table (12x faster)
**Expected Time**: ~4 minutes (vs 48 minutes before optimization)

```bash
python3 -m validation_framework.cli validate test-data/configs/optimized_validations_test.yaml
```

---

### ultimate_validation_showcase.yaml
**Purpose**: Ultimate test of all 33 validation types
**Dataset**: HI-Large + LI-Large (357M rows, 10.1 GB Parquet)
**Validations**: All 33 types (excluding SQL)
**Cross-file**: 5 validations testing referential integrity
**Use Case**: Complete framework capability demonstration

```bash
python3 -m validation_framework.cli validate test-data/configs/ultimate_validation_showcase.yaml
```

---

## Usage Notes

### File Paths
All configs use **relative paths** assuming you run from the `data-validation-tool` directory:

```bash
cd data-validation-tool
python3 -m validation_framework.cli validate test-data/configs/{config_name}.yaml
```

### Dataset Requirements

| Config | Dataset | Required Download |
|--------|---------|-------------------|
| comprehensive_test_config.yaml | E-commerce (100K) | No - included |
| comprehensive_large_test_config.yaml | HI-Large (179M) | **Yes - Kaggle** |
| optimized_validations_test.yaml | HI-Large (179M) | **Yes - Kaggle** |
| ultimate_validation_showcase.yaml | HI-Large + LI-Large (357M) | **Yes - Kaggle** |

See `test-data/README.md` for Kaggle download instructions.

### Modifying Configs

All configs can be edited to:
- Change chunk size (memory/speed tradeoff)
- Enable/disable specific validations
- Adjust severity levels
- Modify parameter values
- Change output paths

### Output

Each config generates:
- **HTML Report**: Interactive visualization with charts
- **JSON Summary**: Machine-readable results for CI/CD

Example:
```yaml
output:
  html_report: "validation_report.html"
  json_summary: "validation_summary.json"
```

---

**Questions?**
See the main `test-data/README.md` for dataset information and download instructions.

🐕 **DataK9 - Your K9 Guardian for Data Quality** 🐕
