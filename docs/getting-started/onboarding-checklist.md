# DataK9 Onboarding Checklist

Step-by-step checklist to get up and running with DataK9.

---

## Phase 1: Installation

- [ ] **Clone the repository**
  ```bash
  git clone https://github.com/danieledge/datak9.git
  cd datak9/data-validation-tool
  ```

- [ ] **Install dependencies**
  ```bash
  pip install -r requirements.txt
  ```

- [ ] **Verify installation**
  ```bash
  python3 -m validation_framework.cli --version
  ```
  Expected: `python -m validation_framework.cli, version 0.1.0`

- [ ] **List available validations**
  ```bash
  python3 -m validation_framework.cli list-validations
  ```
  Expected: 37 validation types listed

---

## Phase 2: First Profile

- [ ] **Profile a data file**
  ```bash
  python3 -m validation_framework.cli profile your_data.csv -o profile_report.html
  ```

- [ ] **Open the HTML report** in your browser

- [ ] **Review the profile report**
  - Data quality summary
  - Column statistics
  - PII detection warnings
  - Suggested validations

- [ ] **Generate a validation config** (optional)
  ```bash
  python3 -m validation_framework.cli profile your_data.csv -c generated_config.yaml
  ```

---

## Phase 3: First Validation

- [ ] **Create a validation config** (`my_validation.yaml`)
  ```yaml
  validation_job:
    name: "My First Validation"

  files:
    - name: "my_data"
      path: "your_data.csv"
      format: "csv"
      validations:
        - type: "EmptyFileCheck"
          severity: "ERROR"
        - type: "MandatoryFieldCheck"
          severity: "ERROR"
          params:
            fields: ["id", "name"]
  ```

- [ ] **Run the validation**
  ```bash
  python3 -m validation_framework.cli validate my_validation.yaml
  ```

- [ ] **Review results** in terminal output

- [ ] **Generate HTML report**
  ```bash
  python3 -m validation_framework.cli validate my_validation.yaml -o report.html
  ```

- [ ] **Generate JSON output** (for CI/CD)
  ```bash
  python3 -m validation_framework.cli validate my_validation.yaml -j results.json
  ```

---

## Phase 4: Customize Validations

- [ ] **Review validation catalog**
  See [Validation Reference](../reference/validation-reference.md) for all 37 types

- [ ] **Add field validations**
  ```yaml
  - type: "RegexCheck"
    params:
      field: "email"
      pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

  - type: "RangeCheck"
    params:
      field: "age"
      min_value: 0
      max_value: 120
  ```

- [ ] **Add conditional validations**
  ```yaml
  - type: "MandatoryFieldCheck"
    params:
      fields: ["company_name"]
    condition: "account_type == 'BUSINESS'"
  ```

- [ ] **Test with your real data**

---

## Phase 5: Integration (Optional)

- [ ] **CI/CD Integration**
  - Use exit codes (0=pass, 1=fail)
  - Parse JSON output for detailed results
  - See [CI/CD Integration Guide](../using-datak9/cicd-integration.md)

- [ ] **AutoSys Integration**
  - See [AutoSys Integration Guide](../using-datak9/autosys-integration.md)

- [ ] **Database Validation**
  - See [Database Quick Start](../guides/database/DATABASE_QUICKSTART.md)

---

## Phase 6: Advanced Features (Optional)

- [ ] **Try DataK9 Studio** (Visual IDE)
  - Open `datak9-studio.html` in browser
  - Build configs visually
  - See [Studio Guide](../using-datak9/studio-guide.md)

- [ ] **Enable ML Analysis** in profiler
  ```bash
  python3 -m validation_framework.cli profile data.csv --beta-ml
  ```

- [ ] **Cross-file validation**
  - See [Cross-File Validation Guide](../guides/advanced/CROSS_FILE_VALIDATION_QUICK_REFERENCE.md)

- [ ] **CDA Gap Analysis** (regulatory compliance)
  - See [CDA Gap Analysis Guide](../guides/advanced/CDA_GAP_ANALYSIS_GUIDE.md)

---

## Quick Reference

| Task | Command |
|------|---------|
| Profile data | `python3 -m validation_framework.cli profile data.csv` |
| Validate data | `python3 -m validation_framework.cli validate config.yaml` |
| List validations | `python3 -m validation_framework.cli list-validations` |
| Generate sample config | `python3 -m validation_framework.cli init-config` |
| CDA analysis | `python3 -m validation_framework.cli cda-analysis config.yaml` |

---

## Getting Help

- [FAQ](../using-datak9/faq.md)
- [Troubleshooting](../using-datak9/troubleshooting.md)
- [Full Documentation](../README.md)
- [GitHub Issues](https://github.com/danieledge/datak9/issues)

---

**Completed the checklist? You're ready to guard your data pipelines with DataK9!**
