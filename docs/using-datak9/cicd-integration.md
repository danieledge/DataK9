# CI/CD Integration Guide

**Integrate DataK9 with Modern CI/CD Pipelines**

DataK9 integrates seamlessly with Jenkins, GitLab CI, GitHub Actions, and other CI/CD tools. This guide shows you how to implement data validation as part of your continuous integration and deployment workflows.

---

## Table of Contents

1. [Overview](#overview)
2. [GitHub Actions](#github-actions)
3. [GitLab CI](#gitlab-ci)
4. [Jenkins](#jenkins)
5. [Azure Pipelines](#azure-pipelines)
6. [CircleCI](#circleci)
7. [Best Practices](#best-practices)
8. [Artifacts and Reporting](#artifacts-and-reporting)
9. [Troubleshooting](#troubleshooting)

---

## Overview

### Why DataK9 in CI/CD?

**Use Cases:**

✅ **Data Pipeline Testing** - Validate sample data before deployment
✅ **Schema Validation** - Ensure schema changes don't break validations
✅ **Quality Gates** - Block deployments if data quality issues found
✅ **Automated Testing** - Test validation configs in pull requests
✅ **Configuration Validation** - Ensure YAML configs are valid

### Integration Pattern

```
Code Push → CI Pipeline Triggered
    ↓
Install DataK9
    ↓
Run Validations on Test Data
    ↓
Generate Reports
    ↓
Check Exit Code
    ├─ Exit 0: ✅ Pass → Deploy
    └─ Exit 1: ❌ Fail → Block, Alert
```

---

## GitHub Actions

### Basic Workflow

**.github/workflows/data-validation.yml:**

```yaml
name: DataK9 Validation

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  # Run daily at 2 AM
  schedule:
    - cron: '0 2 * * *'

jobs:
  validate-data:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install DataK9
        run: |
          pip install --upgrade pip
          pip install -r requirements.txt
          pip install -e .

      - name: Run DataK9 Validation
        run: |
          python3 -m validation_framework.cli validate configs/test_data.yaml

      - name: Upload Validation Report
        if: always()  # Upload even if validation fails
        uses: actions/upload-artifact@v3
        with:
          name: validation-report
          path: validation_report.html

      - name: Upload JSON Summary
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: validation-summary
          path: validation_summary.json
```

### Advanced: Matrix Testing

**Test Multiple Configurations:**

```yaml
name: DataK9 Matrix Validation

on: [push, pull_request]

jobs:
  validate:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        config:
          - customers
          - orders
          - products
          - inventory
      fail-fast: false  # Continue even if one fails

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install DataK9
        run: |
          pip install -r requirements.txt
          pip install -e .

      - name: Validate ${{ matrix.config }}
        run: |
          python3 -m validation_framework.cli validate \
            configs/${{ matrix.config }}.yaml

      - name: Upload Report
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: ${{ matrix.config }}-report
          path: validation_report.html
```

### With Slack Notifications

```yaml
name: DataK9 Validation with Slack

on: [push]

jobs:
  validate:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install DataK9
        run: pip install -r requirements.txt

      - name: Run Validation
        id: validation
        run: |
          python3 -m validation_framework.cli validate config.yaml
        continue-on-error: true

      - name: Notify Slack on Success
        if: steps.validation.outcome == 'success'
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK }}
          payload: |
            {
              "text": "✅ DataK9 Validation Passed",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "*DataK9 Validation Passed* :white_check_mark:\nBranch: ${{ github.ref }}\nCommit: ${{ github.sha }}"
                  }
                }
              ]
            }

      - name: Notify Slack on Failure
        if: steps.validation.outcome == 'failure'
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK }}
          payload: |
            {
              "text": "❌ DataK9 Validation Failed",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "*DataK9 Validation Failed* :x:\nBranch: ${{ github.ref }}\nCommit: ${{ github.sha }}\n<${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}|View Logs>"
                  }
                }
              ]
            }

      - name: Fail if validation failed
        if: steps.validation.outcome == 'failure'
        run: exit 1
```

---

## GitLab CI

### Basic Pipeline

**.gitlab-ci.yml:**

```yaml
image: python:3.9

stages:
  - validate
  - deploy

variables:
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.cache/pip"

cache:
  paths:
    - .cache/pip

before_script:
  - pip install --upgrade pip
  - pip install -r requirements.txt
  - pip install -e .

validate_data:
  stage: validate
  script:
    - echo "🐕 DataK9 Validation Started"
    - python3 -m validation_framework.cli validate configs/data_validation.yaml
    - echo "✅ Validation Passed"
  artifacts:
    when: always
    paths:
      - validation_report.html
      - validation_summary.json
    expire_in: 30 days
  only:
    - main
    - develop
    - merge_requests

deploy_pipeline:
  stage: deploy
  script:
    - echo "Deploying data pipeline..."
    - ./deploy.sh
  dependencies:
    - validate_data
  only:
    - main
```

### Parallel Validation

```yaml
stages:
  - validate
  - report
  - deploy

.validate_template: &validate_config
  stage: validate
  script:
    - python3 -m validation_framework.cli validate configs/$CONFIG_FILE.yaml
  artifacts:
    when: always
    paths:
      - ${CONFIG_FILE}_report.html
      - ${CONFIG_FILE}_summary.json

validate_customers:
  <<: *validate_config
  variables:
    CONFIG_FILE: customers

validate_orders:
  <<: *validate_config
  variables:
    CONFIG_FILE: orders

validate_products:
  <<: *validate_config
  variables:
    CONFIG_FILE: products

aggregate_reports:
  stage: report
  script:
    - python3 scripts/aggregate_reports.py
  artifacts:
    paths:
      - combined_report.html
  dependencies:
    - validate_customers
    - validate_orders
    - validate_products
```

### With Code Quality Integration

```yaml
validate_data_quality:
  stage: validate
  script:
    - python3 -m validation_framework.cli validate config.yaml --json validation_summary.json
    - python3 scripts/convert_to_codequality.py validation_summary.json > codequality.json
  artifacts:
    reports:
      codequality: codequality.json
    paths:
      - validation_report.html
```

**scripts/convert_to_codequality.py:**

```python
"""Convert DataK9 JSON to GitLab Code Quality format"""
import json
import sys

with open(sys.argv[1]) as f:
    datak9_report = json.load(f)

code_quality = []

for file_report in datak9_report.get('files', []):
    for validation in file_report.get('validations', []):
        if not validation['passed']:
            code_quality.append({
                "description": validation['message'],
                "severity": "major" if validation['severity'] == "ERROR" else "minor",
                "location": {
                    "path": file_report['file_path'],
                    "lines": {
                        "begin": 1
                    }
                }
            })

print(json.dumps(code_quality, indent=2))
```

---

## Jenkins

### Declarative Pipeline

**Jenkinsfile:**

```groovy
pipeline {
    agent any

    environment {
        VENV_DIR = "${WORKSPACE}/venv"
    }

    stages {
        stage('Setup') {
            steps {
                echo '🐕 Setting up DataK9 environment'
                sh '''
                    python3 -m venv ${VENV_DIR}
                    . ${VENV_DIR}/bin/activate
                    pip install --upgrade pip
                    pip install -r requirements.txt
                    pip install -e .
                '''
            }
        }

        stage('Validate Data') {
            steps {
                echo '🐕 Running DataK9 validation'
                sh '''
                    . ${VENV_DIR}/bin/activate
                    python3 -m validation_framework.cli validate configs/data_validation.yaml
                '''
            }
        }

        stage('Archive Reports') {
            steps {
                archiveArtifacts artifacts: 'validation_report.html, validation_summary.json',
                                 allowEmptyArchive: false
            }
        }
    }

    post {
        always {
            publishHTML([
                allowMissing: false,
                alwaysLinkToLastBuild: true,
                keepAll: true,
                reportDir: '.',
                reportFiles: 'validation_report.html',
                reportName: 'DataK9 Validation Report'
            ])
        }

        failure {
            emailext (
                subject: "❌ DataK9 Validation Failed - ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                body: """
                    DataK9 validation failed.

                    Job: ${env.JOB_NAME}
                    Build: ${env.BUILD_NUMBER}
                    URL: ${env.BUILD_URL}

                    Check the validation report: ${env.BUILD_URL}DataK9_Validation_Report/
                """,
                to: 'data-quality-team@company.com'
            )
        }

        success {
            echo '✅ DataK9 validation passed'
        }
    }
}
```

### Scripted Pipeline with Multiple Files

```groovy
node {
    def configs = ['customers', 'orders', 'products', 'inventory']
    def results = [:]

    stage('Setup') {
        checkout scm

        sh '''
            python3 -m venv venv
            . venv/bin/activate
            pip install -r requirements.txt
            pip install -e .
        '''
    }

    stage('Validate') {
        // Run validations in parallel
        def stepsForParallel = configs.collectEntries {
            ["Validate ${it}" : transformIntoStep(it)]
        }

        parallel stepsForParallel
    }

    stage('Report') {
        publishHTML([
            allowMissing: false,
            alwaysLinkToLastBuild: true,
            keepAll: true,
            reportDir: 'reports',
            reportFiles: '*.html',
            reportName: 'DataK9 Reports'
        ])
    }
}

def transformIntoStep(configName) {
    return {
        stage("Validate ${configName}") {
            sh """
                . venv/bin/activate
                python3 -m validation_framework.cli validate configs/${configName}.yaml
                mv validation_report.html reports/${configName}_report.html
            """
        }
    }
}
```

---

## Azure Pipelines

**azure-pipelines.yml:**

```yaml
trigger:
  - main
  - develop

pool:
  vmImage: 'ubuntu-latest'

variables:
  PYTHON_VERSION: '3.9'

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '$(PYTHON_VERSION)'
    displayName: 'Use Python $(PYTHON_VERSION)'

  - script: |
      python -m pip install --upgrade pip
      pip install -r requirements.txt
      pip install -e .
    displayName: 'Install DataK9'

  - script: |
      python3 -m validation_framework.cli validate configs/data_validation.yaml
    displayName: '🐕 Run DataK9 Validation'

  - task: PublishBuildArtifacts@1
    condition: always()
    inputs:
      PathtoPublish: 'validation_report.html'
      ArtifactName: 'validation-report'
      publishLocation: 'Container'
    displayName: 'Publish Validation Report'

  - task: PublishBuildArtifacts@1
    condition: always()
    inputs:
      PathtoPublish: 'validation_summary.json'
      ArtifactName: 'validation-summary'
      publishLocation: 'Container'
    displayName: 'Publish JSON Summary'

  - script: |
      python3 scripts/check_validation_result.py validation_summary.json
    displayName: 'Check Validation Result'
```

**scripts/check_validation_result.py:**

```python
"""Check validation results and fail pipeline if needed"""
import json
import sys

with open(sys.argv[1]) as f:
    report = json.load(f)

if report['overall_status'] == 'FAILED':
    print(f"❌ Validation failed: {report['total_errors']} errors")
    sys.exit(1)
elif report['overall_status'] == 'WARNING':
    print(f"⚠️ Validation passed with warnings: {report['total_warnings']} warnings")
    sys.exit(0)
else:
    print(f"✅ Validation passed")
    sys.exit(0)
```

---

## CircleCI

**.circleci/config.yml:**

```yaml
version: 2.1

jobs:
  validate:
    docker:
      - image: cimg/python:3.9
    steps:
      - checkout

      - restore_cache:
          keys:
            - v1-dependencies-{{ checksum "requirements.txt" }}
            - v1-dependencies-

      - run:
          name: Install DataK9
          command: |
            python3 -m venv venv
            . venv/bin/activate
            pip install --upgrade pip
            pip install -r requirements.txt
            pip install -e .

      - save_cache:
          paths:
            - ./venv
          key: v1-dependencies-{{ checksum "requirements.txt" }}

      - run:
          name: Run DataK9 Validation
          command: |
            . venv/bin/activate
            python3 -m validation_framework.cli validate configs/data_validation.yaml

      - store_artifacts:
          path: validation_report.html
          destination: reports/validation_report.html

      - store_artifacts:
          path: validation_summary.json
          destination: reports/validation_summary.json

workflows:
  version: 2
  validate-and-deploy:
    jobs:
      - validate
      - deploy:
          requires:
            - validate
          filters:
            branches:
              only: main
```

---

## Best Practices

### 1. Use Virtual Environments

**Always isolate dependencies:**

```yaml
# Good
- run: |
    python3 -m venv venv
    . venv/bin/activate
    pip install -r requirements.txt

# Bad (installs globally)
- run: pip install -r requirements.txt
```

### 2. Cache Dependencies

**Speed up builds:**

```yaml
# GitHub Actions
- uses: actions/cache@v3
  with:
    path: ~/.cache/pip
    key: ${{ runner.os }}-pip-${{ hashFiles('**/requirements.txt') }}

# GitLab CI
cache:
  paths:
    - .cache/pip
```

### 3. Run on Pull Requests

**Catch issues before merge:**

```yaml
on:
  pull_request:
    branches: [ main ]
    paths:
      - 'configs/**'
      - 'data/**'
      - '.github/workflows/validation.yml'
```

### 4. Parallel Execution

**Validate multiple files faster:**

```yaml
strategy:
  matrix:
    config: [customers, orders, products]
  fail-fast: false
```

### 5. Always Upload Reports

**Even on failure:**

```yaml
- name: Upload Report
  if: always()  # Run even if validation fails
  uses: actions/upload-artifact@v3
```

### 6. Set Timeouts

**Prevent hanging builds:**

```yaml
jobs:
  validate:
    timeout-minutes: 30  # Kill if runs > 30 min
```

### 7. Use Environments

**Different configs per environment:**

```yaml
jobs:
  validate-dev:
    environment: development
    steps:
      - run: python3 -m validation_framework.cli validate configs/dev.yaml

  validate-prod:
    environment: production
    steps:
      - run: python3 -m validation_framework.cli validate configs/prod.yaml
```

### 8. Version Pin Dependencies

**Ensure reproducible builds:**

```
# requirements.txt
pandas==2.0.0
pyyaml==6.0
validation-framework==1.53
```

---

## Artifacts and Reporting

### Artifact Strategy

**What to Save:**

1. **HTML Reports** - Human-readable validation results
2. **JSON Summaries** - Machine-readable results
3. **Logs** - Detailed execution logs
4. **Failed Data Samples** - For investigation

**Retention:**

```yaml
artifacts:
  when: always
  paths:
    - validation_report.html
    - validation_summary.json
    - validation.log
  expire_in: 30 days  # Auto-cleanup old artifacts
```

### Report Integration

**GitHub Actions - Job Summary:**

```yaml
- name: Create Job Summary
  if: always()
  run: |
    echo "## 🐕 DataK9 Validation Results" >> $GITHUB_STEP_SUMMARY
    echo "" >> $GITHUB_STEP_SUMMARY
    python3 scripts/generate_summary.py >> $GITHUB_STEP_SUMMARY
```

**scripts/generate_summary.py:**

```python
import json

with open('validation_summary.json') as f:
    report = json.load(f)

status_emoji = {
    'PASSED': '✅',
    'FAILED': '❌',
    'WARNING': '⚠️'
}

print(f"**Status:** {status_emoji[report['overall_status']]} {report['overall_status']}")
print(f"**Errors:** {report['total_errors']}")
print(f"**Warnings:** {report['total_warnings']}")
print(f"**Duration:** {report['duration_seconds']:.2f}s")
print()

print("### Files Validated")
print()
print("| File | Status | Errors | Warnings |")
print("|------|--------|--------|----------|")

for file_report in report['files']:
    status = status_emoji.get(file_report['status'], '❓')
    print(f"| {file_report['file_name']} | {status} {file_report['status']} | {file_report['error_count']} | {file_report['warning_count']} |")
```

---

## Troubleshooting

### Issue 1: Pipeline Fails but Validation Passes Locally

**Cause:** Environment differences

**Fix:**
```yaml
# Pin Python version
- uses: actions/setup-python@v4
  with:
    python-version: '3.9'  # Match local version

# Pin dependencies
pip install -r requirements-lock.txt
```

### Issue 2: Artifacts Not Uploaded

**Cause:** Path mismatch

**Fix:**
```yaml
# Check actual output paths
- run: ls -la

# Use correct paths
- uses: actions/upload-artifact@v3
  with:
    path: ./validation_report.html  # Use relative path
```

### Issue 3: Timeout on Large Files

**Cause:** Large file validation in CI

**Fix:**
```yaml
# Use sample data for CI
- run: |
    python3 -m validation_framework.cli profile large_file.parquet --sample-rows 100000
    python3 -m validation_framework.cli validate config_sample.yaml
```

---

## Next Steps

**You've learned CI/CD integration! Now:**

1. **[AutoSys Integration](autosys-integration.md)** - Enterprise job schedulers
2. **[Best Practices](best-practices.md)** - Production deployment
3. **[Monitoring](monitoring.md)** - Track validation metrics
4. **[Troubleshooting](troubleshooting.md)** - Solve common issues

---

**🐕 Continuous validation, continuous quality - DataK9 guards every commit**
