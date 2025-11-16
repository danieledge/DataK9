# Creating Custom Reporters

**Extending DataK9 with Custom Report Formats**

DataK9 generates beautiful HTML reports and machine-readable JSON summaries by default. But sometimes you need custom output formats - PDF reports for executives, Slack notifications for teams, database logs for auditing, or integrations with monitoring systems.

This guide shows you how to create custom reporters so DataK9 can communicate validation results in any format you need.

---

## Table of Contents

1. [Quick Start: 30-Second Example](#quick-start-30-second-example)
2. [Understanding the Reporter Interface](#understanding-the-reporter-interface)
3. [Step-by-Step Tutorial](#step-by-step-tutorial)
4. [Testing Custom Reporters](#testing-custom-reporters)
5. [Real-World Examples](#real-world-examples)
6. [Best Practices](#best-practices)
7. [Next Steps](#next-steps)

---

## Quick Start: 30-Second Example

**Want CSV reports?** Here's a minimal reporter:

```python
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport
import pandas as pd

class CSVReporter(Reporter):
    """Export validation results to CSV"""

    def generate(self, report: ValidationReport, output_path: str):
        """Generate CSV report"""
        rows = []

        for file_report in report.file_reports:
            for validation in file_report.validations:
                rows.append({
                    'file': file_report.file_name,
                    'validation': validation.rule_name,
                    'severity': validation.severity.value,
                    'status': 'PASS' if validation.passed else 'FAIL',
                    'failures': validation.failed_count
                })

        df = pd.DataFrame(rows)
        df.to_csv(output_path, index=False)
```

**Use it:**
```python
from validation_framework.core.engine import ValidationEngine
from my_reporters import CSVReporter

# Run validation
engine = ValidationEngine.from_config("config.yaml")
report = engine.run()

# Generate CSV report
csv_reporter = CSVReporter()
csv_reporter.generate(report, "validation_results.csv")
```

That's it! Now you have validation results in CSV format.

---

## Understanding the Reporter Interface

All reporters implement a simple interface. Understanding this contract is essential.

### The Base Class

```python
from abc import ABC, abstractmethod
from validation_framework.core.results import ValidationReport

class Reporter(ABC):
    """
    Base class for all reporters in DataK9.

    Reporters transform ValidationReport objects into
    various output formats (HTML, JSON, PDF, etc.).
    """

    @abstractmethod
    def generate(self, report: ValidationReport, output_path: str):
        """
        Generate report from validation results.

        Args:
            report: ValidationReport object with all results
            output_path: Where to save the generated report

        The report object contains:
        - report.job_name: Name of validation job
        - report.overall_status: PASSED, FAILED, or WARNING
        - report.total_errors: Count of ERROR severity failures
        - report.total_warnings: Count of WARNING severity failures
        - report.execution_time: When validation ran
        - report.duration_seconds: How long it took
        - report.file_reports: List of FileValidationReport objects

        Each FileValidationReport contains:
        - file_name: Name of the file
        - file_path: Path to the file
        - status: File-level status
        - validations: List of ValidationResult objects
        - metadata: File metadata (row count, columns, etc.)

        Each ValidationResult contains:
        - rule_name: Name of validation rule
        - severity: ERROR or WARNING
        - passed: True/False
        - message: Result message
        - failed_count: Number of failures
        - total_count: Total rows checked
        - sample_failures: List of failure examples
        """
        pass
```

### What Your Reporter Must Do

1. **Implement `generate()` method**
   - Accept ValidationReport and output_path
   - Extract needed data from report object
   - Format data appropriately
   - Write to output_path (or send to external system)
   - Handle errors gracefully

2. **No return value needed**
   - Just write output or send data
   - Log success/failure
   - Raise exceptions for critical errors

### Available Data in ValidationReport

```python
# Job-level information
report.job_name              # "Customer Data Validation"
report.description           # "Daily customer data quality check"
report.overall_status        # Status.PASSED, Status.FAILED, Status.WARNING
report.execution_time        # datetime object
report.duration_seconds      # 12.5
report.total_errors          # 3
report.total_warnings        # 7

# File-level information
for file_report in report.file_reports:
    file_report.file_name           # "customers.csv"
    file_report.file_path           # "/data/customers.csv"
    file_report.file_format         # "csv"
    file_report.status              # Status.FAILED
    file_report.error_count         # 2
    file_report.warning_count       # 5
    file_report.execution_time      # 8.3 seconds

    # Validation-level information
    for validation in file_report.validations:
        validation.rule_name            # "Email Format Check"
        validation.severity             # Severity.ERROR
        validation.passed               # False
        validation.message              # "Found 15 invalid emails"
        validation.failed_count         # 15
        validation.total_count          # 10000
        validation.execution_time       # 2.1 seconds
        validation.sample_failures      # List of example failures

        # Sample failure structure
        for failure in validation.sample_failures[:5]:
            failure['row']      # 42
            failure['field']    # "email"
            failure['value']    # "invalid-email"
            failure['message']  # "Invalid email format"

    # File metadata
    file_report.metadata['row_count']       # 10000
    file_report.metadata['columns']         # ['id', 'name', 'email']
    file_report.metadata['column_count']    # 3
```

---

## Step-by-Step Tutorial

Let's build a production-grade **Markdown Reporter** that generates GitHub-flavored markdown reports perfect for version control and documentation.

### Step 1: Create the Reporter File

Create `validation_framework/reporters/markdown_reporter.py`:

```python
"""
Markdown reporter for DataK9.

Generates GitHub-flavored markdown reports that can be
committed to version control, viewed in GitHub, or converted
to other formats.

File: validation_framework/reporters/markdown_reporter.py
Author: Daniel Edge
"""
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport, Status, Severity
from typing import List
import logging

logger = logging.getLogger(__name__)


class MarkdownReporter(Reporter):
    """
    Generate markdown validation reports.

    DataK9 uses this reporter to create readable, version-controllable
    documentation of validation results. Perfect for:
    - Committing to Git alongside data files
    - Viewing in GitHub/GitLab
    - Converting to PDF or HTML with pandoc
    - Documentation and audit trails

    Features:
    - GitHub-flavored markdown syntax
    - Colored emoji indicators (✅ ❌ ⚠️)
    - Expandable details sections
    - Summary tables
    - Sample failures
    - Execution metrics
    """

    def generate(self, report: ValidationReport, output_path: str):
        """
        Generate markdown report.

        Args:
            report: ValidationReport object
            output_path: Path for output .md file
        """
        try:
            logger.info(f"🐕 DataK9 generating Markdown report: {output_path}")

            # Build markdown content
            md_lines = []

            # Header
            md_lines.extend(self._generate_header(report))

            # Executive summary
            md_lines.extend(self._generate_summary(report))

            # File reports
            for file_report in report.file_reports:
                md_lines.extend(self._generate_file_section(file_report))

            # Footer
            md_lines.extend(self._generate_footer(report))

            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(md_lines))

            logger.info(f"✅ Markdown report generated: {output_path}")

        except Exception as e:
            logger.error(f"Error generating markdown report: {str(e)}")
            raise

    def _generate_header(self, report: ValidationReport) -> List[str]:
        """Generate report header."""
        status_emoji = {
            Status.PASSED: "✅",
            Status.FAILED: "❌",
            Status.WARNING: "⚠️"
        }

        emoji = status_emoji.get(report.overall_status, "❓")

        return [
            f"# {emoji} {report.job_name}",
            "",
            f"> **Status:** {report.overall_status.value}  ",
            f"> **Executed:** {report.execution_time.strftime('%Y-%m-%d %H:%M:%S')}  ",
            f"> **Duration:** {report.duration_seconds:.2f} seconds  ",
            f"> **Errors:** {report.total_errors}  ",
            f"> **Warnings:** {report.total_warnings}  ",
            "",
            "---",
            ""
        ]

    def _generate_summary(self, report: ValidationReport) -> List[str]:
        """Generate executive summary table."""
        lines = [
            "## 📊 Summary",
            "",
            "| File | Status | Errors | Warnings | Row Count |",
            "|------|--------|--------|----------|-----------|"
        ]

        for file_report in report.file_reports:
            status_emoji = "✅" if file_report.status == Status.PASSED else "❌"
            row_count = file_report.metadata.get('row_count', 'N/A')

            lines.append(
                f"| {file_report.file_name} | {status_emoji} {file_report.status.value} | "
                f"{file_report.error_count} | {file_report.warning_count} | {row_count} |"
            )

        lines.extend(["", "---", ""])
        return lines

    def _generate_file_section(self, file_report) -> List[str]:
        """Generate section for one file."""
        lines = [
            f"## 📄 {file_report.file_name}",
            "",
            f"**Path:** `{file_report.file_path}`  ",
            f"**Format:** {file_report.file_format}  ",
            f"**Rows:** {file_report.metadata.get('row_count', 'N/A')}  ",
            f"**Columns:** {file_report.metadata.get('column_count', 'N/A')}  ",
            f"**Status:** {file_report.status.value}  ",
            "",
            "### Validation Results",
            ""
        ]

        # Validation results table
        lines.extend([
            "| Rule | Severity | Status | Failures | Total | Time |",
            "|------|----------|--------|----------|-------|------|"
        ])

        for validation in file_report.validations:
            status_icon = "✅" if validation.passed else (
                "❌" if validation.severity == Severity.ERROR else "⚠️"
            )

            status_text = "PASS" if validation.passed else "FAIL"

            lines.append(
                f"| {validation.rule_name} | {validation.severity.value} | "
                f"{status_icon} {status_text} | {validation.failed_count} | "
                f"{validation.total_count} | {validation.execution_time:.2f}s |"
            )

        lines.append("")

        # Failed validations details
        failed_validations = [v for v in file_report.validations if not v.passed]

        if failed_validations:
            lines.extend(["### ❌ Failed Validations", ""])

            for validation in failed_validations:
                lines.extend(self._generate_validation_details(validation))

        lines.extend(["---", ""])
        return lines

    def _generate_validation_details(self, validation) -> List[str]:
        """Generate detailed section for a failed validation."""
        severity_emoji = "❌" if validation.severity == Severity.ERROR else "⚠️"

        lines = [
            f"#### {severity_emoji} {validation.rule_name}",
            "",
            f"**Message:** {validation.message}  ",
            f"**Severity:** {validation.severity.value}  ",
            f"**Failures:** {validation.failed_count} / {validation.total_count}  ",
            ""
        ]

        # Sample failures
        if validation.sample_failures:
            lines.extend([
                "<details>",
                "<summary>📋 Sample Failures (click to expand)</summary>",
                "",
                "```",
            ])

            # Show up to 10 sample failures
            for i, failure in enumerate(validation.sample_failures[:10], 1):
                if isinstance(failure, dict):
                    lines.append(f"Failure {i}:")
                    for key, value in failure.items():
                        lines.append(f"  {key}: {value}")
                    lines.append("")
                else:
                    lines.append(f"{i}. {failure}")

            lines.extend([
                "```",
                "</details>",
                ""
            ])

        return lines

    def _generate_footer(self, report: ValidationReport) -> List[str]:
        """Generate report footer."""
        return [
            "---",
            "",
            "## 🐕 DataK9 Report",
            "",
            f"Generated by **DataK9** - Your K9 guardian for data quality  ",
            f"Execution Time: {report.execution_time.strftime('%Y-%m-%d %H:%M:%S')}  ",
            f"Duration: {report.duration_seconds:.2f} seconds  ",
            "",
            "*This report was automatically generated by DataK9 validation framework.*",
            ""
        ]
```

### Step 2: Use the Reporter

```python
from validation_framework.core.engine import ValidationEngine
from validation_framework.reporters.markdown_reporter import MarkdownReporter

# Run validation
engine = ValidationEngine.from_config("config.yaml")
report = engine.run()

# Generate markdown report
md_reporter = MarkdownReporter()
md_reporter.generate(report, "validation_report.md")
```

### Step 3: Example Output

The reporter generates markdown like this:

```markdown
# ✅ Customer Data Validation

> **Status:** PASSED
> **Executed:** 2024-01-15 14:30:22
> **Duration:** 12.34 seconds
> **Errors:** 0
> **Warnings:** 2

---

## 📊 Summary

| File | Status | Errors | Warnings | Row Count |
|------|--------|--------|----------|-----------|
| customers.csv | ✅ PASSED | 0 | 2 | 10000 |

---

## 📄 customers.csv

**Path:** `/data/customers.csv`
**Format:** csv
**Rows:** 10000
**Columns:** 5
**Status:** PASSED

### Validation Results

| Rule | Severity | Status | Failures | Total | Time |
|------|----------|--------|----------|-------|------|
| Email Format Check | ERROR | ✅ PASS | 0 | 10000 | 2.34s |
| Age Range Check | WARNING | ⚠️ FAIL | 15 | 10000 | 1.12s |

### ❌ Failed Validations

#### ⚠️ Age Range Check

**Message:** Found 15 ages outside expected range
**Severity:** WARNING
**Failures:** 15 / 10000

<details>
<summary>📋 Sample Failures (click to expand)</summary>

```
Failure 1:
  row: 42
  field: age
  value: 150
  message: Age 150 exceeds maximum of 120
```
</details>
```

### Step 4: Commit Reports to Git

```bash
# Add markdown reports to version control
git add validation_report.md
git commit -m "Add daily validation report for 2024-01-15"
git push

# View in GitHub - renders beautifully!
```

---

## Testing Custom Reporters

Testing reporters ensures they handle all edge cases correctly.

### Test Template

Create `tests/test_markdown_reporter.py`:

```python
"""
Tests for MarkdownReporter.

Tests markdown generation, formatting, and error handling.

Author: Daniel Edge
"""
import pytest
from pathlib import Path
from datetime import datetime
from validation_framework.reporters.markdown_reporter import MarkdownReporter
from validation_framework.core.results import (
    ValidationReport,
    FileValidationReport,
    ValidationResult,
    Status,
    Severity
)


@pytest.fixture
def sample_report():
    """Create sample ValidationReport for testing."""

    # Create validation results
    validation1 = ValidationResult(
        rule_name="Email Format Check",
        severity=Severity.ERROR,
        passed=True,
        message="All emails valid",
        failed_count=0,
        total_count=1000,
        execution_time=2.5
    )

    validation2 = ValidationResult(
        rule_name="Age Range Check",
        severity=Severity.WARNING,
        passed=False,
        message="Found 5 ages outside range",
        failed_count=5,
        total_count=1000,
        sample_failures=[
            {"row": 42, "field": "age", "value": 150, "message": "Age too high"},
            {"row": 99, "field": "age", "value": 5, "message": "Age too low"},
        ],
        execution_time=1.2
    )

    # Create file report
    file_report = FileValidationReport(
        file_name="test.csv",
        file_path="/data/test.csv",
        file_format="csv",
        status=Status.WARNING,
        validations=[validation1, validation2],
        metadata={
            "row_count": 1000,
            "column_count": 5,
            "columns": ["id", "name", "email", "age", "city"]
        },
        execution_time=3.7
    )

    # Create validation report
    report = ValidationReport(
        job_name="Test Validation Job",
        description="Test job description",
        execution_time=datetime(2024, 1, 15, 14, 30, 0),
        duration_seconds=3.7,
        overall_status=Status.WARNING,
        config={},
        file_reports=[file_report]
    )

    return report


class TestMarkdownReporter:
    """Tests for MarkdownReporter"""

    def test_generate_creates_file(self, sample_report, tmp_path):
        """Test that generate() creates markdown file."""
        output_path = tmp_path / "report.md"

        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        assert output_path.exists()
        assert output_path.stat().st_size > 0

    def test_report_contains_job_name(self, sample_report, tmp_path):
        """Test report includes job name."""
        output_path = tmp_path / "report.md"

        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        content = output_path.read_text()
        assert "Test Validation Job" in content

    def test_report_contains_status(self, sample_report, tmp_path):
        """Test report includes overall status."""
        output_path = tmp_path / "report.md"

        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        content = output_path.read_text()
        assert "WARNING" in content
        assert "⚠️" in content  # Warning emoji

    def test_report_contains_summary_table(self, sample_report, tmp_path):
        """Test report includes summary table."""
        output_path = tmp_path / "report.md"

        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        content = output_path.read_text()

        # Check for markdown table syntax
        assert "| File | Status | Errors | Warnings |" in content
        assert "|------|--------|--------|----------|" in content
        assert "test.csv" in content

    def test_report_contains_validation_results(self, sample_report, tmp_path):
        """Test report includes validation results."""
        output_path = tmp_path / "report.md"

        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        content = output_path.read_text()

        assert "Email Format Check" in content
        assert "Age Range Check" in content
        assert "✅ PASS" in content
        assert "⚠️ FAIL" in content

    def test_report_contains_sample_failures(self, sample_report, tmp_path):
        """Test report includes sample failures for failed validations."""
        output_path = tmp_path / "report.md"

        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        content = output_path.read_text()

        # Check for sample failures
        assert "Sample Failures" in content
        assert "row: 42" in content
        assert "Age too high" in content

    def test_empty_report(self, tmp_path):
        """Test handling of report with no files."""
        output_path = tmp_path / "report.md"

        # Create empty report
        report = ValidationReport(
            job_name="Empty Job",
            description="No files",
            execution_time=datetime.now(),
            duration_seconds=0.1,
            overall_status=Status.PASSED,
            config={},
            file_reports=[]
        )

        reporter = MarkdownReporter()
        reporter.generate(report, str(output_path))

        assert output_path.exists()
        content = output_path.read_text()
        assert "Empty Job" in content

    def test_multiple_files(self, tmp_path):
        """Test report with multiple files."""
        # Create report with multiple files
        file_reports = []

        for i in range(3):
            validation = ValidationResult(
                rule_name=f"Check {i}",
                severity=Severity.ERROR,
                passed=True,
                message="OK",
                failed_count=0,
                total_count=100,
                execution_time=0.5
            )

            file_report = FileValidationReport(
                file_name=f"file{i}.csv",
                file_path=f"/data/file{i}.csv",
                file_format="csv",
                status=Status.PASSED,
                validations=[validation],
                metadata={"row_count": 100},
                execution_time=0.5
            )

            file_reports.append(file_report)

        report = ValidationReport(
            job_name="Multi-File Job",
            description="Multiple files",
            execution_time=datetime.now(),
            duration_seconds=1.5,
            overall_status=Status.PASSED,
            config={},
            file_reports=file_reports
        )

        output_path = tmp_path / "report.md"
        reporter = MarkdownReporter()
        reporter.generate(report, str(output_path))

        content = output_path.read_text()

        # All files should be in report
        assert "file0.csv" in content
        assert "file1.csv" in content
        assert "file2.csv" in content

    def test_unicode_handling(self, sample_report, tmp_path):
        """Test handling of unicode characters."""
        # Add unicode to report
        sample_report.job_name = "Validación de Datos 数据验证"

        output_path = tmp_path / "report.md"
        reporter = MarkdownReporter()
        reporter.generate(sample_report, str(output_path))

        # Should not raise encoding errors
        content = output_path.read_text(encoding='utf-8')
        assert "Validación de Datos 数据验证" in content
```

### Run Tests

```bash
# Run reporter tests
pytest tests/test_markdown_reporter.py -v

# Run with coverage
pytest tests/test_markdown_reporter.py --cov=validation_framework.reporters

# Output:
# tests/test_markdown_reporter.py::TestMarkdownReporter::test_generate_creates_file PASSED
# tests/test_markdown_reporter.py::TestMarkdownReporter::test_report_contains_job_name PASSED
# ...
# ✅ All tests passed
```

---

## Real-World Examples

### Example 1: Slack Notification Reporter

Send validation results to Slack:

```python
"""
Slack reporter for DataK9.

Posts validation results to Slack channels for team visibility.

File: validation_framework/reporters/slack_reporter.py
Author: Daniel Edge
"""
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport, Status, Severity
import requests
import logging

logger = logging.getLogger(__name__)


class SlackReporter(Reporter):
    """
    Post validation results to Slack.

    DataK9 uses this to notify teams of validation results in real-time.

    Configuration:
        webhook_url (str): Slack webhook URL
        channel (str): Slack channel name
        mention_on_failure (str): User/group to mention on failures (@here, @channel)

    Example:
        reporter = SlackReporter(
            webhook_url="https://hooks.slack.com/services/...",
            channel="#data-quality",
            mention_on_failure="@data-team"
        )
    """

    def __init__(
        self,
        webhook_url: str,
        channel: str = "#data-quality",
        mention_on_failure: str = None
    ):
        """Initialize SlackReporter."""
        self.webhook_url = webhook_url
        self.channel = channel
        self.mention_on_failure = mention_on_failure

    def generate(self, report: ValidationReport, output_path: str = None):
        """
        Post validation results to Slack.

        Args:
            report: ValidationReport object
            output_path: Not used for Slack (optional)
        """
        try:
            logger.info("🐕 DataK9 posting results to Slack")

            # Build message
            message = self._build_message(report)

            # Post to Slack
            response = requests.post(
                self.webhook_url,
                json=message,
                timeout=10
            )

            response.raise_for_status()
            logger.info(f"✅ Posted to Slack: {self.channel}")

        except Exception as e:
            logger.error(f"Error posting to Slack: {str(e)}")
            raise

    def _build_message(self, report: ValidationReport) -> dict:
        """Build Slack message payload."""

        # Status emoji and color
        if report.overall_status == Status.PASSED:
            emoji = "✅"
            color = "good"  # Green
        elif report.overall_status == Status.FAILED:
            emoji = "❌"
            color = "danger"  # Red
        else:
            emoji = "⚠️"
            color = "warning"  # Yellow

        # Mention on failure
        mention = ""
        if report.overall_status == Status.FAILED and self.mention_on_failure:
            mention = f"{self.mention_on_failure} "

        # Build message
        message = {
            "channel": self.channel,
            "username": "DataK9",
            "icon_emoji": ":dog:",
            "text": f"{mention}{emoji} *{report.job_name}* - {report.overall_status.value}",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {
                            "title": "Status",
                            "value": report.overall_status.value,
                            "short": True
                        },
                        {
                            "title": "Duration",
                            "value": f"{report.duration_seconds:.2f}s",
                            "short": True
                        },
                        {
                            "title": "Errors",
                            "value": str(report.total_errors),
                            "short": True
                        },
                        {
                            "title": "Warnings",
                            "value": str(report.total_warnings),
                            "short": True
                        }
                    ],
                    "footer": "DataK9 - Your K9 guardian for data quality",
                    "ts": int(report.execution_time.timestamp())
                }
            ]
        }

        # Add file details
        if report.file_reports:
            file_summary = "\n".join([
                f"• {fr.file_name}: {fr.status.value} "
                f"({fr.error_count} errors, {fr.warning_count} warnings)"
                for fr in report.file_reports
            ])

            message["attachments"].append({
                "color": color,
                "title": "Files Validated",
                "text": file_summary,
                "mrkdwn_in": ["text"]
            })

        return message
```

**Usage:**

```python
from validation_framework.reporters.slack_reporter import SlackReporter

# Run validation
engine = ValidationEngine.from_config("config.yaml")
report = engine.run()

# Post to Slack
slack_reporter = SlackReporter(
    webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
    channel="#data-quality",
    mention_on_failure="@data-team"
)
slack_reporter.generate(report)
```

### Example 2: Database Audit Logger

Log validation results to database for trending and analysis:

```python
"""
Database audit logger for DataK9.

Stores validation results in database for historical tracking,
trending, and compliance auditing.

File: validation_framework/reporters/db_audit_reporter.py
Author: Daniel Edge
"""
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport, Status, Severity
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()


class ValidationAuditLog(Base):
    """Database model for validation audit logs."""
    __tablename__ = 'validation_audit_log'

    id = sa.Column(sa.Integer, primary_key=True)
    job_name = sa.Column(sa.String(255), nullable=False)
    execution_time = sa.Column(sa.DateTime, nullable=False)
    overall_status = sa.Column(sa.String(50), nullable=False)
    duration_seconds = sa.Column(sa.Float)
    total_errors = sa.Column(sa.Integer)
    total_warnings = sa.Column(sa.Integer)
    file_count = sa.Column(sa.Integer)
    config_json = sa.Column(sa.Text)
    created_at = sa.Column(sa.DateTime, default=datetime.utcnow)


class FileAuditLog(Base):
    """Database model for file-level audit logs."""
    __tablename__ = 'file_audit_log'

    id = sa.Column(sa.Integer, primary_key=True)
    validation_id = sa.Column(sa.Integer, sa.ForeignKey('validation_audit_log.id'))
    file_name = sa.Column(sa.String(500), nullable=False)
    file_path = sa.Column(sa.String(1000))
    file_format = sa.Column(sa.String(50))
    status = sa.Column(sa.String(50))
    row_count = sa.Column(sa.Integer)
    error_count = sa.Column(sa.Integer)
    warning_count = sa.Column(sa.Integer)
    execution_time = sa.Column(sa.Float)


class DatabaseAuditReporter(Reporter):
    """
    Store validation results in database.

    DataK9 uses this for historical tracking, SLA monitoring,
    and compliance auditing.

    Example:
        reporter = DatabaseAuditReporter(
            connection_string="postgresql://user:pass@localhost/audit_db"
        )
    """

    def __init__(self, connection_string: str):
        """Initialize database reporter."""
        self.engine = sa.create_engine(connection_string)

        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)

    def generate(self, report: ValidationReport, output_path: str = None):
        """
        Store validation results in database.

        Args:
            report: ValidationReport object
            output_path: Not used for database (optional)
        """
        try:
            logger.info("🐕 DataK9 storing results in database")

            # Create session
            Session = sa.orm.sessionmaker(bind=self.engine)
            session = Session()

            # Create validation audit record
            audit_log = ValidationAuditLog(
                job_name=report.job_name,
                execution_time=report.execution_time,
                overall_status=report.overall_status.value,
                duration_seconds=report.duration_seconds,
                total_errors=report.total_errors,
                total_warnings=report.total_warnings,
                file_count=len(report.file_reports),
                config_json=str(report.config)
            )

            session.add(audit_log)
            session.flush()  # Get the ID

            # Create file audit records
            for file_report in report.file_reports:
                file_log = FileAuditLog(
                    validation_id=audit_log.id,
                    file_name=file_report.file_name,
                    file_path=file_report.file_path,
                    file_format=file_report.file_format,
                    status=file_report.status.value,
                    row_count=file_report.metadata.get('row_count'),
                    error_count=file_report.error_count,
                    warning_count=file_report.warning_count,
                    execution_time=file_report.execution_time
                )
                session.add(file_log)

            # Commit transaction
            session.commit()
            logger.info(f"✅ Stored validation audit log ID: {audit_log.id}")

        except Exception as e:
            logger.error(f"Error storing audit log: {str(e)}")
            session.rollback()
            raise
        finally:
            session.close()
```

### Example 3: Email Reporter with Attachments

Send detailed email reports:

```python
"""
Email reporter for DataK9.

Sends validation results via email with HTML formatting
and optional PDF attachments.

File: validation_framework/reporters/email_reporter.py
Author: Daniel Edge
"""
from validation_framework.reporters.base import Reporter
from validation_framework.reporters.html_reporter import HTMLReporter
from validation_framework.core.results import ValidationReport, Status
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class EmailReporter(Reporter):
    """
    Send validation results via email.

    DataK9 uses this to notify stakeholders of validation results.

    Configuration:
        smtp_host (str): SMTP server hostname
        smtp_port (int): SMTP server port
        from_email (str): Sender email address
        to_emails (List[str]): Recipient email addresses
        subject_template (str): Email subject template
        attach_html (bool): Attach HTML report

    Example:
        reporter = EmailReporter(
            smtp_host="smtp.gmail.com",
            smtp_port=587,
            from_email="datak9@example.com",
            to_emails=["team@example.com"],
            subject_template="[DataK9] {job_name} - {status}"
        )
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        from_email: str,
        to_emails: list,
        username: str = None,
        password: str = None,
        subject_template: str = "[DataK9] {job_name} - {status}",
        attach_html: bool = True
    ):
        """Initialize email reporter."""
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_email = from_email
        self.to_emails = to_emails
        self.username = username or from_email
        self.password = password
        self.subject_template = subject_template
        self.attach_html = attach_html

    def generate(self, report: ValidationReport, output_path: str = None):
        """
        Send validation results via email.

        Args:
            report: ValidationReport object
            output_path: Optional path for HTML attachment
        """
        try:
            logger.info(f"🐕 DataK9 sending email to {', '.join(self.to_emails)}")

            # Build email message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = self.subject_template.format(
                job_name=report.job_name,
                status=report.overall_status.value
            )
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)

            # Plain text version
            text_body = self._build_text_body(report)
            msg.attach(MIMEText(text_body, 'plain'))

            # HTML version
            html_body = self._build_html_body(report)
            msg.attach(MIMEText(html_body, 'html'))

            # Attach HTML report if requested
            if self.attach_html and output_path:
                # Generate HTML report
                html_reporter = HTMLReporter()
                html_reporter.generate(report, output_path)

                # Attach file
                with open(output_path, 'rb') as f:
                    attachment = MIMEBase('application', 'octet-stream')
                    attachment.set_payload(f.read())

                encoders.encode_base64(attachment)
                attachment.add_header(
                    'Content-Disposition',
                    f'attachment; filename="{Path(output_path).name}"'
                )
                msg.attach(attachment)

            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                if self.password:
                    server.login(self.username, self.password)
                server.send_message(msg)

            logger.info("✅ Email sent successfully")

        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            raise

    def _build_text_body(self, report: ValidationReport) -> str:
        """Build plain text email body."""
        status_symbol = {
            Status.PASSED: "[PASS]",
            Status.FAILED: "[FAIL]",
            Status.WARNING: "[WARN]"
        }

        symbol = status_symbol.get(report.overall_status, "[????]")

        lines = [
            f"{symbol} {report.job_name}",
            "=" * 60,
            f"Status: {report.overall_status.value}",
            f"Executed: {report.execution_time}",
            f"Duration: {report.duration_seconds:.2f} seconds",
            f"Errors: {report.total_errors}",
            f"Warnings: {report.total_warnings}",
            "",
            "Files:",
            "-" * 60
        ]

        for file_report in report.file_reports:
            lines.append(
                f"  {file_report.file_name}: {file_report.status.value} "
                f"({file_report.error_count} errors, {file_report.warning_count} warnings)"
            )

        lines.extend([
            "",
            "---",
            "Generated by DataK9 - Your K9 guardian for data quality"
        ])

        return "\n".join(lines)

    def _build_html_body(self, report: ValidationReport) -> str:
        """Build HTML email body."""
        # Color based on status
        color = {
            Status.PASSED: "#28a745",  # Green
            Status.FAILED: "#dc3545",  # Red
            Status.WARNING: "#ffc107"  # Yellow
        }.get(report.overall_status, "#6c757d")

        # Build file rows
        file_rows = ""
        for fr in report.file_reports:
            file_rows += f"""
            <tr>
                <td>{fr.file_name}</td>
                <td>{fr.status.value}</td>
                <td>{fr.error_count}</td>
                <td>{fr.warning_count}</td>
            </tr>
            """

        return f"""
        <html>
          <body style="font-family: Arial, sans-serif;">
            <h2 style="color: {color};">
              {report.job_name} - {report.overall_status.value}
            </h2>

            <table style="border-collapse: collapse; margin-bottom: 20px;">
              <tr>
                <td style="padding: 5px;"><strong>Executed:</strong></td>
                <td style="padding: 5px;">{report.execution_time}</td>
              </tr>
              <tr>
                <td style="padding: 5px;"><strong>Duration:</strong></td>
                <td style="padding: 5px;">{report.duration_seconds:.2f} seconds</td>
              </tr>
              <tr>
                <td style="padding: 5px;"><strong>Errors:</strong></td>
                <td style="padding: 5px; color: red;">{report.total_errors}</td>
              </tr>
              <tr>
                <td style="padding: 5px;"><strong>Warnings:</strong></td>
                <td style="padding: 5px; color: orange;">{report.total_warnings}</td>
              </tr>
            </table>

            <h3>Files Validated:</h3>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
              <thead style="background-color: #f8f9fa;">
                <tr>
                  <th>File</th>
                  <th>Status</th>
                  <th>Errors</th>
                  <th>Warnings</th>
                </tr>
              </thead>
              <tbody>
                {file_rows}
              </tbody>
            </table>

            <p style="margin-top: 30px; color: #6c757d;">
              <small>Generated by DataK9 - Your K9 guardian for data quality</small>
            </p>
          </body>
        </html>
        """
```

---

## Best Practices

### 1. **Handle Missing Data Gracefully**

Reports may have incomplete data:

```python
def generate(self, report: ValidationReport, output_path: str):
    for file_report in report.file_reports:
        # ✅ GOOD: Handle missing metadata
        row_count = file_report.metadata.get('row_count', 'N/A')
        columns = file_report.metadata.get('columns', [])

        # ✅ GOOD: Handle empty validations list
        if not file_report.validations:
            logger.warning(f"No validations for {file_report.file_name}")
            continue
```

### 2. **Use Appropriate Output Formats**

Choose format based on audience:

```python
# Executives: High-level PDF summary
# Data Engineers: Detailed JSON for automation
# Business Users: Visual HTML reports
# DevOps: Slack/email notifications
# Auditors: Database logs with history
```

### 3. **Include Actionable Information**

Help users fix issues:

```python
# ❌ BAD: Vague message
"Validation failed"

# ✅ GOOD: Actionable message
"Found 15 invalid emails in customer.csv rows 42, 99, 156...
 Fix: Update email validation rules or correct source data"
```

### 4. **Respect Output Path Parameter**

Some reporters don't use output_path (Slack, email):

```python
def generate(self, report: ValidationReport, output_path: str = None):
    """
    Generate report.

    Args:
        output_path: Optional. Not used for Slack reporter.
    """
    # It's OK to ignore output_path for notification-style reporters
    pass
```

### 5. **Log Reporter Actions**

Help users debug issues:

```python
import logging

logger = logging.getLogger(__name__)

def generate(self, report: ValidationReport, output_path: str):
    logger.info(f"🐕 Generating {self.__class__.__name__}")
    logger.debug(f"Output path: {output_path}")

    # ... generation logic

    logger.info(f"✅ Report generated: {output_path}")
```

### 6. **Handle Encoding Properly**

Support international characters:

```python
def generate(self, report: ValidationReport, output_path: str):
    # ✅ Always specify encoding
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)
```

### 7. **Test with Real Reports**

Use actual validation results in tests:

```python
def test_with_real_report():
    """Test with actual validation results"""
    # Run real validation
    engine = ValidationEngine.from_config("test_config.yaml")
    report = engine.run()

    # Test reporter with real data
    reporter = MyReporter()
    reporter.generate(report, "output.txt")

    # Verify output
    assert Path("output.txt").exists()
```

---

## Next Steps

Now that you understand custom reporters, explore:

1. **[Custom Validations](custom-validations.md)** - Create domain-specific validation rules
2. **[Custom Loaders](custom-loaders.md)** - Add support for new file formats
3. **[API Reference](api-reference.md)** - Complete API documentation
4. **[Testing Guide](testing-guide.md)** - Comprehensive testing strategies

---

## Questions?

**Need help?** Check these resources:

- **Built-in Reporters**: `validation_framework/reporters/` for examples
- **HTML Reporter**: `validation_framework/reporters/html_reporter.py` for advanced template
- **JSON Reporter**: `validation_framework/reporters/json_reporter.py` for simple example
- **Results Classes**: `validation_framework/core/results.py` for data structures

**Found a bug?** Report it on [GitHub Issues](https://github.com/danieledge/data-validation-tool/issues)

**Want to contribute?** See **[Contributing Guide](contributing.md)**

---

**🐕 Make DataK9's bark heard everywhere - your guardian reports in any format you need**
