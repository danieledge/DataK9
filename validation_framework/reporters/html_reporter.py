"""
HTML report generator.

Generates comprehensive, interactive HTML reports with:
- Executive summary
- Per-file validation results
- Interactive tables
- Visual charts
- Collapsible sections
- Export capabilities
"""

from pathlib import Path
from jinja2 import Template
from validation_framework.reporters.base import Reporter
from validation_framework.core.results import ValidationReport, Status


class HTMLReporter(Reporter):
    """
    Generates rich HTML validation reports.

    The HTML report includes:
    - Executive summary with overall status
    - Detailed validation results per file
    - Sample failures with context
    - Statistics and charts
    - Interactive filtering and sorting
    """

    def generate(self, report: ValidationReport, output_path: str, cda_report=None):
        """
        Generate HTML report from validation results.

        Args:
            report: ValidationReport with validation results
            output_path: Path where HTML file should be written
            cda_report: Optional CDA gap analysis report

        Raises:
            IOError: If unable to write report file
        """
        try:
            # Prepare template data
            template_data = self._prepare_template_data(report, cda_report)

            # Render HTML
            html_content = self._render_html(template_data)

            # Ensure output directory exists
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Write HTML file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"HTML report generated: {output_path}")

        except Exception as e:
            raise IOError(f"Error generating HTML report: {str(e)}")

    def _prepare_template_data(self, report: ValidationReport, cda_report=None) -> dict:
        """
        Prepare data for template rendering.

        Args:
            report: ValidationReport to extract data from
            cda_report: Optional CDA gap analysis report

        Returns:
            Dictionary with template data
        """
        # Calculate summary statistics
        total_validations = sum(f.total_validations for f in report.file_reports)
        passed_validations = total_validations - report.total_errors - report.total_warnings

        return {
            "report": report,
            "total_validations": total_validations,
            "passed_validations": passed_validations,
            "Status": Status,
            "cda_report": cda_report,
        }

    def _render_html(self, template_data: dict) -> str:
        """
        Render HTML using embedded template.

        Args:
            template_data: Data to pass to template

        Returns:
            Rendered HTML string
        """
        template = Template(HTML_TEMPLATE)
        return template.render(**template_data)


# Embedded HTML template with modern dark theme
# Self-contained with inline CSS and JavaScript - mobile responsive
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ report.job_name }} - Validation Report</title>
    <style>
        /* Modern Dark Theme - Mobile Responsive */
        :root {
            --bg-primary: #1a1b26;
            --bg-secondary: #24283b;
            --bg-tertiary: #2f3549;
            --text-primary: #c0caf5;
            --text-secondary: #9aa5ce;
            --text-muted: #565f89;
            --success: #9ece6a;
            --error: #f7768e;
            --warning: #e0af68;
            --info: #7aa2f7;
            --border: #3b4261;
            --shadow: rgba(0, 0, 0, 0.3);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            min-height: 100vh;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 1rem;
        }

        @media (min-width: 768px) {
            .container {
                padding: 2rem;
            }
        }

        /* Header */
        header {
            background: linear-gradient(135deg, #1f2335 0%, #24283b 50%, #1a1b26 100%);
            color: white;
            padding: 0;
            border-radius: 12px;
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px var(--shadow);
            border: 1px solid var(--border);
            overflow: hidden;
        }

        .header-banner {
            background: linear-gradient(135deg, #7aa2f7 0%, #bb9af7 100%);
            padding: 0.5rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }

        .header-brand {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .header-icon {
            font-size: 2.5rem;
            animation: pulse-icon 2s infinite;
        }

        @keyframes pulse-icon {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.1); }
        }

        .header-badge {
            background: rgba(255, 255, 255, 0.2);
            backdrop-filter: blur(10px);
            padding: 0.5rem 1rem;
            border-radius: 20px;
            font-size: 0.875rem;
            font-weight: 600;
            border: 1px solid rgba(255, 255, 255, 0.3);
        }

        .header-content {
            padding: 2rem;
        }

        header h1 {
            font-size: clamp(1.5rem, 4vw, 2.5rem);
            font-weight: 700;
            margin-bottom: 0.5rem;
            background: linear-gradient(135deg, #7aa2f7 0%, #bb9af7 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        header .subtitle {
            color: var(--text-secondary);
            font-size: clamp(0.875rem, 2vw, 1.125rem);
            margin-bottom: 1.5rem;
        }

        header .description {
            color: var(--text-muted);
            font-size: 0.938rem;
            line-height: 1.6;
            max-width: 800px;
        }

        /* Status Badge */
        .status-badge {
            display: inline-flex;
            align-items: center;
            padding: 0.5rem 1rem;
            border-radius: 8px;
            font-weight: 600;
            font-size: 0.875rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            gap: 0.5rem;
        }

        .status-badge::before {
            content: '';
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: block;
        }

        .status-passed {
            background: rgba(158, 206, 106, 0.15);
            color: var(--success);
            border: 1px solid var(--success);
        }

        .status-passed::before {
            background: var(--success);
        }

        .status-failed {
            background: rgba(247, 118, 142, 0.15);
            color: var(--error);
            border: 1px solid var(--error);
        }

        .status-failed::before {
            background: var(--error);
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .status-warning {
            background: rgba(224, 175, 104, 0.15);
            color: var(--warning);
            border: 1px solid var(--warning);
        }

        .status-warning::before {
            background: var(--warning);
        }

        /* Summary Grid */
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .summary-card {
            background: var(--bg-secondary);
            padding: 1.5rem;
            border-radius: 12px;
            border: 1px solid var(--border);
            box-shadow: 0 4px 16px var(--shadow);
            transition: transform 0.2s, box-shadow 0.2s;
        }

        .summary-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 24px var(--shadow);
        }

        .summary-card h3 {
            color: var(--text-secondary);
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 0.75rem;
            font-weight: 600;
        }

        .summary-card .value {
            font-size: 2.5rem;
            font-weight: 700;
            line-height: 1;
        }

        .summary-card.highlight-passed .value {
            color: var(--success);
        }

        .summary-card.highlight-failed .value {
            color: var(--error);
        }

        .summary-card.highlight-warning .value {
            color: var(--warning);
        }

        /* File Section */
        .file-section {
            background: var(--bg-secondary);
            margin-bottom: 1.5rem;
            border-radius: 12px;
            border: 1px solid var(--border);
            overflow: hidden;
            box-shadow: 0 4px 16px var(--shadow);
        }

        .file-header {
            padding: 1.5rem;
            background: var(--bg-tertiary);
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
            border-bottom: 1px solid var(--border);
            transition: background 0.2s;
        }

        .file-header:hover {
            background: #363b52;
        }

        .file-header h2 {
            font-size: clamp(1.125rem, 3vw, 1.5rem);
            display: flex;
            align-items: center;
            gap: 1rem;
            flex: 1;
            min-width: 200px;
        }

        .file-stats {
            display: flex;
            gap: 1.5rem;
            font-size: 0.875rem;
            flex-wrap: wrap;
        }

        .stat-item {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .stat-label {
            color: var(--text-muted);
        }

        .stat-value {
            font-weight: 600;
        }

        .stat-value.error {
            color: var(--error);
        }

        .stat-value.warning {
            color: var(--warning);
        }

        .toggle-icon {
            transition: transform 0.3s;
            color: var(--text-secondary);
        }

        .toggle-icon.rotated {
            transform: rotate(180deg);
        }

        /* File Content */
        .file-content {
            padding: 1.5rem;
        }

        .file-meta {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
            padding: 1rem;
            background: var(--bg-primary);
            border-radius: 8px;
            border: 1px solid var(--border);
        }

        .meta-item {
            display: flex;
            flex-direction: column;
            gap: 0.25rem;
        }

        .meta-label {
            font-size: 0.75rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .meta-value {
            font-weight: 600;
            color: var(--text-primary);
            word-break: break-word;
        }

        /* Validation Item */
        .validation-list {
            margin-top: 1.5rem;
        }

        .validation-list > h3 {
            font-size: 1.25rem;
            margin-bottom: 1rem;
            color: var(--text-primary);
        }

        .validation-item {
            border: 1px solid var(--border);
            border-radius: 8px;
            margin-bottom: 1rem;
            overflow: hidden;
            background: var(--bg-primary);
        }

        .validation-item.failed {
            border-color: var(--error);
        }

        .validation-header {
            padding: 1rem;
            background: var(--bg-tertiary);
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 1rem;
            flex-wrap: wrap;
            transition: background 0.2s;
        }

        .validation-header:hover {
            background: #363b52;
        }

        .validation-title {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            flex: 1;
            min-width: 200px;
        }

        .validation-icon {
            font-size: 1.5rem;
        }

        .validation-name {
            font-size: 1rem;
            font-weight: 600;
        }

        .severity-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }

        .severity-error {
            background: rgba(247, 118, 142, 0.15);
            color: var(--error);
            border: 1px solid var(--error);
        }

        .severity-warning {
            background: rgba(224, 175, 104, 0.15);
            color: var(--warning);
            border: 1px solid var(--warning);
        }

        .severity-success {
            background: rgba(158, 206, 106, 0.15);
            color: var(--success);
            border: 1px solid var(--success);
        }

        .validation-stats {
            display: flex;
            gap: 1rem;
            font-size: 0.875rem;
            align-items: center;
            flex-wrap: wrap;
        }

        .validation-details {
            padding: 1.5rem;
            background: var(--bg-secondary);
            border-top: 1px solid var(--border);
            display: none;
        }

        .validation-details.show {
            display: block;
        }

        .validation-message {
            padding: 1rem;
            background: var(--bg-primary);
            border-left: 3px solid var(--info);
            border-radius: 4px;
            margin-bottom: 1.5rem;
            font-size: 0.938rem;
        }

        .validation-message strong {
            color: var(--info);
        }

        /* Failures Table */
        .failures-section {
            margin-top: 1.5rem;
        }

        .failures-section h4 {
            font-size: 1rem;
            margin-bottom: 1rem;
            color: var(--error);
        }

        .failures-table-wrapper {
            overflow-x: auto;
            border-radius: 8px;
            border: 1px solid var(--border);
        }

        .failures-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
        }

        .failures-table th {
            background: var(--bg-tertiary);
            padding: 0.75rem;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid var(--border);
            color: var(--text-primary);
            white-space: nowrap;
        }

        .failures-table td {
            padding: 0.75rem;
            border-bottom: 1px solid var(--border);
            color: var(--text-secondary);
        }

        .failures-table tr:hover {
            background: var(--bg-primary);
        }

        .failures-table tr:last-child td {
            border-bottom: none;
        }

        .code {
            background: var(--bg-tertiary);
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
            font-family: 'Fira Code', 'Courier New', monospace;
            font-size: 0.875em;
            color: var(--info);
            border: 1px solid var(--border);
        }

        .error-message {
            color: var(--error);
            font-size: 0.875rem;
        }

        /* Mobile Optimizations */
        @media (max-width: 768px) {
            .file-header,
            .validation-header {
                flex-direction: column;
                align-items: flex-start;
            }

            .file-stats,
            .validation-stats {
                width: 100%;
                justify-content: space-between;
            }

            .summary-grid {
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            }

            .file-meta {
                grid-template-columns: 1fr;
            }

            .failures-table {
                font-size: 0.75rem;
            }

            .failures-table th,
            .failures-table td {
                padding: 0.5rem;
            }
        }

        /* Print Styles */
        @media print {
            body {
                background: white;
                color: black;
            }

            .file-header,
            .validation-header {
                cursor: default;
            }

            .validation-details {
                display: block !important;
            }

            .toggle-icon {
                display: none;
            }
        }

        /* Loading Animation */
        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        .file-section,
        .summary-card {
            animation: fadeIn 0.5s ease-out;
        }

        /* Charts Section */
        .charts-section {
            background: var(--bg-secondary);
            padding: 2rem;
            border-radius: 12px;
            border: 1px solid var(--border);
            margin-bottom: 2rem;
            box-shadow: 0 4px 16px var(--shadow);
        }

        .charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 2rem;
            margin-top: 1.5rem;
        }

        .chart-container {
            background: var(--bg-primary);
            padding: 1.5rem;
            border-radius: 8px;
            border: 1px solid var(--border);
        }

        .chart-container h3 {
            font-size: 1rem;
            margin-bottom: 1rem;
            color: var(--text-primary);
            text-align: center;
        }

        .chart-wrapper {
            position: relative;
            height: 250px;
        }

        canvas {
            max-width: 100%;
            height: auto !important;
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header>
            <div class="header-banner">
                <div class="header-brand">
                    <span class="header-icon">🔍</span>
                    <span style="font-weight: 600; font-size: 1.125rem;">Data Quality Validation</span>
                </div>
                <div class="header-badge">
                    Report Generated
                </div>
            </div>
            <div class="header-content">
                <h1>{{ report.job_name }}</h1>
                <div class="subtitle">
                    🕐 {{ report.execution_time.strftime('%Y-%m-%d %H:%M:%S') }}
                    | ⏱️ Duration: {{ "%.2f"|format(report.duration_seconds) }}s
                    | 📁 {{ report.file_reports|length }} file{{ 's' if report.file_reports|length != 1 else '' }} processed
                </div>
                {% if report.description %}
                <div class="description">{{ report.description }}</div>
                {% endif %}
            </div>
        </header>

        <!-- Summary Section -->
        <div class="summary-grid">
            <div class="summary-card {% if report.overall_status == Status.PASSED %}highlight-passed{% elif report.overall_status == Status.FAILED %}highlight-failed{% else %}highlight-warning{% endif %}">
                <h3>Overall Status</h3>
                <div class="value">
                    <span class="status-badge {% if report.overall_status == Status.PASSED %}status-passed{% elif report.overall_status == Status.FAILED %}status-failed{% else %}status-warning{% endif %}">
                        {{ report.overall_status.value }}
                    </span>
                </div>
            </div>

            <div class="summary-card">
                <h3>Total Validations</h3>
                <div class="value">{{ total_validations }}</div>
            </div>

            <div class="summary-card highlight-failed">
                <h3>❌ Errors</h3>
                <div class="value">{{ report.total_errors }}</div>
            </div>

            <div class="summary-card highlight-warning">
                <h3>⚠️ Warnings</h3>
                <div class="value">{{ report.total_warnings }}</div>
            </div>

            <div class="summary-card highlight-passed">
                <h3>✅ Passed</h3>
                <div class="value">{{ passed_validations }}</div>
            </div>

            <div class="summary-card">
                <h3>📁 Files</h3>
                <div class="value">{{ report.file_reports|length }}</div>
            </div>
        </div>

        {% if cda_report %}
        <!-- CDA Coverage Section -->
        <div style="background: var(--bg-secondary); padding: 1.5rem; border-radius: 8px; border: 1px solid var(--border); margin-bottom: 2rem;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; flex-wrap: wrap; gap: 1rem;">
                <h3 style="color: var(--text-primary); margin: 0;">🛡️ Critical Data Attribute Coverage</h3>
                {% if cda_report.has_tier1_gaps %}
                <span style="background: var(--error); color: white; padding: 0.375rem 0.75rem; border-radius: 4px; font-size: 0.813rem; font-weight: 600;">
                    ⚠️ TIER_1 GAPS DETECTED
                </span>
                {% elif cda_report.total_gaps > 0 %}
                <span style="background: var(--warning); color: #1a1b26; padding: 0.375rem 0.75rem; border-radius: 4px; font-size: 0.813rem; font-weight: 600;">
                    ⚠️ Coverage Gaps
                </span>
                {% else %}
                <span style="background: var(--success); color: #1a1b26; padding: 0.375rem 0.75rem; border-radius: 4px; font-size: 0.813rem; font-weight: 600;">
                    ✓ Full Coverage
                </span>
                {% endif %}
            </div>

            <!-- CDA Summary Metrics -->
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 1rem; margin-bottom: 1.5rem;">
                <div style="text-align: center; padding: 1rem; background: var(--bg-primary); border-radius: 6px;">
                    <div style="font-size: 1.5rem; font-weight: 700; color: var(--info);">{{ cda_report.total_cdas }}</div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">Total CDAs</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: var(--bg-primary); border-radius: 6px;">
                    <div style="font-size: 1.5rem; font-weight: 700; color: var(--success);">{{ cda_report.total_covered }}</div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">Covered</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: var(--bg-primary); border-radius: 6px;">
                    <div style="font-size: 1.5rem; font-weight: 700; color: {% if cda_report.total_gaps > 0 %}var(--error){% else %}var(--success){% endif %};">{{ cda_report.total_gaps }}</div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">Gaps</div>
                </div>
                <div style="text-align: center; padding: 1rem; background: var(--bg-primary); border-radius: 6px;">
                    <div style="font-size: 1.5rem; font-weight: 700; color: {% if cda_report.overall_coverage >= 90 %}var(--success){% elif cda_report.overall_coverage >= 70 %}var(--warning){% else %}var(--error){% endif %};">{{ "%.0f"|format(cda_report.overall_coverage) }}%</div>
                    <div style="font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;">Coverage</div>
                </div>
            </div>

            <!-- Per-File CDA Coverage -->
            {% for file_result in cda_report.file_results %}
            <div style="margin-bottom: 1rem; padding: 1rem; background: var(--bg-primary); border-radius: 6px; border-left: 3px solid {% if file_result.gap_cdas > 0 %}var(--warning){% else %}var(--success){% endif %};">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                    <strong style="color: var(--text-primary);">{{ file_result.file_name }}</strong>
                    <span style="font-size: 0.875rem; color: {% if file_result.gap_cdas > 0 %}var(--warning){% else %}var(--success){% endif %};">
                        {{ file_result.covered_cdas }}/{{ file_result.total_cdas }} covered
                    </span>
                </div>

                {% if file_result.field_coverage %}
                <div style="display: flex; flex-wrap: wrap; gap: 0.5rem;">
                    {% for field_cov in file_result.field_coverage %}
                    <span style="display: inline-flex; align-items: center; gap: 0.25rem; padding: 0.25rem 0.5rem; background: var(--bg-secondary); border-radius: 4px; font-size: 0.75rem; border: 1px solid {% if field_cov.is_covered %}var(--success){% else %}var(--error){% endif %};">
                        {% if field_cov.is_covered %}
                        <span style="color: var(--success);">✓</span>
                        {% else %}
                        <span style="color: var(--error);">✗</span>
                        {% endif %}
                        <span style="color: var(--text-secondary);">{{ field_cov.cda.field }}</span>
                        <span style="color: var(--text-muted); font-size: 0.688rem;">{{ field_cov.cda.tier.value }}</span>
                    </span>
                    {% endfor %}
                </div>
                {% endif %}
            </div>
            {% endfor %}

            <p style="margin: 0; color: var(--text-muted); font-size: 0.813rem; font-style: italic;">
                CDAs are Critical Data Attributes requiring validation coverage for regulatory compliance. Run <code style="background: var(--bg-tertiary); padding: 0.125rem 0.375rem; border-radius: 3px;">cda-analysis</code> for detailed gap report.
            </p>
        </div>
        {% endif %}

        <!-- Status Legend -->
        <div style="background: var(--bg-secondary); padding: 1.5rem; border-radius: 8px; border: 1px solid var(--border); margin-bottom: 2rem;">
            <h3 style="color: var(--text-primary); margin-bottom: 1rem;">📋 Status Definitions</h3>
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 1rem;">
                <div style="padding: 0.75rem; background: var(--bg-primary); border-radius: 6px; border-left: 3px solid var(--error);">
                    <strong style="color: var(--error);">FAILED</strong>
                    <p style="margin: 0.5rem 0 0 0; color: var(--text-muted); font-size: 0.875rem;">
                        One or more ERROR-severity checks failed. Critical data quality issues detected that should prevent data load.
                    </p>
                </div>
                <div style="padding: 0.75rem; background: var(--bg-primary); border-radius: 6px; border-left: 3px solid var(--warning);">
                    <strong style="color: var(--warning);">WARNING</strong>
                    <p style="margin: 0.5rem 0 0 0; color: var(--text-muted); font-size: 0.875rem;">
                        All ERROR checks passed, but one or more WARNING-severity checks failed. Data may be loaded with caution.
                    </p>
                </div>
                <div style="padding: 0.75rem; background: var(--bg-primary); border-radius: 6px; border-left: 3px solid var(--success);">
                    <strong style="color: var(--success);">PASSED</strong>
                    <p style="margin: 0.5rem 0 0 0; color: var(--text-muted); font-size: 0.875rem;">
                        All validation checks passed successfully. Data meets all quality standards and is ready for load.
                    </p>
                </div>
            </div>
        </div>

        <!-- Charts & Visualizations -->
        <div class="charts-section">
            <h2 style="color: var(--text-primary); margin-bottom: 0.5rem;">📊 Validation Insights</h2>
            <p style="color: var(--text-muted); font-size: 0.938rem; margin-bottom: 1.5rem;">
                Visual overview of validation results and data quality metrics
            </p>
            <div class="charts-grid">
                <div class="chart-container">
                    <h3>Validation Results Distribution</h3>
                    <div class="chart-wrapper">
                        <canvas id="resultsChart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <h3>Results by Severity</h3>
                    <div class="chart-wrapper">
                        <canvas id="severityChart"></canvas>
                    </div>
                </div>
                <div class="chart-container">
                    <h3>Files Validation Status</h3>
                    <div class="chart-wrapper">
                        <canvas id="filesChart"></canvas>
                    </div>
                </div>
            </div>
        </div>

        <!-- File Sections -->
        {% for file_report in report.file_reports %}
        <div class="file-section">
            <div class="file-header" onclick="toggleFile('file-{{ loop.index }}')">
                <h2>
                    <span class="status-badge {% if file_report.status == Status.PASSED %}status-passed{% elif file_report.status == Status.FAILED %}status-failed{% else %}status-warning{% endif %}">
                        {{ file_report.status.value }}
                    </span>
                    {{ file_report.file_name }}
                </h2>
                <div class="file-stats">
                    <div class="stat-item">
                        <span class="stat-label">Checks:</span>
                        <span class="stat-value">{{ file_report.total_validations }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Errors:</span>
                        <span class="stat-value error">{{ file_report.error_count }}</span>
                    </div>
                    <div class="stat-item">
                        <span class="stat-label">Warnings:</span>
                        <span class="stat-value warning">{{ file_report.warning_count }}</span>
                    </div>
                    <span class="toggle-icon" id="toggle-file-{{ loop.index }}">▼</span>
                </div>
            </div>

            <div class="file-content" id="file-{{ loop.index }}" style="display: block;">
                <!-- File Metadata -->
                <div class="file-meta">
                    <div class="meta-item">
                        <span class="meta-label">File Path</span>
                        <span class="meta-value">{{ file_report.file_path }}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Format</span>
                        <span class="meta-value">{{ file_report.file_format|upper }}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">File Size</span>
                        <span class="meta-value">
                            {% if file_report.metadata.file_size_mb %}
                                {{ "%.2f"|format(file_report.metadata.file_size_mb) }} MB
                            {% else %}
                                N/A
                            {% endif %}
                        </span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Rows</span>
                        <span class="meta-value">
                            {% if file_report.metadata.total_rows %}
                                {{ "{:,}".format(file_report.metadata.total_rows) }}
                            {% elif file_report.metadata.estimated_rows %}
                                ~{{ "{:,}".format(file_report.metadata.estimated_rows) }}
                            {% else %}
                                N/A
                            {% endif %}
                        </span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Columns</span>
                        <span class="meta-value">{{ file_report.metadata.column_count if file_report.metadata.column_count else 'N/A' }}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Duration</span>
                        <span class="meta-value">{{ "%.2f"|format(file_report.execution_time) }}s</span>
                    </div>
                </div>

                <!-- Validations -->
                <div class="validation-list">
                    <h3>🔍 Validation Results</h3>
                    {% for result in file_report.validation_results %}
                    <div class="validation-item {% if not result.passed %}failed{% endif %}">
                        <div class="validation-header" onclick="toggleValidation('validation-{{ file_report.file_name }}-{{ loop.index }}')">
                            <div class="validation-title">
                                <span class="validation-icon">
                                    {% if result.passed %}✅{% else %}❌{% endif %}
                                </span>
                                <div>
                                    <div class="validation-name">{{ result.rule_name }}</div>
                                    <span class="severity-badge {% if result.passed %}severity-success{% else %}{% if result.severity.value == 'ERROR' %}severity-error{% else %}severity-warning{% endif %}{% endif %}">
                                        {% if result.passed %}PASSED{% else %}{{ result.severity.value }}{% endif %}
                                    </span>
                                </div>
                            </div>
                            <div class="validation-stats">
                                {% if not result.passed %}
                                    <span style="color: var(--error);">{{ result.failed_count }} failures</span>
                                {% endif %}
                                {% if result.total_count > 0 %}
                                    <span style="color: var(--text-muted);">{{ "%.1f"|format((result.total_count - result.failed_count) / result.total_count * 100) }}% pass rate</span>
                                {% endif %}
                                <span class="toggle-icon" id="toggle-validation-{{ file_report.file_name }}-{{ loop.index }}">▼</span>
                            </div>
                        </div>

                        <div class="validation-details" id="validation-{{ file_report.file_name }}-{{ loop.index }}">
                            <div class="validation-message">
                                <strong>Result:</strong> {{ result.message }}
                            </div>

                            {% if not result.passed and result.sample_failures %}
                                <div class="failures-section">
                                    <h4>❌ Sample Failures (showing {{ result.sample_failures|length }} of {{ result.failed_count }})</h4>
                                    <div class="failures-table-wrapper">
                                        <table class="failures-table">
                                            <thead>
                                                <tr>
                                                    <th>Row</th>
                                                    {% if result.sample_failures[0].field %}
                                                        <th>Field</th>
                                                    {% endif %}
                                                    {% if result.sample_failures[0].value %}
                                                        <th>Value</th>
                                                    {% endif %}
                                                    <th>Issue</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {% for failure in result.sample_failures %}
                                                <tr>
                                                    <td><span class="code">#{{ failure.row }}</span></td>
                                                    {% if failure.field %}
                                                        <td><span class="code">{{ failure.field }}</span></td>
                                                    {% endif %}
                                                    {% if failure.value %}
                                                        <td><span class="code">{{ failure.value }}</span></td>
                                                    {% endif %}
                                                    <td><span class="error-message">{{ failure.message }}</span></td>
                                                </tr>
                                                {% endfor %}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            {% endif %}
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
        </div>
        {% endfor %}
    </div>

    <!-- Chart.js Library (Self-Hosted) -->
    <script src="resources/js/chart.min.js"></script>

    <script>
        // Chart.js global configuration
        Chart.defaults.color = '#9aa5ce';
        Chart.defaults.borderColor = '#3b4261';
        Chart.defaults.font.family = "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";

        // Color palette matching Tokyo Night theme
        const colors = {
            success: '#9ece6a',
            error: '#f7768e',
            warning: '#e0af68',
            info: '#7aa2f7',
            purple: '#bb9af7',
        };

        // Results Distribution Chart (Donut)
        const resultsCtx = document.getElementById('resultsChart');
        if (resultsCtx) {
            new Chart(resultsCtx, {
                type: 'doughnut',
                data: {
                    labels: ['Passed', 'Errors', 'Warnings'],
                    datasets: [{
                        data: [
                            {{ passed_validations }},
                            {{ report.total_errors }},
                            {{ report.total_warnings }}
                        ],
                        backgroundColor: [
                            colors.success,
                            colors.error,
                            colors.warning
                        ],
                        borderWidth: 2,
                        borderColor: '#24283b'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 15,
                                usePointStyle: true,
                                font: { size: 12 }
                            }
                        },
                        tooltip: {
                            backgroundColor: '#1a1b26',
                            titleColor: '#c0caf5',
                            bodyColor: '#9aa5ce',
                            borderColor: '#3b4261',
                            borderWidth: 1,
                            padding: 12,
                            displayColors: true,
                            callbacks: {
                                label: function(context) {
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const percentage = ((context.parsed / total) * 100).toFixed(1);
                                    return context.label + ': ' + context.parsed + ' (' + percentage + '%)';
                                }
                            }
                        }
                    }
                }
            });
        }

        // Severity Chart (Horizontal Bar)
        const severityCtx = document.getElementById('severityChart');
        if (severityCtx) {
            // Calculate severity breakdown
            const errorCount = {{ report.total_errors }};
            const warningCount = {{ report.total_warnings }};

            new Chart(severityCtx, {
                type: 'bar',
                data: {
                    labels: ['Errors', 'Warnings'],
                    datasets: [{
                        label: 'Count',
                        data: [errorCount, warningCount],
                        backgroundColor: [colors.error, colors.warning],
                        borderWidth: 0
                    }]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: '#1a1b26',
                            titleColor: '#c0caf5',
                            bodyColor: '#9aa5ce',
                            borderColor: '#3b4261',
                            borderWidth: 1,
                            padding: 12
                        }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            ticks: { precision: 0 },
                            grid: { color: '#3b4261' }
                        },
                        y: {
                            grid: { display: false }
                        }
                    }
                }
            });
        }

        // Files Status Chart (Bar)
        const filesCtx = document.getElementById('filesChart');
        if (filesCtx) {
            const fileNames = [
                {% for file_report in report.file_reports %}
                '{{ file_report.file_name }}'{% if not loop.last %},{% endif %}
                {% endfor %}
            ];

            const fileErrors = [
                {% for file_report in report.file_reports %}
                {{ file_report.error_count }}{% if not loop.last %},{% endif %}
                {% endfor %}
            ];

            const fileWarnings = [
                {% for file_report in report.file_reports %}
                {{ file_report.warning_count }}{% if not loop.last %},{% endif %}
                {% endfor %}
            ];

            new Chart(filesCtx, {
                type: 'bar',
                data: {
                    labels: fileNames,
                    datasets: [
                        {
                            label: 'Errors',
                            data: fileErrors,
                            backgroundColor: colors.error,
                            borderWidth: 0
                        },
                        {
                            label: 'Warnings',
                            data: fileWarnings,
                            backgroundColor: colors.warning,
                            borderWidth: 0
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 15,
                                usePointStyle: true,
                                font: { size: 12 }
                            }
                        },
                        tooltip: {
                            backgroundColor: '#1a1b26',
                            titleColor: '#c0caf5',
                            bodyColor: '#9aa5ce',
                            borderColor: '#3b4261',
                            borderWidth: 1,
                            padding: 12
                        }
                    },
                    scales: {
                        x: {
                            stacked: false,
                            grid: { display: false }
                        },
                        y: {
                            stacked: false,
                            beginAtZero: true,
                            ticks: { precision: 0 },
                            grid: { color: '#3b4261' }
                        }
                    }
                }
            });
        }

        // File and validation toggle functions
        function toggleFile(fileId) {
            const content = document.getElementById(fileId);
            const icon = document.getElementById('toggle-' + fileId);

            if (content.style.display === 'none') {
                content.style.display = 'block';
                icon.classList.remove('rotated');
            } else {
                content.style.display = 'none';
                icon.classList.add('rotated');
            }
        }

        function toggleValidation(validationId) {
            const details = document.getElementById(validationId);
            const icon = document.getElementById('toggle-' + validationId);

            details.classList.toggle('show');
            icon.classList.toggle('rotated');
        }

        // Auto-expand failed validations
        document.addEventListener('DOMContentLoaded', function() {
            const failedValidations = document.querySelectorAll('.validation-item.failed');
            failedValidations.forEach(function(item) {
                const header = item.querySelector('.validation-header');
                if (header) {
                    header.click();
                }
            });
        });
    </script>
</body>
</html>
"""
