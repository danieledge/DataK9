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


# Embedded HTML template with modern dark theme matching executive profiler report
# Self-contained with inline CSS and JavaScript - mobile responsive
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ report.job_name }} - Validation Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        /* Modern Dark Theme - Matching Executive Profiler Report */
        :root {
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-card: #1e293b;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --border: #334155;
            --success: #10b981;
            --success-soft: rgba(16, 185, 129, 0.1);
            --warning: #f59e0b;
            --warning-soft: rgba(245, 158, 11, 0.1);
            --error: #ef4444;
            --error-soft: rgba(239, 68, 68, 0.1);
            --critical: #dc2626;
            --critical-soft: rgba(220, 38, 38, 0.1);
            --info: #3b82f6;
            --info-soft: rgba(59, 130, 246, 0.1);
            --accent: #8b5cf6;
            --accent-soft: rgba(139, 92, 246, 0.1);
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            min-height: 100vh;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }

        /* Header - Matching Executive Style */
        .header {
            background: linear-gradient(135deg, var(--bg-secondary) 0%, #0f172a 100%);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 32px;
            margin-bottom: 24px;
            position: relative;
            overflow: hidden;
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, var(--accent) 0%, var(--info) 50%, var(--success) 100%);
        }

        .header-top {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            flex-wrap: wrap;
            gap: 16px;
            margin-bottom: 16px;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .brand-icon {
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, var(--accent), var(--info));
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
        }

        .brand-text {
            font-size: 24px;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent), var(--info));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .header-badge {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 16px;
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 24px;
            font-size: 13px;
            color: var(--text-secondary);
        }

        .header h1 {
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 8px;
            color: var(--text-primary);
        }

        .header-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            color: var(--text-secondary);
            font-size: 14px;
        }

        .header-meta span {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        /* KPI Belt - Matching Executive Style */
        .kpi-belt {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }

        .kpi-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        .kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
        }

        .kpi-value {
            font-size: 36px;
            font-weight: 700;
            line-height: 1;
            margin-bottom: 8px;
        }

        .kpi-label {
            font-size: 12px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }

        .kpi-trend {
            font-size: 12px;
            padding: 4px 8px;
            border-radius: 12px;
            display: inline-block;
        }

        .kpi-trend.good { background: var(--success-soft); color: var(--success); }
        .kpi-trend.warning { background: var(--warning-soft); color: var(--warning); }
        .kpi-trend.critical { background: var(--error-soft); color: var(--error); }

        .kpi-card.status .kpi-value { font-size: 24px; }
        .kpi-card.passed .kpi-value { color: var(--success); }
        .kpi-card.failed .kpi-value { color: var(--error); }
        .kpi-card.warning .kpi-value { color: var(--warning); }
        .kpi-card.errors .kpi-value { color: var(--error); }
        .kpi-card.warnings .kpi-value { color: var(--warning); }

        /* Accordion Sections */
        .accordion {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            margin-bottom: 16px;
            overflow: hidden;
        }

        .accordion-header {
            padding: 20px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            transition: background 0.2s;
            gap: 16px;
        }

        .accordion-header:hover {
            background: rgba(255, 255, 255, 0.02);
        }

        .accordion-title-group {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .accordion-icon {
            width: 40px;
            height: 40px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
        }

        .accordion-icon.file { background: var(--info-soft); }
        .accordion-icon.passed { background: var(--success-soft); }
        .accordion-icon.failed { background: var(--error-soft); }
        .accordion-icon.warning { background: var(--warning-soft); }
        .accordion-icon.chart { background: var(--accent-soft); }
        .accordion-icon.info { background: var(--info-soft); }

        .accordion-title {
            font-size: 16px;
            font-weight: 600;
            color: var(--text-primary);
        }

        .accordion-subtitle {
            font-size: 13px;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .accordion-meta {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .accordion-badge {
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }

        .accordion-badge.good { background: var(--success-soft); color: var(--success); }
        .accordion-badge.warning { background: var(--warning-soft); color: var(--warning); }
        .accordion-badge.critical { background: var(--error-soft); color: var(--error); }
        .accordion-badge.info { background: var(--info-soft); color: var(--info); }

        .accordion-chevron {
            color: var(--text-muted);
            transition: transform 0.3s;
            font-size: 12px;
        }

        .accordion.open .accordion-chevron {
            transform: rotate(180deg);
        }

        .accordion-body {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
        }

        .accordion.open .accordion-body {
            max-height: 5000px;
        }

        .accordion-content {
            padding: 0 24px 24px;
            border-top: 1px solid var(--border);
        }

        /* File Meta Grid */
        .file-meta-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 16px;
            padding: 20px;
            background: var(--bg-primary);
            border-radius: 8px;
            margin-top: 16px;
        }

        .meta-item {
            text-align: center;
        }

        .meta-label {
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }

        .meta-value {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
        }

        /* Validation Results */
        .validation-item {
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin-top: 12px;
            overflow: hidden;
        }

        .validation-item.failed {
            border-left: 3px solid var(--error);
        }

        .validation-item.warning-status {
            border-left: 3px solid var(--warning);
        }

        .validation-item.passed-status {
            border-left: 3px solid var(--success);
        }

        .validation-header {
            padding: 16px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            transition: background 0.2s;
            gap: 12px;
            flex-wrap: wrap;
        }

        .validation-header:hover {
            background: rgba(255, 255, 255, 0.02);
        }

        .validation-title {
            display: flex;
            align-items: center;
            gap: 12px;
            flex: 1;
            min-width: 200px;
        }

        .validation-icon { font-size: 20px; }

        .validation-name {
            font-weight: 600;
            font-size: 14px;
        }

        .severity-badge {
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
        }

        .severity-error { background: var(--error-soft); color: var(--error); }
        .severity-warning { background: var(--warning-soft); color: var(--warning); }
        .severity-success { background: var(--success-soft); color: var(--success); }

        .validation-stats {
            display: flex;
            gap: 16px;
            font-size: 13px;
            align-items: center;
            flex-wrap: wrap;
        }

        .validation-stats .stat {
            color: var(--text-muted);
        }

        .validation-stats .stat.error { color: var(--error); }

        .validation-details {
            padding: 16px;
            background: var(--bg-secondary);
            border-top: 1px solid var(--border);
            display: none;
        }

        .validation-details.show { display: block; }

        .validation-message {
            padding: 12px 16px;
            background: var(--bg-primary);
            border-left: 3px solid var(--info);
            border-radius: 6px;
            margin-bottom: 16px;
            font-size: 14px;
        }

        /* Failures Table */
        .failures-section { margin-top: 16px; }

        .failures-section h4 {
            font-size: 14px;
            margin-bottom: 12px;
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
            font-size: 13px;
        }

        .failures-table th {
            background: var(--bg-primary);
            padding: 12px;
            text-align: left;
            font-weight: 600;
            border-bottom: 1px solid var(--border);
            color: var(--text-secondary);
            white-space: nowrap;
        }

        .failures-table td {
            padding: 10px 12px;
            border-bottom: 1px solid var(--border);
            color: var(--text-secondary);
        }

        .failures-table tr:hover { background: rgba(255, 255, 255, 0.02); }
        .failures-table tr:last-child td { border-bottom: none; }

        .code {
            background: var(--bg-primary);
            padding: 2px 8px;
            border-radius: 4px;
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 12px;
            color: var(--info);
        }

        .error-message { color: var(--error); }

        /* Charts Section */
        .charts-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-top: 16px;
        }

        .chart-card {
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 20px;
        }

        .chart-card h3 {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 16px;
            text-align: center;
        }

        .chart-wrapper {
            position: relative;
            height: 220px;
        }

        /* Status Legend */
        .legend-section {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px 24px;
            margin-bottom: 24px;
        }

        .legend-title {
            font-size: 14px;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .legend-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 12px;
        }

        .legend-item {
            padding: 12px 16px;
            background: var(--bg-primary);
            border-radius: 8px;
            border-left: 3px solid;
        }

        .legend-item.passed { border-color: var(--success); }
        .legend-item.warning { border-color: var(--warning); }
        .legend-item.failed { border-color: var(--error); }

        .legend-item strong {
            display: block;
            margin-bottom: 4px;
        }

        .legend-item.passed strong { color: var(--success); }
        .legend-item.warning strong { color: var(--warning); }
        .legend-item.failed strong { color: var(--error); }

        .legend-item p {
            font-size: 13px;
            color: var(--text-muted);
            margin: 0;
        }

        /* CDA Section */
        .cda-section {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px 24px;
            margin-bottom: 24px;
        }

        .cda-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            flex-wrap: wrap;
            gap: 12px;
        }

        .cda-title {
            font-size: 16px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .cda-metrics {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 12px;
            margin-bottom: 20px;
        }

        .cda-metric {
            text-align: center;
            padding: 16px;
            background: var(--bg-primary);
            border-radius: 8px;
        }

        .cda-metric-value {
            font-size: 24px;
            font-weight: 700;
        }

        .cda-metric-label {
            font-size: 11px;
            color: var(--text-muted);
            text-transform: uppercase;
        }

        .cda-file {
            padding: 12px 16px;
            background: var(--bg-primary);
            border-radius: 8px;
            margin-bottom: 8px;
            border-left: 3px solid;
        }

        .cda-file.covered { border-color: var(--success); }
        .cda-file.gaps { border-color: var(--warning); }

        .cda-file-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 8px;
        }

        .cda-fields {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }

        .cda-field {
            padding: 4px 10px;
            background: var(--bg-secondary);
            border-radius: 4px;
            font-size: 12px;
            border: 1px solid;
        }

        .cda-field.covered { border-color: var(--success); color: var(--success); }
        .cda-field.gap { border-color: var(--error); color: var(--error); }

        /* Toggle Icon */
        .toggle-icon {
            transition: transform 0.3s;
            color: var(--text-muted);
            font-size: 12px;
        }

        .toggle-icon.rotated { transform: rotate(180deg); }

        /* Mobile Optimizations */
        @media (max-width: 768px) {
            .container { padding: 16px; }
            .header { padding: 20px; }
            .kpi-belt { grid-template-columns: repeat(2, 1fr); }
            .kpi-value { font-size: 28px; }
            .cda-metrics { grid-template-columns: repeat(2, 1fr); }
        }

        /* Print Styles */
        @media print {
            body { background: white; color: black; }
            .accordion-body { max-height: none !important; }
            .validation-details { display: block !important; }
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <div class="header-top">
                <div class="brand">
                    <div class="brand-icon">🛡️</div>
                    <span class="brand-text">DataK9</span>
                </div>
                <div class="header-badge">
                    <span>📋</span>
                    <span>Validation Report</span>
                </div>
            </div>
            <h1>{{ report.job_name }}</h1>
            <div class="header-meta">
                <span>🕐 {{ report.execution_time.strftime('%Y-%m-%d %H:%M:%S') }}</span>
                <span>⏱️ {{ "%.2f"|format(report.duration_seconds) }}s</span>
                <span>📁 {{ report.file_reports|length }} file{{ 's' if report.file_reports|length != 1 else '' }}</span>
                <span>🔍 {{ total_validations }} validations</span>
            </div>
            {% if report.description %}
            <p style="margin-top: 12px; color: var(--text-muted); font-size: 14px;">{{ report.description }}</p>
            {% endif %}
        </div>

        <!-- KPI Belt -->
        <section class="kpi-belt">
            <div class="kpi-card status {% if report.overall_status == Status.PASSED %}passed{% elif report.overall_status == Status.FAILED %}failed{% else %}warning{% endif %}">
                <div class="kpi-value">{{ report.overall_status.value }}</div>
                <div class="kpi-label">Overall Status</div>
                <div class="kpi-trend {% if report.overall_status == Status.PASSED %}good{% elif report.overall_status == Status.FAILED %}critical{% else %}warning{% endif %}">
                    {% if report.overall_status == Status.PASSED %}All checks passed{% elif report.overall_status == Status.FAILED %}Action required{% else %}Review recommended{% endif %}
                </div>
            </div>

            <div class="kpi-card">
                <div class="kpi-value">{{ total_validations }}</div>
                <div class="kpi-label">Total Checks</div>
                <div class="kpi-trend good">{{ report.file_reports|length }} files validated</div>
            </div>

            <div class="kpi-card errors">
                <div class="kpi-value">{{ report.total_errors }}</div>
                <div class="kpi-label">Errors</div>
                <div class="kpi-trend {% if report.total_errors == 0 %}good{% else %}critical{% endif %}">
                    {% if report.total_errors == 0 %}No errors{% else %}Critical issues{% endif %}
                </div>
            </div>

            <div class="kpi-card warnings">
                <div class="kpi-value">{{ report.total_warnings }}</div>
                <div class="kpi-label">Warnings</div>
                <div class="kpi-trend {% if report.total_warnings == 0 %}good{% else %}warning{% endif %}">
                    {% if report.total_warnings == 0 %}No warnings{% else %}Review suggested{% endif %}
                </div>
            </div>

            <div class="kpi-card passed">
                <div class="kpi-value">{{ passed_validations }}</div>
                <div class="kpi-label">Passed</div>
                <div class="kpi-trend good">{{ "%.1f"|format(passed_validations / total_validations * 100 if total_validations > 0 else 0) }}% success rate</div>
            </div>
        </section>

        {% if cda_report %}
        <!-- CDA Coverage Section -->
        <div class="cda-section">
            <div class="cda-header">
                <div class="cda-title">🛡️ Critical Data Attribute Coverage</div>
                {% if cda_report.total_gaps > 0 %}
                <span class="accordion-badge warning">⚠️ Coverage Gaps</span>
                {% else %}
                <span class="accordion-badge good">✓ Full Coverage</span>
                {% endif %}
            </div>

            <div class="cda-metrics">
                <div class="cda-metric">
                    <div class="cda-metric-value" style="color: var(--info);">{{ cda_report.total_cdas }}</div>
                    <div class="cda-metric-label">Total CDAs</div>
                </div>
                <div class="cda-metric">
                    <div class="cda-metric-value" style="color: var(--success);">{{ cda_report.total_covered }}</div>
                    <div class="cda-metric-label">Covered</div>
                </div>
                <div class="cda-metric">
                    <div class="cda-metric-value" style="color: {% if cda_report.total_gaps > 0 %}var(--error){% else %}var(--success){% endif %};">{{ cda_report.total_gaps }}</div>
                    <div class="cda-metric-label">Gaps</div>
                </div>
                <div class="cda-metric">
                    <div class="cda-metric-value" style="color: {% if cda_report.overall_coverage >= 90 %}var(--success){% elif cda_report.overall_coverage >= 70 %}var(--warning){% else %}var(--error){% endif %};">{{ "%.0f"|format(cda_report.overall_coverage) }}%</div>
                    <div class="cda-metric-label">Coverage</div>
                </div>
            </div>

            {% for file_result in cda_report.file_results %}
            <div class="cda-file {% if file_result.gap_cdas > 0 %}gaps{% else %}covered{% endif %}">
                <div class="cda-file-header">
                    <strong>{{ file_result.file_name }}</strong>
                    <span style="color: {% if file_result.gap_cdas > 0 %}var(--warning){% else %}var(--success){% endif %};">
                        {{ file_result.covered_cdas }}/{{ file_result.total_cdas }} covered
                    </span>
                </div>
                {% if file_result.field_coverage %}
                <div class="cda-fields">
                    {% for field_cov in file_result.field_coverage %}
                    <span class="cda-field {% if field_cov.is_covered %}covered{% else %}gap{% endif %}">
                        {% if field_cov.is_covered %}✓{% else %}✗{% endif %} {{ field_cov.cda.field }}
                    </span>
                    {% endfor %}
                </div>
                {% endif %}
            </div>
            {% endfor %}

            <p style="margin-top: 12px; color: var(--text-muted); font-size: 12px; font-style: italic;">
                CDAs are Critical Data Attributes requiring validation coverage.
            </p>
        </div>
        {% endif %}

        <!-- Status Legend -->
        <div class="legend-section">
            <div class="legend-title">📋 Status Definitions</div>
            <div class="legend-grid">
                <div class="legend-item failed">
                    <strong>FAILED</strong>
                    <p>One or more ERROR-severity checks failed. Critical data quality issues detected.</p>
                </div>
                <div class="legend-item warning">
                    <strong>WARNING</strong>
                    <p>All ERROR checks passed, but WARNING-severity checks failed. Review recommended.</p>
                </div>
                <div class="legend-item passed">
                    <strong>PASSED</strong>
                    <p>All validation checks passed. Data meets quality standards and is ready for use.</p>
                </div>
            </div>
        </div>

        <!-- Charts & Visualizations -->
        <div class="accordion open" id="charts-accordion">
            <div class="accordion-header" onclick="toggleAccordion('charts-accordion')">
                <div class="accordion-title-group">
                    <div class="accordion-icon chart">📊</div>
                    <div>
                        <div class="accordion-title">Validation Insights</div>
                        <div class="accordion-subtitle">Visual overview of validation results</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-body">
                <div class="accordion-content">
                    <div class="charts-grid">
                        <div class="chart-card">
                            <h3>Results Distribution</h3>
                            <div class="chart-wrapper">
                                <canvas id="resultsChart"></canvas>
                            </div>
                        </div>
                        <div class="chart-card">
                            <h3>By Severity</h3>
                            <div class="chart-wrapper">
                                <canvas id="severityChart"></canvas>
                            </div>
                        </div>
                        <div class="chart-card">
                            <h3>Files Status</h3>
                            <div class="chart-wrapper">
                                <canvas id="filesChart"></canvas>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- File Sections -->
        {% for file_report in report.file_reports %}
        <div class="accordion {% if file_report.status != Status.PASSED %}open{% endif %}" id="file-accordion-{{ loop.index }}">
            <div class="accordion-header" onclick="toggleAccordion('file-accordion-{{ loop.index }}')">
                <div class="accordion-title-group">
                    <div class="accordion-icon {% if file_report.status == Status.PASSED %}passed{% elif file_report.status == Status.FAILED %}failed{% else %}warning{% endif %}">
                        {% if file_report.status == Status.PASSED %}✓{% elif file_report.status == Status.FAILED %}✗{% else %}⚠{% endif %}
                    </div>
                    <div>
                        <div class="accordion-title">{{ file_report.file_name }}</div>
                        <div class="accordion-subtitle">{{ file_report.total_validations }} checks • {{ file_report.file_format|upper }}</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    {% if file_report.error_count > 0 %}
                    <span class="accordion-badge critical">{{ file_report.error_count }} errors</span>
                    {% endif %}
                    {% if file_report.warning_count > 0 %}
                    <span class="accordion-badge warning">{{ file_report.warning_count }} warnings</span>
                    {% endif %}
                    {% if file_report.status == Status.PASSED %}
                    <span class="accordion-badge good">All passed</span>
                    {% endif %}
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-body">
                <div class="accordion-content">
                    <!-- File Metadata -->
                    <div class="file-meta-grid">
                        <div class="meta-item">
                            <div class="meta-label">File Path</div>
                            <div class="meta-value" style="font-size: 12px; word-break: break-all;">{{ file_report.file_path }}</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Format</div>
                            <div class="meta-value">{{ file_report.file_format|upper }}</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Size</div>
                            <div class="meta-value">{% if file_report.metadata.file_size_mb %}{{ "%.2f"|format(file_report.metadata.file_size_mb) }} MB{% else %}N/A{% endif %}</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Rows</div>
                            <div class="meta-value">{% if file_report.metadata.total_rows %}{{ "{:,}".format(file_report.metadata.total_rows) }}{% elif file_report.metadata.estimated_rows %}~{{ "{:,}".format(file_report.metadata.estimated_rows) }}{% else %}N/A{% endif %}</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Columns</div>
                            <div class="meta-value">{{ file_report.metadata.column_count if file_report.metadata.column_count else 'N/A' }}</div>
                        </div>
                        <div class="meta-item">
                            <div class="meta-label">Duration</div>
                            <div class="meta-value">{{ "%.2f"|format(file_report.execution_time) }}s</div>
                        </div>
                    </div>

                    <!-- Validations -->
                    <h4 style="margin: 20px 0 12px; font-size: 14px; color: var(--text-primary);">🔍 Validation Results</h4>
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

    <script>
        // Chart.js global configuration - updated for new color scheme
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.borderColor = '#334155';
        Chart.defaults.font.family = "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";

        // Color palette matching new design
        const colors = {
            success: '#10b981',
            error: '#ef4444',
            warning: '#f59e0b',
            info: '#3b82f6',
            accent: '#8b5cf6',
        };

        // Accordion toggle function
        function toggleAccordion(id) {
            const accordion = document.getElementById(id);
            accordion.classList.toggle('open');
        }

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
                        borderColor: '#1e293b'
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
                                font: { size: 11 }
                            }
                        },
                        tooltip: {
                            backgroundColor: '#0f172a',
                            titleColor: '#f1f5f9',
                            bodyColor: '#94a3b8',
                            borderColor: '#334155',
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
                            backgroundColor: '#0f172a',
                            titleColor: '#f1f5f9',
                            bodyColor: '#94a3b8',
                            borderColor: '#334155',
                            borderWidth: 1,
                            padding: 12
                        }
                    },
                    scales: {
                        x: {
                            beginAtZero: true,
                            ticks: { precision: 0 },
                            grid: { color: '#334155' }
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
                                font: { size: 11 }
                            }
                        },
                        tooltip: {
                            backgroundColor: '#0f172a',
                            titleColor: '#f1f5f9',
                            bodyColor: '#94a3b8',
                            borderColor: '#334155',
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
                            grid: { color: '#334155' }
                        }
                    }
                }
            });
        }

        // Validation toggle function
        function toggleValidation(validationId) {
            const details = document.getElementById(validationId);
            const icon = document.getElementById('toggle-' + validationId);

            details.classList.toggle('show');
            if (icon) icon.classList.toggle('rotated');
        }

        // Auto-expand failed validations on load
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
