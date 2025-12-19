"""
SLA HTML Report Generator.

Generates elegant, interactive HTML reports for SLA compliance visualization.
Uses a modern dark theme with traffic light status indicators.

Author: Daniel Edge
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List
from jinja2 import Template

from .sla_models import SLAReport, SLAResult, SLAStatus


class SLAHTMLReporter:
    """
    Generates elegant HTML reports for SLA compliance.

    Features:
    - Executive summary with traffic light KPIs
    - Interactive CDA status table
    - Drill-down details for each field
    - Trend visualization
    - Export capabilities
    """

    def generate(
        self,
        report: SLAReport,
        output_path: str,
        title: Optional[str] = None,
        show_details: bool = True
    ) -> str:
        """
        Generate HTML report from SLA evaluation results.

        Args:
            report: SLAReport with evaluation results
            output_path: Path where HTML file should be written
            title: Optional custom title for the report
            show_details: Whether to show detailed breakdown

        Returns:
            Path to generated report

        Raises:
            IOError: If unable to write report file
        """
        try:
            # Prepare template data
            template_data = self._prepare_template_data(report, title, show_details)

            # Render HTML
            html_content = self._render_html(template_data)

            # Ensure output directory exists
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Write HTML file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            return str(output_file)

        except Exception as e:
            raise IOError(f"Error generating SLA HTML report: {str(e)}")

    def _prepare_template_data(
        self,
        report: SLAReport,
        title: Optional[str],
        show_details: bool
    ) -> dict:
        """Prepare data for template rendering."""
        # Sort results by status severity (RED first, then AMBER, GREEN, NOT_EVALUATED)
        status_order = {
            SLAStatus.RED: 0,
            SLAStatus.AMBER: 1,
            SLAStatus.GREEN: 2,
            SLAStatus.NOT_EVALUATED: 3,
        }
        sorted_results = sorted(
            report.results,
            key=lambda r: (status_order.get(r.status, 4), r.field)
        )

        # Calculate compliance percentage
        evaluated = [r for r in report.results if r.status != SLAStatus.NOT_EVALUATED]
        if evaluated:
            compliant = sum(1 for r in evaluated if r.status in (SLAStatus.GREEN, SLAStatus.AMBER))
            compliance_pct = (compliant / len(evaluated)) * 100
        else:
            compliance_pct = 100.0

        # Calculate overall health score (weighted by severity)
        if evaluated:
            # GREEN = 100, AMBER = 70, RED = 0
            scores = []
            for r in evaluated:
                if r.status == SLAStatus.GREEN:
                    scores.append(100)
                elif r.status == SLAStatus.AMBER:
                    scores.append(70)
                else:
                    scores.append(0)
            health_score = sum(scores) / len(scores)
        else:
            health_score = 100.0

        # Determine overall status
        if report.red_count > 0:
            overall_status = "RED"
            overall_message = "SLA Breached"
        elif report.amber_count > 0:
            overall_status = "AMBER"
            overall_message = "SLA Warning"
        elif report.green_count > 0:
            overall_status = "GREEN"
            overall_message = "SLA Compliant"
        else:
            overall_status = "GREY"
            overall_message = "No Data"

        # Group by tier for breakdown
        tier_groups = {}
        for result in report.results:
            tier = result.tier_name
            if tier not in tier_groups:
                tier_groups[tier] = {"total": 0, "green": 0, "amber": 0, "red": 0, "na": 0}
            tier_groups[tier]["total"] += 1
            if result.status == SLAStatus.GREEN:
                tier_groups[tier]["green"] += 1
            elif result.status == SLAStatus.AMBER:
                tier_groups[tier]["amber"] += 1
            elif result.status == SLAStatus.RED:
                tier_groups[tier]["red"] += 1
            else:
                tier_groups[tier]["na"] += 1

        return {
            "report": report,
            "title": title or f"SLA Compliance Report - {report.file_name}",
            "sorted_results": sorted_results,
            "compliance_pct": compliance_pct,
            "health_score": health_score,
            "overall_status": overall_status,
            "overall_message": overall_message,
            "tier_groups": tier_groups,
            "show_details": show_details,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "SLAStatus": SLAStatus,
        }

    def _render_html(self, template_data: dict) -> str:
        """Render HTML using embedded template."""
        template = Template(SLA_HTML_TEMPLATE)
        return template.render(**template_data)


def generate_sla_report(
    report: SLAReport,
    output_path: str,
    title: Optional[str] = None,
    show_details: bool = True
) -> str:
    """
    Convenience function to generate SLA HTML report.

    Args:
        report: SLAReport with evaluation results
        output_path: Path where HTML file should be written
        title: Optional custom title
        show_details: Whether to show detailed breakdown

    Returns:
        Path to generated report
    """
    reporter = SLAHTMLReporter()
    return reporter.generate(report, output_path, title, show_details)


# Embedded HTML template with modern dark theme
SLA_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        /* Modern Dark Theme - SLA Compliance Report */
        :root {
            --bg-primary: #0f172a;
            --bg-secondary: #1e293b;
            --bg-card: #1e293b;
            --bg-hover: #334155;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --border: #334155;
            --border-light: #475569;

            /* Traffic light colors */
            --green: #10b981;
            --green-soft: rgba(16, 185, 129, 0.15);
            --green-glow: rgba(16, 185, 129, 0.4);
            --amber: #f59e0b;
            --amber-soft: rgba(245, 158, 11, 0.15);
            --amber-glow: rgba(245, 158, 11, 0.4);
            --red: #ef4444;
            --red-soft: rgba(239, 68, 68, 0.15);
            --red-glow: rgba(239, 68, 68, 0.4);
            --grey: #6b7280;
            --grey-soft: rgba(107, 114, 128, 0.15);

            /* Accent colors */
            --accent: #8b5cf6;
            --accent-soft: rgba(139, 92, 246, 0.15);
            --info: #3b82f6;
            --info-soft: rgba(59, 130, 246, 0.15);
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
            max-width: 1200px;
            margin: 0 auto;
            padding: 32px 24px;
        }

        /* Header Section */
        .header {
            background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-card) 100%);
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
            background: {% if overall_status == 'GREEN' %}var(--green){% elif overall_status == 'AMBER' %}var(--amber){% elif overall_status == 'RED' %}var(--red){% else %}var(--grey){% endif %};
        }

        .header-content {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            flex-wrap: wrap;
            gap: 24px;
        }

        .header-left h1 {
            font-size: 1.75rem;
            font-weight: 700;
            margin-bottom: 8px;
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .header-left h1 .shield {
            font-size: 1.5rem;
        }

        .header-meta {
            display: flex;
            gap: 24px;
            flex-wrap: wrap;
            color: var(--text-secondary);
            font-size: 0.875rem;
        }

        .header-meta span {
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .overall-status {
            display: flex;
            align-items: center;
            gap: 16px;
        }

        .status-indicator {
            width: 80px;
            height: 80px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 2rem;
            {% if overall_status == 'GREEN' %}
            background: var(--green-soft);
            box-shadow: 0 0 30px var(--green-glow);
            {% elif overall_status == 'AMBER' %}
            background: var(--amber-soft);
            box-shadow: 0 0 30px var(--amber-glow);
            {% elif overall_status == 'RED' %}
            background: var(--red-soft);
            box-shadow: 0 0 30px var(--red-glow);
            animation: pulse-red 2s infinite;
            {% else %}
            background: var(--grey-soft);
            {% endif %}
        }

        @keyframes pulse-red {
            0%, 100% { box-shadow: 0 0 30px var(--red-glow); }
            50% { box-shadow: 0 0 50px var(--red-glow); }
        }

        .status-text {
            text-align: right;
        }

        .status-text .label {
            font-size: 0.875rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }

        .status-text .value {
            font-size: 1.5rem;
            font-weight: 700;
            {% if overall_status == 'GREEN' %}color: var(--green);
            {% elif overall_status == 'AMBER' %}color: var(--amber);
            {% elif overall_status == 'RED' %}color: var(--red);
            {% else %}color: var(--grey);{% endif %}
        }

        /* KPI Cards Grid */
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }

        .kpi-card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            position: relative;
            overflow: hidden;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }

        .kpi-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
        }

        .kpi-card.green { border-left: 4px solid var(--green); }
        .kpi-card.amber { border-left: 4px solid var(--amber); }
        .kpi-card.red { border-left: 4px solid var(--red); }
        .kpi-card.grey { border-left: 4px solid var(--grey); }
        .kpi-card.info { border-left: 4px solid var(--info); }

        .kpi-icon {
            font-size: 1.5rem;
            margin-bottom: 8px;
        }

        .kpi-value {
            font-size: 2rem;
            font-weight: 700;
            margin-bottom: 4px;
        }

        .kpi-card.green .kpi-value { color: var(--green); }
        .kpi-card.amber .kpi-value { color: var(--amber); }
        .kpi-card.red .kpi-value { color: var(--red); }
        .kpi-card.grey .kpi-value { color: var(--grey); }
        .kpi-card.info .kpi-value { color: var(--info); }

        .kpi-label {
            font-size: 0.875rem;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }

        /* Progress Bar */
        .health-bar-container {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 24px;
        }

        .health-bar-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
        }

        .health-bar-title {
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .health-bar-value {
            font-size: 1.25rem;
            font-weight: 700;
            {% if health_score >= 90 %}color: var(--green);
            {% elif health_score >= 70 %}color: var(--amber);
            {% else %}color: var(--red);{% endif %}
        }

        .health-bar {
            height: 12px;
            background: var(--bg-primary);
            border-radius: 6px;
            overflow: hidden;
            position: relative;
        }

        .health-bar-fill {
            height: 100%;
            border-radius: 6px;
            transition: width 0.5s ease;
            {% if health_score >= 90 %}background: linear-gradient(90deg, var(--green), #34d399);
            {% elif health_score >= 70 %}background: linear-gradient(90deg, var(--amber), #fbbf24);
            {% else %}background: linear-gradient(90deg, var(--red), #f87171);{% endif %}
            width: {{ health_score }}%;
        }

        .health-bar-segments {
            display: flex;
            justify-content: space-between;
            margin-top: 8px;
            font-size: 0.75rem;
            color: var(--text-muted);
        }

        /* CDA Table Section */
        .section {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            margin-bottom: 24px;
            overflow: hidden;
        }

        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
            cursor: pointer;
            transition: background 0.2s ease;
        }

        .section-header:hover {
            background: var(--bg-hover);
        }

        .section-title {
            font-size: 1.125rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .section-toggle {
            color: var(--text-secondary);
            transition: transform 0.2s ease;
        }

        .section.collapsed .section-toggle {
            transform: rotate(-90deg);
        }

        .section-content {
            padding: 0;
            overflow: hidden;
            transition: max-height 0.3s ease;
        }

        .section.collapsed .section-content {
            max-height: 0;
            padding: 0;
        }

        /* Results Table */
        .results-table {
            width: 100%;
            border-collapse: collapse;
        }

        .results-table th {
            background: var(--bg-primary);
            padding: 14px 16px;
            text-align: left;
            font-weight: 600;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-secondary);
            border-bottom: 1px solid var(--border);
        }

        .results-table td {
            padding: 14px 16px;
            border-bottom: 1px solid var(--border);
            vertical-align: middle;
        }

        .results-table tbody tr {
            transition: background 0.15s ease;
        }

        .results-table tbody tr:hover {
            background: var(--bg-hover);
        }

        .results-table tbody tr:last-child td {
            border-bottom: none;
        }

        .field-name {
            font-weight: 600;
            font-family: 'SF Mono', Monaco, 'Courier New', monospace;
        }

        .tier-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.75rem;
            font-weight: 500;
            text-transform: uppercase;
        }

        .tier-badge.critical {
            background: var(--red-soft);
            color: var(--red);
        }

        .tier-badge.high {
            background: var(--amber-soft);
            color: var(--amber);
        }

        .tier-badge.standard {
            background: var(--info-soft);
            color: var(--info);
        }

        .tier-badge.low {
            background: var(--grey-soft);
            color: var(--text-secondary);
        }

        .tier-badge.custom {
            background: var(--accent-soft);
            color: var(--accent);
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
        }

        .status-badge.green {
            background: var(--green-soft);
            color: var(--green);
        }

        .status-badge.amber {
            background: var(--amber-soft);
            color: var(--amber);
        }

        .status-badge.red {
            background: var(--red-soft);
            color: var(--red);
        }

        .status-badge.grey {
            background: var(--grey-soft);
            color: var(--grey);
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
        }

        .status-badge.green .status-dot { background: var(--green); }
        .status-badge.amber .status-dot { background: var(--amber); }
        .status-badge.red .status-dot { background: var(--red); }
        .status-badge.grey .status-dot { background: var(--grey); }

        .accuracy-bar {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .accuracy-bar-track {
            flex: 1;
            height: 8px;
            background: var(--bg-primary);
            border-radius: 4px;
            overflow: hidden;
            min-width: 80px;
        }

        .accuracy-bar-fill {
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }

        .accuracy-bar-fill.green { background: var(--green); }
        .accuracy-bar-fill.amber { background: var(--amber); }
        .accuracy-bar-fill.red { background: var(--red); }

        .accuracy-value {
            font-weight: 600;
            font-size: 0.875rem;
            min-width: 55px;
            text-align: right;
        }

        .record-count {
            font-size: 0.8rem;
            color: var(--text-secondary);
        }

        .record-count .bad {
            color: var(--red);
            font-weight: 600;
        }

        /* Tier Breakdown Section */
        .tier-breakdown {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            padding: 16px;
        }

        .tier-card {
            background: var(--bg-primary);
            border-radius: 8px;
            padding: 14px;
            text-align: center;
        }

        .tier-card-name {
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
            margin-bottom: 8px;
            color: var(--text-secondary);
        }

        .tier-card-counts {
            display: flex;
            justify-content: center;
            gap: 10px;
            font-size: 0.875rem;
        }

        .tier-card-counts .count {
            display: flex;
            align-items: center;
            gap: 4px;
        }

        /* Validations List */
        .validations-list {
            display: flex;
            flex-wrap: wrap;
            gap: 6px;
        }

        .validation-tag {
            background: var(--accent-soft);
            color: var(--accent);
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 0.7rem;
            font-family: 'SF Mono', Monaco, monospace;
        }

        /* Footer */
        .footer {
            text-align: center;
            padding: 24px;
            color: var(--text-muted);
            font-size: 0.8rem;
        }

        .footer a {
            color: var(--accent);
            text-decoration: none;
        }

        /* Responsive */
        @media (max-width: 768px) {
            .container { padding: 16px; }
            .header { padding: 20px; }
            .header-content { flex-direction: column; }
            .overall-status { width: 100%; justify-content: center; }
            .status-text { text-align: center; }
            .kpi-grid { grid-template-columns: repeat(2, 1fr); }
            .results-table { font-size: 0.875rem; }
            .results-table th, .results-table td { padding: 10px 8px; }
        }

        /* Print styles */
        @media print {
            body { background: white; color: black; }
            .section-header { cursor: default; }
            .section.collapsed .section-content { max-height: none; }
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header class="header">
            <div class="header-content">
                <div class="header-left">
                    <h1>
                        <span class="shield">&#128737;</span>
                        {{ title }}
                    </h1>
                    <div class="header-meta">
                        <span>&#128196; {{ report.file_name }}</span>
                        <span>&#128202; {{ report.dataset_row_count | default(0) }} records</span>
                        <span>&#128337; {{ generated_at }}</span>
                    </div>
                </div>
                <div class="overall-status">
                    <div class="status-text">
                        <div class="label">Overall Status</div>
                        <div class="value">{{ overall_message }}</div>
                    </div>
                    <div class="status-indicator">
                        {% if overall_status == 'GREEN' %}&#10004;{% elif overall_status == 'AMBER' %}&#9888;{% elif overall_status == 'RED' %}&#10060;{% else %}&#8212;{% endif %}
                    </div>
                </div>
            </div>
        </header>

        <!-- KPI Cards -->
        <div class="kpi-grid">
            <div class="kpi-card green">
                <div class="kpi-icon">&#9989;</div>
                <div class="kpi-value">{{ report.green_count }}</div>
                <div class="kpi-label">Compliant</div>
            </div>
            <div class="kpi-card amber">
                <div class="kpi-icon">&#9888;</div>
                <div class="kpi-value">{{ report.amber_count }}</div>
                <div class="kpi-label">Warning</div>
            </div>
            <div class="kpi-card red">
                <div class="kpi-icon">&#10060;</div>
                <div class="kpi-value">{{ report.red_count }}</div>
                <div class="kpi-label">Breached</div>
            </div>
            <div class="kpi-card grey">
                <div class="kpi-icon">&#9898;</div>
                <div class="kpi-value">{{ report.not_evaluated_count }}</div>
                <div class="kpi-label">Not Evaluated</div>
            </div>
            <div class="kpi-card info">
                <div class="kpi-icon">&#128200;</div>
                <div class="kpi-value">{{ "%.1f"|format(compliance_pct) }}%</div>
                <div class="kpi-label">Compliance Rate</div>
            </div>
        </div>

        <!-- Health Score Bar -->
        <div class="health-bar-container">
            <div class="health-bar-header">
                <div class="health-bar-title">
                    &#128154; Data Quality Health Score
                </div>
                <div class="health-bar-value">{{ "%.1f"|format(health_score) }}%</div>
            </div>
            <div class="health-bar">
                <div class="health-bar-fill"></div>
            </div>
            <div class="health-bar-segments">
                <span>0%</span>
                <span>Critical (&lt;70%)</span>
                <span>Warning (70-90%)</span>
                <span>Healthy (&gt;90%)</span>
                <span>100%</span>
            </div>
        </div>

        {% if tier_groups %}
        <!-- Tier Breakdown -->
        <div class="section">
            <div class="section-header" onclick="toggleSection(this)">
                <div class="section-title">
                    &#127919; SLA by Tier
                </div>
                <span class="section-toggle">&#9660;</span>
            </div>
            <div class="section-content">
                <div class="tier-breakdown">
                    {% for tier_name, counts in tier_groups.items() %}
                    <div class="tier-card">
                        <div class="tier-card-name">{{ tier_name }}</div>
                        <div class="tier-card-counts">
                            <span class="count" style="color: var(--green)">&#9989; {{ counts.green }}</span>
                            <span class="count" style="color: var(--amber)">&#9888; {{ counts.amber }}</span>
                            <span class="count" style="color: var(--red)">&#10060; {{ counts.red }}</span>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
        </div>
        {% endif %}

        <!-- CDA Results Table -->
        <div class="section">
            <div class="section-header" onclick="toggleSection(this)">
                <div class="section-title">
                    &#128202; Critical Data Attribute Status
                    <span style="color: var(--text-muted); font-weight: normal; font-size: 0.875rem;">
                        ({{ report.total_cdas }} CDAs)
                    </span>
                </div>
                <span class="section-toggle">&#9660;</span>
            </div>
            <div class="section-content">
                <table class="results-table">
                    <thead>
                        <tr>
                            <th>Field</th>
                            <th>Tier</th>
                            <th>Status</th>
                            <th>Accuracy</th>
                            <th>Tolerance</th>
                            <th>Records</th>
                            {% if show_details %}
                            <th>Validations</th>
                            {% endif %}
                        </tr>
                    </thead>
                    <tbody>
                        {% for result in sorted_results %}
                        <tr>
                            <td class="field-name">{{ result.field }}</td>
                            <td>
                                <span class="tier-badge {{ result.tier_name }}">
                                    {% if result.tier_name == 'critical' %}&#128293;{% elif result.tier_name == 'high' %}&#9888;{% elif result.tier_name == 'standard' %}&#128309;{% elif result.tier_name == 'low' %}&#128310;{% else %}&#10024;{% endif %}
                                    {{ result.tier_name }}
                                </span>
                            </td>
                            <td>
                                {% if result.status == SLAStatus.GREEN %}
                                <span class="status-badge green"><span class="status-dot"></span>GREEN</span>
                                {% elif result.status == SLAStatus.AMBER %}
                                <span class="status-badge amber"><span class="status-dot"></span>AMBER</span>
                                {% elif result.status == SLAStatus.RED %}
                                <span class="status-badge red"><span class="status-dot"></span>RED</span>
                                {% else %}
                                <span class="status-badge grey"><span class="status-dot"></span>N/A</span>
                                {% endif %}
                            </td>
                            <td>
                                <div class="accuracy-bar">
                                    <div class="accuracy-bar-track">
                                        <div class="accuracy-bar-fill {% if result.status == SLAStatus.GREEN %}green{% elif result.status == SLAStatus.AMBER %}amber{% else %}red{% endif %}"
                                             style="width: {{ result.accuracy }}%"></div>
                                    </div>
                                    <span class="accuracy-value">{{ "%.2f"|format(result.accuracy) }}%</span>
                                </div>
                            </td>
                            <td>{{ "%.2f"|format(result.tolerance * 100) }}%</td>
                            <td>
                                <div class="record-count">
                                    {% if result.bad_records > 0 %}
                                    <span class="bad">{{ result.bad_records }}</span> / {{ result.evaluated_records }} failed
                                    {% else %}
                                    {{ result.evaluated_records }} &#10004;
                                    {% endif %}
                                </div>
                            </td>
                            {% if show_details %}
                            <td>
                                <div class="validations-list">
                                    {% for v in result.contributing_validations %}
                                    <span class="validation-tag">{{ v }}</span>
                                    {% endfor %}
                                    {% if not result.contributing_validations %}
                                    <span style="color: var(--text-muted); font-size: 0.75rem;">None</span>
                                    {% endif %}
                                </div>
                            </td>
                            {% endif %}
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>

        {% if report.get_breaches() %}
        <!-- Breaches Detail -->
        <div class="section">
            <div class="section-header" onclick="toggleSection(this)">
                <div class="section-title" style="color: var(--red);">
                    &#128680; SLA Breaches Requiring Attention
                </div>
                <span class="section-toggle">&#9660;</span>
            </div>
            <div class="section-content" style="padding: 20px;">
                {% for breach in report.get_breaches() %}
                <div style="background: var(--red-soft); border: 1px solid rgba(239, 68, 68, 0.3); border-radius: 8px; padding: 16px; margin-bottom: 12px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;">
                        <div>
                            <strong style="color: var(--text-primary); font-size: 1rem;">{{ breach.field }}</strong>
                            <span class="tier-badge {{ breach.tier_name }}" style="margin-left: 8px;">{{ breach.tier_name }}</span>
                        </div>
                        <div style="text-align: right;">
                            <div style="color: var(--red); font-weight: 700; font-size: 1.25rem;">
                                {{ "%.4f"|format(breach.failure_rate * 100) }}% failure
                            </div>
                            <div style="color: var(--text-secondary); font-size: 0.8rem;">
                                Tolerance: {{ "%.2f"|format(breach.tolerance * 100) }}%
                            </div>
                        </div>
                    </div>
                    <div style="margin-top: 12px; font-size: 0.875rem; color: var(--text-secondary);">
                        <strong>{{ breach.bad_records }}</strong> records failed out of <strong>{{ breach.evaluated_records }}</strong> evaluated
                        {% if breach.contributing_validations %}
                        <span style="margin-left: 12px;">
                            via: {% for v in breach.contributing_validations %}<span class="validation-tag">{{ v }}</span> {% endfor %}
                        </span>
                        {% endif %}
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        {% endif %}

        <!-- Footer -->
        <footer class="footer">
            <p>Generated by <strong>DataK9</strong> SLA Compliance Framework</p>
            <p>{{ generated_at }}</p>
        </footer>
    </div>

    <script>
        function toggleSection(header) {
            const section = header.parentElement;
            section.classList.toggle('collapsed');
        }
    </script>
</body>
</html>
"""
