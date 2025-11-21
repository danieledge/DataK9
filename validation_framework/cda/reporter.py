"""
CDA Gap Analysis Reporter

Generates HTML reports for CDA gap analysis results.
"""

from datetime import datetime
from typing import List, Optional
import html

from .models import CDAGapResult, CDAAnalysisReport, CDATier, CDAFieldCoverage


class CDAReporter:
    """
    Generates HTML reports for CDA gap analysis.

    Creates professional, audit-ready reports showing:
    - Overall coverage metrics
    - Tier-by-tier breakdown
    - Detailed field coverage
    - Recommendations for uncovered fields
    """

    def generate_html(self, report: CDAAnalysisReport) -> str:
        """
        Generate complete HTML report for CDA gap analysis.

        Args:
            report: CDA analysis report to render

        Returns:
            Complete HTML document as string
        """
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CDA Gap Analysis Report - {html.escape(report.job_name)}</title>
    <style>
        {self._get_styles()}
    </style>
</head>
<body>
    <div class="container">
        {self._render_header(report)}
        {self._render_summary(report)}
        {self._render_file_results(report)}
        {self._render_footer(report)}
    </div>
</body>
</html>"""

    def _get_styles(self) -> str:
        """Return CSS styles for the report."""
        return """
        :root {
            --primary: #4A90E2;
            --success: #28a745;
            --warning: #ffc107;
            --danger: #dc3545;
            --text: #333;
            --bg: #f8f9fa;
            --card-bg: #fff;
            --border: #dee2e6;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }

        .header {
            background: linear-gradient(135deg, var(--primary), #2c5282);
            color: white;
            padding: 30px;
            border-radius: 12px;
            margin-bottom: 24px;
        }

        .header h1 {
            font-size: 28px;
            margin-bottom: 8px;
        }

        .header .subtitle {
            opacity: 0.9;
            font-size: 16px;
        }

        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }

        .metric-card {
            background: var(--card-bg);
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            text-align: center;
        }

        .metric-card .value {
            font-size: 36px;
            font-weight: 700;
            color: var(--primary);
        }

        .metric-card .label {
            font-size: 14px;
            color: #666;
            margin-top: 4px;
        }

        .metric-card.danger .value { color: var(--danger); }
        .metric-card.success .value { color: var(--success); }
        .metric-card.warning .value { color: var(--warning); }

        .alert {
            padding: 16px 20px;
            border-radius: 8px;
            margin-bottom: 24px;
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .alert-danger {
            background: #fee2e2;
            border-left: 4px solid var(--danger);
            color: #991b1b;
        }

        .alert-success {
            background: #d1fae5;
            border-left: 4px solid var(--success);
            color: #065f46;
        }

        .alert-warning {
            background: #fef3c7;
            border-left: 4px solid var(--warning);
            color: #92400e;
        }

        .file-section {
            background: var(--card-bg);
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 24px;
            overflow: hidden;
        }

        .file-header {
            background: #f1f5f9;
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .file-header h2 {
            font-size: 18px;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .coverage-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 600;
        }

        .coverage-badge.high { background: #d1fae5; color: #065f46; }
        .coverage-badge.medium { background: #fef3c7; color: #92400e; }
        .coverage-badge.low { background: #fee2e2; color: #991b1b; }

        .tier-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1px;
            background: var(--border);
            border-bottom: 1px solid var(--border);
        }

        .tier-box {
            background: var(--card-bg);
            padding: 16px;
            text-align: center;
        }

        .tier-box .tier-name {
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #666;
            margin-bottom: 4px;
        }

        .tier-box .tier-stat {
            font-size: 20px;
            font-weight: 600;
        }

        .tier-box.tier1 .tier-stat { color: var(--danger); }
        .tier-box.tier2 .tier-stat { color: var(--warning); }
        .tier-box.tier3 .tier-stat { color: var(--primary); }

        .field-table {
            width: 100%;
            border-collapse: collapse;
        }

        .field-table th {
            background: #f8fafc;
            padding: 12px 16px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            color: #64748b;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .field-table td {
            padding: 12px 16px;
            border-top: 1px solid var(--border);
        }

        .field-table tr:hover {
            background: #f8fafc;
        }

        .status-icon {
            font-size: 18px;
        }

        .status-icon.covered { color: var(--success); }
        .status-icon.gap { color: var(--danger); }

        .tier-tag {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: 600;
        }

        .tier-tag.tier1 { background: #fee2e2; color: #991b1b; }
        .tier-tag.tier2 { background: #fef3c7; color: #92400e; }
        .tier-tag.tier3 { background: #dbeafe; color: #1e40af; }

        .validation-list {
            display: flex;
            flex-wrap: wrap;
            gap: 4px;
        }

        .validation-tag {
            background: #e2e8f0;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            color: #475569;
        }

        .no-coverage {
            color: var(--danger);
            font-style: italic;
            font-size: 13px;
        }

        .footer {
            text-align: center;
            padding: 20px;
            color: #64748b;
            font-size: 13px;
        }

        .footer a {
            color: var(--primary);
            text-decoration: none;
        }

        @media print {
            body { background: white; }
            .container { max-width: 100%; }
            .file-section { break-inside: avoid; }
        }
        """

    def _render_header(self, report: CDAAnalysisReport) -> str:
        """Render report header."""
        return f"""
        <div class="header">
            <h1>CDA Gap Analysis Report</h1>
            <div class="subtitle">{html.escape(report.job_name)} • Generated {report.analysis_timestamp.strftime('%Y-%m-%d %H:%M')}</div>
        </div>
        """

    def _render_summary(self, report: CDAAnalysisReport) -> str:
        """Render summary metrics and alerts."""
        coverage_pct = report.overall_coverage
        coverage_class = 'success' if coverage_pct >= 90 else ('warning' if coverage_pct >= 70 else 'danger')

        # Alert section
        alert_html = ""
        if report.tier1_at_risk:
            alert_html = """
            <div class="alert alert-danger">
                <span style="font-size: 24px">⚠️</span>
                <div>
                    <strong>AUDIT RISK:</strong> Tier 1 (Regulatory) Critical Data Attributes have gaps in validation coverage.
                    Immediate action required to ensure compliance.
                </div>
            </div>
            """
        elif report.has_gaps:
            alert_html = """
            <div class="alert alert-warning">
                <span style="font-size: 24px">⚠</span>
                <div>
                    <strong>Attention:</strong> Some Critical Data Attributes lack validation coverage.
                    Review gaps and add appropriate validations.
                </div>
            </div>
            """
        else:
            alert_html = """
            <div class="alert alert-success">
                <span style="font-size: 24px">✓</span>
                <div>
                    <strong>Fully Covered:</strong> All Critical Data Attributes have validation coverage.
                </div>
            </div>
            """

        return f"""
        <div class="summary-grid">
            <div class="metric-card">
                <div class="value">{report.total_cdas}</div>
                <div class="label">Total CDAs Defined</div>
            </div>
            <div class="metric-card success">
                <div class="value">{report.total_covered}</div>
                <div class="label">Covered</div>
            </div>
            <div class="metric-card {'danger' if report.total_gaps > 0 else 'success'}">
                <div class="value">{report.total_gaps}</div>
                <div class="label">Gaps</div>
            </div>
            <div class="metric-card {coverage_class}">
                <div class="value">{coverage_pct:.0f}%</div>
                <div class="label">Overall Coverage</div>
            </div>
        </div>
        {alert_html}
        """

    def _render_file_results(self, report: CDAAnalysisReport) -> str:
        """Render results for each file."""
        sections = []
        for result in report.results:
            sections.append(self._render_file_section(result))
        return '\n'.join(sections)

    def _render_file_section(self, result: CDAGapResult) -> str:
        """Render section for a single file."""
        coverage_pct = result.coverage_percentage
        badge_class = 'high' if coverage_pct >= 90 else ('medium' if coverage_pct >= 70 else 'low')

        # Tier breakdown
        tier_html = self._render_tier_breakdown(result)

        # Field table
        field_rows = []
        for fc in sorted(result.field_coverage, key=lambda x: (x.cda.tier.priority, not x.is_covered)):
            field_rows.append(self._render_field_row(fc))

        return f"""
        <div class="file-section">
            <div class="file-header">
                <h2>📄 {html.escape(result.file_name)}</h2>
                <span class="coverage-badge {badge_class}">{coverage_pct:.0f}% Coverage</span>
            </div>
            {tier_html}
            <table class="field-table">
                <thead>
                    <tr>
                        <th style="width: 40px">Status</th>
                        <th>Field</th>
                        <th style="width: 100px">Tier</th>
                        <th>Validation Coverage</th>
                        <th>Description</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(field_rows)}
                </tbody>
            </table>
        </div>
        """

    def _render_tier_breakdown(self, result: CDAGapResult) -> str:
        """Render tier-by-tier coverage breakdown."""
        tier_boxes = []
        for tier in [CDATier.TIER_1, CDATier.TIER_2, CDATier.TIER_3]:
            stats = result.get_tier_coverage(tier)
            total = stats['total']
            covered = stats['covered']
            gaps = stats['gaps']

            if total == 0:
                stat_text = "N/A"
            else:
                pct = (covered / total) * 100
                stat_text = f"{covered}/{total} ({pct:.0f}%)"

            tier_class = f"tier{tier.priority}"
            tier_boxes.append(f"""
            <div class="tier-box {tier_class}">
                <div class="tier-name">{tier.display_name} (Tier {tier.priority})</div>
                <div class="tier-stat">{stat_text}</div>
            </div>
            """)

        return f"""<div class="tier-grid">{''.join(tier_boxes)}</div>"""

    def _render_field_row(self, fc: CDAFieldCoverage) -> str:
        """Render a single field row in the coverage table."""
        status_icon = "✓" if fc.is_covered else "✗"
        status_class = "covered" if fc.is_covered else "gap"
        tier_class = f"tier{fc.cda.tier.priority}"

        if fc.is_covered:
            coverage_html = f"""
            <div class="validation-list">
                {''.join(f'<span class="validation-tag">{html.escape(v)}</span>' for v in fc.covering_validations)}
            </div>
            """
        else:
            coverage_html = '<span class="no-coverage">No validation coverage</span>'

        return f"""
        <tr>
            <td><span class="status-icon {status_class}">{status_icon}</span></td>
            <td><strong>{html.escape(fc.cda.field)}</strong></td>
            <td><span class="tier-tag {tier_class}">{fc.cda.tier.display_name}</span></td>
            <td>{coverage_html}</td>
            <td>{html.escape(fc.cda.description)}</td>
        </tr>
        """

    def _render_footer(self, report: CDAAnalysisReport) -> str:
        """Render report footer."""
        return f"""
        <div class="footer">
            Generated by <a href="https://github.com/danieledge/data-validation-tool">DataK9</a> •
            {report.analysis_timestamp.strftime('%Y-%m-%d %H:%M:%S')}
        </div>
        """

    def save_html(self, report: CDAAnalysisReport, filepath: str) -> None:
        """
        Save HTML report to file.

        Args:
            report: CDA analysis report
            filepath: Output file path
        """
        html_content = self.generate_html(report)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
