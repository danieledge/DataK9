"""
Documentation Generator for Validation Configurations.

This module generates comprehensive technical documentation from validation
configurations in HTML and Markdown formats, suitable for auditors, technical
staff, and stakeholders.

Author: Daniel Edge
"""

from typing import Dict, Any, List
from datetime import datetime


class DocumentGenerator:
    """
    Generates technical documentation from validation configurations.

    Supports HTML and Markdown output formats with configurable sections
    including cover page, table of contents, executive summary, detailed
    specifications, and severity matrix.
    """

    def __init__(self, config):
        """
        Initialize document generator with validation configuration.

        Args:
            config: ValidationConfig object containing validation rules
        """
        self.config = config

    def generate_html(self, title: str, options: Dict[str, bool], theme: str = 'dark') -> str:
        """
        Generate HTML documentation.

        Args:
            title: Document title
            options: Dictionary of section options (include_toc, include_summary, etc.)
            theme: Theme to use ('dark' or 'light'), defaults to 'dark'

        Returns:
            Complete HTML document as string
        """
        date = datetime.now().strftime('%B %d, %Y')
        stats = self._calculate_statistics()

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self._escape_html(title)} - DataK9 Documentation</title>
    <style>
        /* Modern Dark Theme - Matching Executive Profiler Report */
        :root {{
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
            --info: #3b82f6;
            --info-soft: rgba(59, 130, 246, 0.1);
            --accent: #8b5cf6;
            --accent-soft: rgba(139, 92, 246, 0.1);
        }}

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            min-height: 100vh;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }}

        /* Header - Matching Executive Style */
        .header {{
            background: linear-gradient(135deg, var(--bg-secondary) 0%, #0f172a 100%);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 32px;
            margin-bottom: 24px;
            position: relative;
            overflow: hidden;
        }}

        .header::before {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, var(--accent) 0%, var(--info) 50%, var(--success) 100%);
        }}

        .header-top {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            flex-wrap: wrap;
            gap: 16px;
            margin-bottom: 16px;
        }}

        .brand {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}

        .brand-icon {{
            width: 48px;
            height: 48px;
            background: linear-gradient(135deg, var(--accent), var(--info));
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 24px;
        }}

        .brand-text {{
            font-size: 24px;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent), var(--info));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}

        .header-meta {{
            display: flex;
            gap: 16px;
            flex-wrap: wrap;
            color: var(--text-muted);
            font-size: 13px;
        }}

        .header-meta span {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}

        .report-title {{
            font-size: 28px;
            font-weight: 700;
            margin-bottom: 8px;
        }}

        .report-subtitle {{
            color: var(--text-secondary);
            font-size: 14px;
        }}

        /* KPI Belt */
        .kpi-belt {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}

        .kpi-card {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            transition: transform 0.2s, box-shadow 0.2s;
        }}

        .kpi-card:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
        }}

        .kpi-value {{
            font-size: 36px;
            font-weight: 700;
            line-height: 1;
            margin-bottom: 8px;
        }}

        .kpi-label {{
            font-size: 12px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .kpi-card.files .kpi-value {{ color: var(--info); }}
        .kpi-card.validations .kpi-value {{ color: var(--accent); }}
        .kpi-card.errors .kpi-value {{ color: var(--error); }}
        .kpi-card.warnings .kpi-value {{ color: var(--warning); }}

        /* Accordion Sections */
        .accordion {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            margin-bottom: 16px;
            overflow: hidden;
        }}

        .accordion-header {{
            padding: 20px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            transition: background 0.2s;
            gap: 16px;
        }}

        .accordion-header:hover {{
            background: rgba(255, 255, 255, 0.02);
        }}

        .accordion-title-group {{
            display: flex;
            align-items: center;
            gap: 16px;
        }}

        .accordion-icon {{
            width: 40px;
            height: 40px;
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 20px;
        }}

        .accordion-icon.file {{ background: var(--info-soft); }}
        .accordion-icon.matrix {{ background: var(--accent-soft); }}
        .accordion-icon.toc {{ background: var(--success-soft); }}

        .accordion-title {{
            font-size: 16px;
            font-weight: 600;
        }}

        .accordion-subtitle {{
            font-size: 13px;
            color: var(--text-muted);
        }}

        .accordion-meta {{
            display: flex;
            gap: 12px;
            align-items: center;
        }}

        .accordion-badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
        }}

        .accordion-badge.error {{ background: var(--error-soft); color: var(--error); }}
        .accordion-badge.warning {{ background: var(--warning-soft); color: var(--warning); }}
        .accordion-badge.info {{ background: var(--info-soft); color: var(--info); }}

        .accordion-chevron {{
            color: var(--text-muted);
            transition: transform 0.3s;
            font-size: 20px;
        }}

        .accordion.open .accordion-chevron {{
            transform: rotate(180deg);
        }}

        .accordion-content {{
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
        }}

        .accordion.open .accordion-content {{
            max-height: 5000px;
        }}

        .accordion-body {{
            padding: 0 24px 24px 24px;
        }}

        /* Validation Cards */
        .validation-card {{
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 16px 20px;
            margin-bottom: 12px;
            border-left: 3px solid var(--border);
        }}

        .validation-card.error {{ border-left-color: var(--error); }}
        .validation-card.warning {{ border-left-color: var(--warning); }}

        .validation-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }}

        .validation-type {{
            font-size: 15px;
            font-weight: 600;
        }}

        .severity-badge {{
            padding: 4px 10px;
            border-radius: 10px;
            font-size: 10px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .severity-badge.error {{ background: var(--error-soft); color: var(--error); }}
        .severity-badge.warning {{ background: var(--warning-soft); color: var(--warning); }}

        .validation-description {{
            font-size: 13px;
            color: var(--text-secondary);
            margin-bottom: 10px;
        }}

        .validation-params {{
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 12px;
            font-family: 'SF Mono', Monaco, monospace;
            font-size: 12px;
            color: var(--text-secondary);
            line-height: 1.5;
        }}

        /* Table Styling */
        .data-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}

        .data-table th {{
            background: var(--bg-primary);
            padding: 12px 16px;
            text-align: left;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 0.5px;
            border-bottom: 1px solid var(--border);
        }}

        .data-table td {{
            padding: 12px 16px;
            border-bottom: 1px solid var(--border);
        }}

        .data-table tr:hover {{
            background: rgba(255, 255, 255, 0.02);
        }}

        .data-table tr:last-child td {{
            border-bottom: none;
        }}

        /* File Info Row */
        .file-info {{
            display: flex;
            gap: 20px;
            margin-bottom: 16px;
            padding: 12px 16px;
            background: var(--bg-primary);
            border-radius: 8px;
            font-size: 13px;
            color: var(--text-secondary);
        }}

        .file-info span {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}

        /* Footer */
        .footer {{
            text-align: center;
            padding: 24px;
            color: var(--text-muted);
            font-size: 12px;
            border-top: 1px solid var(--border);
            margin-top: 24px;
        }}

        .footer a {{
            color: var(--accent);
            text-decoration: none;
        }}

        /* Print Styles */
        @media print {{
            body {{ background: white; color: black; }}
            .header {{ background: #f8f9fa; }}
            .accordion {{ break-inside: avoid; }}
            .accordion-content {{ max-height: none !important; }}
        }}

        /* Responsive */
        @media (max-width: 768px) {{
            .container {{ padding: 16px; }}
            .header {{ padding: 20px; }}
            .kpi-belt {{ grid-template-columns: repeat(2, 1fr); }}
            .kpi-value {{ font-size: 28px; }}
            .report-title {{ font-size: 22px; }}
        }}
    </style>
    <script>
        function toggleAccordion(id) {{
            const accordion = document.getElementById(id);
            accordion.classList.toggle('open');
        }}
    </script>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header class="header">
            <div class="header-top">
                <div class="brand">
                    <div class="brand-icon">K9</div>
                    <span class="brand-text">DataK9</span>
                </div>
                <div class="header-meta">
                    <span>Generated: {date}</span>
                    <span>v{self._escape_html(self.config.version or '1.0')}</span>
                </div>
            </div>
            <h1 class="report-title">{self._escape_html(title)}</h1>
            <p class="report-subtitle">Technical Validation Specification for {self._escape_html(self.config.job_name)}</p>
        </header>

        <!-- KPI Belt -->
        <section class="kpi-belt">
            <div class="kpi-card files">
                <div class="kpi-value">{stats['file_count']}</div>
                <div class="kpi-label">Files</div>
            </div>
            <div class="kpi-card validations">
                <div class="kpi-value">{stats['total_validations']}</div>
                <div class="kpi-label">Validations</div>
            </div>
            <div class="kpi-card errors">
                <div class="kpi-value">{stats['error_count']}</div>
                <div class="kpi-label">Error Checks</div>
            </div>
            <div class="kpi-card warnings">
                <div class="kpi-value">{stats['warning_count']}</div>
                <div class="kpi-label">Warning Checks</div>
            </div>
        </section>
"""

        # Detailed Specifications
        if options.get('include_specs', True):
            html += self._generate_html_specs()

        # Severity Matrix
        if options.get('include_matrix', True):
            html += self._generate_html_matrix()

        # Footer
        html += f"""
        <!-- Footer -->
        <footer class="footer">
            <p>Generated by <strong>DataK9</strong> Data Quality Framework</p>
            <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </footer>
    </div>
</body>
</html>
"""

        return html

    def _generate_html_toc(self, options: Dict[str, bool]) -> str:
        """Generate HTML table of contents."""
        html = """
        <div class="toc">
            <h2>Table of Contents</h2>
            <ul>
"""

        if options.get('include_summary', True):
            html += '                <li><a href="#summary">1. Executive Summary</a></li>\n'

        if options.get('include_specs', True):
            html += '                <li><a href="#specifications">2. Validation Specifications</a></li>\n'
            for idx, file_config in enumerate(self.config.files):
                html += f'                    <li style="padding-left: 20px;"><a href="#file-{idx}">2.{idx + 1} {self._escape_html(file_config.name)}</a></li>\n'

        if options.get('include_matrix', True):
            html += '                <li><a href="#matrix">3. Severity Matrix</a></li>\n'

        html += """
            </ul>
        </div>
"""
        return html

    def _generate_html_summary(self) -> str:
        """Generate HTML executive summary."""
        stats = self._calculate_statistics()

        html = f"""
        <div id="summary">
            <h2>1. Executive Summary</h2>
            <p>This document specifies the data validation requirements for the <strong>{self._escape_html(self.config.job_name)}</strong> project.</p>

            <div class="summary-grid">
                <div class="summary-stat">
                    <div class="label">Files</div>
                    <div class="value">{stats['file_count']}</div>
                </div>
                <div class="summary-stat">
                    <div class="label">Validations</div>
                    <div class="value">{stats['total_validations']}</div>
                </div>
                <div class="summary-stat">
                    <div class="label">Error Checks</div>
                    <div class="value">{stats['error_count']}</div>
                </div>
                <div class="summary-stat">
                    <div class="label">Warning Checks</div>
                    <div class="value">{stats['warning_count']}</div>
                </div>
            </div>

            <h3>Validation Overview</h3>
            <p>The validation framework implements <strong>{stats['total_validations']} validation rules</strong> across <strong>{stats['file_count']} data file(s)</strong>. These validations ensure data quality, completeness, and conformance to business rules before data loading.</p>

            <h3>Severity Breakdown</h3>
            <ul>
                <li><strong>ERROR:</strong> {stats['error_count']} validation(s) - Critical checks that must pass</li>
                <li><strong>WARNING:</strong> {stats['warning_count']} validation(s) - Quality checks that should be reviewed</li>
            </ul>
        </div>
"""
        return html

    def _generate_html_specs(self) -> str:
        """Generate HTML detailed specifications with accordion sections."""
        html = ""

        for file_idx, file_config in enumerate(self.config.files):
            validations = self._get_attr(file_config, 'validations', [])
            # Count errors and warnings for this file
            error_count = sum(1 for v in validations if self._get_severity_value(v) == 'ERROR')
            warning_count = sum(1 for v in validations if self._get_severity_value(v) == 'WARNING')

            file_name = self._get_attr(file_config, 'name', 'Unnamed File')
            file_path = self._get_attr(file_config, 'path', 'Path not specified')
            file_format = self._get_attr(file_config, 'format', 'AUTO')
            file_format_display = file_format.upper() if file_format else 'AUTO'

            html += f"""
        <!-- File: {self._escape_html(file_name)} -->
        <div class="accordion open" id="file-accordion-{file_idx}">
            <div class="accordion-header" onclick="toggleAccordion('file-accordion-{file_idx}')">
                <div class="accordion-title-group">
                    <div class="accordion-icon file">📁</div>
                    <div>
                        <div class="accordion-title">{self._escape_html(file_name)}</div>
                        <div class="accordion-subtitle">{self._escape_html(file_path)}</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-badge info">{len(validations)} checks</span>
"""
            if error_count > 0:
                html += f'                    <span class="accordion-badge error">{error_count} errors</span>\n'
            if warning_count > 0:
                html += f'                    <span class="accordion-badge warning">{warning_count} warnings</span>\n'

            html += f"""                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-content">
                <div class="accordion-body">
                    <div class="file-info">
                        <span>📊 Format: {self._escape_html(file_format_display)}</span>
                        <span>✓ {len(validations)} validations configured</span>
                    </div>
"""

            if validations:
                for val in validations:
                    severity = self._get_severity_value(val).lower()
                    val_type = self._get_attr(val, 'type', 'Unknown')
                    val_desc = self._get_validation_description(val)
                    val_params = self._get_attr(val, 'params', {})

                    # Format parameters
                    params_html = ""
                    if val_params:
                        param_items = []
                        for k, v in val_params.items():
                            if isinstance(v, (list, dict)):
                                import json
                                v_str = json.dumps(v)[:80] + ('...' if len(json.dumps(v)) > 80 else '')
                            else:
                                v_str = str(v)
                            param_items.append(f"{k}: {self._escape_html(v_str)}")
                        params_html = " | ".join(param_items)

                    html += f"""
                    <div class="validation-card {severity}">
                        <div class="validation-header">
                            <span class="validation-type">{self._escape_html(val_type)}</span>
                            <span class="severity-badge {severity}">{severity.upper()}</span>
                        </div>
                        <p class="validation-description">{self._escape_html(val_desc)}</p>
"""

                    if params_html:
                        html += f"""                        <div class="validation-params">{params_html}</div>
"""

                    html += """                    </div>
"""
            else:
                html += """
                    <p style="color: var(--text-muted); font-style: italic;">No validations configured for this file.</p>
"""

            html += """
                </div>
            </div>
        </div>
"""

        return html

    def _generate_html_matrix(self) -> str:
        """Generate HTML severity matrix with accordion style."""
        html = """
        <!-- Severity Matrix -->
        <div class="accordion" id="matrix-accordion">
            <div class="accordion-header" onclick="toggleAccordion('matrix-accordion')">
                <div class="accordion-title-group">
                    <div class="accordion-icon matrix">📋</div>
                    <div>
                        <div class="accordion-title">Severity Matrix</div>
                        <div class="accordion-subtitle">Summary of all validations by file and severity</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-content">
                <div class="accordion-body">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>File</th>
                                <th>Validation Type</th>
                                <th>Severity</th>
                                <th>Description</th>
                            </tr>
                        </thead>
                        <tbody>
"""

        for file_config in self.config.files:
            file_name = self._get_attr(file_config, 'name', 'Unnamed')
            validations = self._get_attr(file_config, 'validations', [])
            for val in validations:
                severity = self._get_severity_value(val).lower()
                val_type = self._get_attr(val, 'type', 'Unknown')
                val_desc = self._get_validation_description(val)
                html += f"""
                            <tr>
                                <td>{self._escape_html(file_name)}</td>
                                <td>{self._escape_html(val_type)}</td>
                                <td><span class="severity-badge {severity}">{severity.upper()}</span></td>
                                <td>{self._escape_html(val_desc)}</td>
                            </tr>
"""

        html += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
"""
        return html

    def generate_markdown(self, title: str, options: Dict[str, bool]) -> str:
        """
        Generate Markdown documentation.

        Args:
            title: Document title
            options: Dictionary of section options

        Returns:
            Complete Markdown document as string
        """
        date = datetime.now().strftime('%B %d, %Y')

        md = f"# {title}\n\n"
        md += "**Technical Validation Specification**\n\n"
        md += f"- **Generated:** {date}\n"
        md += f"- **Project:** {self.config.job_name}\n"
        md += f"- **Version:** {self.config.version or '1.0'}\n\n"
        md += "---\n\n"

        # Table of Contents
        if options.get('include_toc', True):
            md += self._generate_markdown_toc(options)

        # Executive Summary
        if options.get('include_summary', True):
            md += self._generate_markdown_summary()

        # Detailed Specifications
        if options.get('include_specs', True):
            md += self._generate_markdown_specs()

        # Severity Matrix
        if options.get('include_matrix', True):
            md += self._generate_markdown_matrix()

        # Footer
        md += "\n---\n\n"
        md += "*Generated by DataK9 Data Quality Framework*\n"
        md += "*Your K9 guardian for data quality*\n"
        md += f"*Document created: {datetime.now().isoformat()}*\n"

        return md

    def _generate_markdown_toc(self, options: Dict[str, bool]) -> str:
        """Generate Markdown table of contents."""
        md = "## Table of Contents\n\n"

        if options.get('include_summary', True):
            md += "1. [Executive Summary](#1-executive-summary)\n"

        if options.get('include_specs', True):
            md += "2. [Validation Specifications](#2-validation-specifications)\n"
            for idx, file_config in enumerate(self.config.files):
                anchor = self._sanitize_anchor(file_config.name)
                md += f"   - [{file_config.name}](#2{idx + 1}-{anchor})\n"

        if options.get('include_matrix', True):
            md += "3. [Severity Matrix](#3-severity-matrix)\n"

        md += "\n"
        return md

    def _generate_markdown_summary(self) -> str:
        """Generate Markdown executive summary."""
        stats = self._calculate_statistics()

        md = "## 1. Executive Summary\n\n"
        md += f"This document specifies the data validation requirements for the **{self.config.job_name}** project.\n\n"
        md += "### Key Statistics\n\n"
        md += "| Metric | Count |\n"
        md += "|--------|-------|\n"
        md += f"| Files | {stats['file_count']} |\n"
        md += f"| Total Validations | {stats['total_validations']} |\n"
        md += f"| Error Checks | {stats['error_count']} |\n"
        md += f"| Warning Checks | {stats['warning_count']} |\n\n"
        md += "### Overview\n\n"
        md += f"The validation framework implements **{stats['total_validations']} validation rules** across **{stats['file_count']} data file(s)**. These validations ensure data quality, completeness, and conformance to business rules before data loading.\n\n"
        md += "### Severity Breakdown\n\n"
        md += f"- **ERROR:** {stats['error_count']} validation(s) - Critical checks that must pass\n"
        md += f"- **WARNING:** {stats['warning_count']} validation(s) - Quality checks that should be reviewed\n\n"

        return md

    def _generate_markdown_specs(self) -> str:
        """Generate Markdown detailed specifications."""
        md = "## 2. Validation Specifications\n\n"

        for file_idx, file_config in enumerate(self.config.files):
            md += f"### 2.{file_idx + 1} {file_config.name}\n\n"
            md += f"- **File Path:** `{file_config.path or 'Not specified'}`\n"
            md += f"- **Format:** {file_config.format.upper() if file_config.format else 'AUTO'}\n"
            md += f"- **Validations:** {len(file_config.validations)}\n\n"

            if file_config.validations:
                for val_idx, val in enumerate(file_config.validations):
                    md += f"#### {val_idx + 1}. {val.name}\n\n"
                    md += f"- **Severity:** `{val.severity.value}`\n"
                    md += f"- **Description:** {val.get_description()}\n"

                    if val.params:
                        import json
                        params_str = json.dumps(val.params, indent=2)
                        md += "- **Parameters:**\n\n"
                        md += f"```json\n{params_str}\n```\n"

                    md += "\n"
            else:
                md += "*No validations configured for this file.*\n\n"

        return md

    def _generate_markdown_matrix(self) -> str:
        """Generate Markdown severity matrix."""
        md = "## 3. Severity Matrix\n\n"
        md += "| File | Validation Type | Severity | Description |\n"
        md += "|------|----------------|----------|-------------|\n"

        for file_config in self.config.files:
            for val in file_config.validations:
                md += f"| {file_config.name} | {val.name} | `{val.severity.value}` | {val.get_description()} |\n"

        md += "\n"
        return md

    def _calculate_statistics(self) -> Dict[str, int]:
        """Calculate validation statistics."""
        error_count = 0
        warning_count = 0
        total_validations = 0

        for file_config in self.config.files:
            validations = self._get_attr(file_config, 'validations', [])
            total_validations += len(validations)
            for val in validations:
                severity = self._get_attr(val, 'severity')
                severity_value = severity.value if hasattr(severity, 'value') else str(severity)
                if severity_value == 'ERROR':
                    error_count += 1
                elif severity_value == 'WARNING':
                    warning_count += 1

        return {
            'file_count': len(self.config.files),
            'total_validations': total_validations,
            'error_count': error_count,
            'warning_count': warning_count
        }

    def _get_attr(self, obj, attr: str, default=None):
        """Get attribute from object or dict."""
        if isinstance(obj, dict):
            return obj.get(attr, default)
        return getattr(obj, attr, default)

    def _get_severity_value(self, val) -> str:
        """Get severity value as string from validation object or dict."""
        severity = self._get_attr(val, 'severity')
        if hasattr(severity, 'value'):
            return severity.value
        return str(severity)

    def _get_validation_description(self, val) -> str:
        """Get description from validation, handling both object and dict."""
        desc = self._get_attr(val, 'description', '')
        if desc:
            return desc
        # Try get_description method for objects
        if hasattr(val, 'get_description'):
            return val.get_description()
        return self._get_attr(val, 'type', 'Validation check')

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        if not text:
            return ''
        return (str(text)
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))

    def _sanitize_anchor(self, text: str) -> str:
        """Sanitize text for use in anchor links."""
        return text.lower().replace(' ', '-').replace('_', '-')
