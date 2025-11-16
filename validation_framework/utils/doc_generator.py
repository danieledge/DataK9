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

        # Generate theme-specific CSS
        theme_css = self._get_theme_css(theme)

        html = f"""<!DOCTYPE html>
<html lang="en" data-theme="{theme}">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{self._escape_html(title)}</title>
    <style>
        /* Modern professional documentation styling with theme support */
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        {theme_css}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.7;
            color: var(--text-primary);
            max-width: 1200px;
            margin: 0 auto;
            padding: 40px 20px;
            background: var(--bg-page);
            transition: background-color 0.3s ease, color 0.3s ease;
        }}
        .document {{
            background: var(--bg-document);
            padding: 80px;
            box-shadow: var(--shadow-document);
            border-radius: 12px;
            position: relative;
        }}

        /* Theme toggle button */
        .theme-toggle {{
            position: fixed;
            top: 20px;
            right: 20px;
            background: var(--bg-accent);
            border: 2px solid var(--border-color);
            color: var(--text-primary);
            padding: 12px 24px;
            border-radius: 25px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            box-shadow: var(--shadow-button);
            transition: all 0.3s ease;
            z-index: 1000;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .theme-toggle:hover {{
            transform: translateY(-2px);
            box-shadow: var(--shadow-button-hover);
        }}

        .header {{
            padding: 32px 0 24px 0;
            border-bottom: 2px solid var(--accent-primary);
            margin-bottom: 40px;
        }}
        .header h1 {{
            font-size: 32px;
            color: var(--text-heading);
            margin-bottom: 8px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }}
        .header .meta {{
            font-size: 13px;
            color: var(--text-tertiary);
            display: flex;
            gap: 20px;
            flex-wrap: wrap;
        }}
        .header .meta-item {{
            display: flex;
            align-items: center;
            gap: 6px;
        }}

        h2 {{
            font-size: 32px;
            color: var(--text-heading);
            margin: 40px 0 20px 0;
            padding-bottom: 10px;
            border-bottom: 3px solid var(--border-heading);
            font-weight: 700;
            letter-spacing: -0.5px;
        }}
        h3 {{
            font-size: 24px;
            color: var(--text-heading);
            margin: 30px 0 14px 0;
            font-weight: 600;
        }}
        h4 {{
            font-size: 20px;
            color: var(--accent-primary);
            margin: 24px 0 12px 0;
            font-weight: 600;
        }}
        p {{
            margin: 12px 0;
            color: var(--text-primary);
            line-height: 1.8;
        }}

        .toc {{
            background: var(--bg-card);
            padding: 40px;
            border-radius: 12px;
            margin: 40px 0;
            border: 1px solid var(--border-color);
        }}
        .toc h2 {{
            margin-top: 0;
            border-bottom: none;
        }}
        .toc ul {{
            list-style: none;
            padding-left: 0;
        }}
        .toc li {{
            padding: 12px 0;
            border-bottom: 1px solid var(--border-subtle);
        }}
        .toc li:last-child {{
            border-bottom: none;
        }}
        .toc a {{
            color: var(--accent-primary);
            text-decoration: none;
            font-weight: 500;
            transition: color 0.2s ease;
        }}
        .toc a:hover {{
            color: var(--accent-hover);
            text-decoration: underline;
        }}

        .summary-compact {{
            display: flex;
            gap: 24px;
            padding: 16px 20px;
            background: var(--bg-card);
            border-radius: 8px;
            border-left: 4px solid var(--accent-primary);
            margin-bottom: 32px;
            flex-wrap: wrap;
        }}
        .summary-stat {{
            display: flex;
            align-items: baseline;
            gap: 8px;
        }}
        .summary-stat .label {{
            font-size: 13px;
            color: var(--text-tertiary);
            font-weight: 500;
        }}
        .summary-stat .value {{
            font-size: 20px;
            font-weight: 700;
            color: var(--accent-primary);
        }}

        .file-section {{
            margin: 32px 0;
            padding: 28px;
            background: var(--bg-card);
            border-radius: 10px;
            border-left: 4px solid var(--accent-primary);
            border: 1px solid var(--border-color);
            border-left: 4px solid var(--accent-primary);
        }}
        .validation-card {{
            background: var(--bg-card);
            padding: 20px;
            margin: 16px 0;
            border-radius: 8px;
            border-left: 3px solid var(--border-color);
        }}
        .validation-card.error {{ border-left-color: var(--error-color); }}
        .validation-card.warning {{ border-left-color: var(--warning-color); }}

        .validation-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
        }}
        .validation-type {{
            font-size: 18px;
            font-weight: 600;
            color: var(--text-heading);
        }}
        .severity-badge {{
            padding: 6px 14px;
            border-radius: 16px;
            font-size: 11px;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }}
        .severity-badge.error {{
            background: var(--error-color);
            color: var(--error-text);
        }}
        .severity-badge.warning {{
            background: var(--warning-color);
            color: var(--warning-text);
        }}

        .params {{
            background: var(--bg-code);
            padding: 16px;
            border-radius: 8px;
            font-family: 'SF Mono', 'Monaco', 'Inconsolata', 'Roboto Mono', 'Courier New', monospace;
            font-size: 14px;
            color: var(--text-code);
            white-space: pre-wrap;
            line-height: 1.6;
            border: 1px solid var(--border-color);
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 24px 0;
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid var(--border-color);
        }}
        th, td {{
            padding: 16px;
            text-align: left;
            border-bottom: 1px solid var(--border-table);
        }}
        th {{
            background: var(--accent-primary);
            color: white;
            font-weight: 700;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        tr:hover {{
            background: var(--bg-hover);
        }}
        tr:last-child td {{
            border-bottom: none;
        }}

        .footer {{
            margin-top: 60px;
            padding-top: 30px;
            border-top: 2px solid var(--border-color);
            text-align: center;
            color: var(--text-tertiary);
            font-size: 13px;
        }}
        .footer p {{
            margin: 8px 0;
        }}

        @media print {{
            body {{ background: white !important; }}
            .document {{ box-shadow: none; padding: 20px; }}
            .cover {{ page-break-after: always; }}
            .file-section {{ page-break-inside: avoid; }}
            .theme-toggle {{ display: none; }}
        }}

        @media (max-width: 768px) {{
            .document {{ padding: 40px 24px; }}
            .cover {{ padding: 60px 24px; margin: -40px -24px 60px -24px; }}
            .cover h1 {{ font-size: 36px; }}
            h2 {{ font-size: 28px; }}
            h3 {{ font-size: 22px; }}
            .summary-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
    <script>
        // Theme toggle functionality
        function toggleTheme() {{
            const html = document.documentElement;
            const currentTheme = html.getAttribute('data-theme');
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
            html.setAttribute('data-theme', newTheme);

            // Update button text
            const button = document.querySelector('.theme-toggle');
            button.innerHTML = newTheme === 'dark'
                ? '<span>☀️</span> Light Mode'
                : '<span>🌙</span> Dark Mode';

            // Save preference
            localStorage.setItem('doc-theme', newTheme);
        }}

        // Load saved theme preference on page load
        window.addEventListener('DOMContentLoaded', function() {{
            const savedTheme = localStorage.getItem('doc-theme');
            if (savedTheme) {{
                document.documentElement.setAttribute('data-theme', savedTheme);
                const button = document.querySelector('.theme-toggle');
                button.innerHTML = savedTheme === 'dark'
                    ? '<span>☀️</span> Light Mode'
                    : '<span>🌙</span> Dark Mode';
            }}
        }});
    </script>
</head>
<body>
    <button class="theme-toggle" onclick="toggleTheme()">
        <span>☀️</span> Light Mode
    </button>

    <div class="document">
"""

        # Compact Header
        stats = self._calculate_statistics()
        html += f"""
        <div class="header">
            <h1>{self._escape_html(title)}</h1>
            <div class="meta">
                <div class="meta-item">
                    <span>📅</span>
                    <span>{date}</span>
                </div>
                <div class="meta-item">
                    <span>📦</span>
                    <span>{self._escape_html(self.config.job_name)}</span>
                </div>
                <div class="meta-item">
                    <span>🏷️</span>
                    <span>v{self._escape_html(self.config.version or '1.0')}</span>
                </div>
            </div>
        </div>
"""

        # Compact Summary
        if options.get('include_summary', True):
            html += f"""
        <div class="summary-compact">
            <div class="summary-stat">
                <span class="label">Files:</span>
                <span class="value">{stats['file_count']}</span>
            </div>
            <div class="summary-stat">
                <span class="label">Validations:</span>
                <span class="value">{stats['total_validations']}</span>
            </div>
            <div class="summary-stat">
                <span class="label">Errors:</span>
                <span class="value">{stats['error_count']}</span>
            </div>
            <div class="summary-stat">
                <span class="label">Warnings:</span>
                <span class="value">{stats['warning_count']}</span>
            </div>
        </div>
"""

        # Detailed Specifications
        if options.get('include_specs', True):
            html += self._generate_html_specs()

        # Severity Matrix
        if options.get('include_matrix', True):
            html += self._generate_html_matrix()

        # Footer
        html += f"""
        <div class="footer">
            <p>Generated by DataK9 · {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        </div>
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
        """Generate HTML detailed specifications."""
        html = """
        <div id="specifications">
            <h2>Validation Specifications</h2>
"""

        for file_idx, file_config in enumerate(self.config.files):
            html += f"""
            <div class="file-section" id="file-{file_idx}">
                <h3>{self._escape_html(file_config.name)}</h3>
                <div style="display: flex; gap: 24px; margin-bottom: 16px; font-size: 13px; color: var(--text-secondary);">
                    <span>📁 {self._escape_html(file_config.path or 'Not specified')}</span>
                    <span>📊 {self._escape_html(file_config.format.upper() if file_config.format else 'AUTO')}</span>
                    <span>✓ {len(file_config.validations)} validations</span>
                </div>
"""

            if file_config.validations:
                for val in file_config.validations:
                    severity = val.severity.value.lower()

                    # Format parameters more concisely
                    params_html = ""
                    if val.params:
                        param_items = []
                        for k, v in val.params.items():
                            if isinstance(v, (list, dict)):
                                import json
                                v_str = json.dumps(v)[:100] + ('...' if len(json.dumps(v)) > 100 else '')
                            else:
                                v_str = str(v)
                            param_items.append(f"<span style='color: var(--text-tertiary);'>{k}:</span> {self._escape_html(v_str)}")
                        params_html = " • ".join(param_items)

                    html += f"""
                <div class="validation-card {severity}">
                    <div class="validation-header">
                        <div class="validation-type">{self._escape_html(val.name)}</div>
                        <span class="severity-badge {severity}">{severity.upper()}</span>
                    </div>
                    <p style="margin: 8px 0; font-size: 14px;">{self._escape_html(val.get_description())}</p>
"""

                    if params_html:
                        html += f"""
                    <div style="margin-top: 12px; padding: 12px; background: var(--bg-code); border-radius: 6px; font-size: 13px; font-family: monospace; color: var(--text-code); line-height: 1.6;">
                        {params_html}
                    </div>
"""

                    html += """
                </div>
"""
            else:
                html += """
                <p><em>No validations configured for this file.</em></p>
"""

            html += """
            </div>
"""

        html += """
        </div>
"""
        return html

    def _generate_html_matrix(self) -> str:
        """Generate HTML severity matrix."""
        html = """
        <div id="matrix">
            <h2>3. Severity Matrix</h2>
            <p>Summary of all validations organized by file and severity level.</p>

            <table>
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
            for val in file_config.validations:
                severity = val.severity.value.lower()
                html += f"""
                    <tr>
                        <td>{self._escape_html(file_config.name)}</td>
                        <td>{self._escape_html(val.name)}</td>
                        <td><span class="severity-badge {severity}">{val.severity.value}</span></td>
                        <td>{self._escape_html(val.get_description())}</td>
                    </tr>
"""

        html += """
                </tbody>
            </table>
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
            total_validations += len(file_config.validations)
            for val in file_config.validations:
                if val.severity.value == 'ERROR':
                    error_count += 1
                elif val.severity.value == 'WARNING':
                    warning_count += 1

        return {
            'file_count': len(self.config.files),
            'total_validations': total_validations,
            'error_count': error_count,
            'warning_count': warning_count
        }

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

    def _get_theme_css(self, theme: str) -> str:
        """
        Generate CSS variables for the specified theme.

        Args:
            theme: Theme name ('dark' or 'light')

        Returns:
            CSS variable definitions
        """
        if theme == 'dark':
            return """
        /* Dark Theme - Modern & Elegant */
        :root[data-theme="dark"] {{
            /* Background colors */
            --bg-page: #0d1117;
            --bg-document: #161b22;
            --bg-cover: linear-gradient(135deg, #1a1f2e 0%, #2d3748 100%);
            --bg-card: #1c2128;
            --bg-code: #0d1117;
            --bg-hover: #21262d;
            --bg-accent: #238636;

            /* Text colors */
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --text-tertiary: #6e7681;
            --text-heading: #f0f6fc;
            --text-code: #79c0ff;

            /* Accent colors */
            --accent-primary: #4A90E2;
            --accent-hover: #6ba3e8;

            /* Status colors */
            --error-color: #f85149;
            --error-text: #ffffff;
            --warning-color: #d29922;
            --warning-text: #ffffff;

            /* Border colors */
            --border-color: #30363d;
            --border-subtle: #21262d;
            --border-heading: #30363d;
            --border-table: #21262d;

            /* Shadows */
            --shadow-document: 0 16px 48px rgba(0, 0, 0, 0.6);
            --shadow-button: 0 4px 12px rgba(0, 0, 0, 0.4);
            --shadow-button-hover: 0 8px 24px rgba(0, 0, 0, 0.6);
            --shadow-hover: 0 8px 24px rgba(0, 0, 0, 0.4);
        }}
        """
        else:  # light theme
            return """
        /* Light Theme - Clean & Professional */
        :root[data-theme="light"] {{
            /* Background colors */
            --bg-page: #f6f8fa;
            --bg-document: #ffffff;
            --bg-cover: linear-gradient(135deg, #e3f2fd 0%, #f5f5f5 100%);
            --bg-card: #f9fafb;
            --bg-code: #f6f8fa;
            --bg-hover: #f3f4f6;
            --bg-accent: #238636;

            /* Text colors */
            --text-primary: #1f2937;
            --text-secondary: #6b7280;
            --text-tertiary: #9ca3af;
            --text-heading: #111827;
            --text-code: #0969da;

            /* Accent colors */
            --accent-primary: #4A90E2;
            --accent-hover: #357abd;

            /* Status colors */
            --error-color: #d73a49;
            --error-text: #ffffff;
            --warning-color: #f59e0b;
            --warning-text: #ffffff;

            /* Border colors */
            --border-color: #e5e7eb;
            --border-subtle: #f3f4f6;
            --border-heading: #e5e7eb;
            --border-table: #f3f4f6;

            /* Shadows */
            --shadow-document: 0 10px 30px rgba(0, 0, 0, 0.08);
            --shadow-button: 0 2px 8px rgba(0, 0, 0, 0.1);
            --shadow-button-hover: 0 4px 16px rgba(0, 0, 0, 0.15);
            --shadow-hover: 0 4px 16px rgba(0, 0, 0, 0.08);
        }}
        """
