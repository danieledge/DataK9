"""
HTML report generator for data profiling results.

Generates comprehensive, interactive HTML reports with:
- Data type inference visualization
- Statistical distributions with charts
- Quality metrics
- Correlation heatmaps
- Suggested validations
- Auto-generated configuration
"""

import json
from pathlib import Path
from typing import List
from validation_framework.profiler.profile_result import ProfileResult, ColumnProfile
import logging

logger = logging.getLogger(__name__)


class ProfileHTMLReporter:
    """Generate interactive HTML reports for profile results."""

    def generate_report(self, profile: ProfileResult, output_path: str) -> None:
        """
        Generate HTML report from profile result.

        Args:
            profile: ProfileResult to report
            output_path: Path to write HTML file
        """
        logger.info(f"Generating profile HTML report: {output_path}")

        html_content = self._generate_html(profile)

        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"Profile report written to: {output_path}")

    def _generate_html(self, profile: ProfileResult) -> str:
        """Generate complete HTML content."""

        # Prepare data for charts
        column_names = [col.name for col in profile.columns]
        completeness_scores = [col.quality.completeness for col in profile.columns]
        validity_scores = [col.quality.validity for col in profile.columns]
        quality_scores = [col.quality.overall_score for col in profile.columns]

        # Correlation data
        correlation_data = [
            {
                "source": corr.column1,
                "target": corr.column2,
                "value": corr.correlation
            }
            for corr in profile.correlations
        ]

        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Profile Report - {profile.file_name}</title>
    <!-- Chart.js Library (Self-Hosted for Offline Use) -->
    <script src="resources/js/chart.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #e0e0e0;
            padding: 20px;
            min-height: 100vh;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: #1e1e2e;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
            overflow: hidden;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 40px;
            text-align: center;
            border-bottom: 4px solid #4a5568;
        }}

        .header h1 {{
            color: #ffffff;
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
        }}

        .header .subtitle {{
            color: #e0e0e0;
            font-size: 1.1em;
            opacity: 0.9;
        }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px 40px;
            background: #2d2d44;
        }}

        .summary-card {{
            background: #1a1a2e;
            padding: 20px;
            border-radius: 12px;
            border: 2px solid #4a5568;
            transition: transform 0.3s, border-color 0.3s;
        }}

        .summary-card:hover {{
            transform: translateY(-5px);
            border-color: #667eea;
        }}

        .summary-card .label {{
            font-size: 0.85em;
            color: #a0aec0;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 8px;
        }}

        .summary-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #ffffff;
        }}

        .summary-card .subvalue {{
            font-size: 0.9em;
            color: #cbd5e0;
            margin-top: 5px;
        }}

        .quality-badge {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 1.2em;
        }}

        .quality-excellent {{
            background: #48bb78;
            color: #fff;
        }}

        .quality-good {{
            background: #4299e1;
            color: #fff;
        }}

        .quality-fair {{
            background: #ed8936;
            color: #fff;
        }}

        .quality-poor {{
            background: #f56565;
            color: #fff;
        }}

        .section {{
            padding: 40px;
            border-bottom: 2px solid #2d3748;
        }}

        .section:last-child {{
            border-bottom: none;
        }}

        .section-title {{
            font-size: 1.8em;
            color: #ffffff;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
            display: inline-block;
        }}

        .chart-container {{
            background: #2d2d44;
            padding: 30px;
            border-radius: 12px;
            margin: 20px 0;
            border: 2px solid #4a5568;
        }}

        .chart-wrapper {{
            position: relative;
            height: 400px;
        }}

        .column-grid {{
            display: grid;
            gap: 20px;
            margin-top: 20px;
        }}

        .column-card {{
            background: #2d2d44;
            border-radius: 12px;
            padding: 25px;
            border: 2px solid #4a5568;
            transition: border-color 0.3s;
        }}

        .column-card:hover {{
            border-color: #667eea;
        }}

        .column-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #4a5568;
        }}

        .column-name {{
            font-size: 1.3em;
            font-weight: bold;
            color: #ffffff;
        }}

        .type-badge {{
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.85em;
            font-weight: bold;
            text-transform: uppercase;
        }}

        .type-known {{
            background: #48bb78;
            color: white;
        }}

        .type-inferred {{
            background: #4299e1;
            color: white;
        }}

        .type-unknown {{
            background: #718096;
            color: white;
        }}

        .column-content {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }}

        .info-section {{
            background: #1a1a2e;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #4a5568;
        }}

        .info-section h4 {{
            color: #a0aec0;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 12px;
            letter-spacing: 1px;
        }}

        .info-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #2d3748;
        }}

        .info-row:last-child {{
            border-bottom: none;
        }}

        .info-label {{
            color: #cbd5e0;
            font-size: 0.9em;
        }}

        .info-value {{
            color: #ffffff;
            font-weight: 600;
        }}

        .confidence-bar {{
            width: 100%;
            height: 8px;
            background: #2d3748;
            border-radius: 4px;
            overflow: hidden;
            margin-top: 8px;
        }}

        .confidence-fill {{
            height: 100%;
            background: linear-gradient(90deg, #667eea, #764ba2);
            transition: width 0.5s;
        }}

        .issues-list {{
            list-style: none;
            margin-top: 10px;
        }}

        .issues-list li {{
            padding: 8px 12px;
            margin: 5px 0;
            background: #2d3748;
            border-left: 4px solid #ed8936;
            border-radius: 4px;
            font-size: 0.9em;
            color: #fed7aa;
        }}

        .suggestions-grid {{
            display: grid;
            gap: 15px;
            margin-top: 20px;
        }}

        .suggestion-card {{
            background: #2d2d44;
            padding: 20px;
            border-radius: 10px;
            border-left: 5px solid #667eea;
            transition: transform 0.3s;
        }}

        .suggestion-card:hover {{
            transform: translateX(5px);
        }}

        .suggestion-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }}

        .validation-type {{
            font-weight: bold;
            color: #ffffff;
            font-size: 1.1em;
        }}

        .severity-badge {{
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.8em;
            font-weight: bold;
        }}

        .severity-error {{
            background: #f56565;
            color: white;
        }}

        .severity-warning {{
            background: #ed8936;
            color: white;
        }}

        .config-section {{
            background: #2d2d44;
            padding: 25px;
            border-radius: 12px;
            margin-top: 20px;
            border: 2px solid #4a5568;
        }}

        .config-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}

        .copy-button {{
            background: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-weight: bold;
            transition: background 0.3s;
        }}

        .copy-button:hover {{
            background: #5a67d8;
        }}

        .config-code {{
            background: #1a1a2e;
            padding: 20px;
            border-radius: 8px;
            overflow-x: auto;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
            color: #e0e0e0;
            border: 1px solid #4a5568;
        }}

        .config-code pre {{
            margin: 0;
            white-space: pre-wrap;
            word-wrap: break-word;
        }}

        .command-box {{
            background: #1a1a2e;
            padding: 15px 20px;
            border-radius: 8px;
            margin-top: 15px;
            border-left: 4px solid #48bb78;
            font-family: 'Courier New', monospace;
            color: #48bb78;
            font-size: 0.95em;
        }}

        .top-values-table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}

        .top-values-table th {{
            background: #1a1a2e;
            padding: 10px;
            text-align: left;
            color: #a0aec0;
            font-size: 0.85em;
            text-transform: uppercase;
            border-bottom: 2px solid #4a5568;
        }}

        .top-values-table td {{
            padding: 8px 10px;
            border-bottom: 1px solid #2d3748;
            color: #e0e0e0;
        }}

        .percentage-bar {{
            display: inline-block;
            width: 60px;
            height: 6px;
            background: #2d3748;
            border-radius: 3px;
            overflow: hidden;
            margin-right: 8px;
            vertical-align: middle;
        }}

        .percentage-fill {{
            height: 100%;
            background: #667eea;
        }}

        .toc {{
            background: #2d2d44;
            padding: 25px 40px;
            border-bottom: 2px solid #2d3748;
        }}

        .toc h3 {{
            color: #ffffff;
            margin-bottom: 15px;
            font-size: 1.2em;
        }}

        .toc-list {{
            list-style: none;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
        }}

        .toc-list li {{
            padding: 0;
        }}

        .toc-list a {{
            color: #a0aec0;
            text-decoration: none;
            padding: 8px 12px;
            display: block;
            border-radius: 6px;
            transition: background 0.3s, color 0.3s;
        }}

        .toc-list a:hover {{
            background: #1a1a2e;
            color: #667eea;
        }}

        .toc-list a::before {{
            content: "▸ ";
            color: #667eea;
        }}

        /* Mobile Responsiveness */
        @media (max-width: 768px) {{
            body {{
                padding: 10px;
            }}

            .header h1 {{
                font-size: 1.8em;
            }}

            .header .subtitle {{
                font-size: 0.9em;
            }}

            .summary-grid {{
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
                padding: 20px;
                gap: 15px;
            }}

            .summary-card .value {{
                font-size: 1.5em;
            }}

            .section {{
                padding: 20px;
            }}

            .section-title {{
                font-size: 1.4em;
            }}

            .chart-container {{
                padding: 15px;
            }}

            .chart-wrapper {{
                height: 300px;
            }}

            .column-content {{
                grid-template-columns: 1fr;
            }}

            .column-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 10px;
            }}

            .config-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 10px;
            }}

            .top-values-table {{
                font-size: 0.85em;
            }}

            .top-values-table th,
            .top-values-table td {{
                padding: 6px 8px;
            }}

            /* Make tables scrollable on mobile */
            .config-code {{
                font-size: 0.75em;
                overflow-x: auto;
            }}

            .command-box {{
                font-size: 0.8em;
                overflow-x: auto;
            }}
        }}

        @media (max-width: 480px) {{
            .header {{
                padding: 20px;
            }}

            .header h1 {{
                font-size: 1.5em;
            }}

            .summary-grid {{
                grid-template-columns: 1fr;
            }}

            .section {{
                padding: 15px;
            }}

            .chart-wrapper {{
                height: 250px;
            }}
        }}

        @media print {{
            body {{
                background: white;
            }}
            .container {{
                box-shadow: none;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Data Profile Report</h1>
            <div class="subtitle">{profile.file_name}</div>
            <div class="subtitle" style="font-size: 0.9em; margin-top: 10px; opacity: 0.8;">
                Profiled on {profile.profiled_at.strftime('%Y-%m-%d %H:%M:%S')} •
                Processing time: {profile.processing_time_seconds:.2f}s
            </div>
        </div>

        <!-- Table of Contents -->
        <div class="toc">
            <h3>📋 Report Sections</h3>
            <ul class="toc-list">
                <li><a href="#summary">Summary</a></li>
                <li><a href="#quality-overview">Quality Overview</a></li>
                <li><a href="#column-profiles">Column Profiles</a></li>
                {f'<li><a href="#correlations">Correlations</a></li>' if profile.correlations else ''}
                <li><a href="#suggestions">Suggested Validations</a></li>
                <li><a href="#config">Generated Configuration</a></li>
            </ul>
        </div>

        <!-- Summary Section -->
        <div id="summary"></div>
        <div class="summary-grid">
            <div class="summary-card">
                <div class="label">File Size</div>
                <div class="value">{self._format_file_size(profile.file_size_bytes)}</div>
            </div>
            <div class="summary-card">
                <div class="label">Format</div>
                <div class="value">{profile.format.upper()}</div>
            </div>
            <div class="summary-card">
                <div class="label">Total Rows</div>
                <div class="value">{profile.row_count:,}</div>
            </div>
            <div class="summary-card">
                <div class="label">Total Columns</div>
                <div class="value">{profile.column_count}</div>
            </div>
            <div class="summary-card">
                <div class="label">Overall Quality</div>
                <div class="value">
                    <span class="quality-badge {self._get_quality_class(profile.overall_quality_score)}">
                        {profile.overall_quality_score:.1f}%
                    </span>
                </div>
            </div>
        </div>

        <!-- Quality Overview Charts -->
        <div id="quality-overview" class="section">
            <h2 class="section-title">📈 Quality Overview</h2>

            <div id="chart-error" style="display: none; background: #2d3748; padding: 20px; border-radius: 12px; margin-bottom: 20px; border-left: 4px solid #ed8936;">
                <p style="color: #fed7aa; margin-bottom: 10px;">
                    <strong>⚠️ Charts Not Loading?</strong>
                </p>
                <p style="color: #cbd5e0; font-size: 0.9em;">
                    For the best experience with interactive charts, please download this HTML file and open it locally in your browser.
                    Online HTML previews may have restrictions that prevent Chart.js from loading.
                </p>
            </div>

            <div class="chart-container">
                <h3 style="color: #cbd5e0; margin-bottom: 15px;">Data Completeness by Column</h3>
                <div class="chart-wrapper">
                    <canvas id="completenessChart"></canvas>
                </div>
            </div>

            <div class="chart-container">
                <h3 style="color: #cbd5e0; margin-bottom: 15px;">Overall Quality Scores</h3>
                <div class="chart-wrapper">
                    <canvas id="qualityChart"></canvas>
                </div>
            </div>
        </div>

        <!-- Column Profiles Section -->
        <div id="column-profiles" class="section">
            <h2 class="section-title">🔍 Column Profiles</h2>
            <div class="column-grid">
                {self._generate_column_profiles(profile.columns)}
            </div>
        </div>

        <!-- Correlations Section -->
        {self._generate_correlations_section(profile.correlations)}

        <!-- Suggested Validations Section -->
        <div id="suggestions" class="section">
            <h2 class="section-title">💡 Suggested Validations</h2>
            <p style="color: #cbd5e0; margin-bottom: 20px;">
                Based on the data profile, here are {len(profile.suggested_validations)} recommended validations
                to ensure data quality:
            </p>
            <div class="suggestions-grid">
                {self._generate_suggestions(profile.suggested_validations)}
            </div>
        </div>

        <!-- Generated Configuration Section -->
        <div id="config" class="section">
            <h2 class="section-title">⚙️ Generated Validation Configuration</h2>
            <p style="color: #cbd5e0; margin-bottom: 20px;">
                A validation configuration file has been auto-generated based on this profile.
                Copy and save this YAML configuration to run validations:
            </p>

            <div class="config-section">
                <div class="config-header">
                    <h3 style="color: #ffffff;">Validation Config (YAML)</h3>
                    <button class="copy-button" onclick="copyConfig()">📋 Copy Config</button>
                </div>
                <div class="config-code">
                    <pre id="configYaml">{self._escape_html(profile.generated_config_yaml or "")}</pre>
                </div>
            </div>

            <div class="command-box">
                <strong>Run this command:</strong><br/>
                {profile.generated_config_command or ""}
            </div>
        </div>
    </div>

    <script>
        // Check if Chart.js loaded successfully
        if (typeof Chart === 'undefined') {{
            console.error('Chart.js failed to load');
            document.getElementById('chart-error').style.display = 'block';
        }} else {{
            // Chart.js configuration
            Chart.defaults.color = '#cbd5e0';
            Chart.defaults.borderColor = '#4a5568';

        // Completeness Chart
        const completenessCtx = document.getElementById('completenessChart').getContext('2d');
        new Chart(completenessCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(column_names)},
                datasets: [{{
                    label: 'Completeness %',
                    data: {json.dumps(completeness_scores)},
                    backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    borderColor: 'rgba(102, 126, 234, 1)',
                    borderWidth: 2
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        max: 100,
                        grid: {{
                            color: '#2d3748'
                        }}
                    }},
                    x: {{
                        grid: {{
                            color: '#2d3748'
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{
                        display: true,
                        position: 'top'
                    }}
                }}
            }}
        }});

        // Quality Chart
        const qualityCtx = document.getElementById('qualityChart').getContext('2d');
        new Chart(qualityCtx, {{
            type: 'radar',
            data: {{
                labels: {json.dumps(column_names)},
                datasets: [
                    {{
                        label: 'Overall Quality',
                        data: {json.dumps(quality_scores)},
                        backgroundColor: 'rgba(102, 126, 234, 0.2)',
                        borderColor: 'rgba(102, 126, 234, 1)',
                        borderWidth: 2
                    }},
                    {{
                        label: 'Validity',
                        data: {json.dumps(validity_scores)},
                        backgroundColor: 'rgba(72, 187, 120, 0.2)',
                        borderColor: 'rgba(72, 187, 120, 1)',
                        borderWidth: 2
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    r: {{
                        beginAtZero: true,
                        max: 100,
                        grid: {{
                            color: '#2d3748'
                        }},
                        angleLines: {{
                            color: '#2d3748'
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{
                        display: true,
                        position: 'top'
                    }}
                }}
            }}
        }});

        // Copy config function
        function copyConfig() {{
            const configText = document.getElementById('configYaml').textContent;
            navigator.clipboard.writeText(configText).then(() => {{
                const btn = event.target;
                const originalText = btn.textContent;
                btn.textContent = '✓ Copied!';
                btn.style.background = '#48bb78';
                setTimeout(() => {{
                    btn.textContent = originalText;
                    btn.style.background = '#667eea';
                }}, 2000);
            }});
        }}
        }} // Close Chart.js check
    </script>
</body>
</html>'''

        return html

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size with appropriate units."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.2f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.2f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

    def _get_quality_class(self, score: float) -> str:
        """Get CSS class for quality score."""
        if score >= 90:
            return "quality-excellent"
        elif score >= 75:
            return "quality-good"
        elif score >= 50:
            return "quality-fair"
        else:
            return "quality-poor"

    def _generate_column_profiles(self, columns: List[ColumnProfile]) -> str:
        """Generate HTML for column profiles."""
        html_parts = []

        for col in columns:
            type_badge_class = "type-known" if col.type_info.is_known else "type-inferred"

            issues_html = ""
            if col.quality.issues:
                issues_html = "<ul class='issues-list'>" + \
                    "".join(f"<li>{issue}</li>" for issue in col.quality.issues) + \
                    "</ul>"

            # Top values table
            top_values_html = ""
            if col.statistics.top_values:
                rows = []
                for item in col.statistics.top_values[:5]:
                    pct = item['percentage']
                    rows.append(f"""
                        <tr>
                            <td>{self._escape_html(str(item['value']))}</td>
                            <td>{item['count']:,}</td>
                            <td>
                                <span class="percentage-bar">
                                    <span class="percentage-fill" style="width: {pct}%"></span>
                                </span>
                                {pct:.1f}%
                            </td>
                        </tr>
                    """)
                top_values_html = f"""
                    <table class="top-values-table">
                        <thead>
                            <tr>
                                <th>Value</th>
                                <th>Count</th>
                                <th>Percentage</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join(rows)}
                        </tbody>
                    </table>
                """

            html_parts.append(f"""
                <div class="column-card">
                    <div class="column-header">
                        <div class="column-name">{col.name}</div>
                        <div class="type-badge {type_badge_class}">
                            {col.type_info.inferred_type}
                            {'(Known)' if col.type_info.is_known else '(Inferred)'}
                        </div>
                    </div>

                    <div class="column-content">
                        <!-- Type Information -->
                        <div class="info-section">
                            <h4>Type Information</h4>
                            <div class="info-row">
                                <span class="info-label">Inferred Type:</span>
                                <span class="info-value">{col.type_info.inferred_type}</span>
                            </div>
                            {'<div class="info-row"><span class="info-label">Declared Type:</span><span class="info-value">' + col.type_info.declared_type + '</span></div>' if col.type_info.declared_type else ''}
                            <div class="info-row">
                                <span class="info-label">Confidence:</span>
                                <span class="info-value">{col.type_info.confidence * 100:.1f}%</span>
                            </div>
                            <div class="confidence-bar">
                                <div class="confidence-fill" style="width: {col.type_info.confidence * 100}%"></div>
                            </div>
                        </div>

                        <!-- Statistics -->
                        <div class="info-section">
                            <h4>Statistics</h4>
                            <div class="info-row">
                                <span class="info-label">Total Values:</span>
                                <span class="info-value">{col.statistics.count:,}</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Null Count:</span>
                                <span class="info-value">{col.statistics.null_count:,} ({col.statistics.null_percentage:.1f}%)</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Unique Values:</span>
                                <span class="info-value">{col.statistics.unique_count:,} ({col.statistics.unique_percentage:.1f}%)</span>
                            </div>
                            {f'<div class="info-row"><span class="info-label">Range:</span><span class="info-value">{col.statistics.min_value} to {col.statistics.max_value}</span></div>' if col.statistics.min_value is not None else ''}
                            {f'<div class="info-row"><span class="info-label">Mean:</span><span class="info-value">{col.statistics.mean:.2f}</span></div>' if col.statistics.mean is not None else ''}
                        </div>

                        <!-- Quality Metrics -->
                        <div class="info-section">
                            <h4>Quality Metrics</h4>
                            <div class="info-row">
                                <span class="info-label">Overall Score:</span>
                                <span class="info-value">{col.quality.overall_score:.1f}%</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Completeness:</span>
                                <span class="info-value">{col.quality.completeness:.1f}%</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Validity:</span>
                                <span class="info-value">{col.quality.validity:.1f}%</span>
                            </div>
                            <div class="info-row">
                                <span class="info-label">Uniqueness:</span>
                                <span class="info-value">{col.quality.uniqueness:.1f}%</span>
                            </div>
                            {issues_html}
                        </div>
                    </div>

                    <!-- Top Values -->
                    {f'<div style="margin-top: 20px;"><h4 style="color: #a0aec0; margin-bottom: 10px; font-size: 0.9em;">TOP VALUES</h4>{top_values_html}</div>' if top_values_html else ''}
                </div>
            """)

        return "".join(html_parts)

    def _generate_correlations_section(self, correlations: List) -> str:
        """Generate HTML for correlations section."""
        if not correlations:
            return ""

        rows = []
        for corr in correlations[:10]:  # Top 10
            strength = "Strong" if abs(corr.correlation) > 0.7 else "Moderate"
            direction = "Positive" if corr.correlation > 0 else "Negative"

            rows.append(f"""
                <tr>
                    <td>{corr.column1}</td>
                    <td>{corr.column2}</td>
                    <td>{corr.correlation:.3f}</td>
                    <td>{strength} {direction}</td>
                </tr>
            """)

        return f"""
            <div id="correlations" class="section">
                <h2 class="section-title">🔗 Correlations</h2>
                <p style="color: #cbd5e0; margin-bottom: 20px;">
                    Detected {len(correlations)} significant correlations between numeric columns:
                </p>
                <div class="config-section">
                    <table class="top-values-table">
                        <thead>
                            <tr>
                                <th>Column 1</th>
                                <th>Column 2</th>
                                <th>Correlation</th>
                                <th>Strength</th>
                            </tr>
                        </thead>
                        <tbody>
                            {"".join(rows)}
                        </tbody>
                    </table>
                </div>
            </div>
        """

    def _generate_suggestions(self, suggestions: List) -> str:
        """Generate HTML for validation suggestions."""
        html_parts = []

        for sugg in suggestions:
            severity_class = "severity-error" if sugg.severity == "ERROR" else "severity-warning"

            params_html = ""
            if sugg.params:
                param_items = [f"<strong>{k}:</strong> {v}" for k, v in sugg.params.items()]
                params_html = "<br/>".join(param_items)

            html_parts.append(f"""
                <div class="suggestion-card">
                    <div class="suggestion-header">
                        <div class="validation-type">{sugg.validation_type}</div>
                        <div class="severity-badge {severity_class}">{sugg.severity}</div>
                    </div>
                    <div style="color: #cbd5e0; margin-bottom: 10px;">
                        {sugg.reason}
                    </div>
                    {f'<div style="color: #a0aec0; font-size: 0.85em; margin-top: 10px;">{params_html}</div>' if params_html else ''}
                    <div style="color: #718096; font-size: 0.8em; margin-top: 10px;">
                        Confidence: {sugg.confidence:.0f}%
                    </div>
                </div>
            """)

        return "".join(html_parts)

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))
