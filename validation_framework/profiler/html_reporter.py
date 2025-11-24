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
    <!-- Chart.js Library (CDN) -->
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
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
            padding: 0;
            margin: 0;
            min-height: 100vh;
            overflow-x: hidden; /* Prevent horizontal scroll on body */
        }}

        .container {{
            max-width: 100%;
            width: 100%;
            margin: 0 auto;
            background: #1e1e2e;
            overflow-x: hidden; /* Prevent horizontal scroll on container */
        }}

        /* Mobile-first responsive wrapper with constrained max width */
        @media (min-width: 769px) {{
            .container {{
                max-width: 1400px;
                border-radius: 16px;
                box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
                margin: 20px auto;
            }}
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
            border-radius: 8px;
            padding: 16px;
            border: 1px solid #4a5568;
            transition: all 0.2s;
        }}

        .column-card:hover {{
            border-color: #667eea;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2);
        }}

        .column-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            padding-bottom: 10px;
            border-bottom: 1px solid #4a5568;
        }}

        .column-name {{
            font-size: 1.1em;
            font-weight: 600;
            color: #ffffff;
        }}

        .type-badge {{
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.75em;
            font-weight: 600;
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
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}

        /* Compact stats grid */
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
            background: #1a1a2e;
            padding: 12px;
            border-radius: 6px;
            border: 1px solid #4a5568;
        }}

        .stat-item {{
            display: flex;
            flex-direction: column;
            gap: 2px;
        }}

        .stat-label {{
            font-size: 0.7em;
            color: #a0aec0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .stat-value {{
            font-size: 0.9em;
            color: #ffffff;
            font-weight: 500;
        }}

        /* Quality score bar */
        .quality-bar-compact {{
            display: flex;
            align-items: center;
            gap: 10px;
            background: #1a1a2e;
            padding: 8px 12px;
            border-radius: 6px;
            border: 1px solid #4a5568;
        }}

        .quality-label {{
            font-size: 0.7em;
            color: #a0aec0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            min-width: 80px;
        }}

        .quality-progress {{
            flex: 1;
            height: 8px;
            background: #2d3748;
            border-radius: 4px;
            overflow: hidden;
        }}

        .quality-fill {{
            height: 100%;
            transition: width 0.3s;
        }}

        .quality-score {{
            font-size: 0.9em;
            color: #ffffff;
            font-weight: 600;
            min-width: 50px;
            text-align: right;
        }}

        /* Collapsible sections */
        .collapsible-section {{
            background: #1a1a2e;
            border-radius: 6px;
            border: 1px solid #4a5568;
            overflow: hidden;
        }}

        .collapsible-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 8px 12px;
            cursor: pointer;
            user-select: none;
            transition: background 0.2s;
        }}

        .collapsible-header:hover {{
            background: #252542;
        }}

        .collapsible-title {{
            font-size: 0.75em;
            color: #a0aec0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 600;
        }}

        .collapsible-icon {{
            color: #667eea;
            font-size: 0.9em;
            transition: transform 0.2s;
        }}

        .collapsible-icon.expanded {{
            transform: rotate(180deg);
        }}

        .collapsible-content {{
            padding: 0 12px 12px 12px;
            display: none;
        }}

        .collapsible-content.expanded {{
            display: block;
        }}

        .info-section {{
            background: transparent;
            padding: 0;
            border-radius: 0;
            border: none;
        }}

        .info-section h4 {{
            color: #a0aec0;
            font-size: 0.75em;
            text-transform: uppercase;
            margin-bottom: 8px;
            letter-spacing: 0.5px;
        }}

        .info-row {{
            display: flex;
            justify-content: space-between;
            padding: 4px 0;
            font-size: 0.85em;
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

        .severity-text {{
            color: #cbd5e0;
            font-size: 0.85em;
            font-weight: normal;
            font-style: italic;
        }}

        /* Validation Coverage Card */
        .validation-coverage-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            border-radius: 12px;
            margin-bottom: 25px;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.3);
        }}

        .coverage-metrics {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 15px;
            margin-top: 15px;
        }}

        .metric {{
            background: rgba(255, 255, 255, 0.1);
            padding: 15px;
            border-radius: 8px;
            text-align: center;
            backdrop-filter: blur(10px);
        }}

        .metric.highlight-new {{
            background: linear-gradient(135deg, rgba(139, 92, 246, 0.3) 0%, rgba(236, 72, 153, 0.3) 100%);
            border: 2px solid #bb9af7;
        }}

        .metric-value {{
            font-size: 2em;
            font-weight: bold;
            color: #ffffff;
            line-height: 1;
            margin-bottom: 5px;
        }}

        .metric-label {{
            font-size: 0.75em;
            color: #e0e0e0;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .metric-sub {{
            font-size: 0.65em;
            color: #cbd5e0;
            margin-top: 3px;
        }}

        .coverage-bar-mini {{
            height: 6px;
            background: rgba(255, 255, 255, 0.2);
            border-radius: 3px;
            margin-top: 8px;
            overflow: hidden;
        }}

        .coverage-fill {{
            height: 100%;
            background: linear-gradient(90deg, #10b981 0%, #3b82f6 100%);
            border-radius: 3px;
            transition: width 0.5s ease;
        }}

        /* Suggestion Categories */
        .suggestion-category {{
            margin-bottom: 20px;
        }}

        .category-header {{
            background: #3d3d5c;
            padding: 15px 20px;
            border-radius: 8px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background 0.3s;
        }}

        .category-header:hover {{
            background: #4a4a6a;
        }}

        .category-header h3 {{
            margin: 0;
            color: #ffffff;
            font-size: 1.1em;
        }}

        .category-count {{
            color: #a0aec0;
            font-weight: normal;
            font-size: 0.9em;
        }}

        .category-content {{
            padding: 15px 0;
        }}

        .toggle-icon {{
            color: #cbd5e0;
            transition: transform 0.3s;
        }}

        .toggle-icon.rotated {{
            transform: rotate(180deg);
        }}


        /* Suggestion Actions */
        .suggestion-actions {{
            display: flex;
            gap: 10px;
            align-items: center;
            flex-wrap: wrap;
        }}

        .copy-validation-btn {{
            background: #667eea;
            color: white;
            border: none;
            padding: 6px 12px;
            border-radius: 5px;
            cursor: pointer;
            font-size: 0.75em;
            font-weight: 600;
            transition: all 0.3s;
        }}

        .copy-validation-btn:hover {{
            background: #5a67d8;
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(102, 126, 234, 0.4);
        }}

        .copy-validation-btn:active {{
            transform: translateY(0);
        }}

        /* Confidence Bar */
        .confidence-container {{
            margin-top: 12px;
            padding-top: 10px;
            border-top: 1px solid rgba(255, 255, 255, 0.1);
        }}

        .confidence-label {{
            color: #a0aec0;
            font-size: 0.75em;
            margin-bottom: 5px;
        }}

        .confidence-bar {{
            height: 6px;
            background: rgba(255, 255, 255, 0.1);
            border-radius: 3px;
            overflow: hidden;
        }}

        .confidence-fill {{
            height: 100%;
            border-radius: 3px;
            transition: width 0.5s ease;
        }}

        .validation-yaml {{
            display: none;
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

        /* Sticky navigation for desktop, collapsible for mobile */
        .toc {{
            background: #2d2d44;
            padding: 20px;
            border-bottom: 2px solid #2d3748;
            position: sticky;
            top: 0;
            z-index: 100;
            max-width: 100%;
            overflow-x: hidden;
        }}

        .toc h3 {{
            color: #ffffff;
            margin-bottom: 15px;
            font-size: 1.1em;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .toc-toggle {{
            display: none; /* Hidden on desktop */
            color: #667eea;
            font-size: 0.9em;
        }}

        .toc-list {{
            list-style: none;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 8px;
            max-width: 100%;
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
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}

        .toc-list a:hover {{
            background: #1a1a2e;
            color: #667eea;
        }}

        .toc-list a::before {{
            content: "▸ ";
            color: #667eea;
        }}

        /* Mobile Responsiveness - Enhanced */
        @media (max-width: 768px) {{
            body {{
                padding: 0;
                background: #1e1e2e; /* Solid background on mobile */
            }}

            .container {{
                border-radius: 0;
                box-shadow: none;
            }}

            .header {{
                padding: 20px 15px;
            }}

            .header h1 {{
                font-size: 1.6em;
            }}

            .header .subtitle {{
                font-size: 0.85em;
            }}

            /* Collapsible TOC on mobile */
            .toc {{
                position: static; /* Not sticky on mobile */
                padding: 15px;
            }}

            .toc h3 {{
                font-size: 1em;
            }}

            .toc-toggle {{
                display: inline-block; /* Show toggle on mobile */
            }}

            .toc-list {{
                grid-template-columns: 1fr; /* Single column on mobile */
                max-height: 0;
                overflow: hidden;
                transition: max-height 0.3s ease-out;
            }}

            .toc-list.mobile-expanded {{
                max-height: 500px;
                margin-top: 10px;
            }}

            .summary-grid {{
                grid-template-columns: repeat(2, 1fr); /* 2 columns on tablet */
                padding: 15px;
                gap: 10px;
            }}

            .summary-card {{
                padding: 12px;
            }}

            .summary-card .value {{
                font-size: 1.3em;
            }}

            .summary-card .label {{
                font-size: 0.7em;
            }}

            .section {{
                padding: 15px;
            }}

            .section-title {{
                font-size: 1.3em;
            }}

            .chart-container {{
                padding: 12px;
            }}

            .chart-wrapper {{
                height: 280px;
            }}

            .column-content {{
                grid-template-columns: 1fr;
            }}

            .column-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 8px;
            }}

            .config-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 10px;
            }}

            .stats-grid {{
                grid-template-columns: 1fr; /* Single column stats on mobile */
            }}

            /* Make tables horizontally scrollable with shadow indicators */
            .top-values-table {{
                font-size: 0.8em;
                display: block;
                overflow-x: auto;
                white-space: nowrap;
                -webkit-overflow-scrolling: touch;
            }}

            .top-values-table th,
            .top-values-table td {{
                padding: 6px 8px;
            }}

            .config-code {{
                font-size: 0.7em;
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
            }}

            .command-box {{
                font-size: 0.75em;
                overflow-x: auto;
                -webkit-overflow-scrolling: touch;
            }}

            /* Adjust suggestion cards for mobile */
            .suggestion-card {{
                padding: 15px;
            }}

            .validation-type {{
                font-size: 1em;
            }}
        }}

        @media (max-width: 480px) {{
            .header {{
                padding: 15px 10px;
            }}

            .header h1 {{
                font-size: 1.4em;
            }}

            .header .subtitle {{
                font-size: 0.75em;
            }}

            .summary-grid {{
                grid-template-columns: 1fr; /* Single column on small mobile */
                padding: 10px;
                gap: 8px;
            }}

            .summary-card {{
                padding: 10px;
            }}

            .section {{
                padding: 10px;
            }}

            .section-title {{
                font-size: 1.1em;
            }}

            .chart-wrapper {{
                height: 220px;
            }}

            .toc {{
                padding: 10px;
            }}

            .stats-grid {{
                gap: 6px;
                padding: 10px;
            }}

            .config-code {{
                font-size: 0.65em;
            }}

            .command-box {{
                font-size: 0.7em;
                padding: 10px 15px;
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
    <script>
        // Collapsible section functionality
        document.addEventListener('DOMContentLoaded', function() {{
            const collapsibleHeaders = document.querySelectorAll('.collapsible-header');

            collapsibleHeaders.forEach(header => {{
                header.addEventListener('click', function() {{
                    const content = this.nextElementSibling;
                    const icon = this.querySelector('.collapsible-icon');

                    if (content.classList.contains('expanded')) {{
                        content.classList.remove('expanded');
                        icon.classList.remove('expanded');
                    }} else {{
                        content.classList.add('expanded');
                        icon.classList.add('expanded');
                    }}
                }});
            }});

            // Mobile TOC toggle functionality
            const tocHeader = document.querySelector('.toc h3');
            const tocList = document.querySelector('.toc-list');

            if (tocHeader && tocList) {{
                tocHeader.addEventListener('click', function() {{
                    if (window.innerWidth <= 768) {{
                        tocList.classList.toggle('mobile-expanded');
                        const toggle = tocHeader.querySelector('.toc-toggle');
                        if (toggle) {{
                            toggle.textContent = tocList.classList.contains('mobile-expanded') ? '▲' : '▼';
                        }}
                    }}
                }});
            }}

            // Smooth scrolling for anchor links
            document.querySelectorAll('a[href^="#"]').forEach(anchor => {{
                anchor.addEventListener('click', function(e) {{
                    e.preventDefault();
                    const target = document.querySelector(this.getAttribute('href'));
                    if (target) {{
                        target.scrollIntoView({{
                            behavior: 'smooth',
                            block: 'start'
                        }});
                        // Close mobile TOC after navigation
                        if (window.innerWidth <= 768 && tocList) {{
                            tocList.classList.remove('mobile-expanded');
                            const toggle = tocHeader.querySelector('.toc-toggle');
                            if (toggle) {{
                                toggle.textContent = '▼';
                            }}
                        }}
                    }}
                }});
            }});
        }});
    </script>
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
            <h3>📋 Report Sections <span class="toc-toggle">▼</span></h3>
            <ul class="toc-list">
                <li><a href="#summary">Summary</a></li>
                {f'<li><a href="#phase2-summary">🔬 Advanced Analysis</a></li>' if any(col.temporal_analysis or (col.pii_info and col.pii_info.get('detected')) for col in profile.columns) else ''}
                <li><a href="#quality-overview">Quality Overview</a></li>
                <li><a href="#column-profiles">Column Profiles</a></li>
                {f'<li><a href="#correlations">Correlations</a></li>' if profile.correlations else ''}
                <li><a href="#suggestions">Suggested Validations</a></li>
                <li><a href="#config">Generated Configuration</a></li>
            </ul>
        </div>

        <!-- Unified Summary Section -->
        <div id="summary"></div>
        {self._generate_unified_summary(profile)}

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
                <h3 style="color: #cbd5e0; margin-bottom: 10px;">Data Completeness by Column</h3>
                <div style="color: #9ca3af; font-size: 0.85em; margin-bottom: 15px; padding: 10px; background: #1f2937; border-radius: 6px; border-left: 3px solid #3b82f6;">
                    📊 <strong>How to read this chart:</strong> Shows the percentage of non-null values for each column.
                    Higher bars (closer to 100%) indicate more complete data. Columns below 95% may need attention.
                    <div style="margin-top: 8px; color: #cbd5e0;">
                        • <span style="color: #10b981;">Green zone (95-100%)</span> = Excellent completeness<br>
                        • <span style="color: #f59e0b;">Orange zone (80-95%)</span> = Acceptable, monitor for improvements<br>
                        • <span style="color: #ef4444;">Red zone (below 80%)</span> = Significant data gaps, requires investigation
                    </div>
                </div>
                <div class="chart-wrapper">
                    <canvas id="completenessChart"></canvas>
                </div>
            </div>

            <div class="chart-container">
                <h3 style="color: #cbd5e0; margin-bottom: 10px;">Overall Quality Scores</h3>
                <div style="color: #9ca3af; font-size: 0.85em; margin-bottom: 15px; padding: 10px; background: #1f2937; border-radius: 6px; border-left: 3px solid #3b82f6;">
                    📊 <strong>How to read this chart:</strong> Displays a composite quality score (0-100%) for each column,
                    combining multiple factors: completeness (no nulls), validity (correct format), uniqueness (distinct values),
                    and consistency (pattern uniformity).
                    <div style="margin-top: 8px; color: #cbd5e0;">
                        • <span style="color: #10b981;">Score 90-100%</span> = Excellent quality, ready for use<br>
                        • <span style="color: #3b82f6;">Score 70-89%</span> = Good quality, minor improvements possible<br>
                        • <span style="color: #f59e0b;">Score 50-69%</span> = Fair quality, review recommended<br>
                        • <span style="color: #ef4444;">Score below 50%</span> = Poor quality, immediate action needed
                    </div>
                </div>
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

        // Toggle category function
        function toggleCategory(categoryId) {{
            const content = document.getElementById('category-' + categoryId);
            const icon = document.getElementById('toggle-' + categoryId);

            if (content.style.display === 'none') {{
                content.style.display = 'block';
                icon.classList.remove('rotated');
            }} else {{
                content.style.display = 'none';
                icon.classList.add('rotated');
            }}
        }}

        // Copy validation YAML function
        function copyValidation(yamlId) {{
            const yamlText = document.getElementById(yamlId).textContent;
            navigator.clipboard.writeText(yamlText).then(() => {{
                const btn = event.target;
                const originalText = btn.textContent;
                btn.textContent = '✓ Copied!';
                btn.style.background = '#48bb78';
                setTimeout(() => {{
                    btn.textContent = originalText;
                    btn.style.background = '#667eea';
                }}, 2000);
            }}).catch(err => {{
                console.error('Failed to copy:', err);
                alert('Failed to copy to clipboard');
            }});
        }}

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

            observations_html = ""
            if col.quality.observations:
                observations_html = "<ul class='issues-list'>" + \
                    "".join(f"<li>{obs}</li>" for obs in col.quality.observations) + \
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

            # Determine quality color
            quality_score = col.quality.overall_score
            if quality_score >= 90:
                quality_color = '#10b981'  # Green
            elif quality_score >= 75:
                quality_color = '#3b82f6'  # Blue
            elif quality_score >= 50:
                quality_color = '#f59e0b'  # Orange
            else:
                quality_color = '#ef4444'  # Red

            # Build additional stats for numeric/string types
            extra_stats = ""
            if col.statistics.min_value is not None and col.statistics.max_value is not None:
                extra_stats += f"""
                    <div class="stat-item">
                        <div class="stat-label">Range</div>
                        <div class="stat-value">{col.statistics.min_value} to {col.statistics.max_value}</div>
                    </div>
                """
            if col.statistics.mean is not None:
                extra_stats += f"""
                    <div class="stat-item">
                        <div class="stat-label">Mean</div>
                        <div class="stat-value">{col.statistics.mean:.2f}</div>
                    </div>
                """
            if col.statistics.min_length is not None:
                extra_stats += f"""
                    <div class="stat-item">
                        <div class="stat-label">Length Range</div>
                        <div class="stat-value">{col.statistics.min_length} - {col.statistics.max_length}</div>
                    </div>
                """

            html_parts.append(f"""
                <div class="column-card">
                    <div class="column-header">
                        <div class="column-name">{col.name}</div>
                        <div class="type-badge {type_badge_class}">
                            {col.type_info.inferred_type}
                        </div>
                    </div>

                    <div class="column-content">
                        <!-- Compact Stats Grid -->
                        <div class="stats-grid">
                            <div class="stat-item">
                                <div class="stat-label">Total Values</div>
                                <div class="stat-value">{col.statistics.count:,}</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">Nulls</div>
                                <div class="stat-value">{col.statistics.null_count:,} ({col.statistics.null_percentage:.1f}%)</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">Unique</div>
                                <div class="stat-value">{col.statistics.unique_count:,} ({col.statistics.unique_percentage:.1f}%)</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-label">Type Confidence</div>
                                <div class="stat-value">{col.type_info.confidence * 100:.0f}%</div>
                            </div>
                            {extra_stats}
                        </div>

                        <!-- Intelligent Sampling Info (if available) -->
                        {self._generate_sampling_info(col)}

                        <!-- Quality Metrics with Explanations -->
                        <div style="margin-top: 16px;">
                            <div style="font-weight: 600; margin-bottom: 12px; color: #1f2937;">Quality Metrics</div>

                            <!-- Overall Score -->
                            <div class="quality-bar-compact">
                                <div class="quality-label">Overall Quality</div>
                                <div class="quality-progress">
                                    <div class="quality-fill" style="width: {quality_score}%; background: {quality_color};"></div>
                                </div>
                                <div class="quality-score" style="color: {quality_color};">{quality_score:.0f}%</div>
                            </div>

                            <!-- Completeness -->
                            <div class="quality-bar-compact">
                                <div class="quality-label">Completeness</div>
                                <div class="quality-progress">
                                    <div class="quality-fill" style="width: {col.quality.completeness}%; background: {'#10b981' if col.quality.completeness >= 95 else '#f59e0b' if col.quality.completeness >= 90 else '#ef4444'};"></div>
                                </div>
                                <div class="quality-score">{col.quality.completeness:.0f}%</div>
                            </div>
                            <div style="font-size: 12px; color: #6b7280; margin-left: 8px; margin-bottom: 8px;">
                                {f"{'✓' if col.quality.completeness >= 95 else '⚠'} {col.statistics.null_count:,} null values ({col.statistics.null_percentage:.1f}%)" if col.statistics.null_count > 0 else "✓ No missing values"}
                            </div>

                            <!-- Type Confidence (Validity) -->
                            <div class="quality-bar-compact">
                                <div class="quality-label">Type Confidence</div>
                                <div class="quality-progress">
                                    <div class="quality-fill" style="width: {col.type_info.confidence * 100}%; background: {'#10b981' if col.type_info.confidence >= 0.95 else '#f59e0b' if col.type_info.confidence >= 0.80 else '#ef4444'};"></div>
                                </div>
                                <div class="quality-score">{col.type_info.confidence * 100:.0f}%</div>
                            </div>
                            <div style="font-size: 12px; color: #6b7280; margin-left: 8px; margin-bottom: 8px;">
                                {f"{'✓' if col.type_info.confidence >= 0.95 else '⚠'} {col.type_info.confidence * 100:.1f}% of values match {col.type_info.inferred_type} type"}
                                {self._generate_type_conflicts_display(col) if col.type_info.confidence < 0.95 else ''}
                            </div>

                            <!-- Uniqueness -->
                            <div class="quality-bar-compact">
                                <div class="quality-label">Uniqueness</div>
                                <div class="quality-progress">
                                    <div class="quality-fill" style="width: {col.quality.uniqueness}%; background: #667eea;"></div>
                                </div>
                                <div class="quality-score">{col.quality.uniqueness:.0f}%</div>
                            </div>
                            <div style="font-size: 12px; color: #6b7280; margin-left: 8px; margin-bottom: 8px;">
                                {f"ℹ {col.statistics.unique_count:,} unique values - All values unique (potential key)" if col.statistics.cardinality == 1.0 and col.statistics.count > 1 else f"ℹ {col.statistics.unique_count:,} unique values ({col.statistics.unique_percentage:.1f}%)"}
                            </div>

                            <!-- Consistency -->
                            <div class="quality-bar-compact">
                                <div class="quality-label">Consistency</div>
                                <div class="quality-progress">
                                    <div class="quality-fill" style="width: {col.quality.consistency}%; background: {'#10b981' if col.quality.consistency >= 80 else '#f59e0b' if col.quality.consistency >= 50 else '#ef4444'};"></div>
                                </div>
                                <div class="quality-score">{col.quality.consistency:.0f}%</div>
                            </div>
                            <div style="font-size: 12px; color: #6b7280; margin-left: 8px; margin-bottom: 8px;">
                                {f"ℹ {len(col.statistics.pattern_samples)} different patterns detected" if col.statistics.pattern_samples and len(col.statistics.pattern_samples) > 1 else "✓ Consistent pattern"}
                            </div>
                        </div>

                        <!-- Collapsible: Top Values -->
                        {f'''<div class="collapsible-section">
                            <div class="collapsible-header">
                                <div class="collapsible-title">📊 Top Values ({len(col.statistics.top_values)})</div>
                                <div class="collapsible-icon">▼</div>
                            </div>
                            <div class="collapsible-content">
                                {top_values_html}
                            </div>
                        </div>''' if top_values_html else ''}

                        <!-- Data Insights (for additional observations not covered above) -->
                        {f'''<div class="collapsible-section">
                            <div class="collapsible-header">
                                <div class="collapsible-title">ℹ Additional Data Insights ({len(col.quality.observations)})</div>
                                <div class="collapsible-icon">▼</div>
                            </div>
                            <div class="collapsible-content">
                                <div style="padding-top: 8px;">
                                    {observations_html}
                                </div>
                            </div>
                        </div>''' if col.quality.observations else ''}
                    </div>

                    <!-- Phase 2: Semantic Understanding (FIBO) -->
                    {self._generate_semantic_viz(col) if col.semantic_info and col.semantic_info.get('primary_tag') != 'unknown' else ''}

                    <!-- Phase 2: Temporal Analysis -->
                    {self._generate_temporal_viz(col) if col.temporal_analysis else ''}

                    <!-- Phase 2: PII Detection -->
                    {self._generate_pii_viz(col) if col.pii_info and col.pii_info.get('detected') else ''}
                </div>
            """)

        return "".join(html_parts)

    def _generate_sampling_info(self, col: ColumnProfile) -> str:
        """Generate HTML for intelligent sampling transparency info."""
        # Check if column has intelligent sampling metadata
        if not hasattr(col.statistics, 'semantic_type') or col.statistics.semantic_type is None:
            return ""

        semantic_type = col.statistics.semantic_type
        sample_size = getattr(col.statistics, 'sample_size', None)
        sampling_strategy = getattr(col.statistics, 'sampling_strategy', None)

        # Skip if semantic type is 'unknown' (default behavior)
        if semantic_type == 'unknown':
            return ""

        # Semantic type icons and colors
        type_icons = {
            'id': '🔑',
            'email': '📧',
            'phone': '📞',
            'date': '📅',
            'datetime': '📅',
            'timestamp': '⏰',
            'amount': '💰',
            'price': '💰',
            'category': '🏷️',
            'text': '📝',
            'code': '🔢',
            'unknown': '❓'
        }

        type_colors = {
            'id': '#8b5cf6',
            'email': '#3b82f6',
            'phone': '#3b82f6',
            'date': '#10b981',
            'datetime': '#10b981',
            'timestamp': '#10b981',
            'amount': '#f59e0b',
            'price': '#f59e0b',
            'category': '#ec4899',
            'text': '#6366f1',
            'code': '#14b8a6',
            'unknown': '#6b7280'
        }

        icon = type_icons.get(semantic_type, '🧠')
        color = type_colors.get(semantic_type, '#667eea')

        return f"""
        <div style="margin-top: 12px; padding: 12px; background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%); border-left: 3px solid {color}; border-radius: 6px;">
            <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 6px;">
                <span style="font-size: 18px;">{icon}</span>
                <span style="font-weight: 600; color: {color}; font-size: 13px; text-transform: capitalize;">
                    {semantic_type.replace('_', ' ')} Field
                </span>
                <span style="margin-left: auto; font-size: 11px; color: #9ca3af; background: rgba(156, 163, 175, 0.2); padding: 2px 8px; border-radius: 10px;">
                    Intelligent Sampling
                </span>
            </div>
            {f'<div style="font-size: 12px; color: #cbd5e0; line-height: 1.5;">{sampling_strategy}</div>' if sampling_strategy else ''}
            {f'<div style="font-size: 11px; color: #9ca3af; margin-top: 4px;">Sample size: {sample_size:,} values</div>' if sample_size else ''}
        </div>
        """

    def _generate_type_conflicts_display(self, col: ColumnProfile) -> str:
        """
        Generate HTML display for type conflicts when confidence < 95%.
        Shows what other types were detected and their percentages.
        """
        if not hasattr(col.type_info, 'type_conflicts') or not col.type_info.type_conflicts:
            return ""

        conflicts = col.type_info.type_conflicts
        if not conflicts:
            return ""

        # Build conflict display
        conflict_items = []
        for conflict in conflicts[:3]:  # Show top 3 conflicts
            conflict_items.append(
                f"{conflict['type']} ({conflict['percentage']}%)"
            )

        conflicts_text = ", ".join(conflict_items)

        return f"""
        <div style="margin-top: 6px; padding: 8px; background: rgba(245, 158, 11, 0.1); border-left: 2px solid #f59e0b; border-radius: 4px;">
            <div style="font-size: 11px; color: #f59e0b; font-weight: 600; margin-bottom: 3px;">
                Mixed Data Types Detected
            </div>
            <div style="font-size: 11px; color: #cbd5e0; line-height: 1.4;">
                Also found: {conflicts_text}
            </div>
            <div style="font-size: 10px; color: #9ca3af; margin-top: 4px; font-style: italic;">
                This may indicate data quality issues or valid mixed-type usage (e.g., alphanumeric IDs)
            </div>
        </div>
        """

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

    def _categorize_suggestions(self, suggestions: List) -> dict:
        """Categorize validation suggestions by type."""
        # Define validation categories
        categories = {
            'file': {'name': '📋 File-Level Validations', 'validations': ['EmptyFileCheck', 'RowCountRangeCheck'], 'suggestions': []},
            'uniqueness': {'name': '🔑 Uniqueness & Identity', 'validations': ['UniqueKeyCheck', 'DuplicateCheck'], 'suggestions': []},
            'temporal': {'name': '📅 Temporal Validations', 'validations': ['DateRangeCheck', 'DateSequenceCheck', 'DateGapCheck'], 'suggestions': []},
            'statistical': {'name': '📈 Statistical Validations', 'validations': ['OutlierDetectionCheck', 'DistributionCheck'], 'suggestions': []},
            'pattern': {'name': '📐 String & Pattern Validations', 'validations': ['StringLengthCheck', 'FormatCheck', 'RegexPatternCheck'], 'suggestions': []},
            'range': {'name': '📊 Data Range & Boundaries', 'validations': ['RangeCheck', 'NumericRangeCheck'], 'suggestions': []},
            'completeness': {'name': '✓ Data Completeness', 'validations': ['MandatoryFieldCheck', 'NullCheck'], 'suggestions': []},
            'values': {'name': '🎯 Value Constraints', 'validations': ['ValidValuesCheck', 'EnumCheck'], 'suggestions': []},
        }

        # Categorize each suggestion
        for sugg in suggestions:
            categorized = False
            for cat_key, cat_data in categories.items():
                if sugg.validation_type in cat_data['validations']:
                    cat_data['suggestions'].append(sugg)
                    categorized = True
                    break

            # If not categorized, add to a misc category
            if not categorized:
                if 'other' not in categories:
                    categories['other'] = {'name': '⚙️ Other Validations', 'validations': [], 'suggestions': []}
                categories['other']['suggestions'].append(sugg)

        # Remove empty categories
        return {k: v for k, v in categories.items() if v['suggestions']}

    def _generate_coverage_summary(self, suggestions: List) -> str:
        """Generate validation coverage summary card."""
        total_suggestions = len(suggestions)
        unique_types = len(set(sugg.validation_type for sugg in suggestions))

        # Estimate coverage (35 total validation types in framework)
        coverage_pct = min(100, (unique_types / 35.0) * 100)

        return f"""
        <div class="validation-coverage-card">
            <h3 style="color: #ffffff; margin-bottom: 15px;">🎯 Validation Coverage Analysis</h3>
            <div class="coverage-metrics">
                <div class="metric">
                    <div class="metric-value">{total_suggestions}</div>
                    <div class="metric-label">Total Suggestions</div>
                </div>
                <div class="metric">
                    <div class="metric-value">{unique_types}/35</div>
                    <div class="metric-label">Validation Types</div>
                </div>
                <div class="metric">
                    <div class="metric-value" style="color: {'#10b981' if coverage_pct >= 40 else '#f59e0b' if coverage_pct >= 20 else '#ef4444'};">{coverage_pct:.0f}%</div>
                    <div class="metric-label">Coverage</div>
                    <div class="coverage-bar-mini">
                        <div class="coverage-fill" style="width: {coverage_pct:.0f}%;"></div>
                    </div>
                </div>
            </div>
        </div>
        """

    def _generate_suggestion_yaml(self, sugg, suggestion_id: int) -> str:
        """Generate YAML snippet for a validation suggestion."""
        params_yaml = ""
        if sugg.params:
            params_lines = []
            for k, v in sugg.params.items():
                if isinstance(v, str):
                    params_lines.append(f'          {k}: "{v}"')
                elif isinstance(v, list):
                    params_lines.append(f'          {k}:')
                    for item in v:
                        if isinstance(item, str):
                            params_lines.append(f'            - "{item}"')
                        else:
                            params_lines.append(f'            - {item}')
                else:
                    params_lines.append(f'          {k}: {v}')
            params_yaml = "\n        params:\n" + "\n".join(params_lines)

        return f"""      - type: "{sugg.validation_type}"
        severity: "{sugg.severity}"{params_yaml}"""

    def _generate_suggestions(self, suggestions: List) -> str:
        """Generate enhanced HTML for validation suggestions with categorization."""
        if not suggestions:
            return "<p style='color: #9ca3af;'>No validation suggestions generated.</p>"

        # Generate coverage summary
        coverage_html = self._generate_coverage_summary(suggestions)

        # Categorize suggestions
        categorized = self._categorize_suggestions(suggestions)

        html_parts = [coverage_html]
        suggestion_counter = 0

        # Generate HTML for each category
        for cat_key, cat_data in categorized.items():
            cat_name = cat_data['name']
            cat_suggestions = cat_data['suggestions']

            html_parts.append(f"""
            <div class="suggestion-category">
                <div class="category-header" onclick="toggleCategory('{cat_key}')">
                    <h3>{cat_name} <span class="category-count">({len(cat_suggestions)})</span></h3>
                    <span class="toggle-icon" id="toggle-{cat_key}">▼</span>
                </div>
                <div class="category-content" id="category-{cat_key}">
            """)

            for sugg in cat_suggestions:
                suggestion_counter += 1
                severity_text = f"Recommend {sugg.severity} severity"

                # Confidence bar color
                confidence = sugg.confidence
                if confidence >= 80:
                    conf_color = '#10b981'
                elif confidence >= 50:
                    conf_color = '#f59e0b'
                else:
                    conf_color = '#ef4444'

                params_html = ""
                if sugg.params:
                    param_items = [f"<strong>{k}:</strong> {v}" for k, v in sugg.params.items()]
                    params_html = "<br/>".join(param_items)

                # Generate YAML for copy button
                yaml_snippet = self._generate_suggestion_yaml(sugg, suggestion_counter)

                html_parts.append(f"""
                    <div class="suggestion-card">
                        <div class="suggestion-header">
                            <div class="validation-type">
                                {sugg.validation_type}
                            </div>
                            <div class="suggestion-actions">
                                <button class="copy-validation-btn" onclick="copyValidation('yaml-{suggestion_counter}')" title="Copy YAML snippet">
                                    📋 Copy YAML
                                </button>
                            </div>
                        </div>
                        <div style="color: #cbd5e0; margin-bottom: 10px;">
                            {sugg.reason}
                        </div>
                        <div class="severity-text">{severity_text}</div>
                        {f'<div style="color: #a0aec0; font-size: 0.85em; margin-top: 10px;">{params_html}</div>' if params_html else ''}
                        <div class="confidence-container">
                            <div class="confidence-label">Confidence: {confidence:.0f}%</div>
                            <div class="confidence-bar">
                                <div class="confidence-fill" style="width: {confidence:.0f}%; background: {conf_color};"></div>
                            </div>
                        </div>
                        <pre id="yaml-{suggestion_counter}" class="validation-yaml" style="display:none;">{yaml_snippet}</pre>
                    </div>
                """)

            html_parts.append("""
                </div>
            </div>
            """)

        return "".join(html_parts)

    def _generate_unified_summary(self, profile: ProfileResult) -> str:
        """Generate unified compact summary combining basic stats and Phase 2 findings."""

        # Count Phase 2 features and intelligent sampling
        temporal_columns = [col for col in profile.columns if col.temporal_analysis]
        pii_columns = [col for col in profile.columns if col.pii_info and col.pii_info.get('detected')]
        intelligent_sampled_columns = [
            col for col in profile.columns
            if hasattr(col.statistics, 'semantic_type') and
            col.statistics.semantic_type and
            col.statistics.semantic_type != 'unknown'
        ]
        has_phase2 = bool(temporal_columns or pii_columns or profile.dataset_privacy_risk or intelligent_sampled_columns)

        # Quality color
        quality_score = profile.overall_quality_score
        if quality_score >= 90:
            quality_color = '#10b981'
            quality_label = 'EXCELLENT'
        elif quality_score >= 75:
            quality_color = '#3b82f6'
            quality_label = 'GOOD'
        elif quality_score >= 50:
            quality_color = '#f59e0b'
            quality_label = 'FAIR'
        else:
            quality_color = '#ef4444'
            quality_label = 'POOR'

        # Build Phase 2 insights if available
        phase2_cards = ""
        if has_phase2:
            # Temporal insights
            if temporal_columns:
                freq_counts = {}
                for col in temporal_columns:
                    freq = col.temporal_analysis.get('frequency', {}).get('inferred', 'unknown')
                    freq_counts[freq] = freq_counts.get(freq, 0) + 1

                freq_summary = ", ".join([f"{count} {freq}" for freq, count in sorted(freq_counts.items())])
                phase2_cards += f'''
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; border-left: 3px solid #10b981;">
                    <div style="color: #10b981; font-size: 0.75em; text-transform: uppercase; font-weight: 600; margin-bottom: 6px;">📅 Temporal Analysis</div>
                    <div style="color: #cbd5e0; font-size: 0.85em;">{len(temporal_columns)} datetime column(s) • {freq_summary}</div>
                </div>
                '''

            # PII insights
            if pii_columns:
                high_risk = [col for col in pii_columns if col.pii_info.get('risk_score', 0) >= 70]
                pii_types = set()
                for col in pii_columns:
                    for pii_type in col.pii_info.get('pii_types', []):
                        pii_types.add(pii_type.get('name', 'Unknown'))

                types_summary = ", ".join(list(pii_types)[:3])
                risk_indicator = f"⚠️ {len(high_risk)} HIGH RISK" if high_risk else ""
                phase2_cards += f'''
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; border-left: 3px solid #f59e0b;">
                    <div style="color: #f59e0b; font-size: 0.75em; text-transform: uppercase; font-weight: 600; margin-bottom: 6px;">🔒 PII Detection</div>
                    <div style="color: #cbd5e0; font-size: 0.85em;">{len(pii_columns)} column(s) with PII • {types_summary} {risk_indicator}</div>
                </div>
                '''

            # Intelligent sampling insights
            if intelligent_sampled_columns:
                # Count semantic types
                type_counts = {}
                for col in intelligent_sampled_columns:
                    sem_type = col.statistics.semantic_type
                    type_counts[sem_type] = type_counts.get(sem_type, 0) + 1

                types_summary = ", ".join([f"{count} {typ}" for typ, count in sorted(type_counts.items())])
                optimization_pct = (len(intelligent_sampled_columns) / len(profile.columns) * 100) if profile.columns else 0
                phase2_cards += f'''
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; border-left: 3px solid #8b5cf6;">
                    <div style="color: #8b5cf6; font-size: 0.75em; text-transform: uppercase; font-weight: 600; margin-bottom: 6px;">🧠 Intelligent Sampling</div>
                    <div style="color: #cbd5e0; font-size: 0.85em;">{len(intelligent_sampled_columns)} column(s) optimized • {types_summary}</div>
                </div>
                '''

        # Analysis scope note
        scope_note = ""
        if has_phase2 and pii_columns:
            scope_note = f'''
            <div style="background: #1e293b; padding: 10px 12px; border-radius: 6px; border-left: 3px solid #3b82f6; margin-top: 16px;">
                <div style="color: #9ca3af; font-size: 0.75em;">
                    ℹ️ <strong>Analysis Scope:</strong> All {profile.row_count:,} rows analyzed for statistics and quality metrics.
                    PII detection based on sample of up to 1,000 values per column for performance.
                </div>
            </div>
            '''

        return f'''
        <div class="section" style="background: linear-gradient(135deg, #1e3a8a 0%, #312e81 100%); padding: 24px; margin-bottom: 20px;">
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: {20 if phase2_cards else 0}px;">
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; text-align: center;">
                    <div style="color: #9ca3af; font-size: 0.7em; text-transform: uppercase; margin-bottom: 4px;">Format</div>
                    <div style="color: #ffffff; font-size: 1.1em; font-weight: 600;">{profile.format.upper()}</div>
                </div>
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; text-align: center;">
                    <div style="color: #9ca3af; font-size: 0.7em; text-transform: uppercase; margin-bottom: 4px;">Size</div>
                    <div style="color: #ffffff; font-size: 1.1em; font-weight: 600;">{self._format_file_size(profile.file_size_bytes)}</div>
                </div>
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; text-align: center;">
                    <div style="color: #9ca3af; font-size: 0.7em; text-transform: uppercase; margin-bottom: 4px;">Total Rows</div>
                    <div style="color: #ffffff; font-size: 1.1em; font-weight: 600;" title="All rows analyzed">{profile.row_count:,}</div>
                    <div style="color: #6b7280; font-size: 0.65em; margin-top: 2px;">100% analyzed</div>
                </div>
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; text-align: center;">
                    <div style="color: #9ca3af; font-size: 0.7em; text-transform: uppercase; margin-bottom: 4px;">Columns</div>
                    <div style="color: #ffffff; font-size: 1.1em; font-weight: 600;">{profile.column_count}</div>
                </div>
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; text-align: center;">
                    <div style="color: #9ca3af; font-size: 0.7em; text-transform: uppercase; margin-bottom: 4px;">Quality</div>
                    <div style="color: {quality_color}; font-size: 1.1em; font-weight: 700;">{quality_score:.0f}% {quality_label}</div>
                </div>
                <div style="background: #1e293b; padding: 12px; border-radius: 6px; text-align: center;">
                    <div style="color: #9ca3af; font-size: 0.7em; text-transform: uppercase; margin-bottom: 4px;">Processing Time</div>
                    <div style="color: #ffffff; font-size: 1.1em; font-weight: 600;">{profile.processing_time_seconds:.1f}s</div>
                </div>
            </div>
            {f'<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 12px;">{phase2_cards}</div>' if phase2_cards else ''}
            {scope_note}
        </div>
        '''

    def _generate_phase2_summary(self, profile: ProfileResult) -> str:
        """Generate Phase 2 findings summary section."""

        # Count Phase 2 features
        temporal_columns = [col for col in profile.columns if col.temporal_analysis]
        pii_columns = [col for col in profile.columns if col.pii_info and col.pii_info.get('detected')]

        # If no Phase 2 features detected, return empty
        if not temporal_columns and not pii_columns and not profile.dataset_privacy_risk:
            return ""

        # Analyze temporal findings
        temporal_insights = []
        if temporal_columns:
            # Count by frequency
            freq_counts = {}
            gap_columns = []
            trend_columns = {'increasing': [], 'decreasing': [], 'stationary': []}

            for col in temporal_columns:
                temp = col.temporal_analysis
                freq = temp.get('frequency', {}).get('inferred', 'unknown')
                freq_counts[freq] = freq_counts.get(freq, 0) + 1

                if temp.get('gaps', {}).get('gaps_detected'):
                    gap_columns.append(col.name)

                trend = temp.get('trend', {}).get('direction', 'unknown')
                if trend in trend_columns:
                    trend_columns[trend].append(col.name)

            temporal_insights.append(f"Found {len(temporal_columns)} datetime/timestamp columns")
            if freq_counts:
                freq_list = [f"{count} {freq}" for freq, count in sorted(freq_counts.items())]
                temporal_insights.append(f"Frequencies: {', '.join(freq_list)}")
            if gap_columns:
                temporal_insights.append(f"{len(gap_columns)} column(s) have missing time periods")
            if trend_columns['increasing']:
                temporal_insights.append(f"{len(trend_columns['increasing'])} column(s) show increasing trends")
            if trend_columns['decreasing']:
                temporal_insights.append(f"{len(trend_columns['decreasing'])} column(s) show decreasing trends")

        # Analyze PII findings
        pii_insights = []
        if pii_columns:
            high_risk = [col for col in pii_columns if col.pii_info.get('risk_score', 0) >= 70]
            moderate_risk = [col for col in pii_columns if 40 <= col.pii_info.get('risk_score', 0) < 70]
            low_risk = [col for col in pii_columns if col.pii_info.get('risk_score', 0) < 40]

            # Collect PII types
            pii_type_counts = {}
            all_frameworks = set()
            for col in pii_columns:
                for pii_type in col.pii_info.get('pii_types', []):
                    type_name = pii_type.get('name', 'Unknown')
                    pii_type_counts[type_name] = pii_type_counts.get(type_name, 0) + 1
                all_frameworks.update(col.pii_info.get('regulatory_frameworks', []))

            pii_insights.append(f"Detected {len(pii_columns)} column(s) containing PII")
            if high_risk:
                pii_insights.append(f"⚠️ {len(high_risk)} HIGH RISK column(s) require immediate attention")
            if moderate_risk:
                pii_insights.append(f"{len(moderate_risk)} MODERATE RISK column(s)")
            if pii_type_counts:
                top_types = sorted(pii_type_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                type_list = [f"{name} ({count})" for name, count in top_types]
                pii_insights.append(f"Types detected: {', '.join(type_list)}")
            if all_frameworks:
                pii_insights.append(f"Applicable regulations: {', '.join(sorted(all_frameworks)[:5])}")

        # Dataset privacy risk
        privacy_summary = ""
        if profile.dataset_privacy_risk:
            risk = profile.dataset_privacy_risk
            risk_level = risk.get('risk_level', 'unknown').upper()
            risk_score = risk.get('risk_score', 0)
            risk_color = '#ef4444' if risk_score >= 70 else '#f59e0b' if risk_score >= 40 else '#10b981'

            privacy_summary = f'''
            <div style="margin-top: 20px; padding: 15px; background: #2d3748; border-radius: 8px; border-left: 4px solid {risk_color};">
                <h4 style="color: {risk_color}; margin-bottom: 10px;">
                    🔒 Dataset Privacy Risk: {risk_level} ({risk_score:.1f}/100)
                </h4>
                <div style="color: #cbd5e0; font-size: 0.9em;">
                    {risk.get('pii_column_count', 0)} of {profile.column_count} columns ({risk.get('pii_column_percentage', 0):.1f}%) contain personally identifiable information
                </div>
            </div>
            '''

        # Build summary HTML
        summary_html = f'''
        <div id="phase2-summary" class="section" style="background: linear-gradient(135deg, #1e3a8a 0%, #312e81 100%); border-radius: 12px; padding: 30px; margin-bottom: 30px;">
            <h2 style="color: #60a5fa; margin-bottom: 20px; font-size: 1.5em;">
                🔬 Advanced Analysis Summary
            </h2>
            <div style="color: #cbd5e0; font-size: 0.95em; margin-bottom: 20px;">
                This report includes advanced profiling features: temporal pattern analysis and PII detection with privacy risk assessment.
            </div>
        '''

        if temporal_insights:
            summary_html += f'''
            <div style="margin-bottom: 20px; padding: 15px; background: #1e293b; border-radius: 8px; border-left: 4px solid #10b981;">
                <h4 style="color: #10b981; margin-bottom: 10px;">📅 Temporal Analysis Findings</h4>
                <ul style="color: #cbd5e0; margin-left: 20px; font-size: 0.9em;">
                    {"".join([f"<li style='margin: 6px 0;'>{insight}</li>" for insight in temporal_insights])}
                </ul>
            </div>
            '''

        if pii_insights:
            summary_html += f'''
            <div style="margin-bottom: 20px; padding: 15px; background: #1e293b; border-radius: 8px; border-left: 4px solid #f59e0b;">
                <h4 style="color: #f59e0b; margin-bottom: 10px;">🔒 PII Detection Findings</h4>
                <ul style="color: #cbd5e0; margin-left: 20px; font-size: 0.9em;">
                    {"".join([f"<li style='margin: 6px 0;'>{insight}</li>" for insight in pii_insights])}
                </ul>
            </div>
            '''

        summary_html += privacy_summary
        summary_html += "</div>"

        return summary_html

    def _generate_temporal_viz(self, col: ColumnProfile) -> str:
        """Generate temporal analysis visualization for a column."""
        if not col.temporal_analysis:
            return ""

        temporal = col.temporal_analysis

        # Extract key metrics
        frequency = temporal.get('frequency', {})
        freq_str = frequency.get('inferred', 'unknown')
        freq_confidence = frequency.get('confidence', 0) * 100

        gaps = temporal.get('gaps', {})
        gap_count = gaps.get('gap_count', 0)
        gaps_detected = gaps.get('gaps_detected', False)

        trend = temporal.get('trend', {})
        trend_direction = trend.get('direction', 'unknown')
        trend_r2 = trend.get('r_squared', 0)

        date_range = temporal.get('date_range', {})
        span_days = date_range.get('span_days', 0)

        # Generate frequency explanation
        freq_explain = {
            'daily': 'Data points occur approximately once per day',
            'weekly': 'Data points occur approximately once per week',
            'monthly': 'Data points occur approximately once per month',
            'hourly': 'Data points occur approximately once per hour',
            'quarterly': 'Data points occur approximately once per quarter',
            'yearly': 'Data points occur approximately once per year',
            'irregular': 'No consistent time interval detected between data points'
        }.get(freq_str.lower(), 'Time interval pattern detected in the data')

        # Generate trend explanation
        trend_explain = {
            'increasing': 'Values are generally rising over time',
            'decreasing': 'Values are generally declining over time',
            'stationary': 'Values remain relatively stable over time',
            'unknown': 'No clear trend pattern detected'
        }.get(trend_direction.lower(), 'Trend analysis performed')

        # R² interpretation
        if trend_r2 >= 0.7:
            r2_explain = 'Strong trend (highly predictable)'
        elif trend_r2 >= 0.4:
            r2_explain = 'Moderate trend (somewhat predictable)'
        else:
            r2_explain = 'Weak trend (less predictable)'

        return f'''
        <div style="margin-top: 20px; padding: 15px; background: #374151; border-radius: 8px; border-left: 4px solid #10b981;">
            <h4 style="color: #10b981; margin-bottom: 10px; font-size: 0.95em;">
                📅 TEMPORAL ANALYSIS
            </h4>
            <div style="color: #9ca3af; font-size: 0.85em; margin-bottom: 15px; padding: 8px; background: #1f2937; border-radius: 4px;">
                ℹ️ Temporal analysis examines time-based patterns in your date/timestamp data, including frequency, gaps, and trends.
            </div>

            <div class="info-row">
                <span class="info-label">Frequency:</span>
                <span class="info-value">{freq_str.title()} ({freq_confidence:.0f}% confidence)</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 10px;">
                {freq_explain}
            </div>

            <div class="info-row">
                <span class="info-label">Date Range:</span>
                <span class="info-value">{span_days} days</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 10px;">
                Total time span covered by this column's data
            </div>

            <div class="info-row">
                <span class="info-label">Gaps Detected:</span>
                <span class="info-value">{"Yes - " + str(gap_count) + " gaps" if gaps_detected else "No gaps"}</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 10px;">
                {f"Missing {gap_count} expected time periods based on detected frequency" if gaps_detected else "All expected time periods present in sequence"}
            </div>

            <div class="info-row">
                <span class="info-label">Trend:</span>
                <span class="info-value">{trend_direction.title()} (R² = {trend_r2:.3f})</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px;">
                {trend_explain} • {r2_explain}
            </div>
        </div>
        '''

    def _generate_pii_viz(self, col: ColumnProfile) -> str:
        """Generate PII detection visualization for a column."""
        if not col.pii_info or not col.pii_info.get('detected'):
            return ""

        pii = col.pii_info
        risk_score = pii.get('risk_score', 0)
        pii_types = pii.get('pii_types', [])
        frameworks = pii.get('regulatory_frameworks', [])
        redaction = pii.get('redaction_strategy')
        if isinstance(redaction, dict):
            redaction = redaction.get('strategy', 'mask')
        elif not redaction:
            redaction = 'mask'

        # Determine risk level color
        if risk_score >= 70:
            risk_color = '#ef4444'  # Red
            risk_level = 'HIGH'
        elif risk_score >= 40:
            risk_color = '#f59e0b'  # Orange
            risk_level = 'MODERATE'
        else:
            risk_color = '#10b981'  # Green
            risk_level = 'LOW'

        # Format PII types
        pii_types_str = ", ".join([
            f"{pt.get('name', 'Unknown')} ({int(pt.get('confidence', 0) * 100)}%)"
            for pt in pii_types[:3]  # Top 3 types
        ])

        # Format frameworks
        frameworks_str = ", ".join(frameworks[:5]) if frameworks else "None detected"

        # Generate risk explanation
        risk_explanations = {
            'HIGH': 'This data poses significant privacy risks and requires strong protection measures',
            'MODERATE': 'This data should be handled with care and appropriate security measures',
            'LOW': 'This data has minimal privacy risk but should still be protected'
        }
        risk_explain = risk_explanations.get(risk_level, 'Privacy risk assessment completed')

        # Generate redaction explanation
        redaction_explanations = {
            'mask': 'Replace characters with asterisks (e.g., ***@email.com)',
            'hash': 'Convert to one-way cryptographic hash for anonymization',
            'encrypt': 'Use reversible encryption for authorized access only',
            'remove': 'Delete this data entirely if not essential'
        }
        redaction_explain = redaction_explanations.get(redaction.lower(), 'Apply appropriate data protection method')

        return f'''
        <div style="margin-top: 20px; padding: 15px; background: #374151; border-radius: 8px; border-left: 4px solid {risk_color};">
            <h4 style="color: {risk_color}; margin-bottom: 10px; font-size: 0.95em;">
                🔒 PII DETECTED - {risk_level} RISK
            </h4>
            <div style="color: #9ca3af; font-size: 0.85em; margin-bottom: 15px; padding: 8px; background: #1f2937; border-radius: 4px;">
                ℹ️ PII (Personally Identifiable Information) detection identifies sensitive data that could identify individuals and assesses privacy risks.
            </div>

            <div class="info-row">
                <span class="info-label">Risk Score:</span>
                <span class="info-value" style="color: {risk_color}; font-weight: bold;">{risk_score}/100</span>
            </div>
            <div style="background: #1f2937; border-radius: 4px; height: 8px; margin: 10px 0;">
                <div style="background: {risk_color}; height: 100%; width: {risk_score}%; border-radius: 4px; transition: width 0.3s;"></div>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-bottom: 15px;">
                {risk_explain}
            </div>

            <div class="info-row">
                <span class="info-label">PII Types:</span>
                <span class="info-value">{pii_types_str or "Unknown"}</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 10px;">
                Types of personally identifiable information detected in this column
            </div>

            <div class="info-row">
                <span class="info-label">Regulatory Frameworks:</span>
                <span class="info-value" style="font-size: 0.85em;">{frameworks_str}</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 10px;">
                Privacy regulations that may apply to this data (GDPR, CCPA, HIPAA, etc.)
            </div>

            <div class="info-row">
                <span class="info-label">Recommended Action:</span>
                <span class="info-value">{redaction.title() if redaction else "Review and mask"}</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 15px;">
                {redaction_explain}
            </div>

            <div style="margin-top: 12px; padding: 10px; background: #1f2937; border-radius: 4px; border-left: 3px solid {risk_color};">
                <div style="color: #cbd5e0; font-size: 0.85em;">
                    <strong>⚠️ Privacy Protection Required</strong><br>
                    This column contains personally identifiable information. Best practices:<br>
                    • Implement data masking for non-production environments<br>
                    • Establish role-based access controls<br>
                    • Enable audit logging for PII access<br>
                    • Document data retention and deletion policies<br>
                    • Ensure compliance with applicable privacy regulations
                </div>
            </div>
        </div>
        '''

    def _generate_semantic_viz(self, col: ColumnProfile) -> str:
        """Generate semantic understanding visualization for a column (FIBO-based)."""
        if not col.semantic_info:
            return ""

        semantic = col.semantic_info
        primary_tag = semantic.get('primary_tag', 'unknown')
        if primary_tag == 'unknown':
            return ""

        all_tags = semantic.get('semantic_tags', [primary_tag])
        confidence = semantic.get('confidence', 0.0)
        explanation = semantic.get('explanation', '')
        evidence = semantic.get('evidence', {})
        fibo_source = semantic.get('fibo_source', None)

        # Determine confidence color
        if confidence >= 0.90:
            conf_color = '#10b981'  # Green
            conf_level = 'HIGH'
        elif confidence >= 0.70:
            conf_color = '#3b82f6'  # Blue
            conf_level = 'GOOD'
        elif confidence >= 0.50:
            conf_color = '#f59e0b'  # Orange
            conf_level = 'MODERATE'
        else:
            conf_color = '#9ca3af'  # Gray
            conf_level = 'LOW'

        # Tag category icons (based on primary tag prefix)
        tag_icons = {
            'money': '💰',
            'banking': '🏦',
            'loan': '🏠',
            'security': '📈',
            'party': '👤',
            'identifier': '🔑',
            'temporal': '📅',
            'category': '🏷️',
            'risk': '⚠️',
            'contact': '📧',
            'pii': '🔒'
        }

        # Get category from primary tag
        category = primary_tag.split('.')[0] if '.' in primary_tag else primary_tag
        icon = tag_icons.get(category, '🧠')

        # Format semantic tags as badges
        tags_html = " ".join([
            f'<span style="display: inline-block; background: #374151; color: #9ca3af; padding: 4px 10px; border-radius: 12px; font-size: 0.75em; margin: 2px;">{tag}</span>'
            for tag in all_tags[:5]  # Show top 5 tags
        ])

        # Format evidence
        evidence_items = []
        if evidence.get('visions_type'):
            evidence_items.append(f"✓ Visions detected as <strong>{evidence['visions_type']}</strong>")
        if evidence.get('fibo_match'):
            evidence_items.append(f"✓ Matched FIBO pattern for <strong>{evidence['fibo_match']}</strong>")
        if evidence.get('name_match'):
            evidence_items.append(f"✓ Column name pattern match")

        evidence_html = "<br>".join(evidence_items) if evidence_items else "Based on data properties and patterns"

        # Format FIBO reference
        fibo_html = ""
        if fibo_source:
            fibo_class_name = fibo_source.split(':')[-1] if ':' in fibo_source else fibo_source
            fibo_html = f'''
            <div style="margin-top: 10px; padding: 8px; background: #1f2937; border-radius: 4px; border-left: 3px solid #667eea;">
                <div style="color: #cbd5e0; font-size: 0.85em;">
                    <strong>🏦 FIBO Ontology Reference</strong><br>
                    <span style="color: #9ca3af;">Class:</span> <code style="color: #a5b4fc; background: #374151; padding: 2px 6px; border-radius: 3px; font-size: 0.9em;">{fibo_class_name}</code><br>
                    <span style="color: #9ca3af; font-size: 0.8em;">Financial Industry Business Ontology - industry standard for financial data semantics</span>
                </div>
            </div>
            '''

        return f'''
        <div style="margin-top: 20px; padding: 15px; background: linear-gradient(135deg, #1e293b 0%, #334155 100%); border-radius: 8px; border-left: 4px solid {conf_color};">
            <h4 style="color: {conf_color}; margin-bottom: 10px; font-size: 0.95em;">
                {icon} SEMANTIC UNDERSTANDING
            </h4>
            <div style="color: #9ca3af; font-size: 0.85em; margin-bottom: 15px; padding: 8px; background: #1f2937; border-radius: 4px;">
                ℹ️ DataK9 uses FIBO (Financial Industry Business Ontology) to understand what this column represents and suggest appropriate validations.
            </div>

            <div class="info-row">
                <span class="info-label">Primary Classification:</span>
                <span class="info-value" style="color: {conf_color}; font-weight: bold;">{primary_tag}</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.8em; margin-left: 20px; margin-top: 4px; margin-bottom: 15px;">
                {explanation}
            </div>

            <div class="info-row">
                <span class="info-label">Confidence:</span>
                <span class="info-value" style="color: {conf_color}; font-weight: bold;">{int(confidence * 100)}% ({conf_level})</span>
            </div>
            <div style="background: #1f2937; border-radius: 4px; height: 8px; margin: 10px 0;">
                <div style="background: {conf_color}; height: 100%; width: {int(confidence * 100)}%; border-radius: 4px; transition: width 0.3s;"></div>
            </div>

            <div class="info-row" style="margin-top: 15px;">
                <span class="info-label">All Semantic Tags:</span>
            </div>
            <div style="margin-top: 8px; margin-bottom: 15px;">
                {tags_html}
            </div>

            <div class="info-row">
                <span class="info-label">Evidence:</span>
            </div>
            <div style="color: #9ca3af; font-size: 0.85em; margin-left: 20px; margin-top: 8px; margin-bottom: 15px;">
                {evidence_html}
            </div>

            {fibo_html}

            <div style="margin-top: 12px; padding: 10px; background: #1f2937; border-radius: 4px; border-left: 3px solid {conf_color};">
                <div style="color: #cbd5e0; font-size: 0.85em;">
                    <strong>💡 Smart Validations</strong><br>
                    Based on the semantic understanding of <strong>{primary_tag}</strong>, DataK9 automatically suggests context-aware validation rules that make sense for this type of data.
                </div>
            </div>
        </div>
        '''

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (text
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#39;'))
