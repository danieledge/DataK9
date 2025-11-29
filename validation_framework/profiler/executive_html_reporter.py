"""
Executive HTML report generator for data profiling results.

Generates a modern, executive-style dashboard with:
- KPI cards for key metrics
- Interactive accordion sections
- Chart.js visualizations (doughnut, bar, bubble)
- WordCloud for categorical data
- Column explorer with detailed statistics
- Validation suggestions with copy-to-clipboard YAML
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from validation_framework.profiler.profile_result import ProfileResult, ColumnProfile
from validation_framework.profiler.insight_engine import InsightEngine, generate_insights
import logging
import math

logger = logging.getLogger(__name__)


class ExecutiveHTMLReporter:
    """Generate executive-style HTML reports for profile results."""

    def generate_report(self, profile: ProfileResult, output_path: str) -> None:
        """
        Generate HTML report from profile result.

        Args:
            profile: ProfileResult to report
            output_path: Path to write HTML file
        """
        logger.info(f"Generating executive profile HTML report: {output_path}")

        html_content = self._generate_html(profile)

        # Write to file
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)

        logger.info(f"Executive profile report written to: {output_path}")

    def _generate_html(self, profile: ProfileResult) -> str:
        """Generate complete HTML content."""

        # Prepare data
        file_size_mb = profile.file_size_bytes / (1024 * 1024)
        file_size_display = self._format_file_size(profile.file_size_bytes)
        processing_time_display = self._format_duration(profile.processing_time_seconds)
        profiled_at_display = profile.profiled_at.strftime("%Y-%m-%d %H:%M")

        # Calculate aggregate metrics
        avg_completeness = sum(col.quality.completeness for col in profile.columns) / len(profile.columns) if profile.columns else 0
        avg_validity = sum(col.quality.validity for col in profile.columns) / len(profile.columns) if profile.columns else 0
        avg_consistency = sum(col.quality.consistency for col in profile.columns) / len(profile.columns) if profile.columns else 0
        avg_uniqueness = sum(col.quality.uniqueness for col in profile.columns) / len(profile.columns) if profile.columns else 0

        # Count data types
        type_counts = self._count_data_types(profile.columns)

        # PII detection
        pii_columns = self._get_pii_columns(profile.columns)
        pii_count = len(pii_columns)
        max_pii_risk = max((col.pii_info.get('risk_score', 0) for col in profile.columns if col.pii_info and col.pii_info.get('detected')), default=0)

        # Temporal columns
        temporal_columns = [col for col in profile.columns if col.temporal_analysis and col.temporal_analysis.get('available')]

        # Find categorical columns for word cloud (low cardinality string columns)
        categorical_columns = self._get_categorical_columns(profile.columns)

        # Quality status
        quality_status = self._get_quality_status(profile.overall_quality_score)

        # Get sampling info
        sampling_info = self._get_sampling_info(profile.columns)

        # Run insight engine for narrative insights
        profile_dict = profile.to_dict()
        insights = generate_insights(profile_dict)

        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <title>Data Quality Report - {profile.file_name}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js" defer></script>
    <script src="https://cdn.jsdelivr.net/npm/wordcloud@1.2.2/src/wordcloud2.min.js" defer></script>

    <style>
{self._get_css()}
    </style>
</head>
<body>
    <!-- Top Navigation -->
    <header class="top-nav">
        <div class="top-inner">
            <div class="brand">
                <div class="brand-mark">DK9</div>
                <div class="brand-text">
                    <span>DataK9</span>
                    <span>Data Profiler</span>
                </div>
            </div>
            <div class="file-meta">
                <span class="file-name-pill">{profile.file_name}</span>
                <span><span class="pill-dot"></span>{profile.row_count:,} rows</span>
                <span><span class="pill-dot"></span>{file_size_display}</span>
                <span><span class="pill-dot"></span>{processing_time_display}</span>
                <span><span class="pill-dot"></span>{profiled_at_display}</span>
            </div>
        </div>
    </header>

    <!-- Sticky Section Navigator -->
    <nav class="sticky-nav" id="stickyNav">
        <div class="sticky-nav-inner">
            <button class="nav-btn active" data-section="summary">Summary</button>
            <button class="nav-btn" data-section="risks">Risks</button>
            <button class="nav-btn" data-section="quality">Quality</button>
            <button class="nav-btn" data-section="columns">Columns</button>
            <button class="nav-btn" data-section="validations">Validations</button>
            <span class="nav-spacer"></span>
            <button class="expand-all-btn" id="expandAllBtn" onclick="toggleExpandAll()" title="Expand/Collapse all sections">
                <span class="expand-icon">+</span> Expand All
            </button>
        </div>
    </nav>

    <main class="page">
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 1. HEADER & SAMPLING BANNER                                     -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <section class="page-header" id="section-summary">
            <div class="page-title-block">
                <h1>Data Profile Report</h1>
                <p>Analysis of {profile.row_count:,} records across {profile.column_count} columns</p>
            </div>
        </section>

        <!-- v2 Sampling Coverage Banner -->
        {self._generate_sampling_banner_v2(profile, insights)}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 2. METRICS DASHBOARD (3 rows: Core, Quality, Types)             -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        {self._generate_metrics_dashboard_v2(profile, avg_completeness, avg_validity, avg_consistency, avg_uniqueness, type_counts)}

        <!-- FIBO Semantic Categories Summary -->
        {self._generate_fibo_summary(profile.columns)}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 3. DATA QUALITY OVERVIEW - High-level metrics first             -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" id="section-quality" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #1e3a5f 0%, #0d1f3c 100%); border-radius: 8px; border-left: 4px solid #3b82f6;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">Data Quality Overview</h2>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #94a3b8;">Type distribution, value patterns, and quality scores by column</p>
                </div>
                <div style="background: rgba(255,255,255,0.15); padding: 4px 12px; border-radius: 4px; font-size: 0.8em; font-weight: 600; color: white;">OVERVIEW</div>
            </div>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <!-- Data Types Accordion -->
                {self._generate_overview_accordion(profile, type_counts, avg_completeness, avg_validity, avg_consistency, avg_uniqueness)}

                <!-- Quality Metrics Accordion -->
                {self._generate_quality_accordion(profile)}

                <!-- Value Distribution Accordion -->
                {self._generate_distribution_accordion(profile, categorical_columns)}

                <!-- Temporal Analysis Accordion -->
                {self._generate_temporal_accordion(temporal_columns) if temporal_columns else ''}
            </div>
        </div>

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 4. DATA INSIGHTS - ML-powered patterns & anomalies              -->
        <!-- ═══════════════════════════════════════════════════════════════ -->

        <!-- PII Risk Section (if detected) - still important to highlight -->
        {self._generate_pii_section(pii_columns) if pii_count > 0 else ''}

        <!-- v2 Insight Widgets (Plain English + Examples + Technical) -->
        {self._generate_ml_section_v2(profile.ml_findings) if profile.ml_findings else ''}

        <!-- Advanced Visualizations -->
        {self._generate_advanced_visualizations(profile.ml_findings, profile.columns) if profile.ml_findings and profile.ml_findings.get('visualizations') else ''}

        <!-- FIBO Semantic Analysis (if applicable) -->
        {self._generate_fibo_section(profile)}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 5. COLUMN-LEVEL QUALITY SUMMARY                                 -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" id="section-columns" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #374151 0%, #1f2937 100%); border-radius: 8px; border-left: 4px solid #6b7280;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">Column-Level Analysis</h2>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #9ca3af;">Detailed statistics, patterns, and quality metrics for each field</p>
                </div>
                <div style="background: rgba(255,255,255,0.15); padding: 4px 12px; border-radius: 4px; font-size: 0.8em; font-weight: 600; color: white;">{profile.column_count} COLUMNS</div>
            </div>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <!-- Column Explorer -->
                {self._generate_column_explorer(profile)}
            </div>
        </div>

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 6. RECOMMENDED VALIDATIONS                                      -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" id="section-validations" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #065f46 0%, #022c22 100%); border-radius: 8px; border-left: 4px solid #10b981;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">Recommended Validations</h2>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #6ee7b7;">Based on our analysis, implement these rules to protect data quality</p>
                </div>
                <div style="background: rgba(255,255,255,0.15); padding: 4px 12px; border-radius: 4px; font-size: 0.8em; font-weight: 600; color: white;">ACTION</div>
            </div>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <!-- Validation Suggestions -->
                {self._generate_suggestions_accordion(profile)}

                <!-- Full Validation Config -->
                {self._generate_full_config_accordion(profile)}
            </div>
        </div>

        <!-- Sampling Summary -->
        {self._generate_sampling_summary_enhanced(profile, sampling_info, insights)}
    </main>

    <script>
{self._get_javascript(profile, type_counts, categorical_columns)}
    </script>
</body>
</html>'''

        return html

    def _get_css(self) -> str:
        """Return the CSS styles - v2 widget-based dashboard design."""
        return '''        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            /* Core backgrounds - deep navy/slate palette */
            --bg-main: #0a0f1a;
            --bg-elevated: #0f172a;
            --bg-card: linear-gradient(180deg, #0f172a 0%, #0a0f1a 100%);
            --bg-hover: #1e293b;
            --bg-tertiary: #151d30;
            --card-bg: #0f172a;
            --border-subtle: rgba(148, 163, 184, 0.08);
            --border-color: rgba(148, 163, 184, 0.15);
            --border-focus: rgba(96, 165, 250, 0.4);

            /* Accent colors */
            --accent: #60a5fa;
            --accent-soft: rgba(96, 165, 250, 0.08);
            --accent-gradient: linear-gradient(135deg, #3b82f6, #8b5cf6);
            --primary: #8b5cf6;
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
            --text-tertiary: #64748b;
            --text-muted: #64748b;
            --good: #10b981;
            --good-soft: rgba(16, 185, 129, 0.12);
            --warning: #f59e0b;
            --warning-soft: rgba(245, 158, 11, 0.12);
            --critical: #ef4444;
            --critical-soft: rgba(239, 68, 68, 0.12);
            --info: #3b82f6;
            --info-soft: rgba(59, 130, 246, 0.12);
            --radius-sm: 6px;
            --radius-md: 10px;
            --radius-lg: 14px;
            --radius-xl: 20px;
            --shadow-card: 0 4px 24px rgba(0, 0, 0, 0.4);
            --shadow-glow: 0 0 40px rgba(96, 165, 250, 0.08);
            --transition-fast: 0.15s ease-out;
            --transition-med: 0.25s ease-out;
        }

        body {
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: radial-gradient(ellipse at top left, #111827 0%, #030712 50%, #030712 100%);
            color: var(--text-primary);
            min-height: 100vh;
            line-height: 1.5;
        }

        .top-nav {
            background: rgba(11, 16, 32, 0.85);
            backdrop-filter: blur(12px);
            border-bottom: 1px solid var(--border-subtle);
            position: sticky;
            top: 0;
            z-index: 100;
        }

        .top-inner {
            max-width: 1600px;
            margin: 0 auto;
            padding: 14px 32px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 24px;
            flex-wrap: wrap;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 14px;
        }

        .brand-mark {
            width: 42px;
            height: 42px;
            background: var(--accent-gradient);
            border-radius: var(--radius-md);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 800;
            font-size: 1.1em;
            color: #fff;
            box-shadow: 0 4px 16px rgba(59, 130, 246, 0.3);
        }

        .brand-text {
            display: flex;
            flex-direction: column;
            gap: 2px;
        }

        .brand-text span:first-child {
            font-weight: 700;
            font-size: 1.05em;
            color: var(--text-primary);
        }

        .brand-text span:last-child {
            font-size: 0.75em;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .file-meta {
            display: flex;
            align-items: center;
            gap: 20px;
            font-size: 0.85em;
            color: var(--text-secondary);
            flex-wrap: wrap;
        }

        .file-name-pill {
            background: var(--accent-soft);
            color: var(--accent);
            padding: 6px 14px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.85em;
            border: 1px solid rgba(96, 165, 250, 0.2);
        }

        .pill-dot {
            display: inline-block;
            width: 6px;
            height: 6px;
            background: var(--text-muted);
            border-radius: 50%;
            margin-right: 8px;
        }

        .page {
            max-width: 1600px;
            margin: 0 auto;
            padding: 32px;
        }

        .page-header {
            margin-bottom: 32px;
        }

        .page-title-block h1 {
            font-size: 1.4em;
            font-weight: 700;
            margin-bottom: 6px;
            background: linear-gradient(135deg, #f1f5f9 0%, #94a3b8 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }

        .page-title-block p {
            color: var(--text-secondary);
            font-size: 0.95em;
        }

        .kpi-belt {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }

        @media (max-width: 768px) {
            .kpi-belt {
                grid-template-columns: repeat(2, 1fr);
            }
        }

        .kpi-card {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 20px;
            text-align: center;
        }

        .kpi-label {
            font-size: 0.75em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 8px;
        }

        .kpi-value {
            font-size: 1.5em;
            font-weight: 700;
            color: var(--text-primary);
        }

        .kpi-value.critical {
            color: var(--critical);
        }

        .kpi-trend {
            font-size: 0.75em;
            margin-top: 8px;
        }

        .kpi-trend.good { color: var(--good); }
        .kpi-trend.warning { color: var(--warning); }
        .kpi-trend.critical { color: var(--critical); }

        .sampling-bar {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 16px 24px;
            margin-bottom: 24px;
            display: flex;
            flex-wrap: wrap;
            gap: 24px;
            align-items: center;
        }

        .sampling-bar-title {
            font-weight: 600;
            color: var(--text-primary);
        }

        .sampling-stat {
            display: flex;
            flex-direction: column;
            gap: 2px;
        }

        .sampling-stat-label {
            font-size: 0.7em;
            text-transform: uppercase;
            color: var(--text-muted);
        }

        .sampling-stat-value {
            font-size: 0.9em;
            color: var(--text-secondary);
        }

        .sampling-stat-value.highlight {
            color: var(--accent);
            font-weight: 600;
        }

        .hint-box {
            background: var(--bg-elevated);
            border-left: 3px solid var(--accent);
            padding: 12px 16px;
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            font-size: 0.82em;
            color: var(--text-secondary);
            margin-bottom: 16px;
        }

        /* FIBO Section Styles */
        .fibo-section {
            background: linear-gradient(135deg, #1e1b4b 0%, var(--bg-card) 100%);
            border: 1px solid rgba(139, 92, 246, 0.3);
            border-radius: var(--radius-lg);
            padding: 20px;
            margin: 16px 0;
        }

        .fibo-header {
            display: flex;
            align-items: center;
            gap: 14px;
            margin-bottom: 16px;
        }

        .fibo-icon {
            font-size: 2em;
        }

        .fibo-title h3 {
            color: #c4b5fd;
            font-size: 1.1em;
            margin: 0;
        }

        .fibo-title p {
            color: var(--text-muted);
            font-size: 0.85em;
            margin: 2px 0 0 0;
        }

        .fibo-content {
            font-size: 0.9em;
        }

        .fibo-description {
            color: var(--text-secondary);
            line-height: 1.6;
            margin-bottom: 16px;
        }

        .fibo-description strong {
            color: #a5b4fc;
        }

        .fibo-stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 12px;
            margin-bottom: 16px;
        }

        .fibo-stat {
            background: var(--bg-elevated);
            padding: 14px;
            border-radius: var(--radius-md);
            text-align: center;
        }

        .fibo-stat-value {
            font-size: 1.2em;
            font-weight: 600;
            color: #c4b5fd;
        }

        .fibo-stat-label {
            font-size: 0.75em;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 4px;
        }

        .fibo-stat-note {
            font-size: 0.7em;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .fibo-categories {
            margin-bottom: 8px;
        }

        .fibo-categories-label {
            font-size: 0.8em;
            color: var(--text-muted);
            margin-right: 8px;
        }

        .fibo-category-tag {
            display: inline-block;
            background: rgba(139, 92, 246, 0.15);
            color: #c4b5fd;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            margin: 2px;
        }

        .layout-grid {
            display: block;
            max-width: 100%;
            overflow: hidden;
        }

        .main-column {
            min-width: 0;
        }

        /* Quality Alerts */
        .quality-alert-item {
            display: grid;
            grid-template-columns: auto auto 1fr;
            gap: 12px;
            padding: 10px 14px;
            border-radius: var(--radius-sm);
            align-items: center;
            font-size: 0.9em;
        }

        .quality-alert-item.critical {
            background: rgba(239, 68, 68, 0.1);
            border-left: 3px solid var(--critical);
        }

        .quality-alert-item.warning {
            background: rgba(245, 158, 11, 0.08);
            border-left: 3px solid var(--warning);
        }

        .quality-alert-item.info {
            background: rgba(59, 130, 246, 0.08);
            border-left: 3px solid var(--info);
        }

        .alert-icon { font-size: 1.1em; }
        .alert-column { font-weight: 600; color: var(--text-primary); min-width: 100px; }
        .alert-issue { color: var(--text-secondary); font-weight: 500; }
        .alert-detail { color: var(--text-muted); font-size: 0.9em; grid-column: 2 / -1; }

        @media (max-width: 768px) {
            .quality-alert-item {
                grid-template-columns: auto 1fr;
            }
            .alert-detail {
                grid-column: 1 / -1;
                margin-top: 4px;
            }
        }

        .accordion {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            margin-bottom: 16px;
            overflow: hidden;
        }

        .accordion-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 20px;
            cursor: pointer;
            transition: background var(--transition-fast);
        }

        .accordion-header:hover {
            background: var(--bg-hover);
        }

        .accordion-title-group {
            display: flex;
            align-items: center;
            gap: 14px;
        }

        .accordion-icon {
            width: 36px;
            height: 36px;
            border-radius: var(--radius-md);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.1em;
        }

        .accordion-icon.overview { background: var(--info-soft); }
        .accordion-icon.quality { background: var(--good-soft); }
        .accordion-icon.columns { background: var(--accent-soft); }
        .accordion-icon.issues { background: var(--critical-soft); }

        .accordion-title {
            font-weight: 600;
            color: var(--text-primary);
        }

        .accordion-subtitle {
            font-size: 0.8em;
            color: var(--text-muted);
        }

        .accordion-meta {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .accordion-badge {
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.7em;
            font-weight: 600;
            text-transform: uppercase;
        }

        .accordion-badge.good { background: var(--good-soft); color: var(--good); }
        .accordion-badge.warning { background: var(--warning-soft); color: var(--warning); }
        .accordion-badge.critical { background: var(--critical-soft); color: var(--critical); }
        .accordion-badge.info { background: var(--info-soft); color: var(--info); }

        .accordion-chevron {
            color: var(--text-muted);
            transition: transform var(--transition-fast);
        }

        .accordion.open .accordion-chevron {
            transform: rotate(180deg);
        }

        .accordion-body {
            display: none;
            border-top: 1px solid var(--border-subtle);
        }

        .accordion.open .accordion-body {
            display: block;
        }

        .accordion-content {
            padding: 20px;
        }

        .overview-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }

        .overview-stat {
            background: var(--bg-elevated);
            padding: 16px;
            border-radius: var(--radius-md);
        }

        .overview-stat-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }

        .overview-stat-label {
            font-size: 0.8em;
            color: var(--text-muted);
            text-transform: uppercase;
        }

        .overview-stat-value {
            font-size: 1.5em;
            font-weight: 700;
            color: var(--text-primary);
        }

        .overview-stat-bar {
            height: 6px;
            background: var(--bg-main);
            border-radius: 3px;
            overflow: hidden;
            margin-top: 8px;
        }

        .overview-stat-fill {
            height: 100%;
            border-radius: 3px;
            transition: width 0.5s ease;
        }

        .overview-stat-fill.good { background: var(--good); }
        .overview-stat-fill.warning { background: var(--warning); }
        .overview-stat-fill.critical { background: var(--critical); }

        .overview-stat-hint {
            font-size: 0.75em;
            color: var(--text-muted);
            margin-top: 8px;
        }

        .chart-container {
            background: var(--bg-elevated);
            padding: 20px;
            border-radius: var(--radius-md);
            margin-bottom: 16px;
        }

        .chart-title {
            font-size: 0.9em;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 16px;
        }

        .column-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .column-row {
            background: var(--bg-elevated);
            border-radius: var(--radius-md);
            overflow: hidden;
        }

        .column-row-header {
            display: grid;
            grid-template-columns: auto auto 1fr auto auto auto;
            align-items: center;
            gap: 12px;
            padding: 12px 16px;
            cursor: pointer;
            transition: background var(--transition-fast);
        }

        @media (min-width: 1200px) {
            .column-row-header {
                grid-template-columns: auto auto minmax(200px, 1fr) minmax(150px, auto) minmax(200px, auto) auto;
            }
        }

        @media (max-width: 768px) {
            .column-row-header {
                grid-template-columns: auto auto 1fr auto;
                grid-template-rows: auto auto auto;
                padding: 12px;
            }
            .column-quick-stats {
                grid-column: 3 / -1;
                grid-row: 2;
                justify-self: start;
            }
            .column-tags {
                grid-column: 1 / -1;
                grid-row: 3;
                margin-top: 8px;
                padding-top: 8px;
                border-top: 1px solid var(--border-subtle);
            }
            .column-tag {
                font-size: 0.7em;
                padding: 4px 10px;
            }
            .column-tag.fibo {
                font-size: 0.65em;
                max-width: 100%;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }
        }

        .column-row-header:hover {
            background: var(--bg-hover);
        }

        .column-expand-icon {
            color: var(--text-muted);
            font-size: 0.7em;
            transition: transform var(--transition-fast);
        }

        .column-row.expanded .column-expand-icon {
            transform: rotate(90deg);
        }

        .column-type-icon {
            width: 32px;
            height: 32px;
            border-radius: var(--radius-sm);
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.9em;
        }

        .column-type-icon.string { background: rgba(139, 92, 246, 0.2); }
        .column-type-icon.number { background: rgba(59, 130, 246, 0.2); }
        .column-type-icon.date { background: rgba(245, 158, 11, 0.2); }

        .column-info {
            flex: 1;
            min-width: 0;
        }

        .column-name {
            font-weight: 600;
            color: var(--text-primary);
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .column-type {
            font-size: 0.75em;
            color: var(--text-muted);
        }

        .column-quick-stats {
            display: flex;
            gap: 16px;
            font-size: 0.75em;
            color: var(--text-secondary);
        }

        .column-tags {
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
        }

        .column-tag {
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.65em;
            font-weight: 600;
            text-transform: uppercase;
        }

        .column-tag.pii { background: var(--critical-soft); color: var(--critical); }
        .column-tag.temporal { background: var(--warning-soft); color: var(--warning); }
        .column-tag.semantic { background: var(--info-soft); color: var(--info); }
        .column-tag.sparse { background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
        .column-tag.incomplete { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
        .column-tag.duplicate-risk { background: rgba(168, 85, 247, 0.15); color: #c084fc; }
        .column-tag.fibo {
            background: linear-gradient(135deg, rgba(139, 92, 246, 0.15) 0%, rgba(59, 130, 246, 0.15) 100%);
            color: #7c3aed;
            border: 1px solid rgba(139, 92, 246, 0.3);
        }
        .column-tag.fibo .fibo-icon { margin-right: 4px; }
        .column-tag.fibo-domain {
            background: rgba(59, 130, 246, 0.1);
            color: #3b82f6;
            font-size: 0.6em;
        }

        .column-quality-score {
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.75em;
            font-weight: 600;
        }

        .column-quality-score.good { background: var(--good-soft); color: var(--good); }
        .column-quality-score.warning { background: var(--warning-soft); color: var(--warning); }
        .column-quality-score.critical { background: var(--critical-soft); color: var(--critical); }

        .column-details {
            display: none;
            border-top: 1px solid var(--border-subtle);
        }

        .column-row.expanded .column-details {
            display: block;
        }

        .column-details-content {
            padding: 16px;
        }

        .column-stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 12px;
        }

        .column-stat {
            background: var(--bg-main);
            padding: 10px;
            border-radius: var(--radius-sm);
        }

        .column-stat-label {
            font-size: 0.7em;
            color: var(--text-muted);
            text-transform: uppercase;
        }

        .column-stat-value {
            font-size: 0.9em;
            color: var(--text-primary);
            font-weight: 500;
        }

        .top-values-section {
            margin-top: 12px;
            padding-top: 12px;
            border-top: 1px solid var(--border-subtle);
        }

        .top-values-title {
            font-size: 0.75em;
            color: var(--text-muted);
            text-transform: uppercase;
            margin-bottom: 8px;
        }

        .top-values-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
            gap: 6px;
        }

        .top-value-item {
            background: var(--bg-main);
            padding: 6px 10px;
            border-radius: var(--radius-sm);
            display: flex;
            justify-content: space-between;
            font-size: 0.8em;
        }

        .top-value-name {
            color: var(--text-secondary);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: 100px;
        }

        .top-value-count {
            color: var(--text-muted);
        }

        .sampled-note {
            font-size: 0.7em;
            color: var(--text-muted);
            font-style: italic;
            margin-top: 6px;
        }

        .suggestion-card {
            background: var(--bg-elevated);
            border-radius: var(--radius-md);
            padding: 16px;
            margin-bottom: 12px;
            border-left: 3px solid var(--info);
        }

        .suggestion-card.high-priority {
            border-left-color: #8b5cf6;
        }

        .suggestion-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 8px;
        }

        .suggestion-type {
            font-weight: 600;
            color: var(--text-primary);
        }

        .suggestion-confidence {
            font-size: 0.75em;
            color: var(--text-muted);
        }

        .suggestion-reason {
            font-size: 0.85em;
            color: var(--text-secondary);
            margin-bottom: 12px;
        }

        .suggestion-field {
            font-size: 0.8em;
            color: var(--text-muted);
            margin-bottom: 6px;
            padding: 4px 8px;
            background: var(--bg-card);
            border-radius: var(--radius-sm);
            display: inline-block;
        }

        .suggestion-field strong {
            color: var(--accent);
        }

        .suggestion-yaml-container {
            display: flex;
            gap: 8px;
            align-items: flex-start;
            margin-top: 8px;
        }

        .suggestion-yaml {
            flex: 1;
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-sm);
            padding: 10px 12px;
            font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
            font-size: 0.75em;
            color: var(--text-secondary);
            margin: 0;
            white-space: pre-wrap;
            word-break: break-word;
            overflow-x: auto;
        }

        .copy-yaml-btn {
            background: var(--accent-soft);
            color: var(--accent);
            border: 1px solid rgba(96, 165, 250, 0.3);
            padding: 6px 12px;
            border-radius: var(--radius-sm);
            font-size: 0.75em;
            cursor: pointer;
            transition: all var(--transition-fast);
        }

        .copy-yaml-btn:hover {
            background: var(--accent);
            color: #fff;
        }

        /* Full Config Section */
        .full-config-container {
            margin-top: 12px;
        }

        .config-actions {
            margin-bottom: 10px;
        }

        .copy-config-btn {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: #fff;
            border: none;
            padding: 10px 20px;
            border-radius: var(--radius-md);
            font-size: 0.85em;
            font-weight: 600;
            cursor: pointer;
            transition: all var(--transition-fast);
        }

        .copy-config-btn:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(16, 185, 129, 0.3);
        }

        .full-config-yaml {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: var(--radius-md);
            padding: 16px;
            font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
            font-size: 0.8em;
            color: var(--text-secondary);
            margin: 0;
            white-space: pre;
            overflow-x: auto;
            max-height: 500px;
            overflow-y: auto;
        }

        .pii-risk-card {
            background: var(--critical-soft);
            border: 1px solid rgba(239, 68, 68, 0.3);
            border-radius: var(--radius-md);
            padding: 16px;
            margin-bottom: 16px;
        }

        .pii-risk-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .pii-risk-score {
            font-size: 1.5em;
            font-weight: 700;
            color: var(--critical);
        }

        .pii-risk-level {
            font-size: 0.85em;
            color: var(--critical);
        }

        .pii-column-item {
            background: var(--bg-elevated);
            padding: 12px;
            border-radius: var(--radius-sm);
            margin-top: 12px;
            display: flex;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }

        .pii-column-name {
            font-weight: 600;
            color: var(--text-primary);
        }

        .pii-type-badge {
            background: var(--critical-soft);
            color: var(--critical);
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 0.7em;
            font-weight: 600;
        }

        .pii-frameworks {
            display: flex;
            gap: 6px;
        }

        .pii-framework-tag {
            background: var(--warning-soft);
            color: var(--warning);
            padding: 2px 6px;
            border-radius: 6px;
            font-size: 0.65em;
            font-weight: 600;
        }

        /* ML Analysis Section Styles */
        .beta-badge {
            background: linear-gradient(135deg, #8b5cf6 0%, #6366f1 100%);
            color: white;
            font-size: 0.6em;
            padding: 2px 6px;
            border-radius: 4px;
            margin-left: 8px;
            font-weight: 600;
            vertical-align: middle;
        }

        .accordion-icon.ml {
            background: linear-gradient(135deg, rgba(139, 92, 246, 0.1) 0%, rgba(99, 102, 241, 0.1) 100%);
            color: #8b5cf6;
        }

        .ml-summary-card {
            background: linear-gradient(135deg, rgba(139, 92, 246, 0.1) 0%, rgba(99, 102, 241, 0.05) 100%);
            border: 1px solid rgba(139, 92, 246, 0.2);
            border-radius: var(--radius-md);
            padding: 20px;
            margin-bottom: 20px;
        }

        .ml-summary-header {
            text-align: center;
            margin-bottom: 16px;
        }

        .ml-summary-count {
            font-size: 1.6em;
            font-weight: 700;
            color: #8b5cf6;
            line-height: 1;
        }

        .ml-summary-label {
            font-size: 0.9em;
            color: var(--text-secondary);
            margin-top: 4px;
        }

        .ml-findings-list {
            border-top: 1px solid rgba(139, 92, 246, 0.15);
            padding-top: 12px;
        }

        .ml-finding-item {
            padding: 8px 0;
            color: var(--text-secondary);
            font-size: 0.9em;
            border-bottom: 1px solid rgba(139, 92, 246, 0.08);
        }

        .ml-finding-item:last-child {
            border-bottom: none;
        }

        .ml-detail-section {
            background: var(--bg-elevated);
            border-radius: var(--radius-md);
            padding: 16px;
            margin-bottom: 16px;
        }

        .ml-detail-header {
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 12px;
            font-size: 1em;
        }

        .ml-hint {
            background: var(--bg-tertiary);
            border-left: 3px solid #0ea5e9;
            padding: 10px 14px;
            margin-bottom: 12px;
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            font-size: 0.9em;
            color: var(--text-secondary);
            line-height: 1.5;
        }

        .ml-hint em {
            font-style: normal;
            font-weight: 600;
            color: #38bdf8;
        }

        /* Collapsible ML hints */
        .ml-hint-collapse {
            margin-bottom: 14px;
        }

        .ml-hint-collapse summary {
            cursor: pointer;
            color: var(--text-secondary);
            font-size: 0.85em;
            padding: 8px 0;
            list-style: none;
        }

        .ml-hint-collapse summary::-webkit-details-marker {
            display: none;
        }

        .ml-hint-collapse summary::before {
            content: '▶ ';
            font-size: 0.7em;
            margin-right: 4px;
            color: var(--text-muted);
        }

        .ml-hint-collapse[open] summary::before {
            content: '▼ ';
        }

        .ml-hint-content {
            background: var(--bg-tertiary);
            border-left: 3px solid #0ea5e9;
            padding: 10px 14px;
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            margin-top: 8px;
            font-size: 0.85em;
            color: var(--text-secondary);
            line-height: 1.5;
        }

        .ml-detail-item {
            display: grid;
            grid-template-columns: 150px 100px 1fr;
            gap: 12px;
            padding: 10px;
            background: var(--bg-main);
            border-radius: var(--radius-sm);
            margin-bottom: 8px;
            align-items: center;
        }

        .ml-detail-col {
            color: var(--text-primary);
        }

        .ml-detail-count {
            color: #8b5cf6;
            font-weight: 600;
            font-size: 0.85em;
        }

        .ml-detail-desc {
            color: var(--text-secondary);
            font-size: 0.85em;
        }

        @media (max-width: 768px) {
            .ml-detail-item {
                grid-template-columns: 1fr;
                gap: 6px;
            }
        }

        /* Mobile-friendly sample rows */
        .sample-rows-container {
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
            max-width: 100%;
        }
        .sample-row-card {
            background: var(--bg-elevated);
            border-radius: 6px;
            padding: 8px 12px;
            margin-bottom: 6px;
            font-size: 11px;
        }
        .sample-row-card div {
            display: flex;
            justify-content: space-between;
            padding: 2px 0;
            border-bottom: 1px solid var(--border);
        }
        .sample-row-card div:last-child {
            border-bottom: none;
        }
        .sample-row-card .field-name {
            color: var(--text-secondary);
            font-weight: 500;
        }
        .sample-row-card .field-value {
            color: var(--text-primary);
            text-align: right;
            word-break: break-all;
        }

        .ml-charts-row {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }

        .ml-chart-card {
            background: var(--bg-elevated);
            border-radius: var(--radius-md);
            padding: 16px;
        }

        .ml-chart-title {
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 12px;
            font-size: 0.9em;
        }

        .ml-chart-container {
            position: relative;
            height: 200px;
        }

        .freshness-content {
            padding: 0;
        }

        .freshness-timeline {
            display: flex;
            flex-direction: column;
            gap: 12px;
        }

        .freshness-item {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .freshness-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
        }

        .freshness-dot.good { background: var(--good); box-shadow: 0 0 8px var(--good); }
        .freshness-dot.warning { background: var(--warning); box-shadow: 0 0 8px var(--warning); }

        .freshness-info {
            flex: 1;
        }

        .freshness-label {
            font-size: 0.75em;
            color: var(--text-muted);
            text-transform: uppercase;
        }

        .freshness-value {
            font-size: 0.9em;
            color: var(--text-primary);
        }

        .column-search {
            margin-bottom: 12px;
        }

        .column-search input {
            width: 100%;
            padding: 10px 14px;
            background: var(--bg-main);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-md);
            color: var(--text-primary);
            font-size: 0.9em;
        }

        .column-search input:focus {
            outline: none;
            border-color: var(--accent);
        }

        /* ======================================================
           STICKY NAVIGATION
           ====================================================== */
        .sticky-nav {
            position: sticky;
            top: 60px;
            z-index: 99;
            background: var(--bg-main);
            border-bottom: 1px solid var(--border-subtle);
            padding: 0;
            margin: 0 -32px;
            padding: 0 32px;
        }

        .sticky-nav-inner {
            display: flex;
            gap: 8px;
            padding: 12px 0;
            overflow-x: auto;
            scrollbar-width: none;
            -ms-overflow-style: none;
        }

        .sticky-nav-inner::-webkit-scrollbar {
            display: none;
        }

        .nav-btn {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            color: var(--text-secondary);
            padding: 8px 16px;
            border-radius: var(--radius-md);
            font-size: 0.85em;
            font-weight: 500;
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.2s ease;
        }

        .nav-btn:hover {
            background: var(--bg-hover);
            border-color: var(--accent);
            color: var(--text-primary);
        }

        .nav-btn.active {
            background: var(--accent);
            border-color: var(--accent);
            color: white;
        }

        .nav-spacer {
            flex: 1;
        }

        .expand-all-btn {
            background: transparent;
            border: 1px solid var(--border-subtle);
            color: var(--text-muted);
            padding: 6px 12px;
            border-radius: var(--radius-md);
            font-size: 0.75em;
            font-weight: 400;
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.2s ease;
            opacity: 0.7;
            margin-left: auto;
        }

        .expand-all-btn:hover {
            opacity: 1;
            border-color: var(--accent);
            color: var(--text-secondary);
        }

        .expand-all-btn.expanded {
            background: rgba(74, 144, 226, 0.1);
            border-color: var(--accent);
            color: var(--accent);
        }

        .expand-all-btn .expand-icon {
            font-weight: 600;
            margin-right: 4px;
        }

        /* ======================================================
           EXECUTIVE SUMMARY
           ====================================================== */
        .executive-summary {
            background: linear-gradient(135deg, var(--bg-card) 0%, rgba(74, 144, 226, 0.08) 100%);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 24px;
            margin-bottom: 24px;
            display: grid;
            grid-template-columns: 1fr auto;
            grid-template-rows: auto auto;
            gap: 20px;
        }

        .summary-verdict {
            grid-column: 1 / -1;
            padding: 16px 20px;
            border-radius: var(--radius-md);
            border-left: 4px solid;
        }

        .summary-verdict.good {
            background: rgba(16, 185, 129, 0.1);
            border-left-color: var(--good);
        }

        .summary-verdict.warning {
            background: rgba(251, 191, 36, 0.1);
            border-left-color: var(--warning);
        }

        .summary-verdict.critical {
            background: rgba(239, 68, 68, 0.1);
            border-left-color: var(--critical);
        }

        .verdict-text {
            font-size: 1.25em;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 4px;
        }

        .verdict-detail {
            font-size: 0.9em;
            color: var(--text-secondary);
        }

        .summary-stats {
            display: flex;
            gap: 24px;
        }

        .stat-item {
            text-align: center;
        }

        .stat-value {
            font-size: 1.5em;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.2;
        }

        .stat-label {
            font-size: 0.75em;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .priority-actions {
            grid-column: 1;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .actions-header {
            font-size: 0.8em;
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }

        .action-item {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 14px;
            background: var(--bg-main);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-md);
            color: var(--text-secondary);
            text-decoration: none;
            font-size: 0.9em;
            transition: all 0.2s ease;
        }

        .action-item:hover {
            background: var(--bg-hover);
            border-color: var(--accent);
            color: var(--text-primary);
        }

        .action-icon {
            font-size: 1.1em;
        }

        .action-text {
            flex: 1;
        }

        .action-arrow {
            color: var(--accent);
            font-weight: bold;
        }

        .summary-tools {
            display: flex;
            align-items: flex-end;
        }

        .export-btn {
            background: var(--accent);
            color: white;
            border: none;
            padding: 10px 18px;
            border-radius: var(--radius-md);
            font-size: 0.9em;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s ease;
            white-space: nowrap;
        }

        .export-btn:hover {
            background: var(--accent-hover);
            transform: translateY(-1px);
        }

        /* ======================================================
           COLUMN QUALITY HEATMAP
           ====================================================== */
        .column-heatmap {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-lg);
            padding: 20px;
            margin-bottom: 20px;
        }

        .heatmap-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 16px;
            flex-wrap: wrap;
            gap: 12px;
        }

        .heatmap-title {
            font-size: 1em;
            font-weight: 600;
            color: var(--text-primary);
        }

        .heatmap-legend {
            display: flex;
            gap: 16px;
            font-size: 0.8em;
            color: var(--text-muted);
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 4px;
        }

        .legend-dot {
            width: 12px;
            height: 12px;
            border-radius: 3px;
        }

        .heatmap-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(90px, 1fr));
            gap: 8px;
        }

        .heatmap-cell {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 10px 6px;
            border-radius: var(--radius-sm);
            cursor: pointer;
            transition: transform 0.15s ease, box-shadow 0.15s ease;
        }

        .heatmap-cell:hover {
            transform: scale(1.05);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }

        .heatmap-name {
            font-size: 0.75em;
            color: rgba(255,255,255,0.9);
            text-align: center;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: 100%;
        }

        .heatmap-score {
            font-size: 1.1em;
            font-weight: 700;
            color: white;
        }

        .heatmap-good, .legend-dot.heatmap-good { background: linear-gradient(135deg, #10b981, #059669); }
        .heatmap-ok, .legend-dot.heatmap-ok { background: linear-gradient(135deg, #3b82f6, #2563eb); }
        .heatmap-warning, .legend-dot.heatmap-warning { background: linear-gradient(135deg, #f59e0b, #d97706); }
        .heatmap-critical, .legend-dot.heatmap-critical { background: linear-gradient(135deg, #ef4444, #dc2626); }

        /* ======================================================
           COLUMN SEARCH AND FILTER CONTROLS
           ====================================================== */
        .column-controls {
            display: flex;
            gap: 16px;
            margin-bottom: 16px;
            flex-wrap: wrap;
            align-items: center;
        }

        .search-box {
            position: relative;
            flex: 1;
            min-width: 200px;
        }

        .search-input {
            width: 100%;
            padding: 10px 14px 10px 36px;
            background: var(--bg-main);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-md);
            color: var(--text-primary);
            font-size: 0.9em;
        }

        .search-input:focus {
            outline: none;
            border-color: var(--accent);
        }

        .search-icon {
            position: absolute;
            left: 12px;
            top: 50%;
            transform: translateY(-50%);
            color: var(--text-muted);
            font-size: 0.9em;
        }

        .filter-buttons {
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
        }

        .filter-btn {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            color: var(--text-secondary);
            padding: 8px 12px;
            border-radius: var(--radius-sm);
            font-size: 0.8em;
            cursor: pointer;
            transition: all 0.2s ease;
            white-space: nowrap;
        }

        .filter-btn:hover {
            background: var(--bg-hover);
            border-color: var(--accent);
        }

        .filter-btn.active {
            background: var(--accent);
            border-color: var(--accent);
            color: white;
        }

        .sort-controls {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .sort-select {
            padding: 8px 12px;
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: var(--radius-sm);
            color: var(--text-primary);
            font-size: 0.85em;
            cursor: pointer;
        }

        /* ======================================================
           MOBILE RESPONSIVE STYLES
           ====================================================== */
        @media (max-width: 768px) {
            .page { padding: 16px; }
            .top-inner { padding: 12px 16px; }
            .file-meta { display: none; }
            .column-quick-stats { display: none; }
            .column-tags { display: none; }

            /* Sticky nav adjustments */
            .sticky-nav {
                margin: 0 -16px;
                padding: 0 16px;
                top: 56px;
            }

            .nav-btn {
                padding: 6px 12px;
                font-size: 0.8em;
            }

            /* Executive summary mobile */
            .executive-summary {
                display: flex;
                flex-direction: column;
                padding: 16px;
            }

            .summary-stats {
                flex-wrap: wrap;
                justify-content: space-around;
                gap: 16px;
            }

            .stat-value {
                font-size: 1.25em;
            }

            .priority-actions {
                width: 100%;
            }

            .summary-tools {
                width: 100%;
                justify-content: center;
            }

            .export-btn {
                width: 100%;
            }

            /* Heatmap mobile */
            .heatmap-header {
                flex-direction: column;
                align-items: flex-start;
            }

            .heatmap-grid {
                grid-template-columns: repeat(auto-fill, minmax(70px, 1fr));
            }

            .heatmap-cell {
                padding: 8px 4px;
            }

            .heatmap-name {
                font-size: 0.65em;
            }

            .heatmap-score {
                font-size: 0.95em;
            }

            /* Column controls mobile */
            .column-controls {
                flex-direction: column;
                align-items: stretch;
            }

            .search-box {
                width: 100%;
            }

            .filter-buttons {
                justify-content: flex-start;
            }

            .sort-controls {
                width: 100%;
            }

            .sort-select {
                width: 100%;
            }
        }

        /* Tablet adjustments */
        @media (min-width: 769px) and (max-width: 1024px) {
            .executive-summary {
                grid-template-columns: 1fr;
            }

            .summary-tools {
                justify-content: flex-start;
            }
        }

        /* ======================================================
           v2 SAMPLING COVERAGE BANNER
           ====================================================== */
        .sampling-banner {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border: 1px solid rgba(148, 163, 184, 0.15);
            border-radius: 16px;
            padding: 20px 24px;
            margin-bottom: 24px;
        }

        .sampling-banner-header {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 16px;
        }

        .sampling-banner-icon {
            font-size: 1.5em;
        }

        .sampling-banner-title {
            font-size: 1em;
            font-weight: 600;
            color: var(--text-primary);
        }

        .sampling-progress-container {
            margin-bottom: 16px;
        }

        .sampling-progress-bar {
            height: 10px;
            background: rgba(30, 41, 59, 0.8);
            border-radius: 5px;
            overflow: hidden;
            margin-bottom: 8px;
        }

        .sampling-progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6 0%, #8b5cf6 100%);
            border-radius: 5px;
            transition: width 0.5s ease;
        }

        .sampling-progress-label {
            font-size: 0.9em;
            color: var(--text-secondary);
            text-align: center;
        }

        .sampling-progress-label strong {
            color: var(--accent);
        }

        .sampling-methods-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
        }

        .sampling-method {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 14px;
            background: rgba(15, 23, 42, 0.6);
            border-radius: 8px;
            font-size: 0.85em;
        }

        .sampling-method-icon {
            font-size: 1.1em;
        }

        .sampling-method.full .sampling-method-icon { color: #10b981; }
        .sampling-method.sampled .sampling-method-icon { color: #f59e0b; }

        .sampling-method-text {
            color: var(--text-secondary);
        }

        .sampling-method-text strong {
            color: var(--text-primary);
            font-weight: 500;
        }

        /* Sample Size Explanation (Collapsible) */
        .sample-size-explanation {
            margin-top: 16px;
            background: rgba(15, 23, 42, 0.4);
            border-radius: 8px;
            border: 1px solid rgba(148,163,184,0.05);
        }

        .sample-size-summary {
            cursor: pointer;
            padding: 12px 16px;
            font-size: 0.85em;
            color: var(--accent);
            display: flex;
            align-items: center;
            gap: 8px;
            list-style: none;
        }

        .sample-size-summary::-webkit-details-marker { display: none; }

        .sample-size-summary::after {
            content: '▸';
            margin-left: auto;
            transition: transform 0.2s;
        }

        .sample-size-explanation[open] .sample-size-summary::after {
            transform: rotate(90deg);
        }

        .sample-size-content {
            padding: 0 16px 16px;
            color: var(--text-secondary);
            font-size: 0.85em;
            line-height: 1.6;
        }

        .sample-size-content p {
            margin: 0 0 12px 0;
        }

        .sample-size-stats {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
            margin: 16px 0;
        }

        .sample-stat {
            text-align: center;
            padding: 12px;
            background: rgba(15, 23, 42, 0.6);
            border-radius: 8px;
        }

        .sample-stat-value {
            display: block;
            font-size: 1.4em;
            font-weight: 700;
            color: var(--accent);
        }

        .sample-stat-label {
            font-size: 0.75em;
            color: var(--text-muted);
            margin-top: 4px;
            display: block;
        }

        .sample-size-table {
            width: 100%;
            border-collapse: collapse;
            margin: 12px 0;
            font-size: 0.9em;
        }

        .sample-size-table th, .sample-size-table td {
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid rgba(148,163,184,0.08);
        }

        .sample-size-table th {
            color: var(--text-muted);
            font-weight: 500;
            font-size: 0.85em;
        }

        .sample-size-table .value-highlight {
            color: #10b981;
        }

        .sample-size-note {
            font-style: italic;
            color: var(--text-muted);
            border-left: 2px solid var(--accent);
            padding-left: 12px;
            margin: 16px 0 0 0;
        }

        @media (max-width: 640px) {
            .sample-size-stats {
                grid-template-columns: 1fr;
            }
        }

        /* FIBO Semantic Categories Summary */
        .fibo-summary {
            background: linear-gradient(135deg, rgba(139, 92, 246, 0.08) 0%, rgba(59, 130, 246, 0.05) 100%);
            border: 1px solid rgba(139, 92, 246, 0.15);
            border-radius: 12px;
            padding: 16px 20px;
            margin-bottom: 20px;
        }

        .fibo-summary-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 14px;
        }

        .fibo-summary-icon {
            font-size: 1.3em;
        }

        .fibo-summary-title {
            font-weight: 600;
            color: var(--text-primary);
            font-size: 0.95em;
        }

        .fibo-summary-count {
            margin-left: auto;
            font-size: 0.8em;
            color: var(--text-muted);
            background: rgba(139, 92, 246, 0.15);
            padding: 4px 10px;
            border-radius: 12px;
        }

        .fibo-categories-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }

        .fibo-category-chip {
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(15, 23, 42, 0.6);
            padding: 8px 14px;
            border-radius: 20px;
            border: 1px solid rgba(148,163,184,0.08);
            transition: all 0.2s;
        }

        .fibo-category-chip:hover {
            background: rgba(15, 23, 42, 0.8);
            border-color: rgba(139, 92, 246, 0.3);
        }

        .fibo-icon {
            font-size: 1em;
        }

        .fibo-label {
            font-weight: 600;
            color: var(--text-primary);
            font-size: 0.85em;
        }

        .fibo-count {
            background: var(--accent);
            color: white;
            font-size: 0.7em;
            font-weight: 700;
            padding: 2px 7px;
            border-radius: 10px;
            min-width: 18px;
            text-align: center;
        }

        .fibo-tags {
            font-size: 0.75em;
            color: var(--text-muted);
            max-width: 150px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        @media (max-width: 640px) {
            .fibo-categories-grid {
                flex-direction: column;
            }
            .fibo-tags {
                display: none;
            }
        }

        /* FIBO Badge - Inline with column name (for mobile visibility) */
        .column-name-row {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-wrap: wrap;
        }

        .fibo-badge-mobile {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            background: rgba(139, 92, 246, 0.12);
            color: var(--text-secondary);
            font-size: 0.7em;
            padding: 2px 8px;
            border-radius: 12px;
            border: 1px solid rgba(139, 92, 246, 0.2);
            white-space: nowrap;
        }

        @media (min-width: 1024px) {
            /* Hide mobile badge on desktop - shown in expanded details instead */
            .fibo-badge-mobile {
                display: none;
            }
        }

        @media (max-width: 640px) {
            .fibo-badge-mobile {
                font-size: 0.65em;
                padding: 2px 6px;
            }
        }

        /* ======================================================
           v2 METRICS DASHBOARD GRID
           ====================================================== */
        .metrics-dashboard {
            display: flex;
            flex-direction: column;
            gap: 16px;
            margin-bottom: 24px;
        }

        .metrics-row {
            display: grid;
            gap: 12px;
        }

        .metrics-row.core {
            grid-template-columns: repeat(4, 1fr);
        }

        .metrics-row.quality {
            grid-template-columns: repeat(4, 1fr);
        }

        .metrics-row.types {
            grid-template-columns: repeat(4, 1fr);
        }

        @media (max-width: 768px) {
            .metrics-row.core,
            .metrics-row.quality,
            .metrics-row.types {
                grid-template-columns: repeat(2, 1fr);
            }
        }

        .metric-widget {
            background: var(--bg-card);
            border: 1px solid var(--border-subtle);
            border-radius: 12px;
            padding: 16px;
            text-align: center;
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }

        .metric-widget:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
        }

        .metric-widget-label {
            font-size: 0.7em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 6px;
        }

        .metric-widget-value {
            font-size: 1.6em;
            font-weight: 700;
            color: var(--text-primary);
            line-height: 1.2;
        }

        .metric-widget-trend {
            font-size: 0.75em;
            margin-top: 6px;
            color: var(--text-secondary);
        }

        .metric-widget-trend.good { color: var(--good); }
        .metric-widget-trend.warning { color: var(--warning); }
        .metric-widget-trend.critical { color: var(--critical); }

        /* Quality gauge styling */
        .metric-gauge {
            position: relative;
            width: 60px;
            height: 60px;
            margin: 0 auto 8px auto;
        }

        .metric-gauge svg {
            transform: rotate(-90deg);
        }

        .metric-gauge-value {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 0.9em;
            font-weight: 700;
            color: var(--text-primary);
        }

        /* ======================================================
           v2 INSIGHT WIDGET (Masterpiece Design)
           ====================================================== */
        .insight-widget {
            background: linear-gradient(180deg, #0f172a 0%, #0a0f1a 100%);
            border: 1px solid rgba(148, 163, 184, 0.08);
            border-radius: 16px;
            padding: 0;
            margin-bottom: 20px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
            overflow: hidden;
        }

        .insight-widget-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 18px 24px;
            border-bottom: 1px solid rgba(148, 163, 184, 0.1);
            background: rgba(15, 23, 42, 0.5);
        }

        .insight-widget-title-group {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .insight-widget-icon {
            font-size: 1.3em;
        }

        .insight-widget-title {
            font-size: 1.05em;
            font-weight: 600;
            color: var(--text-primary);
        }

        .insight-widget-badge {
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.75em;
            font-weight: 600;
            background: rgba(139, 92, 246, 0.15);
            color: #a78bfa;
        }

        .insight-widget-badge.critical { background: rgba(239, 68, 68, 0.15); color: #f87171; }
        .insight-widget-badge.warning { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
        .insight-widget-badge.info { background: rgba(59, 130, 246, 0.15); color: #60a5fa; }
        .insight-widget-badge.good { background: rgba(16, 185, 129, 0.15); color: #34d399; }

        /* ML Model Badge */
        .insight-ml-badge {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.7em;
            font-weight: 500;
            background: linear-gradient(135deg, rgba(139, 92, 246, 0.2), rgba(59, 130, 246, 0.2));
            color: #a78bfa;
            border: 1px solid rgba(139, 92, 246, 0.3);
            margin-left: 8px;
        }

        .insight-widget-body {
            padding: 24px;
        }

        /* Plain English Summary Section */
        .insight-summary {
            margin-bottom: 24px;
        }

        .insight-summary-label {
            font-size: 0.75em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 8px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .insight-summary-label::before {
            content: '💬';
            font-size: 1.2em;
        }

        .insight-summary-text {
            font-size: 1em;
            line-height: 1.7;
            color: var(--text-secondary);
            padding: 16px;
            background: rgba(0, 0, 0, 0.2);
            border-radius: 10px;
            border-left: 3px solid var(--accent);
        }

        /* Example Table Section */
        .insight-examples {
            margin-bottom: 20px;
        }

        .insight-examples-label {
            font-size: 0.75em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 12px;
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .insight-examples-label::before {
            content: '📋';
            font-size: 1.2em;
        }

        .insight-examples-table {
            width: 100%;
            background: rgba(0, 0, 0, 0.3);
            border-radius: 10px;
            overflow: hidden;
            font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
            font-size: 0.85em;
        }

        .insight-examples-table thead {
            background: rgba(15, 23, 42, 0.8);
        }

        .insight-examples-table th {
            padding: 12px 16px;
            text-align: left;
            font-weight: 600;
            color: var(--text-primary);
            border-bottom: 1px solid rgba(148, 163, 184, 0.1);
        }

        .insight-examples-table td {
            padding: 10px 16px;
            color: var(--text-secondary);
            border-bottom: 1px solid rgba(148, 163, 184, 0.05);
        }

        .insight-examples-table tr:last-child td {
            border-bottom: none;
        }

        .insight-examples-table tr:hover {
            background: rgba(96, 165, 250, 0.05);
        }

        .insight-examples-table .value-highlight {
            color: #f87171;
            font-weight: 600;
        }

        .insight-examples-table .value-normal {
            color: #94a3b8;
        }

        .insight-examples-table .row-id {
            color: var(--text-muted);
            font-size: 0.9em;
        }

        /* Technical Details Collapsible */
        .insight-technical {
            border-top: 1px solid rgba(148, 163, 184, 0.1);
            margin-top: 4px;
        }

        .insight-technical-toggle {
            display: flex;
            align-items: center;
            gap: 8px;
            width: 100%;
            padding: 14px 0;
            background: transparent;
            border: none;
            cursor: pointer;
            color: var(--text-secondary);
            font-size: 0.85em;
            transition: color 0.2s ease;
        }

        .insight-technical-toggle:hover {
            color: var(--accent);
        }

        .insight-technical-toggle::before {
            content: '🔬';
            font-size: 1.1em;
        }

        .insight-technical-toggle .toggle-arrow {
            margin-left: auto;
            transition: transform 0.2s ease;
        }

        .insight-technical.open .toggle-arrow {
            transform: rotate(180deg);
        }

        .insight-technical-content {
            display: none;
            padding: 16px;
            background: rgba(15, 23, 42, 0.6);
            border-radius: 10px;
            margin-bottom: 8px;
        }

        .insight-technical.open .insight-technical-content {
            display: block;
        }

        .insight-technical-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
        }

        .insight-technical-item {
            display: flex;
            flex-direction: column;
            gap: 4px;
        }

        .insight-technical-item-label {
            font-size: 0.7em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
        }

        .insight-technical-item-value {
            font-size: 0.9em;
            color: var(--text-secondary);
            font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
        }

        .insight-technical-context {
            margin-top: 16px;
            padding-top: 16px;
            border-top: 1px solid rgba(148, 163, 184, 0.08);
        }

        .insight-technical-context-title {
            font-size: 0.75em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--text-muted);
            margin-bottom: 10px;
        }

        .insight-technical-context-list {
            list-style: none;
            font-size: 0.85em;
            color: var(--text-secondary);
        }

        .insight-technical-context-list li {
            padding: 4px 0;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .insight-technical-context-list li::before {
            content: '•';
            color: var(--accent);
        }

        /* ======================================================
           v2 SECTION HEADERS (Observation framing)
           ====================================================== */
        .section-header-v2 {
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border: 1px solid rgba(148, 163, 184, 0.1);
            border-radius: 12px;
            padding: 16px 20px;
            margin: 28px 0 16px 0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .section-header-v2-icon {
            font-size: 1.3em;
            margin-right: 12px;
        }

        .section-header-v2-title {
            font-size: 1.1em;
            font-weight: 600;
            color: var(--text-primary);
        }

        .section-header-v2-subtitle {
            font-size: 0.85em;
            color: var(--text-secondary);
            margin-top: 2px;
        }

        .section-header-v2-badge {
            padding: 6px 14px;
            border-radius: 8px;
            font-size: 0.75em;
            font-weight: 600;
            text-transform: uppercase;
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-secondary);
        }

        .section-header-v2-badge.observations { background: rgba(139, 92, 246, 0.15); color: #a78bfa; }
        .section-header-v2-badge.patterns { background: rgba(59, 130, 246, 0.15); color: #60a5fa; }
        .section-header-v2-badge.info { background: rgba(16, 185, 129, 0.15); color: #34d399; }'''

    def _get_javascript(self, profile: ProfileResult, type_counts: Dict[str, int], categorical_columns: List[Dict]) -> str:
        """Generate the JavaScript for charts and interactions."""

        # Prepare chart data
        column_names = [col.name[:10] for col in profile.columns]  # Truncate long names
        quality_scores = [col.quality.overall_score for col in profile.columns]

        # Type distribution data
        type_labels = [f"{t} ({c})" for t, c in type_counts.items()]
        type_data = list(type_counts.values())

        # Word cloud data from categorical columns
        word_cloud_data = []
        for cat_col in categorical_columns[:3]:  # Limit to top 3 categorical columns
            for tv in cat_col.get('top_values', [])[:10]:
                word_cloud_data.append([str(tv.get('value', '')), tv.get('count', 0)])

        # Bubble chart data - find the best metric for x-axis based on variation
        completeness_values = [col.quality.completeness for col in profile.columns]
        uniqueness_values = [col.quality.uniqueness for col in profile.columns]
        consistency_values = [col.quality.consistency for col in profile.columns]

        completeness_range = max(completeness_values) - min(completeness_values) if completeness_values else 0
        uniqueness_range = max(uniqueness_values) - min(uniqueness_values) if uniqueness_values else 0
        consistency_range = max(consistency_values) - min(consistency_values) if consistency_values else 0

        # Choose the metric with most variation for x-axis
        if completeness_range >= 5:
            x_axis_metric = 'completeness'
            x_axis_label = 'Completeness %'
        elif uniqueness_range >= 5:
            x_axis_metric = 'uniqueness'
            x_axis_label = 'Uniqueness %'
        elif consistency_range >= 5:
            x_axis_metric = 'consistency'
            x_axis_label = 'Consistency %'
        else:
            x_axis_metric = 'completeness'
            x_axis_label = 'Completeness %'

        bubble_data = []
        for col in profile.columns:
            x_val = getattr(col.quality, x_axis_metric)
            bubble_data.append({
                'x': x_val,
                'y': col.quality.validity,
                'r': min(max(5, col.statistics.unique_count / 1000), 15) if col.statistics.unique_count else 5,
                'label': col.name[:15]
            })

        return f'''
        // Accordion toggle
        function toggleAccordion(header) {{
            const accordion = header.closest('.accordion');
            accordion.classList.toggle('open');
        }}

        // Column row toggle
        function toggleColumnRow(row) {{
            row.classList.toggle('expanded');
        }}

        // ======================================================
        // EXPAND ALL TOGGLE
        // ======================================================
        let allExpanded = false;

        function toggleExpandAll() {{
            const btn = document.getElementById('expandAllBtn');
            allExpanded = !allExpanded;

            // Update button appearance
            btn.classList.toggle('expanded', allExpanded);
            btn.innerHTML = allExpanded
                ? '<span class="expand-icon">-</span> Collapse All'
                : '<span class="expand-icon">+</span> Expand All';

            // Toggle all accordions
            document.querySelectorAll('.accordion').forEach(acc => {{
                if (allExpanded) {{
                    acc.classList.add('open');
                }} else {{
                    acc.classList.remove('open');
                }}
            }});

            // Toggle all column rows
            document.querySelectorAll('.column-row').forEach(row => {{
                if (allExpanded) {{
                    row.classList.add('expanded');
                }} else {{
                    row.classList.remove('expanded');
                }}
            }});

            // Toggle all <details> elements
            document.querySelectorAll('details').forEach(details => {{
                details.open = allExpanded;
            }});

            // Toggle all technical sections
            document.querySelectorAll('.insight-technical').forEach(section => {{
                if (allExpanded) {{
                    section.classList.add('open');
                }} else {{
                    section.classList.remove('open');
                }}
            }});
        }}

        // ======================================================
        // STICKY NAVIGATION
        // ======================================================
        const navBtns = document.querySelectorAll('.nav-btn');
        const sections = {{
            'summary': document.getElementById('section-summary'),
            'risks': document.getElementById('section-risks'),
            'quality': document.getElementById('section-quality'),
            'columns': document.getElementById('section-columns'),
            'validations': document.getElementById('section-validations')
        }};

        navBtns.forEach(btn => {{
            btn.addEventListener('click', function() {{
                const sectionId = this.dataset.section;
                const section = sections[sectionId];
                if (section) {{
                    // Update active state
                    navBtns.forEach(b => b.classList.remove('active'));
                    this.classList.add('active');
                    // Smooth scroll to section
                    section.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
                }}
            }});
        }});

        // Update nav on scroll
        let scrollTimeout;
        window.addEventListener('scroll', function() {{
            clearTimeout(scrollTimeout);
            scrollTimeout = setTimeout(function() {{
                let currentSection = 'summary';
                const scrollPos = window.scrollY + 150;

                for (const [name, el] of Object.entries(sections)) {{
                    if (el && el.offsetTop <= scrollPos) {{
                        currentSection = name;
                    }}
                }}

                navBtns.forEach(btn => {{
                    btn.classList.toggle('active', btn.dataset.section === currentSection);
                }});
            }}, 50);
        }});

        // ======================================================
        // COLUMN SEARCH AND FILTER
        // ======================================================
        let currentFilter = 'all';
        let currentSort = 'name';

        function filterColumns() {{
            const query = document.getElementById('columnSearch')?.value?.toLowerCase() || '';
            const rows = document.querySelectorAll('.column-row');

            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                const type = row.dataset?.type || '';
                const hasIssues = row.dataset?.issues === 'true';

                // Apply text search
                let showBySearch = !query || text.includes(query);

                // Apply filter
                let showByFilter = true;
                switch(currentFilter) {{
                    case 'issues':
                        showByFilter = hasIssues;
                        break;
                    case 'numeric':
                        showByFilter = ['integer', 'float'].includes(type);
                        break;
                    case 'string':
                        showByFilter = type === 'string';
                        break;
                    case 'date':
                        showByFilter = ['date', 'datetime'].includes(type);
                        break;
                    default:
                        showByFilter = true;
                }}

                row.style.display = (showBySearch && showByFilter) ? '' : 'none';
            }});
        }}

        function filterByType(type) {{
            currentFilter = type;

            // Update button states
            document.querySelectorAll('.filter-btn').forEach(btn => {{
                btn.classList.toggle('active', btn.textContent.toLowerCase().includes(type) ||
                    (type === 'all' && btn.textContent === 'All'));
            }});

            filterColumns();
        }}

        function sortColumns() {{
            const sortBy = document.getElementById('columnSort')?.value || 'name';
            currentSort = sortBy;

            const container = document.querySelector('.column-explorer .accordion-content');
            if (!container) return;

            const rows = Array.from(container.querySelectorAll('.column-row'));

            rows.sort((a, b) => {{
                switch(sortBy) {{
                    case 'quality':
                        return parseFloat(b.dataset?.quality || 0) - parseFloat(a.dataset?.quality || 0);
                    case 'completeness':
                        return parseFloat(b.dataset?.completeness || 0) - parseFloat(a.dataset?.completeness || 0);
                    case 'issues':
                        const aIssues = a.dataset?.issues === 'true' ? 1 : 0;
                        const bIssues = b.dataset?.issues === 'true' ? 1 : 0;
                        return bIssues - aIssues;
                    default:
                        return (a.dataset?.name || '').localeCompare(b.dataset?.name || '');
                }}
            }});

            rows.forEach(row => container.appendChild(row));
        }}

        // ======================================================
        // EXPORT ANOMALIES TO CSV
        // ======================================================
        function exportAnomalies() {{
            // Gather all issues from the report
            const issues = [];

            // Collect column issues
            document.querySelectorAll('.column-row').forEach(row => {{
                const colName = row.dataset?.name || 'Unknown';
                const quality = row.dataset?.quality || 'N/A';
                row.querySelectorAll('.issue-item, .alert-item').forEach(issue => {{
                    issues.push({{
                        column: colName,
                        quality_score: quality,
                        issue_type: 'Column Issue',
                        description: issue.textContent.trim()
                    }});
                }});
            }});

            // Collect ML findings
            document.querySelectorAll('.ml-finding-card').forEach(card => {{
                const title = card.querySelector('.finding-title')?.textContent || 'ML Finding';
                card.querySelectorAll('.anomaly-item, .alert-text').forEach(item => {{
                    issues.push({{
                        column: 'ML Analysis',
                        quality_score: 'N/A',
                        issue_type: title,
                        description: item.textContent.trim()
                    }});
                }});
            }});

            // Collect alerts
            document.querySelectorAll('.alert-badge').forEach(alert => {{
                issues.push({{
                    column: 'Alert',
                    quality_score: 'N/A',
                    issue_type: alert.classList.contains('critical') ? 'Critical' :
                               alert.classList.contains('warning') ? 'Warning' : 'Info',
                    description: alert.textContent.trim()
                }});
            }});

            if (issues.length === 0) {{
                alert('No issues found to export.');
                return;
            }}

            // Generate CSV
            const headers = ['Column', 'Quality Score', 'Issue Type', 'Description'];
            const csvRows = [headers.join(',')];

            issues.forEach(issue => {{
                const row = [
                    '"' + (issue.column || '').replace(/"/g, '""') + '"',
                    issue.quality_score,
                    '"' + (issue.issue_type || '').replace(/"/g, '""') + '"',
                    '"' + (issue.description || '').replace(/"/g, '""') + '"'
                ];
                csvRows.push(row.join(','));
            }});

            const csvContent = csvRows.join('\\n');

            // Download CSV
            const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = 'data_quality_issues.csv';
            link.click();

            // Update button to show success
            const btn = event?.target;
            if (btn) {{
                const originalText = btn.textContent;
                btn.textContent = '✓ Exported!';
                btn.style.background = '#10b981';
                setTimeout(() => {{
                    btn.textContent = originalText;
                    btn.style.background = '';
                }}, 1500);
            }}
        }}

        // ======================================================
        // HEATMAP CLICK TO SCROLL
        // ======================================================
        document.querySelectorAll('.heatmap-cell').forEach(cell => {{
            cell.addEventListener('click', function() {{
                const colName = this.title.split(':')[0];
                const row = document.querySelector(`.column-row[data-name="${{colName}}"]`);
                if (row) {{
                    row.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                    row.classList.add('expanded');
                    setTimeout(() => row.style.outline = '2px solid var(--accent)', 300);
                    setTimeout(() => row.style.outline = '', 1500);
                }}
            }});
        }});

        // Copy YAML to clipboard
        function copyYaml(yamlContent) {{
            navigator.clipboard.writeText(yamlContent).then(() => {{
                const btn = event.target;
                const originalText = btn.textContent;
                btn.textContent = '✓ Copied!';
                btn.style.background = '#10b981';
                setTimeout(() => {{
                    btn.textContent = originalText;
                    btn.style.background = '';
                }}, 1500);
            }});
        }}

        // Copy full config to clipboard
        function copyFullConfig() {{
            const yamlEl = document.getElementById('fullConfigYaml');
            if (yamlEl) {{
                navigator.clipboard.writeText(yamlEl.textContent).then(() => {{
                    const btn = event.target;
                    const originalText = btn.textContent;
                    btn.textContent = '✓ Copied!';
                    setTimeout(() => {{
                        btn.textContent = originalText;
                    }}, 1500);
                }});
            }}
        }}

        // Initialize charts when library is ready
        function initCharts() {{
            if (typeof Chart === 'undefined') {{
                setTimeout(initCharts, 100);
                return;
            }}

            // Chart defaults
            const chartDefaults = {{
            responsive: true,
            maintainAspectRatio: true,
            plugins: {{
                legend: {{
                    labels: {{
                        color: '#94a3b8',
                        font: {{ size: 11 }}
                    }}
                }}
            }}
        }};

        // Type Distribution Chart
        new Chart(document.getElementById('typeChart'), {{
            type: 'doughnut',
            data: {{
                labels: {json.dumps(type_labels)},
                datasets: [{{
                    data: {json.dumps(type_data)},
                    backgroundColor: [
                        'rgba(139, 92, 246, 0.8)',
                        'rgba(59, 130, 246, 0.8)',
                        'rgba(16, 185, 129, 0.8)',
                        'rgba(245, 158, 11, 0.8)',
                        'rgba(239, 68, 68, 0.8)'
                    ],
                    borderColor: 'transparent',
                    borderWidth: 0
                }}]
            }},
            options: {{
                ...chartDefaults,
                cutout: '65%',
                plugins: {{
                    legend: {{
                        position: 'right',
                        labels: {{ color: '#94a3b8', padding: 16 }}
                    }}
                }}
            }}
        }});

        // Quality by Column Chart
        new Chart(document.getElementById('qualityChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(column_names)},
                datasets: [{{
                    label: 'Quality Score',
                    data: {json.dumps(quality_scores)},
                    backgroundColor: function(context) {{
                        const value = context.raw;
                        if (value >= 80) return 'rgba(16, 185, 129, 0.8)';
                        if (value >= 70) return 'rgba(59, 130, 246, 0.8)';
                        if (value >= 60) return 'rgba(245, 158, 11, 0.8)';
                        return 'rgba(239, 68, 68, 0.8)';
                    }},
                    borderRadius: 4
                }}]
            }},
            options: {{
                ...chartDefaults,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        max: 100,
                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                        ticks: {{
                            color: '#64748b',
                            callback: value => value + '%'
                        }}
                    }},
                    x: {{
                        grid: {{ display: false }},
                        ticks: {{
                            color: '#64748b',
                            maxRotation: 45,
                            font: {{ size: 9 }}
                        }}
                    }}
                }}
            }}
        }});

        // Bubble Chart
        new Chart(document.getElementById('bubbleChart'), {{
            type: 'bubble',
            data: {{
                datasets: [{{
                    label: 'Columns',
                    data: {json.dumps(bubble_data)},
                    backgroundColor: function(context) {{
                        const y = context.raw?.y;
                        if (!y) return 'rgba(148, 163, 184, 0.5)';
                        if (y >= 95) return 'rgba(16, 185, 129, 0.6)';
                        if (y >= 90) return 'rgba(59, 130, 246, 0.6)';
                        return 'rgba(245, 158, 11, 0.6)';
                    }}
                }}]
            }},
            options: {{
                ...chartDefaults,
                plugins: {{
                    legend: {{ display: false }},
                    tooltip: {{
                        callbacks: {{
                            label: function(context) {{
                                return context.raw.label + ': ' + context.raw.y + '% validity';
                            }}
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        title: {{ display: true, text: '{x_axis_label}', color: '#64748b' }},
                        min: Math.max(0, Math.min(...{json.dumps([b['x'] for b in bubble_data])}) - 5),
                        max: 101,
                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                        ticks: {{ color: '#64748b' }}
                    }},
                    y: {{
                        title: {{ display: true, text: 'Validity %', color: '#64748b' }},
                        min: Math.max(0, Math.min(...{json.dumps([col.quality.validity for col in profile.columns])}) - 5),
                        max: 101,
                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                        ticks: {{ color: '#64748b' }}
                    }}
                }}
            }}
        }});
        }} // end initCharts

        // Call initCharts when page is ready
        if (document.readyState === 'complete') {{
            initCharts();
        }} else {{
            window.addEventListener('load', initCharts);
        }}

        // Word Cloud - wait for library to load
        function initWordCloud() {{
            const wordCloudContainer = document.getElementById('wordCloudContainer');
            if (!wordCloudContainer) return;

            if (typeof WordCloud === 'undefined') {{
                // Library not loaded yet, retry in 100ms
                setTimeout(initWordCloud, 100);
                return;
            }}

            const wordCloudData = {json.dumps(word_cloud_data)};
            if (wordCloudData.length > 0) {{
                // Normalize weights to a reasonable range for display
                const maxWeight = Math.max(...wordCloudData.map(w => w[1]));
                const normalizedData = wordCloudData.map(w => [w[0], (w[1] / maxWeight) * 100]);

                WordCloud(wordCloudContainer, {{
                    list: normalizedData,
                    gridSize: 4,
                    weightFactor: function(size) {{
                        return Math.max(14, size * 0.5);
                    }},
                    fontFamily: 'system-ui, -apple-system, sans-serif',
                    color: function(word, weight) {{
                        if (weight > 80) return '#8b5cf6';
                        if (weight > 50) return '#60a5fa';
                        if (weight > 25) return '#10b981';
                        return '#94a3b8';
                    }},
                    backgroundColor: 'transparent',
                    rotateRatio: 0.2,
                    rotationSteps: 2,
                    shuffle: true,
                    drawOutOfBound: false,
                    shrinkToFit: true
                }});
            }}
        }}
        // Initialize word cloud after page load
        if (document.readyState === 'complete') {{
            initWordCloud();
        }} else {{
            window.addEventListener('load', initWordCloud);
        }}
'''

    def _generate_sampling_summary(self, profile: ProfileResult, sampling_info: Dict) -> str:
        """Generate the sampling summary bar."""
        return f'''
        <section class="sampling-bar" style="flex-direction: column; align-items: stretch;">
            <div style="display: flex; flex-wrap: wrap; gap: 24px; align-items: center;">
                <div class="sampling-bar-title">🔬 Processing Summary</div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Total Rows</span>
                    <span class="sampling-stat-value highlight">{profile.row_count:,}</span>
                </div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Full Scan</span>
                    <span class="sampling-stat-value">Row count, nulls, file metadata</span>
                </div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Sampled</span>
                    <span class="sampling-stat-value">Statistics, patterns, top values</span>
                </div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Sample Size</span>
                    <span class="sampling-stat-value">{sampling_info.get('typical_sample', 'N/A'):,} rows ({sampling_info.get('sample_pct', 0):.4f}%)</span>
                </div>
            </div>
            <details style="margin-top: 12px; margin-bottom: 0;">
                <summary style="cursor: pointer; color: var(--text-secondary); font-size: 0.9em; padding: 8px 0;">ℹ️ About sampling methodology...</summary>
                <div class="hint-box" style="margin-top: 8px; margin-bottom: 0; border-left-color: var(--info);">
                    <strong>📊 Sampling methodology:</strong> For {profile.row_count:,} rows, statistical sampling provides high confidence results while keeping processing time manageable.
                    <br><strong style="color: var(--warning);">⚠️ Limitations:</strong> Rare values (&lt;0.01% occurrence) and extreme outliers may not be captured in sampled statistics.
                    <br><strong>*</strong> Unique count marked with * means every sampled row was unique - actual cardinality is likely much higher.
                </div>
            </details>
        </section>'''

    def _generate_quality_alerts(self, profile: ProfileResult) -> str:
        """Generate data quality alerts for columns with significant issues."""
        alerts = []

        for col in profile.columns:
            # Sparse columns (>50% null)
            if col.quality.completeness < 50:
                null_pct = 100 - col.quality.completeness
                alerts.append({
                    'severity': 'critical',
                    'icon': '🚨',
                    'column': col.name,
                    'issue': 'Sparse Data',
                    'detail': f'{null_pct:.1f}% missing values - consider if this column is usable'
                })
            # High null columns (20-50% null)
            elif col.quality.completeness < 80:
                null_pct = 100 - col.quality.completeness
                alerts.append({
                    'severity': 'warning',
                    'icon': '⚠️',
                    'column': col.name,
                    'issue': 'Missing Data',
                    'detail': f'{null_pct:.1f}% missing values - may require imputation or handling'
                })

            # ID columns with duplicates
            is_potential_id = col.name.lower() in ['id', 'key', 'code'] or col.name.lower().endswith('_id') or col.name.lower().endswith('id')
            if is_potential_id and col.statistics.unique_percentage < 100 and col.statistics.unique_percentage > 50:
                dup_pct = 100 - col.statistics.unique_percentage
                alerts.append({
                    'severity': 'warning',
                    'icon': '🔄',
                    'column': col.name,
                    'issue': 'Potential Duplicates',
                    'detail': f'ID-like column has {dup_pct:.1f}% duplicate values'
                })

            # Very low uniqueness for string columns (might indicate data quality issue)
            # Skip ID-like columns - they're expected to have limited unique values relative to row count
            col_lower = col.name.lower()
            is_id_column = any(kw in col_lower for kw in ['account', 'id', 'key', 'code', 'num', 'number', 'ref'])
            if (col.type_info.inferred_type == 'string' and
                col.statistics.unique_percentage < 1 and
                col.statistics.unique_count > 1 and
                col.statistics.unique_count < 100 and  # Only flag truly low cardinality
                not is_id_column):
                alerts.append({
                    'severity': 'info',
                    'icon': 'ℹ️',
                    'column': col.name,
                    'issue': 'Low Cardinality',
                    'detail': f'Only {col.statistics.unique_count} unique values - likely categorical'
                })

            # Placeholder values detected (?, N/A, etc.)
            placeholder_count = getattr(col.statistics, 'placeholder_null_count', 0)
            if placeholder_count > 0:
                placeholder_pct = (placeholder_count / col.statistics.count * 100) if col.statistics.count > 0 else 0
                placeholders_found = getattr(col.statistics, 'placeholder_values_found', {})
                placeholder_examples = ', '.join(f'"{k}"' for k in list(placeholders_found.keys())[:3])
                alerts.append({
                    'severity': 'warning',
                    'icon': '❓',
                    'column': col.name,
                    'issue': 'Placeholder Values',
                    'detail': f'{placeholder_count:,} placeholder values ({placeholder_pct:.1f}%) detected: {placeholder_examples}'
                })

            # Zero-inflated distribution (for numeric columns)
            # Skip binary flags (unique_count == 2) - they're expected to be imbalanced
            is_binary_flag = col.statistics.unique_count == 2
            is_flag_column = any(kw in col.name.lower() for kw in ['flag', 'is_', 'has_', 'launder', 'fraud', 'active', 'enabled'])
            if (col.statistics.top_values and
                col.type_info.inferred_type in ['integer', 'float', 'number'] and
                not is_binary_flag and not is_flag_column):
                top_value = col.statistics.top_values[0] if col.statistics.top_values else None
                if top_value:
                    top_val = top_value.get('value')
                    top_count = top_value.get('count', 0)
                    # For sampled data, we need to estimate percentage from sample proportions
                    # Sum all top value counts to get the sample size actually counted
                    sample_total = sum(tv.get('count', 0) for tv in col.statistics.top_values)
                    if sample_total > 0:
                        sample_pct = (top_count / sample_total) * 100
                    else:
                        sample_pct = 0
                    # Check if top value is 0 (string or int) and dominates the distribution (>50% of sample)
                    if str(top_val).strip() in ['0', '0.0', '0.00'] and sample_pct > 50:
                        alerts.append({
                            'severity': 'info',
                            'icon': '📊',
                            'column': col.name,
                            'issue': 'Zero-Inflated',
                            'detail': f'~{sample_pct:.0f}% of values are 0 - consider if this is expected or indicates missing data'
                        })

            # Class imbalance for categorical columns (low cardinality)
            if col.statistics.unique_count and 2 <= col.statistics.unique_count <= 10:
                top_values = col.statistics.top_values[:2] if col.statistics.top_values else []
                if len(top_values) >= 2:
                    # For sampled data, calculate percentages from sample proportions
                    sample_total = sum(tv.get('count', 0) for tv in col.statistics.top_values)
                    if sample_total > 0:
                        top_count = top_values[0].get('count', 0)
                        second_count = top_values[1].get('count', 0)
                        top_pct = (top_count / sample_total) * 100
                        second_pct = (second_count / sample_total) * 100
                        # Significant imbalance: top class > 70% or ratio > 3:1
                        if top_pct > 70 or (second_pct > 0 and top_pct / second_pct > 3):
                            top_val = str(top_values[0].get('value', '')).strip()[:20]
                            alerts.append({
                                'severity': 'info',
                                'icon': '⚖️',
                                'column': col.name,
                                'issue': 'Class Imbalance',
                                'detail': f'Dominant value "{top_val}" appears in ~{top_pct:.0f}% of rows'
                            })

        if not alerts:
            return ''

        # Sort by severity, then by issue importance (Low Cardinality is less important)
        severity_order = {'critical': 0, 'warning': 1, 'info': 2}
        issue_priority = {
            'Sparse': 0, 'Incomplete': 1, 'Duplicate Risk': 2,
            'Placeholder Values': 3, 'Zero-Inflated': 4, 'Class Imbalance': 5,
            'Low Cardinality': 10  # Deprioritize - very common and often expected
        }
        alerts.sort(key=lambda x: (severity_order.get(x['severity'], 3), issue_priority.get(x['issue'], 6)))

        alert_items = ''
        displayed_count = min(12, len(alerts))
        for alert in alerts[:displayed_count]:
            severity_class = alert['severity']
            alert_items += f'''
                <div class="quality-alert-item {severity_class}">
                    <span class="alert-icon">{alert['icon']}</span>
                    <span class="alert-column">{alert['column']}</span>
                    <span class="alert-issue">{alert['issue']}</span>
                    <span class="alert-detail">{alert['detail']}</span>
                </div>'''

        # Add "more alerts" indicator if there are hidden alerts
        hidden_count = len(alerts) - displayed_count
        if hidden_count > 0:
            alert_items += f'''
                <div class="quality-alert-item" style="opacity: 0.7; font-style: italic;">
                    <span class="alert-icon">+</span>
                    <span class="alert-detail">...and {hidden_count} more alerts (see column details below)</span>
                </div>'''

        critical_count = sum(1 for a in alerts if a['severity'] == 'critical')
        warning_count = sum(1 for a in alerts if a['severity'] == 'warning')

        header_style = 'background: linear-gradient(135deg, rgba(239, 68, 68, 0.15) 0%, var(--bg-card) 100%); border: 1px solid rgba(239, 68, 68, 0.3);' if critical_count > 0 else 'background: linear-gradient(135deg, rgba(245, 158, 11, 0.1) 0%, var(--bg-card) 100%); border: 1px solid rgba(245, 158, 11, 0.2);'

        return f'''
        <section class="quality-alerts-section" id="section-alerts" style="{header_style} border-radius: var(--radius-lg); padding: 16px 20px; margin-bottom: 16px;">
            <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 12px;">
                <span style="font-size: 1.3em;">{'🚨' if critical_count > 0 else '⚠️'}</span>
                <div>
                    <div style="font-weight: 600; color: {'var(--critical)' if critical_count > 0 else 'var(--warning)'};">Data Quality Alerts</div>
                    <div style="font-size: 0.85em; color: var(--text-secondary);">{len(alerts)} issue(s) detected{f' • {critical_count} critical' if critical_count else ''}{f' • {warning_count} warnings' if warning_count else ''}</div>
                </div>
            </div>
            <div class="quality-alerts-list" style="display: flex; flex-direction: column; gap: 8px;">
                {alert_items}
            </div>
        </section>'''

    def _generate_fibo_section(self, profile: ProfileResult) -> str:
        """Generate FIBO semantic analysis methodology section."""
        # Count columns with semantic understanding
        semantic_columns = [
            col for col in profile.columns
            if col.semantic_info and col.semantic_info.get('primary_tag') != 'unknown'
        ]

        # Check if semantic tagging was attempted (any column has semantic_info)
        columns_with_semantic_info = [col for col in profile.columns if col.semantic_info]

        # If semantic tagging wasn't run at all, skip the section
        if not columns_with_semantic_info:
            return ""

        # If we have columns analyzed but none matched, still show the section
        if not semantic_columns:
            return f'''
        <section class="fibo-section">
            <div class="fibo-header">
                <div class="fibo-icon">🏦</div>
                <div class="fibo-title">
                    <h3>FIBO Semantic Analysis</h3>
                    <p>Financial Industry Business Ontology mapping</p>
                </div>
            </div>
            <div class="fibo-content">
                <div class="fibo-description">
                    DataK9 uses <strong>FIBO</strong> (Financial Industry Business Ontology) - an industry-standard
                    semantic framework maintained by the EDM Council - to automatically understand financial data semantics.
                </div>
                <div class="fibo-stats-grid">
                    <div class="fibo-stat">
                        <div class="fibo-stat-value">{len(columns_with_semantic_info)}</div>
                        <div class="fibo-stat-label">Columns Analyzed</div>
                        <div class="fibo-stat-note">for semantic patterns</div>
                    </div>
                    <div class="fibo-stat">
                        <div class="fibo-stat-value">0</div>
                        <div class="fibo-stat-label">FIBO Matches</div>
                        <div class="fibo-stat-note">no patterns matched</div>
                    </div>
                </div>
                <div class="hint-box" style="margin-top: 12px; border-left-color: #6b7280;">
                    ℹ️ <strong>No FIBO patterns matched:</strong> Column names in this dataset don't match common financial data patterns.
                    FIBO semantic tagging works best with columns named like "amount", "account_id", "currency", "transaction_date", etc.
                    <br><a href="https://spec.edmcouncil.org/fibo/" target="_blank" style="color: #8b5cf6;">Learn more about FIBO →</a>
                </div>
            </div>
        </section>'''

        # Categorize semantic matches
        fibo_matched = [
            col for col in semantic_columns
            if col.semantic_info.get('evidence', {}).get('fibo_match')
        ]

        # Get unique semantic categories
        categories = set()
        for col in semantic_columns:
            tag = col.semantic_info.get('primary_tag', '')
            if '.' in tag:
                categories.add(tag.split('.')[0])
            else:
                categories.add(tag)

        categories_list = sorted(categories)[:6]  # Limit to 6 categories
        categories_html = "".join([
            f'<span class="fibo-category-tag">{cat}</span>'
            for cat in categories_list
        ])

        return f'''
        <section class="fibo-section">
            <div class="fibo-header">
                <div class="fibo-icon">🏦</div>
                <div class="fibo-title">
                    <h3>FIBO Semantic Analysis</h3>
                    <p>Financial Industry Business Ontology mapping</p>
                </div>
            </div>
            <div class="fibo-content">
                <div class="fibo-stats-grid">
                    <div class="fibo-stat">
                        <div class="fibo-stat-value">{len(semantic_columns)}</div>
                        <div class="fibo-stat-label">Columns Classified</div>
                        <div class="fibo-stat-note">of {len(profile.columns)} total</div>
                    </div>
                    <div class="fibo-stat">
                        <div class="fibo-stat-value">{len(fibo_matched)}</div>
                        <div class="fibo-stat-label">FIBO Pattern Matches</div>
                        <div class="fibo-stat-note">mapped to ontology</div>
                    </div>
                    <div class="fibo-stat">
                        <div class="fibo-stat-value">{len(categories)}</div>
                        <div class="fibo-stat-label">Semantic Domains</div>
                        <div class="fibo-stat-note">detected categories</div>
                    </div>
                </div>
                <div class="fibo-categories">
                    <span class="fibo-categories-label">Detected domains:</span>
                    {categories_html}
                </div>
                <details style="margin-top: 12px;">
                    <summary style="cursor: pointer; color: var(--text-secondary); font-size: 0.9em; padding: 8px 0;">ℹ️ What is FIBO and how does it help?</summary>
                    <div class="hint-box" style="margin-top: 8px; border-left-color: #8b5cf6;">
                        <strong>🏦 FIBO</strong> (Financial Industry Business Ontology) is an industry-standard semantic framework maintained by the EDM Council.
                        DataK9 uses it to automatically understand the meaning and purpose of your data columns.
                        <br><br>
                        <strong>💡 Benefits:</strong> Columns identified as financial data types (e.g., "money.amount", "identifier.account")
                        automatically receive context-appropriate validation suggestions and intelligent ML analysis filtering.
                        <br><a href="https://spec.edmcouncil.org/fibo/" target="_blank" style="color: #8b5cf6;">Learn more about FIBO →</a>
                    </div>
                </details>
            </div>
        </section>'''

    def _generate_pii_section(self, pii_columns: List[Dict]) -> str:
        """Generate the PII risk section."""
        if not pii_columns:
            return ''

        max_risk = max(col.get('risk_score', 0) for col in pii_columns)
        risk_level = 'Critical' if max_risk > 75 else 'High' if max_risk > 50 else 'Medium'

        # Build table rows for clearer display
        pii_rows = ''
        for col in pii_columns:
            col_name = col.get('name', 'Unknown')
            pii_types = col.get('pii_types', [])
            type_name = pii_types[0].get('name', 'Unknown') if pii_types else 'Unknown'
            # If type name matches column name, show "Detected" instead to avoid "Account Account"
            if type_name.lower() == col_name.lower():
                type_name = f"{type_name} (detected)"
            frameworks = col.get('frameworks', [])
            framework_str = ', '.join(frameworks) if frameworks else 'N/A'
            risk_score = col.get('risk_score', 0)

            pii_rows += f'''
                <tr>
                    <td><strong>{col_name}</strong></td>
                    <td><span class="pii-type-badge">{type_name}</span></td>
                    <td>{framework_str}</td>
                    <td style="color: {'var(--critical)' if risk_score > 75 else 'var(--warning)' if risk_score > 50 else 'var(--text-primary)'};">{risk_score}</td>
                </tr>'''

        pii_items = f'''
            <table style="width: 100%; border-collapse: collapse; margin-top: 12px;">
                <thead>
                    <tr style="text-align: left; border-bottom: 1px solid var(--border-color);">
                        <th style="padding: 8px; color: var(--text-secondary);">Column</th>
                        <th style="padding: 8px; color: var(--text-secondary);">PII Type</th>
                        <th style="padding: 8px; color: var(--text-secondary);">Frameworks</th>
                        <th style="padding: 8px; color: var(--text-secondary);">Risk</th>
                    </tr>
                </thead>
                <tbody>{pii_rows}</tbody>
            </table>'''

        return f'''
        <div class="accordion open pii-alert" data-accordion="pii" style="border: 2px solid var(--critical); background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, var(--bg-card) 100%);">
            <div class="accordion-header" onclick="toggleAccordion(this)" style="background: linear-gradient(135deg, rgba(239, 68, 68, 0.15) 0%, transparent 100%);">
                <div class="accordion-title-group">
                    <div class="accordion-icon issues" style="background: var(--critical-soft);">🔒</div>
                    <div>
                        <div class="accordion-title" style="color: var(--critical);">⚠️ Privacy & PII Risk Detected</div>
                        <div class="accordion-subtitle">{len(pii_columns)} column(s) contain sensitive data</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-badge critical">{risk_level.upper()}</span>
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-body">
                <div class="accordion-content">
                    <div class="pii-risk-card">
                        <div class="pii-risk-header">
                            <div class="pii-risk-score">{max_risk} / 100</div>
                            <div class="pii-risk-level">{risk_level} Risk</div>
                        </div>
                    </div>
                    {pii_items}
                </div>
            </div>
        </div>'''

    def _generate_ml_section(self, ml_findings: Dict) -> str:
        """Generate the Data Insights section - narrative-driven with severity levels."""
        if not ml_findings:
            return ''

        summary = ml_findings.get('summary', {})
        total_issues = summary.get('total_issues', 0)
        severity = summary.get('severity', 'none')
        sample_info = ml_findings.get('sample_info', {})
        analysis_time = ml_findings.get('analysis_time_seconds', 0)
        analyzed_rows = sample_info.get('analyzed_rows', 0)
        original_rows = sample_info.get('original_rows', analyzed_rows)

        # Determine severity badge style
        severity_map = {
            'high': ('critical', 'ACTION NEEDED'),
            'medium': ('warning', 'REVIEW'),
            'low': ('good', 'MINOR'),
            'none': ('good', 'CLEAN')
        }
        badge_class, badge_text = severity_map.get(severity, ('good', 'UNKNOWN'))

        # Extract all findings for categorization
        format_anomalies = ml_findings.get('format_anomalies', {})
        rare_categories = ml_findings.get('rare_categories', {})
        cross_issues = ml_findings.get('cross_column_issues', [])
        numeric_outliers = ml_findings.get('numeric_outliers', {})
        autoencoder = ml_findings.get('autoencoder_anomalies', {})
        corr_anomalies = ml_findings.get('correlation_anomalies', {})
        corr_breaks = corr_anomalies.get('correlation_breaks', []) if corr_anomalies else []
        temporal_patterns = ml_findings.get('temporal_patterns', {})
        temporal_warnings = {k: v for k, v in temporal_patterns.items() if v.get('warning')}
        benford_analysis = ml_findings.get('benford_analysis', {})
        clustering = ml_findings.get('clustering_analysis', {})

        # Calculate counts for summary
        outlier_count = sum(f.get('anomaly_count', 0) for f in numeric_outliers.values())
        format_count = sum(f.get('anomaly_count', 0) for f in format_anomalies.values())
        rare_count = sum(f.get('total_rare_count', 0) for f in rare_categories.values())
        cross_count = sum(i.get('total_issues', 0) for i in cross_issues)
        ae_count = autoencoder.get('anomaly_count', 0) if autoencoder else 0
        corr_break_count = sum(b.get('anomaly_count', 0) for b in corr_breaks)

        details_html = ''

        # ═══════════════════════════════════════════════════════════════
        # EXECUTIVE SUMMARY - Key risks at a glance
        # ═══════════════════════════════════════════════════════════════
        risk_items = []
        if outlier_count > 0:
            pct = (outlier_count / analyzed_rows * 100) if analyzed_rows > 0 else 0
            risk_items.append(f'<span style="color: #ef4444;">Extreme Outliers ({pct:.1f}%)</span>')
        if ae_count > 0:
            pct = autoencoder.get('anomaly_percentage', 0)
            risk_items.append(f'<span style="color: #8b5cf6;">Multi-Column Anomalies ({pct:.1f}%)</span>')
        if cross_count > 0:
            risk_items.append(f'<span style="color: #06b6d4;">Cross-Field Issues ({cross_count:,})</span>')
        if rare_count > 0:
            risk_items.append(f'<span style="color: #f59e0b;">Rare Values ({rare_count:,})</span>')
        if temporal_warnings:
            risk_items.append(f'<span style="color: #22c55e;">Temporal Gaps</span>')

        if risk_items:
            details_html += f'''
                <div style="background: var(--card-bg); border-radius: 10px; padding: 16px 20px; margin-bottom: 20px; border: 1px solid var(--border-subtle);">
                    <div style="font-weight: 600; margin-bottom: 10px; color: var(--text-primary);">Key Risks</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 12px; font-size: 0.9em;">
                        {" • ".join(risk_items)}
                    </div>
                </div>'''

        # ═══════════════════════════════════════════════════════════════
        # SECTION 1: OUTLIER & ANOMALY ANALYSIS (Critical/High)
        # ═══════════════════════════════════════════════════════════════
        has_outliers = numeric_outliers or (autoencoder and ae_count > 0)

        if has_outliers:
            section_severity = 'CRITICAL' if outlier_count > analyzed_rows * 0.01 else 'HIGH'
            details_html += f'''
                <div style="background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%); color: white; padding: 16px 20px; margin: 16px 0 12px 0; border-radius: 10px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-weight: 600; font-size: 1.05em;">Outlier & Anomaly Analysis</div>
                            <div style="font-size: 0.85em; opacity: 0.9;">Values that deviate significantly from expected patterns</div>
                        </div>
                        <div style="background: rgba(255,255,255,0.2); padding: 4px 10px; border-radius: 4px; font-size: 0.8em; font-weight: 600;">{section_severity}</div>
                    </div>
                </div>'''

            # Numeric outliers with business impact and actions
            if numeric_outliers:
                for col, data in list(numeric_outliers.items())[:4]:
                    count = data.get('anomaly_count', 0)
                    pct = data.get('anomaly_percentage', (count / analyzed_rows * 100) if analyzed_rows > 0 else 0)
                    max_multiplier = data.get('max_deviation_from_median', 0)

                    # Generate business impact based on severity
                    if pct > 5:
                        impact = 'Mean/average calculations severely distorted. ML models will overfit to outliers.'
                        action = 'Export outlier records for review. Add RangeCheck validation to catch future issues.'
                    elif max_multiplier > 1000:
                        impact = f'Extreme values ({max_multiplier:,.0f}x median) will break visualizations and distort aggregates.'
                        action = 'Investigate data source for entry errors or currency conversion issues.'
                    else:
                        impact = 'Statistical analysis may be skewed. Consider winsorizing for analytics.'
                        action = 'Review outliers to determine if they are valid edge cases or errors.'

                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #ef4444;">
                            <div style="display: flex; justify-content: space-between; align-items: start;">
                                <div style="font-weight: 600; margin-bottom: 6px;">{col}</div>
                                <div style="font-size: 0.8em; color: #ef4444; font-weight: 600;">{pct:.2f}% outliers</div>
                            </div>
                            <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">{count:,} extreme values detected</div>
                            <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                                <strong style="color: var(--text-secondary);">Impact:</strong> {impact}
                            </div>
                            <div style="font-size: 0.85em; color: #22c55e;">
                                <strong>Action:</strong> {action}
                            </div>
                        </div>'''

            # Multi-column anomalies (autoencoder)
            if autoencoder and ae_count > 0:
                ae_pct = autoencoder.get('anomaly_percentage', 0)
                interpretation = autoencoder.get('interpretation', 'Records with unusual feature combinations that don\'t fit normal patterns')

                # Check for right-skewed reconstruction errors (dynamic explanation)
                # Skew detection: median near zero while mean/max are much higher indicates right-skew
                ae_error_stats = autoencoder.get('error_stats', {})
                ae_median_err = ae_error_stats.get('median', 0)
                ae_q75_err = ae_error_stats.get('q75', 0)
                ae_mean_err = ae_error_stats.get('mean', 0)
                ae_max_err = ae_error_stats.get('max', 0)
                # Right-skewed if: median is zero, OR median << q75, OR median << mean (less than 0.1% of mean)
                ae_is_skewed = (
                    ae_median_err == 0 or
                    (ae_q75_err > 0 and ae_median_err < ae_q75_err * 0.1) or
                    (ae_mean_err > 0 and ae_median_err < ae_mean_err * 0.001)
                )
                ae_skew_note = ''
                if ae_is_skewed and (ae_q75_err > 0 or ae_mean_err > 0):
                    ae_skew_note = '<div style="font-size: 0.8em; color: var(--text-muted); margin-top: 6px; font-style: italic;">Note: Reconstruction errors are heavily right-skewed; anomaly threshold determined from upper tail distribution.</div>'

                # Generate business impact based on severity
                if ae_pct > 5:
                    ae_impact = 'Significant portion of data has unusual multi-field patterns. May indicate systematic data issues or fraud.'
                    ae_action = 'Export anomalous records for manual review. Consider implementing cross-field validation rules.'
                elif ae_pct > 1:
                    ae_impact = 'Notable subset of records don\'t fit normal patterns. Could be edge cases or data quality issues.'
                    ae_action = 'Sample and review anomalous records to understand the root cause.'
                else:
                    ae_impact = 'Small number of unusual records detected. Likely genuine edge cases or rare scenarios.'
                    ae_action = 'Spot-check a few examples to verify they are valid business scenarios.'

                details_html += f'''
                    <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #8b5cf6;">
                        <div style="display: flex; justify-content: space-between; align-items: start;">
                            <div style="font-weight: 600; margin-bottom: 6px;">Multi-Column Anomalies</div>
                            <div style="font-size: 0.8em; color: #8b5cf6; font-weight: 600;">{ae_pct:.2f}%</div>
                        </div>
                        <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">{ae_count:,} records structurally unusual across multiple fields</div>
                        <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                            <strong style="color: var(--text-secondary);">Impact:</strong> {ae_impact}
                        </div>
                        <div style="font-size: 0.85em; color: #22c55e;">
                            <strong>Action:</strong> {ae_action}
                        </div>
                        {ae_skew_note}
                    </div>'''

        # ═══════════════════════════════════════════════════════════════
        # SECTION 2: CROSS-FIELD CONSISTENCY (Medium/High)
        # ═══════════════════════════════════════════════════════════════
        has_cross = cross_issues or corr_breaks

        if has_cross:
            section_severity = 'HIGH' if cross_count > 100 else 'MEDIUM'
            details_html += f'''
                <div style="background: linear-gradient(135deg, #f59e0b 0%, #d97706 100%); color: white; padding: 16px 20px; margin: 20px 0 12px 0; border-radius: 10px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-weight: 600; font-size: 1.05em;">Cross-Field Consistency</div>
                            <div style="font-size: 0.85em; opacity: 0.9;">Relationships between columns that don't match expected patterns</div>
                        </div>
                        <div style="background: rgba(255,255,255,0.2); padding: 4px 10px; border-radius: 4px; font-size: 0.8em; font-weight: 600;">{section_severity}</div>
                    </div>
                </div>'''

            # Cross-column issues
            if cross_issues:
                for issue in cross_issues[:3]:
                    cols = issue.get('columns', [])
                    total = issue.get('total_issues', 0)
                    interpretation = issue.get('interpretation', 'Values between these fields show unexpected relationships')

                    # Generate business impact based on count
                    if total > 1000:
                        cross_impact = 'Widespread data integrity issue. Business rules may not be enforced at entry point.'
                        cross_action = 'Implement cross-field validation at data entry. Review ETL transformation logic.'
                    elif total > 100:
                        cross_impact = 'Moderate number of records violate expected field relationships.'
                        cross_action = 'Add CrossFieldValidation rule to catch inconsistencies in future loads.'
                    else:
                        cross_impact = 'Small number of logical inconsistencies between related fields.'
                        cross_action = 'Review sample records to determine if manual correction is needed.'

                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #06b6d4;">
                            <div style="font-weight: 600; margin-bottom: 6px;">{cols[0]} ↔ {cols[1] if len(cols) > 1 else "related field"}</div>
                            <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">{total:,} records with inconsistent values</div>
                            <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                                <strong style="color: var(--text-secondary);">Impact:</strong> {cross_impact}
                            </div>
                            <div style="font-size: 0.85em; color: #22c55e;">
                                <strong>Action:</strong> {cross_action}
                            </div>
                        </div>'''

            # Correlation breaks
            if corr_breaks:
                for cb in corr_breaks[:2]:
                    cols = cb.get('columns', [])
                    count = cb.get('anomaly_count', 0)
                    interpretation = cb.get('interpretation', 'Records that deviate from the expected correlation pattern')

                    # Generate business impact
                    if count > 500:
                        corr_impact = 'Significant deviation from historical patterns. May indicate data drift or process change.'
                        corr_action = 'Compare with historical data. Check if source system changed or new data flows were added.'
                    else:
                        corr_impact = 'Some records break expected statistical relationships between fields.'
                        corr_action = 'Review flagged records for data entry errors or special business scenarios.'

                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #0891b2;">
                            <div style="font-weight: 600; margin-bottom: 6px;">{cols[0] if cols else "Column"} ↔ {cols[1] if len(cols) > 1 else "related"}: Pattern Break</div>
                            <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">{count:,} records deviate from expected relationship</div>
                            <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                                <strong style="color: var(--text-secondary);">Impact:</strong> {corr_impact}
                            </div>
                            <div style="font-size: 0.85em; color: #22c55e;">
                                <strong>Action:</strong> {corr_action}
                            </div>
                        </div>'''

        # ═══════════════════════════════════════════════════════════════
        # SECTION 3: CATEGORICAL & VALUE QUALITY (Medium)
        # ═══════════════════════════════════════════════════════════════
        has_categorical = rare_categories or format_anomalies

        if has_categorical:
            details_html += f'''
                <div style="background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%); color: white; padding: 16px 20px; margin: 20px 0 12px 0; border-radius: 10px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-weight: 600; font-size: 1.05em;">Categorical & Value Quality</div>
                            <div style="font-size: 0.85em; opacity: 0.9;">Rare values, format inconsistencies, and domain violations</div>
                        </div>
                        <div style="background: rgba(255,255,255,0.2); padding: 4px 10px; border-radius: 4px; font-size: 0.8em; font-weight: 600;">MEDIUM</div>
                    </div>
                </div>'''

            # Rare categories
            if rare_categories:
                for col, data in list(rare_categories.items())[:3]:
                    rare_vals = data.get('rare_values', [])
                    total_rare = data.get('total_rare_count', 0)
                    vals_display = ', '.join([f'"{v["value"]}"' for v in rare_vals[:3]])

                    # Generate business impact based on severity
                    if total_rare > 100:
                        rare_impact = 'Many infrequent values suggest inconsistent data entry or missing standardization.'
                        rare_action = 'Create AllowedValuesCheck validation with approved values. Consider data cleansing.'
                    else:
                        rare_impact = 'A few uncommon values that may be typos, edge cases, or valid but rare categories.'
                        rare_action = 'Review rare values to decide if they should be corrected or added to valid list.'

                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #f59e0b;">
                            <div style="font-weight: 600; margin-bottom: 6px;">{col}: {total_rare:,} rare values</div>
                            <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">Examples: {vals_display}</div>
                            <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                                <strong style="color: var(--text-secondary);">Impact:</strong> {rare_impact}
                            </div>
                            <div style="font-size: 0.85em; color: #22c55e;">
                                <strong>Action:</strong> {rare_action}
                            </div>
                        </div>'''

            # Format anomalies
            if format_anomalies:
                for col, data in list(format_anomalies.items())[:3]:
                    count = data.get('anomaly_count', 0)
                    pattern_desc = data.get('dominant_pattern_description', data.get('dominant_pattern', 'standard format'))
                    sample_anomalies = data.get('sample_anomalies', [])
                    unique_anomalies = list(dict.fromkeys(sample_anomalies))[:3]
                    anomaly_display = ', '.join(f'"{s}"' for s in unique_anomalies) if unique_anomalies else 'N/A'

                    # Generate business impact
                    if count > 1000:
                        fmt_impact = 'Widespread format inconsistency will break parsing, integrations, and downstream systems.'
                        fmt_action = 'Add FormatValidation rule. Fix data at source or implement ETL transformation.'
                    elif count > 100:
                        fmt_impact = 'Moderate format inconsistencies may cause integration failures or data loss.'
                        fmt_action = 'Implement PatternCheck validation. Review input form validation rules.'
                    else:
                        fmt_impact = 'Minor format deviations that may cause issues in strict systems.'
                        fmt_action = 'Review anomalies to determine if format validation should be enforced.'

                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #dc2626;">
                            <div style="font-weight: 600; margin-bottom: 6px;">{col}: {count:,} format mismatches</div>
                            <div style="font-size: 0.9em; color: var(--text-secondary);">Expected pattern: {pattern_desc}</div>
                            <div style="font-size: 0.85em; color: #dc2626; margin-bottom: 6px;">Found: {anomaly_display}</div>
                            <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                                <strong style="color: var(--text-secondary);">Impact:</strong> {fmt_impact}
                            </div>
                            <div style="font-size: 0.85em; color: #22c55e;">
                                <strong>Action:</strong> {fmt_action}
                            </div>
                        </div>'''

        # ═══════════════════════════════════════════════════════════════
        # SECTION 4: TEMPORAL ANALYSIS (if warnings)
        # ═══════════════════════════════════════════════════════════════
        if temporal_warnings:
            details_html += f'''
                <div style="background: linear-gradient(135deg, #22c55e 0%, #16a34a 100%); color: white; padding: 16px 20px; margin: 20px 0 12px 0; border-radius: 10px;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <div style="font-weight: 600; font-size: 1.05em;">Temporal Analysis</div>
                            <div style="font-size: 0.85em; opacity: 0.9;">Time-based patterns and gaps in your data</div>
                        </div>
                        <div style="background: rgba(255,255,255,0.2); padding: 4px 10px; border-radius: 4px; font-size: 0.8em; font-weight: 600;">INFO</div>
                    </div>
                </div>'''

            for col, data in list(temporal_warnings.items())[:2]:
                interpretation = data.get('interpretation', 'Unusual timing pattern detected')
                gap_count = data.get('gap_count', 0)
                largest_gap = data.get('largest_gap', '')

                # Generate business impact
                if gap_count > 5:
                    temp_impact = 'Multiple time gaps detected. May indicate data ingestion failures or system downtime.'
                    temp_action = 'Review ingestion logs for failures. Consider implementing data freshness monitoring.'
                else:
                    temp_impact = 'Time patterns show some irregularities worth understanding.'
                    temp_action = 'Investigate if gaps correlate with known business events or maintenance windows.'

                details_html += f'''
                    <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #22c55e;">
                        <div style="font-weight: 600; margin-bottom: 6px;">{col}: Temporal Irregularity</div>
                        <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">{interpretation}</div>
                        <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                            <strong style="color: var(--text-secondary);">Impact:</strong> {temp_impact}
                        </div>
                        <div style="font-size: 0.85em; color: #22c55e;">
                            <strong>Action:</strong> {temp_action}
                        </div>
                    </div>'''

        # ═══════════════════════════════════════════════════════════════
        # SECTION 5: DATA CONTEXT (Informational)
        # ═══════════════════════════════════════════════════════════════
        has_context = benford_analysis or (clustering and clustering.get('n_clusters', 0) > 0)

        if has_context:
            details_html += '''
                <div style="background: linear-gradient(135deg, #6b7280 0%, #4b5563 100%); color: white; padding: 16px 20px; margin: 20px 0 12px 0; border-radius: 10px;">
                    <div>
                        <div style="font-weight: 600; font-size: 1.05em;">Data Characteristics</div>
                        <div style="font-size: 0.85em; opacity: 0.9;">Understanding your data's natural structure</div>
                    </div>
                </div>'''

            # Benford's Law
            if benford_analysis:
                for col, data in list(benford_analysis.items())[:2]:
                    is_suspicious = data.get('is_suspicious', False)
                    chi_square = data.get('chi_square', 0)
                    p_value = data.get('p_value', 0)

                    if is_suspicious:
                        interpretation = 'Digit distribution deviates from natural patterns - may indicate synthetic or manipulated data'
                        benford_impact = 'Unusual digit patterns may indicate data manipulation, synthetic generation, or non-organic sources.'
                        benford_action = 'Review data provenance. Consider forensic analysis if fraud is suspected.'
                    else:
                        interpretation = 'Digit distribution follows natural patterns (Benford\'s Law) - consistent with organic financial data'
                        benford_impact = 'Data shows natural patterns consistent with real-world financial data.'
                        benford_action = 'No action needed. This is a positive indicator of data authenticity.'

                    status_text = "Deviates (p < 0.05)" if is_suspicious else "Conforms (p ≥ 0.05)"
                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid {"#f59e0b" if is_suspicious else "#22c55e"};">
                            <div style="font-weight: 600; margin-bottom: 6px;">{col}: {"Unusual" if is_suspicious else "Natural"} Digit Distribution</div>
                            <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">Benford's Law: {status_text} (χ²={chi_square:.1f})</div>
                            <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                                <strong style="color: var(--text-secondary);">Impact:</strong> {benford_impact}
                            </div>
                            <div style="font-size: 0.85em; color: #22c55e;">
                                <strong>Action:</strong> {benford_action}
                            </div>
                        </div>'''

            # Clustering
            if clustering and clustering.get('n_clusters', 0) > 0:
                n_clusters = clustering.get('n_clusters', 0)
                noise_points = clustering.get('noise_points', 0)
                noise_pct = (noise_points / analyzed_rows * 100) if analyzed_rows > 0 else 0

                # Generate impact based on noise
                if noise_pct > 10:
                    cluster_impact = f'{noise_pct:.1f}% of records are outliers that don\'t fit any natural group. These may need special handling.'
                    cluster_action = 'Review noise points for data quality issues or create separate processing rules for edge cases.'
                else:
                    cluster_impact = f'Data has clear structure with {n_clusters} natural groupings. Low noise indicates consistent data patterns.'
                    cluster_action = 'Consider using clusters for segmentation analysis or targeted validation rules.'

                details_html += f'''
                    <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #8b5cf6;">
                        <div style="font-weight: 600; margin-bottom: 6px;">Natural Data Clustering</div>
                        <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">Data naturally forms {n_clusters} distinct groups • {noise_points:,} noise points ({noise_pct:.1f}%)</div>
                        <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                            <strong style="color: var(--text-secondary);">Impact:</strong> {cluster_impact}
                        </div>
                        <div style="font-size: 0.85em; color: #22c55e;">
                            <strong>Action:</strong> {cluster_action}
                        </div>
                    </div>'''

        return f'''
        <div class="accordion open" data-accordion="data-insights">
            <div class="accordion-header" onclick="toggleAccordion(this)">
                <div class="accordion-title-group">
                    <div class="accordion-icon ml">💡</div>
                    <div>
                        <div class="accordion-title">Data Insights</div>
                        <div class="accordion-subtitle">Analyzed {analyzed_rows:,} rows{f" (sample of {original_rows:,})" if original_rows > analyzed_rows else ""} in {analysis_time:.1f}s</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-badge {badge_class}">{badge_text}</span>
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-body">
                <div class="accordion-content">
                    {details_html}
                </div>
            </div>
        </div>'''

    def _generate_ml_section_v2(self, ml_findings: Dict) -> str:
        """
        Generate v2 Data Insights section with masterpiece insight widgets.

        New design features:
        1. Plain English summary (always visible)
        2. Example table with real data (always visible)
        3. Collapsible technical details with data science explanation

        Language: Observations, not issues. Awareness, not problems to fix.
        """
        if not ml_findings:
            return ''

        sample_info = ml_findings.get('sample_info', {})
        analysis_time = ml_findings.get('analysis_time_seconds', 0)
        analyzed_rows = sample_info.get('analyzed_rows', 0)
        original_rows = sample_info.get('original_rows', analyzed_rows)

        # Extract all findings
        numeric_outliers = ml_findings.get('numeric_outliers', {})
        # Try multivariate_outliers first (new key), fallback to autoencoder_anomalies
        autoencoder = ml_findings.get('multivariate_outliers', ml_findings.get('autoencoder_anomalies', {}))
        cross_issues = ml_findings.get('cross_column_issues', [])
        corr_anomalies = ml_findings.get('correlation_anomalies', {})
        corr_breaks = corr_anomalies.get('correlation_breaks', []) if corr_anomalies else []
        rare_categories = ml_findings.get('rare_categories', {})
        format_anomalies = ml_findings.get('format_anomalies', {})
        temporal_patterns = ml_findings.get('temporal_patterns', {})
        benford_analysis = ml_findings.get('benford_analysis', {})
        clustering = ml_findings.get('clustering_analysis', {})

        widgets_html = ''

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 1: OUTLIER PATTERNS (Isolation Forest ML)
        # ═══════════════════════════════════════════════════════════════
        if numeric_outliers:
            total_outliers = sum(f.get('anomaly_count', 0) for f in numeric_outliers.values())
            outlier_pct = (total_outliers / analyzed_rows * 100) if analyzed_rows > 0 else 0

            # Plain English explanation
            plain_english = f'''About {outlier_pct:.1f}% of values ({total_outliers:,} records) are dramatically different
from typical values in their columns. These could be large transactions, data entry errors,
currency conversion issues, or legitimate edge cases worth reviewing.'''

            # Build example table rows from sample_rows (correct key)
            example_rows = ''
            for col_name, data in list(numeric_outliers.items())[:3]:
                # Try sample_rows first, then fallback to other keys
                samples = data.get('sample_rows', data.get('sample_anomalies', data.get('sample_outliers', [])))
                top_anomalies = data.get('top_anomalies', [])
                normal_range = data.get('normal_range', {})
                median = normal_range.get('median', data.get('median', 0))

                # Use sample_rows if available
                for sample in samples[:2]:
                    if isinstance(sample, dict):
                        val = sample.get(col_name, 'N/A')
                        # Try to format numeric values
                        try:
                            val_num = float(str(val).replace(',', ''))
                            val_display = f'{val_num:,.2f}'
                        except:
                            val_display = str(val)
                        median_display = f'{median:,.2f}' if median else 'N/A'
                        example_rows += f'''
                        <tr>
                            <td>{col_name}</td>
                            <td class="value-highlight">{val_display}</td>
                            <td class="value-normal">{median_display}</td>
                        </tr>'''

                # If no sample_rows, use top_anomalies values
                if not samples and top_anomalies:
                    for val in top_anomalies[:2]:
                        try:
                            val_display = f'{float(val):,.2f}'
                        except:
                            val_display = str(val)
                        median_display = f'{median:,.2f}' if median else 'N/A'
                        example_rows += f'''
                        <tr>
                            <td>{col_name}</td>
                            <td class="value-highlight">{val_display}</td>
                            <td class="value-normal">{median_display}</td>
                        </tr>'''

            if not example_rows:
                example_rows = '<tr><td colspan="3" style="text-align: center; color: var(--text-muted);">Sample data not available</td></tr>'

            # Technical details
            tech_items = []
            for col_name, data in list(numeric_outliers.items())[:3]:
                method = data.get('method', 'Isolation Forest')
                count = data.get('anomaly_count', 0)
                confidence = data.get('confidence', 'N/A')
                tech_items.append(f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">{col_name}</span>
                        <span class="insight-technical-item-value">{count:,} outliers • {method} • confidence: {confidence}</span>
                    </div>''')

            widgets_html += self._build_insight_widget(
                icon="🧠",
                title="Outlier Patterns",
                badge_text=f"{outlier_pct:.1f}% of rows",
                badge_class="warning" if outlier_pct > 1 else "info",
                plain_english=plain_english,
                table_headers=["Column", "Outlier Value", "Typical (Median)"],
                table_rows=example_rows,
                technical_items=''.join(tech_items),
                technical_context=[
                    f"ML Model: Isolation Forest (unsupervised anomaly detection)",
                    f"Sample Size: {analyzed_rows:,} rows",
                    f"Columns Analyzed: {len(numeric_outliers)}"
                ],
                ml_model="Isolation Forest"
            )
        else:
            # Show that we checked but found nothing - with meaningful data context
            widgets_html += self._build_insight_widget(
                icon="✓",
                title="Outlier Patterns",
                badge_text="100% normal",
                badge_class="good",
                plain_english=f'''All {analyzed_rows:,} rows analyzed show values within expected statistical ranges.
No extreme outliers were detected in any numeric column. This indicates consistent
data without unexpected spikes or anomalous values that might warrant review.''',
                table_headers=["Metric", "Finding"],
                table_rows=f'''<tr><td>Rows Checked</td><td class="value-normal">{analyzed_rows:,}</td></tr>
                <tr><td>Outliers Found</td><td class="value-normal">0 (0.00%)</td></tr>
                <tr><td>Status</td><td class="value-normal">All values within normal range</td></tr>''',
                technical_items='''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Method</span>
                        <span class="insight-technical-item-value">Isolation Forest (contamination=auto)</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Implication</span>
                        <span class="insight-technical-item-value">Data values are consistent and well-distributed</span>
                    </div>
                ''',
                technical_context=[
                    "Isolation Forest ML algorithm scanned all numeric columns",
                    "No values exceeded the anomaly score threshold (>3σ from mean)",
                    "This is a positive finding indicating clean, consistent data"
                ],
                ml_model="Isolation Forest"
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 2: UNUSUAL COMBINATIONS (Isolation Forest Multivariate)
        # ═══════════════════════════════════════════════════════════════
        ae_count = autoencoder.get('anomaly_count', 0) if autoencoder else 0
        if autoencoder and ae_count > 0:
            ae_pct = autoencoder.get('anomaly_percentage', 0)
            method = autoencoder.get('method', 'Isolation Forest')

            # Check for right-skewed reconstruction errors (dynamic explanation)
            # Skew detection: median near zero while mean/max are much higher indicates right-skew
            ae_error_stats = autoencoder.get('error_stats', {})
            ae_median_err = ae_error_stats.get('median', 0)
            ae_q75_err = ae_error_stats.get('q75', 0)
            ae_mean_err = ae_error_stats.get('mean', 0)
            # Right-skewed if: median is zero, OR median << q75, OR median << mean
            ae_is_skewed = (
                ae_median_err == 0 or
                (ae_q75_err > 0 and ae_median_err < ae_q75_err * 0.1) or
                (ae_mean_err > 0 and ae_median_err < ae_mean_err * 0.001)
            )
            ae_skew_note = ''
            if ae_is_skewed and (ae_q75_err > 0 or ae_mean_err > 0):
                ae_skew_note = ' (Note: reconstruction errors are heavily right-skewed in this dataset; anomaly threshold determined from upper tail distribution.)'

            plain_english = f'''About {ae_pct:.2f}% of records ({ae_count:,}) have unusual combinations
of values across multiple fields. These records don't fit the normal patterns we see
in your data - they could be rare but valid scenarios, or may warrant investigation.{ae_skew_note}'''

            # Build example table from sample_rows (correct key)
            example_rows = ''
            sample_records = autoencoder.get('sample_rows', autoencoder.get('sample_anomalies', []))
            contributing_cols = autoencoder.get('contributing_columns', [])

            for i, record in enumerate(sample_records[:5]):
                if isinstance(record, dict):
                    # Format key values for display
                    key_vals = []
                    for k, v in list(record.items())[:4]:
                        try:
                            val_num = float(str(v).replace(',', ''))
                            key_vals.append(f"{k}: {val_num:,.2f}")
                        except:
                            key_vals.append(f"{k}: {v}")
                    fields_display = ' | '.join(key_vals)
                    example_rows += f'''
                    <tr>
                        <td style="max-width: 400px; font-size: 0.85em;">{fields_display}</td>
                    </tr>'''

            # Add contributing column info if available
            if contributing_cols and not example_rows:
                for col_info in contributing_cols[:3]:
                    col_name = col_info.get('column', 'Unknown')
                    z_score = col_info.get('z_score_diff', 0)
                    normal_mean = col_info.get('normal_mean', 0)
                    anomaly_mean = col_info.get('anomaly_mean', 0)
                    example_rows += f'''
                    <tr>
                        <td><strong>{col_name}</strong>: Anomaly avg {anomaly_mean:,.2f} vs normal {normal_mean:,.2f} (z-diff: {z_score:.1f})</td>
                    </tr>'''

            if not example_rows:
                example_rows = '<tr><td style="text-align: center; color: var(--text-muted);">Sample records not available</td></tr>'

            widgets_html += self._build_insight_widget(
                icon="🧠",
                title="Unusual Combinations",
                badge_text=f"{ae_pct:.2f}%",
                badge_class="warning" if ae_pct > 1 else "info",
                plain_english=plain_english,
                table_headers=["Sample Records with Unusual Patterns"],
                table_rows=example_rows,
                technical_items=f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Method</span>
                        <span class="insight-technical-item-value">{method}</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Columns Analyzed</span>
                        <span class="insight-technical-item-value">{autoencoder.get('columns_analyzed', 'All numeric')}</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Contamination</span>
                        <span class="insight-technical-item-value">{autoencoder.get('contamination_used', 'auto')}</span>
                    </div>
                ''',
                technical_context=[
                    f"ML Model: {method} (multivariate anomaly detection)",
                    "Analyzes multiple columns together to find unusual combinations",
                    "Useful for detecting fraud, data entry errors, or rare edge cases"
                ],
                ml_model=method
            )
        else:
            # Show that multivariate analysis was performed but found nothing
            widgets_html += self._build_insight_widget(
                icon="✓",
                title="Unusual Combinations",
                badge_text="None detected",
                badge_class="good",
                plain_english='''Multivariate analysis found no unusual combinations across your columns.
Records show consistent relationships between fields. This indicates well-structured
data without suspicious multi-field patterns.''',
                table_headers=["Analysis", "Result"],
                table_rows=f'''<tr><td>Multivariate Isolation Forest</td><td class="value-normal">No anomalies detected</td></tr>
                <tr><td>Rows Analyzed</td><td>{analyzed_rows:,}</td></tr>''',
                technical_items='''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Method</span>
                        <span class="insight-technical-item-value">Isolation Forest (multivariate)</span>
                    </div>
                ''',
                technical_context=[
                    "Multivariate analysis examines relationships across all numeric columns",
                    "No records showed unusual combinations of values",
                    "This is a positive indicator of data consistency"
                ],
                ml_model="Isolation Forest"
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 3: CROSS-FIELD RELATIONSHIPS
        # ═══════════════════════════════════════════════════════════════
        if cross_issues or corr_breaks:
            # Use only extreme ratio count - this is the primary cross-field metric
            # Correlation breaks are a different statistical measure and shouldn't be added
            extreme_ratio_count = sum(i.get('total_issues', 0) for i in cross_issues)
            corr_break_total = sum(b.get('anomaly_count', 0) for b in corr_breaks)
            # Get the outlier percentage from the data (dynamic, defaults to 1% if not available)
            corr_outlier_percent = corr_breaks[0].get('outlier_percent', 1) if corr_breaks else 1

            # Build description based on what we found
            if extreme_ratio_count > 0 and corr_break_total > 0:
                plain_english = f'''We found {extreme_ratio_count:,} records with extreme value ratios between
related amount fields (e.g., Amount Paid vs Amount Received differ by >10x).
Additionally, the top {corr_outlier_percent}% of records ({corr_break_total:,} rows) exhibit unusually
large regression residuals, indicating atypical relationships between correlated fields.'''
            elif extreme_ratio_count > 0:
                plain_english = f'''We found {extreme_ratio_count:,} records where related amount fields have
extreme ratios (>10x or <0.1x). These could indicate currency conversion,
fees, or data entry issues that warrant review.'''
            else:
                plain_english = f'''The top {corr_outlier_percent}% of records ({corr_break_total:,} rows) exhibit unusually
large regression residuals between correlated fields. These statistical outliers
deviate from expected linear relationships and may indicate edge cases worth investigating.'''

            # Collect all samples with their ratios for sorting
            all_samples = []
            for issue in cross_issues:
                cols = issue.get('columns', [])
                samples = issue.get('sample_rows', issue.get('sample_issues', []))
                median_ratio = issue.get('median_ratio', 1.0)
                for sample in samples:
                    if isinstance(sample, dict):
                        ratio = sample.get('ratio', None)
                        if ratio is not None:
                            # Calculate deviation from median (log scale)
                            log_dev = abs(math.log10(max(ratio, 1e-10)) - math.log10(max(median_ratio, 1e-10)))
                            all_samples.append({
                                'cols': cols,
                                'sample': sample,
                                'ratio': ratio,
                                'deviation': log_dev
                            })

            # Sort by deviation (most extreme first) and take top 5
            all_samples.sort(key=lambda x: x['deviation'], reverse=True)

            # Format ratio for display - handle near-zero baselines dynamically
            def fmt_ratio(r, near_zero=False):
                if near_zero:
                    return '<span title="Ratio extremely large due to near-zero baseline (below 1st percentile threshold)">∞ (near-zero baseline)</span>'
                if r >= 10000:
                    return f'<span title="Ratio extremely large due to near-zero baseline">{r:,.0f}x</span>'
                elif r >= 1000:
                    return f'{r:,.0f}x'
                elif r >= 10:
                    return f'{r:.1f}x'
                elif r >= 1:
                    return f'{r:.2f}x'
                else:
                    return f'{r:.4f}x'

            example_rows = ''
            for item in all_samples[:5]:
                cols = item['cols']
                sample = item['sample']
                ratio = item['ratio']
                near_zero = sample.get('near_zero_baseline', False)
                row_idx = sample.get('row_index', '?')
                col1, col2 = cols[0] if len(cols) > 0 else '?', cols[1] if len(cols) > 1 else '?'
                val1 = sample.get(col1, 'N/A')
                val2 = sample.get(col2, 'N/A')
                # Format large numbers
                if isinstance(val1, (int, float)):
                    val1 = f'{val1:,.2f}' if isinstance(val1, float) else f'{val1:,}'
                if isinstance(val2, (int, float)):
                    val2 = f'{val2:,.2f}' if isinstance(val2, float) else f'{val2:,}'
                row_display = f'{row_idx:,}' if isinstance(row_idx, int) else str(row_idx)
                example_rows += f'''
                    <tr>
                        <td class="row-id">{row_display}</td>
                        <td>{col1} ↔ {col2}</td>
                        <td>{val1} ↔ {val2}</td>
                        <td class="value-highlight">{fmt_ratio(ratio, near_zero)}</td>
                    </tr>'''

            if not example_rows:
                example_rows = '<tr><td colspan="4" style="text-align: center; color: var(--text-muted);">Sample data not available</td></tr>'

            # Primary badge shows extreme ratio count (most actionable)
            primary_count = extreme_ratio_count if extreme_ratio_count > 0 else corr_break_total

            # Build technical items with clear separation of methods
            tech_items = f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Extreme Ratios</span>
                        <span class="insight-technical-item-value">{extreme_ratio_count:,} records (ratio >10x or <0.1x)</span>
                    </div>'''
            if corr_break_total > 0:
                tech_items += f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Correlation Outliers</span>
                        <span class="insight-technical-item-value">{corr_break_total:,} records (top {corr_outlier_percent}% regression residuals)</span>
                    </div>'''
            tech_items += f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Column Pairs</span>
                        <span class="insight-technical-item-value">{len(cross_issues)} analyzed</span>
                    </div>'''

            widgets_html += self._build_insight_widget(
                icon="⚡",
                title="Cross-Field Relationships",
                badge_text=f"{primary_count:,} records",
                badge_class="warning" if primary_count > 100 else "info",
                plain_english=plain_english,
                table_headers=["Row #", "Fields", "Values", "Ratio"],
                table_rows=example_rows,
                technical_items=tech_items,
                technical_context=[
                    "Extreme ratio: identifies records where related fields differ by >10x",
                    f"Correlation outlier: top {corr_outlier_percent}% deviations from expected linear relationship",
                    "Near-zero baseline: ratio shown as '∞' when denominator falls below 1st percentile threshold",
                    "Examples sorted by ratio extremeness (most extreme first)"
                ]
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 4: VALUE DISTRIBUTION (Rare Categories)
        # ═══════════════════════════════════════════════════════════════
        if rare_categories:
            total_rare = sum(f.get('total_rare_count', 0) for f in rare_categories.values())

            plain_english = f'''Some categorical fields contain rare values that appear infrequently
({total_rare:,} total instances). These could be typos, edge cases, or legitimate
but uncommon categories. Review to decide if standardization is needed.'''

            example_rows = ''
            for col_name, data in list(rare_categories.items())[:3]:
                rare_vals = data.get('rare_values', [])
                for rv in rare_vals[:3]:
                    val = rv.get('value', 'N/A')
                    count = rv.get('count', 0)
                    pct = rv.get('percentage', 0)
                    example_rows += f'''
                    <tr>
                        <td>{col_name}</td>
                        <td class="value-highlight">"{val}"</td>
                        <td>{count:,}</td>
                        <td class="value-normal">{pct:.2f}%</td>
                    </tr>'''

            if not example_rows:
                example_rows = '<tr><td colspan="4" style="text-align: center; color: var(--text-muted);">No rare values found</td></tr>'

            widgets_html += self._build_insight_widget(
                icon="📊",
                title="Value Distribution",
                badge_text=f"{total_rare:,} rare values",
                badge_class="info",
                plain_english=plain_english,
                table_headers=["Column", "Rare Value", "Count", "Frequency"],
                table_rows=example_rows,
                technical_items=f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Detection Method</span>
                        <span class="insight-technical-item-value">Frequency analysis (threshold &lt; 1%)</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Columns Analyzed</span>
                        <span class="insight-technical-item-value">{len(rare_categories)}</span>
                    </div>
                ''',
                technical_context=[
                    "Rare values are those appearing in less than 1% of records",
                    "High cardinality columns may have many legitimate rare values",
                    "Consider AllowedValuesCheck validation for controlled vocabularies"
                ]
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 5: TEMPORAL PATTERNS
        # ═══════════════════════════════════════════════════════════════
        temporal_warnings = {k: v for k, v in temporal_patterns.items() if v.get('warning')}
        if temporal_warnings:
            # Calculate total large gaps dynamically
            total_large_gaps = sum(d.get('large_gaps_count', 0) for d in temporal_warnings.values())

            plain_english = f'''Your date/time fields show {total_large_gaps:,} significant temporal gaps - intervals
exceeding 10× the median event frequency for this dataset. These gaps may indicate
data collection issues, seasonal patterns, or system downtime.'''

            example_rows = ''
            for col_name, data in list(temporal_warnings.items())[:3]:
                interpretation = data.get('interpretation', 'Temporal irregularity detected')
                # large_gaps_count = significant gaps (>10x median frequency from ML)
                significant_gaps = data.get('large_gaps_count', 0)
                # Try both old and new key names for largest gap
                largest_gap = data.get('largest_gap_days', data.get('largest_gap', 'N/A'))
                if isinstance(largest_gap, (int, float)):
                    largest_gap = f'{largest_gap} days'
                example_rows += f'''
                <tr>
                    <td>{col_name}</td>
                    <td>{interpretation[:60]}...</td>
                    <td>{significant_gaps:,} large gaps</td>
                    <td class="value-highlight">{largest_gap}</td>
                </tr>'''

            if not example_rows:
                example_rows = '<tr><td colspan="4" style="text-align: center; color: var(--text-muted);">No temporal issues</td></tr>'

            widgets_html += self._build_insight_widget(
                icon="📅",
                title="Temporal Patterns",
                badge_text=f"{len(temporal_warnings)} fields",
                badge_class="info",
                plain_english=plain_english,
                table_headers=["Column", "Pattern", "Gaps", "Largest"],
                table_rows=example_rows,
                technical_items=f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Analysis Type</span>
                        <span class="insight-technical-item-value">Time series gap detection</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Large Gap Threshold</span>
                        <span class="insight-technical-item-value">10× median event interval (dynamic)</span>
                    </div>
                ''',
                technical_context=[
                    "Large gaps: intervals exceeding 10× the median frequency (dynamically computed)",
                    "Seasonality analysis looks for recurring patterns",
                    "Consider FreshnessCheck validation for time-sensitive data"
                ]
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 6: DATA AUTHENTICITY (Benford's Law)
        # Always show - even when not analyzed, it's useful to explain why
        # ═══════════════════════════════════════════════════════════════
        if benford_analysis:
            # Use is_suspicious flag (reliable) instead of confidence string (semantics vary)
            concerning = {k: v for k, v in benford_analysis.items() if v.get('is_suspicious', False)}
            natural = {k: v for k, v in benford_analysis.items() if not v.get('is_suspicious', True)}

            if concerning:
                plain_english = f'''The digit distribution in {len(concerning)} numeric field(s) deviates from
natural patterns (Benford's Law). This could indicate synthetic data generation,
rounding, or data manipulation - though some data types naturally don't follow Benford's.'''
                badge_class = "warning"
            else:
                plain_english = f'''The digit patterns in your numeric fields follow natural distributions
(Benford's Law). This is a positive sign of organic, real-world data rather than
synthetic or manipulated values.'''
                badge_class = "good"

            example_rows = ''
            for col_name, data in list(benford_analysis.items())[:4]:
                chi_square = data.get('chi_square', 0)
                p_value = data.get('p_value', 0)
                is_suspicious = data.get('is_suspicious', False)
                # Show clear status based on is_suspicious flag
                status = "Deviates" if is_suspicious else "Conforms"
                example_rows += f'''
                <tr>
                    <td>{col_name}</td>
                    <td class="{'value-highlight' if is_suspicious else 'value-normal'}">{status}</td>
                    <td>{chi_square:.2f}</td>
                    <td>{p_value:.4f}</td>
                </tr>'''

            widgets_html += self._build_insight_widget(
                icon="🔍",
                title="Data Authenticity",
                badge_text="Benford's Law",
                badge_class=badge_class,
                plain_english=plain_english,
                table_headers=["Column", "Status", "Chi-Square", "P-Value"],
                table_rows=example_rows,
                technical_items=f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Test</span>
                        <span class="insight-technical-item-value">Benford's Law Chi-Square Test</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Significance Level</span>
                        <span class="insight-technical-item-value">α = 0.05</span>
                    </div>
                ''',
                technical_context=[
                    "Benford's Law describes expected first-digit distribution in natural datasets",
                    "Financial data, population counts, and measurements typically follow this pattern",
                    "Deviations may be legitimate (e.g., assigned IDs) or indicate data issues"
                ]
            )
        else:
            # Always show Benford section - explain when not analyzed
            # Check if there are money-related columns that could be tested
            money_cols = [col for col in profile.columns if col.semantic_info and
                         col.semantic_info.get('primary_tag', '').startswith('money')] if hasattr(self, '_current_profile') else []

            plain_english = '''Benford's Law analysis checks whether digit patterns in financial data
follow natural distributions. This can help detect synthetic or manipulated data.
Analysis was not performed on this dataset - this may be because no financial
amount fields were detected, or the analysis was skipped during profiling.'''

            widgets_html += self._build_insight_widget(
                icon="🔍",
                title="Data Authenticity",
                badge_text="Not Analyzed",
                badge_class="info",
                plain_english=plain_english,
                table_headers=["Status", "Reason"],
                table_rows='''
                <tr>
                    <td>Skipped</td>
                    <td>No applicable money/amount columns detected for Benford's Law analysis</td>
                </tr>''',
                technical_items='''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Applicable Column Types</span>
                        <span class="insight-technical-item-value">Financial amounts, counts, populations</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">When to Use</span>
                        <span class="insight-technical-item-value">Fraud detection, data validation, synthetic data detection</span>
                    </div>
                ''',
                technical_context=[
                    "Benford's Law applies to naturally occurring numeric data spanning multiple orders of magnitude",
                    "IDs, codes, and assigned numbers do not follow Benford's Law",
                    "To enable: ensure columns have FIBO semantic tags like 'money.amount'"
                ]
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 7: NATURAL CLUSTERING
        # ═══════════════════════════════════════════════════════════════
        if clustering and clustering.get('n_clusters', 0) > 0:
            n_clusters = clustering.get('n_clusters', 0)
            noise_points = clustering.get('noise_points', 0)
            rows_analyzed_cluster = clustering.get('rows_analyzed', analyzed_rows)
            noise_pct = clustering.get('noise_percentage', (noise_points / rows_analyzed_cluster * 100) if rows_analyzed_cluster > 0 else 0)

            plain_english = f'''Your data naturally groups into {n_clusters} distinct clusters. This reveals
underlying structure - perhaps different customer segments, transaction types, or
data sources. {noise_pct:.1f}% of records don't fit any cluster (these may be unusual cases).'''

            # Build cluster details table showing top clusters
            clusters_info = clustering.get('clusters', [])
            example_rows = ''

            # Show top 5 clusters by size
            for cluster in clusters_info[:5]:
                cluster_id = cluster.get('cluster_id', '?')
                size = cluster.get('size', 0)
                pct = cluster.get('percentage', 0)

                # Build characteristics summary
                chars = cluster.get('characteristics', {})
                char_summary = []
                for col_name, stats in list(chars.items())[:2]:  # Show first 2 columns
                    mean = stats.get('mean', 0)
                    if mean > 1000000:
                        char_summary.append(f"{col_name}: {mean/1000000:.1f}M")
                    elif mean > 1000:
                        char_summary.append(f"{col_name}: {mean/1000:.1f}K")
                    else:
                        char_summary.append(f"{col_name}: {mean:.1f}")

                char_display = ', '.join(char_summary) if char_summary else 'N/A'

                example_rows += f'''
                <tr>
                    <td class="value-normal">Cluster {cluster_id}</td>
                    <td>{size:,} ({pct:.1f}%)</td>
                    <td style="font-size: 0.85em;">{char_display}</td>
                </tr>'''

            # Add noise points row
            example_rows += f'''
            <tr style="border-top: 1px solid rgba(148,163,184,0.1);">
                <td class="value-highlight">Noise (outliers)</td>
                <td>{noise_points:,} ({noise_pct:.1f}%)</td>
                <td style="font-size: 0.85em; color: var(--text-muted);">Records not fitting any cluster</td>
            </tr>'''

            cluster_method = clustering.get('method', 'DBSCAN')
            columns_analyzed = clustering.get('columns_analyzed', [])
            cols_display = ', '.join(columns_analyzed[:4]) if isinstance(columns_analyzed, list) else str(columns_analyzed)
            if isinstance(columns_analyzed, list) and len(columns_analyzed) > 4:
                cols_display += f' +{len(columns_analyzed)-4} more'

            widgets_html += self._build_insight_widget(
                icon="🧠",
                title="Natural Clustering",
                badge_text=f"{n_clusters} clusters",
                badge_class="info",
                plain_english=plain_english,
                table_headers=["Cluster", "Size", "Key Characteristics"],
                table_rows=example_rows,
                technical_items=f'''
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Algorithm</span>
                        <span class="insight-technical-item-value">{cluster_method} (density-based)</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Features</span>
                        <span class="insight-technical-item-value">{cols_display}</span>
                    </div>
                    <div class="insight-technical-item">
                        <span class="insight-technical-item-label">Rows Analyzed</span>
                        <span class="insight-technical-item-value">{rows_analyzed_cluster:,}</span>
                    </div>
                ''',
                technical_context=[
                    f"ML Model: {cluster_method} (unsupervised clustering)",
                    "Finds clusters of arbitrary shape based on point density",
                    "Noise points are records that don't belong to any dense region"
                ],
                ml_model=cluster_method
            )

        # Wrap all widgets in a section
        if not widgets_html:
            return ''

        sample_note = f" (sample of {original_rows:,})" if original_rows > analyzed_rows else ""

        return f'''
        <section id="section-risks">
            <div class="section-header-v2">
                <div>
                    <span class="section-header-v2-icon">💡</span>
                    <span class="section-header-v2-title">Data Insights</span>
                    <div class="section-header-v2-subtitle">Patterns observed from analyzing {analyzed_rows:,} rows{sample_note} in {analysis_time:.1f}s</div>
                </div>
                <span class="section-header-v2-badge observations">OBSERVATIONS</span>
            </div>
            {widgets_html}
        </section>
        '''

    def _build_insight_widget(self, icon: str, title: str, badge_text: str, badge_class: str,
                              plain_english: str, table_headers: list, table_rows: str,
                              technical_items: str, technical_context: list,
                              ml_model: str = None) -> str:
        """
        Build a single insight widget with the masterpiece design.

        Args:
            icon: Emoji icon for the widget
            title: Widget title
            badge_text: Text for the badge (e.g., "0.3% of rows")
            badge_class: CSS class for badge (critical, warning, info, good)
            plain_english: Plain language explanation of the insight
            table_headers: List of column headers for example table
            table_rows: Pre-built HTML rows for the example table
            technical_items: Pre-built HTML for technical details grid
            technical_context: List of context bullet points
            ml_model: Optional ML model name to display (e.g., "Isolation Forest")
        """
        # Build table headers
        headers_html = ''.join([f'<th>{h}</th>' for h in table_headers])

        # Build technical context list
        context_html = ''.join([f'<li>{item}</li>' for item in technical_context])

        # Generate unique ID for toggle functionality
        widget_id = f"tech_{hash(title) % 10000}"

        # ML model badge if specified
        ml_badge = ''
        if ml_model:
            ml_badge = f'<span class="insight-ml-badge">🧠 {ml_model}</span>'

        return f'''
        <div class="insight-widget">
            <div class="insight-widget-header">
                <div class="insight-widget-title-group">
                    <span class="insight-widget-icon">{icon}</span>
                    <span class="insight-widget-title">{title}</span>
                    {ml_badge}
                </div>
                <span class="insight-widget-badge {badge_class}">{badge_text}</span>
            </div>

            <div class="insight-widget-body">
                <!-- Plain English Summary -->
                <div class="insight-summary">
                    <div class="insight-summary-label">What This Means</div>
                    <div class="insight-summary-text">{plain_english}</div>
                </div>

                <!-- Example Table -->
                <div class="insight-examples">
                    <div class="insight-examples-label">Examples From Your Data</div>
                    <table class="insight-examples-table">
                        <thead>
                            <tr>{headers_html}</tr>
                        </thead>
                        <tbody>
                            {table_rows}
                        </tbody>
                    </table>
                </div>

                <!-- Technical Details (Collapsible) -->
                <div class="insight-technical" id="{widget_id}">
                    <button class="insight-technical-toggle" onclick="document.getElementById('{widget_id}').classList.toggle('open')">
                        Show Technical Details
                        <span class="toggle-arrow">▼</span>
                    </button>
                    <div class="insight-technical-content">
                        <div class="insight-technical-grid">
                            {technical_items}
                        </div>
                        <div class="insight-technical-context">
                            <div class="insight-technical-context-title">Statistical Context</div>
                            <ul class="insight-technical-context-list">
                                {context_html}
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        '''

    def _generate_advanced_visualizations(self, ml_findings: Dict, columns: List = None) -> str:
        """
        Generate advanced visualization section with interactive charts.

        Includes:
        1. Log-scaled distribution plots for amount fields
        2. Scatterplot for Amount Received vs Amount Paid
        3. Class imbalance bar chart
        4. Anomaly score distribution
        5. Reconstruction error distribution
        """
        if not ml_findings:
            return ''

        viz_data = ml_findings.get('visualizations', {})
        if not viz_data:
            return ''

        # Build column stats lookup for true min/max from Parquet metadata
        column_stats = {}
        if columns:
            for col in columns:
                if col.statistics:
                    column_stats[col.name] = {
                        'true_min': col.statistics.min_value,
                        'true_max': col.statistics.max_value
                    }

        sample_info = viz_data.get('sample_info', {})
        sample_note = ""
        if sample_info.get('is_sampled'):
            sample_note = f"<span style='color: var(--text-muted); font-size: 0.85em;'>(Based on {sample_info.get('sample_size', 0):,} sample of {sample_info.get('total_rows', 0):,} rows)</span>"

        sections_html = []

        # ═══════════════════════════════════════════════════════════════
        # 1. AMOUNT FIELD DISTRIBUTIONS (Log-scaled)
        # ═══════════════════════════════════════════════════════════════
        amount_dists = viz_data.get('amount_distributions', {})
        if amount_dists:
            charts_html = ''
            chart_scripts = []
            is_sampled = sample_info.get('is_sampled', False)
            for idx, (col, dist_data) in enumerate(list(amount_dists.items())[:4]):
                if not dist_data or not dist_data.get('histogram'):
                    continue

                chart_id = f'amountLogHist_{idx}'
                sample_min = dist_data.get('min_value', 0)
                sample_max = dist_data.get('max_value', 0)
                median_val = dist_data.get('median', 0)
                mean_val = dist_data.get('mean', 0)

                # Get true min/max from column stats (Parquet metadata for all rows)
                col_true_stats = column_stats.get(col, {})
                true_min = col_true_stats.get('true_min', sample_min)
                true_max = col_true_stats.get('true_max', sample_max)

                # Format large numbers more readably
                def fmt_num(n):
                    if n >= 1e12:
                        return f'{n/1e12:.2f}T'
                    elif n >= 1e9:
                        return f'{n/1e9:.2f}B'
                    elif n >= 1e6:
                        return f'{n/1e6:.2f}M'
                    elif n >= 1e3:
                        return f'{n/1e3:.1f}K'
                    else:
                        return f'{n:,.2f}'

                # Display true min/max prominently, with sample stats if different
                if is_sampled and (abs(true_max - sample_max) > 1e6 or abs(true_min - sample_min) > 1e6):
                    stats_line = f'Min: {fmt_num(true_min)} | Max: {fmt_num(true_max)} (all rows)'
                    sample_line = f'<span style="font-size: 0.75em; color: var(--text-muted);">Sample Max: {fmt_num(sample_max)}</span>'
                else:
                    stats_line = f'Min: {fmt_num(true_min)} | Max: {fmt_num(true_max)} | Median: {fmt_num(median_val)}'
                    sample_line = ''

                charts_html += f'''
                    <div style="flex: 1; min-width: 300px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-subtle);">
                        <h4 style="margin: 0 0 8px 0; font-size: 0.95em; color: var(--text-primary);">{col}</h4>
                        <div style="font-size: 0.8em; color: var(--text-muted); margin-bottom: 4px;">
                            {stats_line}
                        </div>
                        {f'<div style="margin-bottom: 8px;">{sample_line}</div>' if sample_line else ''}
                        <div style="height: 180px;">
                            <canvas id="{chart_id}"></canvas>
                        </div>
                    </div>'''

                histogram = dist_data.get('histogram', [])
                bin_edges = dist_data.get('bin_edges', [])
                labels = [f'{bin_edges[i]:,.0f}' if bin_edges[i] >= 1 else f'{bin_edges[i]:.2f}'
                          for i in range(len(bin_edges)-1)] if len(bin_edges) > 1 else []

                chart_scripts.append({
                    'id': chart_id,
                    'labels': labels[:len(histogram)],
                    'data': histogram
                })

            if charts_html:
                sections_html.append(f'''
                    <div class="accordion" data-accordion="viz-amounts">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #10b981, #059669);">📊</div>
                                <div>
                                    <div class="accordion-title">Amount Distributions (Log Scale)</div>
                                    <div class="accordion-subtitle">Visualize skewed financial data distributions</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(amount_dists)} Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            <p style="color: var(--text-secondary); margin-bottom: 16px;">
                                <strong>Why log scale?</strong> Financial data often spans multiple orders of magnitude.
                                Log scaling reveals patterns in both small and large values that would be hidden in linear plots.
                                {sample_note}
                                {' See Column Explorer for true min/max from all rows.' if is_sampled else ''}
                            </p>
                            <div style="display: flex; flex-wrap: wrap; gap: 16px;">
                                {charts_html}
                            </div>
                            <script>
                            document.addEventListener('DOMContentLoaded', function() {{
                                const amountCharts = {json.dumps(chart_scripts)};
                                amountCharts.forEach(chart => {{
                                    const ctx = document.getElementById(chart.id);
                                    if (ctx) {{
                                        new Chart(ctx, {{
                                            type: 'bar',
                                            data: {{
                                                labels: chart.labels,
                                                datasets: [{{
                                                    label: 'Frequency',
                                                    data: chart.data,
                                                    backgroundColor: 'rgba(16, 185, 129, 0.6)',
                                                    borderColor: 'rgba(16, 185, 129, 1)',
                                                    borderWidth: 1
                                                }}]
                                            }},
                                            options: {{
                                                responsive: true,
                                                maintainAspectRatio: false,
                                                plugins: {{ legend: {{ display: false }} }},
                                                scales: {{
                                                    x: {{
                                                        title: {{ display: true, text: 'Value (log scale)', color: '#64748b' }},
                                                        ticks: {{ display: false }},
                                                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }}
                                                    }},
                                                    y: {{
                                                        title: {{ display: true, text: 'Count', color: '#64748b' }},
                                                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                        ticks: {{ color: '#64748b' }}
                                                    }}
                                                }}
                                            }}
                                        }});
                                    }}
                                }});
                            }});
                            </script>
                        </div>
                    </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 2. AMOUNT SCATTER PLOT (Received vs Paid)
        # ═══════════════════════════════════════════════════════════════
        scatter_data = viz_data.get('amount_scatter')
        if scatter_data and scatter_data.get('points'):
            x_col = scatter_data.get('x_column', 'Received')
            y_col = scatter_data.get('y_column', 'Paid')
            points = scatter_data.get('points', [])
            total_points = scatter_data.get('total_points', len(points))

            sections_html.append(f'''
                <div class="accordion" data-accordion="viz-scatter">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #3b82f6, #1d4ed8);">⚡</div>
                            <div>
                                <div class="accordion-title">{x_col} vs {y_col}</div>
                                <div class="accordion-subtitle">Identify mismatches between related amount fields</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{total_points:,} Points</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <p style="color: var(--text-secondary); margin-bottom: 16px;">
                            <strong>Interpretation:</strong> Points should cluster near the diagonal line (y=x) if received and paid amounts match.
                            Deviations indicate potential currency conversion, fees, or data issues. {sample_note}
                        </p>
                        <div style="height: 400px; background: var(--bg-card); border-radius: 8px; padding: 16px;">
                            <canvas id="amountScatterChart"></canvas>
                        </div>
                        <script>
                        document.addEventListener('DOMContentLoaded', function() {{
                            const scatterCtx = document.getElementById('amountScatterChart');
                            if (scatterCtx) {{
                                const scatterPoints = {json.dumps(points[:1000])};
                                const maxVal = Math.max(...scatterPoints.map(p => Math.max(p.x, p.y)));
                                new Chart(scatterCtx, {{
                                    type: 'scatter',
                                    data: {{
                                        datasets: [
                                            {{
                                                label: 'Transactions',
                                                data: scatterPoints,
                                                backgroundColor: 'rgba(59, 130, 246, 0.5)',
                                                borderColor: 'rgba(59, 130, 246, 0.8)',
                                                pointRadius: 3
                                            }},
                                            {{
                                                label: 'y = x (Expected)',
                                                data: [{{x: 0, y: 0}}, {{x: maxVal, y: maxVal}}],
                                                type: 'line',
                                                borderColor: 'rgba(239, 68, 68, 0.7)',
                                                borderDash: [5, 5],
                                                borderWidth: 2,
                                                pointRadius: 0,
                                                fill: false
                                            }}
                                        ]
                                    }},
                                    options: {{
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        plugins: {{
                                            legend: {{ position: 'top', labels: {{ color: '#94a3b8' }} }}
                                        }},
                                        scales: {{
                                            x: {{
                                                title: {{ display: true, text: '{x_col}', color: '#64748b' }},
                                                grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                ticks: {{ color: '#64748b' }}
                                            }},
                                            y: {{
                                                title: {{ display: true, text: '{y_col}', color: '#64748b' }},
                                                grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                ticks: {{ color: '#64748b' }}
                                            }}
                                        }}
                                    }}
                                }});
                            }}
                        }});
                        </script>
                    </div>
                </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 3. CLASS IMBALANCE CHARTS
        # ═══════════════════════════════════════════════════════════════
        class_data = viz_data.get('class_imbalance', {})
        if class_data:
            imbalance_charts = ''
            imbalance_scripts = []

            for idx, (col, data) in enumerate(list(class_data.items())[:4]):
                classes = data.get('classes', [])
                if not classes:
                    continue

                chart_id = f'classImbalance_{idx}'
                is_target = data.get('is_target_like', False)
                is_binary = data.get('is_binary', False)
                total = data.get('total', 0)

                # Check for imbalance
                if is_binary and len(classes) >= 2:
                    minority_pct = min(c['percentage'] for c in classes)
                    imbalance_status = 'critical' if minority_pct < 10 else 'warning' if minority_pct < 30 else 'good'
                    imbalance_note = f"Minority class: {minority_pct:.1f}%"
                else:
                    imbalance_status = 'info'
                    imbalance_note = f"{len(classes)} classes"

                imbalance_charts += f'''
                    <div style="flex: 1; min-width: 280px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-subtle);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                            <h4 style="margin: 0; font-size: 0.95em; color: var(--text-primary);">{col}</h4>
                            <span class="accordion-badge {imbalance_status}">{imbalance_note}</span>
                        </div>
                        <div style="height: 200px;">
                            <canvas id="{chart_id}"></canvas>
                        </div>
                        <div style="font-size: 0.8em; color: var(--text-muted); margin-top: 8px; text-align: center;">
                            {'🎯 Likely ML target' if is_target else ''} Total: {total:,}
                        </div>
                    </div>'''

                imbalance_scripts.append({
                    'id': chart_id,
                    'labels': [c['value'] for c in classes],
                    'data': [c['count'] for c in classes],
                    'percentages': [c['percentage'] for c in classes]
                })

            if imbalance_charts:
                sections_html.append(f'''
                    <div class="accordion" data-accordion="viz-imbalance">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #f59e0b, #d97706);">⚖️</div>
                                <div>
                                    <div class="accordion-title">Class Distribution & Imbalance</div>
                                    <div class="accordion-subtitle">Target variable distributions for ML readiness</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(class_data)} Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            <p style="color: var(--text-secondary); margin-bottom: 16px;">
                                <strong>Why this matters:</strong> Severely imbalanced classes (&lt;10% minority) can cause ML models
                                to ignore the minority class entirely. Consider resampling, class weights, or alternative metrics.
                                {sample_note}
                            </p>
                            <div style="display: flex; flex-wrap: wrap; gap: 16px;">
                                {imbalance_charts}
                            </div>
                            <script>
                            document.addEventListener('DOMContentLoaded', function() {{
                                const imbalanceCharts = {json.dumps(imbalance_scripts)};
                                const colors = ['rgba(139, 92, 246, 0.8)', 'rgba(59, 130, 246, 0.8)', 'rgba(16, 185, 129, 0.8)',
                                               'rgba(245, 158, 11, 0.8)', 'rgba(239, 68, 68, 0.8)', 'rgba(236, 72, 153, 0.8)'];
                                imbalanceCharts.forEach(chart => {{
                                    const ctx = document.getElementById(chart.id);
                                    if (ctx) {{
                                        new Chart(ctx, {{
                                            type: 'bar',
                                            data: {{
                                                labels: chart.labels,
                                                datasets: [{{
                                                    label: 'Count',
                                                    data: chart.data,
                                                    backgroundColor: colors.slice(0, chart.labels.length),
                                                    borderWidth: 0,
                                                    borderRadius: 4
                                                }}]
                                            }},
                                            options: {{
                                                responsive: true,
                                                maintainAspectRatio: false,
                                                plugins: {{
                                                    legend: {{ display: false }},
                                                    tooltip: {{
                                                        callbacks: {{
                                                            label: function(context) {{
                                                                const pct = chart.percentages[context.dataIndex];
                                                                return `Count: ${{context.raw.toLocaleString()}} (${{pct}}%)`;
                                                            }}
                                                        }}
                                                    }}
                                                }},
                                                scales: {{
                                                    x: {{
                                                        grid: {{ display: false }},
                                                        ticks: {{ color: '#94a3b8', font: {{ size: 11 }} }}
                                                    }},
                                                    y: {{
                                                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                        ticks: {{ color: '#64748b' }}
                                                    }}
                                                }}
                                            }}
                                        }});
                                    }}
                                }});
                            }});
                            </script>
                        </div>
                    </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 4. ACTIVITY TIMELINE (Temporal Density)
        # ═══════════════════════════════════════════════════════════════
        temporal_density = viz_data.get('temporal_density', {})
        if temporal_density:
            timeline_charts = ''
            timeline_scripts = []

            for idx, (col, data) in enumerate(list(temporal_density.items())[:2]):
                if not data or not data.get('histogram'):
                    continue

                chart_id = f'activityTimeline_{idx}'
                min_date = data.get('min_date', '')
                max_date = data.get('max_date', '')
                total_records = data.get('total_records', 0)
                gaps_detected = data.get('gaps_detected', 0)
                peak = data.get('peak_activity', {})
                date_range_days = data.get('date_range_days', 0)

                gap_badge = f'<span class="accordion-badge warning">{gaps_detected} Gaps</span>' if gaps_detected > 0 else ''

                timeline_charts += f'''
                    <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; margin-bottom: 16px; border: 1px solid var(--border-subtle);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                            <h4 style="margin: 0; font-size: 0.95em; color: var(--text-primary);">📅 {col}</h4>
                            {gap_badge}
                        </div>
                        <div style="font-size: 0.8em; color: var(--text-muted); margin-bottom: 12px;">
                            Range: {date_range_days} days | Peak: {peak.get('label', 'N/A')} ({peak.get('count', 0):,} events)
                        </div>
                        <div style="height: 200px;">
                            <canvas id="{chart_id}"></canvas>
                        </div>
                    </div>'''

                histogram = data.get('histogram', [])
                labels = data.get('labels', [])

                timeline_scripts.append({
                    'id': chart_id,
                    'labels': labels,
                    'data': histogram,
                    'gaps': data.get('gap_labels', [])
                })

            if timeline_charts:
                sections_html.append(f'''
                    <div class="accordion" data-accordion="viz-timeline">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #8b5cf6, #6d28d9);">📈</div>
                                <div>
                                    <div class="accordion-title">Activity Timeline</div>
                                    <div class="accordion-subtitle">Event density over time - detect coverage gaps</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(temporal_density)} Temporal Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            <p style="color: var(--text-secondary); margin-bottom: 16px;">
                                <strong>What this shows:</strong> The distribution of events/transactions over time.
                                Daily gaps represent calendar days with zero recorded events after aggregation.
                                Gaps may indicate ingestion issues, business seasonality, or expected quiet periods.
                                {sample_note}
                            </p>
                            {timeline_charts}
                            <script>
                            document.addEventListener('DOMContentLoaded', function() {{
                                const timelineCharts = {json.dumps(timeline_scripts)};
                                timelineCharts.forEach(chart => {{
                                    const ctx = document.getElementById(chart.id);
                                    if (ctx) {{
                                        // Color bars - highlight gaps in red
                                        const bgColors = chart.data.map((val, idx) => {{
                                            return chart.gaps.includes(chart.labels[idx]) ? 'rgba(239, 68, 68, 0.7)' : 'rgba(139, 92, 246, 0.6)';
                                        }});

                                        new Chart(ctx, {{
                                            type: 'bar',
                                            data: {{
                                                labels: chart.labels,
                                                datasets: [{{
                                                    label: 'Events',
                                                    data: chart.data,
                                                    backgroundColor: bgColors,
                                                    borderWidth: 0
                                                }}]
                                            }},
                                            options: {{
                                                responsive: true,
                                                maintainAspectRatio: false,
                                                plugins: {{ legend: {{ display: false }} }},
                                                scales: {{
                                                    x: {{
                                                        title: {{ display: true, text: 'Time Period', color: '#64748b' }},
                                                        ticks: {{
                                                            maxRotation: 45,
                                                            minRotation: 45,
                                                            color: '#64748b',
                                                            font: {{ size: 9 }},
                                                            maxTicksLimit: 15
                                                        }},
                                                        grid: {{ display: false }}
                                                    }},
                                                    y: {{
                                                        title: {{ display: true, text: 'Event Count', color: '#64748b' }},
                                                        grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                        ticks: {{ color: '#64748b' }}
                                                    }}
                                                }}
                                            }}
                                        }});
                                    }}
                                }});
                            }});
                            </script>
                        </div>
                    </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 5. RECONSTRUCTION ERROR DISTRIBUTION (Autoencoder)
        # ═══════════════════════════════════════════════════════════════
        recon_errors = viz_data.get('reconstruction_errors')
        if recon_errors:
            mean_err = recon_errors.get('mean', 0)
            median_err = recon_errors.get('median', 0)
            q75_err = recon_errors.get('q75', 0)
            std_err = recon_errors.get('std', 0)
            threshold = recon_errors.get('threshold', 0)
            # If threshold is 0 or missing, compute from mean + 2.5*std (approximation)
            if threshold == 0 and std_err > 0:
                threshold = mean_err + 2.5 * std_err
            anomaly_count = recon_errors.get('anomaly_count', 0)
            anomaly_pct = recon_errors.get('anomaly_percentage', 0)

            # Smart formatting for very small values (use scientific notation)
            def fmt_err(val):
                if val == 0:
                    return "0"
                elif abs(val) < 0.0001:
                    return f"{val:.2e}"
                else:
                    return f"{val:.4f}"

            threshold_str = fmt_err(threshold)
            median_str = fmt_err(median_err)
            q75_str = fmt_err(q75_err)

            # Dynamic explanation for right-skewed distributions where errors collapse to near-zero
            # Skew detection: median near zero while mean/max are much higher indicates right-skew
            # Right-skewed if: median is zero, OR median << q75, OR median << mean (less than 0.1% of mean)
            skew_explanation = ""
            is_right_skewed = (
                median_err == 0 or
                (q75_err > 0 and median_err < q75_err * 0.1) or
                (mean_err > 0 and median_err < mean_err * 0.001)
            )
            if is_right_skewed and (q75_err > 0 or mean_err > 0):
                skew_explanation = '<br><em style="font-size: 0.9em; color: var(--text-muted);">Note: Reconstruction errors are heavily right-skewed in this dataset; most errors are near zero, so the anomaly threshold is determined from the upper tail of the error distribution rather than from quartiles.</em>'

            sections_html.append(f'''
                <div class="accordion" data-accordion="viz-autoencoder">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #ec4899, #be185d);">🧠</div>
                            <div>
                                <div class="accordion-title">Autoencoder Reconstruction Errors</div>
                                <div class="accordion-subtitle">Deep learning anomaly detection threshold</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge {'critical' if anomaly_pct > 5 else 'warning' if anomaly_pct > 1 else 'good'}">{anomaly_count:,} Anomalies</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <p style="color: var(--text-secondary); margin-bottom: 16px;">
                            <strong>What this shows:</strong> The autoencoder learns normal patterns and measures how well it can
                            reconstruct each record. High reconstruction error indicates the record is unusual.
                            Records above the threshold ({threshold_str}) are flagged as anomalies.
                            {sample_note}{skew_explanation}
                        </p>
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; margin-bottom: 20px;">
                            <div style="background: var(--bg-card); padding: 14px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: bold; color: var(--text-primary);">{median_str}</div>
                                <div style="font-size: 0.8em; color: var(--text-muted);">Median Error</div>
                            </div>
                            <div style="background: var(--bg-card); padding: 14px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: bold; color: var(--text-primary);">{q75_str}</div>
                                <div style="font-size: 0.8em; color: var(--text-muted);">Q75 Error</div>
                            </div>
                            <div style="background: var(--bg-card); padding: 14px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: bold; color: var(--critical);">{threshold_str}</div>
                                <div style="font-size: 0.8em; color: var(--text-muted);">Threshold</div>
                            </div>
                            <div style="background: var(--bg-card); padding: 14px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: bold; color: var(--warning);">{anomaly_pct:.2f}%</div>
                                <div style="font-size: 0.8em; color: var(--text-muted);">Anomaly Rate</div>
                            </div>
                        </div>
                        <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; height: 200px;">
                            <canvas id="reconErrorChart"></canvas>
                        </div>
                        <script>
                        document.addEventListener('DOMContentLoaded', function() {{
                            const reconCtx = document.getElementById('reconErrorChart');
                            if (reconCtx) {{
                                // Use median for center (robust to outliers) and IQR-based scale
                                const median = {median_err};
                                const q75 = {q75_err};
                                const threshold = {threshold};
                                // Use IQR to estimate distribution width (more robust than std)
                                const iqr = (q75 - median) * 2;  // Approximate IQR
                                const displayStd = iqr > 0 ? iqr : 0.001;  // Fallback for very tight distributions
                                const bins = 40;
                                const labels = [];
                                const data = [];
                                const bgColors = [];

                                for (let i = 0; i < bins; i++) {{
                                    const x = median - 3*displayStd + (6*displayStd/bins) * i;
                                    const y = Math.exp(-0.5 * Math.pow((x - median) / displayStd, 2)) * 1000;
                                    labels.push(x.toExponential(2));
                                    data.push(Math.max(0, y));
                                    bgColors.push(x > threshold ? 'rgba(239, 68, 68, 0.7)' : 'rgba(139, 92, 246, 0.6)');
                                }}

                                new Chart(reconCtx, {{
                                    type: 'bar',
                                    data: {{
                                        labels: labels,
                                        datasets: [{{
                                            label: 'Frequency',
                                            data: data,
                                            backgroundColor: bgColors,
                                            borderWidth: 0
                                        }}]
                                    }},
                                    options: {{
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        plugins: {{
                                            legend: {{ display: false }},
                                            annotation: {{
                                                annotations: {{
                                                    thresholdLine: {{
                                                        type: 'line',
                                                        xMin: threshold,
                                                        xMax: threshold,
                                                        borderColor: 'rgba(239, 68, 68, 1)',
                                                        borderWidth: 2,
                                                        borderDash: [5, 5],
                                                        label: {{
                                                            display: true,
                                                            content: 'Threshold',
                                                            position: 'start'
                                                        }}
                                                    }}
                                                }}
                                            }}
                                        }},
                                        scales: {{
                                            x: {{
                                                display: false
                                            }},
                                            y: {{
                                                grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                ticks: {{ display: false }}
                                            }}
                                        }}
                                    }}
                                }});
                            }}
                        }});
                        </script>
                    </div>
                </div>''')

        # Combine all sections
        if not sections_html:
            return ''

        return f'''
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- ADVANCED VISUALIZATIONS                                          -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #0d9488 0%, #065f46 100%); border-radius: 8px; border-left: 4px solid #14b8a6;">
            <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">📈 ADVANCED VISUALIZATIONS</h2>
            <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #99f6e4;">Interactive charts for deeper data understanding</p>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                {''.join(sections_html)}
            </div>
        </div>
        '''

    def _generate_overview_accordion(self, profile: ProfileResult, type_counts: Dict,
                                     avg_completeness: float, avg_validity: float,
                                     avg_consistency: float, avg_uniqueness: float) -> str:
        """Generate the data types accordion (simplified - quality metrics shown in dashboard above)."""

        # Build type breakdown text
        type_breakdown = ', '.join([f"{count} {t}" for t, count in type_counts.items() if count > 0])

        return f'''
                <div class="accordion" data-accordion="overview">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon overview">🔤</div>
                            <div>
                                <div class="accordion-title">Data Types</div>
                                <div class="accordion-subtitle">{type_breakdown}</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{profile.column_count} columns</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>🔤 Column Types:</strong> Shows the distribution of data types across your columns. Understanding types helps identify potential type mismatches or conversion needs.
                            </div>
                            <div class="chart-container">
                                <div class="chart-title">Data Type Distribution</div>
                                <canvas id="typeChart" height="100"></canvas>
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_quality_accordion(self, profile: ProfileResult) -> str:
        """Generate the quality metrics accordion."""
        return f'''
                <div class="accordion" data-accordion="quality">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">✓</div>
                            <div>
                                <div class="accordion-title">Quality Metrics</div>
                                <div class="accordion-subtitle">Detailed quality breakdown by dimension</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge {'good' if profile.overall_quality_score >= 80 else 'warning'}">{profile.overall_quality_score:.0f}%</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>📈 Quality Score Breakdown:</strong> The overall quality score combines multiple factors:
                                <br>• <strong>Completeness</strong> (40%): Percentage of non-null values
                                <br>• <strong>Validity</strong> (30%): Values matching expected type/format
                                <br>• <strong>Consistency</strong> (20%): Pattern uniformity across the column
                                <br>• <strong>Uniqueness</strong> (10%): Cardinality relative to column type
                            </div>
                            <div class="chart-container">
                                <div class="chart-title">Quality Score by Column</div>
                                <canvas id="qualityChart" height="120"></canvas>
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_distribution_accordion(self, profile: ProfileResult, categorical_columns: List[Dict]) -> str:
        """Generate the value distribution accordion with charts."""
        cat_names = ', '.join(c.get('name', '') for c in categorical_columns[:3]) if categorical_columns else 'N/A'

        # Determine which metric to show on x-axis based on variation
        completeness_values = [col.quality.completeness for col in profile.columns]
        uniqueness_values = [col.quality.uniqueness for col in profile.columns]
        consistency_values = [col.quality.consistency for col in profile.columns]

        completeness_range = max(completeness_values) - min(completeness_values) if completeness_values else 0
        uniqueness_range = max(uniqueness_values) - min(uniqueness_values) if uniqueness_values else 0
        consistency_range = max(consistency_values) - min(consistency_values) if consistency_values else 0

        if completeness_range >= 5:
            x_axis_metric_name = 'Completeness'
        elif uniqueness_range >= 5:
            x_axis_metric_name = 'Uniqueness'
        elif consistency_range >= 5:
            x_axis_metric_name = 'Consistency'
        else:
            x_axis_metric_name = 'Completeness'

        return f'''
                <div class="accordion open" data-accordion="distribution">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">📊</div>
                            <div>
                                <div class="accordion-title">Value Distribution</div>
                                <div class="accordion-subtitle">Top values in categorical columns</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">3 charts</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>📊 Value Analysis:</strong> These charts show column quality metrics and value distributions. Useful for:
                                <br>• Identifying data quality issues
                                <br>• Building ValidValuesCheck validations
                                <br>• Understanding business domain distribution
                            </div>
                            <div style="display: grid; gap: 20px;">
                                <div class="chart-container">
                                    <div class="chart-title">Column Quality: {x_axis_metric_name} vs Validity</div>
                                    <div class="hint-box" style="margin-bottom: 12px; margin-top: 0;">
                                        Bubble size represents unique value count. Position shows {x_axis_metric_name.lower()} (x) vs validity (y).
                                    </div>
                                    <canvas id="bubbleChart" height="200"></canvas>
                                </div>
                                <div class="chart-container">
                                    <div class="chart-title">Categorical Values Word Cloud</div>
                                    <div class="hint-box" style="margin-bottom: 12px; margin-top: 0;">
                                        Word size represents frequency. Showing top values from: {cat_names}
                                    </div>
                                    <div id="wordCloudContainer" style="width: 100%; height: 250px; background: var(--bg-elevated); border-radius: var(--radius-md);"></div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_column_explorer(self, profile: ProfileResult) -> str:
        """Generate the column explorer accordion."""

        column_rows = ''
        for col in profile.columns:
            column_rows += self._generate_column_row(col)

        tagged_count = sum(1 for col in profile.columns if col.semantic_info or col.pii_info and col.pii_info.get('detected'))

        # Generate heatmap
        heatmap_html = self._generate_column_heatmap(profile)

        # Generate search controls
        search_controls = self._generate_column_search_controls()

        return f'''
                <!-- Column Quality Heatmap -->
                {heatmap_html}

                <div class="accordion column-explorer open" data-accordion="columns" id="section-columns">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon columns">📋</div>
                            <div>
                                <div class="accordion-title">Column Explorer</div>
                                <div class="accordion-subtitle">{profile.column_count} columns with semantic analysis</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{tagged_count} tagged</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>🧠 Intelligent Analysis:</strong> Columns are automatically analyzed for:
                                <br>• <strong>Semantic meaning</strong> - MONEY, TIMESTAMP, ACCOUNT_ID, etc.
                                <br>• <strong>PII detection</strong> - Credit cards, SSN, emails, phones
                                <br>• <strong>Temporal patterns</strong> - Date ranges, frequency, gaps
                                <br>Click any column to see details. Use the heatmap above for a quick overview.
                                <details style="margin-top: 8px; color: var(--text-tertiary); font-size: 0.85em;">
                                    <summary style="cursor: pointer; color: var(--text-secondary);">📊 <strong>Data Source Guide</strong> (click to expand)</summary>
                                    <div style="margin-top: 8px; padding: 10px; background: rgba(var(--primary-color-rgb), 0.05); border-radius: 6px;">
                                        <strong>From ALL rows (exact values):</strong>
                                        <br>• Row Count, Null Count - computed during full file scan
                                        <br>• Min/Max (numeric) - from Parquet metadata or full scan
                                        <br><br>
                                        <strong>From analysis sample (statistical metrics):</strong>
                                        <br>• Mean, Std Dev, Median - computed from analysis sample rows
                                        <br>• ML Analysis - patterns, outliers, clusters from sampled data
                                        <br><br>
                                        <strong>Per-column value tracking (adaptive):</strong>
                                        <br>• Top Values - tracked differently per column based on cardinality
                                        <br>• Low cardinality columns: all unique values captured (7-1000)
                                        <br>• High cardinality columns: capped at 5K-10K for memory efficiency
                                        <br>• <em>"Sampled: N" shows values tracked, not rows analyzed</em>
                                    </div>
                                </details>
                            </div>
                            {search_controls}
                            <div class="column-list" id="columnList">
                                {column_rows}
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_column_row(self, col: ColumnProfile) -> str:
        """Generate a single column row for the explorer."""

        # Determine icon and type class
        inferred_type = col.type_info.inferred_type
        if inferred_type in ['date', 'datetime', 'timestamp']:
            icon = '📅'
            type_class = 'date'
        elif inferred_type in ['integer', 'float', 'number']:
            icon = '🔢'
            type_class = 'number'
        else:
            icon = '📝'
            type_class = 'string'

        # Check for PII
        if col.pii_info and col.pii_info.get('detected'):
            icon = '🔐'

        # Quality score class
        score = col.quality.overall_score
        score_class = 'good' if score >= 80 else 'warning' if score >= 60 else 'critical'

        # Tags
        tags = ''
        # Critical quality tags first
        if col.quality.completeness < 50:
            tags += '<span class="column-tag sparse">SPARSE</span>'
        elif col.quality.completeness < 80:
            tags += '<span class="column-tag incomplete">INCOMPLETE</span>'

        # Check for potential ID column with duplicates (high uniqueness expected but not 100%)
        is_potential_id = col.name.lower() in ['id', 'key', 'code'] or col.name.lower().endswith('_id') or col.name.lower().endswith('id')
        if is_potential_id and col.statistics.unique_percentage < 100 and col.statistics.unique_percentage > 50:
            tags += '<span class="column-tag duplicate-risk">DUPLICATES</span>'

        if col.pii_info and col.pii_info.get('detected'):
            tags += '<span class="column-tag pii">PII</span>'
        if col.temporal_analysis and col.temporal_analysis.get('available'):
            tags += '<span class="column-tag temporal">TEMPORAL</span>'
        # Show FIBO classification if available (primary_tag like "money.amount")
        if col.semantic_info and col.semantic_info.get('primary_tag') and col.semantic_info.get('primary_tag') != 'unknown':
            fibo_tag = col.semantic_info.get('primary_tag')
            tags += f'<span class="column-tag fibo">{fibo_tag}</span>'
        elif col.statistics.semantic_type and col.statistics.semantic_type != 'unknown':
            # Fallback to generic semantic type if no FIBO tag
            tags += f'<span class="column-tag semantic">{col.statistics.semantic_type.upper()}</span>'

        # Stats
        stats = self._generate_column_stats(col)

        # Top values
        top_values = self._generate_top_values(col)

        # Determine if column has issues for filtering
        has_issues = bool(col.quality.issues) or score < 70 or col.quality.completeness < 80

        # Build data attributes for JS sorting/filtering
        data_attrs = f'data-name="{col.name}" data-type="{inferred_type}" data-quality="{score:.1f}" data-completeness="{col.quality.completeness:.1f}" data-issues="{str(has_issues).lower()}"'

        # Format completeness with appropriate precision
        completeness = col.quality.completeness
        if completeness == 100:
            completeness_str = "100% complete"
        elif completeness >= 99:
            completeness_str = f"{completeness:.2f}% complete"
        else:
            completeness_str = f"{completeness:.1f}% complete"

        # Build FIBO badge for mobile (visible next to name)
        fibo_badge_mobile = ''
        if col.semantic_info and col.semantic_info.get('primary_tag'):
            fibo_tag = col.semantic_info.get('primary_tag')
            # Get high-level category for icon
            category = fibo_tag.split('.')[0] if '.' in fibo_tag else fibo_tag
            category_icons = {
                'money': '💰', 'identifier': '🔑', 'party': '👤', 'datetime': '📅',
                'location': '📍', 'account': '🏦', 'transaction': '💸', 'product': '📦'
            }
            fibo_icon = category_icons.get(category.lower(), '🏛️')
            fibo_badge_mobile = f'<span class="fibo-badge-mobile" title="{fibo_tag}">{fibo_icon} {fibo_tag}</span>'

        return f'''
                                <div class="column-row" onclick="toggleColumnRow(this)" {data_attrs}>
                                    <div class="column-row-header">
                                        <span class="column-expand-icon">▶</span>
                                        <div class="column-type-icon {type_class}">{icon}</div>
                                        <div class="column-info">
                                            <div class="column-name-row">
                                                <span class="column-name">{col.name}</span>
                                                {fibo_badge_mobile}
                                            </div>
                                            <div class="column-type">{inferred_type} ({col.type_info.confidence*100:.0f}% confidence)</div>
                                        </div>
                                        <div class="column-quick-stats">
                                            <span>{completeness_str}</span>
                                            <span>{col.statistics.unique_percentage:.2f}% unique</span>
                                        </div>
                                        <div class="column-tags">
                                            {tags}
                                        </div>
                                        <span class="column-quality-score {score_class}" title="Overall Quality Score: Combines completeness, validity, consistency, and uniqueness metrics">Quality {score:.0f}%</span>
                                    </div>
                                    <div class="column-details">
                                        <div class="column-details-content">
                                            {stats}
                                            {top_values}
                                        </div>
                                    </div>
                                </div>'''

    def _generate_column_stats(self, col: ColumnProfile) -> str:
        """Generate statistics grid for a column."""
        stats = col.statistics

        # Check if unique count equals sample size (indicates likely higher actual cardinality)
        unique_display = f'{stats.unique_count:,}'
        if stats.sample_size and stats.unique_count >= stats.sample_size:
            unique_display = f'{stats.unique_count:,}*'  # Asterisk indicates capped

        stat_items = [
            ('Row Count', f'{stats.count:,}'),
            ('Null Count', f'{stats.null_count:,}'),
            ('Unique Values', unique_display),
        ]

        # Determine if column is numeric (only show numeric stats for numeric columns)
        is_numeric = col.type_info.inferred_type in ['integer', 'float', 'numeric', 'number']

        # Add type-specific stats - only show mean/std for numeric columns
        if is_numeric and stats.mean is not None:
            stat_items.append(('Mean', self._format_number(stats.mean)))
        if is_numeric and stats.median is not None:
            stat_items.append(('Median', self._format_number(stats.median)))
        if is_numeric and stats.std_dev is not None:
            stat_items.append(('Std Dev', self._format_number(stats.std_dev)))
        # Only show numeric Min/Max for numeric columns to avoid confusion
        # (string min/max shows alphabetical first/last which is misleading for numeric-looking IDs)
        if is_numeric:
            if stats.min_value is not None:
                stat_items.append(('Min', self._format_number(stats.min_value)))
            if stats.max_value is not None:
                stat_items.append(('Max', self._format_number(stats.max_value)))
        if stats.min_length is not None:
            stat_items.append(('Length', f'{stats.min_length}-{stats.max_length} chars'))
        if stats.pattern_samples:
            top_pattern = stats.pattern_samples[0] if stats.pattern_samples else {}
            stat_items.append(('Pattern', f"{top_pattern.get('pattern', 'N/A')[:15]}"))

        stats_html = ''
        for label, value in stat_items[:8]:  # Limit to 8 stats
            stats_html += f'''
                                            <div class="column-stat">
                                                <div class="column-stat-label">{label}</div>
                                                <div class="column-stat-value">{value}</div>
                                            </div>'''

        # Add sample size indicator if values were sampled for Top Values tracking
        if stats.sample_size and stats.sample_size < stats.count:
            stats_html += f'''
                                            <div class="column-stat" style="background: rgba(var(--info-color-rgb), 0.1); border: 1px dashed var(--info-color);" title="Unique values tracked for Top Values (memory optimization for high-cardinality columns)">
                                                <div class="column-stat-label" style="color: var(--info-color);">📊 Values Tracked</div>
                                                <div class="column-stat-value" style="font-size: 0.75em;">{stats.sample_size:,}</div>
                                            </div>'''

        return f'<div class="column-stats-grid">{stats_html}</div>'

    def _generate_top_values(self, col: ColumnProfile) -> str:
        """Generate top values section for a column."""
        top_values = col.statistics.top_values[:5]
        if not top_values:
            return ''

        items = ''
        for tv in top_values:
            value = str(tv.get('value', ''))[:20]
            count = tv.get('count', 0)
            items += f'''
                                                    <div class="top-value-item">
                                                        <span class="top-value-name">{value}</span>
                                                        <span class="top-value-count">{count:,}</span>
                                                    </div>'''

        sample_note = f"Based on {col.statistics.sample_size:,} row sample" if col.statistics.sample_size else "From full dataset"

        return f'''
                                            <div class="top-values-section">
                                                <div class="top-values-title">Top Values (from sample)</div>
                                                <div class="top-values-grid">{items}</div>
                                                <div class="sampled-note">{sample_note}</div>
                                            </div>'''

    def _generate_suggestions_accordion(self, profile: ProfileResult) -> str:
        """Generate validation suggestions accordion."""
        if not profile.suggested_validations:
            return ''

        suggestion_cards = ''
        for i, sugg in enumerate(profile.suggested_validations):
            yaml_content = self._generate_yaml_snippet(sugg)
            # Escape for HTML display
            yaml_display = yaml_content.replace('\\n', '\n')
            priority_class = 'high-priority' if sugg.confidence > 80 else ''

            # Extract field name from params if present
            field_name = sugg.params.get('field') or sugg.params.get('fields', [None])[0] if sugg.params.get('fields') else None
            field_display = f'<div class="suggestion-field">Field: <strong>{field_name}</strong></div>' if field_name else ''

            suggestion_cards += f'''
                            <div class="suggestion-card {priority_class}">
                                <div class="suggestion-header">
                                    <div class="suggestion-type">{sugg.validation_type}</div>
                                    <div class="suggestion-confidence">{sugg.confidence:.0f}% confidence</div>
                                </div>
                                {field_display}
                                <div class="suggestion-reason">{sugg.reason}</div>
                                <div class="suggestion-yaml-container">
                                    <pre class="suggestion-yaml">{yaml_display}</pre>
                                    <button class="copy-yaml-btn" onclick="copyYaml(`{yaml_content.replace('`', '\\`')}`)">📋 Copy</button>
                                </div>
                            </div>'''

        return f'''
                <div class="accordion" data-accordion="suggestions">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">💡</div>
                            <div>
                                <div class="accordion-title">Validation Suggestions</div>
                                <div class="accordion-subtitle">{len(profile.suggested_validations)} suggested checks</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{len(profile.suggested_validations)} suggestions</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>💡 Smart Suggestions:</strong> Based on the data profile, these validations are recommended.
                                Copy individual snippets or use the full config below.
                            </div>
                            {suggestion_cards}
                        </div>
                    </div>
                </div>'''

    def _generate_full_config_accordion(self, profile: ProfileResult) -> str:
        """Generate full validation configuration YAML accordion."""
        if not profile.generated_config_yaml:
            return ''

        # Escape HTML entities in YAML
        yaml_escaped = (profile.generated_config_yaml
                       .replace('&', '&amp;')
                       .replace('<', '&lt;')
                       .replace('>', '&gt;'))

        return f'''
                <div class="accordion" data-accordion="config" id="section-config">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">⚙️</div>
                            <div>
                                <div class="accordion-title">Full Validation Configuration</div>
                                <div class="accordion-subtitle">Ready-to-use YAML config file</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge good">Ready</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>📋 Complete Configuration:</strong> Copy this YAML and save it as a <code>.yaml</code> file
                                to run validations with DataK9 CLI: <code>python3 -m validation_framework.cli validate config.yaml</code>
                            </div>
                            <div class="full-config-container">
                                <div class="config-actions">
                                    <button class="copy-config-btn" onclick="copyFullConfig()">📋 Copy Full Config</button>
                                </div>
                                <pre class="full-config-yaml" id="fullConfigYaml">{yaml_escaped}</pre>
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_temporal_accordion(self, temporal_columns: List[ColumnProfile]) -> str:
        """Generate temporal analysis section - prominent display for time-series insights."""
        if not temporal_columns:
            return ''

        # Build content for all temporal columns
        temporal_items = ''

        # Get ML temporal patterns for consistent gap counts
        ml_temporal = {}
        if hasattr(self, 'profile') and hasattr(self.profile, 'ml_findings') and self.profile.ml_findings:
            ml_temporal = self.profile.ml_findings.get('temporal_patterns', {})

        for col in temporal_columns:
            analysis = col.temporal_analysis or {}
            date_range = analysis.get('date_range', {})
            frequency = analysis.get('frequency', {})
            gaps = analysis.get('gaps', {})
            trend = analysis.get('trend', {})

            # Two sources of gap data:
            # - large_gaps_count (ML): significant gaps (typically >24h) from sampled data
            # - gap_count (profiler): all detected gaps from sampled data
            ml_col_data = ml_temporal.get(col.name, {})
            significant_gaps = ml_col_data.get('large_gaps_count', 0)
            total_gaps = gaps.get('gap_count', 0)
            # Use significant gaps as primary metric (more meaningful for users)
            has_gaps = significant_gaps > 0 or total_gaps > 0
            display_gaps = significant_gaps if significant_gaps > 0 else total_gaps
            gap_label = "large gaps" if significant_gaps > 0 else "gaps"

            # Use column stats for true date range (from parquet metadata = all rows)
            # Fall back to analysis date_range if not available
            true_min = col.statistics.min_value if col.statistics else None
            true_max = col.statistics.max_value if col.statistics else None

            # Format dates - handle both string and datetime types
            if true_min and true_max:
                start_str = str(true_min)[:10] if true_min else 'N/A'
                end_str = str(true_max)[:10] if true_max else 'N/A'
            else:
                start_str = date_range.get('start', 'N/A')[:10] if date_range.get('start') else 'N/A'
                end_str = date_range.get('end', 'N/A')[:10] if date_range.get('end') else 'N/A'

            # Calculate span from ML findings (sample-based) for gap analysis context
            ml_span = ml_col_data.get('date_range', {}).get('span_days', date_range.get('span_days', 'N/A'))
            is_sampled = bool(ml_col_data)
            span_label = f"{ml_span} days (in sample)" if is_sampled and ml_span != 'N/A' else f"{ml_span} days"

            # Build gap analysis detail with both metrics if they differ
            # Also get largest gap from appropriate source
            ml_largest_gap_days = ml_col_data.get('largest_gap_days', None)
            profiler_largest_gap = gaps.get('largest_gap', None)

            # Use ML largest gap (in days) if available and significant gaps are shown
            # Otherwise use profiler largest gap (timedelta string)
            if significant_gaps > 0 and ml_largest_gap_days is not None:
                largest_gap_display = f"{ml_largest_gap_days} days" if isinstance(ml_largest_gap_days, (int, float)) else str(ml_largest_gap_days)
            else:
                largest_gap_display = str(profiler_largest_gap) if profiler_largest_gap else 'None'

            if significant_gaps > 0 and total_gaps > 0 and significant_gaps != total_gaps:
                gap_detail = f'{significant_gaps:,} large • {total_gaps:,} total'
                gap_explanation = "Significant gaps exceed the dynamically computed threshold; raw gaps reflect all intervals above expected frequency."
            elif significant_gaps > 0:
                gap_detail = f'{significant_gaps:,} {gap_label}'
                gap_explanation = "Significant gaps represent intervals exceeding the dynamically computed long-duration threshold."
            else:
                gap_detail = f'{display_gaps:,} {gap_label}'
                gap_explanation = "Raw gaps represent all timestamp intervals exceeding the expected event frequency for this dataset."

            temporal_items += f'''
                <div class="temporal-column-card" style="background: var(--card-bg); border-radius: 12px; padding: 20px; margin-bottom: 16px; border: 1px solid var(--border-color);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                        <h4 style="color: var(--text-primary); margin: 0; font-size: 1.1em;">📅 {col.name}</h4>
                        <span class="accordion-badge {'warning' if has_gaps else 'good'}">{display_gaps:,} {gap_label}</span>
                    </div>

                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px;">
                        <div class="temporal-stat">
                            <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 4px;">Date Range (all rows)</div>
                            <div style="color: var(--text-primary); font-weight: 500;">{start_str} → {end_str}</div>
                            <div style="color: var(--text-tertiary); font-size: 0.8em;">{span_label}</div>
                        </div>

                        <div class="temporal-stat">
                            <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 4px;">Detected Frequency</div>
                            <div style="color: var(--text-primary); font-weight: 500;">{frequency.get('inferred', 'Unknown')}</div>
                            <div style="color: var(--text-tertiary); font-size: 0.8em;">{frequency.get('confidence', 0)*100:.0f}% confidence</div>
                        </div>

                        <div class="temporal-stat">
                            <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 4px;">Trend Direction</div>
                            <div style="color: var(--text-primary); font-weight: 500;">{'📈' if trend.get('direction') == 'increasing' else '📉' if trend.get('direction') == 'decreasing' else '➡️'} {trend.get('direction', 'Unknown')}</div>
                            <div style="color: var(--text-tertiary); font-size: 0.8em;">{trend.get('strength', 'N/A')} strength</div>
                        </div>

                        <div class="temporal-stat">
                            <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 4px;">Gap Analysis (in sample)</div>
                            <div style="color: {'#f59e0b' if has_gaps else 'var(--text-primary)'}; font-weight: 500;">{gap_detail}</div>
                            <div style="color: var(--text-tertiary); font-size: 0.8em;">Largest: {largest_gap_display}</div>
                            <div style="color: var(--text-muted); font-size: 0.7em; margin-top: 4px; font-style: italic;">{gap_explanation}</div>
                            <details style="margin-top: 6px; font-size: 0.75em; color: var(--text-muted);">
                                <summary style="cursor: pointer;">What do these metrics mean?</summary>
                                <div style="margin-top: 4px; padding-left: 8px; border-left: 2px solid var(--border-color);">
                                    <div><strong>Significant gaps:</strong> Intervals exceeding the dynamically computed long-duration threshold (10× median frequency)</div>
                                    <div style="margin-top: 2px;"><strong>Raw gaps:</strong> All timestamp intervals exceeding the expected event frequency for this dataset</div>
                                    <div style="margin-top: 2px;"><strong>Activity timeline:</strong> Calendar days with zero recorded events after aggregation</div>
                                    <div style="margin-top: 2px;"><strong>Largest gap:</strong> Maximum interval detected, measured in days</div>
                                </div>
                            </details>
                        </div>
                    </div>
                </div>'''

        # Note about sampling for temporal analysis
        sample_note = ""
        if hasattr(self, 'profile') and hasattr(self.profile, 'ml_findings') and self.profile.ml_findings:
            sample_info = self.profile.ml_findings.get('sample_info', {})
            if sample_info.get('sampled', False):
                analyzed = sample_info.get('analyzed_rows', 0)
                sample_note = f'<div style="color: var(--text-tertiary); font-size: 0.8em; margin-top: 8px; font-style: italic;">ℹ️ Analysis based on {analyzed:,} sampled rows</div>'

        return f'''
        <div class="accordion open" data-accordion="temporal">
            <div class="accordion-header" onclick="toggleAccordion(this)">
                <div class="accordion-title-group">
                    <div class="accordion-icon" style="background: linear-gradient(135deg, #8b5cf6 0%, #6366f1 100%);">📅</div>
                    <div>
                        <div class="accordion-title">Temporal Analysis</div>
                        <div class="accordion-subtitle">Time series patterns and data freshness</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-badge good">{len(temporal_columns)} column(s)</span>
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-body">
                <div class="accordion-content">
                    <div class="hint-box" style="margin-bottom: 16px;">
                        💡 <strong>What this shows:</strong> Temporal analysis examines date/time columns to understand your data's time coverage,
                        detect missing periods (gaps), identify trends (increasing/decreasing activity over time), and infer the expected data frequency
                        (daily, weekly, monthly). This helps ensure data completeness and spot collection issues.
                        {sample_note}
                    </div>
                    {temporal_items}
                </div>
            </div>
        </div>'''

    def _generate_yaml_snippet(self, sugg) -> str:
        """Generate YAML snippet for a suggestion."""
        params_str = ''
        for key, value in sugg.params.items():
            if isinstance(value, list):
                params_str += f'\\n          {key}:'
                for item in value:
                    params_str += f'\\n            - "{item}"'
            else:
                params_str += f'\\n          {key}: {json.dumps(value)}'

        return f'''      - type: "{sugg.validation_type}"
        severity: "{sugg.severity}"{params_str if params_str else ""}'''

    # =========================================================================
    # NEW PRESENTATION IMPROVEMENTS
    # =========================================================================

    def _generate_executive_summary(self, profile: ProfileResult, pii_count: int, avg_completeness: float) -> str:
        """
        Generate the executive summary section with at-a-glance verdict.
        This provides immediate context for busy stakeholders.
        """
        # Calculate verdict
        score = profile.overall_quality_score
        issues_count = sum(len(col.quality.issues) for col in profile.columns)
        ml_issues = 0
        if profile.ml_findings:
            ml_issues = profile.ml_findings.get('summary', {}).get('total_issues', 0)

        # Determine verdict
        if score >= 90 and pii_count == 0 and issues_count < 3:
            verdict = "✅ Data Quality: EXCELLENT"
            verdict_class = "good"
            verdict_detail = "This dataset meets high quality standards with minimal issues detected."
        elif score >= 75 and pii_count <= 2:
            verdict = "🟡 Data Quality: GOOD with minor concerns"
            verdict_class = "warning"
            verdict_detail = f"Overall quality is acceptable. {issues_count} column issue(s) to review."
        elif score >= 60:
            verdict = "🟠 Data Quality: NEEDS ATTENTION"
            verdict_class = "warning"
            verdict_detail = f"Multiple data quality issues detected ({issues_count} issues). Review recommended."
        else:
            verdict = "🔴 Data Quality: CRITICAL ISSUES"
            verdict_class = "critical"
            verdict_detail = "Significant data quality problems. Immediate review required."

        # Priority actions based on issues
        actions = []

        # Check for PII
        if pii_count > 0:
            actions.append({
                'icon': '🔒',
                'text': f'Review {pii_count} column(s) with potential PII',
                'link': '#section-pii'
            })

        # Check completeness
        if avg_completeness < 90:
            sparse_cols = [col.name for col in profile.columns if col.quality.completeness < 80]
            actions.append({
                'icon': '📊',
                'text': f'{len(sparse_cols)} column(s) have significant missing data',
                'link': '#section-alerts'
            })

        # Check ML findings
        if ml_issues > 100:
            actions.append({
                'icon': '🧠',
                'text': f'{ml_issues:,} records flagged by ML analysis',
                'link': '#section-ml'
            })

        # Check for type issues
        type_issues = [col.name for col in profile.columns if col.quality.validity < 90]
        if type_issues:
            actions.append({
                'icon': '⚠️',
                'text': f'{len(type_issues)} column(s) have data type issues',
                'link': '#section-columns'
            })

        # Limit to top 3 actions
        actions = actions[:3]

        actions_html = ''
        if actions:
            for action in actions:
                actions_html += f'''
                <a href="{action['link']}" class="action-item">
                    <span class="action-icon">{action['icon']}</span>
                    <span class="action-text">{action['text']}</span>
                    <span class="action-arrow">→</span>
                </a>'''

        # Add export button
        export_btn = '''
        <button onclick="exportAnomalies()" class="export-btn" title="Export flagged records to CSV">
            📥 Export Issues
        </button>'''

        return f'''
        <section class="executive-summary">
            <div class="summary-verdict {verdict_class}">
                <div class="verdict-text">{verdict}</div>
                <div class="verdict-detail">{verdict_detail}</div>
            </div>
            <div class="summary-stats">
                <div class="stat-item">
                    <div class="stat-value">{profile.row_count:,}</div>
                    <div class="stat-label">Records</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{profile.column_count}</div>
                    <div class="stat-label">Columns</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{issues_count}</div>
                    <div class="stat-label">Issues</div>
                </div>
                <div class="stat-item">
                    <div class="stat-value">{ml_issues:,}</div>
                    <div class="stat-label">ML Flags</div>
                </div>
            </div>
            {f'<div class="priority-actions"><div class="actions-header">Priority Actions</div>{actions_html}</div>' if actions else ''}
            <div class="summary-tools">
                {export_btn}
            </div>
        </section>'''

    def _generate_column_heatmap(self, profile: ProfileResult) -> str:
        """
        Generate a column quality heatmap visualization.
        Shows all columns with color-coded quality scores at a glance.
        """
        cells = []
        for col in profile.columns:
            score = col.quality.overall_score
            if score >= 90:
                color_class = 'heatmap-good'
            elif score >= 70:
                color_class = 'heatmap-ok'
            elif score >= 50:
                color_class = 'heatmap-warning'
            else:
                color_class = 'heatmap-critical'

            # Truncate long names
            name = col.name[:12] + '...' if len(col.name) > 12 else col.name

            cells.append(f'''
            <div class="heatmap-cell {color_class}" title="{col.name}: {score:.0f}%">
                <span class="heatmap-name">{name}</span>
                <span class="heatmap-score">{score:.0f}</span>
            </div>''')

        return f'''
        <div class="column-heatmap">
            <div class="heatmap-header">
                <span class="heatmap-title">Column Quality Overview</span>
                <div class="heatmap-legend">
                    <span class="legend-item"><span class="legend-dot heatmap-good"></span>90+</span>
                    <span class="legend-item"><span class="legend-dot heatmap-ok"></span>70-89</span>
                    <span class="legend-item"><span class="legend-dot heatmap-warning"></span>50-69</span>
                    <span class="legend-item"><span class="legend-dot heatmap-critical"></span>&lt;50</span>
                </div>
            </div>
            <div class="heatmap-grid">
                {''.join(cells)}
            </div>
        </div>'''

    def _generate_column_search_controls(self) -> str:
        """Generate column search and filter controls."""
        return '''
        <div class="column-controls">
            <div class="search-box">
                <input type="text" id="columnSearch" placeholder="Search columns..."
                       onkeyup="filterColumns()" class="search-input">
                <span class="search-icon">🔍</span>
            </div>
            <div class="filter-buttons">
                <button class="filter-btn active" onclick="filterByType('all')">All</button>
                <button class="filter-btn" onclick="filterByType('issues')">⚠️ With Issues</button>
                <button class="filter-btn" onclick="filterByType('numeric')">Numeric</button>
                <button class="filter-btn" onclick="filterByType('string')">String</button>
                <button class="filter-btn" onclick="filterByType('date')">Date</button>
            </div>
            <div class="sort-controls">
                <select id="columnSort" onchange="sortColumns()" class="sort-select">
                    <option value="name">Sort by Name</option>
                    <option value="quality">Sort by Quality</option>
                    <option value="completeness">Sort by Completeness</option>
                    <option value="issues">Sort by Issues</option>
                </select>
            </div>
        </div>'''

    # Helper methods
    def _build_sample_rows_html(self, sample_rows: List[Dict], max_rows: int = 5) -> str:
        """
        Build mobile-friendly sample rows HTML using cards instead of tables.
        On mobile devices, tables can overflow; cards stack vertically.
        Shows up to 5 examples by default for better context.
        """
        if not sample_rows:
            return ''

        headers = list(sample_rows[0].keys()) if sample_rows else []
        if not headers:
            return ''

        # Build cards for each row - fully stacked vertical layout for mobile
        cards_html = ''
        for i, row in enumerate(sample_rows[:max_rows]):
            row_num = row.get('_row_number', row.get('row_index', i + 1))
            fields_html = ''
            for h in headers:
                if h.startswith('_'):  # Skip internal fields
                    continue
                val = str(row.get(h, ''))[:60]  # Truncate long values
                fields_html += f'''<div style="margin-bottom:6px;">
                    <div style="color:var(--text-tertiary);font-size:10px;text-transform:uppercase;letter-spacing:0.5px;">{h}</div>
                    <div style="font-size:12px;color:var(--text-primary);overflow-wrap:break-word;word-wrap:break-word;">{val}</div>
                </div>'''
            cards_html += f'''<div style="background:var(--card-bg);border:1px solid var(--border-color);border-radius:6px;padding:12px;margin-bottom:8px;max-width:100%;overflow:hidden;">
                <div style="color:var(--text-tertiary);font-size:10px;margin-bottom:8px;">Row #{row_num}</div>
                {fields_html}
            </div>'''

        return f'''<details style="margin-top:10px;" open>
            <summary style="cursor:pointer;color:#8b5cf6;font-size:13px;font-weight:500;">📋 View {len(sample_rows[:max_rows])} example records</summary>
            <div style="margin-top:10px;max-width:100%;overflow:hidden;">{cards_html}</div>
        </details>'''

    def _format_file_size(self, bytes: int) -> str:
        """Format file size for display."""
        if bytes >= 1024 * 1024 * 1024:
            return f"{bytes / (1024 * 1024 * 1024):.2f} GB"
        elif bytes >= 1024 * 1024:
            return f"{bytes / (1024 * 1024):.2f} MB"
        elif bytes >= 1024:
            return f"{bytes / 1024:.2f} KB"
        return f"{bytes} bytes"

    def _format_duration(self, seconds: float) -> str:
        """Format duration for display."""
        if seconds >= 3600:
            hours = int(seconds // 3600)
            mins = int((seconds % 3600) // 60)
            return f"{hours}h {mins}m"
        elif seconds >= 60:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        return f"{seconds:.1f}s"

    def _format_number(self, value: float) -> str:
        """Format large numbers for display."""
        import math
        if value is None:
            return 'N/A'
        # Handle infinity and NaN
        if math.isinf(value):
            return "∞" if value > 0 else "-∞"
        if math.isnan(value):
            return "N/A"
        abs_val = abs(value)
        # Handle extreme values (data corruption / overflow)
        if abs_val >= 1e100:
            return "overflow"
        if abs_val >= 1e9:
            return f"{value/1e9:.2f}B"
        elif abs_val >= 1e6:
            return f"{value/1e6:.2f}M"
        elif abs_val >= 1e3:
            return f"{value/1e3:.2f}K"
        return f"{value:.2f}"

    def _count_data_types(self, columns: List[ColumnProfile]) -> Dict[str, int]:
        """Count columns by inferred data type."""
        counts = {}
        for col in columns:
            dtype = col.type_info.inferred_type
            # Normalize type names
            if dtype in ['integer', 'int']:
                dtype = 'Integer'
            elif dtype in ['float', 'number', 'decimal']:
                dtype = 'Float'
            elif dtype in ['string', 'str', 'text']:
                dtype = 'String'
            elif dtype in ['date', 'datetime', 'timestamp']:
                dtype = 'Date'
            elif dtype in ['boolean', 'bool']:
                dtype = 'Boolean'
            else:
                dtype = dtype.capitalize()

            counts[dtype] = counts.get(dtype, 0) + 1
        return counts

    def _get_pii_columns(self, columns: List[ColumnProfile]) -> List[Dict]:
        """Get list of columns with PII detected."""
        pii_cols = []
        for col in columns:
            if col.pii_info and col.pii_info.get('detected'):
                pii_cols.append({
                    'name': col.name,
                    'risk_score': col.pii_info.get('risk_score', 0),
                    'pii_types': col.pii_info.get('pii_types', []),
                    'frameworks': col.pii_info.get('regulatory_frameworks', [])
                })
        return pii_cols

    def _get_categorical_columns(self, columns: List[ColumnProfile]) -> List[Dict]:
        """Get categorical columns (low cardinality strings) for word cloud."""
        categorical = []
        for col in columns:
            # Consider categorical if string type with low unique percentage or few unique values
            if col.type_info.inferred_type in ['string', 'str', 'text']:
                if col.statistics.unique_count <= 100 or col.statistics.unique_percentage < 1:
                    categorical.append({
                        'name': col.name,
                        'top_values': col.statistics.top_values,
                        'unique_count': col.statistics.unique_count
                    })
        return sorted(categorical, key=lambda x: x['unique_count'])[:5]

    def _get_quality_status(self, score: float) -> Dict[str, str]:
        """Get quality status text and class."""
        if score >= 90:
            return {'text': 'Excellent - data is high quality', 'class': 'good'}
        elif score >= 80:
            return {'text': 'Good - minor issues detected', 'class': 'good'}
        elif score >= 70:
            return {'text': 'Fair - some attention needed', 'class': 'warning'}
        elif score >= 60:
            return {'text': 'Fair - consistency issues detected', 'class': 'warning'}
        else:
            return {'text': 'Needs attention - significant issues', 'class': 'critical'}

    def _get_sampling_info(self, columns: List[ColumnProfile]) -> Dict:
        """Get sampling information from columns."""
        sample_sizes = [col.statistics.sample_size for col in columns if col.statistics.sample_size]
        if sample_sizes:
            typical = max(set(sample_sizes), key=sample_sizes.count)
            total_rows = columns[0].statistics.count if columns else 0
            pct = (typical / total_rows * 100) if total_rows else 0
            return {'typical_sample': typical, 'sample_pct': pct}
        return {'typical_sample': 0, 'sample_pct': 0}

    def _generate_sampling_summary_enhanced(self, profile: ProfileResult, sampling_info: Dict, insights: Dict) -> str:
        """
        Generate enhanced sampling summary with insight engine's explanation.

        Uses the 50k sampling policy explanation from the insight engine.
        """
        insight_sampling = insights.get('sampling_info', {})
        sample_used = insight_sampling.get('sample_used', False)
        sample_size = insight_sampling.get('sample_size', 0)
        sample_fraction = insight_sampling.get('sample_fraction', 0)
        total_rows = insight_sampling.get('total_rows', profile.row_count)

        # Get metrics lists
        full_metrics = insight_sampling.get('full_dataset_metrics', [])
        sampled_metrics = insight_sampling.get('sampled_metrics', [])

        # Format metrics lists for display
        full_metrics_str = ', '.join(full_metrics) if full_metrics else 'Row counts, null counts, metadata'
        sampled_metrics_str = ', '.join(sampled_metrics) if sampled_metrics else 'Statistics, patterns, ML analysis'

        # Sampling explanation from insight engine
        sampling_explanation = insights.get('sampling_explanation', '')
        sampling_overview = insights.get('sampling_overview', '')

        if sample_used:
            sample_display = f"{sample_size:,} rows ({sample_fraction:.2%} of dataset)"
            sample_note = f"{sample_size:,} sample used for statistical analysis"
        else:
            sample_display = f"Full dataset ({total_rows:,} rows)"
            sample_note = "Full dataset analyzed"

        return f'''
        <section class="sampling-bar" style="flex-direction: column; align-items: stretch;">
            <div style="display: flex; flex-wrap: wrap; gap: 24px; align-items: center;">
                <div class="sampling-bar-title">🔬 Analysis Methodology</div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Dataset Size</span>
                    <span class="sampling-stat-value highlight">{total_rows:,} rows</span>
                </div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Sample Used</span>
                    <span class="sampling-stat-value">{sample_display}</span>
                </div>
                <div class="sampling-stat">
                    <span class="sampling-stat-label">Method</span>
                    <span class="sampling-stat-value">{sample_note}</span>
                </div>
            </div>
            <details style="margin-top: 12px; margin-bottom: 0;">
                <summary style="cursor: pointer; color: var(--text-secondary); font-size: 0.9em; padding: 8px 0;">ℹ️ About the sampling methodology...</summary>
                <div class="hint-box" style="margin-top: 8px; margin-bottom: 0; border-left-color: var(--info);">
                    <p style="margin: 0 0 12px 0;"><strong>📊 Sampling Policy:</strong></p>
                    <p style="margin: 0 0 12px 0; color: var(--text-secondary);">{sampling_explanation.replace(chr(10), '<br>')}</p>

                    <p style="margin: 12px 0 8px 0;"><strong>Full Dataset Metrics:</strong> <span style="color: var(--good);">{full_metrics_str}</span></p>
                    <p style="margin: 0;"><strong>{'Sampled' if sample_used else 'All'} Metrics:</strong> <span style="color: var(--accent);">{sampled_metrics_str}</span></p>
                </div>
            </details>
        </section>'''

    def _generate_sampling_banner_v2(self, profile: ProfileResult, insights: Dict) -> str:
        """
        Generate v2 sampling coverage banner with visual progress and method breakdown.

        New design per redesign plan - prominent, clear visibility of what's sampled vs full scan.
        """
        # Try to get sampling info from insights first, then from ml_findings
        insight_sampling = insights.get('sampling_info', {})

        # If no sampling_info in insights, check ml_findings.sample_info
        if not insight_sampling and profile.ml_findings:
            ml_sample_info = profile.ml_findings.get('sample_info', {})
            sample_used = ml_sample_info.get('sampled', ml_sample_info.get('sample_percentage', 100) < 100)
            sample_size = ml_sample_info.get('analyzed_rows', 0)
            total_rows = ml_sample_info.get('original_rows', profile.row_count)
        else:
            sample_used = insight_sampling.get('sample_used', False)
            sample_size = insight_sampling.get('sample_size', 0)
            total_rows = insight_sampling.get('total_rows', profile.row_count)

        # If we still don't have sample info, derive from profile
        if total_rows == 0:
            total_rows = profile.row_count

        # Calculate coverage percentage with meaningful display
        if sample_used and total_rows > 0:
            coverage_pct = min((sample_size / total_rows) * 100, 100)
            # For very small percentages, show more precision or use descriptive text
            if coverage_pct < 0.1:
                coverage_display = f"{sample_size:,} rows sampled from {total_rows:,} total"
            elif coverage_pct < 1:
                coverage_display = f"{coverage_pct:.2f}% sampled ({sample_size:,} of {total_rows:,} rows)"
            else:
                coverage_display = f"{coverage_pct:.1f}% sampled ({sample_size:,} of {total_rows:,} rows)"
        else:
            coverage_pct = 100
            coverage_display = f"100% analyzed ({total_rows:,} rows)"

        # Analysis methods and their coverage
        full_scan_items = [
            ("✓", "Schema & Types", "Full scan"),
            ("✓", "Null Counts", "Full scan"),
            ("✓", "Row Count", "Full scan"),
        ]

        if sample_used:
            sampled_items = [
                ("○", "Outlier Detection", "Sampled"),
                ("○", "Pattern Analysis", "Sampled"),
                ("○", "ML Analysis", "Sampled"),
                ("○", "Correlation", "Sampled"),
            ]
        else:
            sampled_items = [
                ("✓", "Outlier Detection", "Full scan"),
                ("✓", "Pattern Analysis", "Full scan"),
                ("✓", "ML Analysis", "Full scan"),
                ("✓", "Correlation", "Full scan"),
            ]

        # Build methods HTML
        methods_html = ""
        for icon, name, status in full_scan_items + sampled_items:
            method_class = "full" if "Full" in status else "sampled"
            methods_html += f'''
                <div class="sampling-method {method_class}">
                    <span class="sampling-method-icon">{icon}</span>
                    <span class="sampling-method-text"><strong>{name}</strong>: {status}</span>
                </div>'''

        return f'''
        <div class="sampling-banner">
            <div class="sampling-banner-header">
                <span class="sampling-banner-icon">📊</span>
                <span class="sampling-banner-title">Analysis Coverage</span>
            </div>

            <div class="sampling-progress-container">
                <div class="sampling-progress-bar">
                    <div class="sampling-progress-fill" style="width: {coverage_pct}%;"></div>
                </div>
                <div class="sampling-progress-label">
                    <strong>{coverage_display}</strong>
                </div>
            </div>

            <div class="sampling-methods-grid">
                {methods_html}
            </div>

            {self._generate_sample_size_explanation(sample_used, sample_size, total_rows)}
        </div>'''

    def _generate_sample_size_explanation(self, sample_used: bool, sample_size: int, total_rows: int) -> str:
        """
        Generate a collapsible technical explanation of sample size statistical sufficiency.
        Only shown when sampling is used.
        """
        if not sample_used or sample_size >= total_rows:
            return ''

        # Calculate statistical properties
        # For 95% confidence and 1% margin of error, required sample size is:
        # n = (Z^2 * p * (1-p)) / E^2 where Z=1.96, p=0.5, E=0.01 gives ~9,604
        # For 99% confidence and 1% margin of error: Z=2.576 gives ~16,587
        # For detecting 1% events with 95% confidence: ~38,415

        confidence_95_moe_1pct = "9,604"  # Required for 95% CI, 1% MoE
        confidence_99_moe_1pct = "16,587"  # Required for 99% CI, 1% MoE
        detect_1pct_events = "38,415"  # Required to detect 1% occurrence

        margin_of_error = 100 * (1.96 * 0.5) / (sample_size ** 0.5)  # Simplified estimate

        return f'''
            <details class="sample-size-explanation">
                <summary class="sample-size-summary">
                    <span class="sample-size-icon">📐</span>
                    Why is {sample_size:,} rows statistically sufficient?
                </summary>
                <div class="sample-size-content">
                    <p><strong>Statistical sampling theory</strong> tells us that sample size, not population size,
                    determines accuracy. Here's why {sample_size:,} rows provides reliable insights:</p>

                    <div class="sample-size-stats">
                        <div class="sample-stat">
                            <span class="sample-stat-value">{margin_of_error:.2f}%</span>
                            <span class="sample-stat-label">Margin of Error (95% CI)</span>
                        </div>
                        <div class="sample-stat">
                            <span class="sample-stat-value">&lt; 0.1%</span>
                            <span class="sample-stat-label">Events Detectable</span>
                        </div>
                        <div class="sample-stat">
                            <span class="sample-stat-value">~{sample_size // 10000}x</span>
                            <span class="sample-stat-label">Above Minimum Required</span>
                        </div>
                    </div>

                    <div class="sample-size-detail">
                        <h4>Sample Size Requirements</h4>
                        <table class="sample-size-table">
                            <tr>
                                <th>Analysis Goal</th>
                                <th>Required Sample</th>
                                <th>Your Sample</th>
                            </tr>
                            <tr>
                                <td>95% confidence, 1% margin of error</td>
                                <td>{confidence_95_moe_1pct}</td>
                                <td class="value-highlight">✓ {sample_size:,}</td>
                            </tr>
                            <tr>
                                <td>99% confidence, 1% margin of error</td>
                                <td>{confidence_99_moe_1pct}</td>
                                <td class="value-highlight">✓ {sample_size:,}</td>
                            </tr>
                            <tr>
                                <td>Detect events occurring in 1%</td>
                                <td>{detect_1pct_events}</td>
                                <td class="value-highlight">✓ {sample_size:,}</td>
                            </tr>
                        </table>
                    </div>

                    <p class="sample-size-note"><strong>Key insight:</strong> For a population of any size (even billions),
                    a properly randomized sample of {sample_size:,} provides the same statistical power.
                    The Central Limit Theorem ensures sample statistics converge to population parameters
                    regardless of population size.</p>
                </div>
            </details>'''

    def _generate_metrics_dashboard_v2(self, profile: ProfileResult,
                                        avg_completeness: float, avg_validity: float,
                                        avg_consistency: float, avg_uniqueness: float,
                                        type_counts: Dict[str, int]) -> str:
        """
        Generate v2 metrics dashboard with 3-row widget grid.

        Row 1: Core Numbers (rows, columns, quality, processing time)
        Row 2: Quality Dimensions (completeness, validity, consistency, uniqueness)
        Row 3: Field Types (numeric, text, dates, categorical)
        """
        # Format values
        processing_time = self._format_duration(profile.processing_time_seconds)

        # Quality score status
        quality_score = profile.overall_quality_score
        quality_class = "good" if quality_score >= 80 else "warning" if quality_score >= 60 else "critical"

        # Type counts with defaults (keys are capitalized by _count_data_types)
        numeric_count = type_counts.get('Integer', 0) + type_counts.get('Float', 0)
        text_count = type_counts.get('String', 0)
        date_count = type_counts.get('Date', 0) + type_counts.get('Datetime', 0)
        categorical_count = sum(1 for col in profile.columns
                                if col.statistics.cardinality < 0.1 and col.type_info.inferred_type in ['string', 'String'])

        # Helper for quality gauge SVG
        def gauge_svg(pct: float, color: str) -> str:
            circumference = 2 * 3.14159 * 25
            offset = circumference - (pct / 100) * circumference
            return f'''
                <svg width="60" height="60" viewBox="0 0 60 60">
                    <circle cx="30" cy="30" r="25" fill="none" stroke="rgba(148,163,184,0.1)" stroke-width="5"/>
                    <circle cx="30" cy="30" r="25" fill="none" stroke="{color}" stroke-width="5"
                            stroke-dasharray="{circumference}" stroke-dashoffset="{offset}"
                            stroke-linecap="round"/>
                </svg>'''

        # Status text helpers
        def completeness_text(pct: float) -> tuple:
            if pct >= 99.9: return ("No missing values", "good")
            if pct >= 95: return ("Mostly complete", "good")
            if pct >= 80: return ("Some gaps", "warning")
            return ("Significant gaps", "critical")

        def validity_text(pct: float) -> tuple:
            if pct >= 95: return ("Formats valid", "good")
            if pct >= 80: return ("Minor issues", "warning")
            return ("Format problems", "critical")

        def consistency_text(pct: float) -> tuple:
            if pct >= 80: return ("Consistent", "good")
            if pct >= 50: return ("Variable", "warning")
            return ("Inconsistent", "critical")

        def uniqueness_text(pct: float) -> tuple:
            if pct >= 50: return ("High cardinality", "good")
            if pct >= 10: return ("Moderate", "warning")
            return ("Low cardinality", "info")

        comp_text, comp_class = completeness_text(avg_completeness)
        valid_text, valid_class = validity_text(avg_validity)
        consist_text, consist_class = consistency_text(avg_consistency)
        unique_text, unique_class = uniqueness_text(avg_uniqueness)

        return f'''
        <div class="metrics-dashboard">
            <!-- Row 1: Core Numbers -->
            <div class="metrics-row core">
                <div class="metric-widget">
                    <div class="metric-widget-label">Rows Analyzed</div>
                    <div class="metric-widget-value">{profile.row_count:,}</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-widget-label">Columns</div>
                    <div class="metric-widget-value">{profile.column_count}</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-widget-label">Quality Score</div>
                    <div class="metric-widget-value" style="color: var(--{quality_class});">{quality_score:.0f}%</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-widget-label">Processing</div>
                    <div class="metric-widget-value">{processing_time}</div>
                </div>
            </div>

            <!-- Row 2: Quality Dimensions -->
            <div class="metrics-row quality">
                <div class="metric-widget">
                    <div class="metric-gauge">
                        {gauge_svg(avg_completeness, 'var(--good)' if avg_completeness >= 95 else 'var(--warning)' if avg_completeness >= 80 else 'var(--critical)')}
                        <span class="metric-gauge-value">{avg_completeness:.0f}%</span>
                    </div>
                    <div class="metric-widget-label">Completeness</div>
                    <div class="metric-widget-trend {comp_class}">{comp_text}</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-gauge">
                        {gauge_svg(avg_validity, 'var(--good)' if avg_validity >= 95 else 'var(--warning)' if avg_validity >= 80 else 'var(--critical)')}
                        <span class="metric-gauge-value">{avg_validity:.0f}%</span>
                    </div>
                    <div class="metric-widget-label">Validity</div>
                    <div class="metric-widget-trend {valid_class}">{valid_text}</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-gauge">
                        {gauge_svg(avg_consistency, 'var(--good)' if avg_consistency >= 80 else 'var(--warning)' if avg_consistency >= 50 else 'var(--critical)')}
                        <span class="metric-gauge-value">{avg_consistency:.0f}%</span>
                    </div>
                    <div class="metric-widget-label">Consistency</div>
                    <div class="metric-widget-trend {consist_class}">{consist_text}</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-gauge">
                        {gauge_svg(avg_uniqueness, 'var(--info)')}
                        <span class="metric-gauge-value">{avg_uniqueness:.0f}%</span>
                    </div>
                    <div class="metric-widget-label">Uniqueness</div>
                    <div class="metric-widget-trend {unique_class}">{unique_text}</div>
                </div>
            </div>

            <!-- Row 3: Field Types -->
            <div class="metrics-row types">
                <div class="metric-widget">
                    <div class="metric-widget-value" style="color: #60a5fa;">{numeric_count}</div>
                    <div class="metric-widget-label">Numeric</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-widget-value" style="color: #a78bfa;">{text_count}</div>
                    <div class="metric-widget-label">Text</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-widget-value" style="color: #fbbf24;">{date_count}</div>
                    <div class="metric-widget-label">Dates</div>
                </div>
                <div class="metric-widget">
                    <div class="metric-widget-value" style="color: #34d399;">{categorical_count}</div>
                    <div class="metric-widget-label">Categorical</div>
                </div>
            </div>
        </div>'''

    def _generate_fibo_summary(self, columns: List) -> str:
        """
        Generate FIBO semantic categories summary widget.

        Shows all FIBO categories detected in the data with column counts.
        """
        # Collect FIBO categories from columns
        fibo_categories = {}
        for col in columns:
            if col.semantic_info and col.semantic_info.get('primary_tag'):
                tag = col.semantic_info.get('primary_tag', 'unknown')
                fibo_source = col.semantic_info.get('fibo_source', '')
                confidence = col.semantic_info.get('confidence', 0)

                # Group by high-level category (first part of tag)
                category = tag.split('.')[0] if '.' in tag else tag

                if category not in fibo_categories:
                    fibo_categories[category] = {
                        'columns': [],
                        'tags': set(),
                        'fibo_sources': set()
                    }

                fibo_categories[category]['columns'].append(col.name)
                fibo_categories[category]['tags'].add(tag)
                if fibo_source:
                    fibo_categories[category]['fibo_sources'].add(fibo_source)

        if not fibo_categories:
            return ''

        # Build category chips HTML
        chips_html = ''
        category_icons = {
            'money': '💰',
            'identifier': '🔑',
            'party': '👤',
            'datetime': '📅',
            'location': '📍',
            'account': '🏦',
            'transaction': '💸',
            'product': '📦',
            'unknown': '❓'
        }

        for category, data in sorted(fibo_categories.items(), key=lambda x: -len(x[1]['columns'])):
            icon = category_icons.get(category.lower(), '📋')
            col_count = len(data['columns'])
            tags_display = ', '.join(sorted(data['tags']))
            fibo_display = ', '.join(sorted(data['fibo_sources']))[:50]

            chips_html += f'''
                <div class="fibo-category-chip" title="{fibo_display}">
                    <span class="fibo-icon">{icon}</span>
                    <span class="fibo-label">{category.title()}</span>
                    <span class="fibo-count">{col_count}</span>
                    <span class="fibo-tags">{tags_display}</span>
                </div>'''

        # Count only columns that are NOT in the 'unknown' category
        # 'unknown' means the classifier couldn't determine a semantic type, so it's not "mapped"
        total_mapped = sum(len(d['columns']) for cat, d in fibo_categories.items() if cat.lower() != 'unknown')
        unknown_count = len(fibo_categories.get('unknown', {}).get('columns', []))

        return f'''
        <div class="fibo-summary">
            <div class="fibo-summary-header">
                <span class="fibo-summary-icon">🏛️</span>
                <span class="fibo-summary-title">Semantic Classification (FIBO)</span>
                <span class="fibo-summary-count">{total_mapped} of {len(columns)} columns classified{f" ({unknown_count} unknown)" if unknown_count > 0 else ""}</span>
            </div>
            <div class="fibo-categories-grid">
                {chips_html}
            </div>
        </div>'''

    def _generate_key_insights_section(self, insights: Dict) -> str:
        """
        Generate key insights section from insight engine output.

        Displays executive summary and top findings with severity badges.
        """
        exec_summary = insights.get('executive_summary', {})
        detailed_sections = insights.get('detailed_sections', {})

        if not exec_summary:
            return ''

        # Quality tier info
        quality_tier = exec_summary.get('quality_tier', {})
        tier_name = quality_tier.get('label', 'Unknown')
        tier_description = quality_tier.get('description', '')

        # Key findings
        key_findings = exec_summary.get('key_findings', [])

        # Build findings HTML
        findings_html = ''
        if key_findings:
            for finding in key_findings:
                severity = finding.get('severity', 'medium')
                category = finding.get('category', 'general')
                text = finding.get('text', '')

                # Map severity to colors
                severity_colors = {
                    'critical': ('var(--critical)', 'var(--critical-soft)'),
                    'high': ('var(--warning)', 'var(--warning-soft)'),
                    'medium': ('var(--info)', 'var(--info-soft)'),
                    'low': ('var(--text-secondary)', 'rgba(100,116,139,0.1)'),
                    'info': ('var(--text-muted)', 'rgba(100,116,139,0.08)'),
                }
                color, bg_color = severity_colors.get(severity, severity_colors['medium'])

                # Category icon mapping
                category_icons = {
                    'overall_quality': '📊',
                    'pii': '🔒',
                    'outliers': '📈',
                    'authenticity': '🔍',
                    'label_quality': '⚖️',
                    'temporal': '📅',
                    'cross_column': '🔗',
                    'completeness': '📋',
                    'validity': '✅',
                    'ml_analysis': '🧠',
                }
                icon = category_icons.get(category, '•')

                # Convert markdown bold to HTML
                text_html = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)

                findings_html += f'''
                <div style="display: flex; align-items: flex-start; gap: 12px; padding: 12px 16px; background: {bg_color}; border-radius: 8px; border-left: 3px solid {color}; margin-bottom: 8px;">
                    <span style="font-size: 1.2em;">{icon}</span>
                    <div style="flex: 1;">
                        <span style="display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.75em; font-weight: 600; text-transform: uppercase; background: {color}; color: var(--bg-main); margin-bottom: 4px;">{severity}</span>
                        <p style="margin: 4px 0 0 0; color: var(--text-primary); line-height: 1.5;">{text_html}</p>
                    </div>
                </div>'''

        # Build detailed sections HTML (collapsible)
        sections_html = ''
        if detailed_sections:
            for category, section_data in detailed_sections.items():
                header = section_data.get('header', category.replace('_', ' ').title())
                issues = section_data.get('issues', [])

                if not issues:
                    continue

                # Category styling
                category_styles = {
                    'pii': ('🔒', 'var(--critical)', 'Privacy & PII'),
                    'outliers': ('📈', 'var(--warning)', 'Outlier Analysis'),
                    'authenticity': ('🔍', 'var(--info)', 'Data Authenticity'),
                    'label_quality': ('⚖️', 'var(--warning)', 'Label Quality'),
                    'temporal': ('📅', 'var(--accent)', 'Temporal Analysis'),
                    'cross_column': ('🔗', 'var(--info)', 'Cross-Column'),
                    'completeness': ('📋', 'var(--warning)', 'Completeness'),
                    'validity': ('✅', 'var(--good)', 'Validity'),
                    'ml_analysis': ('🧠', 'var(--primary)', 'ML Analysis'),
                }
                icon, color, display_name = category_styles.get(category, ('•', 'var(--text-secondary)', header))

                issues_list = ''
                for issue in issues[:5]:  # Limit to top 5 per category
                    issue_text = issue.get('text', issue.get('id', 'Unknown issue'))
                    issue_severity = issue.get('severity', 'medium')
                    # Convert markdown
                    issue_text_html = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', issue_text)
                    issues_list += f'''<li style="margin-bottom: 8px; color: var(--text-secondary);">{issue_text_html}</li>'''

                sections_html += f'''
                <details style="margin-bottom: 12px;">
                    <summary style="cursor: pointer; padding: 12px 16px; background: var(--bg-card); border-radius: 8px; border-left: 3px solid {color}; display: flex; align-items: center; gap: 12px;">
                        <span style="font-size: 1.2em;">{icon}</span>
                        <span style="font-weight: 600; color: var(--text-primary);">{display_name}</span>
                        <span style="margin-left: auto; font-size: 0.85em; color: var(--text-muted);">{len(issues)} finding(s)</span>
                    </summary>
                    <div style="padding: 12px 16px 12px 32px; background: var(--bg-elevated); border-radius: 0 0 8px 8px;">
                        <ul style="margin: 0; padding-left: 20px; list-style-type: disc;">
                            {issues_list}
                        </ul>
                    </div>
                </details>'''

        return f'''
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- KEY INSIGHTS - Generated by Insight Engine                      -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #065f46 0%, #064e3b 100%); border-radius: 8px; border-left: 4px solid #10b981;">
            <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">💡 KEY INSIGHTS</h2>
            <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #a7f3d0;">Automated analysis findings and recommendations</p>
        </div>

        <section style="padding: 20px; background: var(--bg-card); border-radius: 12px; margin-bottom: 24px;">
            <!-- Quality Tier Badge -->
            <div style="display: flex; align-items: center; gap: 16px; margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid var(--border-subtle);">
                <div style="padding: 8px 16px; background: var(--accent-gradient); border-radius: 8px;">
                    <span style="font-weight: 700; font-size: 1.1em; color: white;">{tier_name}</span>
                </div>
                <span style="color: var(--text-secondary); font-size: 0.95em;">{tier_description}</span>
            </div>

            <!-- Key Findings -->
            <h3 style="margin: 0 0 16px 0; font-size: 1em; color: var(--text-primary); font-weight: 600;">Top Findings</h3>
            {findings_html if findings_html else '<p style="color: var(--text-muted);">No significant issues detected.</p>'}

            <!-- Detailed Sections (Collapsible) -->
            {f'<h3 style="margin: 24px 0 16px 0; font-size: 1em; color: var(--text-primary); font-weight: 600;">Detailed Analysis</h3>' + sections_html if sections_html else ''}
        </section>'''
