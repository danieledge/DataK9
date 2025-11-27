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
            <button class="nav-btn" data-section="alerts">Alerts</button>
            <button class="nav-btn" data-section="ml">ML Analysis</button>
            <button class="nav-btn" data-section="columns">Columns</button>
            <button class="nav-btn" data-section="config">Config</button>
        </div>
    </nav>

    <main class="page">
        <!-- EXECUTIVE SUMMARY - At-a-Glance Verdict -->
        {self._generate_executive_summary(profile, pii_count, avg_completeness)}

        <!-- Page Header -->
        <section class="page-header" id="section-summary">
            <div class="page-title-block">
                <h1>Data Quality & Completeness</h1>
                <p>Snapshot of overall health, completeness and key risks across this dataset.</p>
            </div>
        </section>

        <!-- KPI Belt -->
        <section class="kpi-belt">
            <div class="kpi-card">
                <div class="kpi-label">Overall Quality</div>
                <div class="kpi-value">{profile.overall_quality_score:.1f}%</div>
                <div class="kpi-trend {quality_status['class']}">{quality_status['text']}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Completeness</div>
                <div class="kpi-value">{avg_completeness:.1f}%</div>
                <div class="kpi-trend {'good' if avg_completeness >= 95 else 'warning'}">{'▲ Excellent - minimal nulls' if avg_completeness >= 95 else 'Some missing values detected'}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Privacy Risk</div>
                <div class="kpi-value {'critical' if max_pii_risk > 50 else ''}">{max_pii_risk}</div>
                <div class="kpi-trend {'critical' if max_pii_risk > 50 else 'good'}">{'⚠ ' + str(pii_count) + ' PII columns detected' if pii_count > 0 else '✓ No PII detected'}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Columns Analyzed</div>
                <div class="kpi-value">{profile.column_count}</div>
                <div class="kpi-trend good">All columns profiled</div>
            </div>
        </section>

        <!-- Sampling Summary (Enhanced with Insight Engine) -->
        {self._generate_sampling_summary_enhanced(profile, sampling_info, insights)}

        <!-- Key Insights from Insight Engine -->
        {self._generate_key_insights_section(insights)}

        <!-- Data Quality Alerts (if any critical issues) -->
        {self._generate_quality_alerts(profile)}

        <!-- FIBO Semantic Analysis Section -->
        {self._generate_fibo_section(profile)}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- SECTION: DATA FOUNDATION - Understanding the baseline data      -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #1e3a5f 0%, #0d1f3c 100%); border-radius: 8px; border-left: 4px solid #3b82f6;">
            <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">📊 DATA FOUNDATION</h2>
            <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #94a3b8;">Schema, quality metrics, and value distributions</p>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <!-- Overview Accordion -->
                {self._generate_overview_accordion(profile, type_counts, avg_completeness, avg_validity, avg_consistency, avg_uniqueness)}

                <!-- Quality Metrics Accordion -->
                {self._generate_quality_accordion(profile)}

                <!-- Value Distribution Accordion -->
                {self._generate_distribution_accordion(profile, categorical_columns)}
            </div>
        </div>

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- SECTION: INTELLIGENT ANALYSIS - ML and pattern detection        -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" id="section-ml" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #4c1d95 0%, #2e1065 100%); border-radius: 8px; border-left: 4px solid #8b5cf6;">
            <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">🧠 INTELLIGENT ANALYSIS</h2>
            <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #c4b5fd;">Machine learning anomaly detection and pattern analysis</p>
        </div>

        <!-- ML Analysis Section (if ML analysis was run) -->
        {self._generate_ml_section(profile.ml_findings) if profile.ml_findings else ''}

        <!-- Advanced Visualizations Section -->
        {self._generate_advanced_visualizations(profile.ml_findings) if profile.ml_findings and profile.ml_findings.get('visualizations') else ''}

        <!-- Temporal Analysis Accordion -->
        {self._generate_temporal_accordion(temporal_columns) if temporal_columns else ''}

        <!-- PII Risk Section (if PII detected) -->
        {self._generate_pii_section(pii_columns) if pii_count > 0 else ''}

        <!-- Validation Suggestions Accordion -->
        <div class="layout-grid">
            <div class="main-column">
                {self._generate_suggestions_accordion(profile)}
            </div>
        </div>

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- SECTION: REFERENCE - Detailed drill-down and config             -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #374151 0%, #1f2937 100%); border-radius: 8px; border-left: 4px solid #6b7280;">
            <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">📚 REFERENCE</h2>
            <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #9ca3af;">Detailed column information and configuration</p>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <!-- Column Explorer Accordion -->
                {self._generate_column_explorer(profile)}

                <!-- Full Validation Config Accordion (LAST - reference material) -->
                {self._generate_full_config_accordion(profile)}
            </div>
        </div>
    </main>

    <script>
{self._get_javascript(profile, type_counts, categorical_columns)}
    </script>
</body>
</html>'''

        return html

    def _get_css(self) -> str:
        """Return the CSS styles."""
        return '''        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg-main: #050713;
            --bg-elevated: #0b1020;
            --bg-card: #0d1424;
            --bg-hover: #111a2e;
            --bg-tertiary: #151d30;
            --card-bg: #0d1424;
            --border-subtle: rgba(148, 163, 184, 0.15);
            --border-color: rgba(148, 163, 184, 0.2);
            --border-focus: rgba(96, 165, 250, 0.4);
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
            font-size: 1.8em;
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
            font-size: 2em;
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
            font-size: 1.5em;
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
            font-size: 2.5em;
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
        }'''

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

        # Bubble chart data
        bubble_data = []
        for col in profile.columns:
            bubble_data.append({
                'x': col.quality.completeness,
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
        // STICKY NAVIGATION
        // ======================================================
        const navBtns = document.querySelectorAll('.nav-btn');
        const sections = {{
            'summary': document.getElementById('section-summary'),
            'alerts': document.getElementById('section-alerts'),
            'ml': document.getElementById('section-ml'),
            'columns': document.getElementById('section-columns'),
            'config': document.getElementById('section-config')
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
                        title: {{ display: true, text: 'Completeness %', color: '#64748b' }},
                        min: Math.max(0, Math.min(...{json.dumps([col.quality.completeness for col in profile.columns])}) - 5),
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

        // Word Cloud
        const wordCloudContainer = document.getElementById('wordCloudContainer');
        if (wordCloudContainer && typeof WordCloud !== 'undefined') {{
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

        pii_items = ''
        for col in pii_columns:
            pii_types = col.get('pii_types', [])
            type_name = pii_types[0].get('name', 'Unknown') if pii_types else 'Unknown'
            frameworks = col.get('frameworks', [])

            framework_tags = ''.join(f'<span class="pii-framework-tag">{fw}</span>' for fw in frameworks)

            pii_items += f'''
                <div class="pii-column-item">
                    <div class="pii-column-name">{col.get('name', 'Unknown')}</div>
                    <span class="pii-type-badge">{type_name}</span>
                    <div class="pii-frameworks">{framework_tags}</div>
                </div>'''

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
        """Generate the ML Analysis section (beta feature)."""
        if not ml_findings:
            return ''

        summary = ml_findings.get('summary', {})
        total_issues = summary.get('total_issues', 0)
        severity = summary.get('severity', 'none')
        key_findings = summary.get('key_findings', [])
        sample_info = ml_findings.get('sample_info', {})
        analysis_time = ml_findings.get('analysis_time_seconds', 0)

        # Determine severity badge style
        severity_map = {
            'high': ('critical', 'HIGH RISK'),
            'medium': ('warning', 'MEDIUM'),
            'low': ('good', 'LOW'),
            'none': ('good', 'CLEAN')
        }
        badge_class, badge_text = severity_map.get(severity, ('good', 'UNKNOWN'))

        # Build findings list
        findings_html = ''
        for finding in key_findings[:5]:
            findings_html += f'<div class="ml-finding-item">• {finding}</div>'

        if not findings_html:
            findings_html = '<div class="ml-finding-item">✓ No significant anomalies detected</div>'

        # Build detailed sections organized by tier
        details_html = ''

        # ═══════════════════════════════════════════════════════════════
        # TIER 1: DATA AUTHENTICITY
        # ═══════════════════════════════════════════════════════════════
        benford_analysis = ml_findings.get('benford_analysis', {})
        if benford_analysis:
            details_html += '''
                <div class="ml-tier-header" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 12px 16px; margin: 20px 0 10px 0; border-radius: 8px; font-weight: 600;">
                    📊 TIER 1: DATA AUTHENTICITY — Is this data real or fabricated?
                </div>'''

            benford_items = ''
            for col, data in list(benford_analysis.items())[:3]:
                chi_sq = data.get('chi_square', 0)
                mad = data.get('mean_absolute_deviation', 0)
                confidence = data.get('confidence', 'Unknown')
                interpretation = data.get('interpretation', '')

                # Build digit distribution mini-chart
                digit_dist = data.get('digit_distribution', {})
                digit_bars = ''
                for digit in range(1, 10):
                    d_data = digit_dist.get(digit, digit_dist.get(str(digit), {}))
                    observed = d_data.get('observed', 0)
                    expected = d_data.get('expected', 0)
                    deviation = d_data.get('deviation', 0)
                    bar_color = '#e74c3c' if abs(deviation) > 5 else '#f39c12' if abs(deviation) > 2 else '#2ecc71'
                    digit_bars += f'<span style="display:inline-block;width:20px;text-align:center;margin:1px;padding:2px;background:{bar_color};color:white;font-size:10px;border-radius:2px;" title="Digit {digit}: {observed:.1f}% (expected {expected:.1f}%)">{digit}</span>'

                benford_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong> <span style='color:#6b7280;font-size:0.8em;'>(χ²={chi_sq:.1f}, MAD={mad:.1f}%)</span></div>
                        <div class="ml-detail-count" style="color:#e74c3c;">⚠️ {confidence}</div>
                        <div class="ml-detail-desc">
                            {interpretation}<br>
                            <span style="font-size:0.85em;">First digit distribution: {digit_bars}</span>
                        </div>
                    </div>'''

            plain_english = benford_analysis.get(list(benford_analysis.keys())[0], {}).get('plain_english', '') if benford_analysis else ''
            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">📊 Benford's Law Analysis</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">{plain_english}</div></details>
                    {benford_items}
                </div>'''

        # ═══════════════════════════════════════════════════════════════
        # TIER 2: RECORD ANOMALIES
        # ═══════════════════════════════════════════════════════════════
        numeric_outliers = ml_findings.get('numeric_outliers', {})
        autoencoder = ml_findings.get('autoencoder_anomalies', {})

        if numeric_outliers or (autoencoder and autoencoder.get('anomaly_count', 0) > 0):
            details_html += '''
                <div class="ml-tier-header" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); color: white; padding: 12px 16px; margin: 20px 0 10px 0; border-radius: 8px; font-weight: 600;">
                    🎯 TIER 2: RECORD ANOMALIES — Which specific rows have problems?
                </div>'''

        # Numeric outliers (univariate)
        if numeric_outliers:
            outlier_items = ''
            for col, data in list(numeric_outliers.items())[:5]:
                count = data.get('anomaly_count', 0)
                interpretation = data.get('interpretation', '')
                contamination = data.get('contamination_used', 'auto')
                top_values = data.get('top_anomalies', [])
                top_display = ', '.join(f'{v:,.2f}' if isinstance(v, (int, float)) else str(v) for v in top_values[-3:])
                rows_table = self._build_sample_rows_html(data.get('sample_rows', []))
                contamination_info = f" <span style='color:#6b7280;font-size:0.8em;'>(sensitivity: {contamination})</span>" if contamination != 'auto' else ""

                outlier_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong>{contamination_info}</div>
                        <div class="ml-detail-count">{count:,} outliers</div>
                        <div class="ml-detail-desc">{interpretation or f"Top: {top_display}"}{rows_table}</div>
                    </div>'''

            skipped_semantic = ml_findings.get('skipped_numeric_semantic', [])
            skipped_note = ""
            if skipped_semantic:
                skipped_cols = ', '.join(s['column'] for s in skipped_semantic[:3])
                skipped_note = f'''<br><br><span style="color:#2ecc71;font-size:0.9em;">✓ <em>Smart filtering:</em> {len(skipped_semantic)} column(s) skipped ({skipped_cols}) - these are IDs, not measurements.</span>'''

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🔢 Extreme Values (Isolation Forest)</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">Individual values that are unusually high or low. Like spotting a $10,000 transaction among mostly $50 ones. These could be data entry errors, exceptional cases, or legitimate but unusual activity.{skipped_note}</div></details>
                    {outlier_items}
                </div>'''

        # Autoencoder anomalies
        if autoencoder and autoencoder.get('anomaly_count', 0) > 0:
            ae_count = autoencoder.get('anomaly_count', 0)
            ae_pct = autoencoder.get('anomaly_percentage', 0)
            confidence = autoencoder.get('confidence', 'Unknown')
            architecture = autoencoder.get('architecture', 'Unknown')
            interpretation = autoencoder.get('interpretation', '')
            plain_english = autoencoder.get('plain_english', '')

            contrib_features = autoencoder.get('contributing_features', [])
            features_html = ''
            if contrib_features:
                features_list = ', '.join([f"{f['column']}" for f in contrib_features[:3]])
                features_html = f'<br><span style="font-size:0.85em;color:#6b7280;">Key contributing columns: {features_list}</span>'

            rows_table = self._build_sample_rows_html(autoencoder.get('sample_rows', []))

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🧠 Deep Learning Analysis (Autoencoder)</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">{plain_english}</div></details>
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>Neural Network</strong> <span style='color:#6b7280;font-size:0.8em;'>({architecture})</span></div>
                        <div class="ml-detail-count">{ae_count:,} anomalies ({ae_pct:.2f}%)</div>
                        <div class="ml-detail-desc">
                            {interpretation}{features_html}{rows_table}
                        </div>
                    </div>
                </div>'''

        # ═══════════════════════════════════════════════════════════════
        # TIER 3: DATA QUALITY ISSUES
        # ═══════════════════════════════════════════════════════════════
        rare_categories = ml_findings.get('rare_categories', {})
        format_anomalies = ml_findings.get('format_anomalies', {})
        cross_issues = ml_findings.get('cross_column_issues', [])

        if rare_categories or format_anomalies or cross_issues:
            details_html += '''
                <div class="ml-tier-header" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; padding: 12px 16px; margin: 20px 0 10px 0; border-radius: 8px; font-weight: 600;">
                    📝 TIER 3: DATA QUALITY ISSUES — What needs fixing?
                </div>'''

        # Rare categories
        if rare_categories:
            rare_items = ''
            for col, data in list(rare_categories.items())[:5]:
                rare_vals = data.get('rare_values', [])
                total_rare = data.get('total_rare_count', 0)
                threshold = data.get('threshold_percentage', 0)
                interpretation = data.get('interpretation', '')
                semantic_behavior = data.get('semantic_behavior', 'default')

                vals_display = ', '.join([f'"{v["value"]}" ({v["count"]})' for v in rare_vals[:3]])
                semantic_note = f" <span style='color:#2ecc71;font-size:0.8em;'>[{semantic_behavior}]</span>" if semantic_behavior != 'default' else ""

                rare_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong>{semantic_note}</div>
                        <div class="ml-detail-count">{total_rare:,} rare values</div>
                        <div class="ml-detail-desc">{interpretation}<br><span style="color:#e67e22;">Examples: {vals_display}</span></div>
                    </div>'''

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">📋 Rare/Invalid Values</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">Categories that appear very rarely - could be typos, invalid codes, or legitimate but unusual values. Review to determine if they need correction.</div></details>
                    {rare_items}
                </div>'''

        # Format anomalies
        if format_anomalies:
            format_items = ''
            for col, data in list(format_anomalies.items())[:5]:
                count = data.get('anomaly_count', 0)
                pattern_desc = data.get('dominant_pattern_description', data.get('dominant_pattern', 'Unknown'))
                sample_normal = data.get('sample_dominant_values', [])
                unique_normal = list(dict.fromkeys(sample_normal))[:1]
                normal_display = f'"{unique_normal[0]}"' if unique_normal else ''
                sample_anomalies = data.get('sample_anomalies', [])
                unique_anomalies = list(dict.fromkeys(sample_anomalies))[:3]
                anomaly_display = ', '.join(f'"{s}"' for s in unique_anomalies) if unique_anomalies else 'N/A'
                expected_part = f"Expected: {pattern_desc}"
                if normal_display:
                    expected_part += f" (e.g., {normal_display})"
                rows_table = self._build_sample_rows_html(data.get('sample_rows', []))

                format_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong></div>
                        <div class="ml-detail-count">{count:,} mismatches</div>
                        <div class="ml-detail-desc">{expected_part}<br><span style="color:#e74c3c;">Anomalies: {anomaly_display}</span>{rows_table}</div>
                    </div>'''

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">📝 Format Inconsistencies</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">Values that don't match the expected pattern for this column. Could indicate data entry errors, system migrations, or records from different sources.</div></details>
                    {format_items}
                </div>'''

        # Cross-column issues
        if cross_issues:
            cross_items = ''
            for issue in cross_issues[:3]:
                cols = ' / '.join(issue.get('columns', []))
                total = issue.get('total_issues', 0)
                interpretation = issue.get('interpretation', '')
                rows_table = self._build_sample_rows_html(issue.get('sample_rows', []))

                cross_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{cols}</strong></div>
                        <div class="ml-detail-count">{total:,} issues</div>
                        <div class="ml-detail-desc">{interpretation}{rows_table}</div>
                    </div>'''

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🔗 Cross-Column Issues</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">Values in related columns don't match expected ratios. For example, "Amount Paid" should roughly equal "Amount Received" - large differences may indicate errors.</div></details>
                    {cross_items}
                </div>'''

        # ═══════════════════════════════════════════════════════════════
        # TIER 4: PATTERN ANALYSIS
        # ═══════════════════════════════════════════════════════════════
        temporal_patterns = ml_findings.get('temporal_patterns', {})
        temporal_warnings = {k: v for k, v in temporal_patterns.items() if v.get('warning')}
        corr_anomalies = ml_findings.get('correlation_anomalies', {})

        if temporal_warnings or corr_anomalies:
            details_html += '''
                <div class="ml-tier-header" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%); color: white; padding: 12px 16px; margin: 20px 0 10px 0; border-radius: 8px; font-weight: 600;">
                    ⏰ TIER 4: PATTERN ANALYSIS — Structural insights
                </div>'''

        # Temporal anomalies
        if temporal_warnings:
            temporal_items = ''
            for col, data in list(temporal_warnings.items())[:3]:
                interpretation = data.get('interpretation', 'Suspicious pattern detected')
                rows_table = self._build_sample_rows_html(data.get('sample_rows', []))

                temporal_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong></div>
                        <div class="ml-detail-desc">{interpretation}{rows_table}</div>
                    </div>'''

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">⏰ Temporal Patterns</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">Unusual timing patterns - like too many records at midnight (batch processing artifacts), weekend gaps, or activity spikes outside business hours.</div></details>
                    {temporal_items}
                </div>'''

        # Correlation anomalies
        if corr_anomalies:
            corr_breaks = corr_anomalies.get('correlation_breaks', [])

            if corr_breaks:
                corr_items = ''
                for cb in corr_breaks[:3]:
                    cols = ' / '.join(cb.get('columns', []))
                    count = cb.get('anomaly_count', 0)
                    expected_corr = cb.get('expected_correlation', 0)
                    interpretation = cb.get('interpretation', '')
                    sample_rows = cb.get('sample_rows', [])
                    rows_html = self._build_sample_rows_html(sample_rows, max_rows=5) if sample_rows else ''

                    corr_items += f'''
                        <div class="ml-detail-item">
                            <div class="ml-detail-col"><strong>{cols}</strong> <span style='color:#6b7280;font-size:0.8em;'>(r={expected_corr:.2f})</span></div>
                            <div class="ml-detail-count">{count:,} breaks</div>
                            <div class="ml-detail-desc">{interpretation}{rows_html}</div>
                        </div>'''

                details_html += f'''
                    <div class="ml-detail-section">
                        <div class="ml-detail-header">📈 Correlation Anomalies</div>
                        <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">Some columns naturally move together (e.g., "Amount Paid" and "Amount Received" should be similar). These are records where that expected relationship breaks - one value changed but the other did not follow as expected.</div></details>
                        {corr_items}
                    </div>'''

        # ═══════════════════════════════════════════════════════════════
        # INFORMATIONAL: CLUSTERING (not counted as issues)
        # ═══════════════════════════════════════════════════════════════
        clustering = ml_findings.get('clustering_analysis', {})
        if clustering and clustering.get('n_clusters', 0) > 0:
            n_clusters = clustering.get('n_clusters', 0)
            noise_points = clustering.get('noise_points', 0)
            interpretation = clustering.get('interpretation', '')

            # Build cluster distribution visualization
            cluster_counts = clustering.get('cluster_sizes', {})
            total_in_clusters = sum(v for k, v in cluster_counts.items() if k != -1)
            cluster_html = ''
            cluster_bars = ''
            colors = ['#8b5cf6', '#06b6d4', '#22c55e', '#f59e0b', '#ef4444']
            for idx, (cluster_id, size) in enumerate(sorted(cluster_counts.items(), key=lambda x: -x[1])[:5]):
                if cluster_id != -1:
                    pct = (size / total_in_clusters * 100) if total_in_clusters > 0 else 0
                    color = colors[idx % len(colors)]
                    cluster_html += f'<span style="display:inline-block;margin:2px;padding:4px 10px;background:{color};color:white;border-radius:4px;font-size:0.85em;">Cluster {cluster_id}: {size:,} ({pct:.1f}%)</span>'
                    cluster_bars += f'<div style="height:20px;background:{color};width:{pct}%;min-width:2px;border-radius:2px;" title="Cluster {cluster_id}: {size:,}"></div>'

            # Sample noise rows (records that don't fit any cluster)
            sample_noise = clustering.get('sample_noise_rows', [])
            noise_rows_html = self._build_sample_rows_html(sample_noise, max_rows=5) if sample_noise else ''

            details_html += f'''
                <div class="ml-info-header" style="background: linear-gradient(135deg, #94a3b8 0%, #64748b 100%); color: white; padding: 12px 16px; margin: 20px 0 10px 0; border-radius: 8px; font-weight: 600;">
                    ℹ️ INFORMATIONAL: CLUSTERING — Natural data groupings for context
                </div>
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🔬 DBSCAN Clustering Analysis</div>
                    <details class="ml-hint-collapse"><summary>💡 What this means...</summary><div class="ml-hint-content">DBSCAN groups similar records based on numeric properties. This reveals natural structure in your data - records with similar characteristics cluster together. <strong>Noise points</strong> are records that do not fit any cluster - these are unique or unusual records that may warrant a closer look, though they are not necessarily errors.</div></details>

                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin: 16px 0;">
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 16px; border: 1px solid var(--border-color);">
                            <div style="font-size: 2em; font-weight: bold; color: var(--text-primary);">{n_clusters}</div>
                            <div style="color: var(--text-secondary); font-size: 0.9em;">Natural Clusters</div>
                        </div>
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 16px; border: 1px solid var(--border-color);">
                            <div style="font-size: 2em; font-weight: bold; color: #f59e0b;">{noise_points:,}</div>
                            <div style="color: var(--text-secondary); font-size: 0.9em;">Noise Points (unique records)</div>
                        </div>
                    </div>

                    <div style="margin: 16px 0;">
                        <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 8px;">Cluster Distribution:</div>
                        <div style="display: flex; gap: 2px; height: 24px; background: var(--border-color); border-radius: 4px; overflow: hidden;">
                            {cluster_bars}
                        </div>
                        <div style="margin-top: 8px;">{cluster_html}</div>
                    </div>

                    {f'<div style="margin-top: 16px;"><div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 8px;">📋 Sample Noise Points (unique records worth reviewing):</div>{noise_rows_html}</div>' if noise_rows_html else ''}
                </div>'''

        # Prepare chart data (Multivariate IF removed, DBSCAN informational only)
        outlier_count = sum(f.get('anomaly_count', 0) for f in numeric_outliers.values())
        corr_break_count = sum(b.get('anomaly_count', 0) for b in corr_anomalies.get('correlation_breaks', [])) if corr_anomalies else 0
        format_count = sum(f.get('anomaly_count', 0) for f in format_anomalies.values())
        rare_count = sum(f.get('total_rare_count', 0) for f in rare_categories.values())
        cross_count = sum(i.get('total_issues', 0) for i in cross_issues)
        temporal_count = len(temporal_warnings)
        benford_count = len(benford_analysis) if benford_analysis else 0
        autoencoder_count = autoencoder.get('anomaly_count', 0) if autoencoder else 0

        # Build outlier bar chart data (top 5 columns by outlier count)
        outlier_chart_data = []
        for col, data in sorted(numeric_outliers.items(), key=lambda x: x[1].get('anomaly_count', 0), reverse=True)[:5]:
            outlier_chart_data.append({
                'column': col[:15] + '...' if len(col) > 15 else col,
                'count': data.get('anomaly_count', 0)
            })

        # Chart JS data - organized by tiers (removed: Multivariate IF, Clustering noise)
        chart_labels = ['Outliers', 'Autoencoder', 'Format', 'Rare', 'Cross-Col', 'Correlation', 'Temporal', 'Benford']
        chart_values = [outlier_count, autoencoder_count, format_count, rare_count, cross_count, corr_break_count, temporal_count, benford_count]

        # Filter out zero values for cleaner chart
        non_zero_data = [(l, v) for l, v in zip(chart_labels, chart_values) if v > 0]
        if non_zero_data:
            chart_labels, chart_values = zip(*non_zero_data)
            chart_labels = list(chart_labels)
            chart_values = list(chart_values)
        else:
            chart_labels = ['No Issues']
            chart_values = [0]

        return f'''
        <div class="accordion open" data-accordion="ml-analysis">
            <div class="accordion-header" onclick="toggleAccordion(this)">
                <div class="accordion-title-group">
                    <div class="accordion-icon ml">🧠</div>
                    <div>
                        <div class="accordion-title">ML-Based Anomaly Detection <span class="beta-badge">BETA</span></div>
                        <div class="accordion-subtitle">Machine learning analysis of data patterns</div>
                    </div>
                </div>
                <div class="accordion-meta">
                    <span class="accordion-badge {badge_class}">{badge_text}</span>
                    <span class="accordion-chevron">▼</span>
                </div>
            </div>
            <div class="accordion-body">
                <div class="accordion-content">
                    <div class="hint-box">
                        <strong>🧠 About ML Analysis:</strong> A tiered approach to finding data issues:<br>
                        <strong>Tier 1</strong> checks if data is real (Benford's Law) • <strong>Tier 2</strong> finds problematic records (Outliers, Autoencoder) • <strong>Tier 3</strong> detects quality issues (Format, Rare values) • <strong>Tier 4</strong> reveals patterns (Temporal, Correlation).<br>
                        Analyzed {sample_info.get('analyzed_rows', 0):,} rows ({sample_info.get('sample_percentage', 0):.1f}% of data) in {analysis_time:.1f}s.
                    </div>

                    <div class="ml-charts-row">
                        <div class="ml-summary-card">
                            <div class="ml-summary-header">
                                <div class="ml-summary-count">{total_issues:,}</div>
                                <div class="ml-summary-label">Potential Issues Found</div>
                            </div>
                            <div class="ml-findings-list">
                                {findings_html}
                            </div>
                        </div>

                        <div class="ml-chart-card">
                            <div class="ml-chart-title">Issue Breakdown</div>
                            <div class="ml-chart-container">
                                <canvas id="mlBreakdownChart"></canvas>
                            </div>
                        </div>

                        {f"""<div class="ml-chart-card">
                            <div class="ml-chart-title">Top Outlier Columns</div>
                            <div class="ml-chart-container">
                                <canvas id="mlOutlierChart"></canvas>
                            </div>
                        </div>""" if outlier_chart_data else ""}
                    </div>

                    {details_html}
                </div>
            </div>
        </div>

        <script>
        // ML Charts initialization
        document.addEventListener('DOMContentLoaded', function() {{
            // Issue Breakdown Doughnut Chart
            const breakdownCtx = document.getElementById('mlBreakdownChart');
            if (breakdownCtx) {{
                new Chart(breakdownCtx, {{
                    type: 'doughnut',
                    data: {{
                        labels: {chart_labels},
                        datasets: [{{
                            data: {chart_values},
                            backgroundColor: [
                                'rgba(139, 92, 246, 0.8)',
                                'rgba(236, 72, 153, 0.8)',
                                'rgba(6, 182, 212, 0.8)',
                                'rgba(34, 197, 94, 0.8)',
                                'rgba(245, 158, 11, 0.8)',
                                'rgba(239, 68, 68, 0.8)',
                                'rgba(59, 130, 246, 0.8)',
                                'rgba(168, 85, 247, 0.8)'
                            ],
                            borderWidth: 0
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{
                                position: 'bottom',
                                labels: {{
                                    color: getComputedStyle(document.documentElement).getPropertyValue('--text-secondary'),
                                    font: {{ size: 11 }},
                                    padding: 8
                                }}
                            }}
                        }}
                    }}
                }});
            }}

            // Outlier Bar Chart
            const outlierCtx = document.getElementById('mlOutlierChart');
            if (outlierCtx) {{
                const outlierData = {outlier_chart_data};
                new Chart(outlierCtx, {{
                    type: 'bar',
                    data: {{
                        labels: outlierData.map(d => d.column),
                        datasets: [{{
                            label: 'Outliers',
                            data: outlierData.map(d => d.count),
                            backgroundColor: 'rgba(139, 92, 246, 0.7)',
                            borderColor: 'rgba(139, 92, 246, 1)',
                            borderWidth: 1,
                            borderRadius: 4
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        indexAxis: 'y',
                        plugins: {{
                            legend: {{ display: false }}
                        }},
                        scales: {{
                            x: {{
                                beginAtZero: true,
                                grid: {{
                                    color: 'rgba(148, 163, 184, 0.1)'
                                }},
                                ticks: {{
                                    color: getComputedStyle(document.documentElement).getPropertyValue('--text-secondary')
                                }}
                            }},
                            y: {{
                                grid: {{ display: false }},
                                ticks: {{
                                    color: getComputedStyle(document.documentElement).getPropertyValue('--text-secondary'),
                                    font: {{ size: 11 }}
                                }}
                            }}
                        }}
                    }}
                }});
            }}
        }});
        </script>'''

    def _generate_advanced_visualizations(self, ml_findings: Dict) -> str:
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
            for idx, (col, dist_data) in enumerate(list(amount_dists.items())[:4]):
                if not dist_data or not dist_data.get('histogram'):
                    continue

                chart_id = f'amountLogHist_{idx}'
                min_val = dist_data.get('min_value', 0)
                max_val = dist_data.get('max_value', 0)
                median_val = dist_data.get('median', 0)
                mean_val = dist_data.get('mean', 0)

                charts_html += f'''
                    <div style="flex: 1; min-width: 300px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-subtle);">
                        <h4 style="margin: 0 0 8px 0; font-size: 0.95em; color: var(--text-primary);">{col}</h4>
                        <div style="font-size: 0.8em; color: var(--text-muted); margin-bottom: 12px;">
                            Min: {min_val:,.2f} | Max: {max_val:,.2f} | Median: {median_val:,.2f}
                        </div>
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
                                Gaps indicate periods with missing or low data coverage - potential ingestion issues or business seasonality.
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
            std_err = recon_errors.get('std', 0)
            threshold = recon_errors.get('threshold', 0)
            anomaly_count = recon_errors.get('anomaly_count', 0)
            anomaly_pct = recon_errors.get('anomaly_percentage', 0)

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
                            Records above the threshold ({threshold:.4f}) are flagged as anomalies.
                            {sample_note}
                        </p>
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin-bottom: 20px;">
                            <div style="background: var(--bg-card); padding: 16px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.5em; font-weight: bold; color: var(--text-primary);">{mean_err:.4f}</div>
                                <div style="font-size: 0.85em; color: var(--text-muted);">Mean Error</div>
                            </div>
                            <div style="background: var(--bg-card); padding: 16px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.5em; font-weight: bold; color: var(--text-primary);">{std_err:.4f}</div>
                                <div style="font-size: 0.85em; color: var(--text-muted);">Std Dev</div>
                            </div>
                            <div style="background: var(--bg-card); padding: 16px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.5em; font-weight: bold; color: var(--critical);">{threshold:.4f}</div>
                                <div style="font-size: 0.85em; color: var(--text-muted);">Threshold</div>
                            </div>
                            <div style="background: var(--bg-card); padding: 16px; border-radius: 8px; text-align: center;">
                                <div style="font-size: 1.5em; font-weight: bold; color: var(--warning);">{anomaly_pct:.2f}%</div>
                                <div style="font-size: 0.85em; color: var(--text-muted);">Anomaly Rate</div>
                            </div>
                        </div>
                        <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; height: 200px;">
                            <canvas id="reconErrorChart"></canvas>
                        </div>
                        <script>
                        document.addEventListener('DOMContentLoaded', function() {{
                            const reconCtx = document.getElementById('reconErrorChart');
                            if (reconCtx) {{
                                // Generate simulated distribution based on stats
                                const mean = {mean_err};
                                const std = {std_err};
                                const threshold = {threshold};
                                const bins = 40;
                                const labels = [];
                                const data = [];
                                const bgColors = [];

                                for (let i = 0; i < bins; i++) {{
                                    const x = mean - 3*std + (6*std/bins) * i;
                                    const y = Math.exp(-0.5 * Math.pow((x - mean) / std, 2)) * 1000;
                                    labels.push(x.toFixed(4));
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
        """Generate the overview accordion."""

        def get_bar_class(value):
            if value >= 95: return 'good'
            if value >= 80: return 'warning'
            return 'critical'

        def get_hint(metric, value):
            hints = {
                'completeness': '✓ No missing values - ready for analysis' if value == 100 else f'{100-value:.2f}% of values are null' if value >= 99 else f'{100-value:.1f}% of values are null',
                'validity': f'{value:.0f}% of values match expected formats',
                'consistency': '✓ Patterns are uniform across data' if value >= 95 else 'Some pattern variations detected',
                'uniqueness': 'ℹ Some expected duplication in categorical columns' if value < 80 else '✓ Good uniqueness ratio'
            }
            return hints.get(metric, '')

        return f'''
                <div class="accordion open" data-accordion="overview">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon overview">📊</div>
                            <div>
                                <div class="accordion-title">Overview & Distribution</div>
                                <div class="accordion-subtitle">Data types, completeness breakdown</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge {'good' if profile.overall_quality_score >= 80 else 'warning' if profile.overall_quality_score >= 60 else 'critical'}">{'Healthy' if profile.overall_quality_score >= 80 else 'Fair' if profile.overall_quality_score >= 60 else 'Needs Attention'}</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="hint-box">
                                <strong>📊 How to read these metrics:</strong> Quality dimensions measure different aspects of your data. Higher percentages indicate better data quality.
                            </div>
                            <div class="overview-grid">
                                <div class="overview-stat">
                                    <div class="overview-stat-header">
                                        <span class="overview-stat-label">Completeness</span>
                                    </div>
                                    <div class="overview-stat-value">{avg_completeness:.1f}%</div>
                                    <div class="overview-stat-bar">
                                        <div class="overview-stat-fill {get_bar_class(avg_completeness)}" style="width: {avg_completeness}%"></div>
                                    </div>
                                    <div class="overview-stat-hint">{get_hint('completeness', avg_completeness)}</div>
                                </div>
                                <div class="overview-stat">
                                    <div class="overview-stat-header">
                                        <span class="overview-stat-label">Validity</span>
                                    </div>
                                    <div class="overview-stat-value">{avg_validity:.1f}%</div>
                                    <div class="overview-stat-bar">
                                        <div class="overview-stat-fill {get_bar_class(avg_validity)}" style="width: {avg_validity}%"></div>
                                    </div>
                                    <div class="overview-stat-hint">{get_hint('validity', avg_validity)}</div>
                                </div>
                                <div class="overview-stat">
                                    <div class="overview-stat-header">
                                        <span class="overview-stat-label">Consistency</span>
                                    </div>
                                    <div class="overview-stat-value">{avg_consistency:.1f}%</div>
                                    <div class="overview-stat-bar">
                                        <div class="overview-stat-fill {get_bar_class(avg_consistency)}" style="width: {avg_consistency}%"></div>
                                    </div>
                                    <div class="overview-stat-hint">{get_hint('consistency', avg_consistency)}</div>
                                </div>
                                <div class="overview-stat">
                                    <div class="overview-stat-header">
                                        <span class="overview-stat-label">Uniqueness</span>
                                    </div>
                                    <div class="overview-stat-value">{avg_uniqueness:.1f}%</div>
                                    <div class="overview-stat-bar">
                                        <div class="overview-stat-fill {get_bar_class(avg_uniqueness)}" style="width: {min(avg_uniqueness, 100)}%"></div>
                                    </div>
                                    <div class="overview-stat-hint">{get_hint('uniqueness', avg_uniqueness)}</div>
                                </div>
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
                                    <div class="chart-title">Column Quality: Completeness vs Validity</div>
                                    <div class="hint-box" style="margin-bottom: 12px; margin-top: 0;">
                                        Bubble size represents unique value count. Position shows completeness (x) vs validity (y).
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

        return f'''
                                <div class="column-row" onclick="toggleColumnRow(this)" {data_attrs}>
                                    <div class="column-row-header">
                                        <span class="column-expand-icon">▶</span>
                                        <div class="column-type-icon {type_class}">{icon}</div>
                                        <div class="column-info">
                                            <div class="column-name">{col.name}</div>
                                            <div class="column-type">{inferred_type} ({col.type_info.confidence*100:.0f}% confidence)</div>
                                        </div>
                                        <div class="column-quick-stats">
                                            <span>{completeness_str}</span>
                                            <span>{col.statistics.unique_percentage:.2f}% unique</span>
                                        </div>
                                        <div class="column-tags">
                                            {tags}
                                        </div>
                                        <span class="column-quality-score {score_class}">{score:.0f}%</span>
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

        # Add type-specific stats
        if stats.mean is not None:
            stat_items.append(('Mean', self._format_number(stats.mean)))
        if stats.median is not None:
            stat_items.append(('Median', self._format_number(stats.median)))
        if stats.std_dev is not None:
            stat_items.append(('Std Dev', self._format_number(stats.std_dev)))
        if stats.min_value is not None:
            stat_items.append(('Min', str(stats.min_value)[:20]))
        if stats.max_value is not None:
            stat_items.append(('Max', str(stats.max_value)[:20]))
        if stats.min_length is not None:
            stat_items.append(('Length', f'{stats.min_length}-{stats.max_length} chars'))
        if stats.pattern_samples:
            top_pattern = stats.pattern_samples[0] if stats.pattern_samples else {}
            stat_items.append(('Pattern', f"{top_pattern.get('pattern', 'N/A')[:15]}"))
        if stats.sample_size:
            stat_items.append(('Sample Size', f'{stats.sample_size:,}'))

        stats_html = ''
        for label, value in stat_items[:8]:  # Limit to 8 stats
            stats_html += f'''
                                            <div class="column-stat">
                                                <div class="column-stat-label">{label}</div>
                                                <div class="column-stat-value">{value}</div>
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
        for col in temporal_columns:
            analysis = col.temporal_analysis or {}
            date_range = analysis.get('date_range', {})
            frequency = analysis.get('frequency', {})
            gaps = analysis.get('gaps', {})
            trend = analysis.get('trend', {})

            # Determine if there are issues
            has_gaps = gaps.get('gaps_detected', False)
            gap_count = gaps.get('gap_count', 0)

            temporal_items += f'''
                <div class="temporal-column-card" style="background: var(--card-bg); border-radius: 12px; padding: 20px; margin-bottom: 16px; border: 1px solid var(--border-color);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px;">
                        <h4 style="color: var(--text-primary); margin: 0; font-size: 1.1em;">📅 {col.name}</h4>
                        <span class="accordion-badge {'warning' if has_gaps else 'good'}">{gap_count} gaps</span>
                    </div>

                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px;">
                        <div class="temporal-stat">
                            <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 4px;">Date Range</div>
                            <div style="color: var(--text-primary); font-weight: 500;">{date_range.get('start', 'N/A')[:10]} → {date_range.get('end', 'N/A')[:10]}</div>
                            <div style="color: var(--text-tertiary); font-size: 0.8em;">{date_range.get('span_days', 'N/A')} days total</div>
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
                            <div style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 4px;">Gap Analysis</div>
                            <div style="color: {'#f59e0b' if has_gaps else 'var(--text-primary)'}; font-weight: 500;">{gap_count} gaps found</div>
                            <div style="color: var(--text-tertiary); font-size: 0.8em;">Largest: {gaps.get('largest_gap', 'None')}</div>
                        </div>
                    </div>
                </div>'''

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
        if value is None:
            return 'N/A'
        abs_val = abs(value)
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
            sample_note = "50k sample used for statistical analysis"
        else:
            sample_display = f"Full dataset ({total_rows:,} rows)"
            sample_note = "Full dataset analyzed (under 50k rows)"

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
                <summary style="cursor: pointer; color: var(--text-secondary); font-size: 0.9em; padding: 8px 0;">ℹ️ About the 50k sampling methodology...</summary>
                <div class="hint-box" style="margin-top: 8px; margin-bottom: 0; border-left-color: var(--info);">
                    <p style="margin: 0 0 12px 0;"><strong>📊 Sampling Policy:</strong></p>
                    <p style="margin: 0 0 12px 0; color: var(--text-secondary);">{sampling_explanation.replace(chr(10), '<br>')}</p>

                    <p style="margin: 12px 0 8px 0;"><strong>Full Dataset Metrics:</strong> <span style="color: var(--good);">{full_metrics_str}</span></p>
                    <p style="margin: 0;"><strong>{'Sampled' if sample_used else 'All'} Metrics:</strong> <span style="color: var(--accent);">{sampled_metrics_str}</span></p>
                </div>
            </details>
        </section>'''

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
