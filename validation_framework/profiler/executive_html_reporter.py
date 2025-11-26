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
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from validation_framework.profiler.profile_result import ProfileResult, ColumnProfile
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

        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <title>Data Quality Report - {profile.file_name}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/wordcloud@1.2.2/src/wordcloud2.min.js"></script>

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

    <main class="page">
        <!-- Page Header -->
        <section class="page-header">
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

        <!-- Sampling Summary -->
        {self._generate_sampling_summary(profile, sampling_info)}

        <!-- FIBO Semantic Analysis Section -->
        {self._generate_fibo_section(profile)}

        <!-- PII Risk Section (if PII detected) -->
        {self._generate_pii_section(pii_columns) if pii_count > 0 else ''}

        <!-- ML Analysis Section (if ML analysis was run) -->
        {self._generate_ml_section(profile.ml_findings) if profile.ml_findings else ''}

        <!-- Main Layout -->
        <div class="layout-grid">
            <div class="main-column">
                <!-- Overview Accordion -->
                {self._generate_overview_accordion(profile, type_counts, avg_completeness, avg_validity, avg_consistency, avg_uniqueness)}

                <!-- Quality Metrics Accordion -->
                {self._generate_quality_accordion(profile)}

                <!-- Value Distribution Accordion -->
                {self._generate_distribution_accordion(profile, categorical_columns)}

                <!-- Column Explorer Accordion -->
                {self._generate_column_explorer(profile)}

                <!-- Validation Suggestions Accordion -->
                {self._generate_suggestions_accordion(profile)}

                <!-- Full Validation Config Accordion -->
                {self._generate_full_config_accordion(profile)}

                <!-- Temporal Analysis Accordion (if temporal columns exist) -->
                {self._generate_temporal_accordion(temporal_columns) if temporal_columns else ''}
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
            --border-subtle: rgba(148, 163, 184, 0.15);
            --border-focus: rgba(96, 165, 250, 0.4);
            --accent: #60a5fa;
            --accent-soft: rgba(96, 165, 250, 0.08);
            --accent-gradient: linear-gradient(135deg, #3b82f6, #8b5cf6);
            --text-primary: #f1f5f9;
            --text-secondary: #94a3b8;
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
                grid-template-rows: auto auto;
            }
            .column-quick-stats {
                grid-column: 3 / -1;
                grid-row: 2;
                justify-self: start;
            }
            .column-tags {
                grid-column: 1 / -1;
                grid-row: 3;
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
            background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
            border-left: 3px solid #0ea5e9;
            padding: 10px 14px;
            margin-bottom: 12px;
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            font-size: 0.9em;
            color: #1e3a5f;
            line-height: 1.5;
        }

        .ml-hint em {
            font-style: normal;
            font-weight: 600;
            color: #0369a1;
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

        @media (max-width: 768px) {
            .page { padding: 16px; }
            .top-inner { padding: 12px 16px; }
            .file-meta { display: none; }
            .column-quick-stats { display: none; }
            .column-tags { display: none; }
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

        // Column search
        function filterColumns(query) {{
            const rows = document.querySelectorAll('.column-row');
            const lowerQuery = query.toLowerCase();
            rows.forEach(row => {{
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(lowerQuery) ? '' : 'none';
            }});
        }}

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
            <div class="hint-box" style="margin-top: 12px; margin-bottom: 0; border-left-color: var(--info);">
                <strong>📊 Sampling methodology:</strong> For {profile.row_count:,} rows, statistical sampling provides high confidence results while keeping processing time manageable.
                <br><strong style="color: var(--warning);">⚠️ Limitations:</strong> Rare values (&lt;0.01% occurrence) and extreme outliers may not be captured in sampled statistics.
                <br><strong>*</strong> Unique count marked with * means every sampled row was unique - actual cardinality is likely much higher.
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
                <div class="fibo-description">
                    DataK9 uses <strong>FIBO</strong> (Financial Industry Business Ontology) - an industry-standard
                    semantic framework maintained by the EDM Council - to automatically understand the meaning
                    and purpose of your data columns, enabling context-aware validation suggestions.
                </div>
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
                <div class="hint-box" style="margin-top: 12px; border-left-color: #8b5cf6;">
                    💡 <strong>What this means:</strong> Columns identified as financial data types (e.g., "money.amount", "identifier.account")
                    automatically receive context-appropriate validation suggestions like non-negative checks, precision rules, or uniqueness constraints.
                    <br><a href="https://spec.edmcouncil.org/fibo/" target="_blank" style="color: #8b5cf6;">Learn more about FIBO →</a>
                </div>
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
        <div class="accordion open" data-accordion="pii">
            <div class="accordion-header" onclick="toggleAccordion(this)">
                <div class="accordion-title-group">
                    <div class="accordion-icon issues">🔒</div>
                    <div>
                        <div class="accordion-title">Privacy & PII Risk</div>
                        <div class="accordion-subtitle">Sensitive data detection</div>
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

        # Build detailed sections
        details_html = ''

        # Numeric outliers (univariate)
        numeric_outliers = ml_findings.get('numeric_outliers', {})
        if numeric_outliers:
            outlier_items = ''
            for col, data in list(numeric_outliers.items())[:5]:
                count = data.get('anomaly_count', 0)
                interpretation = data.get('interpretation', '')
                contamination = data.get('contamination_used', 'auto')
                top_values = data.get('top_anomalies', [])
                top_display = ', '.join(f'{v:,.2f}' if isinstance(v, (int, float)) else str(v) for v in top_values[-3:])

                # Build sample rows (mobile-friendly cards)
                rows_table = self._build_sample_rows_html(data.get('sample_rows', []))

                contamination_info = f" <span style='color:#6b7280;font-size:0.8em;'>(contamination: {contamination})</span>" if contamination != 'auto' else ""

                outlier_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong>{contamination_info}</div>
                        <div class="ml-detail-count">{count:,} outliers</div>
                        <div class="ml-detail-desc">{interpretation or f"Top: {top_display}"}{rows_table}</div>
                    </div>'''
            # Show skipped columns info if any
            skipped_semantic = ml_findings.get('skipped_numeric_semantic', [])
            skipped_note = ""
            if skipped_semantic:
                skipped_cols = ', '.join(s['column'] for s in skipped_semantic[:3])
                skipped_note = f'''<br><br><span style="color:#2ecc71;font-size:0.9em;">✓ <em>Smart filtering:</em> {len(skipped_semantic)} column(s) skipped ({skipped_cols}) - these are numeric IDs (like bank codes), not actual measurements. Running outlier detection on IDs is meaningless.</span>'''

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🔢 Univariate Outliers (Isolation Forest)</div>
                    <div class="ml-hint">💡 <em>What this means:</em> These are individual values that are unusually high or low compared to the rest of the data. Think of it as finding prices or amounts that stand out from the crowd - like spotting a $10,000 transaction among mostly $50 ones.{skipped_note}</div>
                    {outlier_items}
                </div>'''

        # Multivariate outliers
        multivariate_outliers = ml_findings.get('multivariate_outliers', {})
        if multivariate_outliers and multivariate_outliers.get('anomaly_count', 0) > 0:
            mv_count = multivariate_outliers.get('anomaly_count', 0)
            mv_pct = multivariate_outliers.get('anomaly_percentage', 0)
            mv_interpretation = multivariate_outliers.get('interpretation', '')
            contributing_cols = multivariate_outliers.get('contributing_columns', [])
            cols_analyzed = multivariate_outliers.get('columns_analyzed', [])

            contrib_html = ''
            if contributing_cols:
                contrib_items = []
                for c in contributing_cols[:3]:
                    contrib_items.append(f"{c['column']} (z-diff: {c['z_score_diff']})")
                contrib_html = f"<br><span style='color:#9ca3af;'>Key factors: {', '.join(contrib_items)}</span>"

            rows_table = self._build_sample_rows_html(multivariate_outliers.get('sample_rows', []))

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🎯 Multivariate Outliers (Cross-Column Patterns)</div>
                    <div class="ml-hint">💡 <em>What this means:</em> These are records where the <strong>combination</strong> of values is unusual, even if each value alone looks normal. For example, a small transaction amount going to a high-risk country might be normal separately, but together they could signal something worth investigating.</div>
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>Combined Analysis</strong> <span style='color:#6b7280;font-size:0.8em;'>({len(cols_analyzed)} columns)</span></div>
                        <div class="ml-detail-count">{mv_count:,} outliers ({mv_pct:.2f}%)</div>
                        <div class="ml-detail-desc">{mv_interpretation}{contrib_html}{rows_table}</div>
                    </div>
                </div>'''

        # Clustering analysis
        clustering = ml_findings.get('clustering_analysis', {})
        if clustering and (clustering.get('n_clusters', 0) > 0 or clustering.get('noise_points', 0) > 0):
            n_clusters = clustering.get('n_clusters', 0)
            noise_count = clustering.get('noise_points', 0)
            noise_pct = clustering.get('noise_percentage', 0)
            cluster_interpretation = clustering.get('interpretation', '')
            clusters = clustering.get('clusters', [])

            cluster_summary = ''
            if clusters:
                cluster_items = []
                for c in clusters[:3]:
                    cluster_items.append(f"Cluster {c['cluster_id']}: {c['size']:,} records ({c['percentage']:.1f}%)")
                cluster_summary = f"<br><span style='color:#9ca3af;'>{'; '.join(cluster_items)}</span>"

            rows_table = self._build_sample_rows_html(clustering.get('sample_noise_rows', []))

            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🔮 Cluster Analysis (DBSCAN)</div>
                    <div class="ml-hint">💡 <em>What this means:</em> This groups similar records together to find natural patterns. Records that don't fit any group are "noise" - they might be errors, edge cases, or genuinely unusual entries worth reviewing.</div>
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{n_clusters} Clusters Found</strong></div>
                        <div class="ml-detail-count">{noise_count:,} noise points ({noise_pct:.1f}%)</div>
                        <div class="ml-detail-desc">{cluster_interpretation}{cluster_summary}{rows_table}</div>
                    </div>
                </div>'''

        # Correlation anomalies
        corr_anomalies = ml_findings.get('correlation_anomalies', {})
        if corr_anomalies:
            corr_breaks = corr_anomalies.get('correlation_breaks', [])
            high_corr_pairs = corr_anomalies.get('high_correlation_pairs', [])

            if corr_breaks:
                corr_items = ''
                for cb in corr_breaks[:3]:
                    cols = ' / '.join(cb.get('columns', []))
                    count = cb.get('anomaly_count', 0)
                    expected_corr = cb.get('expected_correlation', 0)
                    interpretation = cb.get('interpretation', '')

                    corr_items += f'''
                        <div class="ml-detail-item">
                            <div class="ml-detail-col"><strong>{cols}</strong> <span style='color:#6b7280;font-size:0.8em;'>(r={expected_corr})</span></div>
                            <div class="ml-detail-count">{count:,} breaks</div>
                            <div class="ml-detail-desc">{interpretation}</div>
                        </div>'''

                details_html += f'''
                    <div class="ml-detail-section">
                        <div class="ml-detail-header">📈 Correlation Anomalies</div>
                        <div class="ml-hint">💡 <em>What this means:</em> Some columns naturally move together (e.g., "Amount Paid" and "Amount Received" should be similar). These are records where that expected relationship breaks - one value changed but the other didn't follow as expected.</div>
                        {corr_items}
                    </div>'''

        # Format anomalies
        format_anomalies = ml_findings.get('format_anomalies', {})
        if format_anomalies:
            format_items = ''
            for col, data in list(format_anomalies.items())[:5]:
                count = data.get('anomaly_count', 0)
                # Use human-readable description if available, fallback to pattern
                pattern_desc = data.get('dominant_pattern_description', data.get('dominant_pattern', 'Unknown'))
                # Show ONE unique normal value example
                sample_normal = data.get('sample_dominant_values', [])
                unique_normal = list(dict.fromkeys(sample_normal))[:1]  # Dedupe, take 1
                normal_display = f'"{unique_normal[0]}"' if unique_normal else ''
                # Show unique anomaly examples (deduplicated)
                sample_anomalies = data.get('sample_anomalies', [])
                unique_anomalies = list(dict.fromkeys(sample_anomalies))[:3]  # Dedupe, take 3
                anomaly_display = ', '.join(f'"{s}"' for s in unique_anomalies) if unique_anomalies else 'N/A'

                # Build description
                expected_part = f"Expected: {pattern_desc}"
                if normal_display:
                    expected_part += f" (e.g., {normal_display})"

                # Build sample rows (mobile-friendly cards)
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
                    <div class="ml-hint">💡 <em>What this means:</em> Most values in this column follow a consistent pattern (like "XXX-1234"), but some don't match. This could indicate data entry errors, system migration issues, or records that need manual review.</div>
                    {format_items}
                </div>'''

        # Rare categories
        rare_categories = ml_findings.get('rare_categories', {})
        if rare_categories:
            rare_items = ''
            for col, data in list(rare_categories.items())[:5]:
                rare_vals = data.get('rare_values', [])
                total_count = data.get('total_rare_count', 0)
                examples = ', '.join(f'"{v["value"]}"' for v in rare_vals[:3])

                # Show semantic intelligence applied
                semantic_behavior = data.get('semantic_behavior', 'default')
                semantic_reason = data.get('semantic_reason')
                ref_skipped = data.get('reference_values_skipped', 0)
                valid_skipped = data.get('valid_values_skipped', 0)

                # Build context message based on behavior
                context_msg = ""
                if semantic_behavior == 'strict_threshold':
                    context_msg = f'<br><span style="color:#2ecc71;font-size:0.85em;">✓ Strict threshold applied (FIBO: counterparty/entity detection)</span>'
                elif semantic_behavior == 'reference_validate' and ref_skipped > 0:
                    context_msg = f'<br><span style="color:#2ecc71;font-size:0.85em;">✓ {ref_skipped} valid reference values excluded (FIBO semantic validation)</span>'
                elif valid_skipped > 0:
                    context_msg = f'<br><span style="color:#2ecc71;font-size:0.85em;">✓ {valid_skipped} known valid values excluded (domain detection)</span>'

                rare_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong></div>
                        <div class="ml-detail-count">{total_count:,} rare values</div>
                        <div class="ml-detail-desc">Examples: {examples}{context_msg}</div>
                    </div>'''
            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">⚠️ Rare/Suspicious Categories</div>
                    <div class="ml-hint">💡 <em>What this means:</em> These category values appear very infrequently compared to others. This could indicate typos (e.g., "Londn" instead of "London"), test data left in production, or genuinely rare but valid entries that may need verification.<br><br>
                    <em>Smart filtering:</em> The analyzer uses FIBO (Financial Industry Business Ontology) semantic tags to intelligently exclude valid but rare values. For example, rare currencies like "NOK" or "MXN" won't be flagged if the column is recognized as a currency field. Counterparty columns use stricter thresholds since diversity is expected.</div>
                    {rare_items}
                </div>'''

        # Cross-column issues
        cross_issues = ml_findings.get('cross_column_issues', [])
        if cross_issues:
            cross_items = ''
            for issue in cross_issues[:3]:
                cols = ' / '.join(issue.get('columns', []))
                count = issue.get('total_issues', 0)
                interpretation = issue.get('interpretation', '')

                # Build sample rows (mobile-friendly cards)
                rows_table = self._build_sample_rows_html(issue.get('sample_rows', []))

                cross_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{cols}</strong></div>
                        <div class="ml-detail-count">{count:,} issues</div>
                        <div class="ml-detail-desc">{interpretation}{rows_table}</div>
                    </div>'''
            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">🔗 Cross-Column Consistency</div>
                    <div class="ml-hint">💡 <em>What this means:</em> These columns have an expected relationship (e.g., "End Date" should be after "Start Date"), but some records violate that rule. This often indicates data entry errors or process issues.</div>
                    {cross_items}
                </div>'''

        # Temporal patterns
        temporal_patterns = ml_findings.get('temporal_patterns', {})
        temporal_warnings = {k: v for k, v in temporal_patterns.items() if v.get('warning')}
        if temporal_warnings:
            temporal_items = ''
            for col, data in list(temporal_warnings.items())[:3]:
                interpretation = data.get('interpretation', 'Suspicious pattern detected')

                # Build sample rows (mobile-friendly cards)
                rows_table = self._build_sample_rows_html(data.get('sample_rows', []))

                temporal_items += f'''
                    <div class="ml-detail-item">
                        <div class="ml-detail-col"><strong>{col}</strong></div>
                        <div class="ml-detail-desc">{interpretation}{rows_table}</div>
                    </div>'''
            details_html += f'''
                <div class="ml-detail-section">
                    <div class="ml-detail-header">⏰ Temporal Anomalies</div>
                    <div class="ml-hint">💡 <em>What this means:</em> The timing of records shows unusual patterns - like too many transactions at midnight (potential batch processing artifacts), unexpected gaps on weekends, or activity spikes that don't match normal business hours.</div>
                    {temporal_items}
                </div>'''

        # Prepare chart data
        outlier_count = sum(f.get('anomaly_count', 0) for f in numeric_outliers.values())
        mv_outlier_count = multivariate_outliers.get('anomaly_count', 0) if multivariate_outliers else 0
        cluster_noise = clustering.get('noise_points', 0) if clustering else 0
        corr_break_count = sum(b.get('anomaly_count', 0) for b in corr_anomalies.get('correlation_breaks', [])) if corr_anomalies else 0
        format_count = sum(f.get('anomaly_count', 0) for f in format_anomalies.values())
        rare_count = sum(f.get('total_rare_count', 0) for f in rare_categories.values())
        cross_count = sum(i.get('total_issues', 0) for i in cross_issues)
        temporal_count = len(temporal_warnings)

        # Build outlier bar chart data (top 5 columns by outlier count)
        outlier_chart_data = []
        for col, data in sorted(numeric_outliers.items(), key=lambda x: x[1].get('anomaly_count', 0), reverse=True)[:5]:
            outlier_chart_data.append({
                'column': col[:15] + '...' if len(col) > 15 else col,
                'count': data.get('anomaly_count', 0)
            })

        # Chart JS data - include all ML categories
        chart_labels = ['Univariate', 'Multivariate', 'Clustering', 'Correlation', 'Format', 'Rare', 'Cross-Col', 'Temporal']
        chart_values = [outlier_count, mv_outlier_count, cluster_noise, corr_break_count, format_count, rare_count, cross_count, temporal_count]

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
                        <strong>🧠 About ML Analysis:</strong> Uses Isolation Forest (univariate & multivariate), DBSCAN clustering, correlation analysis, and pattern detection to identify anomalies.
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
                'completeness': '✓ No missing values - ready for analysis' if value >= 99 else f'{100-value:.1f}% of values are null',
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

        return f'''
                <div class="accordion" data-accordion="columns">
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
                                <br>Click any column to see detailed statistics and insights.
                            </div>
                            <div class="column-search">
                                <input type="text" placeholder="Search columns by name, type, or tag..." oninput="filterColumns(this.value)">
                            </div>
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

        return f'''
                                <div class="column-row" onclick="toggleColumnRow(this)">
                                    <div class="column-row-header">
                                        <span class="column-expand-icon">▶</span>
                                        <div class="column-type-icon {type_class}">{icon}</div>
                                        <div class="column-info">
                                            <div class="column-name">{col.name}</div>
                                            <div class="column-type">{inferred_type} ({col.type_info.confidence*100:.0f}% confidence)</div>
                                        </div>
                                        <div class="column-quick-stats">
                                            <span>{col.quality.completeness:.0f}% complete</span>
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
                <div class="accordion" data-accordion="config">
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
        """Generate temporal analysis accordion."""
        if not temporal_columns:
            return ''

        col = temporal_columns[0]  # Show first temporal column
        analysis = col.temporal_analysis

        date_range = analysis.get('date_range', {})
        frequency = analysis.get('frequency', {})
        gaps = analysis.get('gaps', {})
        trend = analysis.get('trend', {})

        return f'''
                <div class="accordion" data-accordion="temporal">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon overview">📅</div>
                            <div>
                                <div class="accordion-title">Temporal Analysis</div>
                                <div class="accordion-subtitle">Time series analysis for {col.name}</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge good">Available</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="freshness-content">
                                <div class="freshness-timeline">
                                    <div class="freshness-item">
                                        <div class="freshness-dot good"></div>
                                        <div class="freshness-info">
                                            <div class="freshness-label">Date Range</div>
                                            <div class="freshness-value">{date_range.get('start', 'N/A')[:10]} to {date_range.get('end', 'N/A')[:10]}</div>
                                        </div>
                                    </div>
                                    <div class="freshness-item">
                                        <div class="freshness-dot good"></div>
                                        <div class="freshness-info">
                                            <div class="freshness-label">Time Span</div>
                                            <div class="freshness-value">{date_range.get('span_days', 'N/A')} days</div>
                                        </div>
                                    </div>
                                    <div class="freshness-item">
                                        <div class="freshness-dot good"></div>
                                        <div class="freshness-info">
                                            <div class="freshness-label">Detected Frequency</div>
                                            <div class="freshness-value">{frequency.get('inferred', 'N/A')} ({frequency.get('confidence', 0)*100:.0f}% confidence)</div>
                                        </div>
                                    </div>
                                    <div class="freshness-item">
                                        <div class="freshness-dot {'warning' if gaps.get('gaps_detected') else 'good'}"></div>
                                        <div class="freshness-info">
                                            <div class="freshness-label">Gaps</div>
                                            <div class="freshness-value">{gaps.get('gap_count', 0)} gaps detected (largest: {gaps.get('largest_gap', 'N/A')})</div>
                                        </div>
                                    </div>
                                    <div class="freshness-item">
                                        <div class="freshness-dot good"></div>
                                        <div class="freshness-info">
                                            <div class="freshness-label">Trend</div>
                                            <div class="freshness-value">{trend.get('direction', 'N/A')} ({trend.get('strength', 'N/A')})</div>
                                        </div>
                                    </div>
                                </div>
                            </div>
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

    # Helper methods
    def _build_sample_rows_html(self, sample_rows: List[Dict], max_rows: int = 3) -> str:
        """
        Build mobile-friendly sample rows HTML using cards instead of tables.
        On mobile devices, tables can overflow; cards stack vertically.
        """
        if not sample_rows:
            return ''

        headers = list(sample_rows[0].keys()) if sample_rows else []
        if not headers:
            return ''

        # Build cards for each row - fully stacked vertical layout for mobile
        cards_html = ''
        for i, row in enumerate(sample_rows[:max_rows]):
            fields_html = ''
            for h in headers:
                val = str(row.get(h, ''))[:50]  # Truncate long values
                fields_html += f'<div style="margin-bottom:4px;"><div style="color:var(--text-muted);font-size:9px;text-transform:uppercase;">{h}</div><div style="font-size:11px;overflow-wrap:break-word;word-wrap:break-word;">{val}</div></div>'
            cards_html += f'<div style="background:var(--bg-tertiary);border-radius:4px;padding:10px;margin-bottom:8px;max-width:100%;overflow:hidden;">{fields_html}</div>'

        return f'''<details style="margin-top:8px;">
            <summary style="cursor:pointer;color:var(--primary);font-size:12px;">View sample rows ({len(sample_rows[:max_rows])})</summary>
            <div style="margin-top:8px;max-width:100%;overflow:hidden;">{cards_html}</div>
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
