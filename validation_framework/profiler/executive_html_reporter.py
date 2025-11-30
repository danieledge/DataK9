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
# LLM summarizer is imported dynamically when needed (--beta-llm flag)
import logging
import math

logger = logging.getLogger(__name__)


class ExecutiveHTMLReporter:
    """Generate executive-style HTML reports for profile results."""

    def __init__(self, enable_llm: bool = False):
        """
        Initialize the reporter.

        Args:
            enable_llm: Enable AI-generated summary (--beta-llm flag)
        """
        self._enable_llm = enable_llm

    def _get_llm_summary_html(self, profile_dict: Dict[str, Any]) -> str:
        """Get LLM summary HTML if enabled, empty string otherwise."""
        if not self._enable_llm:
            return ""

        try:
            from validation_framework.profiler.llm_summarizer import get_llm_summary_for_report
            return get_llm_summary_for_report(profile_dict)
        except Exception as e:
            logger.debug(f"LLM summary failed: {e}")
            return ""

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

        # Generate advanced visualization charts categorized by section
        viz_charts = self._generate_advanced_visualizations(
            profile.ml_findings, profile.columns
        ) if profile.ml_findings else {
            'distributions': [], 'anomalies': [], 'temporal': [],
            'correlations': [], 'missingness': [], 'overview': []
        }

        # Pre-join charts for each section
        distribution_charts = ''.join(viz_charts.get('distributions', []))
        anomaly_charts = ''.join(viz_charts.get('anomalies', []))
        temporal_charts = ''.join(viz_charts.get('temporal', []))
        correlation_charts = ''.join(viz_charts.get('correlations', []))
        missingness_charts = ''.join(viz_charts.get('missingness', []))
        overview_charts = ''.join(viz_charts.get('overview', []))

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

    <!-- Sidebar Backdrop (mobile) -->
    <div class="dq-sidebar-backdrop" id="sidebarBackdrop" onclick="toggleSidebar()"></div>

    <!-- Main Layout with Sidebar -->
    <div class="dq-main-layout">
        <!-- Sidebar Navigation -->
        <aside class="dq-sidebar" id="dqSidebar">
            <div class="dq-sidebar-inner">
                <div class="dq-sidebar-section">
                    <div class="dq-sidebar-label">Navigation</div>
                    <ul class="dq-nav-list">
                        <li class="dq-nav-item"><button class="dq-nav-link active" data-section="overview"><span class="nav-num">1</span>Overview</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="engine"><span class="nav-num">2</span>Profiling Engine</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="structure"><span class="nav-num">3</span>Dataset Structure</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="columns"><span class="nav-num">4</span>Field Profiles</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="distributions"><span class="nav-num">5</span>Distributions</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="missingness"><span class="nav-num">6</span>Missingness & Bias</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="anomalies"><span class="nav-num">7</span>Anomalies</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="temporal"><span class="nav-num">8</span>Temporal Analysis</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="correlations"><span class="nav-num">9</span>Correlations</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="validations"><span class="nav-num">10</span>Validation Suggestions</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="yaml"><span class="nav-num">11</span>YAML / Export</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="nextsteps"><span class="nav-num">12</span>Next Steps</button></li>
                        <li class="dq-nav-item"><button class="dq-nav-link" data-section="glossary"><span class="nav-num">13</span>Glossary</button></li>
                    </ul>
                </div>
            </div>
            <div class="dq-sidebar-actions">
                <button class="expand-all-btn" id="expandAllBtn" onclick="toggleExpandAll()" title="Expand/Collapse all sections">
                    <span class="expand-icon">+</span> Expand All
                </button>
                <button class="export-pdf-btn" id="exportPdfBtn" onclick="exportToPDF()" title="Export report to PDF">
                    <span class="pdf-icon">📄</span> Export PDF
                </button>
            </div>
        </aside>

        <!-- Mobile Nav Toggle -->
        <button class="dq-mobile-nav-toggle" id="mobileNavToggle" onclick="toggleSidebar()" aria-label="Toggle navigation">
            ☰
        </button>

        <!-- Main Content Area -->
        <main class="dq-main page">
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 1. HEADER & SAMPLING BANNER                                     -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <section class="page-header" id="section-overview">
            <div class="page-title-block">
                <h1>Data Profile Report</h1>
                <p>Analysis of {profile.row_count:,} records across {profile.column_count} columns</p>
            </div>
        </section>

        <!-- v2 Sampling Coverage Banner -->
        {self._generate_sampling_banner_v2(profile, insights)}

        <!-- Condensed Lineage Banner (expandable) -->
        {self._generate_lineage_banner(profile) if profile.data_lineage else ''}

        <!-- AI-Generated Executive Summary (only with --beta-llm flag) -->
        {self._get_llm_summary_html(profile_dict)}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 2. METRICS DASHBOARD (3 rows: Core, Quality, Types)             -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        {self._generate_metrics_dashboard_v2(profile, avg_completeness, avg_validity, avg_consistency, avg_uniqueness, type_counts)}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 3. DATA QUALITY OVERVIEW - High-level metrics first             -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" id="section-structure" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #1e3a5f 0%, #0d1f3c 100%); border-radius: 8px; border-left: 4px solid #3b82f6;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">Dataset Structure & Semantics</h2>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #94a3b8;">Type distribution, value patterns, and quality scores by column</p>
                </div>
                <div style="background: rgba(255,255,255,0.15); padding: 4px 12px; border-radius: 4px; font-size: 0.8em; font-weight: 600; color: white;">STRUCTURE</div>
            </div>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <!-- Data Types Accordion -->
                {self._generate_overview_accordion(profile, type_counts, avg_completeness, avg_validity, avg_consistency, avg_uniqueness)}

                <!-- Semantic Classification Accordion (unified FIBO + Schema.org) -->
                {self._generate_semantic_classification_accordion(profile)}

                <!-- Quality Metrics Accordion -->
                {self._generate_quality_accordion(profile)}

                <!-- Overview Visualizations - Quality Radar (relocated from Advanced Visualizations) -->
                {overview_charts}

                <!-- Value Distribution Accordion -->
                {self._generate_distribution_accordion(profile, categorical_columns)}

                <!-- Distribution Visualizations (relocated from Advanced Visualizations) -->
                {distribution_charts}

                <!-- Temporal Analysis Accordion -->
                {self._generate_temporal_accordion(temporal_columns) if temporal_columns else ''}

                <!-- Temporal Visualizations (relocated from Advanced Visualizations) -->
                {temporal_charts}
            </div>
        </div>

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 4. DATA INSIGHTS - ML-powered patterns & anomalies              -->
        <!-- ═══════════════════════════════════════════════════════════════ -->

        <!-- PII Risk Section (if detected) - still important to highlight -->
        {self._generate_pii_section(pii_columns) if pii_count > 0 else ''}

        <!-- v2 Insight Widgets (Plain English + Examples + Technical) -->
        {self._generate_ml_section_v2(profile.ml_findings, profile.columns) if profile.ml_findings else ''}

        <!-- Anomaly Visualizations (relocated from Advanced Visualizations) -->
        {anomaly_charts}

        <!-- Missingness Visualizations (relocated from Advanced Visualizations) -->
        {missingness_charts}

        <!-- Column Relationships / Correlations -->
        {self._generate_correlations_section(profile.correlations) if profile.correlations else ''}

        <!-- Correlation Visualizations (relocated from Advanced Visualizations) -->
        {correlation_charts}

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

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 7. DATA LINEAGE & PROVENANCE                                    -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        {self._generate_lineage_section(profile) if profile.data_lineage else ''}

        <!-- ═══════════════════════════════════════════════════════════════ -->
        <!-- 13. GLOSSARY SECTION                                            -->
        <!-- ═══════════════════════════════════════════════════════════════ -->
        <div class="section-divider" id="section-glossary" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #374151 0%, #1f2937 100%); border-radius: 8px; border-left: 4px solid #9ca3af;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">📖 Glossary</h2>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #94a3b8;">Key terms and concepts used in this report</p>
                </div>
                <div style="background: rgba(255,255,255,0.1); padding: 4px 12px; border-radius: 4px; font-size: 0.8em; font-weight: 600; color: #94a3b8;">REFERENCE</div>
            </div>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <div class="accordion" data-accordion="glossary-terms">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #6b7280, #4b5563);">📚</div>
                            <div>
                                <div class="accordion-title">Data Quality & Analysis Terms</div>
                                <div class="accordion-subtitle">Understanding the metrics and methods in this report</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">Reference</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px;">
                            <!-- Data Quality Metrics -->
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border-left: 3px solid #3b82f6;">
                                <h4 style="margin: 0 0 12px 0; font-size: 0.9em; color: #3b82f6; text-transform: uppercase; letter-spacing: 0.5px;">Quality Metrics</h4>
                                <dl style="margin: 0; font-size: 0.85em;">
                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 8px;">Completeness</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Percentage of non-null values in a field. Higher is better for required fields.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Validity</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Percentage of values that match expected format/type. Based on pattern consistency.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Uniqueness</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Ratio of distinct values to total values. Important for ID fields (should be 100%).</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Consistency</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">How well values follow a single pattern/format. Measures data standardization.</dd>
                                </dl>
                            </div>

                            <!-- Statistical Terms -->
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border-left: 3px solid #8b5cf6;">
                                <h4 style="margin: 0 0 12px 0; font-size: 0.9em; color: #8b5cf6; text-transform: uppercase; letter-spacing: 0.5px;">Statistical Terms</h4>
                                <dl style="margin: 0; font-size: 0.85em;">
                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 8px;">IQR (Interquartile Range)</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Range between 25th and 75th percentiles. Values beyond 1.5×IQR are outliers.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Cardinality</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Number of unique values. Low cardinality (≤10) suggests categorical data.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Skewness</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Measure of distribution asymmetry. Positive = right tail, negative = left tail.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Kurtosis</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Measure of distribution "tailedness". High = more outliers than normal distribution.</dd>
                                </dl>
                            </div>

                            <!-- ML/Anomaly Terms -->
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border-left: 3px solid #ef4444;">
                                <h4 style="margin: 0 0 12px 0; font-size: 0.9em; color: #ef4444; text-transform: uppercase; letter-spacing: 0.5px;">Anomaly Detection</h4>
                                <dl style="margin: 0; font-size: 0.85em;">
                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 8px;">Isolation Forest</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">ML algorithm that isolates anomalies by random partitioning. Unusual points are easier to isolate.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Reconstruction Error</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">How poorly a model recreates a data point. High error = unusual pattern combination.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Benford's Law</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Natural distribution of leading digits. Real-world data follows this; deviations may indicate fraud.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">DBSCAN Clustering</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Groups similar records together. Points not fitting any cluster are labeled "noise" (potential anomalies).</dd>
                                </dl>
                            </div>

                            <!-- Semantic Terms -->
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border-left: 3px solid #10b981;">
                                <h4 style="margin: 0 0 12px 0; font-size: 0.9em; color: #10b981; text-transform: uppercase; letter-spacing: 0.5px;">Semantic Analysis</h4>
                                <dl style="margin: 0; font-size: 0.85em;">
                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 8px;">Semantic Type</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">The meaning/purpose of data (e.g., Email, Currency, Date) beyond its technical type.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">PII (Personally Identifiable Information)</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Data that can identify an individual: names, emails, SSN, phone numbers, addresses.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">Schema.org</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Web vocabulary for structured data. Used for standardized semantic type mapping.</dd>

                                    <dt style="font-weight: 600; color: var(--text-primary); margin-top: 12px;">FIBO (Financial Industry Business Ontology)</dt>
                                    <dd style="margin: 4px 0 0 0; color: var(--text-secondary);">Standard vocabulary for financial concepts. Used for finance-specific semantic types.</dd>
                                </dl>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </main>
    </div><!-- end dq-main-layout -->

    <script>
{self._get_javascript(profile, type_counts, categorical_columns)}
    </script>
</body>
</html>'''

        return html

    def _get_css(self) -> str:
        """Return the CSS styles - v3 dark-only strict layout design."""
        return '''        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            /* ═══════════════════════════════════════════════════════════════
               DARK-ONLY DESIGN TOKENS (v3)
               Single theme - no light mode, no toggle
            ═══════════════════════════════════════════════════════════════ */

            /* Core backgrounds - deep navy/slate palette */
            --dq-color-bg: #020617;
            --dq-color-bg-alt: #020617;
            --dq-color-surface: #0b1120;
            --dq-color-surface-alt: #111827;

            /* Legacy compatibility aliases */
            --bg-main: var(--dq-color-bg);
            --bg-elevated: var(--dq-color-surface);
            --bg-card: linear-gradient(180deg, var(--dq-color-surface) 0%, var(--dq-color-bg) 100%);
            --bg-hover: #1e293b;
            --bg-tertiary: #151d30;
            --card-bg: var(--dq-color-surface);

            /* Borders */
            --dq-color-border-subtle: #1f2937;
            --dq-color-border-strong: #374151;
            --border-subtle: rgba(148, 163, 184, 0.08);
            --border-color: rgba(148, 163, 184, 0.15);
            --border-focus: rgba(96, 165, 250, 0.4);

            /* Text colors */
            --dq-color-text-main: #e5e7eb;
            --dq-color-text-muted: #9ca3af;
            --dq-color-text-subtle: #6b7280;
            --dq-color-text-inverse: #020617;
            --text-primary: var(--dq-color-text-main);
            --text-secondary: #94a3b8;
            --text-tertiary: #64748b;
            --text-muted: var(--dq-color-text-subtle);

            /* Accent colors */
            --dq-color-accent: #60a5fa;
            --dq-color-accent-soft: rgba(37, 99, 235, 0.18);
            --dq-color-accent-strong: #3b82f6;
            --accent: var(--dq-color-accent);
            --accent-soft: var(--dq-color-accent-soft);
            --accent-gradient: linear-gradient(135deg, #3b82f6, #8b5cf6);
            --primary: #8b5cf6;

            /* Status colors */
            --dq-color-good: #22c55e;
            --dq-color-good-soft: rgba(34, 197, 94, 0.18);
            --dq-color-caution: #fbbf24;
            --dq-color-caution-soft: rgba(251, 191, 36, 0.18);
            --dq-color-danger: #f97373;
            --dq-color-danger-soft: rgba(248, 113, 113, 0.20);
            --good: var(--dq-color-good);
            --good-soft: var(--dq-color-good-soft);
            --warning: var(--dq-color-caution);
            --warning-soft: var(--dq-color-caution-soft);
            --critical: var(--dq-color-danger);
            --critical-soft: var(--dq-color-danger-soft);
            --info: var(--dq-color-accent-strong);
            --info-soft: rgba(59, 130, 246, 0.12);

            /* Shadows */
            --dq-shadow-soft: 0 1px 3px rgba(15, 23, 42, 0.50);
            --dq-shadow-medium: 0 8px 16px rgba(15, 23, 42, 0.70);
            --shadow-card: var(--dq-shadow-medium);
            --shadow-glow: 0 0 40px rgba(96, 165, 250, 0.08);

            /* Border radius */
            --dq-radius-sm: 4px;
            --dq-radius-md: 8px;
            --dq-radius-lg: 12px;
            --radius-sm: var(--dq-radius-sm);
            --radius-md: var(--dq-radius-md);
            --radius-lg: var(--dq-radius-lg);
            --radius-xl: 20px;

            /* Spacing */
            --dq-space-1: 4px;
            --dq-space-2: 8px;
            --dq-space-3: 12px;
            --dq-space-4: 16px;
            --dq-space-5: 20px;
            --dq-space-6: 24px;

            /* Typography */
            --dq-font-family-sans: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            --dq-font-size-xs: 11px;
            --dq-font-size-sm: 13px;
            --dq-font-size-md: 15px;
            --dq-font-size-lg: 18px;
            --dq-font-size-xl: 22px;
            --dq-line-height-tight: 1.15;
            --dq-line-height-normal: 1.4;
            --dq-line-height-relaxed: 1.6;

            /* Transitions */
            --transition-fast: 0.15s ease-out;
            --transition-med: 0.25s ease-out;

            /* Layout */
            --dq-sidebar-width: 260px;
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

        /* Accordion content - visible by default, hidden when collapsed */
        .accordion-body {
            display: block;
            border-top: 1px solid var(--border-subtle);
        }

        .accordion > .accordion-content {
            display: block;
            border-top: 1px solid var(--border-subtle);
        }

        /* Hide content when accordion has 'collapsed' class - Safari compatible */
        .accordion.collapsed .accordion-body {
            height: 0;
            overflow: hidden;
            padding: 0;
            border-top: none;
        }

        .accordion.collapsed > .accordion-content {
            height: 0;
            overflow: hidden;
            padding: 0;
            border-top: none;
        }

        .accordion.collapsed .accordion-chevron {
            transform: rotate(-90deg);
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
                grid-template-rows: auto auto auto auto;
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
            .column-quality-score {
                grid-column: 1 / -1;
                grid-row: 4;
                justify-self: end;
                margin-top: 8px;
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
           SIDEBAR NAVIGATION (v3 Layout)
           ====================================================== */
        .dq-main-layout {
            display: flex;
            min-height: calc(100vh - 60px);
        }

        .dq-sidebar {
            width: var(--dq-sidebar-width);
            flex-shrink: 0;
            background: var(--dq-color-surface);
            border-right: 1px solid var(--dq-color-border-subtle);
            position: sticky;
            top: 60px;
            height: calc(100vh - 60px);
            overflow-y: auto;
            overflow-x: hidden;
            scrollbar-width: thin;
            scrollbar-color: var(--dq-color-border-strong) transparent;
        }

        .dq-sidebar::-webkit-scrollbar {
            width: 6px;
        }

        .dq-sidebar::-webkit-scrollbar-track {
            background: transparent;
        }

        .dq-sidebar::-webkit-scrollbar-thumb {
            background: var(--dq-color-border-strong);
            border-radius: 3px;
        }

        .dq-sidebar-inner {
            padding: var(--dq-space-4);
        }

        .dq-sidebar-section {
            margin-bottom: var(--dq-space-5);
        }

        .dq-sidebar-label {
            font-size: var(--dq-font-size-xs);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--dq-color-text-subtle);
            padding: var(--dq-space-2) var(--dq-space-3);
            margin-bottom: var(--dq-space-1);
        }

        .dq-nav-list {
            list-style: none;
            margin: 0;
            padding: 0;
        }

        .dq-nav-item {
            margin: 0;
        }

        .dq-nav-link {
            display: flex;
            align-items: center;
            gap: var(--dq-space-2);
            padding: var(--dq-space-2) var(--dq-space-3);
            color: var(--dq-color-text-muted);
            text-decoration: none;
            font-size: var(--dq-font-size-sm);
            border-radius: var(--dq-radius-sm);
            transition: all var(--transition-fast);
            cursor: pointer;
            border: none;
            background: transparent;
            width: 100%;
            text-align: left;
        }

        .dq-nav-link:hover {
            background: var(--bg-hover);
            color: var(--dq-color-text-main);
        }

        .dq-nav-link.active {
            background: var(--dq-color-accent-soft);
            color: var(--dq-color-accent);
            font-weight: 500;
        }

        .dq-nav-link.active::before {
            content: '';
            position: absolute;
            left: 0;
            top: 50%;
            transform: translateY(-50%);
            width: 3px;
            height: 16px;
            background: var(--dq-color-accent);
            border-radius: 0 2px 2px 0;
        }

        .dq-nav-link .nav-icon {
            font-size: 1em;
            opacity: 0.7;
            width: 20px;
            text-align: center;
            flex-shrink: 0;
        }

        .dq-nav-link .nav-num {
            font-size: var(--dq-font-size-xs);
            color: var(--dq-color-text-subtle);
            width: 18px;
            text-align: right;
            flex-shrink: 0;
        }

        .dq-sidebar-actions {
            padding: var(--dq-space-4);
            border-top: 1px solid var(--dq-color-border-subtle);
            display: flex;
            flex-direction: column;
            gap: var(--dq-space-2);
        }

        .dq-main {
            flex: 1;
            min-width: 0;
            max-width: calc(100% - var(--dq-sidebar-width));
        }

        /* Mobile: hide sidebar, show mobile nav toggle */
        .dq-mobile-nav-toggle {
            display: none;
            position: fixed;
            bottom: 20px;
            right: 20px;
            z-index: 1000;
            width: 56px;
            height: 56px;
            border-radius: 50%;
            background: var(--dq-color-accent);
            color: white;
            border: none;
            cursor: pointer;
            box-shadow: var(--dq-shadow-medium);
            font-size: 1.5em;
            align-items: center;
            justify-content: center;
        }

        @media (max-width: 1024px) {
            .dq-sidebar {
                position: fixed;
                left: 0;
                top: 60px;
                z-index: 200;
                transform: translateX(-100%);
                transition: transform var(--transition-med);
            }

            .dq-sidebar.open {
                transform: translateX(0);
            }

            .dq-main {
                max-width: 100%;
            }

            .dq-mobile-nav-toggle {
                display: flex;
            }

            .dq-sidebar-backdrop {
                position: fixed;
                inset: 0;
                top: 60px;
                background: rgba(0, 0, 0, 0.5);
                z-index: 199;
                opacity: 0;
                visibility: hidden;
                transition: all var(--transition-med);
            }

            .dq-sidebar-backdrop.open {
                opacity: 1;
                visibility: visible;
            }
        }

        /* Legacy sticky-nav kept for backwards compatibility but hidden by default */
        .sticky-nav {
            display: none;
        }

        /* Action buttons in sidebar */
        .expand-all-btn {
            background: transparent;
            border: 1px solid var(--border-subtle);
            color: var(--text-muted);
            padding: 8px 12px;
            border-radius: var(--radius-md);
            font-size: var(--dq-font-size-sm);
            font-weight: 400;
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.2s ease;
            width: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
        }

        .expand-all-btn:hover {
            border-color: var(--accent);
            color: var(--text-secondary);
            background: var(--bg-hover);
        }

        .expand-all-btn.expanded {
            background: rgba(74, 144, 226, 0.1);
            border-color: var(--accent);
            color: var(--accent);
        }

        .expand-all-btn .expand-icon {
            font-weight: 600;
        }

        .export-pdf-btn {
            background: linear-gradient(135deg, #059669 0%, #10b981 100%);
            border: none;
            color: white;
            padding: 8px 14px;
            border-radius: var(--radius-md);
            font-size: var(--dq-font-size-sm);
            font-weight: 500;
            cursor: pointer;
            white-space: nowrap;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 6px;
            width: 100%;
        }

        .export-pdf-btn:hover {
            background: linear-gradient(135deg, #047857 0%, #059669 100%);
            transform: translateY(-1px);
            box-shadow: 0 4px 12px rgba(5, 150, 105, 0.3);
        }

        .export-pdf-btn:active {
            transform: translateY(0);
        }

        .export-pdf-btn.exporting {
            opacity: 0.7;
            pointer-events: none;
        }

        .pdf-icon {
            font-size: 1.1em;
        }

        /* Print/PDF styles */
        @media print {
            .top-nav, .sticky-nav, .export-pdf-btn, .expand-all-btn,
            .dq-sidebar, .dq-sidebar-backdrop, .dq-mobile-nav-toggle {
                display: none !important;
            }
            .dq-main-layout {
                display: block !important;
            }
            .dq-main {
                max-width: 100% !important;
            }
            .page {
                padding: 0 !important;
                margin: 0 !important;
            }
            .accordion, .column-row, details, .insight-technical {
                break-inside: avoid;
            }
            .accordion.open .accordion-content,
            .column-row.expanded .column-details,
            details[open] summary ~ *,
            .insight-technical.open {
                display: block !important;
                max-height: none !important;
                overflow: visible !important;
            }
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

        /* Semantic Classification Chips (unified FIBO + Schema.org) */
        .semantic-chip {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 16px;
            font-size: 0.85em;
            transition: all 0.2s;
        }

        .semantic-chip.fibo {
            background: rgba(5, 150, 105, 0.15);
            border: 1px solid rgba(16, 185, 129, 0.3);
        }

        .semantic-chip.fibo:hover {
            background: rgba(5, 150, 105, 0.25);
        }

        .semantic-chip.schema-org {
            background: rgba(59, 130, 246, 0.15);
            border: 1px solid rgba(99, 102, 241, 0.3);
        }

        .semantic-chip.schema-org:hover {
            background: rgba(59, 130, 246, 0.25);
        }

        .semantic-chip .chip-icon {
            font-size: 1em;
        }

        .semantic-chip .chip-label {
            font-weight: 600;
            color: var(--text-primary);
        }

        .semantic-chip .chip-count {
            background: rgba(255,255,255,0.2);
            color: var(--text-primary);
            font-size: 0.75em;
            font-weight: 700;
            padding: 1px 6px;
            border-radius: 8px;
        }

        .semantic-chip .chip-source {
            font-size: 0.7em;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .source-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75em;
            font-weight: 600;
        }

        .source-badge.fibo {
            background: linear-gradient(135deg, #059669 0%, #10b981 100%);
            color: white;
        }

        .source-badge.schema {
            background: linear-gradient(135deg, #3b82f6 0%, #6366f1 100%);
            color: white;
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
            cursor: pointer;
        }

        .insight-widget-header:hover {
            background: rgba(30, 41, 59, 0.5);
        }

        /* Add chevron to insight widget */
        .insight-widget-chevron {
            color: var(--text-muted);
            font-size: 0.8em;
            transition: transform 0.2s ease;
            margin-left: 12px;
        }

        .insight-widget.collapsed .insight-widget-chevron {
            transform: rotate(-90deg);
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
            overflow: hidden;
            max-height: 2000px;
            transition: max-height 0.3s ease;
        }

        .insight-widget.collapsed .insight-widget-body {
            height: 0;
            overflow: hidden;
            padding: 0 24px;
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
            content: '📘';
            font-size: 1.2em;
        }

        .insight-summary-text {
            font-size: 0.95em;
            line-height: 1.6;
            color: var(--text-secondary);
            padding: 14px 16px;
            background: rgba(var(--info-color-rgb), 0.08);
            border-radius: 0 8px 8px 0;
            border-left: 3px solid var(--info-color);
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
           DUAL-LAYER EXPLANATIONS (Details/Summary)
           ====================================================== */
        .dual-layer-explanation {
            margin-top: 16px;
        }

        .dual-layer-summary {
            background: rgba(var(--info-color-rgb), 0.08);
            border-left: 3px solid var(--info-color);
            padding: 12px 16px;
            border-radius: 0 8px 8px 0;
            margin-bottom: 12px;
        }

        .dual-layer-summary-label {
            font-size: 0.75em;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--info-color);
            font-weight: 600;
            margin-bottom: 6px;
        }

        .dual-layer-summary-text {
            font-size: 0.95em;
            color: var(--text-secondary);
            line-height: 1.6;
        }

        .dual-layer-technical {
            background: var(--bg-card);
            border-radius: 8px;
            border: 1px solid var(--border-subtle);
        }

        .dual-layer-technical summary {
            cursor: pointer;
            padding: 10px 14px;
            font-size: 0.8em;
            color: var(--text-muted);
            display: flex;
            align-items: center;
            gap: 8px;
            user-select: none;
        }

        .dual-layer-technical summary:hover {
            color: var(--text-secondary);
            background: rgba(255, 255, 255, 0.02);
        }

        .dual-layer-technical summary::marker {
            color: var(--accent);
        }

        .dual-layer-technical[open] summary {
            border-bottom: 1px solid var(--border-subtle);
        }

        .dual-layer-technical-content {
            padding: 14px;
        }

        .dual-layer-technical-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 10px;
            margin-bottom: 12px;
        }

        .dual-layer-technical-item {
            background: rgba(0, 0, 0, 0.2);
            padding: 8px 12px;
            border-radius: 6px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .dual-layer-technical-item-label {
            font-size: 0.75em;
            color: var(--text-muted);
        }

        .dual-layer-technical-item-value {
            font-size: 0.85em;
            font-weight: 500;
            color: var(--text-primary);
            font-family: 'SF Mono', monospace;
        }

        .dual-layer-technical-context {
            font-size: 0.8em;
            color: var(--text-muted);
            padding-top: 10px;
            border-top: 1px solid var(--border-subtle);
        }

        .dual-layer-technical-context ul {
            margin: 0;
            padding-left: 18px;
        }

        .dual-layer-technical-context li {
            margin-bottom: 4px;
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
        // Accordion toggle - uses 'collapsed' class (content visible by default)
        function toggleAccordion(header) {{
            const accordion = header.closest('.accordion');
            accordion.classList.toggle('collapsed');
        }}

        // Column row toggle
        function toggleColumnRow(row) {{
            row.classList.toggle('expanded');
        }}

        // ======================================================
        // EXPAND ALL TOGGLE
        // ======================================================
        let allExpanded = false;  // Starts collapsed by default

        function toggleExpandAll() {{
            const btn = document.getElementById('expandAllBtn');
            allExpanded = !allExpanded;

            // Update button appearance
            btn.classList.toggle('expanded', allExpanded);
            btn.innerHTML = allExpanded
                ? '<span class="expand-icon">-</span> Collapse All'
                : '<span class="expand-icon">+</span> Expand All';

            // Toggle all accordions (using 'collapsed' class)
            document.querySelectorAll('.accordion').forEach(acc => {{
                if (allExpanded) {{
                    acc.classList.remove('collapsed');
                }} else {{
                    acc.classList.add('collapsed');
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
        // PDF EXPORT
        // ======================================================
        function exportToPDF() {{
            const btn = document.getElementById('exportPdfBtn');
            const originalText = btn.innerHTML;

            // Show loading state
            btn.classList.add('exporting');
            btn.innerHTML = '<span class="pdf-icon">⏳</span> Preparing...';

            // First, expand all sections to ensure nothing is hidden
            // Expand all accordions
            document.querySelectorAll('.accordion').forEach(acc => {{
                acc.classList.add('open');
            }});

            // Expand all column rows
            document.querySelectorAll('.column-row').forEach(row => {{
                row.classList.add('expanded');
            }});

            // Open all <details> elements
            document.querySelectorAll('details').forEach(details => {{
                details.open = true;
            }});

            // Expand all technical sections
            document.querySelectorAll('.insight-technical').forEach(section => {{
                section.classList.add('open');
            }});

            // Switch all dual-view tabs to show both Plain English content
            // (Technical is still available in print via CSS)
            document.querySelectorAll('.plain-view').forEach(view => {{
                view.style.display = 'block';
            }});
            document.querySelectorAll('.tech-view').forEach(view => {{
                view.style.display = 'block';
            }});

            // Update expand button state
            const expandBtn = document.getElementById('expandAllBtn');
            allExpanded = true;
            expandBtn.classList.add('expanded');
            expandBtn.innerHTML = '<span class="expand-icon">-</span> Collapse All';

            // Small delay to let DOM update, then trigger print
            setTimeout(() => {{
                // Create print-specific styles
                const printStyle = document.createElement('style');
                printStyle.id = 'pdf-print-style';
                printStyle.textContent = `
                    @media print {{
                        * {{ -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }}
                        body {{ background: white !important; }}
                        .page {{ max-width: none !important; padding: 20px !important; }}
                        .accordion-content {{ display: block !important; max-height: none !important; }}
                        .column-details {{ display: block !important; max-height: none !important; }}
                        .insight-technical {{ display: block !important; max-height: none !important; }}
                        .plain-view, .tech-view {{ display: block !important; }}
                        .dual-view-tabs {{ display: none !important; }}
                        .nav-btn, .expand-all-btn, .export-pdf-btn {{ display: none !important; }}
                        canvas {{ max-width: 100% !important; height: auto !important; }}
                    }}
                `;
                document.head.appendChild(printStyle);

                // Trigger print dialog (saves as PDF)
                window.print();

                // Clean up after print
                setTimeout(() => {{
                    const style = document.getElementById('pdf-print-style');
                    if (style) style.remove();

                    // Restore button
                    btn.classList.remove('exporting');
                    btn.innerHTML = originalText;
                }}, 500);
            }}, 300);
        }}

        // ======================================================
        // SIDEBAR NAVIGATION
        // ======================================================
        const sidebar = document.getElementById('dqSidebar');
        const backdrop = document.getElementById('sidebarBackdrop');
        const navLinks = document.querySelectorAll('.dq-nav-link');

        // Section IDs mapped to navigation
        const sections = {{
            'overview': document.getElementById('section-overview') || document.getElementById('section-summary'),
            'engine': document.getElementById('section-engine'),
            'structure': document.getElementById('section-structure'),
            'columns': document.getElementById('section-columns'),
            'distributions': document.getElementById('section-distributions'),
            'missingness': document.getElementById('section-missingness'),
            'anomalies': document.getElementById('section-anomalies'),
            'temporal': document.getElementById('section-temporal'),
            'correlations': document.getElementById('section-correlations'),
            'validations': document.getElementById('section-validations'),
            'yaml': document.getElementById('section-yaml'),
            'nextsteps': document.getElementById('section-nextsteps'),
            'glossary': document.getElementById('section-glossary')
        }};

        // Toggle sidebar for mobile
        function toggleSidebar() {{
            sidebar.classList.toggle('open');
            backdrop.classList.toggle('open');
        }}

        // Close sidebar on mobile when clicking a link
        function closeSidebarOnMobile() {{
            if (window.innerWidth <= 1024) {{
                sidebar.classList.remove('open');
                backdrop.classList.remove('open');
            }}
        }}

        // Handle nav link clicks
        navLinks.forEach(link => {{
            link.addEventListener('click', function() {{
                const sectionId = this.dataset.section;
                const section = sections[sectionId];

                // Update active state
                navLinks.forEach(l => l.classList.remove('active'));
                this.classList.add('active');

                if (section) {{
                    // Calculate offset for mobile (account for mobile toggle button)
                    const isMobile = window.innerWidth <= 1024;
                    const offset = isMobile ? 70 : 20; // Mobile needs more offset for floating button

                    // Smooth scroll with offset
                    const targetPosition = section.getBoundingClientRect().top + window.pageYOffset - offset;
                    window.scrollTo({{
                        top: targetPosition,
                        behavior: 'smooth'
                    }});
                }}

                closeSidebarOnMobile();
            }});
        }});

        // Update nav on scroll (highlight active section)
        let scrollTimeout;
        window.addEventListener('scroll', function() {{
            clearTimeout(scrollTimeout);
            scrollTimeout = setTimeout(function() {{
                let currentSection = 'overview';
                const scrollPos = window.scrollY + 150;

                for (const [name, el] of Object.entries(sections)) {{
                    if (el && el.offsetTop <= scrollPos) {{
                        currentSection = name;
                    }}
                }}

                navLinks.forEach(link => {{
                    link.classList.toggle('active', link.dataset.section === currentSection);
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

        // Collapse all accordions after page load (Safari-friendly: render first, then collapse)
        function collapseAllAccordions() {{
            document.querySelectorAll('.accordion').forEach(acc => {{
                acc.classList.add('collapsed');
            }});
            document.querySelectorAll('.insight-widget').forEach(widget => {{
                widget.classList.add('collapsed');
            }});
        }}
        // Run after DOM is ready but content has rendered
        if (document.readyState === 'complete') {{
            setTimeout(collapseAllAccordions, 100);
        }} else {{
            window.addEventListener('load', () => setTimeout(collapseAllAccordions, 100));
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

            # Numeric low cardinality - may indicate categorical coding or flag
            if (col.type_info.inferred_type in ['integer', 'float', 'number'] and
                col.statistics.unique_count > 1 and
                col.statistics.unique_count <= 20 and
                not is_id_column):
                alerts.append({
                    'severity': 'info',
                    'icon': '🔢',
                    'column': col.name,
                    'issue': 'Numeric Low-Cardinality',
                    'detail': f'Only {col.statistics.unique_count} distinct values - may represent categories, ratings, or encoded labels. Consider ValidValuesCheck.'
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
            'Low Cardinality': 10,  # Deprioritize - very common and often expected
            'Numeric Low-Cardinality': 11  # Deprioritize - informational only
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

    def _generate_correlations_section(self, correlations: List) -> str:
        """Generate column correlations/associations section with dual-view."""
        if not correlations:
            return ''

        # Deduplicate correlations (keep strongest per pair)
        seen = {}
        for c in correlations:
            if not isinstance(c, dict):
                # Handle CorrelationResult objects
                c = {
                    'column1': getattr(c, 'column1', ''),
                    'column2': getattr(c, 'column2', ''),
                    'correlation': getattr(c, 'correlation', 0),
                    'strength': getattr(c, 'strength', ''),
                    'direction': getattr(c, 'direction', ''),
                    'type': getattr(c, 'type', 'pearson'),
                }
            key = tuple(sorted([c.get('column1', ''), c.get('column2', '')]))
            corr_val = abs(c.get('correlation', 0))
            if key not in seen or corr_val > abs(seen[key].get('correlation', 0)):
                seen[key] = c

        # Sort by absolute correlation strength
        unique_correlations = sorted(seen.values(), key=lambda x: abs(x.get('correlation', 0)), reverse=True)

        if not unique_correlations:
            return ''

        # Build correlation items with plain English and technical views
        items_html = []
        for rank, c in enumerate(unique_correlations, 1):  # Show all correlations with rank
            col1 = c.get('column1', '')
            col2 = c.get('column2', '')
            corr = c.get('correlation', 0)
            strength = c.get('strength', 'moderate') or 'moderate'
            direction = c.get('direction', 'positive' if corr > 0 else 'negative')
            method = c.get('type', 'pearson')

            # Color based on strength
            if abs(corr) >= 0.7:
                color = '#dc2626' if corr < 0 else '#059669'
                bg = 'rgba(220, 38, 38, 0.08)' if corr < 0 else 'rgba(5, 150, 105, 0.08)'
                strength_label = 'Strong'
            elif abs(corr) >= 0.5:
                color = '#ea580c' if corr < 0 else '#0284c7'
                bg = 'rgba(234, 88, 12, 0.08)' if corr < 0 else 'rgba(2, 132, 199, 0.08)'
                strength_label = 'Moderate'
            else:
                color = '#6b7280'
                bg = 'rgba(107, 114, 128, 0.08)'
                strength_label = 'Weak'

            arrow = '↓' if corr < 0 else '↑'

            # Plain English explanation
            if corr < 0:
                plain_english = f"When <strong>{col1}</strong> increases, <strong>{col2}</strong> tends to decrease (and vice versa)."
                business_insight = "These columns move in opposite directions - this inverse relationship may indicate a trade-off or constraint in your data."
            else:
                plain_english = f"When <strong>{col1}</strong> increases, <strong>{col2}</strong> also tends to increase."
                business_insight = "These columns move together - they may be derived from the same source, or one may influence the other."

            # Technical details
            r_squared = corr ** 2
            variance_explained = f"{r_squared * 100:.1f}%"

            items_html.append(f'''
                <div class="correlation-card" style="background: var(--bg-card); border-radius: 12px; border-left: 4px solid {color}; margin-bottom: 16px; overflow: hidden; position: relative; border: 1px solid var(--border-subtle);">
                    <!-- Rank Badge -->
                    <div style="position: absolute; top: 12px; right: 12px; background: {color}; color: white; width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 0.85em;">#{rank}</div>
                    <!-- Header -->
                    <div style="padding: 16px 20px; display: flex; align-items: center; gap: 14px;">
                        <div style="font-size: 1.8em; color: {color};">{arrow}</div>
                        <div style="flex: 1;">
                            <div style="font-weight: 700; color: var(--text-primary); font-size: 1.1em;">
                                {col1} <span style="color: var(--text-muted); font-weight: 400;">↔</span> {col2}
                            </div>
                            <div style="font-size: 0.85em; color: var(--text-secondary); margin-top: 4px;">
                                {strength_label} {direction} relationship
                            </div>
                        </div>
                        <div style="text-align: right;">
                            <div style="font-weight: 800; font-size: 1.4em; color: {color};">{corr:+.2f}</div>
                            <div style="font-size: 0.75em; color: var(--text-muted);">correlation</div>
                        </div>
                    </div>

                    <!-- Dual View Tabs -->
                    <div style="border-top: 1px solid var(--border-subtle);">
                        <div class="dual-view-tabs" style="display: flex; border-bottom: 1px solid var(--border-subtle);">
                            <button class="tab-btn active" onclick="this.parentElement.nextElementSibling.querySelector('.plain-view').style.display='block'; this.parentElement.nextElementSibling.querySelector('.tech-view').style.display='none'; this.classList.add('active'); this.nextElementSibling.classList.remove('active');"
                                style="flex: 1; padding: 10px; border: none; background: var(--bg-card); cursor: pointer; font-size: 0.85em; font-weight: 600; color: #818cf8; border-bottom: 2px solid #818cf8;">
                                📝 Plain English
                            </button>
                            <button class="tab-btn" onclick="this.parentElement.nextElementSibling.querySelector('.plain-view').style.display='none'; this.parentElement.nextElementSibling.querySelector('.tech-view').style.display='block'; this.classList.add('active'); this.previousElementSibling.classList.remove('active');"
                                style="flex: 1; padding: 10px; border: none; background: var(--bg-primary); cursor: pointer; font-size: 0.85em; font-weight: 500; color: var(--text-secondary); border-bottom: 2px solid transparent;">
                                🔧 Technical Details
                            </button>
                        </div>
                        <div class="tab-content" style="padding: 16px 20px; background: var(--bg-card);">
                            <!-- Plain English View -->
                            <div class="plain-view">
                                <p style="margin: 0 0 10px 0; color: var(--text-primary); line-height: 1.6;">{plain_english}</p>
                                <div style="background: rgba(245, 158, 11, 0.15); padding: 12px; border-radius: 8px; border-left: 3px solid #f59e0b;">
                                    <div style="font-size: 0.8em; color: #fbbf24; font-weight: 600; margin-bottom: 4px;">💡 What This Means</div>
                                    <p style="margin: 0; font-size: 0.9em; color: var(--text-secondary);">{business_insight}</p>
                                </div>
                            </div>
                            <!-- Technical View -->
                            <div class="tech-view" style="display: none;">
                                <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px;">
                                    <div style="background: var(--bg-primary); padding: 12px; border-radius: 8px; border: 1px solid var(--border-subtle);">
                                        <div style="font-size: 0.75em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px;">Coefficient</div>
                                        <div style="font-size: 1.2em; font-weight: 700; color: var(--text-primary);">{corr:+.4f}</div>
                                    </div>
                                    <div style="background: var(--bg-primary); padding: 12px; border-radius: 8px; border: 1px solid var(--border-subtle);">
                                        <div style="font-size: 0.75em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px;">R² (Variance Explained)</div>
                                        <div style="font-size: 1.2em; font-weight: 700; color: var(--text-primary);">{variance_explained}</div>
                                    </div>
                                    <div style="background: var(--bg-primary); padding: 12px; border-radius: 8px; border: 1px solid var(--border-subtle);">
                                        <div style="font-size: 0.75em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px;">Method</div>
                                        <div style="font-size: 1.2em; font-weight: 700; color: var(--text-primary);">{method.title()}</div>
                                    </div>
                                    <div style="background: var(--bg-primary); padding: 12px; border-radius: 8px; border: 1px solid var(--border-subtle);">
                                        <div style="font-size: 0.75em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px;">Strength</div>
                                        <div style="font-size: 1.2em; font-weight: 700; color: {color};">{strength_label}</div>
                                    </div>
                                </div>
                                <div style="margin-top: 12px; padding: 10px 12px; background: var(--bg-primary); border-radius: 6px; border: 1px solid var(--border-subtle);">
                                    <code style="font-size: 0.8em; color: var(--text-secondary);">corr({col1}, {col2}) = {corr:+.4f}</code>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            ''')

        return f'''
        <div class="accordion" id="section-correlations" data-accordion="correlations" style="margin-top: 24px;">
            <div class="accordion-header" onclick="toggleAccordion(this)" style="background: linear-gradient(135deg, #4338ca 0%, #6366f1 100%); color: white; padding: 16px 20px; display: flex; align-items: center; gap: 12px; border-radius: var(--radius-lg) var(--radius-lg) 0 0;">
                <span style="font-size: 1.5em;">🔗</span>
                <div style="flex: 1;">
                    <h3 style="margin: 0; font-size: 1.1em; font-weight: 600;">Correlations & Relationships</h3>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; opacity: 0.9;">{len(unique_correlations)} significant correlation(s) detected</p>
                </div>
                <div style="display: flex; gap: 8px; font-size: 0.75em; align-items: center;">
                    <span style="background: rgba(255,255,255,0.2); padding: 4px 10px; border-radius: 12px;">↑ Positive</span>
                    <span style="background: rgba(255,255,255,0.2); padding: 4px 10px; border-radius: 12px;">↓ Negative</span>
                    <span class="accordion-chevron" style="color: white;">▼</span>
                </div>
            </div>
            <div class="accordion-content" style="background: var(--bg-card); border: 1px solid var(--border-subtle); border-top: none; border-radius: 0 0 var(--radius-lg) var(--radius-lg);">
                <div style="background: rgba(67, 56, 202, 0.1); border: 1px solid rgba(99, 102, 241, 0.3); border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; display: flex; align-items: center; gap: 10px;">
                    <span style="font-size: 1.2em;">📊</span>
                    <p style="margin: 0; font-size: 0.9em; color: var(--text-secondary);">
                        <strong style="color: var(--text-primary);">Ranked by correlation strength</strong> - Strongest relationships appear first.
                        Values closer to +1 or -1 indicate stronger associations between columns.
                    </p>
                </div>
                <div style="display: flex; flex-direction: column; gap: 0;">
                    {''.join(items_html)}
                </div>
            </div>
        </div>
        '''

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
        <div class="accordion pii-alert" data-accordion="pii" style="border: 2px solid var(--critical); background: linear-gradient(135deg, rgba(239, 68, 68, 0.1) 0%, var(--bg-card) 100%);">
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

                    <details class="dual-layer-technical" style="margin-top: 16px;">
                        <summary>🧠 PII Detection Logic (click to expand)</summary>
                        <div class="dual-layer-technical-content">
                            <div class="dual-layer-technical-context">
                                <p style="margin-bottom: 12px; color: var(--text-secondary);">The following semantic types are flagged as PII:</p>
                                <ul>
                                    <li><strong>Direct Identifiers:</strong> Email, Phone, SSN/National ID, Passport, Driver's License</li>
                                    <li><strong>Financial:</strong> Credit Card, Bank Account, Tax ID</li>
                                    <li><strong>Personal:</strong> Name, Birth Date, Address, Medical Record Number</li>
                                    <li><strong>Biometric:</strong> IP Address, Geolocation, Device ID</li>
                                </ul>
                                <p style="margin-top: 12px; color: var(--text-secondary);">Detection uses pattern matching (regex), column name heuristics, and semantic type inference. Risk scores (0-100) are calculated based on re-identification potential and regulatory sensitivity (GDPR, CCPA, HIPAA).</p>
                            </div>
                        </div>
                    </details>
                </div>
            </div>
        </div>'''

    def _generate_lineage_banner(self, profile: ProfileResult) -> str:
        """
        Generate a condensed lineage banner for the report header.

        Shows key provenance info at a glance with expandable details.
        """
        lineage = profile.data_lineage
        if not lineage:
            return ''

        # Format key info
        source_file = lineage.source_path.split("/")[-1] if lineage.source_path and "/" in lineage.source_path else (lineage.source_path or 'Unknown')
        hash_short = lineage.source_hash[:12] + '...' if lineage.source_hash else 'N/A'
        hash_full = lineage.source_hash or 'N/A'
        profiled_at = lineage.profiled_at[:16] if lineage.profiled_at else 'Unknown'
        version = lineage.profiler_version or 'Unknown'
        env = lineage.environment or {}
        hostname = env.get('hostname', 'Unknown')

        # Count analysis types
        analysis_count = len(lineage.analysis_applied) if lineage.analysis_applied else 0

        # Analysis badges (condensed)
        analysis_badges = ''
        if lineage.analysis_applied:
            badges = lineage.analysis_applied[:4]  # Show first 4
            more_count = len(lineage.analysis_applied) - 4
            analysis_badges = ''.join([
                f'<span style="background: rgba(167,139,250,0.15); color: #a78bfa; padding: 2px 6px; border-radius: 3px; font-size: 0.7em; margin-right: 4px;">{a.replace("_", " ").title()}</span>'
                for a in badges
            ])
            if more_count > 0:
                analysis_badges += f'<span style="color: var(--text-muted); font-size: 0.7em;">+{more_count} more</span>'

        return f'''
        <div class="lineage-banner" style="margin: 12px 0; background: linear-gradient(135deg, rgba(76, 29, 149, 0.15) 0%, rgba(46, 16, 101, 0.1) 100%); border: 1px solid rgba(167, 139, 250, 0.3); border-radius: 8px; overflow: hidden;">
            <div class="lineage-banner-header" onclick="this.parentElement.classList.toggle('expanded')" style="padding: 10px 16px; cursor: pointer; display: flex; justify-content: space-between; align-items: center;">
                <div style="display: flex; align-items: center; gap: 16px; flex-wrap: wrap;">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 1em;">🔗</span>
                        <span style="font-weight: 600; color: #a78bfa; font-size: 0.85em;">Data Lineage</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 12px; font-size: 0.75em; color: var(--text-secondary);">
                        <span title="Source file"><span style="color: var(--text-muted);">Source:</span> <span style="color: var(--text-primary); font-family: monospace;">{source_file}</span></span>
                        <span style="color: var(--border-color);">|</span>
                        <span title="SHA-256 hash for integrity verification"><span style="color: var(--text-muted);">Hash:</span> <span style="color: #a78bfa; font-family: monospace;">{hash_short}</span></span>
                        <span style="color: var(--border-color);">|</span>
                        <span><span style="color: var(--text-muted);">Profiled:</span> {profiled_at}</span>
                    </div>
                </div>
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="background: rgba(167,139,250,0.2); color: #a78bfa; padding: 2px 8px; border-radius: 4px; font-size: 0.7em; font-weight: 600;">{analysis_count} ANALYSES</span>
                    <span class="lineage-chevron" style="color: #a78bfa; font-size: 0.8em; transition: transform 0.2s;">▼</span>
                </div>
            </div>
            <div class="lineage-banner-content" style="display: none; padding: 0 16px 12px 16px; border-top: 1px solid rgba(167, 139, 250, 0.2);">
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; padding-top: 12px; font-size: 0.8em;">
                    <div>
                        <div style="color: var(--text-muted); font-size: 0.85em; margin-bottom: 4px;">Full SHA-256 Hash</div>
                        <div style="font-family: monospace; font-size: 0.75em; color: #a78bfa; word-break: break-all; background: rgba(0,0,0,0.2); padding: 6px 8px; border-radius: 4px;">{hash_full}</div>
                    </div>
                    <div>
                        <div style="color: var(--text-muted); font-size: 0.85em; margin-bottom: 4px;">Environment</div>
                        <div style="color: var(--text-primary);">DataK9 v{version} on {hostname}</div>
                    </div>
                    <div style="grid-column: span 2;">
                        <div style="color: var(--text-muted); font-size: 0.85em; margin-bottom: 4px;">Analysis Applied</div>
                        <div>{analysis_badges}</div>
                    </div>
                </div>
                <div style="margin-top: 10px; text-align: right;">
                    <a href="#section-lineage" style="color: #a78bfa; font-size: 0.75em; text-decoration: none;">View full lineage details →</a>
                </div>
            </div>
        </div>
        <style>
            .lineage-banner.expanded .lineage-banner-content {{ display: block !important; }}
            .lineage-banner.expanded .lineage-chevron {{ transform: rotate(180deg); }}
        </style>
        '''

    def _generate_lineage_section(self, profile: ProfileResult) -> str:
        """
        Generate the Data Lineage & Provenance section.

        Displays source tracking, integrity verification, and analysis audit trail
        for regulatory compliance and data governance.
        """
        lineage = profile.data_lineage
        if not lineage:
            return ''

        # Format source info
        source_path = lineage.source_path or 'Unknown'
        source_type = lineage.source_type.upper() if lineage.source_type else 'FILE'
        source_hash_short = lineage.source_hash[:16] + '...' if lineage.source_hash else 'Not computed'
        source_hash_full = lineage.source_hash or 'N/A'

        # Format file size
        size_bytes = lineage.source_size_bytes or 0
        if size_bytes >= 1024 * 1024 * 1024:
            size_display = f"{size_bytes / (1024*1024*1024):.2f} GB"
        elif size_bytes >= 1024 * 1024:
            size_display = f"{size_bytes / (1024*1024):.2f} MB"
        elif size_bytes >= 1024:
            size_display = f"{size_bytes / 1024:.2f} KB"
        else:
            size_display = f"{size_bytes} bytes"

        # Format timestamps
        profiled_at = lineage.profiled_at or 'Unknown'
        source_modified = lineage.source_modified_at or 'Unknown'

        # Environment info
        env = lineage.environment or {}
        hostname = env.get('hostname', 'Unknown')
        os_info = env.get('os', 'Unknown')
        python_version = env.get('python_version', 'Unknown')
        profiler_version = lineage.profiler_version or 'Unknown'

        # Analysis applied
        analysis_list = lineage.analysis_applied or []
        analysis_badges = ''.join([
            f'<span style="display: inline-block; background: rgba(59, 130, 246, 0.15); color: #60a5fa; padding: 4px 10px; border-radius: 4px; font-size: 0.75em; margin: 2px 4px 2px 0; font-weight: 500;">{a.replace("_", " ").title()}</span>'
            for a in analysis_list
        ])

        # Sampling info
        sampling_html = ''
        if lineage.sampling_info:
            s = lineage.sampling_info
            sampling_html = f'''
                <div style="margin-top: 16px; padding: 12px; background: rgba(245, 158, 11, 0.08); border-radius: 6px; border-left: 3px solid #f59e0b;">
                    <div style="font-weight: 600; color: #f59e0b; font-size: 0.85em; margin-bottom: 8px;">⚡ Sampling Applied</div>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 8px; font-size: 0.8em;">
                        <div><span style="color: var(--text-muted);">Total Rows:</span> <span style="color: var(--text-primary); font-weight: 500;">{s.get("total_rows", 0):,}</span></div>
                        <div><span style="color: var(--text-muted);">Sampled:</span> <span style="color: var(--text-primary); font-weight: 500;">{s.get("sampled_rows", 0):,}</span></div>
                        <div><span style="color: var(--text-muted);">Coverage:</span> <span style="color: var(--text-primary); font-weight: 500;">{s.get("sampling_percentage", 0)}%</span></div>
                        <div><span style="color: var(--text-muted);">Strategy:</span> <span style="color: var(--text-primary); font-weight: 500;">{s.get("sampling_strategy", "intelligent")}</span></div>
                    </div>
                </div>
            '''

        return f'''
        <div class="section-divider" id="section-lineage" style="margin: 24px 0 16px 0; padding: 12px 20px; background: linear-gradient(135deg, #4c1d95 0%, #2e1065 100%); border-radius: 8px; border-left: 4px solid #a78bfa;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <div>
                    <h2 style="margin: 0; font-size: 1.1em; color: #f1f5f9; font-weight: 600;">Data Lineage & Provenance</h2>
                    <p style="margin: 4px 0 0 0; font-size: 0.85em; color: #c4b5fd;">Audit trail for compliance, governance, and traceability</p>
                </div>
                <div style="background: rgba(255,255,255,0.15); padding: 4px 12px; border-radius: 4px; font-size: 0.8em; font-weight: 600; color: white;">AUDIT</div>
            </div>
        </div>

        <div class="layout-grid">
            <div class="main-column">
                <div class="accordion" data-accordion="lineage" style="border-left: 3px solid #a78bfa;">
                    <div class="accordion-header" onclick="toggleAccordion(this)" style="background: var(--bg-card);">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #7c3aed, #5b21b6);">🔗</div>
                            <div>
                                <div class="accordion-title">Source Provenance & Integrity</div>
                                <div class="accordion-subtitle">Where this data came from and verification hash</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge" style="background: rgba(167, 139, 250, 0.15); color: #a78bfa;">{source_type}</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content" style="display: block;">
                        <!-- Source Information -->
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 16px;">
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-color);">
                                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 12px; font-size: 0.9em;">📁 Source Details</div>
                                <div style="font-size: 0.8em; line-height: 1.8;">
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                                        <span style="color: var(--text-muted);">Path</span>
                                        <span style="color: var(--text-primary); font-family: monospace; font-size: 0.85em; max-width: 200px; overflow: hidden; text-overflow: ellipsis;" title="{source_path}">{source_path.split("/")[-1] if "/" in source_path else source_path}</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                                        <span style="color: var(--text-muted);">Size</span>
                                        <span style="color: var(--text-primary); font-weight: 500;">{size_display}</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                                        <span style="color: var(--text-muted);">Modified</span>
                                        <span style="color: var(--text-primary);">{source_modified}</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0;">
                                        <span style="color: var(--text-muted);">Type</span>
                                        <span style="color: var(--text-primary);">{source_type}</span>
                                    </div>
                                </div>
                            </div>

                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-color);">
                                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 12px; font-size: 0.9em;">🔐 Integrity Verification</div>
                                <div style="font-size: 0.8em;">
                                    <div style="color: var(--text-muted); margin-bottom: 4px;">SHA-256 Hash</div>
                                    <div style="background: rgba(0,0,0,0.3); padding: 8px 12px; border-radius: 4px; font-family: monospace; font-size: 0.85em; color: #a78bfa; word-break: break-all;" title="{source_hash_full}">{source_hash_full}</div>
                                    <div style="margin-top: 8px; color: var(--text-muted); font-size: 0.75em;">Use this hash to verify data integrity and detect tampering</div>
                                </div>
                            </div>
                        </div>

                        <!-- Processing Info -->
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px;">
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-color);">
                                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 12px; font-size: 0.9em;">⏱️ Processing Record</div>
                                <div style="font-size: 0.8em; line-height: 1.8;">
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                                        <span style="color: var(--text-muted);">Profiled At</span>
                                        <span style="color: var(--text-primary);">{profiled_at}</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                                        <span style="color: var(--text-muted);">Profiler Version</span>
                                        <span style="color: var(--text-primary);">DataK9 v{profiler_version}</span>
                                    </div>
                                    <div style="display: flex; justify-content: space-between; padding: 4px 0;">
                                        <span style="color: var(--text-muted);">Host</span>
                                        <span style="color: var(--text-primary);">{hostname}</span>
                                    </div>
                                </div>
                            </div>

                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-color);">
                                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 12px; font-size: 0.9em;">🔬 Analysis Applied</div>
                                <div style="font-size: 0.8em;">
                                    {analysis_badges if analysis_badges else '<span style="color: var(--text-muted);">No advanced analysis applied</span>'}
                                </div>
                            </div>
                        </div>

                        {sampling_html}

                        <!-- Export for Audit -->
                        <div style="margin-top: 16px; padding: 12px; background: rgba(167, 139, 250, 0.08); border-radius: 6px; border: 1px dashed rgba(167, 139, 250, 0.3);">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div>
                                    <div style="font-size: 0.85em; font-weight: 500; color: var(--text-primary);">💾 Audit Export</div>
                                    <div style="font-size: 0.75em; color: var(--text-muted);">Full lineage data is included in JSON output for audit trails and compliance reporting</div>
                                </div>
                                <span style="background: rgba(167, 139, 250, 0.2); color: #a78bfa; padding: 4px 10px; border-radius: 4px; font-size: 0.7em; font-weight: 600;">JSON EXPORT</span>
                            </div>
                        </div>
                    </div>
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
            risk_items.append(f'<span style="color: #ef4444;">Extreme Outliers ({pct:.2f}%)</span>')
        if ae_count > 0:
            pct = autoencoder.get('anomaly_percentage', 0)
            risk_items.append(f'<span style="color: #8b5cf6;">Multi-Column Anomalies ({pct:.2f}%)</span>')
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
                        action = 'Investigate data source for entry errors, unit inconsistencies, or data import issues.'
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
                    ae_impact = 'Significant portion of data has unusual multi-field patterns. May indicate systematic data issues or process anomalies.'
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
                        interpretation = 'First-digit frequencies deviate from Benford distribution'
                        benford_impact = 'For naturally occurring transactional datasets this may indicate anomalies, but for pricing or tariff tables it can simply reflect business structure.'
                        benford_action = 'Consider the nature of this data before investigating further. Pricing, structured rates, or bounded values often deviate naturally.'
                    else:
                        interpretation = 'Digit distribution follows natural patterns (Benford\'s Law) - consistent with organically-generated data'
                        benford_impact = 'Data shows natural patterns consistent with real-world measurements and counts.'
                        benford_action = 'No action needed. This is a positive indicator of data authenticity.'

                    status_text = "Benford deviation (p < 0.05)" if is_suspicious else "Follows Benford (p ≥ 0.05)"
                    details_html += f'''
                        <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid {"#f59e0b" if is_suspicious else "#22c55e"};">
                            <div style="font-weight: 600; margin-bottom: 6px;">{col}: {"Does not follow Benford" if is_suspicious else "Follows Benford"}</div>
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
                    cluster_impact = f'{noise_pct:.2f}% of records are outliers that don\'t fit any natural group. These may need special handling.'
                    cluster_action = 'Review noise points for data quality issues or create separate processing rules for edge cases.'
                else:
                    cluster_impact = f'Data has clear structure with {n_clusters} natural groupings. Low noise indicates consistent data patterns.'
                    cluster_action = 'Consider using clusters for segmentation analysis or targeted validation rules.'

                details_html += f'''
                    <div style="background: var(--card-bg); border-radius: 8px; padding: 14px; margin-bottom: 10px; border-left: 4px solid #8b5cf6;">
                        <div style="font-weight: 600; margin-bottom: 6px;">Natural Data Clustering</div>
                        <div style="font-size: 0.9em; color: var(--text-secondary); margin-bottom: 6px;">Data naturally forms {n_clusters} distinct groups • {noise_points:,} noise points ({noise_pct:.2f}%)</div>
                        <div style="font-size: 0.85em; color: var(--text-muted); margin-bottom: 6px;">
                            <strong style="color: var(--text-secondary);">Impact:</strong> {cluster_impact}
                        </div>
                        <div style="font-size: 0.85em; color: #22c55e;">
                            <strong>Action:</strong> {cluster_action}
                        </div>
                    </div>'''

        return f'''
        <div class="accordion" data-accordion="data-insights">
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

    def _generate_ml_section_v2(self, ml_findings: Dict, columns: list = None) -> str:
        """
        Generate v2 Data Insights section with masterpiece insight widgets.

        New design features:
        1. Plain English summary (always visible)
        2. Example table with real data (always visible)
        3. Collapsible technical details with data science explanation
        4. Semantic confidence badges (High confidence / Caution)

        Language: Observations, not issues. Awareness, not problems to fix.

        Args:
            ml_findings: Dictionary of ML analysis results
            columns: List of ColumnProfile objects for semantic lookup
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

            # Plain English explanation (domain-neutral, no jargon)
            # Add bridge text explaining relationship to IQR
            plain_english = f'''About {outlier_pct:.2f}% of values ({total_outliers:,} records) stand out as very different
from most other values in their columns. These might be typos, special cases, or simply
unusual but valid entries worth reviewing. These anomalies are based on statistical isolation,
not just being outside the IQR bounds. It is normal for this method to find unusual points
even when IQR outliers are rare.'''

            # Build example table rows from sample_rows (correct key) - DEDUPLICATED
            example_rows = ''
            seen_values = set()  # Track unique (col_name, value) pairs

            for col_name, data in list(numeric_outliers.items())[:3]:
                # Try sample_rows first, then fallback to other keys
                samples = data.get('sample_rows', data.get('sample_anomalies', data.get('sample_outliers', [])))
                top_anomalies = data.get('top_anomalies', [])
                normal_range = data.get('normal_range', {})
                median = normal_range.get('median', data.get('median', 0))

                # Use sample_rows if available - deduplicated
                for sample in samples[:4]:  # Check more samples to find unique ones
                    if isinstance(sample, dict):
                        val = sample.get(col_name, 'N/A')
                        # Create unique key for deduplication
                        val_key = (col_name, str(val))
                        if val_key in seen_values:
                            continue
                        seen_values.add(val_key)

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
                        # Limit to 2 unique examples per column
                        col_count = sum(1 for k in seen_values if k[0] == col_name)
                        if col_count >= 2:
                            break

                # If no sample_rows, use top_anomalies values - deduplicated
                if not samples and top_anomalies:
                    for val in top_anomalies[:4]:  # Check more to find unique ones
                        val_key = (col_name, str(val))
                        if val_key in seen_values:
                            continue
                        seen_values.add(val_key)

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
                        col_count = sum(1 for k in seen_values if k[0] == col_name)
                        if col_count >= 2:
                            break

            # If no sample rows found, generate examples from top_anomalies or outlier bounds
            if not example_rows:
                for col_name, data in list(numeric_outliers.items())[:3]:
                    top_anomalies = data.get('top_anomalies', [])
                    normal_range = data.get('normal_range', {})
                    median = normal_range.get('median', data.get('median', 0))
                    upper_bound = normal_range.get('upper_bound', data.get('upper_bound', 0))
                    lower_bound = normal_range.get('lower_bound', data.get('lower_bound', 0))

                    # Use top_anomalies if available - deduplicated
                    if top_anomalies:
                        for val in top_anomalies[:4]:
                            val_key = (col_name, str(val))
                            if val_key in seen_values:
                                continue
                            seen_values.add(val_key)

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
                            col_count = sum(1 for k in seen_values if k[0] == col_name)
                            if col_count >= 2:
                                break
                    # Otherwise show bounds information
                    elif upper_bound or lower_bound:
                        bound_info = f">{upper_bound:,.2f}" if upper_bound else f"<{lower_bound:,.2f}"
                        median_display = f'{median:,.2f}' if median else 'N/A'
                        example_rows += f'''
                        <tr>
                            <td>{col_name}</td>
                            <td class="value-highlight">{bound_info} (threshold)</td>
                            <td class="value-normal">{median_display}</td>
                        </tr>'''

            # Final fallback if still no examples
            if not example_rows:
                example_rows = '<tr><td colspan="3" style="text-align: center; color: var(--text-muted);">Outliers detected but sample values not recorded</td></tr>'

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

            # Generate semantic confidence badge for outlier analysis
            outlier_semantic_badges = []
            has_caution = False
            for col_name in list(numeric_outliers.keys())[:3]:
                is_high, explanation = self._get_semantic_analytic_confidence(col_name, 'outlier', columns)
                if not is_high and explanation:
                    has_caution = True
                    outlier_semantic_badges.append(self._generate_semantic_confidence_badge(is_high, f"{col_name}: {explanation}"))
                elif is_high and explanation:
                    outlier_semantic_badges.append(self._generate_semantic_confidence_badge(is_high, f"{col_name}: {explanation}"))

            # Build combined semantic badge (show caution if any column has caution)
            semantic_badge = ''
            if has_caution:
                semantic_badge = self._generate_semantic_confidence_badge(
                    False,
                    "Some columns may not be ideal for outlier detection based on their semantic types."
                )
            elif outlier_semantic_badges:
                semantic_badge = self._generate_semantic_confidence_badge(
                    True,
                    "This analysis is well-suited to these fields based on their semantic types."
                )

            widgets_html += self._build_insight_widget(
                icon="🧠",
                title="Field-Level Outliers",
                badge_text=f"{outlier_pct:.2f}% of rows",
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
                ml_model="Isolation Forest",
                semantic_confidence=semantic_badge
            )
        else:
            # Show that we checked but found nothing - with meaningful data context
            widgets_html += self._build_insight_widget(
                icon="✓",
                title="Field-Level Outliers",
                badge_text="100% normal",
                badge_class="good",
                plain_english=f'''All {analyzed_rows:,} rows analyzed have values that look typical. No numbers
stand out as unusually high or low. This suggests consistent data entry without
unexpected spikes or dips.''',
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
            # Plain English explanation - no jargon
            # Add bridge text explaining relationship to IQR
            plain_english = f'''About {ae_pct:.2f}% of records ({ae_count:,}) have unusual combinations
of values across multiple fields. These rows look different from typical patterns in your
data and might be rare cases, data entry issues, or special situations worth reviewing.
These anomalies reflect rows that are hard for the model to reconstruct, which can capture
unusual combinations of values even when each individual field looks numerically well-behaved.'''

            # Build example table from sample_rows - DEDUPLICATED and SORTED by reconstruction error descending
            example_rows = ''
            sample_records = autoencoder.get('sample_rows', autoencoder.get('sample_anomalies', []))
            contributing_cols = autoencoder.get('contributing_columns', [])

            # Sort by reconstruction error (descending) before deduplication
            def get_recon_error(record):
                if isinstance(record, dict):
                    try:
                        return float(record.get('_reconstruction_error', 0))
                    except (ValueError, TypeError):
                        return 0
                return 0

            sample_records_sorted = sorted(sample_records, key=get_recon_error, reverse=True)

            # Deduplicate sample records by converting to hashable representation
            seen_records = set()
            unique_records = []
            for record in sample_records_sorted:
                if isinstance(record, dict):
                    # Create a hashable key from record values
                    record_key = tuple(sorted((k, str(v)) for k, v in record.items()))
                    if record_key not in seen_records:
                        seen_records.add(record_key)
                        unique_records.append(record)

            for i, record in enumerate(unique_records[:5]):
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
                        <td><strong>{col_name}</strong>: Unusual avg {anomaly_mean:,.2f} vs typical {normal_mean:,.2f}</td>
                    </tr>'''

            if not example_rows:
                example_rows = '<tr><td style="text-align: center; color: var(--text-muted);">Sample records not available</td></tr>'

            # Multivariate analysis is always high confidence - considers all fields holistically
            ae_semantic_badge = self._generate_semantic_confidence_badge(
                True,
                "Multi-column pattern analysis considers field interactions holistically."
            ) if columns else ''

            widgets_html += self._build_insight_widget(
                icon="🧠",
                title="Row-Level Anomalies",
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
                    "Useful for detecting data entry errors, process anomalies, or rare edge cases"
                ],
                ml_model=method,
                semantic_confidence=ae_semantic_badge
            )
        else:
            # Show that multivariate analysis was performed but found nothing
            widgets_html += self._build_insight_widget(
                icon="✓",
                title="Row-Level Anomalies",
                badge_text="None detected",
                badge_class="good",
                plain_english='''When looking at multiple columns together, all records appear normal.
The values across different fields fit together as expected, with no unusual
combinations that might indicate errors or special cases.''',
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

            # Build description based on what we found - plain language, no jargon
            if extreme_ratio_count > 0 and corr_break_total > 0:
                plain_english = f'''We found {extreme_ratio_count:,} records where related number columns have
very different scales (one value is 10x or more larger than expected). Also,
{corr_break_total:,} rows show unusual relationships between fields that normally move together.'''
            elif extreme_ratio_count > 0:
                plain_english = f'''We found {extreme_ratio_count:,} records where related number columns have
very different scales than expected. One value might be much larger or smaller
than the other, which could mean different units, adjustments, or entry errors.'''
            else:
                plain_english = f'''{corr_break_total:,} rows show unusual relationships between columns that
normally move together. In most records, when one column goes up, the other follows
a pattern - but these rows break that pattern significantly.'''

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

            # Generate semantic confidence badge for cross-field analysis
            # Cross-field analysis compares numeric columns - check if the pairs are well-suited
            corr_has_caution = False
            corr_cols_analyzed = set()
            for issue in cross_issues[:3]:
                for col_name in issue.get('columns', []):
                    corr_cols_analyzed.add(col_name)
            for col_name in corr_cols_analyzed:
                is_high, _ = self._get_semantic_analytic_confidence(col_name, 'correlation', columns)
                if not is_high:
                    corr_has_caution = True
                    break

            if corr_has_caution:
                corr_semantic_badge = self._generate_semantic_confidence_badge(
                    False, "Some columns being compared may not be ideal for ratio analysis."
                )
            elif columns:
                corr_semantic_badge = self._generate_semantic_confidence_badge(
                    True, "Numeric columns are well-suited for cross-field relationship analysis."
                )
            else:
                corr_semantic_badge = ''

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
                ],
                semantic_confidence=corr_semantic_badge
            )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 4: VALUE DISTRIBUTION (Rare Categories)
        # ═══════════════════════════════════════════════════════════════
        if rare_categories:
            total_rare = sum(f.get('total_rare_count', 0) for f in rare_categories.values())

            plain_english = f'''Some columns have values that appear only a few times ({total_rare:,} total).
These might be spelling mistakes, edge cases, or simply uncommon but valid entries.
Worth reviewing to see if any should be corrected or grouped together.'''

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

            plain_english = f'''Your date/time columns have {total_large_gaps:,} noticeable gaps - periods much
longer than usual between entries. These could mean missing data, seasonal slowdowns,
system downtime, or simply natural pauses in activity.'''

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
        # NOTE: Removed - Benford analysis is shown in the Anomalies section
        # accordion with interactive charts (see _generate_advanced_visualizations)
        # ═══════════════════════════════════════════════════════════════

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
data sources. {noise_pct:.2f}% of records don't fit any cluster (these may be unusual cases).'''

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
                    <td>{size:,} ({pct:.2f}%)</td>
                    <td style="font-size: 0.85em;">{char_display}</td>
                </tr>'''

            # Add noise points row
            example_rows += f'''
            <tr style="border-top: 1px solid rgba(148,163,184,0.1);">
                <td class="value-highlight">Noise (outliers)</td>
                <td>{noise_points:,} ({noise_pct:.2f}%)</td>
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

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 8: TARGET-FEATURE ASSOCIATIONS
        # ═══════════════════════════════════════════════════════════════
        target_feature = ml_findings.get('target_feature_analysis', {})
        if target_feature:
            for target_col, analysis in list(target_feature.items())[:2]:  # Show max 2 targets
                target_dist = analysis.get('target_distribution', {})
                feature_assoc = analysis.get('feature_associations', [])

                if not feature_assoc:
                    continue

                # Calculate class balance
                total_target = sum(d.get('count', 0) for d in target_dist.values())
                minority_pct = min((d.get('percentage', 0) for d in target_dist.values()), default=0)

                # Plain English explanation - no jargon
                top_feature = feature_assoc[0] if feature_assoc else {}
                plain_english = f'''The '{target_col}' column appears to be an outcome with {len(target_dist)} categories.
The column most strongly linked to this outcome is '{top_feature.get("feature", "N/A")}'. This suggests
patterns worth exploring, though it doesn't prove cause and effect.'''

                # Build example table
                example_rows = ''
                for assoc in feature_assoc[:5]:
                    feature_name = assoc.get('feature', '')
                    strength = assoc.get('association_strength', 0)
                    interp = assoc.get('interpretation', '')[:50] + ('...' if len(assoc.get('interpretation', '')) > 50 else '')
                    strength_class = 'value-highlight' if strength > 0.3 else 'value-normal'
                    example_rows += f'''
                    <tr>
                        <td>{feature_name}</td>
                        <td class="{strength_class}">{strength:.3f}</td>
                        <td style="font-size: 0.85em;">{interp}</td>
                    </tr>'''

                widgets_html += self._build_insight_widget(
                    icon="🎯",
                    title=f"Target Analysis: {target_col}",
                    badge_text=f"{len(feature_assoc)} features",
                    badge_class="info",
                    plain_english=plain_english,
                    table_headers=["Feature", "Association", "Interpretation"],
                    table_rows=example_rows,
                    technical_items=f'''
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Target Column</span>
                            <span class="insight-technical-item-value">{target_col}</span>
                        </div>
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Classes</span>
                            <span class="insight-technical-item-value">{len(target_dist)}</span>
                        </div>
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Features Analyzed</span>
                            <span class="insight-technical-item-value">{len(feature_assoc)}</span>
                        </div>
                    ''',
                    technical_context=[
                        "Detected using keyword matching and cardinality analysis",
                        "Association strength uses Cohen's d (numeric) or proportion difference (categorical)",
                        "Higher values indicate stronger predictive relationship"
                    ],
                    ml_model="Target Detection"
                )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 9: MISSINGNESS IMPACT
        # ═══════════════════════════════════════════════════════════════
        missingness_impact = ml_findings.get('missingness_impact', {})
        if missingness_impact:
            for target_col, analysis in list(missingness_impact.items())[:1]:  # Show first target only
                biased_features = analysis.get('features_with_differential_missingness', [])
                total_analyzed = analysis.get('total_features_analyzed', 0)

                if not biased_features:
                    continue

                plain_english = f'''Found {len(biased_features)} column(s) where missing values occur more
often in some groups than others (based on '{target_col}'). This means the gaps in your data
aren't random and could make certain groups under-represented if not handled carefully.'''

                example_rows = ''
                for feat in biased_features[:5]:
                    feature_name = feat.get('feature', '')
                    max_diff = feat.get('max_difference', 0)
                    miss_by_target = feat.get('missingness_by_target', {})
                    rates = [f"{k}: {v.get('rate', 0):.1f}%" for k, v in list(miss_by_target.items())[:2]]
                    example_rows += f'''
                    <tr>
                        <td>{feature_name}</td>
                        <td class="value-highlight">{max_diff:.1f}%</td>
                        <td style="font-size: 0.85em;">{', '.join(rates)}</td>
                    </tr>'''

                widgets_html += self._build_insight_widget(
                    icon="🕳️",
                    title=f"Missing Data Bias: {target_col}",
                    badge_text=f"{len(biased_features)} biased",
                    badge_class="warning" if biased_features else "good",
                    plain_english=plain_english,
                    table_headers=["Feature", "Rate Diff", "Missingness by Class"],
                    table_rows=example_rows,
                    technical_items=f'''
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Target Column</span>
                            <span class="insight-technical-item-value">{target_col}</span>
                        </div>
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Features with Bias</span>
                            <span class="insight-technical-item-value">{len(biased_features)} of {total_analyzed}</span>
                        </div>
                    ''',
                    technical_context=[
                        "Compares missing data rates across target classes",
                        "Threshold: ≥5% difference flags potential non-random missingness",
                        "Non-random missing data can bias ML models and statistical analyses"
                    ],
                    ml_model="Missingness Analysis"
                )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 10: MIXED-TYPE CORRELATIONS
        # ═══════════════════════════════════════════════════════════════
        mixed_corr = ml_findings.get('mixed_correlation_matrix', {})
        if mixed_corr and mixed_corr.get('correlation_ratio'):
            corr_ratio = mixed_corr.get('correlation_ratio', {})
            cols_analyzed = mixed_corr.get('columns_analyzed', {})

            # Get significant correlations
            significant = [(k, v) for k, v in corr_ratio.items() if v.get('eta', 0) >= 0.1]

            if significant:
                top_corr = significant[0][1] if significant else {}
                plain_english = f'''Found {len(significant)} significant categorical-to-numeric relationships.
The strongest: '{top_corr.get("categorical_column", "N/A")}' explains {top_corr.get("eta_squared", 0)*100:.1f}%
of variance in '{top_corr.get("numeric_column", "N/A")}' (η = {top_corr.get("eta", 0):.3f}).'''

                example_rows = ''
                for key, data in list(significant)[:5]:
                    cat_col = data.get('categorical_column', '')
                    num_col = data.get('numeric_column', '')
                    eta = data.get('eta', 0)
                    interp = data.get('interpretation', '')
                    eta_class = 'value-highlight' if eta > 0.3 else 'value-normal'
                    example_rows += f'''
                    <tr>
                        <td>{cat_col}</td>
                        <td>{num_col}</td>
                        <td class="{eta_class}">{eta:.3f}</td>
                    </tr>'''

                widgets_html += self._build_insight_widget(
                    icon="🔗",
                    title="Categorical-Numeric Correlations",
                    badge_text=f"{len(significant)} relationships",
                    badge_class="info",
                    plain_english=plain_english,
                    table_headers=["Categorical", "Numeric", "η (eta)"],
                    table_rows=example_rows,
                    technical_items=f'''
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Categorical Columns</span>
                            <span class="insight-technical-item-value">{len(cols_analyzed.get("categorical", []))}</span>
                        </div>
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Numeric Columns</span>
                            <span class="insight-technical-item-value">{len(cols_analyzed.get("numeric", []))}</span>
                        </div>
                    ''',
                    technical_context=[
                        "Uses Correlation Ratio (η) - measures how much a categorical variable explains numeric variance",
                        "η² represents proportion of variance explained (like R²)",
                        "Values: <0.1 weak, 0.1-0.3 moderate, >0.3 strong"
                    ],
                    ml_model="Correlation Ratio"
                )

        # ═══════════════════════════════════════════════════════════════
        # INSIGHT 11: TARGET CLASS DISTRIBUTIONS
        # ═══════════════════════════════════════════════════════════════
        target_class_dist = ml_findings.get('target_class_distribution', {})
        if target_class_dist:
            for target_col, analysis in list(target_class_dist.items())[:1]:  # Show first target
                class_counts = analysis.get('target_class_counts', {})
                feature_dists = analysis.get('feature_distributions', {})

                if not feature_dists:
                    continue

                # Find most discriminative feature (largest mean difference)
                max_diff_feature = None
                max_diff_val = 0
                for feat_name, feat_data in feature_dists.items():
                    by_class = feat_data.get('by_target_class', {})
                    if len(by_class) >= 2:
                        means = [v.get('mean', 0) for v in by_class.values()]
                        diff = max(means) - min(means) if means else 0
                        if diff > max_diff_val:
                            max_diff_val = diff
                            max_diff_feature = feat_name

                total_samples = sum(class_counts.values())
                plain_english = f'''Analyzing how numeric features distribute across '{target_col}' classes
({len(class_counts)} classes, {total_samples:,} samples). {max_diff_feature or "Multiple features"} shows
the largest difference between classes, which could be useful for predictive modeling.'''

                example_rows = ''
                for feat_name, feat_data in list(feature_dists.items())[:5]:
                    by_class = feat_data.get('by_target_class', {})
                    class_stats = []
                    for cls, stats in list(by_class.items())[:2]:
                        mean = stats.get('mean', 0)
                        if mean > 1000:
                            class_stats.append(f"{cls}: {mean/1000:.1f}K")
                        else:
                            class_stats.append(f"{cls}: {mean:.1f}")
                    example_rows += f'''
                    <tr>
                        <td>{feat_name}</td>
                        <td colspan="2" style="font-size: 0.85em;">{' | '.join(class_stats)}</td>
                    </tr>'''

                widgets_html += self._build_insight_widget(
                    icon="📊",
                    title=f"Class Distributions: {target_col}",
                    badge_text=f"{len(feature_dists)} features",
                    badge_class="info",
                    plain_english=plain_english,
                    table_headers=["Feature", "Mean by Class", ""],
                    table_rows=example_rows,
                    technical_items=f'''
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Target Classes</span>
                            <span class="insight-technical-item-value">{len(class_counts)}</span>
                        </div>
                        <div class="insight-technical-item">
                            <span class="insight-technical-item-label">Features Analyzed</span>
                            <span class="insight-technical-item-value">{len(feature_dists)}</span>
                        </div>
                    ''',
                    technical_context=[
                        "Shows mean and standard deviation of numeric features per target class",
                        "Large differences in means indicate discriminative features",
                        "Useful for feature selection in supervised learning"
                    ],
                    ml_model="Class Distribution"
                )

        # Wrap all widgets in a section
        if not widgets_html:
            return ''

        sample_note = f" (sample of {original_rows:,})" if original_rows > analyzed_rows else ""

        return f'''
        <section id="section-anomalies">
            <div class="section-header-v2">
                <div>
                    <span class="section-header-v2-icon">💡</span>
                    <span class="section-header-v2-title">Anomalies & Patterns</span>
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
                              ml_model: str = None, semantic_confidence: str = None) -> str:
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
            semantic_confidence: Optional HTML for semantic confidence badge
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

        # Semantic confidence badge (optional)
        semantic_badge_html = semantic_confidence if semantic_confidence else ''

        return f'''
        <div class="insight-widget">
            <div class="insight-widget-header" onclick="this.closest('.insight-widget').classList.toggle('collapsed')">
                <div class="insight-widget-title-group">
                    <span class="insight-widget-icon">{icon}</span>
                    <span class="insight-widget-title">{title}</span>
                    {ml_badge}
                </div>
                <div style="display: flex; align-items: center;">
                    <span class="insight-widget-badge {badge_class}">{badge_text}</span>
                    <span class="insight-widget-chevron">▼</span>
                </div>
            </div>

            <div class="insight-widget-body">
                <!-- Plain English Summary (always visible) -->
                <div class="insight-summary">
                    <div class="insight-summary-label">Plain-English Summary</div>
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

                <!-- Semantic Confidence (if applicable) -->
                {semantic_badge_html}

                <!-- Technical Details (Collapsed by default) -->
                <details class="dual-layer-technical">
                    <summary>🧠 Technical Details (click to expand)</summary>
                    <div class="dual-layer-technical-content">
                        <div class="dual-layer-technical-grid">
                            {technical_items}
                        </div>
                        <div class="dual-layer-technical-context">
                            <ul>
                                {context_html}
                            </ul>
                        </div>
                    </div>
                </details>
            </div>
        </div>
        '''

    def _build_dual_layer_explanation(self, plain_english: str, technical_stats: Dict = None,
                                       technical_context: List = None) -> str:
        """
        Build a dual-layer explanation block with plain English summary (always visible)
        and technical details (collapsed by default).

        Args:
            plain_english: Natural language summary (2-4 sentences, no jargon)
            technical_stats: Dict of stat_label -> stat_value pairs to display
            technical_context: List of technical context bullet points
        """
        # Build technical stats grid
        stats_html = ''
        if technical_stats:
            for label, value in technical_stats.items():
                stats_html += f'''
                    <div class="dual-layer-technical-item">
                        <span class="dual-layer-technical-item-label">{label}</span>
                        <span class="dual-layer-technical-item-value">{value}</span>
                    </div>'''

        # Build technical context list
        context_html = ''
        if technical_context:
            context_items = ''.join([f'<li>{item}</li>' for item in technical_context])
            context_html = f'''
                <div class="dual-layer-technical-context">
                    <ul>{context_items}</ul>
                </div>'''

        # Only show technical section if there's content
        technical_section = ''
        if stats_html or context_html:
            technical_section = f'''
                <details class="dual-layer-technical">
                    <summary>🧠 Technical Details (click to expand)</summary>
                    <div class="dual-layer-technical-content">
                        <div class="dual-layer-technical-grid">{stats_html}</div>
                        {context_html}
                    </div>
                </details>'''

        return f'''
            <div class="dual-layer-explanation">
                <div class="dual-layer-summary">
                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                    <div class="dual-layer-summary-text">{plain_english}</div>
                </div>
                {technical_section}
            </div>'''

    def _generate_advanced_visualizations(self, ml_findings: Dict, columns: List = None) -> Dict[str, List[str]]:
        """
        Generate advanced visualization charts categorized by target section.

        Returns a dictionary with charts grouped by section:
        - 'distributions': Amount distributions, box plots, class imbalance, outlier comparison
        - 'anomalies': Autoencoder, Isolation Forest, Benford's Law
        - 'temporal': Activity timeline
        - 'correlations': Scatter plot, feature correlation matrix
        - 'missingness': Missing data pattern analysis
        - 'overview': Data quality radar

        Each section generator can retrieve its relevant charts using _get_viz_for_section().
        """
        # Initialize categorized chart containers
        charts_by_section = {
            'distributions': [],
            'anomalies': [],
            'temporal': [],
            'correlations': [],
            'missingness': [],
            'overview': []
        }

        if not ml_findings:
            return charts_by_section

        viz_data = ml_findings.get('visualizations', {})
        if not viz_data:
            return charts_by_section

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

        # Legacy sections_html for backward compatibility during transition
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
                charts_by_section['distributions'].append(f'''
                    <div class="accordion" data-accordion="viz-amounts">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #10b981, #059669);">📊</div>
                                <div>
                                    <div class="accordion-title">Amount Distributions (Log Scale)</div>
                                    <div class="accordion-subtitle">Visualize skewed numeric data distributions</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(amount_dists)} Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">
                                        These charts show how values are spread across each numeric field. We use a special scale that lets you see both small and large values clearly - otherwise big numbers would hide everything else.
                                        {sample_note}
                                    </div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content" style="padding: 12px;">
                                        <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                            <li>Log-scaled histogram bins for wide-range numeric data</li>
                                            <li>Reveals patterns across multiple orders of magnitude</li>
                                            <li>Min/max shown from full dataset (Parquet metadata)</li>
                                        </ul>
                                    </div>
                                </details>
                            </div>
                            <div style="display: flex; flex-wrap: wrap; gap: 16px; margin-top: 16px;">
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

            charts_by_section['correlations'].append(f'''
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
                        <div class="dual-layer-explanation">
                            <div class="dual-layer-summary">
                                <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                <div class="dual-layer-summary-text">
                                    This chart compares two amount fields. If both amounts are usually the same, points should line up along the diagonal. Points far from the line might indicate data issues or special cases. {sample_note}
                                </div>
                            </div>
                            <details class="dual-layer-technical">
                                <summary>🧠 Technical Details (click to expand)</summary>
                                <div class="dual-layer-technical-content" style="padding: 12px;">
                                    <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                        <li>Diagonal line represents y = x (perfect 1:1 match)</li>
                                        <li>Off-diagonal points may indicate adjustments, fees, or errors</li>
                                        <li>Log scale used when data spans multiple orders of magnitude</li>
                                    </ul>
                                </div>
                            </details>
                        </div>
                        <div style="height: 400px; background: var(--bg-card); border-radius: 8px; padding: 16px; margin-top: 16px;">
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
                                                label: 'Records',
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
            # Separate target columns from non-target columns for visual grouping
            target_charts = ''
            other_charts = ''
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

                # Target columns get a highlighted border and badge
                if is_target:
                    chart_html = f'''
                    <div style="flex: 1; min-width: 280px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 2px solid #f59e0b; position: relative;">
                        <div style="position: absolute; top: -10px; left: 12px; background: linear-gradient(135deg, #f59e0b, #d97706); color: white; font-size: 0.7em; font-weight: 600; padding: 2px 8px; border-radius: 4px;">🎯 ML TARGET</div>
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; margin-top: 4px;">
                            <h4 style="margin: 0; font-size: 0.95em; color: var(--text-primary);">{col}</h4>
                            <span class="accordion-badge {imbalance_status}">{imbalance_note}</span>
                        </div>
                        <div style="height: 200px;">
                            <canvas id="{chart_id}"></canvas>
                        </div>
                        <div style="font-size: 0.8em; color: var(--text-muted); margin-top: 8px; text-align: center;">
                            Total: {total:,}
                        </div>
                    </div>'''
                    target_charts += chart_html
                else:
                    chart_html = f'''
                    <div style="flex: 1; min-width: 280px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-subtle);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                            <h4 style="margin: 0; font-size: 0.95em; color: var(--text-primary);">{col}</h4>
                            <span class="accordion-badge {imbalance_status}">{imbalance_note}</span>
                        </div>
                        <div style="height: 200px;">
                            <canvas id="{chart_id}"></canvas>
                        </div>
                        <div style="font-size: 0.8em; color: var(--text-muted); margin-top: 8px; text-align: center;">
                            Total: {total:,}
                        </div>
                    </div>'''
                    other_charts += chart_html

                imbalance_scripts.append({
                    'id': chart_id,
                    'labels': [c['value'] for c in classes],
                    'data': [c['count'] for c in classes],
                    'percentages': [c['percentage'] for c in classes]
                })

            # Combine charts with section headers if both exist
            imbalance_charts = ''
            if target_charts:
                imbalance_charts += f'''
                    <div style="margin-bottom: 16px;">
                        <div style="font-size: 0.75em; text-transform: uppercase; letter-spacing: 0.5px; color: #f59e0b; font-weight: 600; margin-bottom: 8px;">Detected ML Target</div>
                        <div style="display: flex; flex-wrap: wrap; gap: 16px;">{target_charts}</div>
                    </div>'''
            if other_charts:
                section_label = "Other Categorical Fields" if target_charts else ""
                if section_label:
                    imbalance_charts += f'''
                    <div>
                        <div style="font-size: 0.75em; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted); font-weight: 600; margin-bottom: 8px;">{section_label}</div>
                        <div style="display: flex; flex-wrap: wrap; gap: 16px;">{other_charts}</div>
                    </div>'''
                else:
                    imbalance_charts += f'<div style="display: flex; flex-wrap: wrap; gap: 16px;">{other_charts}</div>'

            if imbalance_charts:
                # Determine if any fields are actual targets (for subtitle wording)
                target_count = sum(1 for col, data in class_data.items() if data.get('is_target_like', False))
                if target_count > 0:
                    subtitle_text = f"Low-cardinality field distributions ({target_count} potential target{'s' if target_count > 1 else ''} detected)"
                else:
                    subtitle_text = "Low-cardinality categorical field distributions"

                charts_by_section['distributions'].append(f'''
                    <div class="accordion" data-accordion="viz-imbalance">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #f59e0b, #d97706);">⚖️</div>
                                <div>
                                    <div class="accordion-title">Class Distribution & Imbalance</div>
                                    <div class="accordion-subtitle">{subtitle_text}</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(class_data)} Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">
                                        These charts show how values are split across categories. When one group is much smaller than others (less than 10%), it can cause problems for analysis because the small group gets overlooked.
                                        {sample_note}
                                    </div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content" style="padding: 12px;">
                                        <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                            <li>Minority class &lt;10% = critical imbalance for ML models</li>
                                            <li>Solutions: SMOTE oversampling, class weights, stratified sampling</li>
                                            <li>Alternative metrics: F1-score, precision-recall AUC, Cohen's kappa</li>
                                        </ul>
                                        <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--border-subtle);">
                                            <p style="font-weight: 600; margin-bottom: 8px; color: var(--text-secondary);">🎯 Target Detection Logic:</p>
                                            <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                                <li><strong>Keyword match:</strong> target, label, class, outcome, churn, fraud, survived, etc.</li>
                                                <li><strong>Pattern match:</strong> is_*, has_*, *_flag, *_indicator prefixes/suffixes</li>
                                                <li><strong>Binary columns:</strong> Fields with exactly 2 unique values + target-like name</li>
                                                <li><strong>Low cardinality:</strong> ≤5 unique values + keyword match</li>
                                            </ul>
                                        </div>
                                    </div>
                                </details>
                            </div>
                            {imbalance_charts}
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
                charts_by_section['temporal'].append(f'''
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
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">
                                        This shows how records are spread out over time. Red bars highlight days with no activity - these gaps might be normal (weekends, holidays) or could indicate missing data.
                                        {sample_note}
                                    </div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content" style="padding: 12px;">
                                        <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                            <li>Daily aggregation of event counts</li>
                                            <li>Gap detection compares consecutive dates</li>
                                            <li>Red bars = calendar days with zero events</li>
                                        </ul>
                                    </div>
                                </details>
                            </div>
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

            # Build plain-English summary for Autoencoder
            if anomaly_pct > 3:
                ae_plain_summary = f"This analysis found {anomaly_count:,} rows ({anomaly_pct:.2f}%) that look unusual compared to the rest of the data. These rows have combinations of values that don't fit the typical patterns, which could indicate data entry issues, rare cases, or special situations worth reviewing."
            elif anomaly_pct > 0.5:
                ae_plain_summary = f"A small number of rows ({anomaly_count:,}, or {anomaly_pct:.2f}%) appear unusual compared to typical patterns. Most of these are likely legitimate edge cases, but they may be worth a quick look."
            else:
                ae_plain_summary = f"Nearly all rows follow normal patterns. Only {anomaly_count:,} rows ({anomaly_pct:.2f}%) look slightly unusual, which is a very low rate indicating consistent, well-behaved data."

            ae_dual_layer = self._build_dual_layer_explanation(
                plain_english=ae_plain_summary,
                technical_stats={
                    "Median Error": median_str,
                    "Q75 Error": q75_str,
                    "Anomaly Threshold": threshold_str,
                    "Anomaly Rate": f"{anomaly_pct:.2f}%",
                    "Anomalies Found": f"{anomaly_count:,}"
                },
                technical_context=[
                    "Autoencoder neural network learns normal data patterns",
                    "Reconstruction error = how different a record is from normal",
                    "Records above threshold are flagged as anomalies",
                    f"Threshold calculated from error distribution upper tail" if is_right_skewed else "Threshold based on mean + 2.5 standard deviations"
                ]
            )

            charts_by_section['anomalies'].append(f'''
                <div class="accordion" data-accordion="viz-autoencoder">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #ec4899, #be185d);">🧠</div>
                            <div>
                                <div class="accordion-title">Autoencoder Reconstruction Errors</div>
                                <div class="accordion-subtitle">Deep learning anomaly detection</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge {'critical' if anomaly_pct > 5 else 'warning' if anomaly_pct > 1 else 'good'}">{anomaly_count:,} Anomalies</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        {ae_dual_layer}
                        <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; height: 200px; margin-top: 16px;">
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

        # ═══════════════════════════════════════════════════════════════
        # 6. ISOLATION FOREST ANOMALY SCORES
        # ═══════════════════════════════════════════════════════════════
        anomaly_scores = viz_data.get('anomaly_scores', {})
        # Show fallback message when there are no anomaly scores
        if not anomaly_scores:
            charts_by_section['anomalies'].append(f'''
                <div class="accordion" data-accordion="viz-anomaly-scores">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #94a3b8, #64748b);">🔍</div>
                            <div>
                                <div class="accordion-title">Isolation Forest Anomaly Scores</div>
                                <div class="accordion-subtitle">Statistical outlier detection per numeric field</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge neutral">Not Available</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <div style="padding: 24px; background: var(--bg-card); border-radius: 8px; text-align: center;">
                            <div style="font-size: 2em; margin-bottom: 12px;">🔍</div>
                            <h4 style="margin: 0 0 8px 0; color: var(--text-primary);">Insufficient Numeric Fields for Anomaly Detection</h4>
                            <p style="color: var(--text-muted); margin: 0; max-width: 500px; margin: 0 auto;">
                                This dataset has too few continuous numeric fields for Isolation Forest analysis. Fields with only a few unique values (like binary flags or small categories) are excluded as they don't benefit from this type of analysis.
                            </p>
                            <p style="color: var(--text-muted); font-size: 0.85em; margin-top: 12px;">
                                💡 <strong>Tip:</strong> Isolation Forest works best on continuous numeric data like amounts, counts, measurements, and scores with many distinct values.
                            </p>
                        </div>
                    </div>
                </div>''')
        elif anomaly_scores:
            anomaly_cards = ''
            for col, data in list(anomaly_scores.items())[:6]:
                min_score = data.get('min_score', 0)
                max_score = data.get('max_score', 0)
                anomaly_count = data.get('anomaly_count', 0)
                total = data.get('total_analyzed', 0)
                pct = (anomaly_count / total * 100) if total > 0 else 0

                # Score interpretation (Isolation Forest: lower = more anomalous)
                severity = 'critical' if pct > 5 else 'warning' if pct > 1 else 'good'

                anomaly_cards += f'''
                    <div style="flex: 1; min-width: 200px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-subtle);">
                        <h4 style="margin: 0 0 12px 0; font-size: 0.9em; color: var(--text-primary);">{col}</h4>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
                            <div style="text-align: center; padding: 8px; background: rgba(139, 92, 246, 0.1); border-radius: 6px;">
                                <div style="font-size: 1.1em; font-weight: bold; color: var(--text-primary);">{anomaly_count:,}</div>
                                <div style="font-size: 0.7em; color: var(--text-muted);">Anomalies</div>
                            </div>
                            <div style="text-align: center; padding: 8px; background: rgba(239, 68, 68, 0.1); border-radius: 6px;">
                                <div style="font-size: 1.1em; font-weight: bold; color: var(--{severity});">{pct:.2f}%</div>
                                <div style="font-size: 0.7em; color: var(--text-muted);">Rate</div>
                            </div>
                        </div>
                        <div style="margin-top: 12px; font-size: 0.75em; color: var(--text-muted);">
                            Score range: {min_score:.3f} to {max_score:.3f}
                        </div>
                    </div>'''

            # Calculate totals for dual-layer explanation
            # Note: total_anomalies sums per-field counts but the SAME ROW may be anomalous in multiple fields
            total_anomalies = sum(d.get('anomaly_count', 0) for d in anomaly_scores.values())
            # Get sample size from any field (all fields analyze the same rows)
            sample_size = next(iter(anomaly_scores.values())).get('total_analyzed', 0) if anomaly_scores else 0
            # Calculate average per-field anomaly rate (total anomalies / sample_size)
            # This represents the average rate of anomalies per field, not row-level rate
            overall_pct = (total_anomalies / sample_size * 100) if sample_size > 0 else 0
            # Per-field average rate is total / (sample * fields), but for the summary display
            # we show total/sample which represents "if each anomaly were unique rows"
            per_field_avg_pct = (total_anomalies / (sample_size * len(anomaly_scores)) * 100) if sample_size > 0 and len(anomaly_scores) > 0 else 0
            avg_min_score = sum(d.get('min_score', 0) for d in anomaly_scores.values()) / len(anomaly_scores) if anomaly_scores else 0
            avg_max_score = sum(d.get('max_score', 0) for d in anomaly_scores.values()) / len(anomaly_scores) if anomaly_scores else 0

            # Dual-layer explanation
            if overall_pct > 3:
                plain_summary = f"A notable number of records ({total_anomalies:,}, or {overall_pct:.2f}%) look unusual compared to the rest of the data. These values stand out because they differ significantly from what most rows contain."
            elif overall_pct > 0.5:
                plain_summary = f"A small number of records ({total_anomalies:,}, or {overall_pct:.2f}%) have values that look unusual. Most of these are likely legitimate edge cases, but they may be worth reviewing."
            else:
                plain_summary = f"Nearly all records ({100-overall_pct:.2f}%) look normal. Only {total_anomalies:,} records appear unusual, which means the data is consistent and well-behaved."

            dual_layer = self._build_dual_layer_explanation(
                plain_english=plain_summary,
                technical_stats={
                    "Algorithm": "Isolation Forest",
                    "Fields Analyzed": f"{len(anomaly_scores)}",
                    "Total Anomalies": f"{total_anomalies:,}",
                    "Anomaly Rate": f"{overall_pct:.3f}%",
                    "Avg Score Range": f"{avg_min_score:.3f} - {avg_max_score:.3f}"
                },
                technical_context=[
                    "Isolation Forest scores: lower values = more anomalous (harder to isolate)",
                    "Contamination parameter set to 'auto' (adaptive threshold)",
                    "Each column analyzed independently for outliers",
                    f"Anomaly rate = total field anomalies ({total_anomalies:,}) / sample size ({sample_size:,})"
                ]
            )

            charts_by_section['anomalies'].append(f'''
                <div class="accordion" data-accordion="viz-anomaly-scores">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #ef4444, #b91c1c);">🔍</div>
                            <div>
                                <div class="accordion-title">Isolation Forest Anomaly Scores</div>
                                <div class="accordion-subtitle">Statistical outlier detection per numeric field</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{len(anomaly_scores)} Fields</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        {dual_layer}
                        <div style="display: flex; flex-wrap: wrap; gap: 12px; margin-top: 16px;">
                            {anomaly_cards}
                        </div>
                    </div>
                </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 7. NUMERIC DISTRIBUTION BOX PLOTS
        # ═══════════════════════════════════════════════════════════════
        numeric_outliers = ml_findings.get('numeric_outliers', {}) if ml_findings else {}
        # Show fallback message when there are no numeric outliers
        if not numeric_outliers:
            charts_by_section['distributions'].append(f'''
                <div class="accordion" data-accordion="viz-numeric-dist">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #94a3b8, #64748b);">📦</div>
                            <div>
                                <div class="accordion-title">Numeric Distribution Box Plots</div>
                                <div class="accordion-subtitle">Visualize value distributions and outliers</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge neutral">Not Available</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <div style="padding: 24px; background: var(--bg-card); border-radius: 8px; text-align: center;">
                            <div style="font-size: 2em; margin-bottom: 12px;">📦</div>
                            <h4 style="margin: 0 0 8px 0; color: var(--text-primary);">No Numeric Fields for Distribution Analysis</h4>
                            <p style="color: var(--text-muted); margin: 0; max-width: 500px; margin: 0 auto;">
                                This dataset doesn't have enough continuous numeric fields for box plot visualization. Binary columns and fields with very few unique values are excluded since they don't have meaningful distributions to show.
                            </p>
                            <p style="color: var(--text-muted); font-size: 0.85em; margin-top: 12px;">
                                💡 <strong>Tip:</strong> Box plots are most useful for numeric data like prices, ages, quantities, or measurements with many different values.
                            </p>
                        </div>
                    </div>
                </div>''')
        elif numeric_outliers:
            box_data = []
            for col, outlier_info in list(numeric_outliers.items())[:8]:
                q1 = outlier_info.get('q1', 0)
                q3 = outlier_info.get('q3', 0)
                median_val = outlier_info.get('median', (q1 + q3) / 2)
                lower = outlier_info.get('lower_bound', q1 - 1.5 * (q3 - q1))
                upper = outlier_info.get('upper_bound', q3 + 1.5 * (q3 - q1))
                outlier_pct = outlier_info.get('outlier_percentage', 0)

                box_data.append({
                    'col': col,
                    'min': lower,
                    'q1': q1,
                    'median': median_val,
                    'q3': q3,
                    'max': upper,
                    'outlier_pct': outlier_pct
                })

            if box_data:
                # Calculate statistics for dual-layer explanation
                avg_outlier_pct = sum(d['outlier_pct'] for d in box_data) / len(box_data) if box_data else 0
                max_outlier = max(box_data, key=lambda d: d['outlier_pct'])
                min_outlier = min(box_data, key=lambda d: d['outlier_pct'])

                # Plain-English summary based on outlier rates
                # Add bridge text explaining IQR vs ML methods
                iqr_bridge_note = " Note: this view uses only traditional box-plot rules (IQR). It may show zero classic outliers even when the ML-based anomaly detectors highlight unusual patterns."

                if avg_outlier_pct > 5:
                    plain_summary = f"Several number columns have notable outliers (averaging {avg_outlier_pct:.2f}% across {len(box_data)} fields). The column '{max_outlier['col']}' has the highest rate at {max_outlier['outlier_pct']:.2f}%. These are values much higher or lower than most, which could be data errors, unusual cases, or genuine extremes worth investigating.{iqr_bridge_note}"
                elif avg_outlier_pct > 1:
                    plain_summary = f"Most number columns have a moderate amount of outliers (averaging {avg_outlier_pct:.2f}%). This is typical for real-world data. '{max_outlier['col']}' has the most outliers ({max_outlier['outlier_pct']:.2f}%), while '{min_outlier['col']}' has the fewest ({min_outlier['outlier_pct']:.2f}%).{iqr_bridge_note}"
                else:
                    plain_summary = f"Outlier rates are low across all {len(box_data)} number columns (averaging {avg_outlier_pct:.2f}%). This means the data is well-behaved with very few extreme values. Most numbers fall in a normal-looking range.{iqr_bridge_note}"

                box_dual_layer = self._build_dual_layer_explanation(
                    plain_english=plain_summary,
                    technical_stats={
                        "Method": "IQR (Tukey's Fences)",
                        "Fields Analyzed": f"{len(box_data)}",
                        "Avg Outlier Rate": f"{avg_outlier_pct:.2f}%",
                        "Highest": f"{max_outlier['col']} ({max_outlier['outlier_pct']:.2f}%)",
                        "Lowest": f"{min_outlier['col']} ({min_outlier['outlier_pct']:.2f}%)"
                    },
                    technical_context=[
                        "Lower bound: Q1 - 1.5 × IQR",
                        "Upper bound: Q3 + 1.5 × IQR",
                        "IQR = Q3 - Q1 (interquartile range)",
                        "Values outside bounds are flagged as outliers"
                    ]
                )

                charts_by_section['distributions'].append(f'''
                    <div class="accordion" data-accordion="viz-box-plots">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #06b6d4, #0891b2);">📦</div>
                                <div>
                                    <div class="accordion-title">Numeric Distribution Summary</div>
                                    <div class="accordion-subtitle">Quartiles and outlier bounds at a glance</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(box_data)} Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            {box_dual_layer}
                            <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; height: 300px;">
                                <canvas id="boxPlotChart"></canvas>
                            </div>
                            <script>
                            document.addEventListener('DOMContentLoaded', function() {{
                                const boxCtx = document.getElementById('boxPlotChart');
                                if (boxCtx) {{
                                    const boxData = {json.dumps(box_data)};
                                    const labels = boxData.map(d => d.col.length > 15 ? d.col.slice(0, 12) + '...' : d.col);

                                    // Normalize data for comparison (show relative positions)
                                    const normalizedData = boxData.map(d => {{
                                        const range = d.max - d.min;
                                        if (range === 0) return {{ q1: 0.25, median: 0.5, q3: 0.75, min: 0, max: 1 }};
                                        return {{
                                            min: 0,
                                            q1: (d.q1 - d.min) / range,
                                            median: (d.median - d.min) / range,
                                            q3: (d.q3 - d.min) / range,
                                            max: 1
                                        }};
                                    }});

                                    new Chart(boxCtx, {{
                                        type: 'bar',
                                        data: {{
                                            labels: labels,
                                            datasets: [
                                                {{
                                                    label: 'IQR',
                                                    data: normalizedData.map(d => d.q3 - d.q1),
                                                    backgroundColor: 'rgba(6, 182, 212, 0.6)',
                                                    borderColor: 'rgba(6, 182, 212, 1)',
                                                    borderWidth: 1
                                                }}
                                            ]
                                        }},
                                        options: {{
                                            responsive: true,
                                            maintainAspectRatio: false,
                                            plugins: {{
                                                legend: {{ display: false }},
                                                tooltip: {{
                                                    callbacks: {{
                                                        label: function(ctx) {{
                                                            const d = boxData[ctx.dataIndex];
                                                            return [
                                                                `Q1: ${{d.q1.toLocaleString()}}`,
                                                                `Median: ${{d.median.toLocaleString()}}`,
                                                                `Q3: ${{d.q3.toLocaleString()}}`,
                                                                `Outliers: ${{d.outlier_pct.toFixed(1)}}%`
                                                            ];
                                                        }}
                                                    }}
                                                }}
                                            }},
                                            scales: {{
                                                x: {{
                                                    grid: {{ display: false }},
                                                    ticks: {{ color: '#94a3b8', font: {{ size: 10 }}, maxRotation: 45 }}
                                                }},
                                                y: {{
                                                    display: false
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
        # 8. BENFORD'S LAW ANALYSIS
        # ═══════════════════════════════════════════════════════════════
        benford_analysis = ml_findings.get('benford_analysis', {}) if ml_findings else {}
        benford_ineligible = ml_findings.get('benford_ineligible', {}) if ml_findings else {}

        # Show fallback if no Benford analysis available
        if not benford_analysis:
            # Build reason from ineligible columns or generic message
            if benford_ineligible:
                ineligible_reasons = list(set(benford_ineligible.values()))[:3]
                fallback_reason = f"No columns suitable for Benford analysis. Reasons: {'; '.join(ineligible_reasons)}"
            else:
                fallback_reason = "No numeric columns with sufficient values for Benford's Law analysis."

            charts_by_section['anomalies'].append(f'''
                <div class="accordion" data-accordion="viz-benford">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #94a3b8, #64748b);">📐</div>
                            <div>
                                <div class="accordion-title">Benford's Law Analysis</div>
                                <div class="accordion-subtitle">Detect potential data fabrication in numeric columns</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge neutral">Not Available</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <div style="padding: 24px; background: var(--bg-card); border-radius: 8px; text-align: center;">
                            <div style="font-size: 2em; margin-bottom: 12px;">📐</div>
                            <h4 style="margin: 0 0 8px 0; color: var(--text-primary);">Benford's Law Not Applicable</h4>
                            <p style="color: var(--text-muted); margin: 0; max-width: 500px; margin: 0 auto;">
                                {fallback_reason}
                            </p>
                            <p style="color: var(--text-muted); font-size: 0.85em; margin-top: 12px;">
                                💡 <strong>Tip:</strong> Benford's Law works best with data spanning multiple orders of magnitude,
                                such as financial transactions, population counts, or invoice amounts. Binary fields, identifiers,
                                and bounded data (like percentages) are not suitable.
                            </p>
                        </div>
                    </div>
                </div>''')

        elif benford_analysis:
            benford_charts = []
            benford_scripts = []
            for idx, (col, data) in enumerate(list(benford_analysis.items())[:4]):
                chart_id = f'benfordChart_{idx}'
                # Support both old format (observed_distribution/expected_distribution)
                # and new format (digit_distribution with nested expected/observed)
                digit_dist = data.get('digit_distribution', {})
                if digit_dist:
                    # Keys may be integers or strings depending on source
                    observed = {}
                    expected = {}
                    for d in range(1, 10):
                        # Try both int and string keys
                        digit_data = digit_dist.get(d, digit_dist.get(str(d), {}))
                        observed[str(d)] = digit_data.get('observed', 0)
                        expected[str(d)] = digit_data.get('expected', 0)
                else:
                    observed = data.get('observed_distribution', {})
                    expected = data.get('expected_distribution', {})
                is_suspicious = data.get('is_suspicious', False)
                chi_sq = data.get('chi_square', 0)
                confidence = data.get('confidence', 'Unknown')

                status_badge = 'critical' if is_suspicious else 'good'
                status_text = 'Benford deviation' if is_suspicious else 'Follows Benford'

                benford_charts.append(f'''
                    <div style="flex: 1; min-width: 280px; background: var(--bg-card); border-radius: 8px; padding: 16px; border: 1px solid var(--border-subtle);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                            <h4 style="margin: 0; font-size: 0.9em; color: var(--text-primary);">{col}</h4>
                            <span class="accordion-badge {status_badge}">{status_text}</span>
                        </div>
                        <div style="height: 180px;">
                            <canvas id="{chart_id}"></canvas>
                        </div>
                        <div style="font-size: 0.75em; color: var(--text-muted); margin-top: 8px;">
                            Chi-square: {chi_sq:.1f} | Confidence: {confidence}
                        </div>
                    </div>''')

                benford_scripts.append({
                    'id': chart_id,
                    'observed': [float(observed.get(str(d), 0)) for d in range(1, 10)],
                    'expected': [float(expected.get(str(d), 0)) for d in range(1, 10)]
                })

            if benford_charts:
                # Calculate statistics for dual-layer explanation
                suspicious_count = sum(1 for _, data in benford_analysis.items() if data.get('is_suspicious', False))
                normal_count = len(benford_analysis) - suspicious_count
                chi_sq_values = [data.get('chi_square', 0) for _, data in benford_analysis.items()]
                avg_chi_sq = sum(chi_sq_values) / len(chi_sq_values) if chi_sq_values else 0

                # Plain-English summary based on results (neutral wording)
                if suspicious_count > 0:
                    plain_summary = f"First-digit frequencies deviate significantly from the Benford distribution in {suspicious_count} of {len(benford_analysis)} columns analyzed. For naturally occurring transactional datasets this may indicate anomalies, but for pricing or tariff tables it can simply reflect business structure."
                else:
                    plain_summary = f"All {len(benford_analysis)} number columns have first-digit patterns that match what we expect from naturally-occurring data. This is a good sign that values weren't artificially created or changed, though it's not proof by itself."

                benford_dual_layer = self._build_dual_layer_explanation(
                    plain_english=plain_summary,
                    technical_stats={
                        "Fields Analyzed": f"{len(benford_analysis)}",
                        "Deviations": f"{suspicious_count}",
                        "Conforming": f"{normal_count}",
                        "Avg Chi-Square": f"{avg_chi_sq:.1f}"
                    },
                    technical_context=[
                        "Benford's Law: Leading digit 1 appears ~30.1% of the time in natural data",
                        "Chi-square test compares observed vs expected distributions",
                        "Higher chi-square values indicate greater deviation from expected",
                        "Works best with data spanning multiple orders of magnitude"
                    ]
                )

                charts_by_section['anomalies'].append(f'''
                    <div class="accordion" data-accordion="viz-benford">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #f97316, #ea580c);">📐</div>
                                <div>
                                    <div class="accordion-title">Benford's Law Analysis</div>
                                    <div class="accordion-subtitle">Detect potential data fabrication in numeric columns</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(benford_analysis)} Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            {benford_dual_layer}
                            <div style="display: flex; flex-wrap: wrap; gap: 16px;">
                                {''.join(benford_charts)}
                            </div>
                            <script>
                            document.addEventListener('DOMContentLoaded', function() {{
                                const benfordData = {json.dumps(benford_scripts)};
                                benfordData.forEach(chart => {{
                                    const ctx = document.getElementById(chart.id);
                                    if (ctx) {{
                                        new Chart(ctx, {{
                                            type: 'bar',
                                            data: {{
                                                labels: ['1', '2', '3', '4', '5', '6', '7', '8', '9'],
                                                datasets: [
                                                    {{
                                                        label: 'Observed %',
                                                        data: chart.observed,
                                                        backgroundColor: 'rgba(59, 130, 246, 0.7)',
                                                        borderWidth: 0
                                                    }},
                                                    {{
                                                        label: 'Expected %',
                                                        data: chart.expected,
                                                        backgroundColor: 'rgba(249, 115, 22, 0.7)',
                                                        borderWidth: 0
                                                    }}
                                                ]
                                            }},
                                            options: {{
                                                responsive: true,
                                                maintainAspectRatio: false,
                                                plugins: {{
                                                    legend: {{ display: true, position: 'top', labels: {{ boxWidth: 12, font: {{ size: 10 }} }} }}
                                                }},
                                                scales: {{
                                                    x: {{ grid: {{ display: false }}, ticks: {{ color: '#94a3b8' }} }},
                                                    y: {{ grid: {{ color: 'rgba(148, 163, 184, 0.1)' }}, ticks: {{ color: '#64748b' }} }}
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
        # 9. DATA QUALITY RADAR CHART
        # ═══════════════════════════════════════════════════════════════
        # Build quality scores from profile data
        completeness = column_stats.get('_avg_completeness', 0) if column_stats else 0
        validity = column_stats.get('_avg_validity', 0) if column_stats else 0
        consistency = column_stats.get('_avg_consistency', 0) if column_stats else 0
        uniqueness = column_stats.get('_avg_uniqueness', 0) if column_stats else 0

        # Only show if we have quality scores
        if any([completeness > 0, validity > 0, consistency > 0, uniqueness > 0]):
            quality_scores = {
                'Completeness': round(completeness, 1),
                'Validity': round(validity, 1),
                'Consistency': round(consistency, 1),
                'Uniqueness': round(uniqueness, 1),
                'Overall': round((completeness + validity + consistency + uniqueness) / 4, 1)
            }

            # Identify strongest and weakest dimensions
            dims_sorted = sorted([(k, v) for k, v in quality_scores.items() if k != 'Overall'], key=lambda x: x[1])
            weakest = dims_sorted[0]
            strongest = dims_sorted[-1]

            # Plain-English summary based on overall score
            overall = quality_scores['Overall']
            if overall >= 90:
                plain_summary = f"This dataset has excellent data quality ({overall:.0f}% overall). All four quality areas score well, with '{strongest[0]}' being the strongest at {strongest[1]}%. The data is ready for analysis and reporting without much cleanup needed."
            elif overall >= 75:
                plain_summary = f"Data quality is good ({overall:.0f}% overall), though there's room for improvement. '{weakest[0]}' is the weakest area at {weakest[1]}%, which may need attention. '{strongest[0]}' is the best at {strongest[1]}%."
            elif overall >= 60:
                plain_summary = f"Data quality is moderate ({overall:.0f}% overall). '{weakest[0]}' scores lowest at {weakest[1]}% and should be improved before using this data for important decisions. Consider cleaning or enriching the data to improve quality."
            else:
                plain_summary = f"Data quality needs attention ({overall:.0f}% overall). '{weakest[0]}' is particularly low at {weakest[1]}%. Significant data cleaning is recommended before using this dataset for analysis."

            radar_dual_layer = self._build_dual_layer_explanation(
                plain_english=plain_summary,
                technical_stats={
                    "Completeness": f"{quality_scores['Completeness']}%",
                    "Validity": f"{quality_scores['Validity']}%",
                    "Consistency": f"{quality_scores['Consistency']}%",
                    "Uniqueness": f"{quality_scores['Uniqueness']}%",
                    "Overall": f"{overall}%"
                },
                technical_context=[
                    "Completeness: % of non-null values (40% weight)",
                    "Validity: % matching expected type/format (30% weight)",
                    "Consistency: Pattern uniformity across values (20% weight)",
                    "Uniqueness: Cardinality relative to column type (10% weight)"
                ]
            )

            charts_by_section['overview'].append(f'''
                <div class="accordion" data-accordion="viz-quality-radar">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #22c55e, #16a34a);">🎯</div>
                            <div>
                                <div class="accordion-title">Data Quality Radar</div>
                                <div class="accordion-subtitle">Multi-dimensional quality assessment at a glance</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge {'good' if quality_scores['Overall'] >= 80 else 'warning' if quality_scores['Overall'] >= 60 else 'critical'}">{quality_scores['Overall']}% Overall</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        {radar_dual_layer}
                        <div style="display: flex; flex-wrap: wrap; gap: 20px; align-items: center;">
                            <div style="flex: 1; min-width: 280px; height: 280px; background: var(--bg-card); border-radius: 8px; padding: 16px;">
                                <canvas id="qualityRadarChart"></canvas>
                            </div>
                            <div style="flex: 1; min-width: 200px;">
                                <div style="display: grid; gap: 12px;">
                                    {''.join([f"""
                                    <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px; background: var(--bg-card); border-radius: 6px;">
                                        <span style="color: var(--text-secondary);">{dim}</span>
                                        <span style="font-weight: bold; color: var({'--success' if score >= 80 else '--warning' if score >= 60 else '--critical'});">{score}%</span>
                                    </div>""" for dim, score in quality_scores.items()])}
                                </div>
                            </div>
                        </div>
                        <script>
                        document.addEventListener('DOMContentLoaded', function() {{
                            const radarCtx = document.getElementById('qualityRadarChart');
                            if (radarCtx) {{
                                new Chart(radarCtx, {{
                                    type: 'radar',
                                    data: {{
                                        labels: ['Completeness', 'Validity', 'Consistency', 'Uniqueness'],
                                        datasets: [{{
                                            label: 'Quality Score',
                                            data: [{quality_scores['Completeness']}, {quality_scores['Validity']}, {quality_scores['Consistency']}, {quality_scores['Uniqueness']}],
                                            backgroundColor: 'rgba(34, 197, 94, 0.2)',
                                            borderColor: 'rgba(34, 197, 94, 1)',
                                            borderWidth: 2,
                                            pointBackgroundColor: 'rgba(34, 197, 94, 1)'
                                        }}]
                                    }},
                                    options: {{
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        plugins: {{ legend: {{ display: false }} }},
                                        scales: {{
                                            r: {{
                                                min: 0,
                                                max: 100,
                                                ticks: {{ stepSize: 20, color: '#64748b' }},
                                                grid: {{ color: 'rgba(148, 163, 184, 0.2)' }},
                                                pointLabels: {{ color: '#94a3b8', font: {{ size: 11 }} }}
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
        # 10. MISSING DATA PATTERN
        # ═══════════════════════════════════════════════════════════════
        missingness_impact = ml_findings.get('missingness_impact', {}) if ml_findings else {}
        if missingness_impact:
            missing_cards = []
            all_diffs = []  # Track all differential values for summary
            all_fields = []  # Track field names
            for target_col, analysis in list(missingness_impact.items())[:2]:
                biased_features = analysis.get('features_with_differential_missingness', [])
                if biased_features:
                    for feat in biased_features[:4]:
                        # Use correct field names from JSON structure
                        field = feat.get('feature', feat.get('field', ''))
                        diff = feat.get('max_difference', feat.get('differential_pct', 0))
                        all_diffs.append(abs(diff))
                        all_fields.append(field)

                        # Calculate overall missing from missingness_by_target if available
                        missingness_by_target = feat.get('missingness_by_target', {})
                        if missingness_by_target:
                            total_missing = 0
                            total_rows = 0
                            for target_val, stats in missingness_by_target.items():
                                rate = stats.get('rate', 0)
                                count = stats.get('total', 0)
                                total_missing += (rate / 100) * count
                                total_rows += count
                            overall = (total_missing / total_rows * 100) if total_rows > 0 else 0
                        else:
                            overall = feat.get('overall_missing_pct', 0)

                        severity = 'critical' if abs(diff) > 10 else 'warning' if abs(diff) > 5 else 'info'

                        missing_cards.append(f'''
                            <div style="flex: 1; min-width: 180px; background: var(--bg-card); border-radius: 8px; padding: 12px; border: 1px solid var(--border-subtle);">
                                <div style="font-size: 0.85em; font-weight: 600; color: var(--text-primary); margin-bottom: 8px;">{field}</div>
                                <div style="display: flex; justify-content: space-between; margin-bottom: 4px;">
                                    <span style="font-size: 0.75em; color: var(--text-muted);">Overall Missing</span>
                                    <span style="font-size: 0.85em; font-weight: 500; color: var(--text-secondary);">{overall:.1f}%</span>
                                </div>
                                <div style="display: flex; justify-content: space-between;">
                                    <span style="font-size: 0.75em; color: var(--text-muted);">Differential</span>
                                    <span style="font-size: 0.85em; font-weight: 600; color: var(--{severity});">{diff:+.1f}%</span>
                                </div>
                            </div>''')

            if missing_cards:
                # Calculate stats for dual-layer explanation
                max_diff = max(all_diffs) if all_diffs else 0
                avg_diff = sum(all_diffs) / len(all_diffs) if all_diffs else 0
                critical_count = sum(1 for d in all_diffs if d > 10)
                warning_count = sum(1 for d in all_diffs if 5 < d <= 10)

                # Find field with highest differential
                max_idx = all_diffs.index(max_diff) if all_diffs else 0
                worst_field = all_fields[max_idx] if all_fields else "N/A"

                # Plain-English summary based on severity
                if max_diff > 10:
                    plain_summary = f"Missing values are not evenly spread across different groups in your data. The column '{worst_field}' has a {max_diff:.2f}% difference in missing rates between groups. Some groups have more gaps than others, which could affect your analysis if you delete or fill in missing rows without considering this."
                elif max_diff > 5:
                    plain_summary = f"Some groups have more missing values than others. The biggest gap is {max_diff:.2f}% in '{worst_field}'. This suggests missing data isn't completely random, so look at patterns before deciding how to handle gaps."
                else:
                    plain_summary = f"Missing values are spread fairly evenly across groups (largest gap: {max_diff:.2f}%). This means the gaps in your data look random, which is a good sign for filling them in with standard methods."

                missing_dual_layer = self._build_dual_layer_explanation(
                    plain_english=plain_summary,
                    technical_stats={
                        "Fields with Bias": f"{len(missing_cards)}",
                        "Max Differential": f"{max_diff:.2f}%",
                        "Avg Differential": f"{avg_diff:.2f}%",
                        "Critical (>10%)": f"{critical_count}",
                        "Warning (5-10%)": f"{warning_count}"
                    },
                    technical_context=[
                        "Differential = max missingness rate difference between target classes",
                        "MCAR: Missing Completely At Random (ideal)",
                        "MAR: Missing At Random (conditional on observed data)",
                        "MNAR: Missing Not At Random (problematic for analysis)"
                    ]
                )

                charts_by_section['missingness'].append(f'''
                    <div class="accordion" data-accordion="viz-missing-pattern">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #a855f7, #7c3aed);">🔳</div>
                                <div>
                                    <div class="accordion-title">Missing Data Pattern Analysis</div>
                                    <div class="accordion-subtitle">Detect non-random missingness across target classes</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge warning">{len(missing_cards)} Biased Fields</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            {missing_dual_layer}
                            <div style="display: flex; flex-wrap: wrap; gap: 12px; margin-top: 16px;">
                                {''.join(missing_cards)}
                            </div>
                        </div>
                    </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 11. FEATURE CORRELATION MINI-HEATMAP
        # ═══════════════════════════════════════════════════════════════
        mixed_corr = ml_findings.get('mixed_correlation_matrix', {}) if ml_findings else {}
        corr_ratio_data = mixed_corr.get('correlation_ratio', {})
        if corr_ratio_data:
            # Build a simple visual representation of correlations
            corr_items = []
            for pair_key, corr_info in list(corr_ratio_data.items())[:12]:
                # Use 'eta' key (correct) with fallback to 'correlation_ratio'
                eta = corr_info.get('eta', corr_info.get('correlation_ratio', 0))
                cat_col = corr_info.get('categorical_column', pair_key.split('_vs_')[0])
                num_col = corr_info.get('numeric_column', pair_key.split('_vs_')[-1])

                # Color intensity based on correlation strength
                intensity = min(100, int(eta * 100))
                bg_color = f'rgba(59, 130, 246, {eta:.2f})' if eta > 0 else 'rgba(148, 163, 184, 0.1)'

                corr_items.append(f'''
                    <div style="display: flex; align-items: center; gap: 8px; padding: 8px; background: var(--bg-card); border-radius: 6px;">
                        <div style="width: 50px; height: 24px; background: {bg_color}; border-radius: 4px; display: flex; align-items: center; justify-content: center;">
                            <span style="font-size: 0.7em; font-weight: bold; color: {'white' if eta > 0.3 else 'var(--text-secondary)'};">{eta:.2f}</span>
                        </div>
                        <div style="font-size: 0.75em; color: var(--text-secondary);">
                            <span style="color: var(--text-primary);">{cat_col[:15]}</span> ↔ <span style="color: var(--text-primary);">{num_col[:15]}</span>
                        </div>
                    </div>''')

            if corr_items:
                # Calculate stats for dual-layer explanation
                eta_values = [corr_info.get('eta', corr_info.get('correlation_ratio', 0)) for _, corr_info in list(corr_ratio_data.items())[:12]]
                max_eta = max(eta_values) if eta_values else 0
                avg_eta = sum(eta_values) / len(eta_values) if eta_values else 0
                strong_count = sum(1 for e in eta_values if e >= 0.3)
                moderate_count = sum(1 for e in eta_values if 0.1 <= e < 0.3)

                # Find strongest correlation pair
                strongest_pair = max(corr_ratio_data.items(), key=lambda x: x[1].get('eta', x[1].get('correlation_ratio', 0)))
                strongest_eta = strongest_pair[1].get('eta', strongest_pair[1].get('correlation_ratio', 0))
                strongest_cat = strongest_pair[1].get('categorical_column', strongest_pair[0].split('_vs_')[0])
                strongest_num = strongest_pair[1].get('numeric_column', strongest_pair[0].split('_vs_')[-1])

                # Plain-English summary based on results
                if strong_count > 0:
                    plain_summary = f"Found {strong_count} strong link(s) between category and number columns. The strongest is between '{strongest_cat}' and '{strongest_num}', meaning the category someone falls into explains about {int(strongest_eta*100)}% of the differences in '{strongest_num}' values."
                elif moderate_count > 0:
                    plain_summary = f"Found {moderate_count} moderate link(s) between category and number columns. No very strong connections, but some categories seem related to certain number patterns."
                else:
                    plain_summary = f"Category columns don't strongly predict number values. The categories and numbers appear to be mostly independent of each other."

                corr_dual_layer = self._build_dual_layer_explanation(
                    plain_english=plain_summary,
                    technical_stats={
                        "Pairs Analyzed": f"{len(eta_values)}",
                        "Max η": f"{max_eta:.3f}",
                        "Avg η": f"{avg_eta:.3f}",
                        "Strong (≥0.3)": f"{strong_count}",
                        "Moderate (0.1-0.3)": f"{moderate_count}"
                    },
                    technical_context=[
                        "Correlation Ratio (η) = √(Between-group variance / Total variance)",
                        "Range: 0 (no relationship) to 1 (perfect prediction)",
                        "η ≥ 0.3: Strong association",
                        "0.1 ≤ η < 0.3: Moderate association",
                        "η < 0.1: Weak or no association"
                    ]
                )

                charts_by_section['correlations'].append(f'''
                    <div class="accordion" data-accordion="viz-correlation">
                        <div class="accordion-header" onclick="toggleAccordion(this)">
                            <div class="accordion-title-group">
                                <div class="accordion-icon" style="background: linear-gradient(135deg, #3b82f6, #1d4ed8);">🔗</div>
                                <div>
                                    <div class="accordion-title">Feature Correlation Matrix</div>
                                    <div class="accordion-subtitle">Categorical-numeric associations using Correlation Ratio (η)</div>
                                </div>
                            </div>
                            <div class="accordion-meta">
                                <span class="accordion-badge info">{len(corr_ratio_data)} Pairs</span>
                                <span class="accordion-chevron">▼</span>
                            </div>
                        </div>
                        <div class="accordion-content">
                            {corr_dual_layer}
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 8px; margin-top: 16px;">
                                {''.join(corr_items)}
                            </div>
                        </div>
                    </div>''')

        # ═══════════════════════════════════════════════════════════════
        # 12. OUTLIER COMPARISON CHART
        # ═══════════════════════════════════════════════════════════════
        # Use all_numeric_outlier_stats for visualization (includes 0% outlier fields)
        # Fallback to numeric_outliers if all_numeric not available
        all_numeric_stats = ml_findings.get('all_numeric_outlier_stats', {}) if ml_findings else {}
        chart_outlier_data = all_numeric_stats if all_numeric_stats else numeric_outliers

        # Show fallback message when there are too few numeric fields
        if not chart_outlier_data or len(chart_outlier_data) < 2:
            fallback_reason = "No numeric outlier data available" if not chart_outlier_data else f"Only {len(chart_outlier_data)} numeric field(s) available (need at least 2 for comparison)"
            charts_by_section['distributions'].append(f'''
                <div class="accordion" data-accordion="viz-outlier-comparison">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #94a3b8, #64748b);">📊</div>
                            <div>
                                <div class="accordion-title">Outlier Comparison</div>
                                <div class="accordion-subtitle">Compare outlier rates across numeric fields</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge neutral">Not Available</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        <div style="padding: 24px; background: var(--bg-card); border-radius: 8px; text-align: center;">
                            <div style="font-size: 2em; margin-bottom: 12px;">📊</div>
                            <h4 style="margin: 0 0 8px 0; color: var(--text-primary);">Insufficient Numeric Fields</h4>
                            <p style="color: var(--text-muted); margin: 0; max-width: 500px; margin: 0 auto;">
                                {fallback_reason}. This chart compares outlier rates across multiple numeric columns to help identify which fields have unusual distributions.
                            </p>
                            <p style="color: var(--text-muted); font-size: 0.85em; margin-top: 12px;">
                                💡 <strong>Tip:</strong> Datasets with monetary amounts, measurements, or counts typically have more numeric fields suitable for this analysis.
                            </p>
                        </div>
                    </div>
                </div>''')
        elif chart_outlier_data and len(chart_outlier_data) >= 2:
            outlier_comparison = []
            for col, data in list(chart_outlier_data.items())[:10]:
                # Try both key formats (anomaly_* from MLAnalyzer, outlier_* from older code)
                outlier_pct = data.get('anomaly_percentage', data.get('outlier_percentage', 0))
                outlier_count = data.get('anomaly_count', data.get('outlier_count', 0))
                outlier_comparison.append({
                    'col': col,
                    'pct': outlier_pct,
                    'count': outlier_count
                })

            # Sort by percentage descending
            outlier_comparison.sort(key=lambda x: x['pct'], reverse=True)

            # Calculate stats for dual-layer explanation
            outlier_pcts = [d['pct'] for d in outlier_comparison]
            total_outlier_count = sum(d['count'] for d in outlier_comparison)
            avg_outlier_pct = sum(outlier_pcts) / len(outlier_pcts) if outlier_pcts else 0
            max_outlier = outlier_comparison[0] if outlier_comparison else {'col': 'N/A', 'pct': 0}
            min_outlier = outlier_comparison[-1] if outlier_comparison else {'col': 'N/A', 'pct': 0}
            high_outlier_count = sum(1 for pct in outlier_pcts if pct > 5)
            moderate_outlier_count = sum(1 for pct in outlier_pcts if 1 < pct <= 5)

            # Check if ALL outlier rates are zero - show success message instead of empty chart
            all_zero_outliers = all(pct == 0 for pct in outlier_pcts)

            # Plain-English summary based on distribution
            if all_zero_outliers:
                plain_summary = f"Excellent! All {len(outlier_comparison)} numeric columns have no IQR outliers detected. This indicates well-behaved, consistent data without extreme values falling outside the expected range."
            elif high_outlier_count > len(outlier_comparison) / 2:
                plain_summary = f"Many columns ({high_outlier_count} of {len(outlier_comparison)}) have a lot of extreme values (>5%). '{max_outlier['col']}' has the most at {max_outlier['pct']:.2f}%. This pattern could mean data quality issues, measurement problems, or data coming from different sources."
            elif max_outlier['pct'] > 10:
                plain_summary = f"Most columns look normal, but '{max_outlier['col']}' stands out with {max_outlier['pct']:.2f}% of values being unusually high or low. Worth looking into for data entry errors or naturally extreme cases."
            elif avg_outlier_pct < 1:
                plain_summary = f"All {len(outlier_comparison)} number columns have very few extreme values (averaging {avg_outlier_pct:.2f}%). This means the data is well-behaved with consistent values across all columns."
            else:
                plain_summary = f"Extreme values vary across columns. '{max_outlier['col']}' has the most ({max_outlier['pct']:.2f}%) while '{min_outlier['col']}' has the fewest ({min_outlier['pct']:.2f}%). Columns with more extremes may be worth looking at more closely."

            outlier_dual_layer = self._build_dual_layer_explanation(
                plain_english=plain_summary,
                technical_stats={
                    "Fields Analyzed": f"{len(outlier_comparison)}",
                    "Total Outliers": f"{total_outlier_count:,}",
                    "Avg Outlier Rate": f"{avg_outlier_pct:.2f}%",
                    "Highest": f"{max_outlier['col']} ({max_outlier['pct']:.2f}%)",
                    "High Rate (>5%)": f"{high_outlier_count} fields"
                },
                technical_context=[
                    "Outlier detection uses IQR (Interquartile Range) method",
                    "Lower bound: Q1 - 1.5 × IQR",
                    "Upper bound: Q3 + 1.5 × IQR",
                    "Red bars: >5% outliers, Yellow: 1-5%, Green: <1%"
                ]
            )

            # If all outlier rates are zero, show a success message instead of empty chart
            if all_zero_outliers:
                charts_by_section['distributions'].append(f'''
                <div class="accordion" data-accordion="viz-outlier-comparison">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #22c55e, #16a34a);">✓</div>
                            <div>
                                <div class="accordion-title">Outlier Comparison</div>
                                <div class="accordion-subtitle">Compare outlier rates across numeric fields</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge good">No Outliers</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        {outlier_dual_layer}
                        <div style="padding: 32px; background: var(--bg-card); border-radius: 8px; text-align: center; border: 2px solid rgba(34, 197, 94, 0.3);">
                            <div style="font-size: 3em; margin-bottom: 16px;">✓</div>
                            <h4 style="margin: 0 0 12px 0; color: #22c55e; font-size: 1.2em;">No IQR Outliers Detected</h4>
                            <p style="color: var(--text-muted); margin: 0; max-width: 500px; margin: 0 auto; line-height: 1.6;">
                                All {len(outlier_comparison)} numeric columns have values within expected ranges.
                                No data points fall outside the IQR bounds (Q1 - 1.5×IQR to Q3 + 1.5×IQR).
                            </p>
                            <div style="margin-top: 20px; display: flex; justify-content: center; gap: 24px; flex-wrap: wrap;">
                                {' '.join([f'<span style="background: rgba(34, 197, 94, 0.1); padding: 6px 12px; border-radius: 6px; font-size: 0.85em; color: #22c55e;">{d["col"]}: 0%</span>' for d in outlier_comparison[:6]])}
                            </div>
                        </div>
                    </div>
                </div>''')
            else:
                charts_by_section['distributions'].append(f'''
                <div class="accordion" data-accordion="viz-outlier-comparison">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon" style="background: linear-gradient(135deg, #ef4444, #dc2626);">📊</div>
                            <div>
                                <div class="accordion-title">Outlier Comparison</div>
                                <div class="accordion-subtitle">Compare outlier rates across numeric fields</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{len(outlier_comparison)} Fields</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-content">
                        {outlier_dual_layer}
                        <div style="background: var(--bg-card); border-radius: 8px; padding: 16px; height: 300px;">
                            <canvas id="outlierComparisonChart"></canvas>
                        </div>
                        <script>
                        document.addEventListener('DOMContentLoaded', function() {{
                            const outlierCtx = document.getElementById('outlierComparisonChart');
                            if (outlierCtx) {{
                                const outlierData = {json.dumps(outlier_comparison)};
                                new Chart(outlierCtx, {{
                                    type: 'bar',
                                    data: {{
                                        labels: outlierData.map(d => d.col.length > 12 ? d.col.slice(0, 10) + '...' : d.col),
                                        datasets: [{{
                                            label: 'Outlier %',
                                            data: outlierData.map(d => d.pct),
                                            backgroundColor: outlierData.map(d => d.pct > 5 ? 'rgba(239, 68, 68, 0.7)' : d.pct > 1 ? 'rgba(245, 158, 11, 0.7)' : 'rgba(34, 197, 94, 0.7)'),
                                            borderWidth: 0,
                                            borderRadius: 4
                                        }}]
                                    }},
                                    options: {{
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        indexAxis: 'y',
                                        plugins: {{
                                            legend: {{ display: false }},
                                            tooltip: {{
                                                callbacks: {{
                                                    label: function(ctx) {{
                                                        const d = outlierData[ctx.dataIndex];
                                                        return `${{d.pct.toFixed(2)}}% (${{d.count.toLocaleString()}} values)`;
                                                    }}
                                                }}
                                            }}
                                        }},
                                        scales: {{
                                            x: {{
                                                title: {{ display: true, text: 'Outlier Percentage', color: '#64748b' }},
                                                grid: {{ color: 'rgba(148, 163, 184, 0.1)' }},
                                                ticks: {{ color: '#64748b' }}
                                            }},
                                            y: {{
                                                grid: {{ display: false }},
                                                ticks: {{ color: '#94a3b8', font: {{ size: 10 }} }}
                                            }}
                                        }}
                                    }}
                                }});
                            }}
                        }});
                        </script>
                    </div>
                </div>''')

        # Return the categorized charts dictionary
        # Each section generator can retrieve its relevant charts using:
        #   charts = self._generate_advanced_visualizations(ml_findings, columns)
        #   section_charts = ''.join(charts.get('section_name', []))
        return charts_by_section

    def _generate_overview_accordion(self, profile: ProfileResult, type_counts: Dict,
                                     avg_completeness: float, avg_validity: float,
                                     avg_consistency: float, avg_uniqueness: float) -> str:
        """Generate the data types accordion with dual-layer explanation."""

        # Build type breakdown text
        type_breakdown = ', '.join([f"{count} {t}" for t, count in type_counts.items() if count > 0])

        # Determine predominant type for plain English description
        predominant_type = max(type_counts.items(), key=lambda x: x[1])[0] if type_counts else 'mixed'
        numeric_count = type_counts.get('Integer', 0) + type_counts.get('Float', 0)
        text_count = type_counts.get('String', 0)

        # Generate plain-English summary
        plain_english = f"Your dataset has {profile.column_count} columns. "
        if numeric_count > text_count:
            plain_english += f"Most columns ({numeric_count}) contain numeric data like counts, amounts, or measurements. "
        elif text_count > numeric_count:
            plain_english += f"Most columns ({text_count}) contain text data like names, categories, or descriptions. "
        else:
            plain_english += "The data has a balanced mix of numeric and text columns. "

        if type_counts.get('Datetime', 0) > 0:
            plain_english += f"There are also {type_counts.get('Datetime', 0)} date/time column(s) for temporal analysis."

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
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">{plain_english}</div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content">
                                        <div class="dual-layer-technical-grid">
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Integer</span>
                                                <span class="dual-layer-technical-item-value">{type_counts.get('Integer', 0)} columns</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Float</span>
                                                <span class="dual-layer-technical-item-value">{type_counts.get('Float', 0)} columns</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">String</span>
                                                <span class="dual-layer-technical-item-value">{type_counts.get('String', 0)} columns</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Datetime</span>
                                                <span class="dual-layer-technical-item-value">{type_counts.get('Datetime', 0)} columns</span>
                                            </div>
                                        </div>
                                        <div class="dual-layer-technical-context">
                                            <ul>
                                                <li>Integer: Whole numbers (counts, IDs, quantities)</li>
                                                <li>Float: Decimal numbers (prices, percentages, measurements)</li>
                                                <li>String: Text data (names, categories, codes)</li>
                                                <li>Datetime: Date/time values for temporal analysis</li>
                                            </ul>
                                        </div>
                                    </div>
                                </details>
                            </div>
                            <div class="chart-container">
                                <div class="chart-title">Data Type Distribution</div>
                                <canvas id="typeChart" height="100"></canvas>
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_semantic_classification_accordion(self, profile: ProfileResult) -> str:
        """
        Generate unified Semantic Classification accordion.

        Combines both Schema.org general semantics and FIBO financial ontology
        classifications into a single comprehensive view.
        """
        columns = profile.columns

        # Collect semantic classifications from all columns
        schema_org_types = {}  # type -> list of column names
        fibo_types = {}        # type -> list of column names
        resolved_classifications = []  # (column_name, resolved_type, source, confidence)

        columns_with_semantics = 0
        fibo_matched = 0
        schema_org_matched = 0
        unclassified_columns = []  # Columns with no strong semantic match

        for col in columns:
            if not col.semantic_info:
                continue

            resolved = col.semantic_info.get('resolved', {})
            schema_org = col.semantic_info.get('schema_org', {})
            fibo = col.semantic_info.get('fibo', {})

            primary_source = resolved.get('primary_source', 'none')
            primary_type = resolved.get('primary_type', '')
            display_label = resolved.get('display_label', '')

            # Get confidence from the source-specific object
            if primary_source == 'fibo' and fibo:
                confidence = fibo.get('confidence', 0)
            elif primary_source == 'schema_org' and schema_org:
                confidence = schema_org.get('confidence', 0)
            else:
                confidence = 0

            if primary_source != 'none' and primary_type:
                columns_with_semantics += 1
                resolved_classifications.append({
                    'column': col.name,
                    'type': primary_type,
                    'display_label': display_label,
                    'source': primary_source,
                    'confidence': confidence
                })

                if primary_source == 'fibo':
                    fibo_matched += 1
                    category = primary_type.split('.')[0] if '.' in primary_type else primary_type
                    if category not in fibo_types:
                        fibo_types[category] = []
                    fibo_types[category].append(col.name)
                elif primary_source == 'schema_org':
                    schema_org_matched += 1
                    if primary_type not in schema_org_types:
                        schema_org_types[primary_type] = []
                    schema_org_types[primary_type].append(col.name)
            else:
                # Track unclassified columns with reasoning
                reasons = []
                schema_conf = schema_org.get('confidence', 0) if schema_org else 0
                fibo_conf = fibo.get('confidence', 0) if fibo else 0

                if schema_conf < 0.5 and fibo_conf < 0.5:
                    reasons.append("No strong pattern match in either ontology")
                if schema_org and schema_org.get('type', '').endswith(('Integer', 'Text', 'Number')):
                    reasons.append("Only generic type detected (no specific semantic meaning)")

                unclassified_columns.append({
                    'column': col.name,
                    'data_type': col.semantic_info.get('structural_type', 'unknown'),
                    'schema_org_tried': schema_org.get('type', 'none') if schema_org else 'none',
                    'schema_org_conf': schema_conf,
                    'fibo_tried': fibo.get('type', 'none') if fibo else 'none',
                    'fibo_conf': fibo_conf,
                    'reasons': reasons or ["Column name/values don't match known patterns"]
                })

        # If no semantic classifications, return empty
        if columns_with_semantics == 0:
            return ''

        # Category icons for both ontologies
        category_icons = {
            # FIBO categories
            'money': '💰', 'identifier': '🔑', 'party': '👤', 'datetime': '📅',
            'location': '📍', 'account': '🏦', 'transaction': '💸', 'product': '📦',
            # Schema.org categories
            'person': '👤', 'organization': '🏢', 'postaladdress': '📍',
            'monetaryamount': '💰', 'contactpoint': '📧', 'datetime': '📅',
            'email': '📧', 'telephone': '📞', 'url': '🔗', 'text': '📝',
            'number': '🔢', 'integer': '🔢', 'boolean': '✓', 'date': '📅',
            'quantitativevalue': '📊', 'propertyvalue': '📋', 'thing': '📦'
        }

        # Build FIBO chips
        fibo_chips = ''
        for category, cols in sorted(fibo_types.items(), key=lambda x: -len(x[1])):
            icon = category_icons.get(category.lower(), '📋')
            fibo_chips += f'''
                <div class="semantic-chip fibo" title="FIBO: {category} - {len(cols)} column(s)">
                    <span class="chip-icon">{icon}</span>
                    <span class="chip-label">{category.title()}</span>
                    <span class="chip-count">{len(cols)}</span>
                    <span class="chip-source">FIBO</span>
                </div>'''

        # Build Schema.org chips
        schema_chips = ''
        for schema_type, cols in sorted(schema_org_types.items(), key=lambda x: -len(x[1])):
            icon = category_icons.get(schema_type.lower(), '📋')
            schema_chips += f'''
                <div class="semantic-chip schema-org" title="Schema.org: {schema_type} - {len(cols)} column(s)">
                    <span class="chip-icon">{icon}</span>
                    <span class="chip-label">{schema_type}</span>
                    <span class="chip-count">{len(cols)}</span>
                    <span class="chip-source">Schema.org</span>
                </div>'''

        # Build column mapping table
        table_rows = ''
        for item in sorted(resolved_classifications, key=lambda x: x['source']):
            source_badge = 'fibo' if item['source'] == 'fibo' else 'schema'
            source_label = 'FIBO' if item['source'] == 'fibo' else 'Schema.org'
            conf_pct = item['confidence'] * 100 if item['confidence'] <= 1 else item['confidence']
            table_rows += f'''
                <tr>
                    <td><code>{item['column']}</code></td>
                    <td>{item['display_label'] or item['type']}</td>
                    <td><span class="source-badge {source_badge}">{source_label}</span></td>
                    <td>{conf_pct:.0f}%</td>
                </tr>'''

        # Build summary stats
        unclassified_count = len(unclassified_columns)

        return f'''
                <div class="accordion" data-accordion="semantics">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon semantics" style="background: linear-gradient(135deg, #8b5cf6 0%, #3b82f6 100%);">🏷️</div>
                            <div>
                                <div class="accordion-title">Semantic Classification</div>
                                <div class="accordion-subtitle">What type of data is in each column?</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge info">{columns_with_semantics}/{len(columns)} classified</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <!-- Plain English Explanation -->
                            <div class="dual-layer plain-layer">
                                <div class="layer-label">📖 What is this?</div>
                                <p style="color: var(--text-secondary); line-height: 1.7; margin-bottom: 12px;">
                                    DataK9 analyzes each column to understand what kind of real-world data it contains.
                                    For example, is "amount" a price? Is "cust_id" a customer identifier? This helps generate smarter validation rules.
                                </p>
                            </div>

                            <!-- How it works -->
                            <div style="background: var(--bg-tertiary); border-radius: 8px; padding: 16px; margin-bottom: 20px;">
                                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 12px; font-size: 0.9em;">
                                    🔍 How Classification Works
                                </div>
                                <p style="color: var(--text-secondary); font-size: 0.85em; line-height: 1.6; margin-bottom: 12px;">
                                    DataK9 checks each column against two standard vocabularies, in order of priority:
                                </p>
                                <div style="display: flex; flex-direction: column; gap: 12px;">
                                    <div style="display: flex; align-items: flex-start; gap: 12px;">
                                        <div style="background: linear-gradient(135deg, #059669 0%, #10b981 100%); color: white; padding: 4px 10px; border-radius: 6px; font-weight: 600; font-size: 0.8em; white-space: nowrap;">1. FIBO</div>
                                        <div style="font-size: 0.85em; color: var(--text-secondary);">
                                            <strong>Financial Industry Business Ontology</strong> — Matches financial patterns like account numbers, transaction amounts, currencies, and party identifiers.
                                            Best for banking, insurance, and financial datasets.
                                        </div>
                                    </div>
                                    <div style="display: flex; align-items: flex-start; gap: 12px;">
                                        <div style="background: linear-gradient(135deg, #3b82f6 0%, #6366f1 100%); color: white; padding: 4px 10px; border-radius: 6px; font-weight: 600; font-size: 0.8em; white-space: nowrap;">2. Schema.org</div>
                                        <div style="font-size: 0.85em; color: var(--text-secondary);">
                                            <strong>Web Vocabulary Standard</strong> — Matches general patterns like names, emails, dates, addresses, phone numbers, and categories.
                                            Works across all types of datasets.
                                        </div>
                                    </div>
                                </div>
                                <p style="color: var(--text-muted); font-size: 0.8em; margin-top: 12px; font-style: italic;">
                                    FIBO is checked first. If no strong financial match is found, Schema.org provides a general classification.
                                </p>
                            </div>

                            <!-- Results Summary -->
                            <div style="margin-bottom: 20px;">
                                <div style="font-weight: 600; color: var(--text-primary); margin-bottom: 12px; font-size: 0.9em;">
                                    📊 Classification Results
                                </div>
                                <div style="display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 16px;">
                                    <div style="background: var(--bg-tertiary); padding: 12px 16px; border-radius: 8px; text-align: center;">
                                        <div style="font-size: 1.5em; font-weight: 700; color: var(--success-color);">{columns_with_semantics}</div>
                                        <div style="font-size: 0.75em; color: var(--text-muted);">Classified</div>
                                    </div>
                                    {f'<div style="background: var(--bg-tertiary); padding: 12px 16px; border-radius: 8px; text-align: center;"><div style="font-size: 1.5em; font-weight: 700; color: #10b981;">{fibo_matched}</div><div style="font-size: 0.75em; color: var(--text-muted);">FIBO matches</div></div>' if fibo_matched > 0 else ''}
                                    {f'<div style="background: var(--bg-tertiary); padding: 12px 16px; border-radius: 8px; text-align: center;"><div style="font-size: 1.5em; font-weight: 700; color: #6366f1;">{schema_org_matched}</div><div style="font-size: 0.75em; color: var(--text-muted);">Schema.org matches</div></div>' if schema_org_matched > 0 else ''}
                                    {f'<div style="background: var(--bg-tertiary); padding: 12px 16px; border-radius: 8px; text-align: center;"><div style="font-size: 1.5em; font-weight: 700; color: var(--warning-color);">{unclassified_count}</div><div style="font-size: 0.75em; color: var(--text-muted);">Unclassified</div></div>' if unclassified_count > 0 else ''}
                                </div>

                                {f"""<div style="margin-bottom: 16px;">
                                    <div style="font-size: 0.8em; color: var(--text-muted); margin-bottom: 8px; display: flex; align-items: center; gap: 6px;">
                                        <span style="background: linear-gradient(135deg, #059669 0%, #10b981 100%); color: white; padding: 2px 8px; border-radius: 4px; font-weight: 600;">FIBO</span>
                                        Financial patterns detected
                                    </div>
                                    <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                                        {fibo_chips}
                                    </div>
                                </div>""" if fibo_chips else ""}

                                {f"""<div style="margin-bottom: 16px;">
                                    <div style="font-size: 0.8em; color: var(--text-muted); margin-bottom: 8px; display: flex; align-items: center; gap: 6px;">
                                        <span style="background: linear-gradient(135deg, #3b82f6 0%, #6366f1 100%); color: white; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Schema.org</span>
                                        General patterns detected
                                    </div>
                                    <div style="display: flex; flex-wrap: wrap; gap: 8px;">
                                        {schema_chips}
                                    </div>
                                </div>""" if schema_chips else ""}
                            </div>

                            {self._generate_unclassified_section(unclassified_columns)}

                            <!-- Technical Layer -->
                            <details style="margin-top: 16px;">
                                <summary style="cursor: pointer; color: var(--text-secondary); font-size: 0.85em; padding: 8px 0;">
                                    🔧 Technical Details: Column Mappings
                                </summary>
                                <div style="margin-top: 12px; overflow-x: auto;">
                                    <table style="width: 100%; border-collapse: collapse; font-size: 0.85em;">
                                        <thead>
                                            <tr style="border-bottom: 1px solid var(--border-subtle);">
                                                <th style="text-align: left; padding: 8px; color: var(--text-muted);">Column</th>
                                                <th style="text-align: left; padding: 8px; color: var(--text-muted);">Semantic Type</th>
                                                <th style="text-align: left; padding: 8px; color: var(--text-muted);">Source</th>
                                                <th style="text-align: left; padding: 8px; color: var(--text-muted);">Confidence</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {table_rows}
                                        </tbody>
                                    </table>
                                </div>
                                <div class="hint-box" style="margin-top: 12px; border-left-color: #8b5cf6;">
                                    <strong>💡 About Semantic Classification:</strong><br>
                                    <strong>FIBO</strong> (Financial Industry Business Ontology) identifies financial domain patterns like accounts, transactions, and monetary values.
                                    <strong>Schema.org</strong> provides general web vocabulary for common types like Person, Organization, Email, and Address.
                                    DataK9 uses the best match from either ontology to drive intelligent validation suggestions.
                                </div>
                            </details>
                        </div>
                    </div>
                </div>'''

    def _generate_unclassified_section(self, unclassified_columns: list) -> str:
        """Generate the unclassified columns section showing why columns didn't match."""
        if not unclassified_columns:
            return ''

        # Build unclassified table rows
        unclassified_rows = ''
        for item in unclassified_columns:
            schema_tried = item['schema_org_tried']
            schema_conf = item['schema_org_conf'] * 100 if item['schema_org_conf'] <= 1 else item['schema_org_conf']
            fibo_tried = item['fibo_tried']
            fibo_conf = item['fibo_conf'] * 100 if item['fibo_conf'] <= 1 else item['fibo_conf']
            reasons = '; '.join(item['reasons'])

            unclassified_rows += f'''
                <tr>
                    <td><code>{item['column']}</code></td>
                    <td>{item['data_type']}</td>
                    <td><span style="color: var(--text-muted);">{schema_tried}</span> <span style="opacity: 0.6;">({schema_conf:.0f}%)</span></td>
                    <td><span style="color: var(--text-muted);">{fibo_tried}</span> <span style="opacity: 0.6;">({fibo_conf:.0f}%)</span></td>
                    <td style="font-size: 0.8em; color: var(--text-muted);">{reasons}</td>
                </tr>'''

        return f'''
                            <!-- Unclassified Columns -->
                            <details style="margin-top: 16px;">
                                <summary style="cursor: pointer; color: var(--warning-color); font-size: 0.85em; padding: 8px 0;">
                                    ⚠️ Unclassified Columns ({len(unclassified_columns)}) - Why they didn't match
                                </summary>
                                <div style="margin-top: 12px; overflow-x: auto;">
                                    <p style="color: var(--text-secondary); font-size: 0.85em; margin-bottom: 12px;">
                                        These columns couldn't be confidently classified by either ontology. This may indicate domain-specific data
                                        that doesn't fit standard financial (FIBO) or web (Schema.org) patterns.
                                    </p>
                                    <table style="width: 100%; border-collapse: collapse; font-size: 0.8em;">
                                        <thead>
                                            <tr style="border-bottom: 1px solid var(--border-subtle);">
                                                <th style="text-align: left; padding: 6px; color: var(--text-muted);">Column</th>
                                                <th style="text-align: left; padding: 6px; color: var(--text-muted);">Data Type</th>
                                                <th style="text-align: left; padding: 6px; color: var(--text-muted);">Schema.org Tried</th>
                                                <th style="text-align: left; padding: 6px; color: var(--text-muted);">FIBO Tried</th>
                                                <th style="text-align: left; padding: 6px; color: var(--text-muted);">Reason</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {unclassified_rows}
                                        </tbody>
                                    </table>
                                </div>
                            </details>'''

    def _generate_quality_accordion(self, profile: ProfileResult) -> str:
        """Generate the quality metrics accordion with dual-layer explanation."""

        # Calculate quality metrics for plain English explanation
        avg_completeness = sum(col.quality.completeness for col in profile.columns) / len(profile.columns) if profile.columns else 0
        avg_validity = sum(col.quality.validity for col in profile.columns) / len(profile.columns) if profile.columns else 0

        # Count columns with issues
        low_completeness_cols = [col.name for col in profile.columns if col.quality.completeness < 90]
        low_validity_cols = [col.name for col in profile.columns if col.quality.validity < 90]

        # Generate plain-English summary
        score = profile.overall_quality_score
        if score >= 90:
            quality_description = "Your data quality is excellent."
        elif score >= 80:
            quality_description = "Your data quality is good with minor gaps."
        elif score >= 70:
            quality_description = "Your data quality is acceptable but has some areas needing attention."
        else:
            quality_description = "Your data quality needs improvement in several areas."

        plain_english = f"{quality_description} "
        if low_completeness_cols:
            if len(low_completeness_cols) <= 3:
                plain_english += f"The columns {', '.join(low_completeness_cols)} have missing values that may affect analysis. "
            else:
                plain_english += f"{len(low_completeness_cols)} columns have significant missing values. "
        else:
            plain_english += "All columns have good data coverage with minimal missing values. "

        return f'''
                <div class="accordion" id="section-missingness" data-accordion="quality">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">✓</div>
                            <div>
                                <div class="accordion-title">Missingness & Bias</div>
                                <div class="accordion-subtitle">Completeness, null patterns, and data coverage</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge {'good' if profile.overall_quality_score >= 80 else 'warning'}">{profile.overall_quality_score:.0f}%</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">{plain_english}</div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content">
                                        <div class="dual-layer-technical-grid">
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Overall Score</span>
                                                <span class="dual-layer-technical-item-value">{profile.overall_quality_score:.1f}%</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Avg Completeness</span>
                                                <span class="dual-layer-technical-item-value">{avg_completeness:.1f}%</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Avg Validity</span>
                                                <span class="dual-layer-technical-item-value">{avg_validity:.1f}%</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Columns Below 90%</span>
                                                <span class="dual-layer-technical-item-value">{len(low_completeness_cols)}</span>
                                            </div>
                                        </div>
                                        <div class="dual-layer-technical-context">
                                            <ul>
                                                <li><strong>Quality Score Formula:</strong> (0.4 × Completeness) + (0.3 × Validity) + (0.2 × Consistency) + (0.1 × Uniqueness)</li>
                                                <li>Completeness (40% weight): Percentage of non-null values</li>
                                                <li>Validity (30% weight): Values matching expected type/format</li>
                                                <li>Consistency (20% weight): Pattern uniformity across column</li>
                                                <li>Uniqueness (10% weight): Cardinality relative to column type</li>
                                            </ul>
                                        </div>
                                    </div>
                                </details>
                            </div>
                            <div class="chart-container">
                                <div class="chart-title">Quality Score by Column</div>
                                <canvas id="qualityChart" height="120"></canvas>
                            </div>
                        </div>
                    </div>
                </div>'''

    def _generate_distribution_accordion(self, profile: ProfileResult, categorical_columns: List[Dict]) -> str:
        """Generate the value distribution accordion with dual-layer explanation."""
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

        # Generate plain-English summary based on data characteristics
        num_categorical = len(categorical_columns)
        if num_categorical > 0:
            # Get column names for display
            cat_names = [c.get('name', '') for c in categorical_columns]
            high_cardinality = [c.get('name', '') for c in categorical_columns if c.get('unique_count', 0) > 50]
            low_cardinality = [c.get('name', '') for c in categorical_columns if c.get('unique_count', 0) <= 10]

            if num_categorical == 1:
                plain_english = f"Your data has 1 categorical column ({cat_names[0]})"
                if low_cardinality:
                    plain_english += " with only a few distinct values, typical of label or code fields."
                else:
                    plain_english += " with many unique values, which could indicate an identifier or free-text field."
            elif num_categorical <= 3:
                plain_english = f"Your data has {num_categorical} categorical columns ({', '.join(cat_names)})"
                if len(low_cardinality) == num_categorical:
                    plain_english += ", each with only a few distinct values. This is typical for label, status, or category fields."
                elif len(high_cardinality) == num_categorical:
                    plain_english += ", each with many unique values, which could indicate identifiers or free-text fields."
                else:
                    plain_english += ". "
                    if low_cardinality:
                        plain_english += f"{', '.join(low_cardinality[:2])} have few values (typical for codes/labels)"
                    if low_cardinality and high_cardinality:
                        plain_english += ", while "
                    if high_cardinality:
                        plain_english += f"{', '.join(high_cardinality[:2])} have many values (possibly identifiers)."
            else:
                plain_english = f"Your data has {num_categorical} categorical columns. "
                if high_cardinality:
                    plain_english += f"Some ({', '.join(high_cardinality[:2])}) have many unique values (possibly identifiers). "
                if low_cardinality:
                    plain_english += f"Most ({', '.join(low_cardinality[:3])}) have few values, typical of status or category fields."
        else:
            plain_english = "This dataset appears to be primarily numeric with limited categorical data. The bubble chart shows how each column performs across quality metrics."

        return f'''
                <div class="accordion" id="section-distributions" data-accordion="distribution">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">📊</div>
                            <div>
                                <div class="accordion-title">Distributions</div>
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
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">{plain_english}</div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content">
                                        <div class="dual-layer-technical-grid">
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Categorical Columns</span>
                                                <span class="dual-layer-technical-item-value">{num_categorical}</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Chart X-Axis</span>
                                                <span class="dual-layer-technical-item-value">{x_axis_metric_name}</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Completeness Range</span>
                                                <span class="dual-layer-technical-item-value">{completeness_range:.1f}%</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Uniqueness Range</span>
                                                <span class="dual-layer-technical-item-value">{uniqueness_range:.1f}%</span>
                                            </div>
                                        </div>
                                        <div class="dual-layer-technical-context">
                                            <ul>
                                                <li>Bubble chart: Each bubble represents a column; size = unique value count</li>
                                                <li>Word cloud: Larger words appear more frequently in your data</li>
                                                <li>Use these patterns to build ValidValuesCheck validations</li>
                                            </ul>
                                        </div>
                                    </div>
                                </details>
                            </div>
                            <div style="display: grid; gap: 20px;">
                                <div class="chart-container">
                                    <div class="chart-title">Column Quality: {x_axis_metric_name} vs Validity</div>
                                    <canvas id="bubbleChart" height="200"></canvas>
                                </div>
                                <div class="chart-container">
                                    <div class="chart-title">Categorical Values Word Cloud</div>
                                    <div style="color: var(--text-muted); font-size: 0.85em; margin-bottom: 8px;">Showing top values from: {cat_names}</div>
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

        # Generate plain-English summary for column explorer
        pii_count = sum(1 for col in profile.columns if col.pii_info and col.pii_info.get('detected'))
        temporal_count = sum(1 for col in profile.columns if col.temporal_analysis and col.temporal_analysis.get('available'))
        low_quality_count = sum(1 for col in profile.columns if col.quality.overall_score < 70)

        plain_english = f"Your dataset has {profile.column_count} columns. "
        if pii_count > 0:
            plain_english += f"{pii_count} column(s) appear to contain sensitive personal information that may need protection. "
        if temporal_count > 0:
            plain_english += f"{temporal_count} column(s) contain dates or timestamps for time-based analysis. "
        if low_quality_count > 0:
            plain_english += f"{low_quality_count} column(s) have quality concerns (below 70% score) that may need attention. "
        else:
            plain_english += "All columns have acceptable quality scores. "
        plain_english += "Click any column in the list below for detailed statistics and patterns."

        return f'''
                <!-- Column Quality Heatmap -->
                {heatmap_html}

                <div class="accordion column-explorer" data-accordion="columns" id="section-columns">
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
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">{plain_english}</div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content">
                                        <div class="dual-layer-technical-grid">
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Total Columns</span>
                                                <span class="dual-layer-technical-item-value">{profile.column_count}</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Semantically Tagged</span>
                                                <span class="dual-layer-technical-item-value">{tagged_count}</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">PII Detected</span>
                                                <span class="dual-layer-technical-item-value">{pii_count}</span>
                                            </div>
                                            <div class="dual-layer-technical-item">
                                                <span class="dual-layer-technical-item-label">Quality Concerns</span>
                                                <span class="dual-layer-technical-item-value">{low_quality_count}</span>
                                            </div>
                                        </div>
                                        <div class="dual-layer-technical-context">
                                            <ul>
                                                <li><strong>Row Count, Null Count, Min/Max:</strong> Computed from ALL rows (exact values)</li>
                                                <li><strong>Mean, Std Dev, Median, ML Analysis:</strong> Computed from analysis sample</li>
                                                <li><strong>Top Values:</strong> Adaptively tracked per column (low cardinality: all values; high cardinality: capped)</li>
                                                <li><strong>Semantic tags:</strong> MONEY, TIMESTAMP, ACCOUNT_ID, etc. based on FIBO ontology</li>
                                            </ul>
                                        </div>
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
        # Show semantic classification (Schema.org + FIBO resolved display_label)
        semantic_label = self._get_semantic_display_label(col)
        if semantic_label:
            # Determine tag style based on primary source
            primary_source = col.semantic_info.get('resolved', {}).get('primary_source', 'schema_org') if col.semantic_info else 'schema_org'
            tag_class = 'fibo' if primary_source == 'fibo' else 'semantic'
            # Add tooltip explaining the semantic source
            source_name = 'FIBO (Financial Industry Business Ontology)' if primary_source == 'fibo' else 'Schema.org'
            source_icon = '🏦' if primary_source == 'fibo' else '🌐'
            tooltip = f"{source_icon} Semantic type from {source_name}"
            tags += f'<span class="column-tag {tag_class}" title="{tooltip}">{semantic_label}</span>'
        elif col.statistics.semantic_type and col.statistics.semantic_type != 'unknown':
            # Fallback to generic semantic type if no resolved semantic
            tags += f'<span class="column-tag semantic">{col.statistics.semantic_type.upper()}</span>'

        # Stats
        stats = self._generate_column_stats(col)

        # Top values
        top_values = self._generate_top_values(col)

        # Semantic summary (plain-English explanation of field meaning)
        semantic_summary = self._generate_semantic_summary_html(col)

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

        # Build semantic badge for mobile (visible next to name)
        semantic_badge_mobile = ''
        semantic_label = self._get_semantic_display_label(col)
        if semantic_label:
            # Get icon based on semantic type
            primary_source = col.semantic_info.get('resolved', {}).get('primary_source', 'schema_org') if col.semantic_info else 'schema_org'
            primary_type = col.semantic_info.get('resolved', {}).get('primary_type', '') if col.semantic_info else ''

            # Category icons for both Schema.org and FIBO types
            category_icons = {
                # FIBO categories
                'money': '💰', 'identifier': '🔑', 'party': '👤', 'datetime': '📅',
                'location': '📍', 'account': '🏦', 'transaction': '💸', 'product': '📦',
                'banking': '🏦', 'loan': '💳', 'security': '📊', 'temporal': '⏰',
                'risk': '⚠️', 'category': '🏷️',
                # Schema.org categories
                'schema:identifier': '🔑', 'schema:name': '📛', 'schema:person': '👤',
                'schema:givenname': '👤', 'schema:familyname': '👤', 'schema:gendertype': '⚧',
                'schema:monetaryamount': '💰', 'schema:quantitativevalue': '🔢',
                'schema:number': '🔢', 'schema:integer': '🔢', 'schema:boolean': '✓',
                'schema:categorycode': '🏷️', 'schema:enumeration': '📋',
                'schema:date': '📅', 'schema:datetime': '📅', 'schema:time': '⏰',
                'schema:place': '📍', 'schema:postaladdress': '📮', 'schema:geocoordinates': '🌐',
                'schema:text': '📝', 'schema:description': '📝', 'schema:email': '📧',
                'schema:telephone': '📞', 'schema:url': '🔗', 'schema:organization': '🏢'
            }

            # Try to find icon from primary_type
            icon = '🏷️'  # Default
            if primary_type:
                type_lower = primary_type.lower()
                if type_lower in category_icons:
                    icon = category_icons[type_lower]
                else:
                    # Try category from dotted notation (e.g., "money.amount" -> "money")
                    category = type_lower.split('.')[0] if '.' in type_lower else type_lower.replace('schema:', '').split(':')[-1]
                    icon = category_icons.get(category, '🏷️')

            semantic_badge_mobile = f'<span class="fibo-badge-mobile" title="{semantic_label}">{icon} {semantic_label}</span>'

        return f'''
                                <div class="column-row" onclick="toggleColumnRow(this)" {data_attrs}>
                                    <div class="column-row-header">
                                        <span class="column-expand-icon">▶</span>
                                        <div class="column-type-icon {type_class}">{icon}</div>
                                        <div class="column-info">
                                            <div class="column-name-row">
                                                <span class="column-name">{col.name}</span>
                                                {semantic_badge_mobile}
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
                                            {semantic_summary}
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

    def _get_semantic_display_label(self, col: ColumnProfile) -> str:
        """
        Get the combined semantic display label from resolved semantic info.

        Returns the resolved display_label which combines Schema.org and FIBO
        semantics, e.g., "Monetary amount (FIBO:MoneyAmount)" or "Identifier (schema:identifier)".

        Args:
            col: ColumnProfile with semantic_info

        Returns:
            Display label string or empty string if no semantic classification
        """
        if not col.semantic_info:
            return ''

        # Try to get display_label from resolved semantic info (new dual-layer structure)
        resolved = col.semantic_info.get('resolved', {})
        if resolved and resolved.get('display_label'):
            return resolved['display_label']

        # Fallback: check schema_org layer
        schema_org = col.semantic_info.get('schema_org', {})
        if schema_org and schema_org.get('display_label'):
            return schema_org['display_label']

        # Fallback: check fibo layer directly (backward compatibility)
        fibo = col.semantic_info.get('fibo', {})
        if fibo and fibo.get('type'):
            fibo_type = fibo['type']
            # Extract class name from FIBO type
            if ':' in fibo_type:
                return fibo_type.split(':')[-1]
            return fibo_type

        # Final fallback: direct type field (legacy structure)
        if col.semantic_info.get('type'):
            sem_type = col.semantic_info['type']
            if ':' in sem_type:
                return sem_type.split(':')[-1]
            return sem_type

        return ''

    def _get_semantic_description(self, col: ColumnProfile) -> str:
        """
        Generate a plain-English description of what the column represents
        based on its Schema.org/FIBO semantic classification.

        Returns a short, neutral sentence explaining the field's meaning.
        """
        if not col.semantic_info:
            return ''

        # Get primary type from resolved semantics
        resolved = col.semantic_info.get('resolved', {})
        primary_type_raw = resolved.get('primary_type') if resolved else None
        primary_type = primary_type_raw.lower() if primary_type_raw else ''

        # If no resolved type, try schema_org layer
        if not primary_type:
            schema_org = col.semantic_info.get('schema_org', {})
            schema_type_raw = schema_org.get('type') if schema_org else None
            primary_type = schema_type_raw.lower() if schema_type_raw else ''

        if not primary_type:
            return ''

        # Get confidence for determining if we should show description
        schema_org = col.semantic_info.get('schema_org', {})
        confidence = schema_org.get('confidence', 0) if schema_org else 0

        # Plain-English descriptions for Schema.org types
        descriptions = {
            # Code-like types (new enhanced detection)
            'schema:identifier': "This field contains identifier-style codes that uniquely label records or entities.",
            'schema:propertyvalue': "This field contains structured property codes such as seat, cabin, room, or other location-like identifiers.",
            'schema:categorycode': "This field uses a small set of short codes to represent categories, locations, or statuses.",

            # Person types
            'schema:person': "This field appears to contain person-related information.",
            'schema:name': "This field contains names, likely referring to people, places, or entities.",
            'schema:givenname': "This field contains first or given names.",
            'schema:familyname': "This field contains surnames or family names.",
            'schema:gendertype': "This field indicates gender classification.",
            'schema:birthdate': "This field contains birth date information.",

            # Numeric types
            'schema:monetaryamount': "This field represents a monetary value such as price, amount, or currency.",
            'schema:quantitativevalue': "This field contains a quantity or measurement.",
            'schema:number': "This field contains numeric values.",
            'schema:integer': "This field contains whole number values.",

            # Categorical types
            'schema:boolean': "This field contains boolean (yes/no, true/false) values.",
            'schema:enumeration': "This field uses a fixed set of predefined values.",

            # Temporal types
            'schema:date': "This field contains date information.",
            'schema:datetime': "This field contains date and time information.",
            'schema:time': "This field contains time-of-day information.",
            'schema:duration': "This field represents a time duration or interval.",

            # Location types
            'schema:place': "This field references a location or place.",
            'schema:postaladdress': "This field contains address information.",
            'schema:addresslocality': "This field contains city or locality information.",
            'schema:addressregion': "This field contains state, province, or region information.",
            'schema:addresscountry': "This field contains country information.",
            'schema:postalcode': "This field contains postal or zip code information.",
            'schema:geocoordinates': "This field contains geographic coordinates.",

            # Contact types
            'schema:email': "This field contains email addresses.",
            'schema:telephone': "This field contains phone numbers.",
            'schema:url': "This field contains web URLs or links.",

            # Text types
            'schema:text': "This field contains free-form text.",
            'schema:description': "This field contains descriptive text or notes.",

            # Organization types
            'schema:organization': "This field references an organization, company, or business entity.",
            'schema:event': "This field references an event or occurrence.",
        }

        description = descriptions.get(primary_type, '')

        # For low-confidence matches, add a qualifier
        if description and confidence < 0.6:
            description = description.replace(
                "This field",
                "This field may"
            ).replace(
                "contains",
                "contain"
            ).replace(
                "represents",
                "represent"
            ).replace(
                "uses",
                "use"
            )
            description += " (interpretation is less certain)"

        return description

    def _generate_semantic_summary_html(self, col: ColumnProfile) -> str:
        """
        Generate HTML snippet for semantic summary to display in column details.

        Shows the semantic label and a plain-English explanation.
        """
        semantic_label = self._get_semantic_display_label(col)
        description = self._get_semantic_description(col)

        if not semantic_label and not description:
            return ''

        # Get primary source for styling
        primary_source = 'schema_org'
        if col.semantic_info:
            primary_source = col.semantic_info.get('resolved', {}).get('primary_source', 'schema_org')

        # Style based on source
        if primary_source == 'fibo':
            badge_style = 'background: rgba(102, 126, 234, 0.15); color: #818cf8; border: 1px solid rgba(102, 126, 234, 0.3);'
            icon = '🏦'
            source_label = 'FIBO'
        else:
            badge_style = 'background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3);'
            icon = '🏷️'
            source_label = 'Schema.org'

        label_html = f'''
            <span style="display: inline-flex; align-items: center; gap: 4px; padding: 3px 10px; border-radius: 12px; font-size: 0.85em; {badge_style}">
                {icon} {semantic_label}
            </span>
        ''' if semantic_label else ''

        description_html = f'''
            <div style="color: var(--text-secondary); font-size: 0.85em; margin-top: 6px; line-height: 1.4;">
                {description}
            </div>
        ''' if description else ''

        if not label_html and not description_html:
            return ''

        return f'''
            <div class="semantic-summary" style="padding: 10px 12px; background: var(--card-darker); border-radius: 8px; margin-bottom: 12px; border-left: 3px solid var(--primary);">
                <div style="font-size: 0.75em; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px;">
                    Field Meaning ({source_label})
                </div>
                {label_html}
                {description_html}
            </div>
        '''

    def _get_semantic_analytic_confidence(self, col_name: str, analytic_type: str, columns: list = None) -> tuple:
        """
        Determine semantic confidence for an analytic on a specific column.

        Returns a tuple: (is_high_confidence: bool, explanation: str)

        Args:
            col_name: Name of the column being analyzed
            analytic_type: Type of analytic ('outlier', 'benford', 'correlation', 'autoencoder')
            columns: List of ColumnProfile objects to look up semantic info

        For Benford and outlier analytics, HIGH confidence ONLY when:
            - Semantic type is clearly numeric/monetary AND non-identifier
            - Semantic resolution is not "unknown" or "generic text"

        HIGH confidence types (for benford/outlier):
            - schema:Number, schema:QuantitativeValue, schema:MonetaryAmount
            - schema:PriceSpecification, fibo:MoneyAmount, fibo:TransactionAmount

        CAUTION types (for benford/outlier):
            - schema:identifier, fibo:AccountIdentifier, codes
            - schema:Text, schema:name, schema:Person
            - schema:Date, schema:DateTime
            - Unknown/unresolved semantic types
        """
        if not columns:
            # No semantic info available
            # For benford/outlier: default to CAUTION (unknown semantics)
            # For other analytics: neutral
            if analytic_type in ('benford', 'outlier'):
                return (False, "This analysis was computed, but the field's semantic type is unclear. Interpret this numeric analysis with extra caution.")
            return (True, "")

        # Find the column
        col = None
        for c in columns:
            if c.name == col_name:
                col = c
                break

        if not col:
            if analytic_type in ('benford', 'outlier'):
                return (False, "This analysis was computed, but the field's semantic type is unclear. Interpret this numeric analysis with extra caution.")
            return (True, "")

        if not col.semantic_info:
            if analytic_type in ('benford', 'outlier'):
                return (False, "This analysis was computed, but the field's semantic type is unclear. Interpret this numeric analysis with extra caution.")
            return (True, "")

        # Get primary type from resolved semantics
        resolved = col.semantic_info.get('resolved', {})
        primary_type = resolved.get('primary_type', '').lower() if resolved else ''

        # If no resolved type, try schema_org or fibo directly
        if not primary_type:
            schema_org = col.semantic_info.get('schema_org', {})
            primary_type = schema_org.get('type', '').lower() if schema_org else ''
        if not primary_type:
            fibo = col.semantic_info.get('fibo', {})
            primary_type = fibo.get('type', '').lower() if fibo else ''

        # Define high-confidence types for numeric analytics (outliers, Benford)
        # These are truly numeric/monetary measures well-suited for distribution analysis
        high_conf_numeric = {
            'schema:number', 'schema:integer', 'schema:quantitativevalue',
            'schema:monetaryamount', 'schema:pricespecification',
            'fibo:moneyamount', 'fibo:transactionamount', 'fibo:amount'
        }

        # Caution types - identifiers, text, names, codes
        caution_types = {
            'schema:identifier', 'schema:text', 'schema:name', 'schema:person',
            'schema:givenname', 'schema:familyname', 'schema:description',
            'fibo:accountidentifier', 'fibo:identifier'
        }

        # Date/time types - caution for Benford and correlation
        datetime_types = {
            'schema:date', 'schema:datetime', 'schema:time', 'schema:duration',
            'schema:birthdate'
        }

        # Boolean - caution for Benford, high for associations
        boolean_types = {'schema:boolean'}

        # Categorical - high for associations, caution for numeric analytics
        categorical_types = {'schema:categorycode', 'schema:enumeration', 'schema:gendertype'}

        # Determine confidence based on analytic type
        if analytic_type in ('outlier', 'benford'):
            # Numeric analytics - stricter requirements
            if primary_type in high_conf_numeric:
                return (True, "This analysis is well-suited to this field based on its numeric or monetary semantics.")
            elif primary_type in caution_types:
                return (False, "This analysis was computed, but this field behaves like an identifier, code, or free text. Interpret the result with caution.")
            elif primary_type in datetime_types:
                return (False, "This analysis was computed, but date/time fields do not typically follow numeric distribution patterns. Interpret with caution.")
            elif primary_type in boolean_types:
                return (False, "This analysis was computed, but boolean fields have limited value ranges. Interpret with caution.")
            elif primary_type in categorical_types:
                return (False, "This analysis was computed, but categorical fields may not be ideal for numeric distribution analysis. Interpret with caution.")
            else:
                # Unknown semantic type - default to CAUTION for benford/outlier
                return (False, "This analysis was computed, but the field's semantic type is unclear. Interpret this numeric analysis with extra caution.")

        elif analytic_type == 'correlation':
            # Correlation analytics
            if primary_type in high_conf_numeric:
                return (True, "This analysis is well-suited to this field because its semantic type indicates it is a numeric or monetary measure.")
            elif primary_type in boolean_types or primary_type in categorical_types:
                return (True, "This analysis is appropriate for categorical/boolean fields using association measures.")
            elif primary_type in caution_types:
                return (False, "This analysis was computed, but identifier and text fields may show spurious correlations. Interpret with caution.")
            elif primary_type in datetime_types:
                return (False, "This analysis was computed, but temporal correlations may reflect time ordering rather than true relationships. Interpret with caution.")
            else:
                # For correlation, unknown types get neutral/cautious text
                return (True, "")

        elif analytic_type == 'autoencoder':
            # Multivariate anomalies - generally applicable
            return (True, "Multi-column pattern analysis considers field interactions holistically.")

        # Default - neutral for unknown analytics
        return (True, "")

    def _generate_semantic_confidence_badge(self, is_high_conf: bool, explanation: str, badge_label: str = None) -> str:
        """Generate HTML badge for semantic confidence.

        Args:
            is_high_conf: Whether this is high confidence (True) or caution (False)
            explanation: Text explanation to display
            badge_label: Optional custom label (default: "Good fit" for high, "Interpret with caution" for low)
        """
        if not explanation:
            return ''

        if is_high_conf:
            label = badge_label or "Good fit"
            return f'''
                <div style="display: flex; align-items: center; gap: 6px; margin-top: 8px; padding: 6px 10px; background: rgba(34, 197, 94, 0.1); border-radius: 6px; font-size: 0.8em;">
                    <span style="background: #22c55e; color: white; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.85em;">{label}</span>
                    <span style="color: var(--text-secondary);">{explanation}</span>
                </div>'''
        else:
            label = badge_label or "Interpret with caution"
            return f'''
                <div style="display: flex; align-items: center; gap: 6px; margin-top: 8px; padding: 6px 10px; background: rgba(245, 158, 11, 0.1); border-radius: 6px; font-size: 0.8em;">
                    <span style="background: #f59e0b; color: white; padding: 2px 8px; border-radius: 4px; font-weight: 600; font-size: 0.85em;">{label}</span>
                    <span style="color: var(--text-secondary);">{explanation}</span>
                </div>'''

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

        # Determine appropriate sample note based on cardinality and sampling method
        stats = col.statistics
        unique_count = stats.unique_count or 0
        sample_size = stats.sample_size or 0
        total_rows = stats.count or 0

        # Low-cardinality categorical: all distinct values are tracked
        if unique_count <= 50 and sample_size >= unique_count:
            sample_note = f"Values tracked: {unique_count:,} distinct categories"
            title_text = "Value Distribution"
        # High-cardinality but showing top N from full scan
        elif sample_size == total_rows or sample_size == 0:
            if unique_count > len(top_values):
                sample_note = f"Top {len(top_values)} of {unique_count:,} distinct values; full column scan used"
            else:
                sample_note = f"All {unique_count:,} distinct values tracked"
            title_text = "Top Values"
        # True row-based sampling (only use this label when actually subsampling rows)
        elif sample_size < total_rows:
            sample_note = f"Based on {sample_size:,} row sample of {total_rows:,} total rows"
            title_text = "Top Values (sampled)"
        else:
            sample_note = "From full dataset"
            title_text = "Top Values"

        return f'''
                                            <div class="top-values-section">
                                                <div class="top-values-title">{title_text}</div>
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
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">
                                        Based on what we learned about your data, here are recommended validations to help catch issues early. Each suggestion includes copy-ready configuration you can add to your validation file.
                                    </div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content" style="padding: 12px;">
                                        <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                            <li>Suggestions are derived from column statistics, patterns, and anomalies</li>
                                            <li>Higher confidence scores indicate stronger signals from the data</li>
                                            <li>Each YAML snippet can be copied directly into your validation config</li>
                                        </ul>
                                    </div>
                                </details>
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
                <div class="accordion" data-accordion="config" id="section-yaml">
                    <div class="accordion-header" onclick="toggleAccordion(this)">
                        <div class="accordion-title-group">
                            <div class="accordion-icon quality">⚙️</div>
                            <div>
                                <div class="accordion-title">YAML / Export</div>
                                <div class="accordion-subtitle">Ready-to-use validation configuration</div>
                            </div>
                        </div>
                        <div class="accordion-meta">
                            <span class="accordion-badge good">Ready</span>
                            <span class="accordion-chevron">▼</span>
                        </div>
                    </div>
                    <div class="accordion-body">
                        <div class="accordion-content">
                            <div class="dual-layer-explanation">
                                <div class="dual-layer-summary">
                                    <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                                    <div class="dual-layer-summary-text">
                                        This is your complete validation configuration file, ready to use. Copy it, save as a .yaml file, and run validations with the DataK9 CLI command shown below.
                                    </div>
                                </div>
                                <details class="dual-layer-technical">
                                    <summary>🧠 Technical Details (click to expand)</summary>
                                    <div class="dual-layer-technical-content" style="padding: 12px;">
                                        <ul style="margin: 0; padding-left: 20px; color: var(--text-secondary); font-size: 0.85em;">
                                            <li>Run command: <code>python3 -m validation_framework.cli validate config.yaml</code></li>
                                            <li>Configuration includes all suggested validations from the profile</li>
                                            <li>Adjust thresholds and parameters based on your specific requirements</li>
                                        </ul>
                                    </div>
                                </details>
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
        analyzed_rows = 0
        if hasattr(self, 'profile') and hasattr(self.profile, 'ml_findings') and self.profile.ml_findings:
            sample_info = self.profile.ml_findings.get('sample_info', {})
            if sample_info.get('sampled', False):
                analyzed_rows = sample_info.get('analyzed_rows', 0)
                sample_note = f'<div style="color: var(--text-tertiary); font-size: 0.8em; margin-top: 8px; font-style: italic;">ℹ️ Analysis based on {analyzed_rows:,} sampled rows</div>'

        # Build plain-English summary
        col_names = [col.name for col in temporal_columns]
        col_names_str = ', '.join(col_names[:3]) + ('...' if len(col_names) > 3 else '')

        # Analyze overall temporal characteristics
        total_gaps = sum(
            (col.temporal_analysis or {}).get('gaps', {}).get('gap_count', 0)
            for col in temporal_columns
        )
        trends = [
            (col.temporal_analysis or {}).get('trend', {}).get('direction', 'unknown')
            for col in temporal_columns
        ]
        increasing_count = trends.count('increasing')
        decreasing_count = trends.count('decreasing')

        plain_english = f"Your data has {len(temporal_columns)} date/time column(s) ({col_names_str}). "
        if total_gaps > 0:
            plain_english += f"There are gaps in the time series - periods where data appears to be missing. This could indicate collection issues, system downtime, or simply periods of no activity. "
        else:
            plain_english += "The time coverage looks continuous with no significant gaps detected. "

        if increasing_count > 0:
            plain_english += f"Activity is trending upward over time in {increasing_count} column(s). "
        elif decreasing_count > 0:
            plain_english += f"Activity is trending downward over time in {decreasing_count} column(s). "

        return f'''
        <div class="accordion" id="section-temporal" data-accordion="temporal">
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
                    <div class="dual-layer-explanation">
                        <div class="dual-layer-summary">
                            <div class="dual-layer-summary-label">📘 Plain-English Summary</div>
                            <div class="dual-layer-summary-text">{plain_english}</div>
                        </div>
                        <details class="dual-layer-technical">
                            <summary>🧠 Technical Details (click to expand)</summary>
                            <div class="dual-layer-technical-content">
                                <div class="dual-layer-technical-grid">
                                    <div class="dual-layer-technical-item">
                                        <span class="dual-layer-technical-item-label">Temporal Columns</span>
                                        <span class="dual-layer-technical-item-value">{len(temporal_columns)}</span>
                                    </div>
                                    <div class="dual-layer-technical-item">
                                        <span class="dual-layer-technical-item-label">Total Gaps Detected</span>
                                        <span class="dual-layer-technical-item-value">{total_gaps:,}</span>
                                    </div>
                                    <div class="dual-layer-technical-item">
                                        <span class="dual-layer-technical-item-label">Increasing Trends</span>
                                        <span class="dual-layer-technical-item-value">{increasing_count}</span>
                                    </div>
                                    <div class="dual-layer-technical-item">
                                        <span class="dual-layer-technical-item-label">Decreasing Trends</span>
                                        <span class="dual-layer-technical-item-value">{decreasing_count}</span>
                                    </div>
                                </div>
                                <div class="dual-layer-technical-context">
                                    <ul>
                                        <li>Gap detection: Identifies periods where expected data is missing</li>
                                        <li>Frequency inference: Detects daily, weekly, monthly patterns</li>
                                        <li>Trend analysis: Determines if activity increases or decreases over time</li>
                                        <li>Date range: Shows the full span of your temporal data</li>
                                    </ul>
                                </div>
                            </div>
                        </details>
                    </div>
                    {sample_note}
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
        <section class="sampling-bar" id="section-engine" style="flex-direction: column; align-items: stretch;">
            <div style="display: flex; flex-wrap: wrap; gap: 24px; align-items: center;">
                <div class="sampling-bar-title">🔬 Profiling Engine & Sampling</div>
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
        Generate unified sampling coverage header - compact with expandable details.

        Consolidates all sampling information into a single, clear header that explains:
        - What was sampled vs full scan
        - Why sampling is used (or why it isn't)
        - Statistical validity of sampling approach
        """
        # Get sampling info from insights or ml_findings
        insight_sampling = insights.get('sampling_info', {})

        if not insight_sampling and profile.ml_findings:
            ml_sample_info = profile.ml_findings.get('sample_info', {})
            sample_used = ml_sample_info.get('sampled', ml_sample_info.get('sample_percentage', 100) < 100)
            sample_size = ml_sample_info.get('analyzed_rows', 0)
            total_rows = ml_sample_info.get('original_rows', profile.row_count)
        else:
            sample_used = insight_sampling.get('sample_used', False)
            sample_size = insight_sampling.get('sample_size', 0)
            total_rows = insight_sampling.get('total_rows', profile.row_count)

        if total_rows == 0:
            total_rows = profile.row_count

        # Determine status and messaging
        if sample_used and total_rows > 0:
            coverage_pct = min((sample_size / total_rows) * 100, 100)
            status_icon = "⚡"
            status_text = "Optimized Analysis"
            status_color = "#f59e0b"  # amber

            if coverage_pct < 1:
                coverage_display = f"{sample_size:,} of {total_rows:,} rows ({coverage_pct:.2f}%)"
            else:
                coverage_display = f"{sample_size:,} of {total_rows:,} rows ({coverage_pct:.1f}%)"

            headline = f"Smart sampling enabled — {sample_size:,} rows analyzed"
            subtext = "Full accuracy with faster performance"
        else:
            coverage_pct = 100
            status_icon = "✓"
            status_text = "Complete Analysis"
            status_color = "#10b981"  # green
            coverage_display = f"{total_rows:,} rows (100%)"
            headline = f"Full dataset analyzed — {total_rows:,} rows"
            subtext = "No sampling required for this dataset size"

        # Build the analysis breakdown
        full_scan_analyses = ["Row count", "Column types", "Null detection", "Basic statistics"]
        if sample_used:
            sampled_analyses = ["Outlier detection", "Pattern analysis", "ML anomalies", "Correlations"]
        else:
            sampled_analyses = []

        return f'''
        <div class="sampling-header" style="background: linear-gradient(135deg, rgba(30, 41, 59, 0.95) 0%, rgba(15, 23, 42, 0.98) 100%); border-radius: 12px; padding: 16px 20px; margin-bottom: 20px; border: 1px solid rgba(148, 163, 184, 0.1);">
            <!-- Compact Header Row -->
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px;">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <div style="background: {status_color}; color: white; width: 36px; height: 36px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1.2em;">{status_icon}</div>
                    <div>
                        <div style="font-weight: 600; color: #f1f5f9; font-size: 0.95em;">{headline}</div>
                        <div style="font-size: 0.8em; color: #94a3b8;">{subtext}</div>
                    </div>
                </div>
                <div style="display: flex; gap: 16px; align-items: center;">
                    <div style="text-align: right;">
                        <div style="font-size: 0.75em; color: #64748b; text-transform: uppercase;">Coverage</div>
                        <div style="font-weight: 600; color: {status_color}; font-size: 0.9em;">{coverage_display}</div>
                    </div>
                </div>
            </div>

            <!-- Expandable Details -->
            <details style="margin-top: 12px;">
                <summary style="cursor: pointer; color: #94a3b8; font-size: 0.85em; padding: 8px 0; list-style: none; display: flex; align-items: center; gap: 6px;">
                    <span style="transition: transform 0.2s;">▶</span>
                    <span>How was this data analyzed?</span>
                </summary>
                <div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(148, 163, 184, 0.1);">
                    <!-- Analysis Method Grid -->
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 16px;">
                        <!-- Full Scan Section -->
                        <div style="background: rgba(16, 185, 129, 0.1); border-radius: 8px; padding: 12px; border-left: 3px solid #10b981;">
                            <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 8px;">
                                <span style="color: #10b981; font-size: 1.1em;">✓</span>
                                <span style="font-weight: 600; color: #f1f5f9; font-size: 0.85em;">Full Dataset Scan</span>
                            </div>
                            <div style="font-size: 0.8em; color: #94a3b8; line-height: 1.5;">
                                {', '.join(full_scan_analyses)}
                            </div>
                            <div style="font-size: 0.75em; color: #64748b; margin-top: 6px;">
                                Always computed on 100% of rows
                            </div>
                        </div>

                        {f'''<!-- Sampled Analysis Section -->
                        <div style="background: rgba(245, 158, 11, 0.1); border-radius: 8px; padding: 12px; border-left: 3px solid #f59e0b;">
                            <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 8px;">
                                <span style="color: #f59e0b; font-size: 1.1em;">⚡</span>
                                <span style="font-weight: 600; color: #f1f5f9; font-size: 0.85em;">Sampled Analysis</span>
                            </div>
                            <div style="font-size: 0.8em; color: #94a3b8; line-height: 1.5;">
                                {', '.join(sampled_analyses)}
                            </div>
                            <div style="font-size: 0.75em; color: #64748b; margin-top: 6px;">
                                Computed on {sample_size:,} representative rows
                            </div>
                        </div>''' if sample_used else f'''<!-- Full Analysis Section -->
                        <div style="background: rgba(16, 185, 129, 0.1); border-radius: 8px; padding: 12px; border-left: 3px solid #10b981;">
                            <div style="display: flex; align-items: center; gap: 6px; margin-bottom: 8px;">
                                <span style="color: #10b981; font-size: 1.1em;">✓</span>
                                <span style="font-weight: 600; color: #f1f5f9; font-size: 0.85em;">Advanced Analysis</span>
                            </div>
                            <div style="font-size: 0.8em; color: #94a3b8; line-height: 1.5;">
                                Outliers, patterns, ML anomalies, correlations
                            </div>
                            <div style="font-size: 0.75em; color: #64748b; margin-top: 6px;">
                                Dataset small enough for complete analysis
                            </div>
                        </div>'''}
                    </div>

                    {self._generate_sampling_explanation(sample_used, sample_size, total_rows)}
                </div>
            </details>
        </div>'''

    def _generate_sampling_explanation(self, sample_used: bool, sample_size: int, total_rows: int) -> str:
        """Generate the statistical explanation section based on whether sampling was used."""
        if not sample_used:
            return f'''
                    <!-- Why No Sampling -->
                    <div style="background: rgba(16, 185, 129, 0.05); border-radius: 8px; padding: 12px; border: 1px solid rgba(16, 185, 129, 0.2);">
                        <div style="font-weight: 600; color: #f1f5f9; font-size: 0.85em; margin-bottom: 8px;">
                            💡 Why wasn't sampling used?
                        </div>
                        <p style="font-size: 0.8em; color: #94a3b8; margin: 0; line-height: 1.6;">
                            Your dataset has <strong>{total_rows:,} rows</strong>, which is small enough to analyze completely
                            without performance issues. Sampling is only applied to datasets exceeding 50,000 rows,
                            where it provides statistically equivalent results with significantly faster processing.
                        </p>
                    </div>'''

        # Calculate statistical properties for sampled data
        margin_of_error = 100 * (1.96 * 0.5) / (sample_size ** 0.5)

        return f'''
                    <!-- Why Sampling Is Valid -->
                    <div style="background: rgba(245, 158, 11, 0.05); border-radius: 8px; padding: 12px; border: 1px solid rgba(245, 158, 11, 0.2);">
                        <div style="font-weight: 600; color: #f1f5f9; font-size: 0.85em; margin-bottom: 8px;">
                            📊 Why is sampling statistically valid?
                        </div>
                        <p style="font-size: 0.8em; color: #94a3b8; margin: 0 0 12px 0; line-height: 1.6;">
                            Statistical theory shows that <strong>sample size, not population size</strong>, determines accuracy.
                            A properly randomized sample of {sample_size:,} rows provides reliable insights about your full {total_rows:,} row dataset.
                        </p>

                        <div style="display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 12px;">
                            <div style="background: rgba(255,255,255,0.05); padding: 8px 12px; border-radius: 6px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: 700; color: #f59e0b;">±{margin_of_error:.1f}%</div>
                                <div style="font-size: 0.7em; color: #64748b;">Margin of Error</div>
                            </div>
                            <div style="background: rgba(255,255,255,0.05); padding: 8px 12px; border-radius: 6px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: 700; color: #10b981;">95%</div>
                                <div style="font-size: 0.7em; color: #64748b;">Confidence Level</div>
                            </div>
                            <div style="background: rgba(255,255,255,0.05); padding: 8px 12px; border-radius: 6px; text-align: center;">
                                <div style="font-size: 1.1em; font-weight: 700; color: #6366f1;">&lt;0.1%</div>
                                <div style="font-size: 0.7em; color: #64748b;">Detectable Events</div>
                            </div>
                        </div>

                        <p style="font-size: 0.75em; color: #64748b; margin: 0; font-style: italic;">
                            The Central Limit Theorem guarantees that sample statistics converge to true population values,
                            regardless of whether your dataset has 100K or 100M rows.
                        </p>
                    </div>'''

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
