"""
Insight templates and narratives for data profiling reports.

This module contains all text templates, narratives, and language generation
for the insight engine. Separated from logic for easier maintenance.

Templates use Python string formatting with named placeholders.
Each template category maps issue IDs to their narrative text.
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass


# =============================================================================
# SAMPLING EXPLANATIONS
# =============================================================================

# These templates use {sample_size} placeholder which should be filled in with the actual sample size
SAMPLING_EXPLANATION_TEMPLATE = """A configurable analysis sample (up to {sample_size:,} rows) is used for statistical and machine-learning-based checks.
This sample size provides strong statistical accuracy (typically within ±0.5–1% for most proportions) while keeping processing fast.
If the dataset contains fewer than {sample_size:,} rows, the full dataset is analysed with no sampling."""

# Default for backwards compatibility (will be replaced by dynamic generation)
SAMPLING_EXPLANATION = SAMPLING_EXPLANATION_TEMPLATE.format(sample_size=100000)

SAMPLING_OVERVIEW_FULL = """This report analyses the **full dataset** ({total_rows:,} rows).
All metrics, statistical analyses, and ML-based detections are computed from complete data."""

SAMPLING_OVERVIEW_SAMPLED_TEMPLATE = """This report uses a **{sample_size:,}-row sample** (~{sample_fraction:.1%} of the dataset) for:
- Validity checks and pattern detection
- Distribution and statistical analysis
- ML-based anomaly detection (Benford, autoencoder, outliers)
- Cross-column ratio checks

**Row counts, null counts, and basic metadata are computed from the full dataset.**

This sample size provides typical error rates under ±1% for most proportions."""

# Default for backwards compatibility
SAMPLING_OVERVIEW_SAMPLED = SAMPLING_OVERVIEW_SAMPLED_TEMPLATE


def get_sampling_explanation(sample_size: int) -> str:
    """Generate sampling explanation with actual sample size."""
    return SAMPLING_EXPLANATION_TEMPLATE.format(sample_size=sample_size)


def get_sampling_overview_sampled(sample_size: int, sample_fraction: float) -> str:
    """Generate sampled overview with actual sample size and fraction."""
    return SAMPLING_OVERVIEW_SAMPLED_TEMPLATE.format(
        sample_size=sample_size,
        sample_fraction=sample_fraction
    )


# =============================================================================
# SAMPLING CLAUSES - Appended to insights based on sampling context
# =============================================================================

def get_sampling_clause(
    basis: str,
    sample_size: Optional[int] = None,
    sample_fraction: Optional[float] = None
) -> str:
    """
    Generate appropriate sampling disclosure clause.

    Args:
        basis: 'full' or 'sample'
        sample_size: Number of rows sampled (if sampled)
        sample_fraction: Fraction of dataset sampled (if sampled)

    Returns:
        Sampling clause string (empty if full dataset)
    """
    if basis == "full":
        return ""
    elif sample_size and sample_fraction:
        return f" (based on a {sample_size:,}-row sample, ~{sample_fraction:.1%} of data)"
    elif sample_size:
        return f" (based on a {sample_size:,}-row sample)"
    else:
        return " (based on sampled data)"


def get_certainty_prefix(
    basis: str,
    sample_size: Optional[int] = None
) -> str:
    """
    Generate certainty prefix for weak samples (rarely used with 50k rule).

    Args:
        basis: 'full' or 'sample'
        sample_size: Number of rows sampled

    Returns:
        Certainty prefix string (empty for strong samples)
    """
    # With 50k sampling, most samples are strong
    # Only add uncertainty prefix for very small samples
    if basis == "sample" and sample_size and sample_size < 1000:
        return "Preliminary analysis suggests that "
    return ""


# =============================================================================
# ISSUE TEMPLATES - Organized by category
# =============================================================================

ISSUE_TEMPLATES: Dict[str, Dict[str, str]] = {
    # -------------------------------------------------------------------------
    # OVERALL QUALITY
    # -------------------------------------------------------------------------
    "overall_quality": {
        "overall_quality_excellent": (
            "Overall data quality is **{score:.1%}**, classified as **Excellent**. "
            "This dataset demonstrates strong completeness, validity, and consistency. "
            "It is well-suited for analytical and operational use."
        ),
        "overall_quality_good": (
            "Overall data quality is **{score:.1%}**, classified as **Good**. "
            "The data is generally reliable with minor issues that may warrant attention "
            "for high-stakes applications."
        ),
        "overall_quality_fair": (
            "Overall data quality is **{score:.1%}**, classified as **Fair**. "
            "Several issues require attention before high-stakes use. "
            "Review the detailed findings below for specific remediation steps."
        ),
        "overall_quality_poor": (
            "Overall data quality is **{score:.1%}**, classified as **Poor**. "
            "Significant quality issues detected that may affect data reliability. "
            "Immediate remediation is recommended before analytical use."
        ),
        "overall_quality_critical": (
            "Overall data quality is **{score:.1%}**, classified as **Critical**. "
            "Major data quality problems detected. This dataset requires substantial "
            "cleaning and validation before any analytical or operational use."
        ),
    },

    # -------------------------------------------------------------------------
    # COMPLETENESS
    # -------------------------------------------------------------------------
    "completeness": {
        "completeness_excellent": (
            "Data completeness is **{score:.1%}** across all columns. "
            "Minimal missing values detected."
        ),
        "completeness_good": (
            "Data completeness is **{score:.1%}**. "
            "Some columns have missing values but overall coverage is good."
        ),
        "completeness_low": (
            "Data completeness is **{score:.1%}**. "
            "**{null_columns}** column(s) have significant missing values. "
            "This may affect analysis reliability."
        ),
        "completeness_critical": (
            "Data completeness is critically low at **{score:.1%}**. "
            "**{null_columns}** column(s) have substantial missing values, "
            "which will significantly impact data usability."
        ),
        "column_missing_values": (
            "Column **{column}** has **{null_pct:.1%}** missing values ({null_count:,} nulls). "
            "{interpretation}"
        ),
    },

    # -------------------------------------------------------------------------
    # PII / PRIVACY
    # -------------------------------------------------------------------------
    "pii": {
        "pii_detected": (
            "Potential PII found in **{count}** column(s): **{columns}**. "
            "These fields must be masked or anonymized before wider use or sharing."
        ),
        "pii_high_risk": (
            "**High privacy risk detected.** The column **{column}** appears to contain "
            "{pii_type} data (risk score: {risk_score}/100). "
            "Immediate action required to protect sensitive information."
        ),
        "pii_medium_risk": (
            "**Medium privacy risk** in column **{column}**: potential {pii_type} detected "
            "(risk score: {risk_score}/100). Review and consider masking."
        ),
        "pii_low_risk": (
            "Low privacy risk in column **{column}**: possible {pii_type} patterns detected "
            "(risk score: {risk_score}/100). Verify and assess sensitivity."
        ),
        "no_pii": (
            "No PII (personally identifiable information) detected in this dataset."
        ),
    },

    # -------------------------------------------------------------------------
    # OUTLIERS
    # -------------------------------------------------------------------------
    "outliers": {
        "outliers_critical": (
            "Column **{column}** contains **{rate:.2%}** extreme outliers (≈{count:,} values). "
            "These outliers significantly exceed expected ranges and may indicate data errors "
            "or require special handling in analysis."
        ),
        "outliers_high": (
            "Column **{column}** has **{rate:.2%}** outliers (≈{count:,} values). "
            "These values deviate substantially from the distribution and may distort "
            "aggregates or ML models."
        ),
        "outliers_medium": (
            "Column **{column}** shows **{rate:.2%}** outliers (≈{count:,} values). "
            "While within acceptable ranges, consider their impact on statistical analyses."
        ),
        "outliers_summary": (
            "Outlier analysis found **{total_outliers:,}** extreme values across "
            "**{affected_columns}** numeric columns. "
            "{severity_note}"
        ),
        "no_outliers": (
            "No significant outliers detected in numeric columns."
        ),
    },

    # -------------------------------------------------------------------------
    # AUTHENTICITY / BENFORD'S LAW
    # -------------------------------------------------------------------------
    "authenticity": {
        "benford_failed": (
            "Column **{column}** deviates significantly from Benford's Law (χ²={chi2:.1f}, "
            "p-value={p_value:.4f}). This suggests the data may be synthetic, manipulated, "
            "or generated through an unusual process."
        ),
        "benford_warning": (
            "Column **{column}** shows moderate deviation from Benford's Law (χ²={chi2:.1f}). "
            "The first-digit distribution differs from natural occurrence patterns, "
            "which may warrant investigation."
        ),
        "benford_passed": (
            "Column **{column}** follows Benford's Law (χ²={chi2:.1f}), consistent with "
            "naturally-occurring numeric data."
        ),
        "benford_summary": (
            "Benford's Law analysis: **{failed_count}** column(s) show significant deviations, "
            "suggesting potential data authenticity concerns."
        ),
        "benford_not_applicable": (
            "Benford's Law analysis not applicable: requires numeric columns spanning "
            "multiple orders of magnitude."
        ),
    },

    # -------------------------------------------------------------------------
    # LABEL QUALITY / CLASS IMBALANCE
    # -------------------------------------------------------------------------
    "label_quality": {
        "class_imbalance_critical": (
            "The target column **{column}** is extremely imbalanced; the minority class "
            "'{minority_class}' appears in only **{minority_fraction:.2%}** of records "
            "({minority_count:,} rows). "
            "This severe imbalance will likely cause ML models to ignore the minority class."
        ),
        "class_imbalance_high": (
            "Significant class imbalance in **{column}**: the minority class "
            "'{minority_class}' represents only **{minority_fraction:.1%}** of data. "
            "Consider resampling or weighted training for ML models."
        ),
        "class_imbalance_moderate": (
            "Moderate class imbalance in **{column}**: minority class at "
            "**{minority_fraction:.1%}**. May affect model performance on minority class."
        ),
        "class_imbalance_ok": (
            "Class distribution in **{column}** is reasonably balanced "
            "(minority class: {minority_fraction:.1%})."
        ),
    },

    # -------------------------------------------------------------------------
    # TEMPORAL ANALYSIS
    # -------------------------------------------------------------------------
    "temporal": {
        "temporal_gaps_critical": (
            "Timestamp column **{column}** has **{gap_count}** significant gaps "
            "over {date_range}. The largest gap spans **{largest_gap}**, "
            "indicating missing data periods that require investigation."
        ),
        "temporal_gaps_high": (
            "Timestamp column **{column}** shows **{gap_count}** gaps over {date_range}. "
            "Largest gap: **{largest_gap}**. Consider filling missing time periods."
        ),
        "temporal_gaps_medium": (
            "Timestamp column **{column}** has **{gap_count}** gaps "
            "(largest: {largest_gap}), which may indicate incomplete time coverage."
        ),
        "temporal_no_gaps": (
            "Timestamp column **{column}** shows continuous coverage with no significant gaps."
        ),
        "temporal_trend": (
            "Temporal analysis of **{column}** reveals a **{trend_direction}** trend "
            "over the date range ({date_range})."
        ),
        "temporal_seasonality": (
            "Seasonality detected in **{column}** with {seasonality_type} patterns."
        ),
    },

    # -------------------------------------------------------------------------
    # CROSS-COLUMN CONSISTENCY
    # -------------------------------------------------------------------------
    "cross_column": {
        "ratio_anomaly": (
            "Cross-column analysis: the ratio of **{col1}** to **{col2}** shows "
            "**{anomaly_rate:.1%}** anomalous values that deviate significantly "
            "from expected patterns."
        ),
        "correlation_unexpected": (
            "Unexpected correlation between **{col1}** and **{col2}**: "
            "{correlation_type} correlation of **{correlation:.2f}** may indicate "
            "data quality issues or hidden relationships."
        ),
        "correlation_missing": (
            "Expected correlation between **{col1}** and **{col2}** not found. "
            "These columns may have data integrity issues."
        ),
        "cross_column_ok": (
            "Cross-column relationships appear consistent with no anomalies detected."
        ),
    },

    # -------------------------------------------------------------------------
    # UNIQUENESS / DUPLICATES
    # -------------------------------------------------------------------------
    "uniqueness": {
        "duplicates_critical": (
            "Column **{column}** has only **{unique_pct:.1%}** unique values, "
            "indicating significant duplication. If this is an identifier field, "
            "data integrity may be compromised."
        ),
        "duplicates_high": (
            "High duplication in **{column}**: **{duplicate_pct:.1%}** duplicate values detected."
        ),
        "potential_id_not_unique": (
            "Column **{column}** appears to be an identifier but has **{duplicate_count:,}** "
            "duplicate values ({duplicate_pct:.1%}). This may indicate data integrity issues."
        ),
        "uniqueness_ok": (
            "Column **{column}** has appropriate uniqueness ({unique_pct:.1%} unique values)."
        ),
    },

    # -------------------------------------------------------------------------
    # VALIDITY
    # -------------------------------------------------------------------------
    "validity": {
        "validity_low": (
            "Column **{column}** has low validity: **{validity_pct:.1%}** of values "
            "match the expected type/format. {interpretation}"
        ),
        "type_mismatch": (
            "Type inconsistency in **{column}**: expected {expected_type} but found "
            "{found_types}. This may cause parsing errors in downstream systems."
        ),
        "pattern_violation": (
            "Column **{column}** has **{violation_pct:.1%}** values not matching "
            "the detected pattern '{pattern}'. Consider data cleaning."
        ),
        "validity_ok": (
            "Column **{column}** shows strong validity ({validity_pct:.1%})."
        ),
    },

    # -------------------------------------------------------------------------
    # ML ANALYSIS / AUTOENCODER
    # -------------------------------------------------------------------------
    "ml_analysis": {
        "autoencoder_anomalies": (
            "Autoencoder analysis detected **{anomaly_count:,}** potential anomalies "
            "(**{anomaly_rate:.2%}** of records). These rows have unusual combinations "
            "of values that don't match typical data patterns."
        ),
        "autoencoder_high_anomalies": (
            "**High anomaly rate detected**: the autoencoder flagged **{anomaly_rate:.2%}** "
            "of records as anomalous. This suggests significant data quality issues or "
            "the presence of distinct subpopulations in the data."
        ),
        "autoencoder_clean": (
            "Autoencoder analysis found minimal anomalies ({anomaly_rate:.2%}), "
            "indicating consistent data patterns."
        ),
        "ml_not_available": (
            "ML-based anomaly detection was not performed. "
            "{reason}"
        ),
    },
}


# =============================================================================
# EXECUTIVE SUMMARY TEMPLATES
# =============================================================================

EXECUTIVE_SUMMARY_INTRO = {
    "excellent": (
        "This dataset demonstrates **excellent data quality** with a score of "
        "**{score:.1%}**. The data is well-suited for analytical and operational use "
        "with minimal concerns."
    ),
    "good": (
        "This dataset shows **good data quality** at **{score:.1%}**. "
        "While generally reliable, a few areas may warrant attention for high-stakes applications."
    ),
    "fair": (
        "Data quality is **fair** at **{score:.1%}**. "
        "Several issues have been identified that should be addressed before relying on this data "
        "for critical decisions."
    ),
    "poor": (
        "Data quality is **concerning** at **{score:.1%}**. "
        "Significant issues have been detected that affect data reliability. "
        "Remediation is recommended before analytical use."
    ),
    "critical": (
        "**Data quality is critically low** at **{score:.1%}**. "
        "Major problems have been identified that significantly impact data usability. "
        "Immediate action is required."
    ),
}


# =============================================================================
# SECTION HEADERS
# =============================================================================

SECTION_HEADERS = {
    "executive_summary": "Executive Summary",
    "sampling_methodology": "About Sampling",
    "pii": "Privacy & PII Analysis",
    "outliers": "Outlier Detection",
    "authenticity": "Data Authenticity (Benford's Law)",
    "label_quality": "Label Quality & Class Balance",
    "temporal": "Temporal Analysis",
    "cross_column": "Cross-Column Consistency",
    "completeness": "Completeness Analysis",
    "validity": "Validity & Type Consistency",
    "uniqueness": "Uniqueness & Duplicates",
    "ml_analysis": "ML-Based Anomaly Detection",
}


# =============================================================================
# SEVERITY LABELS
# =============================================================================

SEVERITY_LABELS = {
    "critical": "Critical",
    "high": "High",
    "medium": "Medium",
    "low": "Low",
    "info": "Info",
}

SEVERITY_DESCRIPTIONS = {
    "critical": "Requires immediate attention; data may be unreliable",
    "high": "Significant issue that should be addressed",
    "medium": "Notable concern; review recommended",
    "low": "Minor issue; consider for completeness",
    "info": "Informational finding; no action required",
}


# =============================================================================
# QUALITY TIER THRESHOLDS
# =============================================================================

QUALITY_TIERS = {
    "excellent": {"min": 0.90, "label": "Excellent", "description": "High confidence for all use cases"},
    "good": {"min": 0.80, "label": "Good", "description": "Suitable for most analytical use"},
    "fair": {"min": 0.60, "label": "Fair", "description": "Use with caution; review findings"},
    "poor": {"min": 0.40, "label": "Poor", "description": "Significant issues; remediation needed"},
    "critical": {"min": 0.0, "label": "Critical", "description": "Major problems; not recommended for use"},
}


def get_quality_tier(score: float) -> Dict[str, Any]:
    """
    Get quality tier information based on score.

    Args:
        score: Quality score (0.0 to 1.0)

    Returns:
        Dict with tier name, label, and description
    """
    for tier_name, tier_info in QUALITY_TIERS.items():
        if score >= tier_info["min"]:
            return {"name": tier_name, **tier_info}
    return {"name": "critical", **QUALITY_TIERS["critical"]}


# =============================================================================
# INTERPRETATION HELPERS
# =============================================================================

NULL_INTERPRETATIONS = {
    "very_high": "This level of missing data may significantly impact analysis reliability.",
    "high": "Consider imputation strategies or excluding this column from critical analyses.",
    "moderate": "Missing values may affect some analyses; consider appropriate handling.",
    "low": "Missing values are within acceptable limits for most use cases.",
    "minimal": "Excellent data completeness.",
}


def get_null_interpretation(null_pct: float) -> str:
    """Get interpretation text based on null percentage."""
    if null_pct >= 0.50:
        return NULL_INTERPRETATIONS["very_high"]
    elif null_pct >= 0.20:
        return NULL_INTERPRETATIONS["high"]
    elif null_pct >= 0.05:
        return NULL_INTERPRETATIONS["moderate"]
    elif null_pct >= 0.01:
        return NULL_INTERPRETATIONS["low"]
    else:
        return NULL_INTERPRETATIONS["minimal"]


OUTLIER_SEVERITY_NOTES = {
    "critical": "Critical: outliers may significantly distort analytical results.",
    "high": "High: consider outlier treatment (winsorization, removal, or robust methods).",
    "medium": "Medium: monitor impact on aggregations and model training.",
    "low": "Low: outliers are within normal variation.",
}


def get_outlier_severity_note(max_rate: float) -> str:
    """Get outlier severity note based on maximum outlier rate."""
    if max_rate >= 0.01:  # 1%+
        return OUTLIER_SEVERITY_NOTES["critical"]
    elif max_rate >= 0.001:  # 0.1%+
        return OUTLIER_SEVERITY_NOTES["high"]
    elif max_rate >= 0.0001:  # 0.01%+
        return OUTLIER_SEVERITY_NOTES["medium"]
    else:
        return OUTLIER_SEVERITY_NOTES["low"]


# =============================================================================
# TEMPLATE RENDERING
# =============================================================================

def render_template(
    template_id: str,
    category: str,
    data: Dict[str, Any],
    basis: str = "full",
    sample_size: Optional[int] = None,
    sample_fraction: Optional[float] = None
) -> str:
    """
    Render an issue template with data and sampling clause.

    Args:
        template_id: Template identifier within category
        category: Issue category (e.g., 'pii', 'outliers')
        data: Dict with template variable values
        basis: 'full' or 'sample'
        sample_size: Number of rows sampled (if applicable)
        sample_fraction: Fraction of dataset sampled (if applicable)

    Returns:
        Rendered template string with sampling disclosure
    """
    # Get template
    category_templates = ISSUE_TEMPLATES.get(category, {})
    template = category_templates.get(template_id)

    if not template:
        return f"[Template not found: {category}/{template_id}]"

    # Get sampling additions
    certainty_prefix = get_certainty_prefix(basis, sample_size)
    sampling_clause = get_sampling_clause(basis, sample_size, sample_fraction)

    # Render template
    try:
        # Handle cross-column templates that expect col1/col2 from columns list
        render_data = data.copy()
        if "columns" in render_data and isinstance(render_data["columns"], list):
            cols = render_data["columns"]
            if len(cols) >= 2:
                render_data["col1"] = cols[0]
                render_data["col2"] = cols[1]

        # Calculate derived values for cross-column templates
        if "total_issues" in render_data and "anomaly_rate" not in render_data:
            # Estimate anomaly rate from total_issues
            # Assume sample size of 50000 if not available
            sample_size = len(render_data.get("sample_rows", [])) or 50000
            render_data["anomaly_rate"] = render_data["total_issues"] / sample_size

        rendered = template.format(**render_data)
    except KeyError as e:
        return f"[Template rendering error: missing key {e}]"

    # Combine: certainty_prefix + rendered + sampling_clause
    return f"{certainty_prefix}{rendered}{sampling_clause}"
