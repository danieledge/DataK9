"""
User-friendly interpretations and explanations for profiling metrics.

Provides plain-language descriptions and detailed explanations for statistical
metrics, making data profiling results accessible to non-technical users.

Author: Daniel Edge
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass


@dataclass
class Interpretation:
    """
    User-friendly interpretation of a metric.

    Attributes:
        short_description: Brief, plain-language summary
        detailed_explanation: Extended explanation with context
        interpretation: What this means for your data
        recommendation: What action to take (if applicable)
        severity: info, success, warning, error
    """
    short_description: str
    detailed_explanation: str
    interpretation: str
    recommendation: Optional[str] = None
    severity: str = "info"  # info, success, warning, error


class MetricInterpreter:
    """Generates user-friendly interpretations for profiling metrics."""

    @staticmethod
    def interpret_schema_summary(profile_result: Any) -> Dict[str, Any]:
        """
        Generate a comprehensive schema summary with explanations.

        Args:
            profile_result: ProfileResult object

        Returns:
            Dictionary with schema summary and interpretations
        """
        columns_by_type = {}
        for col in profile_result.columns:
            col_type = col.type_info.inferred_type
            if col_type not in columns_by_type:
                columns_by_type[col_type] = []
            columns_by_type[col_type].append(col.name)

        return {
            "total_columns": len(profile_result.columns),
            "total_rows": profile_result.row_count,
            "file_size_mb": round(profile_result.file_size_bytes / (1024 * 1024), 2),
            "columns_by_type": columns_by_type,
            "quality_summary": {
                "overall_score": round(profile_result.overall_quality_score, 1),
                "interpretation": MetricInterpreter._interpret_quality_score(
                    profile_result.overall_quality_score
                )
            },
            "issues_detected": MetricInterpreter._summarize_issues(profile_result),
            "pii_detected": MetricInterpreter._detect_pii_columns(profile_result)
        }

    @staticmethod
    def interpret_overall_quality(quality_score: float) -> Interpretation:
        """Interpret overall data quality score."""
        if quality_score >= 90:
            severity = "success"
            interpretation = "Your data is in excellent condition with minimal quality issues."
            recommendation = "The data is ready to use. Continue monitoring for changes."
        elif quality_score >= 75:
            severity = "info"
            interpretation = "Your data has good quality with some minor issues that should be reviewed."
            recommendation = "Review the flagged issues and consider adding validations for critical fields."
        elif quality_score >= 60:
            severity = "warning"
            interpretation = "Your data has moderate quality issues that need attention."
            recommendation = "Investigate quality problems in low-scoring columns and add data validations."
        else:
            severity = "error"
            interpretation = "Your data has significant quality issues that must be addressed."
            recommendation = "Urgent: Review all flagged issues and implement data quality checks before using this data."

        return Interpretation(
            short_description=f"Overall Quality: {quality_score:.1f}/100",
            detailed_explanation=(
                "The overall quality score is calculated by averaging the quality scores of all columns. "
                "Each column's score considers four factors: completeness (how many values are present), "
                "validity (how many values match the expected data type), uniqueness (how diverse the values are), "
                "and consistency (how uniform the patterns are). "
                f"Your score of {quality_score:.1f}/100 indicates the general health of your dataset."
            ),
            interpretation=interpretation,
            recommendation=recommendation,
            severity=severity
        )

    @staticmethod
    def interpret_completeness(completeness: float, column_name: str) -> Interpretation:
        """Interpret completeness percentage."""
        missing_pct = 100 - completeness

        if completeness >= 99:
            severity = "success"
            interpretation = f"Almost all values are present in '{column_name}' (only {missing_pct:.1f}% missing)."
            recommendation = "Consider making this a mandatory field in your validations."
        elif completeness >= 90:
            severity = "info"
            interpretation = f"Most values are present in '{column_name}', with {missing_pct:.1f}% missing."
            recommendation = "Check if the missing values are expected or indicate data collection issues."
        elif completeness >= 70:
            severity = "warning"
            interpretation = f"A noticeable portion of '{column_name}' is missing ({missing_pct:.1f}% of rows)."
            recommendation = "Investigate why so many values are missing and improve data collection."
        else:
            severity = "error"
            interpretation = f"Most of '{column_name}' is empty ({missing_pct:.1f}% missing)!"
            recommendation = "This column is mostly empty. Determine if it's needed or if there's a data collection problem."

        return Interpretation(
            short_description=f"{completeness:.1f}% of values are present",
            detailed_explanation=(
                f"Completeness measures how many rows have actual values (not empty/null). "
                f"In this column, {completeness:.1f}% of the {column_name} entries contain data, "
                f"which means {missing_pct:.1f}% are missing or blank."
            ),
            interpretation=interpretation,
            recommendation=recommendation,
            severity=severity
        )

    @staticmethod
    def interpret_distribution(distribution_metrics: Any, column_name: str) -> Interpretation:
        """Interpret distribution analysis."""
        dist_type = distribution_metrics.distribution_type
        outlier_pct = distribution_metrics.outlier_percentage

        # Explain distribution type
        dist_explanations = {
            "normal": "follows a bell curve (most values are near the average, with fewer at the extremes)",
            "uniform": "values are evenly spread across the range",
            "right_skewed": "has a long tail on the high end (most values are low, with some very high values)",
            "left_skewed": "has a long tail on the low end (most values are high, with some very low values)",
            "heavy_tailed": "has more extreme values than expected (more outliers than normal)",
            "light_tailed": "has fewer extreme values than expected (clustered near the center)"
        }

        dist_explanation = dist_explanations.get(dist_type, "has an unknown distribution pattern")

        if outlier_pct > 10:
            severity = "warning"
            interpretation = (
                f"'{column_name}' {dist_explanation}. "
                f"Warning: {outlier_pct:.1f}% of values are statistical outliers (unusually high or low)."
            )
            recommendation = f"Review the {distribution_metrics.outlier_count} outlier values to determine if they're errors or legitimate extremes."
        elif outlier_pct > 5:
            severity = "info"
            interpretation = (
                f"'{column_name}' {dist_explanation}. "
                f"About {outlier_pct:.1f}% of values are outliers, which is higher than typical."
            )
            recommendation = "Consider investigating the outlier values to ensure data quality."
        else:
            severity = "success"
            interpretation = (
                f"'{column_name}' {dist_explanation} with normal variation. "
                f"Only {outlier_pct:.1f}% of values are outliers, which is expected."
            )
            recommendation = None

        return Interpretation(
            short_description=f"Distribution: {dist_type.replace('_', ' ').title()}",
            detailed_explanation=(
                f"The distribution shows how values are spread out in '{column_name}'. "
                f"This column {dist_explanation}. "
                f"We detected {distribution_metrics.outlier_count} outlier values "
                f"({outlier_pct:.1f}% of the data) using statistical methods. "
                f"Outliers are values that are unusually far from the typical range."
            ),
            interpretation=interpretation,
            recommendation=recommendation,
            severity=severity
        )

    @staticmethod
    def interpret_anomalies(anomaly_info: Any, column_name: str) -> Interpretation:
        """Interpret anomaly detection results."""
        anomaly_pct = anomaly_info.anomaly_percentage
        methods = ", ".join(anomaly_info.anomaly_methods)

        if anomaly_pct > 5:
            severity = "error"
            interpretation = (
                f"Serious data quality issue: {anomaly_pct:.1f}% of '{column_name}' contains "
                f"unusual or problematic values."
            )
            recommendation = (
                f"Investigate the {anomaly_info.anomaly_count} anomalous values immediately. "
                f"These were detected using: {methods}. Check for data entry errors or system issues."
            )
        elif anomaly_pct > 2:
            severity = "warning"
            interpretation = (
                f"Data quality concern: {anomaly_pct:.1f}% of '{column_name}' has anomalies "
                f"(unusual patterns or values)."
            )
            recommendation = (
                f"Review the {anomaly_info.anomaly_count} flagged values. "
                f"Detection methods used: {methods}."
            )
        else:
            severity = "info"
            interpretation = (
                f"Minor anomalies detected: {anomaly_pct:.1f}% of '{column_name}' shows unusual patterns. "
                f"This is generally acceptable."
            )
            recommendation = "Monitor these values but they're likely not a major concern."

        return Interpretation(
            short_description=f"{anomaly_info.anomaly_count} anomalies found ({anomaly_pct:.1f}%)",
            detailed_explanation=(
                f"Anomalies are values that don't fit the normal pattern for this column. "
                f"We found {anomaly_info.anomaly_count} anomalous values ({anomaly_pct:.1f}% of the data) "
                f"using multiple detection methods: {methods}. "
                f"These might be data entry errors, system glitches, or legitimately unusual but valid values."
            ),
            interpretation=interpretation,
            recommendation=recommendation,
            severity=severity
        )

    @staticmethod
    def interpret_temporal(temporal_metrics: Any, column_name: str) -> Interpretation:
        """Interpret temporal analysis."""
        issues = []
        severity = "success"

        if temporal_metrics.has_future_dates:
            issues.append(f"{temporal_metrics.future_date_count} dates are in the future (likely errors)")
            severity = "error"

        if temporal_metrics.has_gaps:
            issues.append(f"{temporal_metrics.gap_count} gaps in the date sequence")
            if severity != "error":
                severity = "warning"

        if temporal_metrics.is_fresh is False:
            issues.append(f"data is {temporal_metrics.days_since_latest} days old (may be stale)")
            if severity == "success":
                severity = "info"

        if issues:
            interpretation = f"Date quality issues detected in '{column_name}': {'; '.join(issues)}."
            if temporal_metrics.has_future_dates:
                recommendation = "Fix future dates immediately - these are likely data entry errors."
            elif temporal_metrics.has_gaps:
                recommendation = "Review the date gaps to determine if they're expected (e.g., weekends/holidays) or indicate missing data."
            else:
                recommendation = "Consider updating this dataset with more recent data."
        else:
            interpretation = (
                f"Date quality looks good in '{column_name}'. "
                f"Data spans from {temporal_metrics.earliest_date} to {temporal_metrics.latest_date} "
                f"with a {temporal_metrics.temporal_pattern} pattern."
            )
            recommendation = None

        return Interpretation(
            short_description=f"Date range: {temporal_metrics.earliest_date} to {temporal_metrics.latest_date}",
            detailed_explanation=(
                f"This column contains dates from {temporal_metrics.earliest_date} to {temporal_metrics.latest_date}, "
                f"covering {temporal_metrics.date_range_days} days. "
                f"The dates follow a {temporal_metrics.temporal_pattern} pattern "
                f"(on average, {temporal_metrics.avg_interval_days:.1f} days between entries). "
                f"We analyzed the dates for common quality issues like gaps, future dates, and staleness."
            ),
            interpretation=interpretation,
            recommendation=recommendation,
            severity=severity
        )

    @staticmethod
    def interpret_patterns(pattern_info: Any, column_name: str) -> Interpretation:
        """Interpret pattern detection."""
        if not pattern_info.semantic_type:
            return Interpretation(
                short_description="No specific pattern detected",
                detailed_explanation=f"The values in '{column_name}' don't match any standard format patterns.",
                interpretation="This column contains free-form text or mixed formats.",
                recommendation=None,
                severity="info"
            )

        semantic_type = pattern_info.semantic_type
        confidence = pattern_info.semantic_confidence

        # Explain semantic types in plain language
        type_explanations = {
            "email": "email addresses (like user@example.com)",
            "phone_us": "US phone numbers",
            "phone_intl": "phone numbers",
            "ssn": "Social Security Numbers",
            "credit_card": "credit card numbers",
            "zipcode_us": "US ZIP codes",
            "url": "website URLs",
            "ipv4": "IP addresses",
            "uuid": "unique identifiers (UUIDs)",
            "currency": "currency amounts"
        }

        type_explanation = type_explanations.get(semantic_type, semantic_type)

        severity = "info"
        if pattern_info.pii_detected:
            severity = "warning"
            interpretation = (
                f"⚠️ Personally Identifiable Information (PII) detected! '{column_name}' contains {type_explanation}. "
                f"{confidence:.0f}% of values match this pattern."
            )
            recommendation = (
                "This column contains sensitive information that must be protected. "
                "Ensure proper access controls, encryption, and compliance with privacy regulations (GDPR, CCPA, etc.)."
            )
        else:
            interpretation = (
                f"'{column_name}' contains {type_explanation}. "
                f"{confidence:.0f}% of values match this expected pattern."
            )
            recommendation = f"Add a validation rule to ensure all values are properly formatted {type_explanation}."

        return Interpretation(
            short_description=f"Contains: {type_explanation}",
            detailed_explanation=(
                f"We analyzed the values in '{column_name}' and found that {confidence:.0f}% match the pattern for {type_explanation}. "
                f"This helps us understand what kind of data this column should contain and detect any values that don't fit the pattern."
            ),
            interpretation=interpretation,
            recommendation=recommendation,
            severity=severity
        )

    @staticmethod
    def interpret_dependencies(dependency_info: Any, column_name: str) -> Interpretation:
        """Interpret functional dependencies."""
        if not dependency_info.depends_on and not dependency_info.determines:
            return None

        explanations = []

        if dependency_info.depends_on:
            dep_columns = ", ".join(dependency_info.depends_on[:3])
            explanations.append(
                f"'{column_name}' is determined by {dep_columns} "
                f"(meaning: if you know {dep_columns}, you can predict '{column_name}')"
            )

        if dependency_info.determines:
            det_columns = ", ".join(dependency_info.determines[:3])
            explanations.append(
                f"'{column_name}' determines {det_columns} "
                f"(meaning: '{column_name}' can be used to look up {det_columns})"
            )

        interpretation = " and ".join(explanations) + "."

        return Interpretation(
            short_description="Relationships detected with other columns",
            detailed_explanation=(
                "A functional dependency means one column's value can predict another column's value. "
                "For example, if 'customer_id' always has the same 'customer_name', then customer_id determines customer_name. "
                f"{interpretation}"
            ),
            interpretation=interpretation,
            recommendation="Use these relationships to add cross-field validation rules that check data consistency.",
            severity="info"
        )

    @staticmethod
    def _interpret_quality_score(score: float) -> str:
        """Simple quality score interpretation."""
        if score >= 90:
            return "Excellent"
        elif score >= 75:
            return "Good"
        elif score >= 60:
            return "Fair"
        else:
            return "Poor"

    @staticmethod
    def _summarize_issues(profile_result: Any) -> List[Dict[str, Any]]:
        """Summarize data quality observations found."""
        observations = []

        for col in profile_result.columns:
            col_observations = []

            # Completeness observations
            if col.quality.completeness < 90:
                col_observations.append({
                    "type": "Missing Values",
                    "category": "completeness",
                    "importance": "critical" if col.quality.completeness < 50 else "attention",
                    "description": f"{100 - col.quality.completeness:.1f}% of values are missing"
                })

            # Anomalies
            if col.anomalies and col.anomalies.has_anomalies and col.anomalies.anomaly_percentage > 2:
                col_observations.append({
                    "type": "Anomalies",
                    "category": "quality",
                    "importance": "critical" if col.anomalies.anomaly_percentage > 5 else "attention",
                    "description": f"{col.anomalies.anomaly_percentage:.1f}% anomalous values"
                })

            # Outliers
            if col.distribution and col.distribution.outlier_percentage > 10:
                col_observations.append({
                    "type": "Outliers",
                    "category": "quality",
                    "importance": "attention",
                    "description": f"{col.distribution.outlier_percentage:.1f}% outlier values"
                })

            # Future dates
            if col.temporal and col.temporal.has_future_dates:
                col_observations.append({
                    "type": "Future Dates",
                    "category": "quality",
                    "importance": "critical",
                    "description": f"{col.temporal.future_date_count} dates in the future"
                })

            # PII - this is informational, not a quality issue
            if col.patterns and col.patterns.pii_detected:
                col_observations.append({
                    "type": "PII Detected",
                    "category": "privacy",
                    "importance": "informational",
                    "description": f"Contains: {', '.join(col.patterns.pii_types)}"
                })

            if col_observations:
                observations.append({
                    "column": col.name,
                    "issues": col_observations  # Keep key name for backward compatibility
                })

        return observations

    @staticmethod
    def _detect_pii_columns(profile_result: Any) -> List[str]:
        """Detect columns containing PII."""
        pii_columns = []
        for col in profile_result.columns:
            if col.patterns and col.patterns.pii_detected:
                pii_columns.append(col.name)
        return pii_columns
