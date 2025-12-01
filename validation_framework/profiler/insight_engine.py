"""
Rule-based insight engine for data profiling reports.

Transforms raw profiling output into structured issues, explanatory text,
executive summaries, and themed sections. Includes sampling transparency.

Architecture:
    ProfileResult -> RuleEngine -> Issues -> TextGenerator -> Report Sections
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import logging

from validation_framework.profiler.insight_templates import (
    ISSUE_TEMPLATES,
    EXECUTIVE_SUMMARY_INTRO,
    SECTION_HEADERS,
    SAMPLING_OVERVIEW_FULL,
    render_template,
    get_quality_tier,
    get_null_interpretation,
    get_outlier_severity_note,
    get_sampling_clause,
    get_sampling_explanation,
    get_sampling_overview_sampled,
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

# Default sampling threshold - datasets > this size use sampling
# This matches the default in engine.py and cli.py (100K rows)
SAMPLE_THRESHOLD = 100_000
SAMPLE_SIZE = 100_000


class Severity(Enum):
    """Issue severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

    @property
    def priority(self) -> int:
        """Numeric priority for sorting (lower = more severe)."""
        priorities = {
            Severity.CRITICAL: 0,
            Severity.HIGH: 1,
            Severity.MEDIUM: 2,
            Severity.LOW: 3,
            Severity.INFO: 4,
        }
        return priorities.get(self, 5)


# =============================================================================
# ISSUE MODEL
# =============================================================================

@dataclass
class Issue:
    """
    Structured issue detected by the rule engine.

    Attributes:
        id: Unique identifier for the issue type
        severity: Issue severity level
        category: Issue category (e.g., 'pii', 'outliers', 'authenticity')
        data: Dict containing values needed for template rendering
        basis: 'full' or 'sample' - indicates data source
        sample_size: Number of rows sampled (if sampled)
        sample_fraction: Fraction of dataset sampled (if sampled)
        rendered_text: Pre-rendered template text (populated by TextGenerator)
    """
    id: str
    severity: Severity
    category: str
    data: Dict[str, Any]
    basis: str = "full"
    sample_size: Optional[int] = None
    sample_fraction: Optional[float] = None
    rendered_text: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "severity": self.severity.value,
            "category": self.category,
            "data": self.data,
            "basis": self.basis,
            "sample_size": self.sample_size,
            "sample_fraction": self.sample_fraction,
            "text": self.rendered_text,
        }


# =============================================================================
# CONFIGURABLE THRESHOLDS
# =============================================================================

@dataclass
class InsightThresholds:
    """
    Configurable thresholds for issue detection.

    All thresholds can be customized per-use-case.
    """
    # Overall quality
    quality_excellent: float = 0.90
    quality_good: float = 0.80
    quality_fair: float = 0.60
    quality_poor: float = 0.40

    # Completeness
    null_critical: float = 0.50  # 50%+ nulls = critical
    null_high: float = 0.20     # 20%+ nulls = high
    null_medium: float = 0.05   # 5%+ nulls = medium

    # Outliers
    outlier_critical: float = 0.01   # 1%+ outliers = critical
    outlier_high: float = 0.001      # 0.1%+ outliers = high
    outlier_medium: float = 0.0001   # 0.01%+ outliers = medium

    # Benford's Law
    benford_critical_chi2: float = 100.0   # Chi-squared threshold for critical
    benford_high_chi2: float = 50.0        # Chi-squared threshold for high
    benford_p_value_threshold: float = 0.05  # P-value for significance

    # Class imbalance (general categorical)
    imbalance_critical: float = 0.01   # Minority < 1% = critical
    imbalance_high: float = 0.05       # Minority < 5% = high
    imbalance_medium: float = 0.10     # Minority < 10% = medium

    # Binary target imbalance (higher thresholds for ML classification targets)
    binary_target_imbalance_high: float = 0.30   # Minority < 30% = high for binary targets
    binary_target_imbalance_medium: float = 0.40  # Minority < 40% = medium for binary targets

    # PII risk scores (0-100)
    pii_high_risk: int = 70
    pii_medium_risk: int = 40

    # Validity
    validity_low: float = 0.80   # Below 80% validity = flagged

    # Uniqueness (for ID columns)
    uniqueness_critical: float = 0.90  # ID columns should be >90% unique


# =============================================================================
# SAMPLING INFO EXTRACTION
# =============================================================================

@dataclass
class SamplingInfo:
    """Information about sampling applied during profiling."""
    total_rows: int
    sample_used: bool
    sample_size: int
    sample_fraction: float
    full_dataset_metrics: List[str] = field(default_factory=list)
    sampled_metrics: List[str] = field(default_factory=list)

    @property
    def basis(self) -> str:
        """Return 'sample' or 'full' based on whether sampling was used."""
        return "sample" if self.sample_used else "full"


def determine_sampling_info(total_rows: int) -> SamplingInfo:
    """
    Determine sampling parameters based on dataset size.

    Implements the sampling policy (default 100K, configurable via --analysis-sample-size):
    - If rows <= threshold: use full dataset
    - If rows > threshold: use sample

    Args:
        total_rows: Total rows in the dataset

    Returns:
        SamplingInfo with sampling configuration
    """
    if total_rows < SAMPLE_THRESHOLD:
        return SamplingInfo(
            total_rows=total_rows,
            sample_used=False,
            sample_size=total_rows,
            sample_fraction=1.0,
            full_dataset_metrics=[
                "row_count", "null_count", "unique_count",
                "min", "max", "validity_checks", "pattern_analysis",
                "ml_anomaly_detection", "benford_analysis"
            ],
            sampled_metrics=[]
        )
    else:
        fraction = SAMPLE_SIZE / total_rows
        return SamplingInfo(
            total_rows=total_rows,
            sample_used=True,
            sample_size=SAMPLE_SIZE,
            sample_fraction=fraction,
            full_dataset_metrics=[
                "row_count", "null_count", "column_metadata"
            ],
            sampled_metrics=[
                "validity_checks", "pattern_analysis",
                "ml_anomaly_detection", "benford_analysis",
                "outlier_detection", "distribution_analysis",
                "cross_column_checks"
            ]
        )


# =============================================================================
# RULE ENGINE
# =============================================================================

class RuleEngine:
    """
    Rule engine that inspects profiler output and emits structured issues.

    Each rule family corresponds to a category of issues and applies
    configurable thresholds to determine severity.
    """

    def __init__(self, thresholds: Optional[InsightThresholds] = None):
        """
        Initialize rule engine with thresholds.

        Args:
            thresholds: Custom thresholds (uses defaults if None)
        """
        self.thresholds = thresholds or InsightThresholds()

    def analyze(self, profile_data: Dict[str, Any]) -> Tuple[List[Issue], SamplingInfo]:
        """
        Analyze profile data and generate issues.

        Args:
            profile_data: Dictionary representation of ProfileResult

        Returns:
            Tuple of (list of Issues, SamplingInfo)
        """
        issues: List[Issue] = []

        # Determine sampling configuration from actual profile data
        total_rows = profile_data.get("row_count", 0)

        # Check ml_findings for actual sample size (preferred source)
        ml_findings = profile_data.get("ml_findings", {})
        ml_sample_info = ml_findings.get("sample_info", {})
        actual_sample_size = ml_sample_info.get("analyzed_rows", 0)
        actual_original_rows = ml_sample_info.get("original_rows", total_rows)
        # Use the explicit 'sampled' flag from ML analysis if available
        ml_sampled = ml_sample_info.get("sampled", None)

        if ml_sampled is True and actual_sample_size > 0:
            # ML analysis explicitly reported sampling was used
            sampling_info = SamplingInfo(
                total_rows=actual_original_rows,
                sample_used=True,
                sample_size=actual_sample_size,
                sample_fraction=actual_sample_size / actual_original_rows if actual_original_rows > 0 else 1.0,
                full_dataset_metrics=["row_count", "null_count", "column_metadata"],
                sampled_metrics=[
                    "validity_checks", "pattern_analysis",
                    "ml_anomaly_detection", "benford_analysis",
                    "outlier_detection", "distribution_analysis",
                    "cross_column_checks"
                ]
            )
        elif ml_sampled is False:
            # ML analysis explicitly reported NO sampling - full analysis done
            sampling_info = SamplingInfo(
                total_rows=actual_original_rows if actual_original_rows > 0 else total_rows,
                sample_used=False,
                sample_size=actual_sample_size if actual_sample_size > 0 else total_rows,
                sample_fraction=1.0,
                full_dataset_metrics=[
                    "row_count", "null_count", "column_types",
                    "validity_checks", "pattern_analysis",
                    "ml_anomaly_detection", "benford_analysis",
                    "outlier_detection", "distribution_analysis",
                    "cross_column_checks"
                ],
                sampled_metrics=[]
            )
        else:
            # No ML info available - fallback to default logic based on row count
            sampling_info = determine_sampling_info(total_rows)

        # Run all rule families
        issues.extend(self._analyze_overall_quality(profile_data, sampling_info))
        issues.extend(self._analyze_completeness(profile_data, sampling_info))
        issues.extend(self._analyze_pii(profile_data, sampling_info))
        issues.extend(self._analyze_outliers(profile_data, sampling_info))
        issues.extend(self._analyze_benford(profile_data, sampling_info))
        issues.extend(self._analyze_class_imbalance(profile_data, sampling_info))
        issues.extend(self._analyze_temporal(profile_data, sampling_info))
        issues.extend(self._analyze_cross_column(profile_data, sampling_info))
        issues.extend(self._analyze_validity(profile_data, sampling_info))
        issues.extend(self._analyze_ml_findings(profile_data, sampling_info))

        # Sort by severity
        issues.sort(key=lambda x: x.severity.priority)

        return issues, sampling_info

    def _analyze_overall_quality(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze overall data quality score."""
        issues = []
        score = profile.get("overall_quality_score", 0) / 100.0  # Convert to 0-1

        tier = get_quality_tier(score)
        tier_name = tier["name"]

        # Map tier to severity
        severity_map = {
            "excellent": Severity.INFO,
            "good": Severity.LOW,
            "fair": Severity.MEDIUM,
            "poor": Severity.HIGH,
            "critical": Severity.CRITICAL,
        }

        issues.append(Issue(
            id=f"overall_quality_{tier_name}",
            severity=severity_map.get(tier_name, Severity.MEDIUM),
            category="overall_quality",
            data={"score": score},
            basis="full",  # Overall quality uses full dataset metrics
        ))

        return issues

    def _analyze_completeness(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze data completeness across columns."""
        issues = []
        columns = profile.get("columns", [])

        if not columns:
            return issues

        # Calculate average completeness
        completeness_scores = [
            col.get("quality", {}).get("completeness", 100) / 100.0
            for col in columns
        ]
        avg_completeness = sum(completeness_scores) / len(completeness_scores)

        # Count columns with significant nulls
        null_columns = []
        for col in columns:
            stats = col.get("statistics", {})
            null_pct = stats.get("null_percentage", 0) / 100.0

            if null_pct >= self.thresholds.null_medium:
                null_columns.append({
                    "name": col.get("name", "unknown"),
                    "null_pct": null_pct,
                    "null_count": stats.get("null_count", 0),
                })

        # Generate issues for columns with high nulls
        for col_info in null_columns:
            null_pct = col_info["null_pct"]

            if null_pct >= self.thresholds.null_critical:
                severity = Severity.CRITICAL
            elif null_pct >= self.thresholds.null_high:
                severity = Severity.HIGH
            else:
                severity = Severity.MEDIUM

            interpretation = get_null_interpretation(null_pct)

            issues.append(Issue(
                id="column_missing_values",
                severity=severity,
                category="completeness",
                data={
                    "column": col_info["name"],
                    "null_pct": null_pct,
                    "null_count": col_info["null_count"],
                    "interpretation": interpretation,
                },
                basis="full",  # Null counts from full dataset
            ))

        return issues

    def _analyze_pii(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze PII detection results."""
        issues = []
        columns = profile.get("columns", [])

        pii_columns = []
        for col in columns:
            pii_info = col.get("pii_info", {})
            if pii_info and pii_info.get("detected"):
                pii_columns.append({
                    "name": col.get("name", "unknown"),
                    "pii_type": pii_info.get("detected_type", "unknown"),
                    "risk_score": pii_info.get("risk_score", 0),
                })

        if not pii_columns:
            # No PII detected - info-level finding
            issues.append(Issue(
                id="no_pii",
                severity=Severity.INFO,
                category="pii",
                data={},
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))
            return issues

        # Summary issue
        pii_names = [c["name"] for c in pii_columns]
        issues.append(Issue(
            id="pii_detected",
            severity=Severity.HIGH,
            category="pii",
            data={
                "count": len(pii_columns),
                "columns": ", ".join(pii_names),
            },
            basis=sampling.basis,
            sample_size=sampling.sample_size if sampling.sample_used else None,
            sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
        ))

        # Individual column issues
        for pii_col in pii_columns:
            risk_score = pii_col["risk_score"]

            if risk_score >= self.thresholds.pii_high_risk:
                severity = Severity.CRITICAL
                template_id = "pii_high_risk"
            elif risk_score >= self.thresholds.pii_medium_risk:
                severity = Severity.HIGH
                template_id = "pii_medium_risk"
            else:
                severity = Severity.MEDIUM
                template_id = "pii_low_risk"

            issues.append(Issue(
                id=template_id,
                severity=severity,
                category="pii",
                data={
                    "column": pii_col["name"],
                    "pii_type": pii_col["pii_type"],
                    "risk_score": risk_score,
                },
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))

        return issues

    def _analyze_outliers(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze outlier detection results from ML findings."""
        issues = []
        ml_findings = profile.get("ml_findings", {})

        if not ml_findings:
            return issues

        # Check both 'numeric_outliers' (actual key) and 'outliers' for compatibility
        outliers = ml_findings.get("numeric_outliers", {}) or ml_findings.get("outliers", {})
        if not outliers:
            issues.append(Issue(
                id="no_outliers",
                severity=Severity.INFO,
                category="outliers",
                data={},
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))
            return issues

        total_outliers = 0
        affected_columns = 0
        max_rate = 0.0

        for col_name, col_data in outliers.items():
            if not isinstance(col_data, dict):
                continue

            # Support both ML findings structure (anomaly_count) and legacy (count)
            outlier_count = col_data.get("anomaly_count", col_data.get("count", 0))
            if outlier_count == 0:
                continue

            # Get rate from anomaly_percentage or calculate from count
            rate = col_data.get("anomaly_percentage", 0) / 100
            if rate == 0:
                total_rows = col_data.get("total_rows", sampling.total_rows)
                rate = outlier_count / total_rows if total_rows > 0 else 0

            total_outliers += outlier_count
            affected_columns += 1
            max_rate = max(max_rate, rate)

            # Determine severity based on rate
            if rate >= self.thresholds.outlier_critical:
                severity = Severity.CRITICAL
                template_id = "outliers_critical"
            elif rate >= self.thresholds.outlier_high:
                severity = Severity.HIGH
                template_id = "outliers_high"
            else:
                severity = Severity.MEDIUM
                template_id = "outliers_medium"

            issues.append(Issue(
                id=template_id,
                severity=severity,
                category="outliers",
                data={
                    "column": col_name,
                    "rate": rate,
                    "count": outlier_count,
                },
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))

        # Summary issue if outliers found
        if total_outliers > 0:
            severity_note = get_outlier_severity_note(max_rate)
            issues.insert(0, Issue(
                id="outliers_summary",
                severity=Severity.HIGH if max_rate >= self.thresholds.outlier_high else Severity.MEDIUM,
                category="outliers",
                data={
                    "total_outliers": total_outliers,
                    "affected_columns": affected_columns,
                    "severity_note": severity_note,
                },
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))

        return issues

    def _analyze_benford(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze Benford's Law results."""
        issues = []
        ml_findings = profile.get("ml_findings", {})

        if not ml_findings:
            return issues

        benford = ml_findings.get("benford", {})
        if not benford:
            return issues

        failed_columns = []

        for col_name, col_data in benford.items():
            if not isinstance(col_data, dict):
                continue

            chi2 = col_data.get("chi_squared", 0)
            p_value = col_data.get("p_value", 1.0)
            conforms = col_data.get("conforms_to_benford", True)

            if not conforms:
                failed_columns.append(col_name)

                if chi2 >= self.thresholds.benford_critical_chi2:
                    severity = Severity.HIGH
                    template_id = "benford_failed"
                else:
                    severity = Severity.MEDIUM
                    template_id = "benford_warning"

                issues.append(Issue(
                    id=template_id,
                    severity=severity,
                    category="authenticity",
                    data={
                        "column": col_name,
                        "chi2": chi2,
                        "p_value": p_value,
                    },
                    basis=sampling.basis,
                    sample_size=sampling.sample_size if sampling.sample_used else None,
                    sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
                ))

        # Summary if failures found
        if failed_columns:
            issues.insert(0, Issue(
                id="benford_summary",
                severity=Severity.HIGH,
                category="authenticity",
                data={"failed_count": len(failed_columns)},
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))

        return issues

    def _is_likely_ml_target(self, col_name: str, unique_count: int) -> bool:
        """
        Check if column is likely an ML classification target.

        Args:
            col_name: Column name
            unique_count: Number of unique values

        Returns:
            True if column appears to be a binary/categorical ML target
        """
        # Common ML target column names
        target_keywords = [
            'target', 'label', 'class', 'survived', 'churn', 'default',
            'fraud', 'spam', 'outcome', 'result', 'status', 'flag',
            'is_', 'has_', 'y_', '_y', 'response', 'conversion'
        ]
        col_lower = col_name.lower()

        # Check for target-like naming
        is_target_name = any(kw in col_lower for kw in target_keywords)

        # Binary columns (2 unique values) with target-like names are likely ML targets
        if unique_count == 2 and is_target_name:
            return True

        # Columns named exactly 'target', 'label', or 'y' are almost certainly ML targets
        if col_lower in ['target', 'label', 'y', 'class', 'survived']:
            return True

        return False

    def _analyze_class_imbalance(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze class imbalance in categorical columns, especially ML targets."""
        issues = []
        columns = profile.get("columns", [])

        for col in columns:
            stats = col.get("statistics", {})
            top_values = stats.get("top_values", [])
            unique_count = stats.get("unique_count", 0)
            col_name = col.get("name", "unknown")

            # Skip columns with too many unique values (not good for classification)
            if unique_count > 20 or unique_count < 2:
                continue

            # Need at least 2 top values to assess imbalance
            if len(top_values) < 2:
                continue

            # Calculate minority fraction
            total_count = sum(tv.get("count", 0) for tv in top_values)
            if total_count == 0:
                continue

            # Get minority class info
            minority_value = top_values[-1] if len(top_values) > 1 else top_values[0]
            minority_class = str(minority_value.get("value", "unknown"))
            minority_count = minority_value.get("count", 0)
            minority_fraction = minority_count / total_count if total_count > 0 else 0

            # Check if this is likely an ML target
            is_ml_target = self._is_likely_ml_target(col_name, unique_count)

            # Apply different thresholds for ML targets vs general columns
            if is_ml_target and unique_count == 2:
                # Binary ML target - use higher thresholds
                if minority_fraction >= self.thresholds.binary_target_imbalance_medium:
                    continue  # Balanced enough

                if minority_fraction < self.thresholds.imbalance_critical:
                    severity = Severity.HIGH
                    template_id = "class_imbalance_critical"
                elif minority_fraction < self.thresholds.binary_target_imbalance_high:
                    severity = Severity.MEDIUM
                    template_id = "class_imbalance_high"
                else:
                    severity = Severity.LOW
                    template_id = "class_imbalance_moderate"

                issues.append(Issue(
                    id=template_id,
                    severity=severity,
                    category="label_quality",
                    data={
                        "column": col_name,
                        "minority_class": minority_class,
                        "minority_fraction": minority_fraction,
                        "minority_count": minority_count,
                        "is_ml_target": True,
                    },
                    basis="full",  # Class distribution from full data
                ))
            else:
                # General categorical - use standard thresholds
                if minority_fraction >= self.thresholds.imbalance_medium:
                    continue

                if minority_fraction < self.thresholds.imbalance_critical:
                    severity = Severity.HIGH
                    template_id = "class_imbalance_critical"
                elif minority_fraction < self.thresholds.imbalance_high:
                    severity = Severity.MEDIUM
                    template_id = "class_imbalance_high"
                else:
                    severity = Severity.LOW
                    template_id = "class_imbalance_moderate"

                issues.append(Issue(
                    id=template_id,
                    severity=severity,
                    category="label_quality",
                    data={
                        "column": col_name,
                        "minority_class": minority_class,
                        "minority_fraction": minority_fraction,
                        "minority_count": minority_count,
                        "is_ml_target": False,
                    },
                    basis="full",
                ))

        return issues

    def _analyze_temporal(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze temporal gaps and patterns."""
        issues = []
        columns = profile.get("columns", [])

        for col in columns:
            temporal = col.get("temporal_analysis", {})
            if not temporal or not temporal.get("available"):
                continue

            gaps = temporal.get("gaps", {})
            gap_count = gaps.get("count", 0)

            if gap_count > 0:
                largest_gap = gaps.get("largest", "unknown")
                date_range = temporal.get("date_range", "unknown")

                if gap_count >= 10:
                    severity = Severity.HIGH
                    template_id = "temporal_gaps_critical"
                elif gap_count >= 5:
                    severity = Severity.MEDIUM
                    template_id = "temporal_gaps_high"
                else:
                    severity = Severity.LOW
                    template_id = "temporal_gaps_medium"

                issues.append(Issue(
                    id=template_id,
                    severity=severity,
                    category="temporal",
                    data={
                        "column": col.get("name", "unknown"),
                        "gap_count": gap_count,
                        "largest_gap": largest_gap,
                        "date_range": date_range,
                    },
                    basis="full",  # Temporal analysis uses full dataset
                ))

        return issues

    def _analyze_cross_column(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze cross-column consistency issues."""
        issues = []
        ml_findings = profile.get("ml_findings", {})

        if not ml_findings:
            return issues

        cross_column = ml_findings.get("cross_column_issues", [])
        if not cross_column:
            return issues

        # Handle both list and dict formats
        if isinstance(cross_column, list):
            cross_column_items = enumerate(cross_column)
        else:
            cross_column_items = cross_column.items()

        for issue_key, issue_data in cross_column_items:
            if not isinstance(issue_data, dict):
                continue

            issue_type = issue_data.get("type", "ratio_anomaly")
            severity_str = issue_data.get("severity", "medium")

            severity_map = {
                "critical": Severity.CRITICAL,
                "high": Severity.HIGH,
                "medium": Severity.MEDIUM,
                "low": Severity.LOW,
            }
            severity = severity_map.get(severity_str, Severity.MEDIUM)

            issues.append(Issue(
                id=issue_type,
                severity=severity,
                category="cross_column",
                data=issue_data,
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))

        return issues

    def _analyze_validity(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze data validity issues."""
        issues = []
        columns = profile.get("columns", [])

        for col in columns:
            quality = col.get("quality", {})
            validity = quality.get("validity", 100) / 100.0

            if validity < self.thresholds.validity_low:
                issues.append(Issue(
                    id="validity_low",
                    severity=Severity.MEDIUM,
                    category="validity",
                    data={
                        "column": col.get("name", "unknown"),
                        "validity_pct": validity,
                        "interpretation": "Values may not match expected type or format.",
                    },
                    basis="full",  # Validity from full scan
                ))

        return issues

    def _analyze_ml_findings(
        self, profile: Dict, sampling: SamplingInfo
    ) -> List[Issue]:
        """Analyze autoencoder and other ML-based findings."""
        issues = []
        ml_findings = profile.get("ml_findings", {})

        if not ml_findings:
            issues.append(Issue(
                id="ml_not_available",
                severity=Severity.INFO,
                category="ml_analysis",
                data={"reason": "ML analysis was not enabled or data was insufficient."},
                basis="full",
            ))
            return issues

        # Autoencoder anomalies
        autoencoder = ml_findings.get("autoencoder", {})
        if autoencoder:
            anomaly_count = autoencoder.get("anomaly_count", 0)
            total_rows = autoencoder.get("total_rows", sampling.total_rows)
            anomaly_rate = anomaly_count / total_rows if total_rows > 0 else 0

            if anomaly_rate > 0.05:  # > 5% anomalies
                template_id = "autoencoder_high_anomalies"
                severity = Severity.HIGH
            elif anomaly_count > 0:
                template_id = "autoencoder_anomalies"
                severity = Severity.MEDIUM
            else:
                template_id = "autoencoder_clean"
                severity = Severity.INFO

            issues.append(Issue(
                id=template_id,
                severity=severity,
                category="ml_analysis",
                data={
                    "anomaly_count": anomaly_count,
                    "anomaly_rate": anomaly_rate,
                },
                basis=sampling.basis,
                sample_size=sampling.sample_size if sampling.sample_used else None,
                sample_fraction=sampling.sample_fraction if sampling.sample_used else None,
            ))

        return issues


# =============================================================================
# TEXT GENERATOR
# =============================================================================

class TextGenerator:
    """
    Renders issue templates into human-readable text.

    Applies sampling clauses and certainty prefixes based on issue metadata.
    """

    def render_issues(self, issues: List[Issue]) -> List[Issue]:
        """
        Render all issues with their template text.

        Args:
            issues: List of Issue objects

        Returns:
            Same list with rendered_text populated
        """
        for issue in issues:
            issue.rendered_text = render_template(
                template_id=issue.id,
                category=issue.category,
                data=issue.data,
                basis=issue.basis,
                sample_size=issue.sample_size,
                sample_fraction=issue.sample_fraction,
            )
        return issues


# =============================================================================
# EXECUTIVE SUMMARY GENERATOR
# =============================================================================

class ExecutiveSummaryGenerator:
    """
    Generates executive summary from issues.

    Selects top issues ensuring category diversity and produces
    flowing prose suitable for executive consumption.
    """

    def generate(
        self,
        issues: List[Issue],
        profile_data: Dict[str, Any],
        sampling_info: SamplingInfo
    ) -> Dict[str, Any]:
        """
        Generate executive summary.

        Args:
            issues: List of rendered issues
            profile_data: Full profile data dict
            sampling_info: Sampling configuration

        Returns:
            Dict with summary components:
                - intro: Opening paragraph
                - key_findings: Top 5 diverse findings
                - prose: Combined narrative text
        """
        # Get quality tier for intro
        quality_score = profile_data.get("overall_quality_score", 0) / 100.0
        tier = get_quality_tier(quality_score)
        tier_name = tier["name"]

        # Get intro template
        intro_template = EXECUTIVE_SUMMARY_INTRO.get(
            tier_name,
            EXECUTIVE_SUMMARY_INTRO["fair"]
        )
        intro = intro_template.format(score=quality_score)

        # Select top 5 issues with category diversity
        key_findings = self._select_diverse_issues(issues, max_count=5)

        # Build narrative prose
        prose = self._build_prose(intro, key_findings, sampling_info)

        return {
            "intro": intro,
            "key_findings": [
                {
                    "severity": f.severity.value,
                    "category": f.category,
                    "text": f.rendered_text,
                }
                for f in key_findings
            ],
            "prose": prose,
            "quality_tier": tier,
        }

    def _select_diverse_issues(
        self, issues: List[Issue], max_count: int = 5
    ) -> List[Issue]:
        """
        Select top issues ensuring no category appears twice.

        Args:
            issues: Sorted list of issues (by severity)
            max_count: Maximum issues to select

        Returns:
            List of diverse top issues
        """
        selected = []
        seen_categories = set()

        for issue in issues:
            # Skip info-level issues for executive summary
            if issue.severity == Severity.INFO:
                continue

            if issue.category not in seen_categories:
                selected.append(issue)
                seen_categories.add(issue.category)

                if len(selected) >= max_count:
                    break

        return selected

    def _build_prose(
        self,
        intro: str,
        findings: List[Issue],
        sampling_info: SamplingInfo
    ) -> str:
        """
        Build flowing prose from intro and findings.

        Args:
            intro: Introduction paragraph
            findings: Selected key findings
            sampling_info: Sampling configuration

        Returns:
            Combined narrative prose
        """
        sections = [intro]

        if findings:
            sections.append("\n\n**Key findings:**")
            for i, finding in enumerate(findings, 1):
                if finding.rendered_text:
                    sections.append(f"\n{i}. {finding.rendered_text}")

        # Add sampling note
        if sampling_info.sample_used:
            note = (
                f"\n\n*Note: Statistical analyses are based on a {sampling_info.sample_size:,}-row "
                f"sample (~{sampling_info.sample_fraction:.1%} of the dataset) "
                "for computational efficiency.*"
            )
            sections.append(note)

        return "".join(sections)


# =============================================================================
# DETAILED SECTIONS GENERATOR
# =============================================================================

class DetailedSectionsGenerator:
    """
    Generates detailed report sections grouped by category.

    Produces markdown-formatted sections for each issue category.
    """

    # Order of sections in report
    SECTION_ORDER = [
        "pii",
        "outliers",
        "authenticity",
        "label_quality",
        "temporal",
        "cross_column",
        "completeness",
        "validity",
        "ml_analysis",
    ]

    def generate(self, issues: List[Issue]) -> Dict[str, Dict[str, Any]]:
        """
        Generate detailed sections grouped by category.

        Args:
            issues: List of rendered issues

        Returns:
            Dict mapping category to section data:
                - header: Section title
                - issues: List of issues in this category
                - markdown: Formatted markdown content
        """
        # Group issues by category
        by_category: Dict[str, List[Issue]] = {}
        for issue in issues:
            if issue.category not in by_category:
                by_category[issue.category] = []
            by_category[issue.category].append(issue)

        # Build sections in order
        sections = {}
        for category in self.SECTION_ORDER:
            if category not in by_category:
                continue

            cat_issues = by_category[category]
            header = SECTION_HEADERS.get(category, category.replace("_", " ").title())

            # Build markdown
            md_lines = [f"### {header}\n"]
            for issue in cat_issues:
                severity_badge = f"[{issue.severity.value.upper()}]"
                if issue.rendered_text:
                    md_lines.append(f"- {severity_badge} {issue.rendered_text}")
                else:
                    md_lines.append(f"- {severity_badge} {issue.id}")

            sections[category] = {
                "header": header,
                "issues": [i.to_dict() for i in cat_issues],
                "markdown": "\n".join(md_lines),
            }

        return sections


# =============================================================================
# INSIGHT ENGINE - MAIN INTERFACE
# =============================================================================

class InsightEngine:
    """
    Main interface for generating insights from profile data.

    Orchestrates the rule engine, text generator, and summary generators
    to produce complete insight output.
    """

    def __init__(self, thresholds: Optional[InsightThresholds] = None):
        """
        Initialize insight engine.

        Args:
            thresholds: Custom detection thresholds (uses defaults if None)
        """
        self.rule_engine = RuleEngine(thresholds)
        self.text_generator = TextGenerator()
        self.summary_generator = ExecutiveSummaryGenerator()
        self.sections_generator = DetailedSectionsGenerator()

    def analyze(self, profile_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze profile data and generate complete insights.

        Args:
            profile_data: Dictionary representation of ProfileResult

        Returns:
            Dict containing:
                - issues: List of all issues with rendered text
                - sampling_info: Sampling configuration details
                - executive_summary: Executive summary components
                - detailed_sections: Category-grouped detailed findings
                - sampling_explanation: Standard sampling explanation text
        """
        # Run rule engine
        issues, sampling_info = self.rule_engine.analyze(profile_data)

        # Render issue text
        self.text_generator.render_issues(issues)

        # Generate executive summary
        executive_summary = self.summary_generator.generate(
            issues, profile_data, sampling_info
        )

        # Generate detailed sections
        detailed_sections = self.sections_generator.generate(issues)

        # Build sampling overview
        if sampling_info.sample_used:
            sampling_overview = get_sampling_overview_sampled(
                sample_size=sampling_info.sample_size,
                sample_fraction=sampling_info.sample_fraction,
            )
        else:
            sampling_overview = SAMPLING_OVERVIEW_FULL.format(
                total_rows=sampling_info.total_rows
            )

        return {
            "issues": [i.to_dict() for i in issues],
            "sampling_info": {
                "total_rows": sampling_info.total_rows,
                "sample_used": sampling_info.sample_used,
                "sample_size": sampling_info.sample_size,
                "sample_fraction": sampling_info.sample_fraction,
                "full_dataset_metrics": sampling_info.full_dataset_metrics,
                "sampled_metrics": sampling_info.sampled_metrics,
            },
            "executive_summary": executive_summary,
            "detailed_sections": detailed_sections,
            "sampling_explanation": get_sampling_explanation(sampling_info.sample_size),
            "sampling_overview": sampling_overview,
        }


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def generate_insights(
    profile_data: Dict[str, Any],
    thresholds: Optional[InsightThresholds] = None
) -> Dict[str, Any]:
    """
    Generate insights from profile data (convenience function).

    Args:
        profile_data: Dictionary representation of ProfileResult
        thresholds: Optional custom thresholds

    Returns:
        Complete insights dict
    """
    engine = InsightEngine(thresholds)
    return engine.analyze(profile_data)
