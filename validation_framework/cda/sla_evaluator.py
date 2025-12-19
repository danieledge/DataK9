"""
SLA Evaluator for CDA Compliance.

Evaluates SLA compliance for Critical Data Attributes based on validation results.
Uses explicit field coverage - no heuristics or inference.

Author: Daniel Edge
"""

from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import logging

from .sla_models import (
    SLADefinition, SLAResult, SLAReport, SLAStatus,
    DEFAULT_TIERS, DEFAULT_WARNING_AT
)
from ..core.results import ValidationResult


class SLAEvaluator:
    """
    Evaluates SLA compliance for CDA fields.

    Uses explicit field coverage from validators - no guessing or heuristics.
    Aggregates failures using union of row IDs when available, otherwise max rate.
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def evaluate(
        self,
        file_name: str,
        dataset_row_count: int,
        cdas: List[Dict[str, Any]],
        validation_results: List[ValidationResult],
        sla_defaults: Optional[Dict[str, Any]] = None
    ) -> SLAReport:
        """
        Evaluate SLA compliance for all CDAs in a file.

        Args:
            file_name: Name of the file being evaluated
            dataset_row_count: Total rows in dataset (for union calculation)
            cdas: CDA definitions from config
            validation_results: Results from validation execution
            sla_defaults: Global SLA settings (warning_at, default_tier, tiers)

        Returns:
            SLAReport with status for each CDA field
        """
        sla_defaults = sla_defaults or {}
        results = []

        for cda_config in cdas:
            field_name = cda_config.get('field')
            if not field_name:
                self.logger.warning(f"CDA config missing 'field': {cda_config}")
                continue

            # Parse SLA definition from config
            try:
                sla_def = SLADefinition.from_config(cda_config, sla_defaults)
            except ValueError as e:
                self.logger.error(f"Invalid SLA config for field '{field_name}': {e}")
                continue

            # Find validations that explicitly cover this field
            field_validations = [
                v for v in validation_results
                if field_name in getattr(v, 'covered_fields', [])
            ]

            if not field_validations:
                self.logger.debug(f"No validations cover field '{field_name}' - SLA not evaluated")
                results.append(SLAResult(
                    field=field_name,
                    status=SLAStatus.NOT_EVALUATED,
                    tier_name=sla_def.tier_name or "unknown",
                    tolerance=sla_def.tolerance,
                    failure_rate=0.0,
                    bad_records=0,
                    evaluated_records=0,
                    aggregation_method="none",
                    contributing_validations=[]
                ))
                continue

            # Aggregate failures across validations
            bad_records, evaluated_records, method = self._aggregate_failures(
                field_validations, dataset_row_count
            )

            # Calculate failure rate
            if evaluated_records == 0:
                failure_rate = 0.0
            else:
                failure_rate = bad_records / evaluated_records

            # Determine traffic light status
            status = self._evaluate_status(failure_rate, sla_def)

            self.logger.debug(
                f"SLA for '{field_name}': {status.value} "
                f"(rate={failure_rate:.4%}, tolerance={sla_def.tolerance:.4%})"
            )

            results.append(SLAResult(
                field=field_name,
                status=status,
                tier_name=sla_def.tier_name or "custom",
                tolerance=sla_def.tolerance,
                failure_rate=failure_rate,
                bad_records=bad_records,
                evaluated_records=evaluated_records,
                aggregation_method=method,
                contributing_validations=[v.rule_name for v in field_validations]
            ))

        report = SLAReport(
            file_name=file_name,
            dataset_row_count=dataset_row_count,
            results=results,
            timestamp=datetime.now()
        )

        self.logger.info(f"SLA evaluation complete: {report.summary}")

        return report

    def _aggregate_failures(
        self,
        validations: List[ValidationResult],
        dataset_row_count: int
    ) -> tuple:
        """
        Aggregate failures across multiple validations for a single field.

        Strategy:
        1. If ANY validation has failed_row_ids → union all row IDs (accurate)
        2. Otherwise → use max failure rate (conservative estimate)

        Args:
            validations: List of validation results covering this field
            dataset_row_count: Total rows in the dataset

        Returns:
            Tuple of (bad_records, evaluated_records, method)
        """
        if not validations:
            return 0, 0, "none"

        # Check if any validation has row IDs for accurate union
        has_row_ids = any(
            getattr(v, 'failed_row_ids', None) is not None
            for v in validations
        )

        if has_row_ids:
            # Union of all failed row IDs - most accurate method
            all_failed_rows: Set[int] = set()
            for v in validations:
                row_ids = getattr(v, 'failed_row_ids', None)
                if row_ids:
                    all_failed_rows.update(row_ids)

            return len(all_failed_rows), dataset_row_count, "union"

        else:
            # Max failure rate - conservative fallback
            # Use the validation with highest failure rate as representative
            worst = max(
                validations,
                key=lambda v: v.failed_count / max(v.total_count, 1)
            )
            return worst.failed_count, worst.total_count, "max_rate"

    def _evaluate_status(
        self,
        failure_rate: float,
        sla_def: SLADefinition
    ) -> SLAStatus:
        """
        Determine traffic light status based on failure rate and tolerance.

        Args:
            failure_rate: Observed failure rate (0.0 to 1.0)
            sla_def: SLA definition with tolerance and warning threshold

        Returns:
            SLAStatus (GREEN, AMBER, or RED)
        """
        # Special case: zero tolerance (critical tier)
        if sla_def.tolerance == 0:
            return SLAStatus.GREEN if failure_rate == 0 else SLAStatus.RED

        # Normal case: check against thresholds
        if failure_rate >= sla_def.tolerance:
            return SLAStatus.RED
        elif failure_rate >= sla_def.warning_threshold:
            return SLAStatus.AMBER
        else:
            return SLAStatus.GREEN


def format_sla_cli_output(report: SLAReport) -> str:
    """
    Format SLA report for CLI output.

    Args:
        report: SLAReport to format

    Returns:
        Formatted string for terminal display
    """
    lines = []
    lines.append("=" * 80)
    lines.append(f"SLA COMPLIANCE — {report.file_name}")
    lines.append("=" * 80)
    lines.append("")

    if not report.results:
        lines.append("No CDAs defined for SLA evaluation.")
        lines.append("")
        return "\n".join(lines)

    # Header
    lines.append(f"{'Field':<20} {'Tier':<10} {'Tolerance':<10} {'Actual':<10} {'Status':<12}")
    lines.append("-" * 62)

    # Status icons
    status_icons = {
        SLAStatus.GREEN: "🟢 GREEN",
        SLAStatus.AMBER: "🟡 AMBER",
        SLAStatus.RED: "🔴 RED",
        SLAStatus.NOT_EVALUATED: "⚪ N/A",
    }

    for result in report.results:
        status_str = status_icons.get(result.status, str(result.status.value))
        lines.append(
            f"{result.field:<20} "
            f"{result.tier_name:<10} "
            f"{result.tolerance:.2%}     "
            f"{result.failure_rate:.2%}     "
            f"{status_str}"
        )

    lines.append("-" * 62)
    lines.append(f"Summary: {report.summary}")
    lines.append("")
    lines.append("=" * 80)

    return "\n".join(lines)
