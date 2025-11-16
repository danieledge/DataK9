"""
Conditional validation wrapper for if-then-else validation logic.

Allows executing different validations based on conditional expressions,
enabling complex business rules without custom code.

Author: daniel edge
"""

from typing import Iterator, Dict, Any, List
import pandas as pd
from validation_framework.validations.base import DataValidationRule, ValidationResult
from validation_framework.core.registry import ValidationRegistry
from validation_framework.core.results import Severity
import logging

logger = logging.getLogger(__name__)


class ConditionalValidation(DataValidationRule):
    """
    Conditional validation wrapper that executes validations based on conditions.

    Supports if-then-else logic:
    - If condition is True, execute then_validate rules
    - If condition is False, execute else_validate rules (optional)

    Configuration:
        params:
            condition (str): Boolean expression to evaluate (required)
            then_validate (list): List of validation configs to run if condition is True
            else_validate (list): List of validation configs to run if condition is False (optional)

    Example YAML - Simple conditional:
        - type: "ConditionalValidation"
          severity: "ERROR"
          params:
            condition: "account_type == 'BUSINESS'"
            then_validate:
              - type: "MandatoryFieldCheck"
                params:
                  fields: ["company_name", "tax_id"]

    Example YAML - If-else logic:
        - type: "ConditionalValidation"
          severity: "ERROR"
          params:
            condition: "customer_type == 'INDIVIDUAL'"
            then_validate:
              - type: "MandatoryFieldCheck"
                params:
                  fields: ["first_name", "last_name", "date_of_birth"]
            else_validate:
              - type: "MandatoryFieldCheck"
                params:
                  fields: ["company_name", "registration_number"]

    Example YAML - Complex nested:
        - type: "ConditionalValidation"
          severity: "ERROR"
          params:
            condition: "order_total > 1000"
            then_validate:
              - type: "MandatoryFieldCheck"
                params:
                  fields: ["manager_approval"]
              - type: "RegexCheck"
                params:
                  field: "manager_approval"
                  pattern: "^MGR[0-9]{6}$"
    """

    def __init__(self, name: str, severity: Severity, params: Dict[str, Any] = None, condition: str = None):
        """
        Initialize ConditionalValidation.

        Note: For ConditionalValidation, the condition should be specified in params["condition"]
        rather than as a direct parameter. This __init__ extracts it and sets self.condition
        so that the inherited _evaluate_condition() method works correctly.

        Args:
            name: Validation name
            severity: Severity level
            params: Parameters including 'condition', 'then_validate', and optionally 'else_validate'
            condition: Direct condition parameter (usually None for ConditionalValidation)
        """
        # Call parent __init__
        super().__init__(name, severity, params, condition)

        # Extract condition from params and set it to self.condition for _evaluate_condition() to use
        if params and "condition" in params and not self.condition:
            self.condition = params.get("condition")

    def get_description(self) -> str:
        """Get human-readable description."""
        condition = self.params.get("condition", "N/A")
        return f"Conditional validation: IF ({condition}) THEN execute validation rules"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Execute conditional validation logic.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult aggregating all sub-validation results
        """
        try:
            condition = self.params.get("condition")
            then_validate_configs = self.params.get("then_validate", [])
            else_validate_configs = self.params.get("else_validate", [])

            if not condition:
                return self._create_result(
                    passed=False,
                    message="ConditionalValidation requires 'condition' parameter",
                    failed_count=1,
                )

            if not then_validate_configs:
                return self._create_result(
                    passed=False,
                    message="ConditionalValidation requires 'then_validate' parameter with at least one validation",
                    failed_count=1,
                )

            # We need to split data into matching and non-matching rows
            # and run appropriate validations on each subset
            total_rows = 0
            matching_data = []
            non_matching_data = []

            # Collect all chunks and split by condition
            for chunk in data_iterator:
                total_rows += len(chunk)

                # Evaluate condition on chunk
                condition_mask = self._evaluate_condition(chunk)

                # Split chunk into matching and non-matching
                matching_rows = chunk[condition_mask]
                non_matching_rows = chunk[~condition_mask]

                if len(matching_rows) > 0:
                    matching_data.append(matching_rows)

                if len(non_matching_rows) > 0:
                    non_matching_data.append(non_matching_rows)

            # Execute then_validate rules on matching data
            then_results = []
            if matching_data:
                logger.debug(f"ConditionalValidation: {len(matching_data)} chunks match condition")
                then_results = self._execute_validations(
                    then_validate_configs,
                    matching_data,
                    context,
                    "then_validate"
                )

            # Execute else_validate rules on non-matching data (if specified)
            else_results = []
            if else_validate_configs and non_matching_data:
                logger.debug(f"ConditionalValidation: {len(non_matching_data)} chunks don't match condition")
                else_results = self._execute_validations(
                    else_validate_configs,
                    non_matching_data,
                    context,
                    "else_validate"
                )

            # Aggregate results
            all_results = then_results + else_results

            if not all_results:
                # No rows matched either condition
                return self._create_result(
                    passed=True,
                    message=f"Conditional validation completed (0 rows matched condition)",
                    total_count=total_rows,
                )

            # Check if any sub-validation failed
            failed_validations = [r for r in all_results if not r.passed]
            total_failures = sum(r.failed_count for r in failed_validations)

            if failed_validations:
                failed_names = [r.rule_name for r in failed_validations]
                return self._create_result(
                    passed=False,
                    message=f"Conditional validation failed: {len(failed_validations)} sub-validation(s) failed ({', '.join(failed_names)})",
                    failed_count=total_failures,
                    total_count=total_rows,
                )

            return self._create_result(
                passed=True,
                message=f"Conditional validation passed all {len(all_results)} sub-validation(s)",
                total_count=total_rows,
            )

        except Exception as e:
            logger.error(f"Error in ConditionalValidation: {str(e)}")
            return self._create_result(
                passed=False,
                message=f"Error in conditional validation: {str(e)}",
                failed_count=1,
            )

    def _execute_validations(
        self,
        validation_configs: List[Dict[str, Any]],
        data_chunks: List[pd.DataFrame],
        context: Dict[str, Any],
        branch_name: str
    ) -> List[ValidationResult]:
        """
        Execute a list of validation configurations on data chunks.

        Args:
            validation_configs: List of validation configuration dictionaries
            data_chunks: List of DataFrame chunks to validate
            context: Validation context
            branch_name: Name of branch (for logging: "then_validate" or "else_validate")

        Returns:
            List of ValidationResult objects
        """
        results = []
        from validation_framework.core.registry import get_registry
        registry = get_registry()

        for val_config in validation_configs:
            try:
                val_type = val_config.get("type")
                if not val_type:
                    logger.warning(f"Validation config missing 'type' in {branch_name}")
                    continue

                # Get validation class from registry
                validation_class = registry.get(val_type)

                # Extract severity - inherit from parent if not specified
                if "severity" in val_config:
                    severity_str = val_config["severity"]
                    if isinstance(severity_str, str):
                        severity = Severity[severity_str.upper()]
                    else:
                        severity = severity_str
                else:
                    # Inherit severity from parent ConditionalValidation
                    severity = self.severity

                # Instantiate nested validation
                nested_validation = validation_class(
                    name=val_type,
                    severity=severity,
                    params=val_config.get("params", {}),
                    condition=val_config.get("condition"),  # Nested validations can also have conditions!
                )

                # Create iterator from data chunks
                def chunk_iterator():
                    for chunk in data_chunks:
                        yield chunk

                # Execute nested validation
                result = nested_validation.validate(chunk_iterator(), context)
                results.append(result)

            except KeyError:
                logger.error(f"Validation type '{val_type}' not found in registry")
                # Create error result
                error_result = ValidationResult(
                    rule_name=val_type,
                    severity=self.severity,
                    passed=False,
                    message=f"Validation type '{val_type}' not found",
                    failed_count=1,
                    total_count=0,
                    sample_failures=[],
                )
                results.append(error_result)

            except Exception as e:
                logger.error(f"Error executing nested validation '{val_type}': {str(e)}")
                error_result = ValidationResult(
                    rule_name=val_type,
                    severity=self.severity,
                    passed=False,
                    message=f"Error: {str(e)}",
                    failed_count=1,
                    total_count=0,
                    sample_failures=[],
                )
                results.append(error_result)

        return results
