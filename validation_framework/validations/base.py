"""Base classes for validation rules."""

from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, Optional
import pandas as pd
from validation_framework.core.results import ValidationResult, Severity
import logging

logger = logging.getLogger(__name__)


class ValidationRule(ABC):
    """Base class for all validation rules."""

    def __init__(self, name: str, severity: Severity, params: Optional[Dict[str, Any]] = None, condition: Optional[str] = None):
        """
        Initialize validation rule.

        Args:
            name: Name of the validation rule
            severity: Severity level (ERROR or WARNING)
            params: Dictionary of parameters for the validation
            condition: Optional conditional expression - validation only runs if condition is True
                      Uses pandas query syntax (e.g., "age >= 18", "status == 'ACTIVE'")
        """
        self.name = name
        self.severity = severity
        self.params = params or {}
        self.condition = condition
        self.description = self.get_description()

    @abstractmethod
    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        Execute the validation rule.

        Args:
            data_iterator: Iterator yielding data chunks
            context: Validation context (file info, other files, etc.)

        Returns:
            ValidationResult object
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Get human-readable description of the validation rule."""
        pass

    def _evaluate_condition(self, df: pd.DataFrame) -> pd.Series:
        """
        Evaluate the condition expression on a DataFrame.

        Args:
            df: DataFrame to evaluate condition against

        Returns:
            Boolean Series indicating which rows match the condition
            If no condition is set, returns all True (run validation on all rows)
        """
        if not self.condition:
            # No condition - validation applies to all rows
            return pd.Series([True] * len(df), index=df.index)

        try:
            # Convert SQL-like syntax to pandas query syntax
            query = self._convert_condition_syntax(self.condition)

            # Evaluate using pandas query
            matching_mask = df.eval(query)
            return matching_mask

        except Exception as e:
            logger.warning(f"Error evaluating condition '{self.condition}': {str(e)}. Running validation on all rows.")
            return pd.Series([True] * len(df), index=df.index)

    def _convert_condition_syntax(self, condition: str) -> str:
        """
        Convert SQL-like condition syntax to pandas query syntax.

        Args:
            condition: Condition string (e.g., "age >= 18 AND status == 'ACTIVE'")

        Returns:
            Pandas-compatible query string
        """
        query = condition
        query = query.replace(" AND ", " & ")
        query = query.replace(" and ", " & ")
        query = query.replace(" OR ", " | ")
        query = query.replace(" or ", " | ")
        query = query.replace(" NOT ", " ~ ")
        query = query.replace(" not ", " ~ ")
        return query

    def _create_result(
        self,
        passed: bool,
        message: str,
        failed_count: int = 0,
        total_count: int = 0,
        sample_failures: list = None,
    ) -> ValidationResult:
        """
        Helper method to create a ValidationResult.

        Args:
            passed: Whether the validation passed
            message: Result message
            failed_count: Number of failures
            total_count: Total number of records checked
            sample_failures: Sample of failed records

        Returns:
            ValidationResult object
        """
        return ValidationResult(
            rule_name=self.name,
            severity=self.severity,
            passed=passed,
            message=message,
            failed_count=failed_count,
            total_count=total_count,
            sample_failures=sample_failures or [],
        )


class FileValidationRule(ValidationRule):
    """Base class for file-level validations (not data content)."""

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """
        File-level validations don't need data iterator.
        They work with file metadata from context.
        """
        return self.validate_file(context)

    @abstractmethod
    def validate_file(self, context: Dict[str, Any]) -> ValidationResult:
        """Validate file-level properties."""
        pass


class DataValidationRule(ValidationRule):
    """Base class for data content validations."""

    @abstractmethod
    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        """Validate data content."""
        pass
