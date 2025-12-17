"""
Async base classes for validation rules.

Provides async validator infrastructure for non-blocking I/O operations,
particularly beneficial for database lookups, API calls, and file operations.

Author: Daniel Edge
"""

import asyncio
from abc import ABC, abstractmethod
from typing import AsyncIterator, Dict, Any, Optional, Coroutine
import pandas as pd
import logging

from validation_framework.core.results import ValidationResult, Severity
from validation_framework.core.exceptions import ConditionEvaluationError

logger = logging.getLogger(__name__)


class AsyncValidationRule(ABC):
    """
    Base class for async validation rules.

    Async validators enable non-blocking execution for I/O-bound validations,
    improving throughput when validating multiple files or performing external
    lookups (databases, APIs, etc.).

    Example:
        >>> class MyAsyncValidator(AsyncValidationRule):
        ...     async def validate_async(self, data_iterator, context):
        ...         total = 0
        ...         failures = []
        ...         async for chunk in data_iterator:
        ...             # Non-blocking processing
        ...             result = await self._check_external_api(chunk)
        ...             total += len(chunk)
        ...             failures.extend(result)
        ...         return self._create_result(
        ...             passed=len(failures) == 0,
        ...             message=f"Checked {total} rows",
        ...             failed_count=len(failures),
        ...             total_count=total
        ...         )
    """

    def __init__(
        self,
        name: str,
        severity: Severity,
        params: Optional[Dict[str, Any]] = None,
        condition: Optional[str] = None
    ):
        """
        Initialize async validation rule.

        Args:
            name: Name of the validation rule
            severity: Severity level (ERROR or WARNING)
            params: Dictionary of parameters for the validation
            condition: Optional conditional expression - validation only runs if condition is True
        """
        self.name = name
        self.severity = severity
        self.params = params or {}
        self.condition = condition
        self.description = self.get_description()

    @abstractmethod
    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Execute the validation rule asynchronously.

        Args:
            data_iterator: Async iterator yielding data chunks
            context: Validation context (file info, other files, etc.)

        Returns:
            ValidationResult object
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Get human-readable description of the validation rule."""
        pass

    async def _evaluate_condition_async(self, df: pd.DataFrame) -> pd.Series:
        """
        Evaluate the condition expression on a DataFrame asynchronously.

        Args:
            df: DataFrame to evaluate condition against

        Returns:
            Boolean Series indicating which rows match the condition

        Raises:
            ConditionEvaluationError: If condition cannot be evaluated
        """
        if not self.condition:
            return pd.Series([True] * len(df), index=df.index)

        try:
            query = self._convert_condition_syntax(self.condition)
            # Run eval in executor to avoid blocking
            loop = asyncio.get_event_loop()
            matching_mask = await loop.run_in_executor(None, df.eval, query)
            return matching_mask

        except Exception as e:
            error_msg = (
                f"Cannot evaluate condition '{self.condition}': {str(e)}. "
                f"Check that all referenced columns exist and the syntax is valid."
            )
            logger.error(error_msg)
            raise ConditionEvaluationError(
                message=error_msg,
                validation_name=self.name,
                condition=self.condition,
                original_exception=e
            )

    def _convert_condition_syntax(self, condition: str) -> str:
        """
        Convert SQL-like condition syntax to pandas query syntax.

        Args:
            condition: Condition string

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


class AsyncFileValidationRule(AsyncValidationRule):
    """
    Base class for async file-level validations.

    File-level validations don't need data content, they work with
    file metadata from context.
    """

    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        File-level validations don't need data iterator.
        They work with file metadata from context.
        """
        return await self.validate_file_async(context)

    @abstractmethod
    async def validate_file_async(self, context: Dict[str, Any]) -> ValidationResult:
        """Validate file-level properties asynchronously."""
        pass


class AsyncDataValidationRule(AsyncValidationRule):
    """
    Base class for async data content validations.

    Data validations process data chunks asynchronously, enabling
    concurrent processing and non-blocking I/O operations.
    """

    @abstractmethod
    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """Validate data content asynchronously."""
        pass


class SyncValidatorAdapter(AsyncValidationRule):
    """
    Adapter to run synchronous validators asynchronously.

    Wraps existing sync validators to work with async infrastructure,
    running them in thread pool executors to avoid blocking the event loop.

    Example:
        >>> from validation_framework.validations.builtin.schema_checks import SchemaMatchCheck
        >>> sync_validator = SchemaMatchCheck(
        ...     name="schema_check",
        ...     severity=Severity.ERROR,
        ...     params={"expected_schema": {"id": "integer"}}
        ... )
        >>> async_adapter = SyncValidatorAdapter(sync_validator)
        >>> result = await async_adapter.validate_async(data_iter, context)
    """

    def __init__(self, sync_validator):
        """
        Initialize adapter with a synchronous validator.

        Args:
            sync_validator: Synchronous ValidationRule instance
        """
        self.sync_validator = sync_validator
        super().__init__(
            name=sync_validator.name,
            severity=sync_validator.severity,
            params=sync_validator.params,
            condition=sync_validator.condition
        )

    def get_description(self) -> str:
        """Get description from wrapped validator."""
        return self.sync_validator.get_description()

    async def validate_async(
        self,
        data_iterator: AsyncIterator[pd.DataFrame],
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Run sync validator in thread pool executor.

        Args:
            data_iterator: Async iterator yielding data chunks
            context: Validation context

        Returns:
            ValidationResult from the sync validator
        """
        # Convert async iterator to sync list (load all chunks)
        chunks = []
        async for chunk in data_iterator:
            chunks.append(chunk)

        # Create sync iterator
        sync_iterator = iter(chunks)

        # Run sync validator in executor to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            self.sync_validator.validate,
            sync_iterator,
            context
        )

        return result


def create_async_validator(
    validator_instance,
    force_async: bool = False
) -> AsyncValidationRule:
    """
    Create async validator from sync or async validator instance.

    If the validator is already async, returns it as-is.
    If the validator is sync, wraps it in SyncValidatorAdapter.

    Args:
        validator_instance: ValidationRule or AsyncValidationRule instance
        force_async: If True, always wrap in adapter even if already async

    Returns:
        AsyncValidationRule instance

    Example:
        >>> from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck
        >>> sync_val = MandatoryFieldCheck(...)
        >>> async_val = create_async_validator(sync_val)
        >>> result = await async_val.validate_async(data_iter, context)
    """
    if isinstance(validator_instance, AsyncValidationRule) and not force_async:
        return validator_instance
    else:
        return SyncValidatorAdapter(validator_instance)
