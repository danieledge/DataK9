"""
Async validation orchestrator for concurrent validation execution.

Manages concurrent execution of multiple async validators with dependency
resolution, concurrency limits, and error handling.

Author: Daniel Edge
"""

import asyncio
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
import logging

from validation_framework.validations.async_base import AsyncValidationRule
from validation_framework.core.results import ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationTask:
    """
    Represents a validation task with dependencies.

    Attributes:
        validator: The async validator to run
        task_id: Unique identifier for this task
        dependencies: Set of task_ids that must complete before this task
        timeout: Optional timeout in seconds
    """
    validator: AsyncValidationRule
    task_id: str
    dependencies: Set[str]
    timeout: Optional[float] = None


class AsyncValidationOrchestrator:
    """
    Orchestrates concurrent execution of async validators.

    Features:
    - Concurrent execution of independent validations
    - Dependency resolution for dependent validations
    - Concurrency limits to prevent resource exhaustion
    - Timeout support for long-running validations
    - Graceful error handling (one failure doesn't stop all)
    - Result aggregation

    Example:
        >>> orchestrator = AsyncValidationOrchestrator(max_concurrency=5)
        >>>
        >>> # Add independent validators
        >>> orchestrator.add_task(
        ...     task_id="schema_check",
        ...     validator=schema_validator,
        ...     dependencies=set()
        ... )
        >>> orchestrator.add_task(
        ...     task_id="data_check",
        ...     validator=data_validator,
        ...     dependencies={"schema_check"}  # Depends on schema check
        ... )
        >>>
        >>> # Execute all validations concurrently
        >>> results = await orchestrator.execute_all(data_iterator, context)
        >>> for task_id, result in results.items():
        ...     print(f"{task_id}: {result.passed}")
    """

    def __init__(
        self,
        max_concurrency: int = 10,
        default_timeout: Optional[float] = None
    ):
        """
        Initialize orchestrator.

        Args:
            max_concurrency: Maximum number of concurrent validations
            default_timeout: Default timeout in seconds for validations
        """
        self.max_concurrency = max_concurrency
        self.default_timeout = default_timeout
        self.tasks: Dict[str, ValidationTask] = {}
        self._semaphore: Optional[asyncio.Semaphore] = None

    def add_task(
        self,
        task_id: str,
        validator: AsyncValidationRule,
        dependencies: Optional[Set[str]] = None,
        timeout: Optional[float] = None
    ) -> None:
        """
        Add a validation task to the orchestrator.

        Args:
            task_id: Unique identifier for this task
            validator: Async validator to execute
            dependencies: Set of task_ids that must complete first
            timeout: Optional timeout in seconds (overrides default)

        Raises:
            ValueError: If task_id already exists
        """
        if task_id in self.tasks:
            raise ValueError(f"Task with id '{task_id}' already exists")

        self.tasks[task_id] = ValidationTask(
            validator=validator,
            task_id=task_id,
            dependencies=dependencies or set(),
            timeout=timeout or self.default_timeout
        )

        logger.debug(f"Added validation task: {task_id}, dependencies: {dependencies or set()}")

    def remove_task(self, task_id: str) -> None:
        """
        Remove a validation task.

        Args:
            task_id: Task identifier to remove

        Raises:
            KeyError: If task_id doesn't exist
        """
        if task_id not in self.tasks:
            raise KeyError(f"Task '{task_id}' not found")

        del self.tasks[task_id]
        logger.debug(f"Removed validation task: {task_id}")

    def clear_tasks(self) -> None:
        """Remove all validation tasks."""
        self.tasks.clear()
        logger.debug("Cleared all validation tasks")

    async def execute_all(
        self,
        data_iterator_factory,
        context: Dict[str, Any]
    ) -> Dict[str, ValidationResult]:
        """
        Execute all validation tasks concurrently with dependency resolution.

        Args:
            data_iterator_factory: Factory function that creates fresh data iterators
                                  Called as: data_iterator_factory() -> AsyncIterator
            context: Validation context passed to all validators

        Returns:
            Dictionary mapping task_id to ValidationResult

        Raises:
            ValueError: If circular dependencies are detected
        """
        if not self.tasks:
            logger.warning("No validation tasks to execute")
            return {}

        # Validate dependencies
        self._validate_dependencies()

        # Create semaphore for concurrency control
        self._semaphore = asyncio.Semaphore(self.max_concurrency)

        # Create dependency tracker
        completed: Set[str] = set()
        results: Dict[str, ValidationResult] = {}
        errors: Dict[str, Exception] = {}

        # Get execution order (topological sort)
        execution_order = self._topological_sort()

        logger.info(f"Executing {len(self.tasks)} validation tasks with max concurrency {self.max_concurrency}")
        logger.debug(f"Execution order: {execution_order}")

        # Execute tasks in order, respecting dependencies
        for task_id in execution_order:
            task = self.tasks[task_id]

            # Wait for dependencies
            while not task.dependencies.issubset(completed):
                await asyncio.sleep(0.1)  # Small delay before checking again

            # Execute task
            try:
                result = await self._execute_task(task, data_iterator_factory, context)
                results[task_id] = result
                completed.add(task_id)
                logger.debug(f"Task {task_id} completed: {result.passed}")

            except Exception as e:
                logger.error(f"Task {task_id} failed with error: {e}", exc_info=True)
                errors[task_id] = e
                completed.add(task_id)  # Mark as completed to unblock dependents

                # Create error result
                from validation_framework.core.results import Severity
                results[task_id] = ValidationResult(
                    rule_name=task.validator.name,
                    severity=task.validator.severity,
                    passed=False,
                    message=f"Validation failed with error: {str(e)}",
                    failed_count=1,
                    total_count=0
                )

        logger.info(f"Completed {len(completed)} validation tasks")
        if errors:
            logger.warning(f"{len(errors)} tasks failed with errors")

        return results

    async def execute_concurrent(
        self,
        task_ids: List[str],
        data_iterator_factory,
        context: Dict[str, Any]
    ) -> Dict[str, ValidationResult]:
        """
        Execute specific tasks concurrently (ignoring dependencies).

        Useful for running known-independent validations in parallel.

        Args:
            task_ids: List of task identifiers to execute
            data_iterator_factory: Factory function for data iterators
            context: Validation context

        Returns:
            Dictionary mapping task_id to ValidationResult
        """
        if not task_ids:
            return {}

        self._semaphore = asyncio.Semaphore(self.max_concurrency)

        tasks_to_run = []
        for task_id in task_ids:
            if task_id not in self.tasks:
                logger.warning(f"Task {task_id} not found, skipping")
                continue
            task = self.tasks[task_id]
            tasks_to_run.append(self._execute_task(task, data_iterator_factory, context))

        logger.info(f"Executing {len(tasks_to_run)} tasks concurrently")

        # Execute all tasks concurrently
        results_list = await asyncio.gather(*tasks_to_run, return_exceptions=True)

        # Map results back to task_ids
        results = {}
        for i, task_id in enumerate(task_ids):
            if task_id not in self.tasks:
                continue

            result = results_list[i]
            if isinstance(result, Exception):
                logger.error(f"Task {task_id} failed: {result}", exc_info=True)
                from validation_framework.core.results import Severity
                results[task_id] = ValidationResult(
                    rule_name=self.tasks[task_id].validator.name,
                    severity=self.tasks[task_id].validator.severity,
                    passed=False,
                    message=f"Validation failed: {str(result)}",
                    failed_count=1,
                    total_count=0
                )
            else:
                results[task_id] = result

        return results

    async def _execute_task(
        self,
        task: ValidationTask,
        data_iterator_factory,
        context: Dict[str, Any]
    ) -> ValidationResult:
        """
        Execute a single validation task with concurrency control.

        Args:
            task: Validation task to execute
            data_iterator_factory: Factory for data iterators
            context: Validation context

        Returns:
            ValidationResult

        Raises:
            asyncio.TimeoutError: If task exceeds timeout
        """
        async with self._semaphore:
            logger.debug(f"Starting task: {task.task_id}")

            # Create fresh data iterator for this task
            data_iterator = data_iterator_factory()

            # Execute with optional timeout
            if task.timeout:
                try:
                    result = await asyncio.wait_for(
                        task.validator.validate_async(data_iterator, context),
                        timeout=task.timeout
                    )
                except asyncio.TimeoutError:
                    logger.error(f"Task {task.task_id} exceeded timeout of {task.timeout}s")
                    raise
            else:
                result = await task.validator.validate_async(data_iterator, context)

            return result

    def _validate_dependencies(self) -> None:
        """
        Validate that all dependencies exist and there are no circular dependencies.

        Raises:
            ValueError: If dependencies are invalid
        """
        # Check all dependencies exist
        all_task_ids = set(self.tasks.keys())
        for task_id, task in self.tasks.items():
            missing = task.dependencies - all_task_ids
            if missing:
                raise ValueError(
                    f"Task '{task_id}' has invalid dependencies: {missing}"
                )

        # Check for circular dependencies
        try:
            self._topological_sort()
        except ValueError as e:
            raise ValueError(f"Circular dependency detected: {e}")

    def _topological_sort(self) -> List[str]:
        """
        Topological sort of tasks based on dependencies.

        Returns:
            List of task_ids in execution order

        Raises:
            ValueError: If circular dependency is detected
        """
        # Kahn's algorithm
        in_degree = {task_id: 0 for task_id in self.tasks}
        adj_list = {task_id: [] for task_id in self.tasks}

        # Build adjacency list and in-degree count
        for task_id, task in self.tasks.items():
            for dep in task.dependencies:
                adj_list[dep].append(task_id)
                in_degree[task_id] += 1

        # Start with nodes with no dependencies
        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            # Process node
            current = queue.pop(0)
            result.append(current)

            # Reduce in-degree for neighbors
            for neighbor in adj_list[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # Check if all nodes were processed
        if len(result) != len(self.tasks):
            raise ValueError("Circular dependency detected")

        return result

    def get_independent_tasks(self) -> List[str]:
        """
        Get list of tasks with no dependencies.

        These can be run immediately without waiting.

        Returns:
            List of task_ids with no dependencies
        """
        return [
            task_id
            for task_id, task in self.tasks.items()
            if not task.dependencies
        ]

    def get_task_info(self, task_id: str) -> Dict[str, Any]:
        """
        Get information about a task.

        Args:
            task_id: Task identifier

        Returns:
            Dictionary with task information

        Raises:
            KeyError: If task_id doesn't exist
        """
        if task_id not in self.tasks:
            raise KeyError(f"Task '{task_id}' not found")

        task = self.tasks[task_id]
        return {
            "task_id": task.task_id,
            "validator_name": task.validator.name,
            "dependencies": list(task.dependencies),
            "timeout": task.timeout,
            "description": task.validator.description
        }

    def __len__(self) -> int:
        """Get number of tasks."""
        return len(self.tasks)

    def __contains__(self, task_id: str) -> bool:
        """Check if task exists."""
        return task_id in self.tasks
