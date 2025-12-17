"""
Example: Using Async Validators for Concurrent Validation

This example demonstrates how to use async validators to improve
validation performance through concurrent execution.

Author: Daniel Edge
"""

import asyncio
import pandas as pd
from pathlib import Path

from validation_framework.validations.async_validators import (
    AsyncSchemaMatchCheck,
    AsyncLookupCheck,
    AsyncRegexCheck
)
from validation_framework.validations.async_base import SyncValidatorAdapter
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck
from validation_framework.core.results import Severity
from validation_framework.core.async_orchestrator import AsyncValidationOrchestrator


async def basic_async_validation_example():
    """Basic example: Running a single async validator."""
    print("\n=== Basic Async Validation ===")

    # Create async schema validator
    schema_validator = AsyncSchemaMatchCheck(
        name="schema_check",
        severity=Severity.ERROR,
        params={
            "expected_schema": {
                "customer_id": "integer",
                "email": "string",
                "signup_date": "date"
            },
            "strict": False
        }
    )

    # File metadata context
    context = {
        "columns": ["customer_id", "email", "signup_date", "extra_field"],
        "dtypes": {
            "customer_id": "int64",
            "email": "object",
            "signup_date": "datetime64[ns]"
        }
    }

    # Schema validators don't need data iterator
    async def empty_iter():
        return
        yield

    result = await schema_validator.validate_async(empty_iter(), context)

    print(f"Schema validation: {'PASSED' if result.passed else 'FAILED'}")
    print(f"Message: {result.message}")


async def async_lookup_validation_example():
    """Example: Async lookup validation with reference data."""
    print("\n=== Async Lookup Validation ===")

    # Create lookup validator
    lookup_validator = AsyncLookupCheck(
        name="country_check",
        severity=Severity.ERROR,
        params={
            "field": "country_code",
            "reference_values": ["US", "UK", "CA", "AU", "DE", "FR"],
            "check_type": "allow",
            "description": "Valid country codes"
        }
    )

    # Sample data
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "country_code": ["US", "UK", "INVALID", "CA", "XX"]
    })

    async def data_iter():
        yield df

    context = {"max_sample_failures": 10}

    result = await lookup_validator.validate_async(data_iter(), context)

    print(f"Lookup validation: {'PASSED' if result.passed else 'FAILED'}")
    print(f"Message: {result.message}")
    if not result.passed:
        print(f"Sample failures:")
        for failure in result.sample_failures[:3]:
            print(f"  Row {failure['row']}: {failure['value']} - {failure['message']}")


async def async_regex_validation_example():
    """Example: Async regex validation for email addresses."""
    print("\n=== Async Regex Validation ===")

    # Create regex validator for email
    regex_validator = AsyncRegexCheck(
        name="email_check",
        severity=Severity.ERROR,
        params={
            "field": "email",
            "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "description": "Valid email format",
            "should_match": True
        }
    )

    # Sample data with some invalid emails
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "email": [
            "john@example.com",
            "invalid-email",
            "alice@company.org",
            "bad@",
            "bob@test.co.uk"
        ]
    })

    async def data_iter():
        yield df

    context = {"max_sample_failures": 10}

    result = await regex_validator.validate_async(data_iter(), context)

    print(f"Email validation: {'PASSED' if result.passed else 'FAILED'}")
    print(f"Message: {result.message}")
    print(f"Failed: {result.failed_count}/{result.total_count}")


async def sync_adapter_example():
    """Example: Using sync validators with async infrastructure."""
    print("\n=== Sync Validator Adapter ===")

    # Create sync validator (MandatoryFieldCheck)
    sync_validator = MandatoryFieldCheck(
        name="mandatory_check",
        severity=Severity.ERROR,
        params={
            "fields": ["customer_id", "email"],
            "allow_whitespace": False
        }
    )

    # Wrap in async adapter
    async_validator = SyncValidatorAdapter(sync_validator)

    # Sample data
    df = pd.DataFrame({
        "customer_id": [1, None, 3],
        "email": ["a@b.com", "c@d.com", ""]
    })

    async def data_iter():
        yield df

    context = {"max_sample_failures": 10}

    result = await async_validator.validate_async(data_iter(), context)

    print(f"Mandatory field check: {'PASSED' if result.passed else 'FAILED'}")
    print(f"Message: {result.message}")


async def orchestrator_concurrent_example():
    """Example: Using orchestrator for concurrent validation."""
    print("\n=== Orchestrator: Concurrent Execution ===")

    # Create orchestrator with max 3 concurrent validations
    orchestrator = AsyncValidationOrchestrator(max_concurrency=3)

    # Create multiple validators
    schema_validator = AsyncSchemaMatchCheck(
        name="schema_check",
        severity=Severity.ERROR,
        params={
            "expected_schema": {
                "id": "integer",
                "email": "string",
                "country": "string"
            }
        }
    )

    email_validator = AsyncRegexCheck(
        name="email_check",
        severity=Severity.ERROR,
        params={
            "field": "email",
            "pattern": r".+@.+\..+",
            "should_match": True
        }
    )

    country_validator = AsyncLookupCheck(
        name="country_check",
        severity=Severity.WARNING,
        params={
            "field": "country",
            "reference_values": ["US", "UK", "CA"],
            "check_type": "allow"
        }
    )

    # Add independent tasks (can run concurrently)
    orchestrator.add_task(
        task_id="schema",
        validator=schema_validator,
        dependencies=set()
    )

    orchestrator.add_task(
        task_id="email",
        validator=email_validator,
        dependencies=set()  # Independent
    )

    orchestrator.add_task(
        task_id="country",
        validator=country_validator,
        dependencies=set()  # Independent
    )

    # Sample data
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "email": ["user@example.com", "test@test.org", "admin@company.com"],
        "country": ["US", "UK", "DE"]
    })

    def data_factory():
        async def data_iter():
            yield df
        return data_iter()

    context = {
        "max_sample_failures": 10,
        "columns": ["id", "email", "country"],
        "dtypes": {"id": "int64", "email": "object", "country": "object"}
    }

    # Execute all validations concurrently
    results = await orchestrator.execute_all(data_factory, context)

    print(f"Executed {len(results)} validations concurrently:")
    for task_id, result in results.items():
        status = "PASSED" if result.passed else "FAILED"
        print(f"  {task_id}: {status} - {result.message}")


async def orchestrator_dependency_example():
    """Example: Using orchestrator with dependencies."""
    print("\n=== Orchestrator: With Dependencies ===")

    orchestrator = AsyncValidationOrchestrator(max_concurrency=3)

    # Schema check must run first
    schema_validator = AsyncSchemaMatchCheck(
        name="schema_check",
        severity=Severity.ERROR,
        params={
            "expected_schema": {
                "product_id": "integer",
                "product_code": "string"
            }
        }
    )

    # These depend on schema being valid
    lookup_validator = AsyncLookupCheck(
        name="product_lookup",
        severity=Severity.ERROR,
        params={
            "field": "product_code",
            "reference_values": ["PROD-001", "PROD-002", "PROD-003"],
            "check_type": "allow"
        }
    )

    regex_validator = AsyncRegexCheck(
        name="code_format",
        severity=Severity.WARNING,
        params={
            "field": "product_code",
            "pattern": r"^PROD-\d{3}$",
            "should_match": True
        }
    )

    # Add tasks with dependencies
    orchestrator.add_task(
        task_id="schema",
        validator=schema_validator,
        dependencies=set()
    )

    orchestrator.add_task(
        task_id="lookup",
        validator=lookup_validator,
        dependencies={"schema"}  # Wait for schema
    )

    orchestrator.add_task(
        task_id="format",
        validator=regex_validator,
        dependencies={"schema"}  # Wait for schema
    )

    df = pd.DataFrame({
        "product_id": [1, 2, 3],
        "product_code": ["PROD-001", "PROD-002", "PROD-999"]
    })

    def data_factory():
        async def data_iter():
            yield df
        return data_iter()

    context = {
        "max_sample_failures": 10,
        "columns": ["product_id", "product_code"],
        "dtypes": {"product_id": "int64", "product_code": "object"}
    }

    results = await orchestrator.execute_all(data_factory, context)

    print(f"Executed {len(results)} validations with dependencies:")
    print("  Execution order: schema -> (lookup, format)")
    for task_id, result in results.items():
        status = "PASSED" if result.passed else "FAILED"
        print(f"  {task_id}: {status} - {result.message}")


async def performance_comparison_example():
    """Example: Compare sync vs async performance."""
    print("\n=== Performance Comparison ===")

    import time

    # Create 5 validators
    validators = [
        AsyncRegexCheck(
            name=f"check_{i}",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r".+@.+",
                "should_match": True
            }
        )
        for i in range(5)
    ]

    df = pd.DataFrame({
        "email": [f"user{i}@example.com" for i in range(1000)]
    })

    def data_factory():
        async def data_iter():
            yield df
        return data_iter()

    context = {"max_sample_failures": 10}

    # Async concurrent execution
    orchestrator = AsyncValidationOrchestrator(max_concurrency=5)
    for i, validator in enumerate(validators):
        orchestrator.add_task(f"check_{i}", validator, dependencies=set())

    start = time.time()
    results = await orchestrator.execute_all(data_factory, context)
    async_time = time.time() - start

    print(f"Concurrent execution (5 validators): {async_time:.3f}s")
    print(f"All validations passed: {all(r.passed for r in results.values())}")


async def main():
    """Run all examples."""
    print("=" * 60)
    print("DataK9 Async Validators Examples")
    print("=" * 60)

    await basic_async_validation_example()
    await async_lookup_validation_example()
    await async_regex_validation_example()
    await sync_adapter_example()
    await orchestrator_concurrent_example()
    await orchestrator_dependency_example()
    await performance_comparison_example()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
