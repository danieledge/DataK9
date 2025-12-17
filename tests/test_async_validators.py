"""
Tests for async validators.

Author: Daniel Edge
"""

import pytest
import asyncio
import pandas as pd
from typing import AsyncIterator

from validation_framework.validations.async_validators import (
    AsyncSchemaMatchCheck,
    AsyncLookupCheck,
    AsyncRegexCheck
)
from validation_framework.validations.async_base import SyncValidatorAdapter
from validation_framework.validations.builtin.field_checks import MandatoryFieldCheck
from validation_framework.core.results import Severity
from validation_framework.core.async_orchestrator import AsyncValidationOrchestrator


class TestAsyncSchemaCheck:
    """Test async schema validation."""

    @pytest.mark.asyncio
    async def test_schema_match_success(self):
        """Test schema validation passes with matching schema."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "id": "integer",
                    "name": "string",
                    "value": "float"
                }
            }
        )

        context = {
            "columns": ["id", "name", "value"],
            "dtypes": {
                "id": "int64",
                "name": "object",
                "value": "float64"
            }
        }

        # Schema checks don't need data iterator
        async def empty_iter():
            return
            yield

        result = await validator.validate_async(empty_iter(), context)

        assert result.passed is True
        assert result.failed_count == 0

    @pytest.mark.asyncio
    async def test_schema_missing_column(self):
        """Test schema validation fails with missing column."""
        validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {
                    "id": "integer",
                    "name": "string",
                    "missing_col": "string"
                }
            }
        )

        context = {
            "columns": ["id", "name"],
            "dtypes": {
                "id": "int64",
                "name": "object"
            }
        }

        async def empty_iter():
            return
            yield

        result = await validator.validate_async(empty_iter(), context)

        assert result.passed is False
        assert "Missing columns" in result.message
        assert "missing_col" in result.message


class TestAsyncLookupCheck:
    """Test async lookup validation."""

    @pytest.mark.asyncio
    async def test_lookup_in_memory_allow(self):
        """Test lookup validation with in-memory reference list (allow)."""
        validator = AsyncLookupCheck(
            name="country_check",
            severity=Severity.ERROR,
            params={
                "field": "country",
                "reference_values": ["US", "UK", "CA"],
                "check_type": "allow",
                "description": "Valid countries"
            }
        )

        # Create test data
        df1 = pd.DataFrame({
            "country": ["US", "UK", "CA"]
        })
        df2 = pd.DataFrame({
            "country": ["US", "CA"]
        })

        async def data_iter():
            yield df1
            yield df2

        context = {"max_sample_failures": 100}

        result = await validator.validate_async(data_iter(), context)

        assert result.passed is True
        assert result.total_count == 5

    @pytest.mark.asyncio
    async def test_lookup_in_memory_deny(self):
        """Test lookup validation with deny list."""
        validator = AsyncLookupCheck(
            name="blocked_check",
            severity=Severity.WARNING,
            params={
                "field": "domain",
                "reference_values": ["spam.com", "blocked.net"],
                "check_type": "deny",
                "description": "Blocked domains"
            }
        )

        df = pd.DataFrame({
            "domain": ["valid.com", "spam.com", "good.org"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await validator.validate_async(data_iter(), context)

        assert result.passed is False
        assert result.failed_count == 1
        assert "spam.com" in result.message

    @pytest.mark.asyncio
    async def test_lookup_invalid_values(self):
        """Test lookup validation catches invalid values."""
        validator = AsyncLookupCheck(
            name="product_check",
            severity=Severity.ERROR,
            params={
                "field": "product_code",
                "reference_values": ["P001", "P002", "P003"],
                "check_type": "allow"
            }
        )

        df = pd.DataFrame({
            "product_code": ["P001", "P999", "P002", "INVALID"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await validator.validate_async(data_iter(), context)

        assert result.passed is False
        assert result.failed_count == 2


class TestAsyncRegexCheck:
    """Test async regex validation."""

    @pytest.mark.asyncio
    async def test_regex_email_valid(self):
        """Test regex validation for valid emails."""
        validator = AsyncRegexCheck(
            name="email_check",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "description": "Valid email format",
                "should_match": True
            }
        )

        df = pd.DataFrame({
            "email": ["user@example.com", "test@test.org", "admin@company.co.uk"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await validator.validate_async(data_iter(), context)

        assert result.passed is True
        assert result.total_count == 3

    @pytest.mark.asyncio
    async def test_regex_email_invalid(self):
        """Test regex validation catches invalid emails."""
        validator = AsyncRegexCheck(
            name="email_check",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "description": "Valid email format",
                "should_match": True
            }
        )

        df = pd.DataFrame({
            "email": ["user@example.com", "invalid", "bad@", "@test.com"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await validator.validate_async(data_iter(), context)

        assert result.passed is False
        assert result.failed_count == 3

    @pytest.mark.asyncio
    async def test_regex_should_not_match(self):
        """Test regex validation with should_match=False."""
        validator = AsyncRegexCheck(
            name="no_special_chars",
            severity=Severity.WARNING,
            params={
                "field": "username",
                "pattern": r"[^a-zA-Z0-9_]",
                "description": "No special characters",
                "should_match": False
            }
        )

        df = pd.DataFrame({
            "username": ["john_doe", "jane123", "bob!smith", "alice@test"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await validator.validate_async(data_iter(), context)

        assert result.passed is False
        assert result.failed_count == 2


class TestSyncValidatorAdapter:
    """Test sync-to-async validator adapter."""

    @pytest.mark.asyncio
    async def test_adapter_mandatory_field(self):
        """Test adapter with mandatory field check."""
        sync_validator = MandatoryFieldCheck(
            name="mandatory_check",
            severity=Severity.ERROR,
            params={
                "fields": ["id", "name"],
                "allow_whitespace": False
            }
        )

        adapter = SyncValidatorAdapter(sync_validator)

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await adapter.validate_async(data_iter(), context)

        assert result.passed is True
        # total_count is fields * rows = 2 * 3 = 6
        assert result.total_count == 6

    @pytest.mark.asyncio
    async def test_adapter_mandatory_field_fails(self):
        """Test adapter catches mandatory field violations."""
        sync_validator = MandatoryFieldCheck(
            name="mandatory_check",
            severity=Severity.ERROR,
            params={
                "fields": ["id", "name"],
                "allow_whitespace": False
            }
        )

        adapter = SyncValidatorAdapter(sync_validator)

        df = pd.DataFrame({
            "id": [1, 2, None],
            "name": ["Alice", "", "Charlie"]
        })

        async def data_iter():
            yield df

        context = {"max_sample_failures": 100}

        result = await adapter.validate_async(data_iter(), context)

        assert result.passed is False


class TestAsyncOrchestrator:
    """Test async validation orchestrator."""

    @pytest.mark.asyncio
    async def test_orchestrator_concurrent_execution(self):
        """Test orchestrator runs independent validators concurrently."""
        orchestrator = AsyncValidationOrchestrator(max_concurrency=3)

        # Add independent validators
        schema_validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={
                "expected_schema": {"id": "integer", "value": "float"}
            }
        )

        regex_validator = AsyncRegexCheck(
            name="regex_check",
            severity=Severity.ERROR,
            params={
                "field": "email",
                "pattern": r".+@.+",
                "should_match": True
            }
        )

        orchestrator.add_task(
            task_id="schema",
            validator=schema_validator,
            dependencies=set()
        )

        orchestrator.add_task(
            task_id="regex",
            validator=regex_validator,
            dependencies=set()
        )

        # Create data factory
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [1.1, 2.2, 3.3],
            "email": ["a@b.com", "c@d.org", "e@f.net"]
        })

        def data_factory():
            async def data_iter():
                yield df
            return data_iter()

        context = {
            "max_sample_failures": 100,
            "columns": ["id", "value", "email"],
            "dtypes": {"id": "int64", "value": "float64", "email": "object"}
        }

        results = await orchestrator.execute_all(data_factory, context)

        assert len(results) == 2
        assert "schema" in results
        assert "regex" in results
        assert results["schema"].passed is True
        assert results["regex"].passed is True

    @pytest.mark.asyncio
    async def test_orchestrator_with_dependencies(self):
        """Test orchestrator respects dependencies."""
        orchestrator = AsyncValidationOrchestrator(max_concurrency=2)

        schema_validator = AsyncSchemaMatchCheck(
            name="schema_check",
            severity=Severity.ERROR,
            params={"expected_schema": {"id": "integer"}}
        )

        lookup_validator = AsyncLookupCheck(
            name="lookup_check",
            severity=Severity.ERROR,
            params={
                "field": "id",
                "reference_values": ["1", "2", "3"],
                "check_type": "allow"
            }
        )

        # lookup depends on schema
        orchestrator.add_task(
            task_id="schema",
            validator=schema_validator,
            dependencies=set()
        )

        orchestrator.add_task(
            task_id="lookup",
            validator=lookup_validator,
            dependencies={"schema"}
        )

        df = pd.DataFrame({"id": [1, 2, 3]})

        def data_factory():
            async def data_iter():
                yield df
            return data_iter()

        context = {
            "max_sample_failures": 100,
            "columns": ["id"],
            "dtypes": {"id": "int64"}
        }

        results = await orchestrator.execute_all(data_factory, context)

        assert len(results) == 2
        assert results["schema"].passed is True
        assert results["lookup"].passed is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
