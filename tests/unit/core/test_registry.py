"""
Unit tests for the validation registry.

Tests the registration, retrieval, and management of validation rules.
"""

import pytest
from validation_framework.core.registry import ValidationRegistry, get_registry, register_validation
from validation_framework.validations.base import ValidationRule, DataValidationRule
from validation_framework.core.results import ValidationResult, Severity
from typing import Iterator, Dict, Any
import pandas as pd


class MockValidation(DataValidationRule):
    """Mock validation for testing."""

    def get_description(self) -> str:
        return "Mock validation for testing"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        return self._create_result(passed=True, message="Mock passed")


class AnotherMockValidation(DataValidationRule):
    """Another mock validation for testing."""

    def get_description(self) -> str:
        return "Another mock validation"

    def validate(self, data_iterator: Iterator[pd.DataFrame], context: Dict[str, Any]) -> ValidationResult:
        return self._create_result(passed=False, message="Mock failed")


class NotAValidation:
    """Not a validation rule - for negative testing."""
    pass


@pytest.mark.unit
class TestValidationRegistry:
    """Tests for ValidationRegistry class."""

    def test_registry_initialization(self):
        """Test that a new registry initializes with empty rules."""
        registry = ValidationRegistry()
        assert len(registry.list_available()) == 0

    def test_register_validation_rule(self):
        """Test registering a validation rule."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)

        assert "MockValidation" in registry.list_available()
        assert len(registry.list_available()) == 1

    def test_register_multiple_rules(self):
        """Test registering multiple validation rules."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)
        registry.register("AnotherMockValidation", AnotherMockValidation)

        available = registry.list_available()
        assert len(available) == 2
        assert "MockValidation" in available
        assert "AnotherMockValidation" in available

    def test_register_invalid_type_raises_error(self):
        """Test that registering a non-ValidationRule class raises TypeError."""
        registry = ValidationRegistry()

        with pytest.raises(TypeError) as exc_info:
            registry.register("NotAValidation", NotAValidation)

        assert "must be a subclass of ValidationRule" in str(exc_info.value)

    def test_get_registered_rule(self):
        """Test retrieving a registered validation rule."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)

        retrieved_class = registry.get("MockValidation")
        assert retrieved_class == MockValidation

    def test_get_nonexistent_rule_raises_error(self):
        """Test that retrieving a non-existent rule raises KeyError."""
        registry = ValidationRegistry()

        with pytest.raises(KeyError) as exc_info:
            registry.get("NonExistent")

        assert "not found in registry" in str(exc_info.value)

    def test_is_registered(self):
        """Test checking if a rule is registered."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)

        assert registry.is_registered("MockValidation") is True
        assert registry.is_registered("NonExistent") is False

    def test_list_available_returns_list(self):
        """Test that list_available returns a list of strings."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)
        registry.register("AnotherMockValidation", AnotherMockValidation)

        available = registry.list_available()

        assert isinstance(available, list)
        assert all(isinstance(name, str) for name in available)

    def test_register_overwrites_existing(self):
        """Test that registering the same name twice overwrites."""
        registry = ValidationRegistry()
        registry.register("Test", MockValidation)
        registry.register("Test", AnotherMockValidation)

        retrieved = registry.get("Test")
        assert retrieved == AnotherMockValidation


@pytest.mark.unit
class TestGlobalRegistry:
    """Tests for global registry functions."""

    def test_get_registry_returns_singleton(self):
        """Test that get_registry returns the same instance."""
        registry1 = get_registry()
        registry2 = get_registry()

        assert registry1 is registry2

    def test_register_validation_function(self):
        """Test the register_validation convenience function."""
        # Note: This modifies the global registry, so we need to be careful
        # In a real scenario, you might want to reset the registry after each test

        initial_count = len(get_registry().list_available())

        # Register a new validation
        register_validation("TestMockValidation", MockValidation)

        # Check it was registered
        registry = get_registry()
        assert "TestMockValidation" in registry.list_available()
        assert len(registry.list_available()) == initial_count + 1

        # Verify we can retrieve it
        retrieved = registry.get("TestMockValidation")
        assert retrieved == MockValidation


@pytest.mark.unit
class TestRegisteredValidationInstantiation:
    """Tests for instantiating registered validations."""

    def test_instantiate_registered_validation(self):
        """Test that we can instantiate a registered validation."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)

        ValidationClass = registry.get("MockValidation")
        instance = ValidationClass(
            name="TestValidation",
            severity=Severity.ERROR,
            params={}
        )

        assert isinstance(instance, MockValidation)
        assert instance.name == "TestValidation"
        assert instance.severity == Severity.ERROR

    def test_instantiated_validation_works(self):
        """Test that an instantiated validation can be executed."""
        registry = ValidationRegistry()
        registry.register("MockValidation", MockValidation)

        ValidationClass = registry.get("MockValidation")
        instance = ValidationClass(
            name="TestValidation",
            severity=Severity.ERROR,
            params={}
        )

        # Create a mock data iterator
        df = pd.DataFrame({"a": [1, 2, 3]})
        data_iterator = iter([df])
        context = {}

        result = instance.validate(data_iterator, context)

        assert isinstance(result, ValidationResult)
        assert result.passed is True
        assert result.message == "Mock passed"
