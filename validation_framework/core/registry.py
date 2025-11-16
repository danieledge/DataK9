"""Registry for managing validation rules."""

from typing import Dict, Type, List
from validation_framework.validations.base import ValidationRule


class ValidationRegistry:
    """Registry for validation rules."""

    def __init__(self) -> None:
        """Initialize the registry."""
        self._rules: Dict[str, Type[ValidationRule]] = {}

    def register(self, name: str, rule_class: Type[ValidationRule]) -> None:
        """
        Register a validation rule.

        Args:
            name: Name to register the rule under
            rule_class: The validation rule class
        """
        if not issubclass(rule_class, ValidationRule):
            raise TypeError(f"{rule_class} must be a subclass of ValidationRule")

        self._rules[name] = rule_class

    def get(self, name: str) -> Type[ValidationRule]:
        """
        Get a validation rule class by name.

        Args:
            name: Name of the rule

        Returns:
            The validation rule class

        Raises:
            KeyError: If rule not found
        """
        if name not in self._rules:
            raise KeyError(f"Validation rule '{name}' not found in registry")
        return self._rules[name]

    def list_available(self) -> List[str]:
        """
        Get list of all registered validation rules.

        Returns:
            List of rule names
        """
        return list(self._rules.keys())

    def is_registered(self, name: str) -> bool:
        """
        Check if a rule is registered.

        Args:
            name: Name of the rule

        Returns:
            True if registered, False otherwise
        """
        return name in self._rules


# Global registry instance
_global_registry = ValidationRegistry()


def get_registry() -> ValidationRegistry:
    """Get the global validation registry."""
    return _global_registry


def register_validation(name: str, rule_class: Type[ValidationRule]) -> None:
    """
    Register a validation rule in the global registry.

    Args:
        name: Name to register the rule under
        rule_class: The validation rule class
    """
    _global_registry.register(name, rule_class)
