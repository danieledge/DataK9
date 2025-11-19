#!/usr/bin/env python3
"""
Validation Definition Loader

Loads validation definitions from the single source of truth JSON file.
This replaces hardcoded metadata and enables automatic sync between
Python validator and JavaScript Studio UI.

Author: Daniel Edge
Date: November 15, 2025
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any


class ValidationDefinitionLoader:
    """
    Loads and provides access to validation definitions from JSON.

    This class serves as the single source of truth for validation metadata,
    ensuring consistency between the Python framework, JavaScript Studio,
    and documentation.

    Example:
        loader = ValidationDefinitionLoader()

        # Get all definitions
        all_defs = loader.get_all_definitions()

        # Get specific validation
        range_check = loader.get_definition('RangeCheck')

        # Get by category
        field_validations = loader.get_by_category('Field-Level')
    """

    def __init__(self, definitions_file: Optional[Path] = None):
        """
        Initialize the loader.

        Args:
            definitions_file: Path to validation_definitions.json
                             If None, uses default location relative to this file
        """
        if definitions_file is None:
            # Default to validation_definitions.json in project root
            project_root = Path(__file__).parent.parent.parent
            definitions_file = project_root / 'validation_definitions.json'

        self.definitions_file = Path(definitions_file)
        self._definitions: Dict[str, Dict[str, Any]] = {}
        self._metadata: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """
        Load validation definitions from JSON file.

        Raises:
            FileNotFoundError: If definitions file doesn't exist
            json.JSONDecodeError: If file contains invalid JSON
        """
        if not self.definitions_file.exists():
            raise FileNotFoundError(
                f"Validation definitions file not found: {self.definitions_file}"
            )

        with open(self.definitions_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Extract metadata
        self._metadata = data.get('_metadata', {})

        # Extract validation definitions (everything except $schema and _metadata)
        self._definitions = {
            k: v for k, v in data.items()
            if k not in ['$schema', '_metadata']
        }

    def get_all_definitions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all validation definitions.

        Returns:
            Dictionary mapping validation type names to their definitions
        """
        return self._definitions.copy()

    def get_definition(self, validation_type: str) -> Optional[Dict[str, Any]]:
        """
        Get definition for a specific validation type.

        Args:
            validation_type: Name of the validation (e.g., 'RangeCheck')

        Returns:
            Validation definition dict or None if not found
        """
        return self._definitions.get(validation_type)

    def get_by_category(self, category: str) -> Dict[str, Dict[str, Any]]:
        """
        Get all validations in a specific category.

        Args:
            category: Category name (e.g., 'Field-Level', 'Schema')

        Returns:
            Dictionary of validations in that category
        """
        return {
            name: defn for name, defn in self._definitions.items()
            if defn.get('category') == category
        }

    def get_categories(self) -> List[str]:
        """
        Get list of all unique categories.

        Returns:
            Sorted list of category names
        """
        categories = {defn.get('category') for defn in self._definitions.values()}
        return sorted(categories)

    def list_validation_types(self) -> List[str]:
        """
        Get sorted list of all validation type names.

        Returns:
            Sorted list of validation type names
        """
        return sorted(self._definitions.keys())

    def get_param_definitions(self, validation_type: str) -> List[Dict[str, Any]]:
        """
        Get parameter definitions for a validation type.

        Args:
            validation_type: Name of the validation

        Returns:
            List of parameter definitions or empty list if not found
        """
        defn = self.get_definition(validation_type)
        if defn:
            return defn.get('params', [])
        return []

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the definitions file.

        Returns:
            Metadata dictionary (version, last_updated, etc.)
        """
        return self._metadata.copy()

    def validate_params(self, validation_type: str, params: Dict[str, Any]) -> List[str]:
        """
        Validate that provided parameters match the definition.

        Args:
            validation_type: Name of the validation
            params: Parameter dictionary to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        param_defs = self.get_param_definitions(validation_type)

        if not param_defs:
            # No parameter definitions found
            return errors

        # Create lookup of param definitions by name
        param_def_map = {p['name']: p for p in param_defs}

        # Check required parameters are present
        for param_def in param_defs:
            param_name = param_def['name']
            if param_def.get('required', False):
                if param_name not in params or params[param_name] is None:
                    errors.append(
                        f"Required parameter '{param_name}' is missing or null"
                    )

        # Validate parameter types (basic validation)
        for param_name, param_value in params.items():
            if param_name in param_def_map:
                param_def = param_def_map[param_name]
                param_type = param_def.get('type')

                # Type-specific validation
                if param_type == 'number' and param_value is not None:
                    try:
                        float(param_value)
                    except (ValueError, TypeError):
                        errors.append(
                            f"Parameter '{param_name}' must be numeric"
                        )

                elif param_type == 'checkbox' and param_value is not None:
                    if not isinstance(param_value, bool):
                        errors.append(
                            f"Parameter '{param_name}' must be boolean"
                        )

        return errors

    def get_validation_count(self) -> int:
        """
        Get total number of validation definitions.

        Returns:
            Number of validation definitions
        """
        return len(self._definitions)

    def get_source_compatibility(self, validation_type: str) -> Dict[str, Any]:
        """
        Get source compatibility information for a validation.

        Args:
            validation_type: Name of the validation

        Returns:
            Source compatibility dictionary or empty dict if not found
        """
        defn = self.get_definition(validation_type)
        if defn:
            return defn.get('source_compatibility', {})
        return {}

    def is_compatible_with(self, validation_type: str, source_type: str) -> bool:
        """
        Check if a validation is compatible with a specific source type.

        Args:
            validation_type: Name of the validation
            source_type: 'file' or 'database'

        Returns:
            True if compatible, False otherwise
        """
        compat = self.get_source_compatibility(validation_type)
        return compat.get(source_type, False)

    def get_by_source_compatibility(
        self,
        source_type: str,
        include_partial: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get validations compatible with a specific source type.

        Args:
            source_type: 'file' or 'database'
            include_partial: If True, includes validations that work but aren't optimized

        Returns:
            Dictionary of compatible validations
        """
        if source_type not in ['file', 'database']:
            raise ValueError(f"Invalid source_type: {source_type}. Must be 'file' or 'database'")

        compatible = {}

        for name, defn in self._definitions.items():
            compat = defn.get('source_compatibility', {})

            # Check if validation supports this source
            if compat.get(source_type, False):
                # If include_partial=False, exclude non-optimized ones
                if not include_partial:
                    optimized_for = compat.get('optimized_for', [source_type])
                    if source_type not in optimized_for:
                        continue

                compatible[name] = defn

        return compatible

    def get_optimal_validations(self, source_type: str) -> Dict[str, Dict[str, Any]]:
        """
        Get validations optimized for a specific source type.

        This returns only validations that are either:
        - Specifically optimized for this source type
        - Work identically on both sources (no optimization preference)

        Args:
            source_type: 'file' or 'database'

        Returns:
            Dictionary of optimal validations for that source
        """
        return self.get_by_source_compatibility(source_type, include_partial=False)

    def get_compatibility_summary(self) -> Dict[str, int]:
        """
        Get summary statistics about source compatibility.

        Returns:
            Dictionary with counts:
            - total: Total number of validations
            - file_compatible: Number compatible with files
            - database_compatible: Number compatible with databases
            - both_compatible: Number compatible with both
            - file_only: Number that only work with files
            - database_only: Number that only work with databases
            - file_optimized: Number optimized for files
            - database_optimized: Number optimized for databases
        """
        total = len(self._definitions)
        file_compatible = 0
        database_compatible = 0
        both_compatible = 0
        file_only = 0
        database_only = 0
        file_optimized = 0
        database_optimized = 0

        for defn in self._definitions.values():
            compat = defn.get('source_compatibility', {})
            file_ok = compat.get('file', False)
            db_ok = compat.get('database', False)
            optimized = compat.get('optimized_for', [])

            if file_ok:
                file_compatible += 1
            if db_ok:
                database_compatible += 1
            if file_ok and db_ok:
                both_compatible += 1
            if file_ok and not db_ok:
                file_only += 1
            if db_ok and not file_ok:
                database_only += 1
            if 'file' in optimized:
                file_optimized += 1
            if 'database' in optimized:
                database_optimized += 1

        return {
            'total': total,
            'file_compatible': file_compatible,
            'database_compatible': database_compatible,
            'both_compatible': both_compatible,
            'file_only': file_only,
            'database_only': database_only,
            'file_optimized': file_optimized,
            'database_optimized': database_optimized
        }

    def export_for_javascript(self) -> str:
        """
        Export definitions as JavaScript object for Studio.

        Returns:
            JavaScript code string defining validationLibrary
        """
        js_lines = []
        js_lines.append("// Auto-generated from validation_definitions.json")
        js_lines.append(f"// Total validations: {self.get_validation_count()}")
        js_lines.append(f"// Generated: {self._metadata.get('last_updated', 'unknown')}")
        js_lines.append("")
        js_lines.append("const validationLibrary = {")

        for i, (name, defn) in enumerate(sorted(self._definitions.items())):
            # Convert Python definition to JavaScript format
            params_js = self._params_to_js(defn.get('params', []))

            js_lines.append(f"    '{name}': {{")
            js_lines.append(f"        icon: '{defn.get('icon', '✓')}',")
            js_lines.append(f"        name: '{name.replace('Check', ' Check').replace('Validation', ' Validation')}',")
            js_lines.append(f"        type: '{name}',")
            js_lines.append(f"        category: '{defn.get('category', 'Other')}',")

            # Escape quotes in description
            description = defn.get('description', '').replace("'", "\\'")
            js_lines.append(f"        description: '{description}',")

            js_lines.append(f"        params: {params_js},")

            examples = defn.get('examples', '').replace("'", "\\'")
            js_lines.append(f"        examples: '{examples}',")

            tips = defn.get('tips', '').replace("'", "\\'")
            js_lines.append(f"        tips: '{tips}'")

            # Add comma except for last item
            ending = "}," if i < len(self._definitions) - 1 else "}"
            js_lines.append(f"    {ending}")

        js_lines.append("};")
        return '\n'.join(js_lines)

    def _params_to_js(self, params: List[Dict[str, Any]]) -> str:
        """
        Convert parameter definitions to JavaScript array syntax.

        Args:
            params: List of parameter definition dictionaries

        Returns:
            JavaScript array string
        """
        if not params:
            return "[]"

        js_params = []
        for param in params:
            param_lines = []
            param_lines.append("            {")

            # Add each property
            for key, value in param.items():
                if key == 'options' and isinstance(value, list):
                    # Array of options
                    options_str = ', '.join(f"'{v}'" for v in value)
                    param_lines.append(f"                {key}: [{options_str}],")
                elif isinstance(value, str):
                    # Escape quotes
                    escaped = value.replace("'", "\\'")
                    param_lines.append(f"                {key}: '{escaped}',")
                elif isinstance(value, bool):
                    param_lines.append(f"                {key}: {str(value).lower()},")
                elif isinstance(value, (int, float)):
                    param_lines.append(f"                {key}: {value},")

            # Remove trailing comma from last property
            if param_lines[-1].endswith(','):
                param_lines[-1] = param_lines[-1][:-1]

            param_lines.append("            }")
            js_params.append('\n'.join(param_lines))

        return "[\n" + ',\n'.join(js_params) + "\n        ]"


# Global singleton instance for easy access
_loader_instance: Optional[ValidationDefinitionLoader] = None


def get_definition_loader() -> ValidationDefinitionLoader:
    """
    Get the global definition loader instance (singleton pattern).

    Returns:
        ValidationDefinitionLoader instance
    """
    global _loader_instance
    if _loader_instance is None:
        _loader_instance = ValidationDefinitionLoader()
    return _loader_instance


# Convenience functions that use the global instance
def get_all_definitions() -> Dict[str, Dict[str, Any]]:
    """Get all validation definitions."""
    return get_definition_loader().get_all_definitions()


def get_definition(validation_type: str) -> Optional[Dict[str, Any]]:
    """Get definition for a specific validation type."""
    return get_definition_loader().get_definition(validation_type)


def get_by_category(category: str) -> Dict[str, Dict[str, Any]]:
    """Get all validations in a specific category."""
    return get_definition_loader().get_by_category(category)


def get_categories() -> List[str]:
    """Get list of all unique categories."""
    return get_definition_loader().get_categories()


def list_validation_types() -> List[str]:
    """Get sorted list of all validation type names."""
    return get_definition_loader().list_validation_types()
