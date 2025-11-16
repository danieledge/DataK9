#!/usr/bin/env python3
"""
Sync Validation Definitions from JSON to Studio JavaScript

This script reads validation definitions from validation_definitions.json
(the single source of truth) and generates a JavaScript validationLibrary
object for DataK9 Studio, ensuring perfect sync between framework and UI.

Usage:
    python3 scripts/sync_validations.py

Author: Daniel Edge
"""

import sys
import json
from pathlib import Path

# Add parent directory to path to import validation framework
sys.path.insert(0, str(Path(__file__).parent.parent))

from validation_framework.utils.definition_loader import ValidationDefinitionLoader


# Metadata extraction no longer needed - we read from JSON now


def generate_js_validation_library():
    """
    Generate JavaScript validationLibrary object from JSON definitions.
    Uses the single source of truth (validation_definitions.json).
    """
    loader = ValidationDefinitionLoader()
    return loader.export_for_javascript()


def generate_summary_report():
    """
    Generate a summary report of all validations by category.
    """
    loader = ValidationDefinitionLoader()
    metadata = loader.get_metadata()

    print("\n" + "="*60)
    print("VALIDATION SYNCHRONIZATION SUMMARY")
    print("="*60)
    print(f"Source: validation_definitions.json")
    print(f"Version: {metadata.get('version', 'unknown')}")
    print(f"Last Updated: {metadata.get('last_updated', 'unknown')}")
    print(f"Total Validations: {loader.get_validation_count()}")
    print(f"Categories: {len(loader.get_categories())}")
    print("\nBy Category:")

    for category in loader.get_categories():
        validations = loader.get_by_category(category)
        print(f"\n{category} ({len(validations)}):")
        for val in sorted(validations.keys()):
            print(f"  - {val}")

    print("\n" + "="*60)


def main():
    """
    Main execution function.
    """
    print("DataK9 Validation Synchronization Tool")
    print("=" * 60)
    print("Scanning validation registry...")

    # Generate summary
    generate_summary_report()

    # Generate JavaScript code
    print("\nGenerating JavaScript validationLibrary...")
    js_code = generate_js_validation_library()

    # Save to file
    output_file = Path(__file__).parent.parent / 'scripts' / 'generated_validations.js'
    output_file.parent.mkdir(exist_ok=True)

    with open(output_file, 'w') as f:
        f.write(js_code)

    print(f"\n✓ Generated JavaScript code saved to: {output_file}")
    print("\nNext steps:")
    print("1. Review generated_validations.js")
    print("2. Copy to datak9-studio.html validationLibrary section")
    print("3. Test all validation types in Studio UI")

    print("\nNote: All parameter definitions are automatically generated from JSON")
    print("To modify parameters, edit validation_definitions.json")


if __name__ == '__main__':
    main()
