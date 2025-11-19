#!/usr/bin/env python3
"""
Add source_compatibility metadata to all validations in validation_definitions.json.

This script reads the existing definitions and adds source_compatibility metadata
based on validation category and type.

Author: Daniel Edge
Date: 2025-11-19
"""

import json
from pathlib import Path

# Define compatibility rules based on validation category and type
COMPATIBILITY_RULES = {
    # File-only validations
    "EmptyFileCheck": {
        "file": True,
        "database": False,
        "notes": "File-only validation. For databases, use RowCountRangeCheck with min_rows > 0."
    },
    "FileSizeCheck": {
        "file": True,
        "database": False,
        "notes": "File-only validation. Not applicable to database tables."
    },

    # Works on both, but optimized for files
    "RowCountRangeCheck": {
        "file": True,
        "database": True,
        "optimized_for": ["file"],
        "notes": "Works on databases but loads all data in chunks. For large tables, consider using SQL COUNT(*) directly."
    },
    "ReferentialIntegrityCheck": {
        "file": True,
        "database": True,
        "optimized_for": ["file"],
        "notes": "Works with both sources. For database-to-database checks, use DatabaseReferentialIntegrityCheck for better performance."
    },
    "CrossFileComparisonCheck": {
        "file": True,
        "database": True,
        "optimized_for": ["file"],
        "notes": "Works on databases but SQL aggregates are more efficient for database-to-database comparisons."
    },
    "CrossFileDuplicateCheck": {
        "file": True,
        "database": True,
        "optimized_for": ["file"],
        "notes": "Works on databases but SQL GROUP BY is faster for duplicate detection across database tables."
    },

    # Database-only validations
    "SQLCustomCheck": {
        "file": False,
        "database": True,
        "notes": "Database-only validation. Requires database connection and executes SQL queries."
    },
    "DatabaseReferentialIntegrityCheck": {
        "file": False,
        "database": True,
        "notes": "Database-only validation using SQL JOINs. For file-to-file checks, use ReferentialIntegrityCheck."
    },
    "DatabaseConstraintCheck": {
        "file": False,
        "database": True,
        "notes": "Database-only validation for checking database constraints (PK, FK, UNIQUE, NOT NULL)."
    },

    # Temporal with baseline file
    "BaselineComparisonCheck": {
        "file": True,
        "database": True,
        "optimized_for": ["file"],
        "notes": "Works with databases. baseline_file parameter can reference a database table."
    },
}

# Default compatibility for most validations
DEFAULT_COMPATIBILITY = {
    "file": True,
    "database": True,
    "notes": "Works identically on file and database sources via chunked iterator pattern."
}


def add_source_compatibility():
    """Add source_compatibility to validation_definitions.json."""
    # Load current definitions
    definitions_file = Path(__file__).parent.parent / "validation_framework" / "validation_definitions.json"

    print(f"Loading definitions from: {definitions_file}")
    with open(definitions_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Update version
    data['_metadata']['version'] = "2.0.0"
    data['_metadata']['last_updated'] = "2025-11-19"
    data['_metadata']['description'] = "Single source of truth for all DataK9 validation definitions with source compatibility"

    # Add source_compatibility to each validation
    updated_count = 0
    for validation_name, validation_def in data.items():
        # Skip metadata and schema
        if validation_name in ['$schema', '_metadata']:
            continue

        # Skip if already has source_compatibility
        if 'source_compatibility' in validation_def:
            print(f"  Skipping {validation_name} (already has source_compatibility)")
            continue

        # Get compatibility rules
        if validation_name in COMPATIBILITY_RULES:
            compat = COMPATIBILITY_RULES[validation_name].copy()
        else:
            compat = DEFAULT_COMPATIBILITY.copy()

        # Add to validation
        validation_def['source_compatibility'] = compat
        updated_count += 1
        print(f"  ✓ Added source_compatibility to {validation_name}")

    # Save updated definitions
    print(f"\nSaving updated definitions...")
    with open(definitions_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"✓ Updated {updated_count} validations")
    print(f"✓ Saved to {definitions_file}")


if __name__ == "__main__":
    add_source_compatibility()
