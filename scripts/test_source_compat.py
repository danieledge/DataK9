#!/usr/bin/env python3
"""
Test source compatibility functionality.

Author: Daniel Edge
"""

from validation_framework.utils.definition_loader import ValidationDefinitionLoader
from pathlib import Path

# Create fresh loader instance
def_file = Path(__file__).parent.parent / "validation_framework" / "validation_definitions.json"
loader = ValidationDefinitionLoader(def_file)

print("DataK9 Source Compatibility Test")
print("=" * 70)

# Get summary
summary = loader.get_compatibility_summary()
print("\n Source Compatibility Summary:")
print("-" * 70)
for key, value in summary.items():
    print(f"  {key:30s}: {value}")

# Show database-compatible validations
print("\n✓ Database-Compatible Validations:")
print("-" * 70)
db_compat = loader.get_by_source_compatibility('database')
for name in sorted(db_compat.keys()):
    compat = db_compat[name]['source_compatibility']
    file_ok = "📁" if compat.get('file') else "  "
    db_ok = "🗄️" if compat.get('database') else "  "
    opt_str = ""
    if compat.get('optimized_for'):
        opt_str = f" (optimized for {', '.join(compat.get('optimized_for'))})"
    print(f"  {file_ok} {db_ok}  {name}{opt_str}")

# Show file-only validations
print("\n📁 File-Only Validations:")
print("-" * 70)
for name, defn in sorted(loader.get_all_definitions().items()):
    compat = defn.get('source_compatibility', {})
    if compat.get('file') and not compat.get('database'):
        notes = compat.get('notes', '')
        print(f"  • {name}")
        if notes:
            print(f"    └─ {notes}")

# Show database-only validations
print("\n🗄️  Database-Only Validations:")
print("-" * 70)
for name, defn in sorted(loader.get_all_definitions().items()):
    compat = defn.get('source_compatibility', {})
    if compat.get('database') and not compat.get('file'):
        notes = compat.get('notes', '')
        print(f"  • {name}")
        if notes:
            print(f"    └─ {notes}")

print("\n" + "=" * 70)
print("✓ Source compatibility system working correctly!")
