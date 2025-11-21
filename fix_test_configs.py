#!/usr/bin/env python3
"""
Script to fix ValidationConfig initialization in test files.

ValidationConfig expects a dict with 'validation_job' key, not keyword args.
"""

import re
import sys

def fix_validation_config_calls(content):
    """
    Fix ValidationConfig initialization patterns.

    Converts:
        config = ValidationConfig(
            job_name="Test",
            files=[...]
        )

    To:
        config_dict = create_config_dict(
            "Test",
            [...]
        )
        config = ValidationConfig(config_dict)
    """

    # Pattern to match ValidationConfig with keyword args
    pattern = r'(\s+)config = ValidationConfig\(\s*\n\s+job_name="([^"]+)",\s*\n\s+files=(\[[\s\S]*?\])\s*\n\s+\)'

    def replace_func(match):
        indent = match.group(1)
        job_name = match.group(2)
        files = match.group(3)

        return f'''{indent}config_dict = create_config_dict(
{indent}    "{job_name}",
{indent}    {files}
{indent})
{indent}config = ValidationConfig(config_dict)'''

    content = re.sub(pattern, replace_func, content)

    return content

def process_file(filepath):
    """Process a single test file."""
    print(f"Processing {filepath}...")

    try:
        with open(filepath, 'r') as f:
            content = f.read()

        # Count patterns before
        before_count = len(re.findall(r'ValidationConfig\(\s*\n\s+job_name=', content))

        # Apply fixes
        fixed_content = fix_validation_config_calls(content)

        # Count patterns after
        after_count = len(re.findall(r'ValidationConfig\(\s*\n\s+job_name=', fixed_content))

        # Write back
        with open(filepath, 'w') as f:
            f.write(fixed_content)

        print(f"  Fixed {before_count - after_count} patterns (remaining: {after_count})")
        return True

    except Exception as e:
        print(f"  Error: {e}")
        return False

if __name__ == '__main__':
    files = [
        'tests/core/test_optimized_engine.py',
        'tests/core/test_sampling_engine.py'
    ]

    success_count = 0
    for filepath in files:
        if process_file(filepath):
            success_count += 1

    print(f"\nProcessed {success_count}/{len(files)} files successfully")
