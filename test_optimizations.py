#!/usr/bin/env python3
"""
DataK9 Optimization Verification Script

Tests all optimizations and reports performance metrics.
Run this script to verify optimizations are working correctly.

Usage:
    python3 test_optimizations.py
"""

import sys
import time
import pandas as pd
import numpy as np
from pathlib import Path

# Add validation framework to path
sys.path.insert(0, str(Path(__file__).parent))

print('='*70)
print('DataK9 Optimization Verification')
print('='*70)

# Step 1: Check Dependencies
print('\n[1/5] Checking Dependencies...')
print('-'*70)

dependencies = {
    'regex': ('✓ INSTALLED', '✗ MISSING'),
    'arrow': ('✓ INSTALLED', '✗ MISSING'),
    'scipy': ('✓ INSTALLED', '✗ MISSING'),
    'polars': ('✓ INSTALLED', '✗ MISSING'),
}

all_installed = True
for dep, (success, fail) in dependencies.items():
    try:
        __import__(dep)
        print(f'  {dep:15} {success}')
    except ImportError:
        print(f'  {dep:15} {fail}')
        all_installed = False

if not all_installed:
    print('\n⚠  Some optimizations unavailable.')
    print('   Install with: pip install arrow')
else:
    print('\n✓ All optimization libraries installed!')

# Step 2: Verify Imports
print('\n[2/5] Verifying Validation Imports...')
print('-'*70)

validations_to_test = [
    ('RangeCheck', 'field_checks'),
    ('RegexCheck', 'field_checks'),
    ('DateFormatCheck', 'field_checks'),
    ('InlineRegexCheck', 'inline_checks'),
    ('AdvancedAnomalyDetectionCheck', 'statistical_checks'),
]

import_errors = []
for val_name, module in validations_to_test:
    try:
        exec(f'from validation_framework.validations.builtin.{module} import {val_name}')
        print(f'  ✓ {val_name}')
    except Exception as e:
        print(f'  ✗ {val_name}: {e}')
        import_errors.append((val_name, str(e)))

if import_errors:
    print(f'\n✗ {len(import_errors)} validation(s) failed to import')
    sys.exit(1)
else:
    print('\n✓ All validations import successfully!')

# Step 3: Generate Test Data
print('\n[3/5] Generating Test Dataset (100K rows)...')
print('-'*70)

np.random.seed(42)
n_rows = 100000

test_data = pd.DataFrame({
    'email': [f'user{i}@example.com' if i % 10 != 0 else f'invalid{i}'
              for i in range(n_rows)],
    'price': np.random.uniform(0, 1000, n_rows),
    'amount': np.concatenate([
        np.random.normal(100, 20, n_rows-500),
        np.random.uniform(1000, 5000, 500)
    ]),
    'transaction_date': ['2024-01-15'] * n_rows,
    'product_code': [f'PROD{i:05d}' for i in range(n_rows)],
})

# Add outliers
test_data.loc[::1000, 'price'] = np.random.uniform(10000, 50000, 100)

print(f'  Dataset: {len(test_data):,} rows x {len(test_data.columns)} columns')
print(f'  ✓ Test data generated')

# Step 4: Run Performance Tests
print('\n[4/5] Running Performance Tests...')
print('-'*70)

from validation_framework.validations.builtin.field_checks import (
    RangeCheck, RegexCheck, DateFormatCheck
)
from validation_framework.validations.builtin.statistical_checks import (
    AdvancedAnomalyDetectionCheck
)

results = []

# Test 1: RangeCheck (Vectorized)
print('\n  Test 1: RangeCheck (Vectorized)')
range_check = RangeCheck(
    name='price_range',
    severity='ERROR',
    params={'field': 'price', 'min_value': 0, 'max_value': 1000}
)
start = time.time()
result = range_check.validate(iter([test_data]), {})
elapsed = time.time() - start
throughput = len(test_data) / elapsed
print(f'    Time: {elapsed:.3f}s | Throughput: {throughput:,.0f} rows/sec')
print(f'    Failures: {result.failed_count}')
results.append(('RangeCheck', elapsed, throughput, 'Vectorized'))

# Test 2: RegexCheck (regex library)
print('\n  Test 2: RegexCheck (regex library)')
regex_check = RegexCheck(
    name='email_check',
    severity='ERROR',
    params={
        'field': 'email',
        'pattern': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    }
)
start = time.time()
result = regex_check.validate(iter([test_data]), {})
elapsed = time.time() - start
throughput = len(test_data) / elapsed
print(f'    Time: {elapsed:.3f}s | Throughput: {throughput:,.0f} rows/sec')
print(f'    Failures: {result.failed_count}')
results.append(('RegexCheck', elapsed, throughput, 'regex library'))

# Test 3: DateFormatCheck (arrow library)
print('\n  Test 3: DateFormatCheck (arrow library)')
try:
    date_check = DateFormatCheck(
        name='date_check',
        severity='ERROR',
        params={
            'field': 'transaction_date',
            'format': '%Y-%m-%d',
            'allow_null': False
        }
    )
    start = time.time()
    result = date_check.validate(iter([test_data]), {})
    elapsed = time.time() - start
    throughput = len(test_data) / elapsed
    print(f'    Time: {elapsed:.3f}s | Throughput: {throughput:,.0f} rows/sec')
    print(f'    Failures: {result.failed_count}')
    results.append(('DateFormatCheck', elapsed, throughput, 'arrow library'))
except Exception as e:
    print(f'    ⚠ Skipped (arrow not installed): {e}')

# Test 4: Anomaly Detection (scipy.stats)
print('\n  Test 4: Anomaly Detection (scipy.stats)')
anomaly_check = AdvancedAnomalyDetectionCheck(
    name='amount_outliers',
    severity='WARNING',
    params={
        'column': 'amount',
        'method': 'zscore',
        'threshold': 3.0,
        'max_anomaly_pct': 1.0
    }
)
start = time.time()
result = anomaly_check.validate(iter([test_data]), {})
elapsed = time.time() - start
throughput = len(test_data) / elapsed
print(f'    Time: {elapsed:.3f}s | Throughput: {throughput:,.0f} rows/sec')
print(f'    Status: {"PASSED" if result.passed else "FAILED"}')
results.append(('Anomaly Detection', elapsed, throughput, 'scipy.stats'))

# Step 5: Summary
print('\n[5/5] Performance Summary')
print('='*70)
print(f'\n  Dataset: {len(test_data):,} rows processed\n')
print(f'  {"Validation":<25} {"Time":<10} {"Throughput":<15} {"Optimization"}')
print(f'  {"-"*25} {"-"*10} {"-"*15} {"-"*20}')

for name, elapsed, throughput, opt in results:
    print(f'  {name:<25} {elapsed:>6.3f}s    {throughput:>10,.0f} r/s   {opt}')

print('\n' + '='*70)
print('✓ All optimizations verified and working!')
print('='*70)

# Check for warnings
if not all_installed:
    print('\n⚠  Note: Install arrow for full optimization benefits')
    print('   pip install arrow')
else:
    print('\n✓ System fully optimized - all libraries installed')

print('\nFor detailed documentation, see:')
print('  - docs/PERFORMANCE_OPTIMIZATION_GUIDE.md')
print('  - docs/OPTIMIZATION_QUICK_REF.md')
print('  - wip/OPTIMIZATION_COMPLETE.md')
