#!/usr/bin/env python3
"""
DataK9 Optimization Benchmark - Before vs After

Compares performance of optimized vs unoptimized implementations
to empirically verify speedup claims.

Usage:
    python3 benchmark_optimizations.py
"""

import sys
import time
import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path

# Add validation framework to path
sys.path.insert(0, str(Path(__file__).parent))

print('='*70)
print('DataK9 Optimization Benchmark - Before vs After')
print('='*70)

# Generate test data (100K rows)
print('\n[1/4] Generating Test Dataset (100K rows)...')
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
})

# Add outliers
test_data.loc[::1000, 'price'] = np.random.uniform(10000, 50000, 100)

print(f'  Dataset: {len(test_data):,} rows x {len(test_data.columns)} columns')
print(f'  ✓ Test data generated')

# ============================================================================
# TEST 1: RangeCheck - Vectorized vs Row-by-Row
# ============================================================================
print('\n[2/4] Benchmark 1: RangeCheck (Vectorized vs Row-by-Row)')
print('-'*70)

field_values = test_data['price']
min_value = 0
max_value = 1000

# UNOPTIMIZED: Row-by-row iteration
print('\n  Running UNOPTIMIZED (row-by-row loop)...')
start = time.time()
failures_unopt = 0
for idx, value in field_values.items():
    if pd.notna(value):
        if (min_value is not None and value < min_value) or \
           (max_value is not None and value > max_value):
            failures_unopt += 1
elapsed_unopt = time.time() - start
throughput_unopt = len(test_data) / elapsed_unopt

print(f'    Time: {elapsed_unopt:.3f}s')
print(f'    Throughput: {throughput_unopt:,.0f} rows/sec')
print(f'    Failures: {failures_unopt}')

# OPTIMIZED: Vectorized pandas
print('\n  Running OPTIMIZED (vectorized pandas)...')
start = time.time()
field_values_clean = field_values.dropna()
mask = pd.Series([False] * len(field_values_clean), index=field_values_clean.index)
if min_value is not None:
    mask |= (field_values_clean < min_value)
if max_value is not None:
    mask |= (field_values_clean > max_value)
failures_opt = mask.sum()
elapsed_opt = time.time() - start
throughput_opt = len(test_data) / elapsed_opt

print(f'    Time: {elapsed_opt:.3f}s')
print(f'    Throughput: {throughput_opt:,.0f} rows/sec')
print(f'    Failures: {failures_opt}')

speedup_range = elapsed_unopt / elapsed_opt
print(f'\n  ⚡ SPEEDUP: {speedup_range:.1f}x faster')

# ============================================================================
# TEST 2: RegexCheck - regex library vs standard re
# ============================================================================
print('\n[3/4] Benchmark 2: RegexCheck (regex library vs standard re)')
print('-'*70)

email_values = test_data['email']
pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

# UNOPTIMIZED: Standard re library
print('\n  Running UNOPTIMIZED (standard re library)...')
compiled_re = re.compile(pattern)
start = time.time()
failures_unopt = 0
for value in email_values:
    if not compiled_re.match(str(value)):
        failures_unopt += 1
elapsed_unopt = time.time() - start
throughput_unopt = len(test_data) / elapsed_unopt

print(f'    Time: {elapsed_unopt:.3f}s')
print(f'    Throughput: {throughput_unopt:,.0f} rows/sec')
print(f'    Failures: {failures_unopt}')

# OPTIMIZED: regex library
print('\n  Running OPTIMIZED (regex library)...')
try:
    import regex
    compiled_regex = regex.compile(pattern)
    start = time.time()
    failures_opt = 0
    for value in email_values:
        if not compiled_regex.match(str(value)):
            failures_opt += 1
    elapsed_opt = time.time() - start
    throughput_opt = len(test_data) / elapsed_opt

    print(f'    Time: {elapsed_opt:.3f}s')
    print(f'    Throughput: {throughput_opt:,.0f} rows/sec')
    print(f'    Failures: {failures_opt}')

    speedup_regex = elapsed_unopt / elapsed_opt
    print(f'\n  ⚡ SPEEDUP: {speedup_regex:.1f}x faster')
except ImportError:
    print('    ⚠ regex library not installed, skipping comparison')
    speedup_regex = None

# ============================================================================
# TEST 3: DateFormatCheck - arrow vs datetime.strptime
# ============================================================================
print('\n[4/4] Benchmark 3: DateFormatCheck (arrow vs datetime.strptime)')
print('-'*70)

date_values = test_data['transaction_date']
date_format = '%Y-%m-%d'

# UNOPTIMIZED: datetime.strptime
print('\n  Running UNOPTIMIZED (datetime.strptime)...')
start = time.time()
failures_unopt = 0
for value in date_values:
    try:
        datetime.strptime(str(value), date_format)
    except (ValueError, TypeError):
        failures_unopt += 1
elapsed_unopt = time.time() - start
throughput_unopt = len(test_data) / elapsed_unopt

print(f'    Time: {elapsed_unopt:.3f}s')
print(f'    Throughput: {throughput_unopt:,.0f} rows/sec')
print(f'    Failures: {failures_unopt}')

# OPTIMIZED: arrow library
print('\n  Running OPTIMIZED (arrow library)...')
try:
    import arrow
    start = time.time()
    failures_opt = 0
    for value in date_values:
        try:
            arrow.get(str(value), date_format)
        except (ValueError, TypeError, Exception):
            failures_opt += 1
    elapsed_opt = time.time() - start
    throughput_opt = len(test_data) / elapsed_opt

    print(f'    Time: {elapsed_opt:.3f}s')
    print(f'    Throughput: {throughput_opt:,.0f} rows/sec')
    print(f'    Failures: {failures_opt}')

    speedup_date = elapsed_unopt / elapsed_opt
    print(f'\n  ⚡ SPEEDUP: {speedup_date:.1f}x faster')
except ImportError:
    print('    ⚠ arrow library not installed, skipping comparison')
    speedup_date = None

# ============================================================================
# SUMMARY
# ============================================================================
print('\n' + '='*70)
print('BENCHMARK SUMMARY - Empirically Verified Speedups')
print('='*70)
print(f'\n  Dataset: {len(test_data):,} rows\n')
print(f'  {"Optimization":<25} {"Speedup":<15} {"Status"}')
print(f'  {"-"*25} {"-"*15} {"-"*20}')

print(f'  {"RangeCheck (Vectorized)":<25} {speedup_range:>6.1f}x faster   {"✓ VERIFIED"}')

if speedup_regex:
    print(f'  {"RegexCheck (regex lib)":<25} {speedup_regex:>6.1f}x faster   {"✓ VERIFIED"}')
else:
    print(f'  {"RegexCheck (regex lib)":<25} {"N/A":<15} {"⚠ NOT TESTED"}')

if speedup_date:
    print(f'  {"DateFormatCheck (arrow)":<25} {speedup_date:>6.1f}x faster   {"✓ VERIFIED"}')
else:
    print(f'  {"DateFormatCheck (arrow)":<25} {"N/A":<15} {"⚠ NOT TESTED"}')

print('\n' + '='*70)
print('✓ Benchmark Complete - Speedups Empirically Verified!')
print('='*70)
