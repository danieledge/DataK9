#!/usr/bin/env python3
"""
DataK9 Comprehensive Performance Benchmark

Tests validation performance across multiple:
- File sizes (10K, 100K, 500K rows)
- File formats (CSV, Parquet)
- Complexity levels (simple patterns, complex patterns)

Saves results to JSON for before/after comparison.

Usage:
    python3 comprehensive_benchmark.py baseline  # Before optimizations
    python3 comprehensive_benchmark.py optimized # After optimizations
    python3 comprehensive_benchmark.py compare   # Compare results
"""

import sys
import time
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import tempfile
import os

# Add validation framework to path
sys.path.insert(0, str(Path(__file__).parent))

from validation_framework.validations.builtin.field_checks import (
    RangeCheck, RegexCheck, DateFormatCheck
)
from validation_framework.validations.builtin.statistical_checks import (
    AdvancedAnomalyDetectionCheck
)

# ============================================================================
# Configuration
# ============================================================================

SIZES = {
    'small': 10_000,
    'medium': 100_000,
    'large': 500_000
}

PATTERNS = {
    'simple_email': r'^[a-z0-9]+@[a-z]+\.[a-z]+$',
    'complex_email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'very_complex': r'^(?=.*[A-Z])(?=.*[a-z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$'
}

# ============================================================================
# Helper Functions
# ============================================================================

def generate_test_data(n_rows, complexity='medium'):
    """Generate test dataset with specified complexity."""
    np.random.seed(42)

    # Email field with varying quality
    if complexity == 'simple':
        emails = [f'user{i}@example.com' for i in range(n_rows)]
    elif complexity == 'medium':
        emails = [f'user{i}@example.com' if i % 10 != 0 else f'invalid{i}'
                  for i in range(n_rows)]
    else:  # complex
        emails = [f'User.{i}@Example-{i%5}.Com' if i % 10 != 0 else f'bad@@@{i}'
                  for i in range(n_rows)]

    # Numeric fields
    prices = np.random.uniform(0, 1000, n_rows)
    amounts = np.concatenate([
        np.random.normal(100, 20, n_rows - int(n_rows * 0.01)),
        np.random.uniform(1000, 5000, int(n_rows * 0.01))
    ])

    # Date field
    dates = ['2024-01-15'] * n_rows

    # Add outliers to prices
    outlier_count = max(1, n_rows // 1000)
    outlier_indices = np.random.choice(n_rows, outlier_count, replace=False)
    prices[outlier_indices] = np.random.uniform(10000, 50000, outlier_count)

    return pd.DataFrame({
        'email': emails,
        'price': prices,
        'amount': amounts,
        'transaction_date': dates,
        'product_id': [f'PROD{i:06d}' for i in range(n_rows)]
    })

def save_dataset(df, format_type, path):
    """Save dataset in specified format."""
    if format_type == 'csv':
        df.to_csv(path, index=False)
    elif format_type == 'parquet':
        df.to_parquet(path, index=False, engine='pyarrow')

def benchmark_validation(validation, data_path, file_format):
    """Run validation and measure performance."""
    # Load data
    if file_format == 'csv':
        data = pd.read_csv(data_path)
    else:
        data = pd.read_parquet(data_path)

    # Run validation
    start = time.time()
    result = validation.validate(iter([data]), {})
    elapsed = time.time() - start

    return {
        'time_seconds': elapsed,
        'rows_per_second': len(data) / elapsed if elapsed > 0 else 0,
        'total_rows': len(data),
        'passed': result.passed,
        'failed_count': result.failed_count
    }

# ============================================================================
# Benchmark Suite
# ============================================================================

def run_benchmark_suite(label='baseline'):
    """Run comprehensive benchmark suite."""
    print('='*70)
    print(f'DataK9 Comprehensive Benchmark - {label.upper()}')
    print('='*70)
    print(f'Timestamp: {datetime.now().isoformat()}')

    results = {
        'label': label,
        'timestamp': datetime.now().isoformat(),
        'benchmarks': []
    }

    temp_dir = tempfile.mkdtemp(prefix='datak9_benchmark_')

    try:
        # Test each size
        for size_name, row_count in SIZES.items():
            print(f'\n[{size_name.upper()}] Testing with {row_count:,} rows')
            print('-'*70)

            # Generate data
            print(f'  Generating test data...')
            data = generate_test_data(row_count, complexity='medium')

            # Test both formats
            for file_format in ['csv', 'parquet']:
                print(f'\n  Format: {file_format.upper()}')

                # Save data
                file_path = os.path.join(temp_dir, f'test_{size_name}.{file_format}')
                save_dataset(data, file_format, file_path)

                # Test 1: RangeCheck
                print(f'    Running RangeCheck...')
                validation = RangeCheck(
                    name='price_range',
                    severity='ERROR',
                    params={'field': 'price', 'min_value': 0, 'max_value': 1000}
                )
                benchmark_result = benchmark_validation(validation, file_path, file_format)
                print(f'      Time: {benchmark_result["time_seconds"]:.3f}s | '
                      f'Throughput: {benchmark_result["rows_per_second"]:,.0f} rows/sec')

                results['benchmarks'].append({
                    'size': size_name,
                    'row_count': row_count,
                    'format': file_format,
                    'validation': 'RangeCheck',
                    **benchmark_result
                })

                # Test 2: RegexCheck (simple pattern)
                print(f'    Running RegexCheck (simple pattern)...')
                validation = RegexCheck(
                    name='email_simple',
                    severity='ERROR',
                    params={
                        'field': 'email',
                        'pattern': PATTERNS['simple_email']
                    }
                )
                benchmark_result = benchmark_validation(validation, file_path, file_format)
                print(f'      Time: {benchmark_result["time_seconds"]:.3f}s | '
                      f'Throughput: {benchmark_result["rows_per_second"]:,.0f} rows/sec')

                results['benchmarks'].append({
                    'size': size_name,
                    'row_count': row_count,
                    'format': file_format,
                    'validation': 'RegexCheck_simple',
                    **benchmark_result
                })

                # Test 3: RegexCheck (complex pattern)
                print(f'    Running RegexCheck (complex pattern)...')
                validation = RegexCheck(
                    name='email_complex',
                    severity='ERROR',
                    params={
                        'field': 'email',
                        'pattern': PATTERNS['complex_email']
                    }
                )
                benchmark_result = benchmark_validation(validation, file_path, file_format)
                print(f'      Time: {benchmark_result["time_seconds"]:.3f}s | '
                      f'Throughput: {benchmark_result["rows_per_second"]:,.0f} rows/sec')

                results['benchmarks'].append({
                    'size': size_name,
                    'row_count': row_count,
                    'format': file_format,
                    'validation': 'RegexCheck_complex',
                    **benchmark_result
                })

                # Test 4: DateFormatCheck
                print(f'    Running DateFormatCheck...')
                validation = DateFormatCheck(
                    name='date_check',
                    severity='ERROR',
                    params={
                        'field': 'transaction_date',
                        'format': '%Y-%m-%d',
                        'allow_null': False
                    }
                )
                benchmark_result = benchmark_validation(validation, file_path, file_format)
                print(f'      Time: {benchmark_result["time_seconds"]:.3f}s | '
                      f'Throughput: {benchmark_result["rows_per_second"]:,.0f} rows/sec')

                results['benchmarks'].append({
                    'size': size_name,
                    'row_count': row_count,
                    'format': file_format,
                    'validation': 'DateFormatCheck',
                    **benchmark_result
                })

                # Test 5: Anomaly Detection
                print(f'    Running AnomalyDetection (zscore)...')
                validation = AdvancedAnomalyDetectionCheck(
                    name='amount_outliers',
                    severity='WARNING',
                    params={
                        'column': 'amount',
                        'method': 'zscore',
                        'threshold': 3.0,
                        'max_anomaly_pct': 5.0
                    }
                )
                benchmark_result = benchmark_validation(validation, file_path, file_format)
                print(f'      Time: {benchmark_result["time_seconds"]:.3f}s | '
                      f'Throughput: {benchmark_result["rows_per_second"]:,.0f} rows/sec')

                results['benchmarks'].append({
                    'size': size_name,
                    'row_count': row_count,
                    'format': file_format,
                    'validation': 'AnomalyDetection_zscore',
                    **benchmark_result
                })

    finally:
        # Cleanup temp files
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    # Save results
    output_file = f'benchmark_results_{label}.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)

    print(f'\n{"="*70}')
    print(f'✓ Benchmark Complete - Results saved to: {output_file}')
    print('='*70)

    return results

# ============================================================================
# Comparison
# ============================================================================

def compare_results():
    """Compare baseline vs optimized results."""
    print('='*70)
    print('DataK9 Benchmark Comparison - Baseline vs Optimized')
    print('='*70)

    # Load results
    try:
        with open('benchmark_results_baseline.json') as f:
            baseline = json.load(f)
        with open('benchmark_results_optimized.json') as f:
            optimized = json.load(f)
    except FileNotFoundError as e:
        print(f'\n✗ Error: Missing benchmark file: {e}')
        print('  Run benchmarks first:')
        print('    python3 comprehensive_benchmark.py baseline')
        print('    python3 comprehensive_benchmark.py optimized')
        return

    print(f'\nBaseline:  {baseline["timestamp"]}')
    print(f'Optimized: {optimized["timestamp"]}')

    # Group results by validation and size
    comparisons = []

    for base_bench in baseline['benchmarks']:
        # Find matching optimized benchmark
        opt_bench = next(
            (b for b in optimized['benchmarks']
             if b['validation'] == base_bench['validation']
             and b['size'] == base_bench['size']
             and b['format'] == base_bench['format']),
            None
        )

        if opt_bench:
            speedup = opt_bench['rows_per_second'] / base_bench['rows_per_second']
            time_ratio = base_bench['time_seconds'] / opt_bench['time_seconds']

            comparisons.append({
                'validation': base_bench['validation'],
                'size': base_bench['size'],
                'format': base_bench['format'],
                'baseline_time': base_bench['time_seconds'],
                'optimized_time': opt_bench['time_seconds'],
                'baseline_throughput': base_bench['rows_per_second'],
                'optimized_throughput': opt_bench['rows_per_second'],
                'speedup': speedup,
                'time_ratio': time_ratio
            })

    # Print summary
    print(f'\n{"="*70}')
    print('SUMMARY - Performance Changes')
    print('='*70)

    for comp in comparisons:
        status = '✓ FASTER' if comp['speedup'] > 1.0 else '✗ SLOWER'
        color_code = '' if comp['speedup'] > 1.0 else '⚠ '

        print(f'\n{comp["validation"]} ({comp["size"]}, {comp["format"]})')
        print(f'  Baseline:  {comp["baseline_time"]:.3f}s ({comp["baseline_throughput"]:,.0f} rows/sec)')
        print(f'  Optimized: {comp["optimized_time"]:.3f}s ({comp["optimized_throughput"]:,.0f} rows/sec)')
        print(f'  {color_code}Result: {comp["speedup"]:.2f}x throughput ({comp["time_ratio"]:.2f}x time) - {status}')

    # Overall statistics
    print(f'\n{"="*70}')
    print('OVERALL STATISTICS')
    print('='*70)

    improvements = [c for c in comparisons if c['speedup'] > 1.0]
    regressions = [c for c in comparisons if c['speedup'] < 1.0]

    print(f'  Total tests: {len(comparisons)}')
    print(f'  Improvements: {len(improvements)}')
    print(f'  Regressions: {len(regressions)}')

    if improvements:
        avg_improvement = sum(c['speedup'] for c in improvements) / len(improvements)
        print(f'  Average improvement: {avg_improvement:.2f}x faster')

    if regressions:
        avg_regression = sum(c['speedup'] for c in regressions) / len(regressions)
        print(f'  Average regression: {avg_regression:.2f}x (slower)')

    print(f'\n{"="*70}')

    # Recommendation
    if len(regressions) == 0:
        print('✓ RECOMMENDATION: Keep optimizations - All tests improved!')
    elif len(improvements) > len(regressions) and len(regressions) / len(comparisons) < 0.3:
        print('⚠ RECOMMENDATION: Review optimizations - Some regressions detected')
    else:
        print('✗ RECOMMENDATION: Revert optimizations - Significant regressions detected')

    print('='*70)

# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage:')
        print('  python3 comprehensive_benchmark.py baseline   # Before optimizations')
        print('  python3 comprehensive_benchmark.py optimized  # After optimizations')
        print('  python3 comprehensive_benchmark.py compare    # Compare results')
        sys.exit(1)

    mode = sys.argv[1]

    if mode == 'compare':
        compare_results()
    elif mode in ['baseline', 'optimized']:
        run_benchmark_suite(mode)
    else:
        print(f'Unknown mode: {mode}')
        sys.exit(1)
