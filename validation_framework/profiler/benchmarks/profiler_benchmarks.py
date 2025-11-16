"""
Profiler Performance Benchmarks

Benchmarks pandas vs Polars profiler performance on datasets of varying sizes:
- Small: 100K rows (ecommerce_transactions.csv)
- Medium: 3M rows (HI-Small_Trans.parquet)
- Large: 179M rows (HI-Large_Trans.parquet)

Usage:
    python3 -m validation_framework.profiler.benchmarks.profiler_benchmarks --all
    python3 -m validation_framework.profiler.benchmarks.profiler_benchmarks --small
    python3 -m validation_framework.profiler.benchmarks.profiler_benchmarks --medium
    python3 -m validation_framework.profiler.benchmarks.profiler_benchmarks --large
"""

import time
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional
import argparse

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from validation_framework.profiler import DataProfiler, PolarsDataProfiler


class ProfilerBenchmark:
    """Benchmark profiler performance on various datasets."""

    def __init__(self, data_dir: str = "../../../test-data"):
        self.data_dir = Path(data_dir)
        self.results = []

    def benchmark_dataset(
        self,
        dataset_name: str,
        file_path: str,
        file_format: str,
        backend: str = "polars",
        enable_correlations: bool = False
    ) -> Dict[str, Any]:
        """
        Benchmark profiling a single dataset.

        Args:
            dataset_name: Name for the dataset
            file_path: Path to data file
            file_format: File format (csv, parquet)
            backend: Backend to use (pandas, polars)
            enable_correlations: Whether to calculate correlations

        Returns:
            Dict with benchmark results
        """
        full_path = self.data_dir / file_path

        if not full_path.exists():
            return {
                'dataset': dataset_name,
                'backend': backend,
                'status': 'SKIPPED',
                'error': f'File not found: {full_path}'
            }

        print(f"\n{'='*80}")
        print(f"Benchmarking: {dataset_name} ({backend} backend)")
        print(f"File: {full_path}")
        print(f"{'='*80}\n")

        try:
            # Select profiler
            if backend == 'polars':
                profiler = PolarsDataProfiler(
                    chunk_size=200000,
                    backend='polars',
                    enable_patterns=True,
                    enable_anomalies=True,
                    enable_correlations=enable_correlations
                )
            else:
                profiler = DataProfiler(
                    chunk_size=50000,
                    enable_correlations=enable_correlations,
                    enable_advanced_stats=False  # Disable for fair comparison
                )

            # Measure profiling time
            start_time = time.time()
            start_memory = self._get_memory_usage()

            if backend == 'polars':
                result = profiler.profile_file(
                    file_path=str(full_path),
                    file_format=file_format
                )
            else:
                result = profiler.profile_file(
                    file_path=str(full_path),
                    file_format=file_format
                )

            end_time = time.time()
            end_memory = self._get_memory_usage()

            duration = end_time - start_time
            memory_used = end_memory - start_memory

            # Extract results
            if backend == 'polars':
                row_count = result.row_count
                column_count = result.column_count
            else:
                row_count = result.row_count
                column_count = result.column_count

            benchmark_result = {
                'dataset': dataset_name,
                'backend': backend,
                'status': 'SUCCESS',
                'duration_seconds': round(duration, 2),
                'memory_mb': round(memory_used / 1024 / 1024, 2),
                'row_count': row_count,
                'column_count': column_count,
                'rows_per_second': round(row_count / duration, 0),
                'file_size_mb': round(full_path.stat().st_size / 1024 / 1024, 2)
            }

            # Print results
            print(f"✓ SUCCESS")
            print(f"  Duration:        {benchmark_result['duration_seconds']}s")
            print(f"  Memory Used:     {benchmark_result['memory_mb']} MB")
            print(f"  Rows:            {benchmark_result['row_count']:,}")
            print(f"  Columns:         {benchmark_result['column_count']}")
            print(f"  Rows/sec:        {benchmark_result['rows_per_second']:,}")
            print(f"  File Size:       {benchmark_result['file_size_mb']} MB")

            return benchmark_result

        except Exception as e:
            error_result = {
                'dataset': dataset_name,
                'backend': backend,
                'status': 'ERROR',
                'error': str(e)
            }

            print(f"✗ ERROR: {str(e)}")

            return error_result

    def _get_memory_usage(self) -> int:
        """Get current process memory usage in bytes."""
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            return process.memory_info().rss
        except ImportError:
            # psutil not available, return 0
            return 0

    def benchmark_small_dataset(self, backend: str = "polars"):
        """Benchmark small dataset (100K rows)."""
        result = self.benchmark_dataset(
            dataset_name="Small (100K rows)",
            file_path="ecommerce_transactions.csv",
            file_format="csv",
            backend=backend,
            enable_correlations=True
        )
        self.results.append(result)
        return result

    def benchmark_medium_dataset(self, backend: str = "polars"):
        """Benchmark medium dataset (3M rows)."""
        result = self.benchmark_dataset(
            dataset_name="Medium (3M rows)",
            file_path="HI-Small_Trans.parquet",
            file_format="parquet",
            backend=backend,
            enable_correlations=False  # Skip correlations for large files
        )
        self.results.append(result)
        return result

    def benchmark_large_dataset(self, backend: str = "polars"):
        """Benchmark large dataset (179M rows)."""
        result = self.benchmark_dataset(
            dataset_name="Large (179M rows)",
            file_path="HI-Large_Trans.parquet",
            file_format="parquet",
            backend=backend,
            enable_correlations=False  # Skip correlations for very large files
        )
        self.results.append(result)
        return result

    def run_comparison_benchmark(self, dataset_type: str = "small"):
        """Run comparison benchmark between pandas and Polars."""
        print(f"\n{'#'*80}")
        print(f"#  COMPARISON BENCHMARK: {dataset_type.upper()} DATASET")
        print(f"{'#'*80}\n")

        # Run pandas benchmark
        print("\n--- PANDAS BACKEND ---")
        if dataset_type == "small":
            pandas_result = self.benchmark_small_dataset(backend="pandas")
        elif dataset_type == "medium":
            pandas_result = self.benchmark_medium_dataset(backend="pandas")
        elif dataset_type == "large":
            pandas_result = self.benchmark_large_dataset(backend="pandas")

        # Run Polars benchmark
        print("\n--- POLARS BACKEND ---")
        if dataset_type == "small":
            polars_result = self.benchmark_small_dataset(backend="polars")
        elif dataset_type == "medium":
            polars_result = self.benchmark_medium_dataset(backend="polars")
        elif dataset_type == "large":
            polars_result = self.benchmark_large_dataset(backend="polars")

        # Calculate speedup
        if (pandas_result.get('status') == 'SUCCESS' and
            polars_result.get('status') == 'SUCCESS'):

            speedup = pandas_result['duration_seconds'] / polars_result['duration_seconds']
            memory_reduction = (1 - (polars_result['memory_mb'] / pandas_result['memory_mb'])) * 100

            print(f"\n{'='*80}")
            print(f"COMPARISON RESULTS: {dataset_type.upper()} DATASET")
            print(f"{'='*80}")
            print(f"  Pandas:          {pandas_result['duration_seconds']}s")
            print(f"  Polars:          {polars_result['duration_seconds']}s")
            print(f"  Speedup:         {speedup:.1f}x faster")
            print(f"  Memory (Pandas): {pandas_result['memory_mb']} MB")
            print(f"  Memory (Polars): {polars_result['memory_mb']} MB")
            print(f"  Memory Savings:  {memory_reduction:.1f}%")
            print(f"{'='*80}\n")

    def print_summary(self):
        """Print benchmark summary."""
        if not self.results:
            print("\nNo benchmark results to summarize.")
            return

        print(f"\n{'#'*80}")
        print(f"#  BENCHMARK SUMMARY")
        print(f"{'#'*80}\n")

        # Group results by backend
        pandas_results = [r for r in self.results if r.get('backend') == 'pandas' and r.get('status') == 'SUCCESS']
        polars_results = [r for r in self.results if r.get('backend') == 'polars' and r.get('status') == 'SUCCESS']

        # Print table header
        print(f"{'Dataset':<25} {'Backend':<10} {'Duration':<12} {'Memory':<12} {'Rows/sec':<12} {'Status'}")
        print(f"{'-'*25} {'-'*10} {'-'*12} {'-'*12} {'-'*12} {'-'*10}")

        for result in self.results:
            dataset = result['dataset']
            backend = result['backend']
            status = result['status']

            if status == 'SUCCESS':
                duration = f"{result['duration_seconds']}s"
                memory = f"{result['memory_mb']} MB"
                rows_per_sec = f"{result['rows_per_second']:,}"
            else:
                duration = "N/A"
                memory = "N/A"
                rows_per_sec = "N/A"

            print(f"{dataset:<25} {backend:<10} {duration:<12} {memory:<12} {rows_per_sec:<12} {status}")

        # Calculate average speedup
        if pandas_results and polars_results:
            print(f"\n{'='*80}")
            print(f"AVERAGE SPEEDUP")
            print(f"{'='*80}")

            for pandas_result in pandas_results:
                dataset = pandas_result['dataset']
                polars_result = next((r for r in polars_results if r['dataset'] == dataset), None)

                if polars_result:
                    speedup = pandas_result['duration_seconds'] / polars_result['duration_seconds']
                    print(f"  {dataset:<25} {speedup:.1f}x faster with Polars")

    def save_results(self, output_file: str = "profiler_benchmark_results.json"):
        """Save benchmark results to JSON file."""
        output_path = Path(output_file)

        with open(output_path, 'w') as f:
            json.dump({
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'results': self.results
            }, f, indent=2)

        print(f"\n✓ Results saved to: {output_path.absolute()}")


def main():
    """Main benchmark runner."""
    parser = argparse.ArgumentParser(description="Profiler Performance Benchmarks")
    parser.add_argument('--all', action='store_true', help='Run all benchmarks')
    parser.add_argument('--small', action='store_true', help='Benchmark small dataset (100K rows)')
    parser.add_argument('--medium', action='store_true', help='Benchmark medium dataset (3M rows)')
    parser.add_argument('--large', action='store_true', help='Benchmark large dataset (179M rows)')
    parser.add_argument('--compare', action='store_true', help='Run pandas vs Polars comparison')
    parser.add_argument('--output', '-o', help='Output JSON file path', default='profiler_benchmark_results.json')
    parser.add_argument('--data-dir', help='Path to test data directory', default='../../../test-data')

    args = parser.parse_args()

    # Create benchmark runner
    benchmark = ProfilerBenchmark(data_dir=args.data_dir)

    # Run requested benchmarks
    if args.all:
        # Run comparison on all datasets
        benchmark.run_comparison_benchmark("small")
        benchmark.run_comparison_benchmark("medium")
        benchmark.run_comparison_benchmark("large")
    elif args.compare:
        # Run comparison on small dataset by default
        benchmark.run_comparison_benchmark("small")
    else:
        # Run individual benchmarks (Polars only)
        if args.small:
            benchmark.benchmark_small_dataset(backend="polars")
        if args.medium:
            benchmark.benchmark_medium_dataset(backend="polars")
        if args.large:
            benchmark.benchmark_large_dataset(backend="polars")

        # If no specific dataset selected, show help
        if not (args.small or args.medium or args.large):
            parser.print_help()
            return

    # Print summary
    benchmark.print_summary()

    # Save results
    benchmark.save_results(args.output)


if __name__ == '__main__':
    main()
