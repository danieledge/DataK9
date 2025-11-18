#!/usr/bin/env python3
"""
Chunk Size Calculator for DataK9 Validation Framework

Intelligently calculates optimal chunk sizes based on:
- File size
- Available RAM
- Data format (CSV vs Parquet)
- Number of validations
- Validation complexity

Author: Daniel Edge
"""

import os
import psutil
from pathlib import Path
from typing import Dict, Tuple, Optional


class ChunkSizeCalculator:
    """
    Calculate optimal chunk size for data validation processing.

    Provides intelligent recommendations based on system resources,
    file characteristics, and validation requirements.
    """

    # Memory safety margin (use only 70% of available RAM)
    MEMORY_SAFETY_MARGIN = 0.7

    # Average bytes per row estimates by format
    BYTES_PER_ROW = {
        'csv': 150,      # Text-based, larger
        'parquet': 50,   # Compressed, much smaller
        'json': 200,     # JSON overhead
        'excel': 100,    # Binary format
    }

    # Validation complexity multipliers (memory usage factor)
    VALIDATION_COMPLEXITY = {
        'simple': 1.0,      # Basic checks (empty, schema, range)
        'moderate': 2.0,    # Statistical checks (outliers, patterns)
        'complex': 4.0,     # Cross-file, duplicates, aggregations
        'heavy': 8.0,       # Distribution analysis, ML-based
    }

    def __init__(self):
        """Initialize calculator with system information."""
        self.available_memory = self._get_available_memory()
        self.total_memory = psutil.virtual_memory().total

    def _get_available_memory(self) -> int:
        """Get available system memory in bytes."""
        vm = psutil.virtual_memory()
        # Use available memory with safety margin
        return int(vm.available * self.MEMORY_SAFETY_MARGIN)

    def calculate_optimal_chunk_size(
        self,
        file_path: str,
        file_format: str = 'csv',
        num_validations: int = 1,
        validation_complexity: str = 'simple',
        target_memory_mb: Optional[int] = None
    ) -> Dict[str, any]:
        """
        Calculate optimal chunk size for a dataset.

        Args:
            file_path: Path to the data file
            file_format: File format (csv, parquet, json, excel)
            num_validations: Number of validations to run
            validation_complexity: Complexity level (simple, moderate, complex, heavy)
            target_memory_mb: Optional target memory limit in MB

        Returns:
            Dictionary with recommendations including:
            - recommended_chunk_size: Optimal rows per chunk
            - estimated_chunks: Number of chunks
            - estimated_memory_mb: Peak memory usage estimate
            - rationale: Explanation of the recommendation
        """
        # Get file size
        file_size_bytes = Path(file_path).stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)

        # Estimate row count
        bytes_per_row = self.BYTES_PER_ROW.get(file_format.lower(), 100)
        estimated_rows = file_size_bytes / bytes_per_row

        # Determine target memory
        if target_memory_mb:
            target_memory_bytes = target_memory_mb * 1024 * 1024
        else:
            target_memory_bytes = self.available_memory

        # Apply validation complexity multiplier
        complexity_factor = self.VALIDATION_COMPLEXITY.get(
            validation_complexity.lower(), 1.0
        )

        # Calculate memory per row in memory (decompressed)
        # Parquet/CSV expand ~3x when loaded into pandas
        memory_expansion_factor = 3.0
        memory_per_row = bytes_per_row * memory_expansion_factor

        # Account for validation overhead
        memory_per_row *= complexity_factor

        # Account for multiple validations (some state accumulation)
        memory_per_row *= (1 + (num_validations * 0.1))

        # Calculate optimal chunk size
        optimal_chunk_size = int(target_memory_bytes / memory_per_row)

        # Apply practical limits
        min_chunk = 10_000
        max_chunk = 2_000_000

        if optimal_chunk_size < min_chunk:
            optimal_chunk_size = min_chunk
            rationale = f"Using minimum chunk size ({min_chunk:,}). File is very large relative to available memory."
        elif optimal_chunk_size > max_chunk:
            optimal_chunk_size = max_chunk
            rationale = f"Using maximum chunk size ({max_chunk:,}). Sufficient memory available."
        else:
            # Round to nearest 10,000
            optimal_chunk_size = round(optimal_chunk_size / 10_000) * 10_000
            rationale = "Calculated based on available memory and validation complexity."

        # Calculate estimates
        estimated_chunks = int(estimated_rows / optimal_chunk_size) + 1
        estimated_memory_mb = (optimal_chunk_size * memory_per_row) / (1024 * 1024)

        # Generate recommendation
        return {
            'recommended_chunk_size': optimal_chunk_size,
            'estimated_rows': int(estimated_rows),
            'estimated_chunks': estimated_chunks,
            'estimated_memory_mb': int(estimated_memory_mb),
            'file_size_mb': int(file_size_mb),
            'available_memory_mb': int(self.available_memory / (1024 * 1024)),
            'rationale': rationale,
            'warnings': self._generate_warnings(
                estimated_memory_mb,
                self.available_memory / (1024 * 1024),
                estimated_chunks
            )
        }

    def _generate_warnings(
        self,
        estimated_memory_mb: float,
        available_memory_mb: float,
        estimated_chunks: int
    ) -> list:
        """Generate warnings based on estimates."""
        warnings = []

        if estimated_memory_mb > available_memory_mb * 0.8:
            warnings.append(
                "WARNING: Estimated memory usage is high. Consider reducing chunk size or freeing memory."
            )

        if estimated_chunks > 10_000:
            warnings.append(
                "WARNING: Very large number of chunks. Processing may be slow. Consider using Parquet format."
            )

        if estimated_memory_mb < 100:
            warnings.append(
                "INFO: Memory usage is very low. Consider increasing chunk size for better performance."
            )

        return warnings

    def get_preset_recommendation(self, file_size_mb: float) -> Tuple[int, str]:
        """
        Get preset chunk size recommendation based on file size.

        Simple preset-based recommendations for quick guidance.

        Args:
            file_size_mb: File size in megabytes

        Returns:
            Tuple of (chunk_size, description)
        """
        if file_size_mb < 10:
            return 50_000, "Small file - use default chunk size"
        elif file_size_mb < 100:
            return 100_000, "Medium file - use moderate chunks"
        elif file_size_mb < 1000:
            return 500_000, "Large file - use large chunks"
        elif file_size_mb < 5000:
            return 1_000_000, "Very large file - use very large chunks"
        else:
            return 2_000_000, "Extreme file - use maximum chunk size"

    def generate_recommendation_report(
        self,
        file_path: str,
        file_format: str = 'csv',
        num_validations: int = 1,
        validation_complexity: str = 'simple'
    ) -> str:
        """
        Generate human-readable recommendation report.

        Args:
            file_path: Path to data file
            file_format: File format
            num_validations: Number of validations
            validation_complexity: Complexity level

        Returns:
            Formatted recommendation report
        """
        result = self.calculate_optimal_chunk_size(
            file_path, file_format, num_validations, validation_complexity
        )

        report = f"""
╔════════════════════════════════════════════════════════════════╗
║           DataK9 Chunk Size Recommendation                     ║
╚════════════════════════════════════════════════════════════════╝

File Information:
  Path: {file_path}
  Format: {file_format.upper()}
  Size: {result['file_size_mb']:,} MB
  Estimated Rows: {result['estimated_rows']:,}

System Resources:
  Available Memory: {result['available_memory_mb']:,} MB
  Validation Count: {num_validations}
  Validation Complexity: {validation_complexity}

Recommendation:
  Chunk Size: {result['recommended_chunk_size']:,} rows/chunk
  Estimated Chunks: {result['estimated_chunks']:,}
  Peak Memory Usage: ~{result['estimated_memory_mb']:,} MB

Rationale:
  {result['rationale']}

YAML Configuration:
  processing:
    chunk_size: {result['recommended_chunk_size']}
    max_sample_failures: 100
"""

        if result['warnings']:
            report += "\n⚠ Warnings:\n"
            for warning in result['warnings']:
                report += f"  • {warning}\n"

        return report


def cli_helper():
    """Interactive CLI helper for chunk size calculation."""
    import sys

    print("\n" + "="*70)
    print("  DataK9 Chunk Size Calculator - Interactive Helper")
    print("="*70 + "\n")

    # Get file path
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("Enter file path: ").strip()

    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        sys.exit(1)

    # Get file format
    file_ext = Path(file_path).suffix.lower().replace('.', '')
    file_format = input(f"File format [{file_ext}]: ").strip() or file_ext

    # Get validation count
    num_val_str = input("Number of validations [5]: ").strip()
    num_validations = int(num_val_str) if num_val_str else 5

    # Get complexity
    print("\nValidation Complexity:")
    print("  1) Simple (basic checks)")
    print("  2) Moderate (statistical checks)")
    print("  3) Complex (cross-file, duplicates)")
    print("  4) Heavy (ML-based, distributions)")
    complexity_choice = input("Select [1]: ").strip() or "1"

    complexity_map = {
        '1': 'simple',
        '2': 'moderate',
        '3': 'complex',
        '4': 'heavy'
    }
    complexity = complexity_map.get(complexity_choice, 'simple')

    # Calculate
    calculator = ChunkSizeCalculator()
    report = calculator.generate_recommendation_report(
        file_path, file_format, num_validations, complexity
    )

    print(report)


if __name__ == "__main__":
    cli_helper()
