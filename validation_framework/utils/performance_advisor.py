#!/usr/bin/env python3
"""
Performance Advisor for DataK9 Validation Framework

Provides intelligent recommendations for:
- File format conversion (CSV → Parquet)
- Chunk size optimization
- Memory usage warnings

Author: Daniel Edge
"""

import os
from pathlib import Path
from typing import Dict, List, Optional


class PerformanceAdvisor:
    """
    Analyzes file characteristics and provides performance recommendations.

    Provides non-blocking advisory warnings to help users optimize
    their data processing workflows.
    """

    # Thresholds for recommendations
    PARQUET_RECOMMENDATION_SIZE_MB = 100  # Recommend Parquet for files > 100MB
    LARGE_FILE_SIZE_MB = 1000  # Files > 1GB get additional warnings
    EXTREME_FILE_SIZE_MB = 5000  # Files > 5GB get urgent recommendations

    def __init__(self):
        """Initialize performance advisor."""
        pass

    def analyze_file(
        self,
        file_path: str,
        operation: str = 'validation'  # 'validation' or 'profile'
    ) -> Dict[str, any]:
        """
        Analyze file and generate performance recommendations.

        Args:
            file_path: Path to the data file
            operation: Type of operation (validation or profile)

        Returns:
            Dictionary with recommendations and warnings
        """
        if not os.path.exists(file_path):
            return {
                'warnings': [],
                'recommendations': [],
                'should_proceed': True
            }

        file_size_bytes = Path(file_path).stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        file_ext = Path(file_path).suffix.lower()

        warnings = []
        recommendations = []

        # Check if CSV and recommend Parquet
        if file_ext == '.csv':
            if file_size_mb > self.EXTREME_FILE_SIZE_MB:
                warnings.append({
                    'level': 'WARNING',
                    'message': f'Very large CSV file detected ({file_size_mb:.1f} MB)',
                    'recommendation': 'Strongly recommend converting to Parquet format',
                    'benefit': '10x faster processing, 70% less memory usage',
                    'action': self._get_conversion_command(file_path)
                })
            elif file_size_mb > self.LARGE_FILE_SIZE_MB:
                warnings.append({
                    'level': 'INFO',
                    'message': f'Large CSV file detected ({file_size_mb:.1f} MB)',
                    'recommendation': 'Consider converting to Parquet format',
                    'benefit': '3-10x faster processing, significant memory savings',
                    'action': self._get_conversion_command(file_path)
                })
            elif file_size_mb > self.PARQUET_RECOMMENDATION_SIZE_MB:
                recommendations.append({
                    'type': 'format',
                    'message': f'CSV file is {file_size_mb:.1f} MB',
                    'suggestion': 'Parquet format recommended for better performance',
                    'benefit': 'Faster processing and lower memory usage',
                    'optional': True
                })

        # Memory warnings for very large files
        if file_size_mb > self.EXTREME_FILE_SIZE_MB:
            warnings.append({
                'level': 'WARNING',
                'message': f'Processing {file_size_mb:.1f} MB file will require significant memory',
                'recommendation': 'Ensure sufficient RAM available (8GB+ recommended)',
                'action': 'Close unnecessary applications to free memory'
            })

        # Operation-specific recommendations
        if operation == 'profile':
            if file_size_mb > self.LARGE_FILE_SIZE_MB:
                recommendations.append({
                    'type': 'sampling',
                    'message': 'Large file profiling may take several minutes',
                    'suggestion': 'Consider using --sample flag to profile a subset',
                    'optional': True
                })

        return {
            'file_size_mb': file_size_mb,
            'file_format': file_ext.replace('.', '').upper(),
            'warnings': warnings,
            'recommendations': recommendations,
            'should_proceed': True,  # Never block, just inform
            'optimal_format': 'PARQUET' if file_ext == '.csv' and file_size_mb > self.PARQUET_RECOMMENDATION_SIZE_MB else file_ext.replace('.', '').upper()
        }

    def _get_conversion_command(self, csv_file: str) -> str:
        """Generate command to convert CSV to Parquet."""
        parquet_file = Path(csv_file).with_suffix('.parquet')
        return f'''
# Convert to Parquet (requires pandas and pyarrow):
python3 -c "
import pandas as pd
df = pd.read_csv('{csv_file}')
df.to_parquet('{parquet_file}', compression='snappy', index=False)
print(f'Converted: {parquet_file}')
"
'''

    def format_warnings_for_cli(self, analysis: Dict) -> List[str]:
        """
        Format warnings for CLI output.

        Args:
            analysis: Analysis results from analyze_file()

        Returns:
            List of formatted warning strings
        """
        lines = []

        # High-priority warnings
        for warning in analysis.get('warnings', []):
            level = warning['level']
            symbol = '⚠' if level == 'WARNING' else 'ℹ'

            lines.append(f"\n{symbol}  {warning['message']}")
            lines.append(f"   Recommendation: {warning['recommendation']}")
            if 'benefit' in warning:
                lines.append(f"   Benefit: {warning['benefit']}")

            if 'action' in warning:
                lines.append(f"   Action: {warning['action']}")

        # Lower-priority recommendations
        for rec in analysis.get('recommendations', []):
            if rec.get('optional'):
                lines.append(f"\n💡 Tip: {rec['message']}")
                lines.append(f"   {rec['suggestion']}")
                if 'benefit' in rec:
                    lines.append(f"   Benefit: {rec['benefit']}")

        return lines

    def get_chunk_size_recommendation(
        self,
        file_size_mb: float,
        file_format: str,
        num_validations: int = 5
    ) -> int:
        """
        Get recommended chunk size based on file characteristics.

        Args:
            file_size_mb: File size in megabytes
            file_format: File format (csv, parquet, json)
            num_validations: Number of validations to run

        Returns:
            Recommended chunk size (rows)
        """
        # Base recommendation on file size
        if file_size_mb < 10:
            base_chunk = 50_000
        elif file_size_mb < 100:
            base_chunk = 100_000
        elif file_size_mb < 1000:
            base_chunk = 500_000
        elif file_size_mb < 5000:
            base_chunk = 1_000_000
        else:
            base_chunk = 2_000_000

        # Adjust for format (Parquet can handle larger chunks)
        if file_format.lower() == 'parquet':
            base_chunk = int(base_chunk * 1.5)
        elif file_format.lower() == 'json':
            base_chunk = int(base_chunk * 0.7)

        # Adjust for validation count
        if num_validations > 20:
            base_chunk = int(base_chunk * 0.8)
        elif num_validations > 50:
            base_chunk = int(base_chunk * 0.6)

        # Cap at maximum
        return min(base_chunk, 2_000_000)

    def should_recommend_parquet(self, file_path: str) -> bool:
        """
        Check if file should get a Parquet conversion recommendation.

        Args:
            file_path: Path to file

        Returns:
            True if Parquet is recommended
        """
        if not os.path.exists(file_path):
            return False

        file_size_mb = Path(file_path).stat().st_size / (1024 * 1024)
        file_ext = Path(file_path).suffix.lower()

        return (file_ext == '.csv' and
                file_size_mb > self.PARQUET_RECOMMENDATION_SIZE_MB)


# Singleton instance
_advisor = None

def get_performance_advisor() -> PerformanceAdvisor:
    """Get global performance advisor instance."""
    global _advisor
    if _advisor is None:
        _advisor = PerformanceAdvisor()
    return _advisor
