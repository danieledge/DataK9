"""
Data profiling module for comprehensive data analysis.

Provides tools for profiling data files to understand structure, quality,
and characteristics before validation or loading.
"""

from validation_framework.profiler.engine import DataProfiler
from validation_framework.profiler.profile_result import ProfileResult

__all__ = ['DataProfiler', 'ProfileResult']
