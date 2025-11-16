"""
Data structures for storing profiling results.

Contains classes for holding comprehensive data profile information
including schema, statistics, quality metrics, and suggestions.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime


@dataclass
class TypeInference:
    """
    Type inference result for a column.

    Attributes:
        declared_type: The type declared in schema (if any)
        inferred_type: The type inferred from actual data
        confidence: Confidence level (0.0 to 1.0)
        is_known: True if type is definitively known (from schema), False if inferred
        type_conflicts: List of conflicting type samples
        sample_values: Sample values used for inference
    """
    declared_type: Optional[str] = None
    inferred_type: str = "unknown"
    confidence: float = 0.0
    is_known: bool = False
    type_conflicts: List[Dict[str, Any]] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "declared_type": self.declared_type,
            "inferred_type": self.inferred_type,
            "confidence": round(self.confidence, 3),
            "is_known": self.is_known,
            "type_conflicts": self.type_conflicts,
            "sample_values": self.sample_values[:10]  # Limit samples
        }


@dataclass
class ColumnStatistics:
    """
    Statistical information for a column.

    Attributes:
        count: Total number of rows
        null_count: Number of null values
        null_percentage: Percentage of nulls
        unique_count: Number of unique values
        unique_percentage: Percentage of unique values
        cardinality: Ratio of unique to total values
        min_value: Minimum value (for numeric/date types)
        max_value: Maximum value (for numeric/date types)
        mean: Mean value (for numeric types)
        median: Median value (for numeric types)
        std_dev: Standard deviation (for numeric types)
        quartiles: Q1, Q2 (median), Q3 (for numeric types)
        mode: Most common value
        mode_frequency: Frequency of mode
        top_values: Most common values with frequencies
        min_length: Minimum string length (for string types)
        max_length: Maximum string length (for string types)
        avg_length: Average string length (for string types)
        pattern_samples: Common patterns detected
    """
    count: int = 0
    null_count: int = 0
    null_percentage: float = 0.0
    unique_count: int = 0
    unique_percentage: float = 0.0
    cardinality: float = 0.0

    # Numeric/date statistics
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    std_dev: Optional[float] = None
    quartiles: Optional[Dict[str, float]] = None

    # Frequency statistics
    mode: Optional[Any] = None
    mode_frequency: Optional[int] = None
    top_values: List[Dict[str, Any]] = field(default_factory=list)

    # String statistics
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None

    # Pattern analysis
    pattern_samples: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "count": self.count,
            "null_count": self.null_count,
            "null_percentage": round(self.null_percentage, 2),
            "unique_count": self.unique_count,
            "unique_percentage": round(self.unique_percentage, 2),
            "cardinality": round(self.cardinality, 3),
        }

        # Add numeric statistics if present
        if self.mean is not None:
            result["mean"] = round(self.mean, 3)
        if self.median is not None:
            result["median"] = round(self.median, 3)
        if self.std_dev is not None:
            result["std_dev"] = round(self.std_dev, 3)
        if self.min_value is not None:
            result["min_value"] = str(self.min_value)
        if self.max_value is not None:
            result["max_value"] = str(self.max_value)
        if self.quartiles:
            result["quartiles"] = self.quartiles

        # Add frequency statistics
        if self.mode is not None:
            result["mode"] = str(self.mode)
            result["mode_frequency"] = self.mode_frequency
        result["top_values"] = self.top_values[:10]  # Limit to top 10

        # Add string statistics if present
        if self.min_length is not None:
            result["min_length"] = self.min_length
            result["max_length"] = self.max_length
            result["avg_length"] = round(self.avg_length, 2) if self.avg_length else None

        # Add pattern samples
        result["pattern_samples"] = self.pattern_samples[:10]

        return result


@dataclass
class QualityMetrics:
    """
    Data quality metrics for a column.

    Attributes:
        completeness: Percentage of non-null values (0-100)
        validity: Percentage of values matching expected type (0-100)
        uniqueness: Percentage of unique values (0-100)
        consistency: Consistency score based on pattern matching (0-100)
        overall_score: Overall quality score (0-100)
        issues: List of detected quality issues
    """
    completeness: float = 0.0
    validity: float = 0.0
    uniqueness: float = 0.0
    consistency: float = 0.0
    overall_score: float = 0.0
    issues: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "completeness": round(self.completeness, 2),
            "validity": round(self.validity, 2),
            "uniqueness": round(self.uniqueness, 2),
            "consistency": round(self.consistency, 2),
            "overall_score": round(self.overall_score, 2),
            "issues": self.issues
        }


@dataclass
class ColumnProfile:
    """
    Complete profile for a single column.

    Attributes:
        name: Column name
        type_info: Type inference information
        statistics: Statistical information
        quality: Quality metrics
    """
    name: str
    type_info: TypeInference
    statistics: ColumnStatistics
    quality: QualityMetrics

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "type_info": self.type_info.to_dict(),
            "statistics": self.statistics.to_dict(),
            "quality": self.quality.to_dict()
        }


@dataclass
class CorrelationResult:
    """
    Correlation between two columns.

    Attributes:
        column1: First column name
        column2: Second column name
        correlation: Correlation coefficient (-1 to 1)
        type: Type of correlation (pearson, spearman, etc.)
    """
    column1: str
    column2: str
    correlation: float
    type: str = "pearson"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "column1": self.column1,
            "column2": self.column2,
            "correlation": round(self.correlation, 3),
            "type": self.type
        }


@dataclass
class ValidationSuggestion:
    """
    Suggested validation based on profile analysis.

    Attributes:
        validation_type: Type of validation to apply
        severity: Suggested severity (ERROR or WARNING)
        params: Parameters for the validation
        reason: Explanation for the suggestion
        confidence: Confidence in the suggestion (0-100)
    """
    validation_type: str
    severity: str
    params: Dict[str, Any]
    reason: str
    confidence: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "validation_type": self.validation_type,
            "severity": self.severity,
            "params": self.params,
            "reason": self.reason,
            "confidence": round(self.confidence, 2)
        }


@dataclass
class ProfileResult:
    """
    Complete data profiling result.

    Attributes:
        file_name: Name of the profiled file
        file_path: Path to the profiled file
        file_size_bytes: Size of file in bytes
        format: File format (csv, excel, etc.)
        row_count: Total number of rows
        column_count: Total number of columns
        profiled_at: Timestamp of profiling
        processing_time_seconds: Time taken to profile
        columns: Profile for each column
        correlations: Correlations between columns
        suggested_validations: Suggested validations
        overall_quality_score: Overall data quality score (0-100)
        generated_config_yaml: Auto-generated validation config
        generated_config_command: CLI command to run the generated config
    """
    file_name: str
    file_path: str
    file_size_bytes: int
    format: str
    row_count: int
    column_count: int
    profiled_at: datetime
    processing_time_seconds: float
    columns: List[ColumnProfile]
    correlations: List[CorrelationResult] = field(default_factory=list)
    suggested_validations: List[ValidationSuggestion] = field(default_factory=list)
    overall_quality_score: float = 0.0
    generated_config_yaml: Optional[str] = None
    generated_config_command: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "file_size_mb": round(self.file_size_bytes / (1024 * 1024), 2),
            "format": self.format,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "profiled_at": self.profiled_at.isoformat(),
            "processing_time_seconds": round(self.processing_time_seconds, 2),
            "overall_quality_score": round(self.overall_quality_score, 2),
            "columns": [col.to_dict() for col in self.columns],
            "correlations": [corr.to_dict() for corr in self.correlations],
            "suggested_validations": [sugg.to_dict() for sugg in self.suggested_validations],
            "generated_config_yaml": self.generated_config_yaml,
            "generated_config_command": self.generated_config_command
        }
