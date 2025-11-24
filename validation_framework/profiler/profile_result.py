"""
Data structures for storing profiling results.

Contains classes for holding comprehensive data profile information
including schema, statistics, quality metrics, and suggestions.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import datetime
import numpy as np


def convert_numpy_types(obj):
    """
    Recursively convert numpy types to Python native types for JSON serialization.

    Args:
        obj: Any object that might contain numpy types

    Returns:
        Object with numpy types converted to Python types
    """
    if isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    else:
        return obj


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
            "confidence": round(float(self.confidence), 3),
            "is_known": bool(self.is_known),
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
        whitespace_null_count: Number of whitespace-only values treated as null
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
    whitespace_null_count: int = 0
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

    # Sampling metadata for transparency
    semantic_type: Optional[str] = None  # Detected semantic type (id, date, amount, etc.)
    sample_size: Optional[int] = None  # Number of rows sampled for statistics
    sampling_strategy: Optional[str] = None  # Description of sampling approach

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "count": int(self.count) if self.count is not None else 0,
            "null_count": int(self.null_count) if self.null_count is not None else 0,
            "null_percentage": round(float(self.null_percentage), 2),
            "unique_count": int(self.unique_count) if self.unique_count is not None else 0,
            "unique_percentage": round(float(self.unique_percentage), 2),
            "cardinality": round(float(self.cardinality), 3),
        }

        # Add numeric statistics if present
        if self.mean is not None:
            result["mean"] = round(float(self.mean), 3)
        if self.median is not None:
            result["median"] = round(float(self.median), 3)
        if self.std_dev is not None:
            result["std_dev"] = round(float(self.std_dev), 3)
        if self.min_value is not None:
            result["min_value"] = str(self.min_value)
        if self.max_value is not None:
            result["max_value"] = str(self.max_value)
        if self.quartiles:
            # Convert quartiles dict values to native Python floats
            result["quartiles"] = {k: round(float(v), 3) for k, v in self.quartiles.items()}

        # Add frequency statistics
        if self.mode is not None:
            result["mode"] = str(self.mode)
            result["mode_frequency"] = int(self.mode_frequency) if self.mode_frequency is not None else 0
        result["top_values"] = self.top_values[:10]  # Limit to top 10

        # Add string statistics if present
        if self.min_length is not None:
            result["min_length"] = int(self.min_length)
            result["max_length"] = int(self.max_length)
            result["avg_length"] = round(float(self.avg_length), 2) if self.avg_length else None

        # Add pattern samples
        result["pattern_samples"] = self.pattern_samples[:10]

        # Add sampling metadata for transparency
        if self.semantic_type:
            result["semantic_type"] = self.semantic_type
        if self.sample_size is not None:
            result["sample_size"] = int(self.sample_size)
        if self.sampling_strategy:
            result["sampling_strategy"] = self.sampling_strategy

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
        issues: List of detected quality issues (actual problems)
        observations: List of informational insights (not problems)
    """
    completeness: float = 0.0
    validity: float = 0.0
    uniqueness: float = 0.0
    consistency: float = 0.0
    overall_score: float = 0.0
    issues: List[str] = field(default_factory=list)
    observations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "completeness": round(self.completeness, 2),
            "validity": round(self.validity, 2),
            "uniqueness": round(self.uniqueness, 2),
            "consistency": round(self.consistency, 2),
            "overall_score": round(self.overall_score, 2),
            "issues": self.issues,
            "observations": self.observations
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
        temporal_analysis: Temporal analysis results (for datetime columns)
        pii_info: PII detection results
        semantic_info: Semantic understanding with FIBO-derived tags
    """
    name: str
    type_info: TypeInference
    statistics: ColumnStatistics
    quality: QualityMetrics
    temporal_analysis: Optional[Dict[str, Any]] = None
    pii_info: Optional[Dict[str, Any]] = None
    semantic_info: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "name": self.name,
            "type_info": self.type_info.to_dict(),
            "statistics": self.statistics.to_dict(),
            "quality": self.quality.to_dict()
        }

        # Add Phase 1 enhancements if present (convert numpy types for JSON)
        if self.temporal_analysis:
            result["temporal_analysis"] = convert_numpy_types(self.temporal_analysis)
        if self.pii_info:
            result["pii_info"] = convert_numpy_types(self.pii_info)
        if self.semantic_info:
            result["semantic_info"] = convert_numpy_types(self.semantic_info)

        return result


@dataclass
class CorrelationResult:
    """
    Correlation between two columns.

    Attributes:
        column1: First column name
        column2: Second column name
        correlation: Correlation coefficient (-1 to 1)
        type: Type of correlation (pearson, spearman, etc.)
        strength: Correlation strength classification (weak, moderate, strong, very strong)
        direction: Correlation direction (positive, negative, none)
        p_value: Statistical significance p-value
        is_significant: Whether correlation is statistically significant
    """
    column1: str
    column2: str
    correlation: float
    type: str = "pearson"
    strength: Optional[str] = None
    direction: Optional[str] = None
    p_value: Optional[float] = None
    is_significant: Optional[bool] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
            "column1": self.column1,
            "column2": self.column2,
            "correlation": round(self.correlation, 3),
            "type": self.type
        }

        # Add enhanced correlation fields if present
        if self.strength:
            result["strength"] = self.strength
        if self.direction:
            result["direction"] = self.direction
        if self.p_value is not None:
            result["p_value"] = round(float(self.p_value), 6)
        if self.is_significant is not None:
            result["is_significant"] = bool(self.is_significant)  # Convert numpy bool to Python bool

        return result


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
        enhanced_correlations: Enhanced multi-method correlation analysis results
        dataset_privacy_risk: Dataset-level privacy risk assessment
        file_metadata: Additional file metadata (compression, row groups, etc.)
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
    enhanced_correlations: Optional[Dict[str, Any]] = None
    dataset_privacy_risk: Optional[Dict[str, Any]] = None
    file_metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        result = {
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

        # Add Phase 1 enhancements if present (convert numpy types for JSON)
        if self.enhanced_correlations:
            result["enhanced_correlations"] = convert_numpy_types(self.enhanced_correlations)
        if self.dataset_privacy_risk:
            result["dataset_privacy_risk"] = convert_numpy_types(self.dataset_privacy_risk)
        if self.file_metadata:
            result["file_metadata"] = convert_numpy_types(self.file_metadata)

        return result
