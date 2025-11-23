"""Configuration parsing and validation."""

import yaml
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from validation_framework.core.results import Severity
from validation_framework.core.exceptions import (
    ConfigError,
    YAMLSizeError,
    ConfigValidationError
)
from validation_framework.core.constants import (
    MAX_YAML_FILE_SIZE,
    MAX_YAML_NESTING_DEPTH,
    MAX_YAML_KEY_COUNT,
    MAX_STRING_LENGTH,
    DEFAULT_CHUNK_SIZE,
    MAX_SAMPLE_FAILURES
)
from validation_framework.utils.path_patterns import PathPatternExpander


# Alias for backwards compatibility
YAMLStructureError = ConfigValidationError


class ValidationConfig:
    """Configuration for a validation job."""

    # Security limits for YAML files - imported from constants module
    # These prevent DoS attacks via malicious configuration files
    MAX_YAML_FILE_SIZE = MAX_YAML_FILE_SIZE
    MAX_YAML_NESTING_DEPTH = MAX_YAML_NESTING_DEPTH
    MAX_YAML_KEYS = MAX_YAML_KEY_COUNT

    def __init__(self, config_dict: Dict[str, Any], run_timestamp: Optional[datetime] = None):
        """
        Initialize from configuration dictionary.

        Args:
            config_dict: Configuration dictionary
            run_timestamp: Optional timestamp for pattern expansion (for consistency)
        """
        self.raw_config = config_dict
        self._run_timestamp = run_timestamp or datetime.now()
        self._pattern_expander = PathPatternExpander(run_timestamp=self._run_timestamp)
        self._parse_config()

    @classmethod
    def from_yaml(cls, config_path: str) -> "ValidationConfig":
        """
        Load configuration from YAML file with security validations.

        Security protections:
        - File size limit: 10 MB
        - Nesting depth limit: 20 levels
        - Total keys limit: 10,000 keys

        Args:
            config_path: Path to YAML configuration file

        Returns:
            ValidationConfig instance

        Raises:
            ConfigError: If file not found or invalid
            YAMLSizeError: If file exceeds size limit
            YAMLStructureError: If YAML structure is too complex
        """
        config_file = Path(config_path)
        if not config_file.exists():
            raise ConfigError(f"Configuration file not found: {config_path}")

        # Check file size to prevent DoS attacks
        file_size = os.path.getsize(config_file)
        if file_size > cls.MAX_YAML_FILE_SIZE:
            raise YAMLSizeError(
                f"Configuration file too large: {file_size:,} bytes. "
                f"Maximum allowed: {cls.MAX_YAML_FILE_SIZE:,} bytes ({cls.MAX_YAML_FILE_SIZE // (1024*1024)} MB)"
            )

        # Read and parse YAML file
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                # Use safe_load to prevent code execution
                config_dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigError(f"Failed to parse YAML file: {str(e)}")
        except UnicodeDecodeError as e:
            raise ConfigError(f"Invalid file encoding (expected UTF-8): {str(e)}")

        # Validate YAML structure to prevent resource exhaustion
        if config_dict is not None:
            cls._validate_yaml_structure(config_dict)

        return cls(config_dict)

    @classmethod
    def _validate_yaml_structure(cls, obj: Any, current_depth: int = 0, total_keys: List[int] = None) -> None:
        """
        Validate YAML structure to prevent DoS attacks.

        Checks for:
        - Excessive nesting depth (prevents stack overflow)
        - Too many keys/items (prevents memory exhaustion)

        Args:
            obj: Object to validate (dict, list, or primitive)
            current_depth: Current nesting depth
            total_keys: Mutable list with single element tracking total key count

        Raises:
            YAMLStructureError: If structure is too complex
        """
        if total_keys is None:
            total_keys = [0]

        # Check nesting depth
        if current_depth > cls.MAX_YAML_NESTING_DEPTH:
            raise YAMLStructureError(
                f"YAML nesting depth exceeds maximum of {cls.MAX_YAML_NESTING_DEPTH} levels. "
                f"This may indicate a malformed or malicious configuration file."
            )

        # Check total number of keys/items
        if total_keys[0] > cls.MAX_YAML_KEYS:
            raise YAMLStructureError(
                f"YAML structure contains more than {cls.MAX_YAML_KEYS:,} keys/items. "
                f"This may indicate a malformed or malicious configuration file."
            )

        # Recursively validate structure
        if isinstance(obj, dict):
            total_keys[0] += len(obj)
            if total_keys[0] > cls.MAX_YAML_KEYS:
                raise YAMLStructureError(
                    f"YAML structure contains more than {cls.MAX_YAML_KEYS:,} keys/items."
                )

            for key, value in obj.items():
                # Validate key is reasonable length
                if isinstance(key, str) and len(key) > 1000:
                    raise YAMLStructureError(
                        f"YAML key exceeds maximum length of 1000 characters: '{key[:50]}...'"
                    )
                cls._validate_yaml_structure(value, current_depth + 1, total_keys)

        elif isinstance(obj, list):
            total_keys[0] += len(obj)
            if total_keys[0] > cls.MAX_YAML_KEYS:
                raise YAMLStructureError(
                    f"YAML structure contains more than {cls.MAX_YAML_KEYS:,} keys/items."
                )

            for item in obj:
                cls._validate_yaml_structure(item, current_depth + 1, total_keys)

        elif isinstance(obj, str):
            # Check for unreasonably long strings (potential DoS)
            if len(obj) > MAX_STRING_LENGTH:
                raise YAMLStructureError(
                    f"YAML contains string exceeding maximum length ({MAX_STRING_LENGTH:,} bytes): '{obj[:50]}...'"
                )

    def _parse_config(self) -> None:
        """Parse and validate configuration."""
        if "validation_job" not in self.raw_config:
            raise ConfigError("Configuration must have 'validation_job' key")

        job_config = self.raw_config["validation_job"]

        # Job metadata
        self.job_name: str = job_config.get("name", "Unnamed Validation Job")
        self.version: str = job_config.get("version", "1.0")
        self.description: Optional[str] = job_config.get("description", None)

        # Files to validate - support both nested and flat (backwards compat) structure
        # NEW format: validation_job.files
        # OLD format: files at root level (backwards compatibility)
        if "files" in job_config:
            files_list = job_config["files"]
        elif "files" in self.raw_config:
            # Backwards compatibility: files at root level
            files_list = self.raw_config["files"]
        else:
            raise ConfigError("Configuration must specify at least one file to validate")

        if not files_list:
            raise ConfigError("Configuration must specify at least one file to validate")

        self.files = self._parse_files(files_list)

        # Output configuration - support both nested and flat structure
        output_config = job_config.get("output", self.raw_config.get("output", {}))

        # Store templates for pattern expansion
        self._html_report_template = output_config.get("html_report", "validation_report.html")
        self._json_summary_template = output_config.get("json_summary", "validation_summary.json")

        # Expand patterns with job context
        context = {'job_name': self.job_name}
        self.html_report_path = self._pattern_expander.expand(self._html_report_template, context)
        self.json_summary_path = self._pattern_expander.expand(self._json_summary_template, context)

        self.fail_on_error = output_config.get("fail_on_error", True)
        self.fail_on_warning = output_config.get("fail_on_warning", False)

        # Processing options
        processing = job_config.get("processing", self.raw_config.get("processing", {}))
        self.chunk_size = processing.get("chunk_size", DEFAULT_CHUNK_SIZE)
        self.parallel_files = processing.get("parallel_files", False)
        self.max_sample_failures = processing.get("max_sample_failures", MAX_SAMPLE_FAILURES)

    def _parse_files(self, files_config: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse files configuration.

        Supports both file and database sources:
        - File sources: path points to file, format is csv/excel/json/parquet
        - Database sources: path is connection string, format is 'database',
          table/query specify what to validate
        """
        parsed_files = []

        for idx, file_config in enumerate(files_config):
            # Check if this is a database source
            format_type = file_config.get("format", "")

            # For database sources, path is optional (can be specified separately)
            # For file sources, path is required
            if format_type != "database" and "path" not in file_config:
                raise ConfigError(f"File configuration {idx} missing 'path'")

            # Infer format if not specified
            if "path" in file_config and not format_type:
                format_type = self._infer_format(file_config["path"])

            # Build parsed file configuration
            parsed_file = {
                "name": file_config.get("name", f"file_{idx}"),
                "path": file_config.get("path", ""),
                "format": format_type,
                "validations": self._parse_validations(file_config.get("validations", [])),
                "metadata": file_config.get("metadata", {}),
            }

            # Add format-specific fields
            if format_type == "database":
                # Database-specific configuration
                parsed_file.update({
                    "connection_string": file_config.get("path", file_config.get("connection_string", "")),
                    "table": file_config.get("table"),
                    "query": file_config.get("query"),
                    "db_type": file_config.get("db_type"),  # Optional, will auto-detect if not provided
                    "max_rows": file_config.get("max_rows"),  # Production safety limit
                    "sample_percent": file_config.get("sample_percent"),  # Sample validation
                })

                # Validate database configuration
                if not parsed_file["connection_string"]:
                    raise ConfigError(
                        f"Database source {idx} must specify 'path' (connection string) "
                        f"or 'connection_string'"
                    )
                if not parsed_file["table"] and not parsed_file["query"]:
                    raise ConfigError(
                        f"Database source {idx} must specify either 'table' or 'query'"
                    )
            else:
                # File-specific configuration
                parsed_file.update({
                    "delimiter": file_config.get("delimiter", ","),
                    "encoding": file_config.get("encoding", "utf-8"),
                    "header": file_config.get("header", 0),
                })

            parsed_files.append(parsed_file)

        return parsed_files

    def _parse_validations(self, validations_config: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse validations configuration."""
        parsed_validations = []

        for validation in validations_config:
            if "type" not in validation:
                raise ConfigError("Validation must specify 'type'")

            # Handle severity - may be string or already a Severity enum
            severity_val = validation.get("severity", "ERROR")
            if isinstance(severity_val, Severity):
                severity = severity_val
            else:
                severity_str = severity_val.upper()
                try:
                    severity = Severity[severity_str]
                except KeyError:
                    raise ConfigError(f"Invalid severity: {severity_str}. Must be ERROR or WARNING")

            parsed_validation = {
                "type": validation["type"],
                "severity": severity,
                "params": validation.get("params", {}),
                "enabled": validation.get("enabled", True),
                "description": validation.get("description", ""),
                "condition": validation.get("condition", None),
                "sampling": validation.get("sampling", {}),  # Preserve sampling configuration
            }

            parsed_validations.append(parsed_validation)

        return parsed_validations

    def _infer_format(self, file_path: str) -> str:
        """Infer file format from extension."""
        suffix = Path(file_path).suffix.lower()
        format_map = {
            ".csv": "csv",
            ".tsv": "csv",
            ".txt": "csv",
            ".xls": "excel",
            ".xlsx": "excel",
            ".parquet": "parquet",
            ".json": "json",
            ".jsonl": "jsonl",
        }
        return format_map.get(suffix, "csv")

    def get_file_by_name(self, name: str) -> Dict[str, Any]:
        """Get file configuration by name."""
        for file_config in self.files:
            if file_config["name"] == name:
                return file_config
        raise ConfigError(f"File not found: {name}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "job_name": self.job_name,
            "version": self.version,
            "files": [f["name"] for f in self.files],
            "html_report_path": self.html_report_path,
            "json_summary_path": self.json_summary_path,
            "fail_on_error": self.fail_on_error,
            "fail_on_warning": self.fail_on_warning,
        }
