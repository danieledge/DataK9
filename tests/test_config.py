"""
Unit tests for configuration parsing and validation.

Tests the ValidationConfig class and configuration file parsing.
"""

import pytest
import tempfile
import yaml
from pathlib import Path

from validation_framework.core.config import ValidationConfig, ConfigError
from validation_framework.core.results import Severity


@pytest.fixture
def valid_config_dict():
    """Fixture providing a valid configuration dictionary."""
    return {
        "validation_job": {
            "name": "Test Validation Job",
            "version": "1.0",
            "description": "Test description",
            "files": [
                {
                    "name": "test_file",
                    "path": "test.csv",
                    "format": "csv",
                    "validations": [
                        {
                            "type": "EmptyFileCheck",
                            "severity": "ERROR"
                        }
                    ]
                }
            ],
            "output": {
                "html_report": "report.html",
                "json_summary": "summary.json",
                "fail_on_error": True,
                "fail_on_warning": False
            },
            "processing": {
                "chunk_size": 50000,
                "parallel_files": False,
                "max_sample_failures": 100
            }
        }
    }


@pytest.fixture
def minimal_config_dict():
    """Fixture providing a minimal valid configuration."""
    return {
        "validation_job": {
            "name": "Minimal Job",
            "files": [
                {
                    "path": "test.csv"
                }
            ]
        }
    }


@pytest.fixture
def temp_config_file(valid_config_dict):
    """Fixture creating a temporary config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(valid_config_dict, f)
        temp_path = f.name

    yield temp_path

    # Cleanup
    Path(temp_path).unlink()


@pytest.mark.unit
class TestValidationConfigInit:
    """Tests for ValidationConfig initialization."""

    def test_init_with_valid_config(self, valid_config_dict):
        """Test initialization with a valid configuration."""
        config = ValidationConfig(valid_config_dict)

        assert config.job_name == "Test Validation Job"
        assert config.version == "1.0"
        assert config.description == "Test description"
        assert len(config.files) == 1
        assert config.html_report_path == "report.html"
        assert config.json_summary_path == "summary.json"
        assert config.fail_on_error is True
        assert config.fail_on_warning is False

    def test_init_with_minimal_config(self, minimal_config_dict):
        """Test initialization with minimal configuration (defaults)."""
        config = ValidationConfig(minimal_config_dict)

        assert config.job_name == "Minimal Job"
        assert config.version == "1.0"
        assert config.description is None
        assert len(config.files) == 1
        # Check defaults
        assert config.html_report_path == "validation_report.html"
        assert config.json_summary_path == "validation_summary.json"
        assert config.fail_on_error is True
        assert config.fail_on_warning is False
        assert config.chunk_size == 50000

    def test_init_missing_validation_job_raises_error(self):
        """Test that missing 'validation_job' key raises ConfigError."""
        invalid_config = {"some_key": "value"}

        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig(invalid_config)

        assert "validation_job" in str(exc_info.value).lower()

    def test_init_missing_files_raises_error(self):
        """Test that missing 'files' key raises ConfigError."""
        invalid_config = {
            "validation_job": {
                "name": "Test"
            }
        }

        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig(invalid_config)

        assert "at least one file" in str(exc_info.value).lower()

    def test_init_empty_files_list_raises_error(self):
        """Test that empty files list raises ConfigError."""
        invalid_config = {
            "validation_job": {
                "name": "Test",
                "files": []
            }
        }

        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig(invalid_config)

        assert "at least one file" in str(exc_info.value).lower()


@pytest.mark.unit
class TestValidationConfigFromYAML:
    """Tests for loading configuration from YAML files."""

    def test_from_yaml_with_valid_file(self, temp_config_file):
        """Test loading configuration from a valid YAML file."""
        config = ValidationConfig.from_yaml(temp_config_file)

        assert config.job_name == "Test Validation Job"
        assert len(config.files) == 1

    def test_from_yaml_with_nonexistent_file_raises_error(self):
        """Test that loading from non-existent file raises ConfigError."""
        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig.from_yaml("nonexistent.yaml")

        assert "not found" in str(exc_info.value).lower()


@pytest.mark.unit
class TestFilesParsing:
    """Tests for files configuration parsing."""

    def test_parse_file_with_all_options(self, valid_config_dict):
        """Test parsing file configuration with all options."""
        config = ValidationConfig(valid_config_dict)

        file_config = config.files[0]
        assert file_config["name"] == "test_file"
        assert file_config["path"] == "test.csv"
        assert file_config["format"] == "csv"
        assert file_config["delimiter"] == ","
        assert file_config["encoding"] == "utf-8"
        assert file_config["header"] == 0

    def test_parse_file_without_name_generates_default(self):
        """Test that files without names get default names."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {"path": "file1.csv"},
                    {"path": "file2.csv"}
                ]
            }
        }

        config = ValidationConfig(config_dict)

        assert config.files[0]["name"] == "file_0"
        assert config.files[1]["name"] == "file_1"

    def test_parse_file_missing_path_raises_error(self):
        """Test that missing file path raises ConfigError."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {"name": "test"}  # Missing path
                ]
            }
        }

        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig(config_dict)

        assert "path" in str(exc_info.value).lower()

    def test_format_inference_from_extension(self):
        """Test automatic format inference from file extension."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {"path": "data.csv"},
                    {"path": "data.xlsx"},
                    {"path": "data.parquet"},
                ]
            }
        }

        config = ValidationConfig(config_dict)

        assert config.files[0]["format"] == "csv"
        assert config.files[1]["format"] == "excel"
        assert config.files[2]["format"] == "parquet"


@pytest.mark.unit
class TestValidationsParsing:
    """Tests for validations configuration parsing."""

    def test_parse_validation_with_params(self):
        """Test parsing validation with parameters."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {
                        "path": "test.csv",
                        "validations": [
                            {
                                "type": "RangeCheck",
                                "severity": "ERROR",
                                "params": {
                                    "field": "amount",
                                    "min_value": 0,
                                    "max_value": 100
                                }
                            }
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        validation = config.files[0]["validations"][0]

        assert validation["type"] == "RangeCheck"
        assert validation["severity"] == Severity.ERROR
        assert validation["params"]["field"] == "amount"
        assert validation["params"]["min_value"] == 0

    def test_parse_validation_without_severity_defaults_to_error(self):
        """Test that validations without severity default to ERROR."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {
                        "path": "test.csv",
                        "validations": [
                            {"type": "EmptyFileCheck"}
                        ]
                    }
                ]
            }
        }

        config = ValidationConfig(config_dict)
        validation = config.files[0]["validations"][0]

        assert validation["severity"] == Severity.ERROR

    def test_parse_validation_with_invalid_severity_raises_error(self):
        """Test that invalid severity raises ConfigError."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {
                        "path": "test.csv",
                        "validations": [
                            {
                                "type": "EmptyFileCheck",
                                "severity": "INVALID"
                            }
                        ]
                    }
                ]
            }
        }

        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig(config_dict)

        assert "severity" in str(exc_info.value).lower()

    def test_parse_validation_missing_type_raises_error(self):
        """Test that validation without type raises ConfigError."""
        config_dict = {
            "validation_job": {
                "name": "Test",
                "files": [
                    {
                        "path": "test.csv",
                        "validations": [
                            {"severity": "ERROR"}  # Missing type
                        ]
                    }
                ]
            }
        }

        with pytest.raises(ConfigError) as exc_info:
            ValidationConfig(config_dict)

        assert "type" in str(exc_info.value).lower()


@pytest.mark.unit
class TestConfigMethods:
    """Tests for ValidationConfig methods."""

    def test_get_file_by_name(self, valid_config_dict):
        """Test retrieving file configuration by name."""
        config = ValidationConfig(valid_config_dict)
        file_config = config.get_file_by_name("test_file")

        assert file_config["name"] == "test_file"
        assert file_config["path"] == "test.csv"

    def test_get_file_by_name_not_found_raises_error(self, valid_config_dict):
        """Test that getting non-existent file raises ConfigError."""
        config = ValidationConfig(valid_config_dict)

        with pytest.raises(ConfigError) as exc_info:
            config.get_file_by_name("nonexistent")

        assert "not found" in str(exc_info.value).lower()

    def test_to_dict(self, valid_config_dict):
        """Test converting configuration to dictionary."""
        config = ValidationConfig(valid_config_dict)
        config_dict = config.to_dict()

        assert config_dict["job_name"] == "Test Validation Job"
        assert config_dict["version"] == "1.0"
        assert len(config_dict["files"]) == 1
        assert config_dict["html_report_path"] == "report.html"
        assert config_dict["fail_on_error"] is True
