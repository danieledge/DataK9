"""
Tests for CLI parameter parity between profiler and validator.

Ensures that both commands have consistent options for:
- CSV handling (encoding, quoting, skip-rows, delimiter)
- Batch/Autosys features (timeout, lock-file, exit-file)
- Logging (log-level, log-file)
"""

import pytest
from click.testing import CliRunner

from validation_framework.cli import cli


class TestValidatorCLIOptions:
    """Test validator CLI has all required options."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_validate_has_encoding_option(self, runner):
        """Test validate command has --encoding option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--encoding' in result.output or '-e' in result.output

    def test_validate_has_quoting_option(self, runner):
        """Test validate command has --quoting option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--quoting' in result.output

    def test_validate_has_skip_rows_option(self, runner):
        """Test validate command has --skip-rows option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--skip-rows' in result.output

    def test_validate_has_delimiter_option(self, runner):
        """Test validate command has --delimiter option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--delimiter' in result.output or '-d' in result.output

    def test_validate_has_timeout_option(self, runner):
        """Test validate command has --timeout option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--timeout' in result.output

    def test_validate_has_lock_file_option(self, runner):
        """Test validate command has --lock-file option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--lock-file' in result.output

    def test_validate_has_exit_file_option(self, runner):
        """Test validate command has --exit-file option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--exit-file' in result.output

    def test_validate_has_log_level_option(self, runner):
        """Test validate command has --log-level option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--log-level' in result.output

    def test_validate_has_log_file_option(self, runner):
        """Test validate command has --log-file option."""
        result = runner.invoke(cli, ['validate', '--help'])
        assert '--log-file' in result.output


class TestProfilerCLIOptions:
    """Test profiler CLI has all required options."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_profile_has_encoding_option(self, runner):
        """Test profile command has --encoding option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--encoding' in result.output or '-e' in result.output

    def test_profile_has_quoting_option(self, runner):
        """Test profile command has --quoting option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--quoting' in result.output

    def test_profile_has_skip_rows_option(self, runner):
        """Test profile command has --skip-rows option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--skip-rows' in result.output

    def test_profile_has_delimiter_option(self, runner):
        """Test profile command has --delimiter option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--delimiter' in result.output or '-d' in result.output

    def test_profile_has_timeout_option(self, runner):
        """Test profile command has --timeout option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--timeout' in result.output

    def test_profile_has_lock_file_option(self, runner):
        """Test profile command has --lock-file option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--lock-file' in result.output

    def test_profile_has_exit_file_option(self, runner):
        """Test profile command has --exit-file option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--exit-file' in result.output

    def test_profile_has_log_level_option(self, runner):
        """Test profile command has --log-level option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--log-level' in result.output

    def test_profile_has_log_file_option(self, runner):
        """Test profile command has --log-file option."""
        result = runner.invoke(cli, ['profile', '--help'])
        assert '--log-file' in result.output


class TestCLIParity:
    """Test CLI parity between profiler and validator."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_csv_options_parity(self, runner):
        """Test that both commands have the same CSV options."""
        validate_help = runner.invoke(cli, ['validate', '--help']).output
        profile_help = runner.invoke(cli, ['profile', '--help']).output

        csv_options = ['--delimiter', '--encoding', '--quoting', '--skip-rows']

        for option in csv_options:
            assert option in validate_help, f"Validator missing {option}"
            assert option in profile_help, f"Profiler missing {option}"

    def test_batch_options_parity(self, runner):
        """Test that both commands have the same batch/Autosys options."""
        validate_help = runner.invoke(cli, ['validate', '--help']).output
        profile_help = runner.invoke(cli, ['profile', '--help']).output

        batch_options = ['--timeout', '--lock-file', '--exit-file']

        for option in batch_options:
            assert option in validate_help, f"Validator missing {option}"
            assert option in profile_help, f"Profiler missing {option}"

    def test_logging_options_parity(self, runner):
        """Test that both commands have the same logging options."""
        validate_help = runner.invoke(cli, ['validate', '--help']).output
        profile_help = runner.invoke(cli, ['profile', '--help']).output

        logging_options = ['--log-level', '--log-file']

        for option in logging_options:
            assert option in validate_help, f"Validator missing {option}"
            assert option in profile_help, f"Profiler missing {option}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
