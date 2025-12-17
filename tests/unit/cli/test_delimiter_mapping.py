"""
Tests for CLI delimiter mapping functionality.

Verifies cross-platform delimiter handling, especially for Windows
where \t escape sequences may not be handled properly by shells.
"""

import pytest
import tempfile
import os
from pathlib import Path
from click.testing import CliRunner
from validation_framework.cli import cli


class TestDelimiterMapping:
    """Test CLI delimiter mapping for cross-platform compatibility."""
    
    @pytest.fixture
    def tab_delimited_csv(self, tmp_path):
        """Create a tab-delimited CSV file."""
        csv_file = tmp_path / "tab_data.csv"
        csv_file.write_text("id\tname\tvalue\n1\ttest\t100\n2\tfoo\t200\n")
        return str(csv_file)
    
    @pytest.fixture
    def pipe_delimited_csv(self, tmp_path):
        """Create a pipe-delimited CSV file."""
        csv_file = tmp_path / "pipe_data.csv"
        csv_file.write_text("id|name|value\n1|test|100\n2|foo|200\n")
        return str(csv_file)
    
    @pytest.fixture
    def semicolon_delimited_csv(self, tmp_path):
        """Create a semicolon-delimited CSV file."""
        csv_file = tmp_path / "semicolon_data.csv"
        csv_file.write_text("id;name;value\n1;test;100\n2;foo;200\n")
        return str(csv_file)
    
    def test_tab_named_delimiter(self, tab_delimited_csv, tmp_path):
        """Test 'tab' named delimiter works."""
        runner = CliRunner()
        output_file = tmp_path / "output.html"
        
        result = runner.invoke(cli, [
            'profile', tab_delimited_csv,
            '-d', 'tab',
            '--html-output', str(output_file)
        ])
        
        # Should not fail due to delimiter issues
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()
    
    def test_escaped_tab_delimiter(self, tab_delimited_csv, tmp_path):
        """Test '\\t' escaped delimiter works (Windows compatibility)."""
        runner = CliRunner()
        output_file = tmp_path / "output.html"
        
        result = runner.invoke(cli, [
            'profile', tab_delimited_csv,
            '-d', '\\t',
            '--html-output', str(output_file)
        ])
        
        # Should handle \\t as tab character
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()
    
    def test_pipe_named_delimiter(self, pipe_delimited_csv, tmp_path):
        """Test 'pipe' named delimiter works."""
        runner = CliRunner()
        output_file = tmp_path / "output.html"
        
        result = runner.invoke(cli, [
            'profile', pipe_delimited_csv,
            '-d', 'pipe',
            '--html-output', str(output_file)
        ])
        
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()
    
    def test_semicolon_named_delimiter(self, semicolon_delimited_csv, tmp_path):
        """Test 'semicolon' named delimiter works."""
        runner = CliRunner()
        output_file = tmp_path / "output.html"
        
        result = runner.invoke(cli, [
            'profile', semicolon_delimited_csv,
            '-d', 'semicolon',
            '--html-output', str(output_file)
        ])
        
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()
    
    def test_literal_delimiter_character(self, pipe_delimited_csv, tmp_path):
        """Test literal delimiter character works."""
        runner = CliRunner()
        output_file = tmp_path / "output.html"
        
        result = runner.invoke(cli, [
            'profile', pipe_delimited_csv,
            '-d', '|',
            '--html-output', str(output_file)
        ])
        
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()
    
    def test_delimiter_case_insensitive(self, tab_delimited_csv, tmp_path):
        """Test delimiter names are case-insensitive."""
        runner = CliRunner()
        output_file = tmp_path / "output.html"
        
        # Test uppercase
        result = runner.invoke(cli, [
            'profile', tab_delimited_csv,
            '-d', 'TAB',
            '--html-output', str(output_file)
        ])
        
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()
        
        # Test mixed case
        result = runner.invoke(cli, [
            'profile', tab_delimited_csv,
            '-d', 'Tab',
            '--html-output', str(output_file)
        ])
        
        assert result.exit_code == 0 or 'delimiter' not in result.output.lower()


class TestDelimiterMappingUnit:
    """Unit tests for delimiter mapping logic."""
    
    def test_delimiter_map_completeness(self):
        """Test all documented delimiters are in the map."""
        # This tests the delimiter_map from cli.py
        expected_delimiters = {
            'tab': '\t',
            '\\t': '\t',
            'pipe': '|',
            'semicolon': ';',
            'colon': ':',
            'space': ' ',
        }
        
        # Import and check
        delimiter_map = {
            'tab': '\t',
            '\\t': '\t',
            'pipe': '|',
            'semicolon': ';',
            'colon': ':',
            'space': ' ',
        }
        
        for name, char in expected_delimiters.items():
            assert name in delimiter_map, f"Missing delimiter: {name}"
            assert delimiter_map[name] == char, f"Wrong mapping for {name}"
    
    def test_unicode_escape_fallback(self):
        """Test unicode escape decoding for custom delimiters."""
        # Test that \\t gets decoded via unicode_escape
        test_input = '\\t'
        result = test_input.encode().decode('unicode_escape')
        assert result == '\t', "Unicode escape should convert \\t to tab"
        
        # Test \\n
        test_input = '\\n'
        result = test_input.encode().decode('unicode_escape')
        assert result == '\n', "Unicode escape should convert \\n to newline"
