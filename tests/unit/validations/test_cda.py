"""
Unit tests for CDA (Critical Data Attribute) gap analysis module.
"""

import pytest
from datetime import datetime
from validation_framework.cda import (
    CDADefinition, CDAFieldCoverage,
    CDAGapResult, CDAGapAnalyzer, CDAReporter
)
from validation_framework.cda.models import CDAAnalysisReport


class TestCDADefinition:
    """Tests for CDADefinition model."""

    def test_from_dict_basic(self):
        """Test creating CDA from dict with basic fields."""
        data = {
            'field': 'customer_id',
            'description': 'Customer identifier'
        }
        cda = CDADefinition.from_dict(data)

        assert cda.field == 'customer_id'
        assert cda.description == 'Customer identifier'

    def test_from_dict_full(self):
        """Test creating CDA from dict with all fields."""
        data = {
            'field': 'tax_id',
            'description': 'Tax identification number',
            'owner': 'Finance Team',
            'data_steward': 'John Smith',
            'regulatory_reference': 'IRS Form 1099'
        }
        cda = CDADefinition.from_dict(data)

        assert cda.field == 'tax_id'
        assert cda.owner == 'Finance Team'
        assert cda.data_steward == 'John Smith'
        assert cda.regulatory_reference == 'IRS Form 1099'


class TestCDAFieldCoverage:
    """Tests for CDAFieldCoverage model."""

    def test_covered_field(self):
        """Test coverage tracking for covered field."""
        cda = CDADefinition(
            field='email',
            description='Email address'
        )
        coverage = CDAFieldCoverage(
            cda=cda,
            is_covered=True,
            covering_validations=['MandatoryFieldCheck', 'RegexCheck']
        )

        assert coverage.is_covered
        assert coverage.coverage_count == 2
        assert coverage.status_icon == "✓"
        assert coverage.status_class == "covered"

    def test_uncovered_field(self):
        """Test coverage tracking for uncovered field."""
        cda = CDADefinition(
            field='tax_id',
            description='Tax ID'
        )
        coverage = CDAFieldCoverage(
            cda=cda,
            is_covered=False,
            covering_validations=[]
        )

        assert not coverage.is_covered
        assert coverage.coverage_count == 0
        assert coverage.status_icon == "✗"
        assert coverage.status_class == "gap"


class TestCDAGapResult:
    """Tests for CDAGapResult model."""

    def test_coverage_percentage(self):
        """Test coverage percentage calculation."""
        result = CDAGapResult(
            file_name='customers',
            total_cdas=10,
            covered_cdas=7,
            gap_cdas=3
        )

        assert result.coverage_percentage == 70.0

    def test_coverage_percentage_empty(self):
        """Test coverage percentage with no CDAs."""
        result = CDAGapResult(
            file_name='customers',
            total_cdas=0,
            covered_cdas=0,
            gap_cdas=0
        )

        assert result.coverage_percentage == 100.0

    def test_has_gaps(self):
        """Test has_gaps property."""
        with_gaps = CDAGapResult(file_name='test', total_cdas=5, covered_cdas=3, gap_cdas=2)
        without_gaps = CDAGapResult(file_name='test', total_cdas=5, covered_cdas=5, gap_cdas=0)

        assert with_gaps.has_gaps
        assert not without_gaps.has_gaps


class TestCDAGapAnalyzer:
    """Tests for CDAGapAnalyzer."""

    def test_analyze_inline_cdas(self):
        """Test analyzing config with inline CDAs (new syntax)."""
        config = {
            'validation_job': {
                'name': 'Test Job',
                'files': [
                    {
                        'name': 'customers',
                        'path': 'customers.csv',
                        'critical_data_attributes': [
                            {'field': 'customer_id', 'description': 'ID'},
                            {'field': 'email', 'description': 'Email'}
                        ],
                        'validations': [
                            {
                                'type': 'MandatoryFieldCheck',
                                'params': {'fields': ['customer_id']}
                            }
                        ]
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        assert report.job_name == 'Test Job'
        assert len(report.results) == 1

        result = report.results[0]
        assert result.file_name == 'customers'
        assert result.total_cdas == 2
        assert result.covered_cdas == 1  # customer_id covered
        assert result.gap_cdas == 1  # email not covered

    def test_analyze_legacy_top_level_cdas(self):
        """Test analyzing config with top-level CDAs (legacy syntax)."""
        config = {
            'validation_job': {
                'name': 'Test Job',
                'critical_data_attributes': {
                    'customers': [
                        {'field': 'customer_id', 'description': 'ID'},
                        {'field': 'email', 'description': 'Email'}
                    ]
                },
                'files': [
                    {
                        'name': 'customers',
                        'path': 'customers.csv',
                        'validations': [
                            {
                                'type': 'MandatoryFieldCheck',
                                'params': {'fields': ['customer_id']}
                            }
                        ]
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        assert report.job_name == 'Test Job'
        assert len(report.results) == 1

        result = report.results[0]
        assert result.file_name == 'customers'
        assert result.total_cdas == 2
        assert result.covered_cdas == 1  # customer_id covered
        assert result.gap_cdas == 1  # email not covered

    def test_analyze_inline_overrides_top_level(self):
        """Test that inline CDAs take precedence over top-level."""
        config = {
            'validation_job': {
                'name': 'Priority Test',
                'critical_data_attributes': {
                    'customers': [
                        {'field': 'should_be_ignored', 'description': 'Top level'}
                    ]
                },
                'files': [
                    {
                        'name': 'customers',
                        'critical_data_attributes': [
                            {'field': 'inline_field', 'description': 'Inline'}
                        ],
                        'validations': [
                            {'type': 'MandatoryFieldCheck', 'params': {'fields': ['inline_field']}}
                        ]
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        result = report.results[0]
        assert result.total_cdas == 1
        assert result.field_coverage[0].cda.field == 'inline_field'

    def test_analyze_no_cdas(self):
        """Test analyzing config without CDAs."""
        config = {
            'validation_job': {
                'name': 'Test Job',
                'files': [
                    {'name': 'data', 'path': 'data.csv', 'validations': []}
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        assert report.job_name == 'Test Job'
        assert len(report.results) == 0  # No CDA results when no CDAs defined

    def test_analyze_all_covered(self):
        """Test config where all CDAs are covered."""
        config = {
            'validation_job': {
                'name': 'Full Coverage',
                'files': [
                    {
                        'name': 'data',
                        'critical_data_attributes': [
                            {'field': 'id', 'description': 'ID'},
                            {'field': 'value', 'description': 'Value'}
                        ],
                        'validations': [
                            {'type': 'MandatoryFieldCheck', 'params': {'fields': ['id', 'value']}}
                        ]
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        assert report.total_cdas == 2
        assert report.total_covered == 2
        assert report.total_gaps == 0
        assert not report.has_gaps

    def test_analyze_multiple_validations_same_field(self):
        """Test field covered by multiple validations."""
        config = {
            'validation_job': {
                'name': 'Multi-validation',
                'files': [
                    {
                        'name': 'users',
                        'critical_data_attributes': [
                            {'field': 'email', 'description': 'Email'}
                        ],
                        'validations': [
                            {'type': 'MandatoryFieldCheck', 'params': {'fields': ['email']}},
                            {'type': 'RegexCheck', 'params': {'field': 'email', 'pattern': '.*@.*'}}
                        ]
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        result = report.results[0]
        email_coverage = result.field_coverage[0]

        assert email_coverage.is_covered
        assert len(email_coverage.covering_validations) == 2
        assert 'MandatoryFieldCheck' in email_coverage.covering_validations
        assert 'RegexCheck' in email_coverage.covering_validations

    def test_recommendations(self):
        """Test recommendation generation for gaps."""
        config = {
            'validation_job': {
                'name': 'Test',
                'files': [
                    {
                        'name': 'data',
                        'critical_data_attributes': [
                            {'field': 'reg_field', 'description': 'Regulatory'},
                            {'field': 'fin_field', 'description': 'Financial'}
                        ],
                        'validations': []
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)
        result = report.results[0]

        recommendations = analyzer.get_recommendations(result)

        assert len(recommendations) == 2
        assert 'MandatoryFieldCheck' in recommendations[0]['suggested_validations']


class TestCDAReporter:
    """Tests for CDAReporter HTML generation."""

    def test_generate_html_basic(self):
        """Test basic HTML report generation."""
        report = CDAAnalysisReport(
            job_name='Test Report',
            results=[
                CDAGapResult(
                    file_name='customers',
                    total_cdas=5,
                    covered_cdas=3,
                    gap_cdas=2
                )
            ]
        )

        reporter = CDAReporter()
        html = reporter.generate_html(report)

        assert '<!DOCTYPE html>' in html
        assert 'Test Report' in html
        assert 'customers' in html
        assert 'CDA Gap Analysis Report' in html

    def test_generate_html_with_gaps(self):
        """Test HTML report includes gap alerts."""
        cda = CDADefinition(field='tax_id', description='Tax ID')
        coverage = CDAFieldCoverage(cda=cda, is_covered=False)

        report = CDAAnalysisReport(
            job_name='Gap Report',
            results=[
                CDAGapResult(
                    file_name='accounts',
                    total_cdas=1,
                    covered_cdas=0,
                    gap_cdas=1,
                    field_coverage=[coverage]
                )
            ]
        )

        reporter = CDAReporter()
        html = reporter.generate_html(report)

        assert 'tax_id' in html


class TestCDAIntegration:
    """Integration tests for CDA analysis."""

    def test_full_analysis_workflow(self):
        """Test complete analysis workflow with inline CDAs."""
        # Simulate a realistic config
        config = {
            'validation_job': {
                'name': 'Financial Data Validation',
                'files': [
                    {
                        'name': 'customers',
                        'path': 'customers.csv',
                        'format': 'csv',
                        'critical_data_attributes': [
                            {'field': 'customer_id', 'description': 'Primary ID'},
                            {'field': 'email', 'description': 'Contact email'},
                            {'field': 'tax_id', 'description': 'Tax number'},
                            {'field': 'balance', 'description': 'Account balance'},
                            {'field': 'phone', 'description': 'Phone number'}
                        ],
                        'validations': [
                            {'type': 'MandatoryFieldCheck', 'params': {'fields': ['customer_id', 'email']}},
                            {'type': 'RegexCheck', 'params': {'field': 'email', 'pattern': '.*@.*'}},
                            {'type': 'RangeCheck', 'params': {'field': 'balance', 'min_value': 0}}
                        ]
                    }
                ]
            }
        }

        # Run analysis
        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        # Verify results
        assert report.job_name == 'Financial Data Validation'
        assert len(report.results) == 1

        result = report.results[0]
        assert result.total_cdas == 5
        assert result.covered_cdas == 3  # customer_id, email, balance
        assert result.gap_cdas == 2  # tax_id, phone

        # Generate HTML report
        reporter = CDAReporter()
        html = reporter.generate_html(report)

        assert 'Financial Data Validation' in html
        assert 'tax_id' in html
        assert 'phone' in html
        assert '60%' in html  # 3/5 = 60% coverage
