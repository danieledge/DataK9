"""
Unit tests for CDA (Critical Data Attribute) gap analysis module.
"""

import pytest
from datetime import datetime
from validation_framework.cda import (
    CDADefinition, CDATier, CDAFieldCoverage,
    CDAGapResult, CDAGapAnalyzer, CDAReporter
)
from validation_framework.cda.models import CDAAnalysisReport


class TestCDATier:
    """Tests for CDATier enum."""

    def test_tier_values(self):
        """Test tier enum values."""
        assert CDATier.TIER_1.value == "TIER_1"
        assert CDATier.TIER_2.value == "TIER_2"
        assert CDATier.TIER_3.value == "TIER_3"

    def test_tier_display_names(self):
        """Test tier display names."""
        assert CDATier.TIER_1.display_name == "Regulatory"
        assert CDATier.TIER_2.display_name == "Financial"
        assert CDATier.TIER_3.display_name == "Operational"

    def test_tier_priorities(self):
        """Test tier priorities (1 = highest)."""
        assert CDATier.TIER_1.priority == 1
        assert CDATier.TIER_2.priority == 2
        assert CDATier.TIER_3.priority == 3


class TestCDADefinition:
    """Tests for CDADefinition model."""

    def test_from_dict_basic(self):
        """Test creating CDA from dict with basic fields."""
        data = {
            'field': 'customer_id',
            'tier': 'TIER_1',
            'description': 'Customer identifier'
        }
        cda = CDADefinition.from_dict(data)

        assert cda.field == 'customer_id'
        assert cda.tier == CDATier.TIER_1
        assert cda.description == 'Customer identifier'

    def test_from_dict_full(self):
        """Test creating CDA from dict with all fields."""
        data = {
            'field': 'tax_id',
            'tier': 'TIER_1',
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

    def test_from_dict_invalid_tier(self):
        """Test that invalid tier defaults to TIER_3."""
        data = {
            'field': 'some_field',
            'tier': 'INVALID_TIER'
        }
        cda = CDADefinition.from_dict(data)

        assert cda.tier == CDATier.TIER_3


class TestCDAFieldCoverage:
    """Tests for CDAFieldCoverage model."""

    def test_covered_field(self):
        """Test coverage tracking for covered field."""
        cda = CDADefinition(
            field='email',
            tier=CDATier.TIER_1,
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
            tier=CDATier.TIER_1,
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

    def test_tier_coverage(self):
        """Test tier coverage statistics."""
        result = CDAGapResult(
            file_name='customers',
            tier_coverage={
                CDATier.TIER_1: {'total': 3, 'covered': 2, 'gaps': 1},
                CDATier.TIER_2: {'total': 2, 'covered': 2, 'gaps': 0}
            }
        )

        tier1_stats = result.get_tier_coverage(CDATier.TIER_1)
        assert tier1_stats['total'] == 3
        assert tier1_stats['gaps'] == 1

        assert result.get_tier_percentage(CDATier.TIER_1) == pytest.approx(66.67, rel=0.01)
        assert result.get_tier_percentage(CDATier.TIER_2) == 100.0


class TestCDAGapAnalyzer:
    """Tests for CDAGapAnalyzer."""

    def test_analyze_basic_config(self):
        """Test analyzing a basic config with CDAs."""
        config = {
            'validation_job': {
                'name': 'Test Job',
                'critical_data_attributes': {
                    'customers': [
                        {'field': 'customer_id', 'tier': 'TIER_1', 'description': 'ID'},
                        {'field': 'email', 'tier': 'TIER_1', 'description': 'Email'}
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
                'critical_data_attributes': {
                    'data': [
                        {'field': 'id', 'tier': 'TIER_1', 'description': 'ID'},
                        {'field': 'value', 'tier': 'TIER_2', 'description': 'Value'}
                    ]
                },
                'files': [
                    {
                        'name': 'data',
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
                'critical_data_attributes': {
                    'users': [
                        {'field': 'email', 'tier': 'TIER_1', 'description': 'Email'}
                    ]
                },
                'files': [
                    {
                        'name': 'users',
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

    def test_tier1_at_risk(self):
        """Test detection of TIER_1 gaps."""
        config = {
            'validation_job': {
                'name': 'Tier 1 Risk',
                'critical_data_attributes': {
                    'accounts': [
                        {'field': 'tax_id', 'tier': 'TIER_1', 'description': 'Tax ID'},
                        {'field': 'balance', 'tier': 'TIER_2', 'description': 'Balance'}
                    ]
                },
                'files': [
                    {
                        'name': 'accounts',
                        'validations': [
                            {'type': 'RangeCheck', 'params': {'field': 'balance', 'min_value': 0}}
                        ]
                    }
                ]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)

        # tax_id (TIER_1) is not covered
        assert report.tier1_at_risk

    def test_recommendations(self):
        """Test recommendation generation for gaps."""
        config = {
            'validation_job': {
                'name': 'Test',
                'critical_data_attributes': {
                    'data': [
                        {'field': 'reg_field', 'tier': 'TIER_1', 'description': 'Regulatory'},
                        {'field': 'fin_field', 'tier': 'TIER_2', 'description': 'Financial'}
                    ]
                },
                'files': [{'name': 'data', 'validations': []}]
            }
        }

        analyzer = CDAGapAnalyzer()
        report = analyzer.analyze(config)
        result = report.results[0]

        recommendations = analyzer.get_recommendations(result)

        assert len(recommendations) == 2
        # Should be sorted by priority (TIER_1 first)
        assert recommendations[0]['tier'] == 'TIER_1'
        assert recommendations[1]['tier'] == 'TIER_2'
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
        cda = CDADefinition(field='tax_id', tier=CDATier.TIER_1, description='Tax ID')
        coverage = CDAFieldCoverage(cda=cda, is_covered=False)

        report = CDAAnalysisReport(
            job_name='Gap Report',
            results=[
                CDAGapResult(
                    file_name='accounts',
                    total_cdas=1,
                    covered_cdas=0,
                    gap_cdas=1,
                    field_coverage=[coverage],
                    tier_coverage={CDATier.TIER_1: {'total': 1, 'covered': 0, 'gaps': 1}}
                )
            ]
        )

        reporter = CDAReporter()
        html = reporter.generate_html(report)

        assert 'AUDIT RISK' in html
        assert 'tax_id' in html
        assert 'No validation coverage' in html


class TestCDAIntegration:
    """Integration tests for CDA analysis."""

    def test_full_analysis_workflow(self):
        """Test complete analysis workflow."""
        # Simulate a realistic config
        config = {
            'validation_job': {
                'name': 'Financial Data Validation',
                'critical_data_attributes': {
                    'customers': [
                        {'field': 'customer_id', 'tier': 'TIER_1', 'description': 'Primary ID'},
                        {'field': 'email', 'tier': 'TIER_1', 'description': 'Contact email'},
                        {'field': 'tax_id', 'tier': 'TIER_1', 'description': 'Tax number'},
                        {'field': 'balance', 'tier': 'TIER_2', 'description': 'Account balance'},
                        {'field': 'phone', 'tier': 'TIER_3', 'description': 'Phone number'}
                    ]
                },
                'files': [
                    {
                        'name': 'customers',
                        'path': 'customers.csv',
                        'format': 'csv',
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

        # tax_id is TIER_1 gap
        assert report.tier1_at_risk

        # Generate HTML report
        reporter = CDAReporter()
        html = reporter.generate_html(report)

        assert 'Financial Data Validation' in html
        assert 'tax_id' in html
        assert 'phone' in html
        assert '60%' in html  # 3/5 = 60% coverage
