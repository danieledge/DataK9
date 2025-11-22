#!/bin/bash

################################################################################
# Comprehensive Test Coverage Runner for DataK9 Data Quality Framework
#
# This script provides comprehensive test execution with detailed coverage
# reporting. It runs all tests and generates both terminal and HTML coverage
# reports for analysis.
#
# Usage:
#   ./run_coverage_tests.sh           # Run all tests with full coverage
#   ./run_coverage_tests.sh --quick   # Run quick coverage check
#   ./run_coverage_tests.sh --html    # Generate and open HTML report
#
# Author: Daniel Edge
# Date: 2025-11-15
################################################################################

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Configuration
TEST_DIR="tests"
COVERAGE_DIR="htmlcov"
MIN_COVERAGE=48  # Minimum coverage percentage required (matches pytest.ini)
TIMEOUT=300  # Test timeout in seconds

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════════════════╗"
    echo "║                                                                       ║"
    echo "║     DATAK9 DATA QUALITY FRAMEWORK - COMPREHENSIVE TEST COVERAGE      ║"
    echo "║                                                                       ║"
    echo "╚═══════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_section() {
    echo -e "\n${WHITE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${WHITE}  $1${NC}"
    echo -e "${WHITE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

################################################################################
# Main Coverage Test Function
################################################################################

run_full_coverage() {
    print_section "Running Complete Test Suite with Coverage Analysis"
    
    print_info "Test directory: $TEST_DIR"
    print_info "Coverage target: >=${MIN_COVERAGE}%"
    print_info "Timeout: ${TIMEOUT}s per test file"
    echo ""
    
    # Run pytest with comprehensive coverage
    python3 -m pytest $TEST_DIR/ \
        --cov=validation_framework \
        --cov-report=html \
        --cov-report=term-missing \
        --cov-report=term \
        --cov-fail-under=$MIN_COVERAGE \
        -v \
        --tb=short \
        --timeout=$TIMEOUT \
        2>&1
    
    local exit_code=$?
    
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════"
    
    if [ $exit_code -eq 0 ]; then
        print_success "All tests passed and coverage target met (>=${MIN_COVERAGE}%)"
    else
        if [ -f ".coverage" ]; then
            print_warning "Tests completed but may have failures or insufficient coverage"
        else
            print_error "Test execution failed (exit code: $exit_code)"
        fi
    fi
    
    # Display coverage summary
    if [ -d "$COVERAGE_DIR" ]; then
        print_info "HTML coverage report generated: ${COVERAGE_DIR}/index.html"
        
        # Extract coverage percentage from HTML if available
        if [ -f "${COVERAGE_DIR}/index.html" ]; then
            local coverage_pct=$(grep -oP 'pc_cov">\K[^%]+' "${COVERAGE_DIR}/index.html" | head -1)
            if [ -n "$coverage_pct" ]; then
                echo -e "${WHITE}Current Coverage: ${CYAN}${coverage_pct}%${NC}"
            fi
        fi
    fi
    
    return $exit_code
}

################################################################################
# Quick Coverage Check
################################################################################

run_quick_coverage() {
    print_section "Quick Coverage Check"
    
    print_info "Running essential tests only..."
    echo ""
    
    # Run subset of fast tests
    python3 -m pytest $TEST_DIR/ \
        --cov=validation_framework \
        --cov-report=term \
        -v \
        -m "unit and not slow" \
        --timeout=60 \
        2>&1
    
    local exit_code=$?
    
    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "Quick coverage check completed"
    else
        print_warning "Some tests may have failed"
    fi
    
    return $exit_code
}

################################################################################
# Generate and Open HTML Report
################################################################################

open_html_report() {
    print_section "Generating HTML Coverage Report"
    
    if [ ! -d "$COVERAGE_DIR" ] || [ ! -f "${COVERAGE_DIR}/index.html" ]; then
        print_info "Generating fresh coverage report..."
        
        python3 -m pytest $TEST_DIR/ \
            --cov=validation_framework \
            --cov-report=html \
            --quiet \
            --timeout=$TIMEOUT
        
        if [ ! -f "${COVERAGE_DIR}/index.html" ]; then
            print_error "Failed to generate HTML report"
            return 1
        fi
    fi
    
    print_success "HTML report available at: ${COVERAGE_DIR}/index.html"
    
    # Try to open in browser
    if command -v xdg-open &> /dev/null; then
        xdg-open "${COVERAGE_DIR}/index.html" 2>/dev/null &
        print_info "Opening report in default browser..."
    elif command -v open &> /dev/null; then
        open "${COVERAGE_DIR}/index.html" 2>/dev/null &
        print_info "Opening report in default browser..."
    else
        print_warning "Could not auto-open browser. Please open manually:"
        echo "  file://$(pwd)/${COVERAGE_DIR}/index.html"
    fi
}

################################################################################
# Coverage Statistics
################################################################################

show_coverage_stats() {
    print_section "Coverage Statistics by Module"
    
    if [ ! -f ".coverage" ]; then
        print_warning "No coverage data found. Run tests first."
        return 1
    fi
    
    # Generate detailed coverage report
    python3 -m coverage report --skip-covered --sort=cover 2>/dev/null || \
    python3 -m coverage report --sort=cover
    
    echo ""
    print_info "For detailed line-by-line coverage, see: ${COVERAGE_DIR}/index.html"
}

################################################################################
# Test Environment Check
################################################################################

check_test_environment() {
    print_section "Checking Test Environment"
    
    local all_ok=true
    
    # Check Python version
    if command -v python3 &> /dev/null; then
        local python_version=$(python3 --version 2>&1 | awk '{print $2}')
        print_success "Python 3 installed: $python_version"
    else
        print_error "Python 3 not found"
        all_ok=false
    fi
    
    # Check pytest
    if python3 -m pytest --version &> /dev/null; then
        local pytest_version=$(python3 -m pytest --version 2>&1 | head -1)
        print_success "pytest installed: $pytest_version"
    else
        print_error "pytest not installed (run: pip install pytest)"
        all_ok=false
    fi
    
    # Check pytest-cov
    if python3 -c "import pytest_cov" &> /dev/null; then
        print_success "pytest-cov installed"
    else
        print_error "pytest-cov not installed (run: pip install pytest-cov)"
        all_ok=false
    fi
    
    # Check test directory
    if [ -d "$TEST_DIR" ]; then
        local test_count=$(find $TEST_DIR -name "test_*.py" | wc -l)
        print_success "Test directory found with $test_count test files"
    else
        print_error "Test directory not found: $TEST_DIR"
        all_ok=false
    fi
    
    echo ""
    
    if [ "$all_ok" = true ]; then
        print_success "Test environment ready"
        return 0
    else
        print_error "Test environment has issues - please resolve above errors"
        return 1
    fi
}

################################################################################
# Main Entry Point
################################################################################

main() {
    clear
    print_header
    
    # Parse command line arguments
    case "${1:-}" in
        --quick|-q)
            run_quick_coverage
            exit $?
            ;;
        --html|-h)
            open_html_report
            exit $?
            ;;
        --stats|-s)
            show_coverage_stats
            exit $?
            ;;
        --check|-c)
            check_test_environment
            exit $?
            ;;
        --help)
            echo "DataK9 Test Coverage Runner"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  (no args)        Run full test suite with coverage analysis"
            echo "  --quick, -q      Run quick coverage check (unit tests only)"
            echo "  --html, -h       Generate and open HTML coverage report"
            echo "  --stats, -s      Show detailed coverage statistics"
            echo "  --check, -c      Check test environment setup"
            echo "  --help           Show this help message"
            echo ""
            exit 0
            ;;
        "")
            check_test_environment
            if [ $? -ne 0 ]; then
                exit 1
            fi
            run_full_coverage
            exit $?
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Run with --help for usage information"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
