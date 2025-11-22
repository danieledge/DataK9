#!/bin/bash

################################################################################
# DataK9 Unified Test Runner
# Author: Daniel Edge
# Description: Comprehensive test execution with intuitive menu system
#
# Features:
#   - All test types (unit, integration, security, validation, etc.)
#   - Coverage analysis with detailed reporting
#   - Test environment validation
#   - Artifact management
#   - Parallel execution support
#   - Interactive and CLI modes
#
# Usage:
#   ./test_runner.sh                    # Interactive menu
#   ./test_runner.sh --all              # Run all tests
#   ./test_runner.sh --coverage         # Run with coverage
#   ./test_runner.sh --help             # Show help
################################################################################

set -e  # Exit on error (disabled during test execution)

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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_DIR="$PROJECT_ROOT/tests"
REPORTS_DIR="$PROJECT_ROOT/test-reports"
COVERAGE_DIR="$REPORTS_DIR/coverage"
ARTIFACTS_DIR="$PROJECT_ROOT/demo-tmp"

# Load coverage threshold from pytest.ini if available
if [ -f "$PROJECT_ROOT/pytest.ini" ]; then
    MIN_COVERAGE=$(grep -oP 'fail_under\s*=\s*\K\d+' "$PROJECT_ROOT/pytest.ini" 2>/dev/null || echo "48")
else
    MIN_COVERAGE=48
fi

TIMEOUT=300  # Test timeout in seconds

################################################################################
# Helper Functions
################################################################################

print_header() {
    # Display DataK9 ASCII logo if available
    if [ -f "$PROJECT_ROOT/resources/images/ascii-art.txt" ]; then
        cat "$PROJECT_ROOT/resources/images/ascii-art.txt"
        echo ""
        echo -e "${CYAN}                      UNIFIED TEST RUNNER${NC}"
        echo ""
    else
        echo -e "${CYAN}"
        echo "╔══════════════════════════════════════════════════════════════════╗"
        echo "║                                                                  ║"
        echo "║              DATAK9 - UNIFIED TEST RUNNER                       ║"
        echo "║                                                                  ║"
        echo "╚══════════════════════════════════════════════════════════════════╝"
        echo -e "${NC}"
    fi
}

print_section() {
    echo -e "\n${WHITE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${WHITE}  $1${NC}"
    echo -e "${WHITE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
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
# Environment Setup
################################################################################

setup_output_directories() {
    # Create reports directory structure if it doesn't exist
    mkdir -p "$REPORTS_DIR"
    mkdir -p "$COVERAGE_DIR"

    # Set COVERAGE_DIR for pytest
    export COVERAGE_FILE="$REPORTS_DIR/.coverage"
}

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
        print_warning "pytest-cov not installed (run: pip install pytest-cov)"
        print_info "Coverage reports will not be available"
    fi

    # Check test directory
    if [ -d "$TEST_DIR" ]; then
        local test_count=$(find "$TEST_DIR" -name "test_*.py" 2>/dev/null | wc -l)
        print_success "Test directory found with $test_count test files"
    else
        print_error "Test directory not found: $TEST_DIR"
        all_ok=false
    fi

    # Check pytest.ini
    if [ -f "$PROJECT_ROOT/pytest.ini" ]; then
        print_success "pytest.ini found (coverage threshold: ${MIN_COVERAGE}%)"
    else
        print_warning "pytest.ini not found in project root"
    fi

    echo ""

    if [ "$all_ok" = true ]; then
        print_success "Test environment ready"
        print_info "Reports will be saved to: $REPORTS_DIR"
        return 0
    else
        print_error "Test environment has issues - please resolve above errors"
        return 1
    fi
}

################################################################################
# Test Execution Functions
################################################################################

run_tests() {
    local test_args="$1"
    local description="$2"
    local start_time=$(date +%s)

    print_section "$description"

    # Ensure output directories exist
    setup_output_directories

    # Change to project root for test execution
    cd "$PROJECT_ROOT"

    # Run pytest with arguments
    set +e  # Don't exit on test failure
    python3 -m pytest $test_args
    local exit_code=$?
    set -e

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "Tests completed successfully in ${duration}s"
    else
        print_error "Tests failed with exit code $exit_code (${duration}s)"
    fi

    echo ""
    read -p "Press Enter to continue..."
    return $exit_code
}

# Category: All Tests
run_all_tests() {
    run_tests "$TEST_DIR/ -v" "Running All Tests"
}

# Category: Test Types
run_unit_tests() {
    run_tests "$TEST_DIR/ -v -m unit" "Running Unit Tests"
}

run_integration_tests() {
    run_tests "$TEST_DIR/ -v -m integration" "Running Integration Tests"
}

run_security_tests() {
    run_tests "$TEST_DIR/ -v -m security" "Running Security Tests"
}

# Category: Validation Tests
run_validation_tests() {
    local validation_test_files=(
        "test_file_validations.py"
        "test_schema_validations.py"
        "test_field_validations.py"
        "test_record_validations.py"
        "test_advanced_validations.py"
        "test_conditional_validations.py"
        "test_cross_file_validations.py"
        "test_inline_validations.py"
        "test_database_validations.py"
        "test_missing_statistical_temporal.py"
    )

    local test_paths=""
    for file in "${validation_test_files[@]}"; do
        if [ -f "$TEST_DIR/$file" ]; then
            test_paths="$test_paths $TEST_DIR/$file"
        fi
    done

    run_tests "$test_paths -v" "Running All Validation Rule Tests"
}

run_comprehensive_regression() {
    if [ -f "$TEST_DIR/test_comprehensive_regression.py" ]; then
        run_tests "$TEST_DIR/test_comprehensive_regression.py -v" \
            "Running Comprehensive Regression Tests (All Validation Types)"
    else
        print_error "Regression test file not found"
        read -p "Press Enter to continue..."
    fi
}

# Category: Component Tests
run_database_tests() {
    local database_test_files=(
        "test_database_integration.py"
        "test_database_validations.py"
        "test_database_profiling_json.py"
    )

    local test_paths=""
    for file in "${database_test_files[@]}"; do
        if [ -f "$TEST_DIR/$file" ]; then
            test_paths="$test_paths $TEST_DIR/$file"
        fi
    done

    run_tests "$test_paths -v" "Running Database Tests"
}

run_profiler_tests() {
    local profiler_test_files=(
        "test_profiler.py"
        "test_polars_profiler.py"
        "test_database_profiling_json.py"
    )

    local test_paths=""
    for file in "${profiler_test_files[@]}"; do
        if [ -f "$TEST_DIR/$file" ]; then
            test_paths="$test_paths $TEST_DIR/$file"
        fi
    done

    run_tests "$test_paths -v" "Running Profiler Tests"
}

run_cli_tests() {
    run_tests "$TEST_DIR/ -v -m cli" "Running CLI Tests"
}

run_performance_tests() {
    run_tests "$TEST_DIR/ -v -m performance" "Running Performance Tests"
}

# Category: Special Test Modes
run_fast_tests() {
    run_tests "$TEST_DIR/ -v -m 'not slow'" "Running Fast Tests (Excluding Slow)"
}

run_parallel_tests() {
    print_section "Running Tests in Parallel"
    setup_output_directories
    cd "$PROJECT_ROOT"

    set +e
    python3 -m pytest "$TEST_DIR/" -v -n auto
    local exit_code=$?
    set -e

    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "Parallel tests completed successfully"
    else
        print_error "Parallel tests failed"
    fi

    echo ""
    read -p "Press Enter to continue..."
    return $exit_code
}

run_specific_file() {
    local file_path="$1"

    if [ ! -f "$file_path" ]; then
        print_error "Test file not found: $file_path"
        read -p "Press Enter to continue..."
        return 1
    fi

    run_tests "$file_path -v" "Running Specific Test File: $file_path"
}

################################################################################
# Coverage Functions
################################################################################

run_with_coverage() {
    print_section "Running Tests with Coverage Analysis"

    setup_output_directories
    cd "$PROJECT_ROOT"

    print_info "Test directory: $TEST_DIR"
    print_info "Coverage threshold: >=${MIN_COVERAGE}%"
    print_info "Coverage report: $COVERAGE_DIR"
    echo ""

    set +e
    python3 -m pytest "$TEST_DIR/" \
        --cov=validation_framework \
        --cov-report=html:"$COVERAGE_DIR" \
        --cov-report=term-missing \
        --cov-report=term:skip-covered \
        --cov-fail-under="$MIN_COVERAGE" \
        -v

    local exit_code=$?
    set -e

    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "Coverage requirements met (>= ${MIN_COVERAGE}%)"

        if [ -d "$COVERAGE_DIR" ]; then
            print_info "HTML coverage report: ${COVERAGE_DIR}/index.html"

            # Extract coverage percentage
            if [ -f "${COVERAGE_DIR}/index.html" ]; then
                local coverage_pct=$(grep -oP 'pc_cov">\K[^%]+' "${COVERAGE_DIR}/index.html" | head -1 2>/dev/null || echo "N/A")
                if [ -n "$coverage_pct" ] && [ "$coverage_pct" != "N/A" ]; then
                    echo -e "${WHITE}Current Coverage: ${CYAN}${coverage_pct}%${NC}"
                fi
            fi

            # Offer to open report
            if command -v xdg-open &> /dev/null || command -v open &> /dev/null; then
                echo ""
                read -p "Open coverage report in browser? (y/n): " -n 1 -r
                echo
                if [[ $REPLY =~ ^[Yy]$ ]]; then
                    if command -v xdg-open &> /dev/null; then
                        xdg-open "${COVERAGE_DIR}/index.html" 2>/dev/null &
                    elif command -v open &> /dev/null; then
                        open "${COVERAGE_DIR}/index.html" 2>/dev/null &
                    fi
                    print_info "Opening report in browser..."
                fi
            fi
        fi
    else
        print_error "Coverage below minimum threshold or tests failed"
    fi

    echo ""
    read -p "Press Enter to continue..."
    return $exit_code
}

run_quick_coverage() {
    print_section "Quick Coverage Check (Unit Tests Only)"

    setup_output_directories
    cd "$PROJECT_ROOT"

    print_info "Running essential tests only..."
    echo ""

    set +e
    python3 -m pytest "$TEST_DIR/" \
        --cov=validation_framework \
        --cov-report=term \
        -v \
        -m "unit and not slow" \
        --timeout=60

    local exit_code=$?
    set -e

    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "Quick coverage check completed"
    else
        print_warning "Some tests may have failed"
    fi

    echo ""
    read -p "Press Enter to continue..."
    return $exit_code
}

show_coverage_stats() {
    print_section "Coverage Statistics by Module"

    cd "$PROJECT_ROOT"

    if [ ! -f "$COVERAGE_FILE" ]; then
        print_warning "No coverage data found. Run tests with coverage first."
        echo ""
        read -p "Press Enter to continue..."
        return 1
    fi

    # Generate detailed coverage report
    python3 -m coverage report --skip-covered --sort=cover 2>/dev/null || \
    python3 -m coverage report --sort=cover

    echo ""
    print_info "For detailed line-by-line coverage, see: ${COVERAGE_DIR}/index.html"
    echo ""
    read -p "Press Enter to continue..."
}

open_coverage_report() {
    print_section "Opening Coverage Report"

    if [ ! -d "$COVERAGE_DIR" ] || [ ! -f "${COVERAGE_DIR}/index.html" ]; then
        print_warning "Coverage report not found. Generating..."

        setup_output_directories
        cd "$PROJECT_ROOT"

        python3 -m pytest "$TEST_DIR/" \
            --cov=validation_framework \
            --cov-report=html:"$COVERAGE_DIR" \
            --quiet \
            --timeout="$TIMEOUT" 2>/dev/null

        if [ ! -f "${COVERAGE_DIR}/index.html" ]; then
            print_error "Failed to generate HTML report"
            echo ""
            read -p "Press Enter to continue..."
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
        echo "  file://${COVERAGE_DIR}/index.html"
    fi

    echo ""
    read -p "Press Enter to continue..."
}

################################################################################
# Utility Functions
################################################################################

view_test_statistics() {
    print_section "Test Suite Statistics"

    cd "$PROJECT_ROOT"
    print_info "Collecting test information..."

    # Count tests by type
    local total_tests=$(python3 -m pytest --collect-only -q "$TEST_DIR/" 2>/dev/null | tail -3 | head -1 | grep -oP '\d+(?= test)' || echo "N/A")
    local unit_tests=$(python3 -m pytest --collect-only -q -m unit "$TEST_DIR/" 2>/dev/null | tail -3 | head -1 | grep -oP '\d+(?= test)' || echo "N/A")
    local integration_tests=$(python3 -m pytest --collect-only -q -m integration "$TEST_DIR/" 2>/dev/null | tail -3 | head -1 | grep -oP '\d+(?= test)' || echo "N/A")

    echo ""
    echo -e "${WHITE}Test Counts:${NC}"
    echo "  Total Tests:       ${total_tests}"
    echo "  Unit Tests:        ${unit_tests}"
    echo "  Integration Tests: ${integration_tests}"

    # Count test files
    local test_files=$(find "$TEST_DIR" -name "test_*.py" 2>/dev/null | wc -l)
    echo ""
    echo -e "${WHITE}Test Files: $test_files${NC}"

    # List test files
    echo ""
    find "$TEST_DIR" -name "test_*.py" -exec basename {} \; 2>/dev/null | sort | sed 's/^/  - /'

    echo ""
    echo -e "${WHITE}Output Locations:${NC}"
    echo "  Reports:  $REPORTS_DIR"
    echo "  Coverage: $COVERAGE_DIR"

    echo ""
    read -p "Press Enter to continue..."
}

clean_test_artifacts() {
    print_section "Cleaning Test Artifacts"

    local removed_count=0

    cd "$PROJECT_ROOT"

    # Remove reports directory
    if [ -d "$REPORTS_DIR" ]; then
        rm -rf "$REPORTS_DIR"
        print_success "Removed test reports directory"
        ((removed_count++))
    fi

    # Remove pytest cache
    if [ -d ".pytest_cache" ]; then
        rm -rf ".pytest_cache"
        print_success "Removed pytest cache"
        ((removed_count++))
    fi

    # Remove Python cache
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
    if [ $? -eq 0 ]; then
        print_success "Removed Python cache files"
        ((removed_count++))
    fi

    # Remove temporary test files from project root
    local temp_files=$(ls tmp*_profile_report.html tmp*_validation.yaml tmp*.csv tmp*.json tmp*.xlsx tmp*.parquet tmp*.db 2>/dev/null | wc -l)
    if [ $temp_files -gt 0 ]; then
        rm -f tmp*_profile_report.html tmp*_validation.yaml tmp*.csv tmp*.json tmp*.xlsx tmp*.parquet tmp*.db 2>/dev/null
        print_success "Removed $temp_files temporary test file(s) from project root"
        ((removed_count++))
    fi

    # Remove demo artifacts
    if [ -d "$ARTIFACTS_DIR" ]; then
        rm -rf "$ARTIFACTS_DIR"
        print_success "Removed demo artifacts directory"
        ((removed_count++))
    fi

    # Remove temporary test files from tests directory
    find "$TEST_DIR" -name "*.pyc" -delete 2>/dev/null
    find "$TEST_DIR" -name ".DS_Store" -delete 2>/dev/null
    find "$TEST_DIR" -name "tmp*" -delete 2>/dev/null

    echo ""
    if [ $removed_count -gt 0 ]; then
        print_success "Cleaned $removed_count artifact type(s)"
    else
        print_info "No artifacts to clean"
    fi

    echo ""
    read -p "Press Enter to continue..."
}

################################################################################
# Interactive Menu
################################################################################

show_menu() {
    clear
    print_header

    echo -e "${CYAN}${WHITE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${WHITE}  SELECT TEST SUITE${NC}"
    echo -e "${CYAN}${WHITE}═══════════════════════════════════════════════════════════════════${NC}"
    echo ""

    echo -e "${YELLOW}▶ QUICK TESTS${NC}"
    echo "  1) All Tests                  - Complete test suite"
    echo "  2) Fast Tests                 - Quick validation (skip slow tests)"
    echo "  3) Quick Coverage Check       - Unit tests with coverage (fast)"
    echo ""

    echo -e "${YELLOW}▶ TEST CATEGORIES${NC}"
    echo "  4) Unit Tests                 - Individual components in isolation"
    echo "  5) Integration Tests          - Components working together"
    echo "  6) Security Tests             - Security and input validation"
    echo ""

    echo -e "${YELLOW}▶ VALIDATION TESTS${NC}"
    echo "  7) All Validation Tests       - All validation rule types"
    echo "  8) Regression Tests           - Comprehensive regression suite"
    echo ""

    echo -e "${YELLOW}▶ COMPONENT TESTS${NC}"
    echo "  9) Database Tests             - DB integration & validation"
    echo "  10) Profiler Tests            - File & database profiling"
    echo "  11) CLI Tests                 - Command-line interface"
    echo "  12) Performance Tests         - Performance benchmarks"
    echo ""

    echo -e "${YELLOW}▶ COVERAGE & REPORTS${NC}"
    echo "  13) Full Coverage Report      - All tests with detailed coverage"
    echo "  14) Coverage Statistics       - View current coverage stats"
    echo "  15) Open Coverage Report      - Open HTML report in browser"
    echo ""

    echo -e "${YELLOW}▶ ADVANCED${NC}"
    echo "  16) Parallel Execution        - Run tests using multiple cores"
    echo "  17) Specific Test File        - Run a single test file"
    echo ""

    echo -e "${YELLOW}▶ UTILITIES${NC}"
    echo "  18) Test Statistics           - View test counts and files"
    echo "  19) Check Environment         - Validate test setup"
    echo "  20) Clean Artifacts           - Remove reports and cache"
    echo ""

    echo "  0) Exit"
    echo ""
    echo -e "${WHITE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -n "Enter choice [0-20]: "
}

################################################################################
# Help Function
################################################################################

show_help() {
    echo "DataK9 Unified Test Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all                Run all tests"
    echo "  --fast               Run fast tests (skip slow)"
    echo "  --unit               Run unit tests only"
    echo "  --integration        Run integration tests only"
    echo "  --security           Run security tests only"
    echo "  --validations        Run all validation rule tests"
    echo "  --regression         Run comprehensive regression tests"
    echo "  --database           Run database tests"
    echo "  --profiler           Run profiler tests"
    echo "  --cli                Run CLI tests only"
    echo "  --performance        Run performance tests only"
    echo "  --coverage           Run with full coverage report"
    echo "  --quick-coverage     Run quick coverage check"
    echo "  --parallel           Run tests in parallel"
    echo "  --file <path>        Run specific test file"
    echo "  --stats              Show test statistics"
    echo "  --check              Check test environment"
    echo "  --clean              Clean test artifacts"
    echo "  --help               Show this help message"
    echo ""
    echo "Interactive Mode:"
    echo "  Run without arguments for an interactive menu"
    echo ""
    echo "Output Locations:"
    echo "  Reports:  $REPORTS_DIR/"
    echo "  Coverage: $COVERAGE_DIR/"
    echo ""
    exit 0
}

################################################################################
# Main Logic
################################################################################

main() {
    # Check if pytest is installed
    if ! python3 -m pytest --version &> /dev/null; then
        print_error "pytest is not installed. Install with: pip install pytest pytest-cov"
        exit 1
    fi

    # Parse command line arguments
    if [ $# -eq 0 ]; then
        # Interactive mode
        while true; do
            show_menu
            read choice

            case $choice in
                1) run_all_tests ;;
                2) run_fast_tests ;;
                3) run_quick_coverage ;;
                4) run_unit_tests ;;
                5) run_integration_tests ;;
                6) run_security_tests ;;
                7) run_validation_tests ;;
                8) run_comprehensive_regression ;;
                9) run_database_tests ;;
                10) run_profiler_tests ;;
                11) run_cli_tests ;;
                12) run_performance_tests ;;
                13) run_with_coverage ;;
                14) show_coverage_stats ;;
                15) open_coverage_report ;;
                16) run_parallel_tests ;;
                17)
                    echo ""
                    echo -n "Enter test file path: "
                    read file_path
                    run_specific_file "$file_path"
                    ;;
                18) view_test_statistics ;;
                19) check_test_environment ;;
                20) clean_test_artifacts ;;
                0)
                    echo ""
                    print_info "Exiting test runner. Goodbye!"
                    exit 0
                    ;;
                *)
                    print_error "Invalid choice. Please try again."
                    sleep 2
                    ;;
            esac
        done
    else
        # Command line mode
        case "$1" in
            --all)
                run_all_tests
                ;;
            --fast)
                run_fast_tests
                ;;
            --unit)
                run_unit_tests
                ;;
            --integration)
                run_integration_tests
                ;;
            --security)
                run_security_tests
                ;;
            --validations)
                run_validation_tests
                ;;
            --regression)
                run_comprehensive_regression
                ;;
            --database)
                run_database_tests
                ;;
            --profiler)
                run_profiler_tests
                ;;
            --cli)
                run_cli_tests
                ;;
            --performance)
                run_performance_tests
                ;;
            --coverage)
                run_with_coverage
                ;;
            --quick-coverage)
                run_quick_coverage
                ;;
            --parallel)
                run_parallel_tests
                ;;
            --file)
                if [ -z "$2" ]; then
                    print_error "Please specify a test file path"
                    exit 1
                fi
                run_specific_file "$2"
                ;;
            --stats)
                view_test_statistics
                ;;
            --check)
                check_test_environment
                ;;
            --clean)
                clean_test_artifacts
                ;;
            --help|-h)
                show_help
                ;;
            *)
                print_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac

        exit $?
    fi
}

# Run main function
main "$@"
