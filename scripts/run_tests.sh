#!/bin/bash

################################################################################
# Test Runner for Data Validation Framework
#
# Interactive test runner with colored output, coverage reporting, and
# multiple test selection options. Designed for both developer use and
# CI/CD integration.
#
# Usage:
#   ./run_tests.sh                 # Interactive menu
#   ./run_tests.sh --all           # Run all tests
#   ./run_tests.sh --unit          # Run unit tests only
#   ./run_tests.sh --integration   # Run integration tests only
#   ./run_tests.sh --security      # Run security tests only
#   ./run_tests.sh --cli           # Run CLI tests only
#   ./run_tests.sh --coverage      # Run with coverage report
#   ./run_tests.sh --fast          # Skip slow tests
#   ./run_tests.sh --file <path>   # Run specific test file
#   ./run_tests.sh --help          # Show help
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

# Test configuration
MIN_COVERAGE=43  # Minimum coverage percentage required
TEST_DIR="tests"
COVERAGE_DIR="htmlcov"
COVERAGE_REPORT="coverage_report.txt"

################################################################################
# Helper Functions
################################################################################

print_header() {
    # Display DataK9 ASCII logo if available
    if [ -f "resources/images/ascii-art.txt" ]; then
        cat resources/images/ascii-art.txt
        echo ""
        echo -e "${CYAN}                          TEST RUNNER${NC}"
        echo ""
    else
        # Fallback to simple header if logo file not found
        echo -e "${CYAN}"
        echo "╔══════════════════════════════════════════════════════════════════╗"
        echo "║                                                                  ║"
        echo "║          DATA VALIDATION FRAMEWORK - TEST RUNNER                ║"
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

show_help() {
    echo "Test Runner for DataK9 Validation Framework"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all                Run all tests"
    echo "  --unit               Run unit tests only"
    echo "  --integration        Run integration tests only"
    echo "  --security           Run security tests only"
    echo "  --validations        Run all validation rule tests (34+ types)"
    echo "  --regression         Run comprehensive regression tests (all 35 types + pos/neg cases)"
    echo "  --database           Run database tests (integration, validation, profiling)"
    echo "  --profiler           Run profiler tests (files and databases)"
    echo "  --cli                Run CLI tests only"
    echo "  --performance        Run performance tests only"
    echo "  --coverage           Run with coverage report"
    echo "  --fast               Skip slow tests"
    echo "  --file <path>        Run specific test file"
    echo "  --verbose            Verbose output"
    echo "  --quiet              Minimal output"
    echo "  --parallel           Run tests in parallel"
    echo "  --help               Show this help message"
    echo ""
    echo "Interactive Mode:"
    echo "  Run without arguments for an interactive menu"
    echo ""
    exit 0
}

################################################################################
# Test Execution Functions
################################################################################

run_tests() {
    local test_args="$1"
    local description="$2"
    local start_time=$(date +%s)

    print_section "$description"

    # Run pytest with arguments
    python3 -m pytest $test_args

    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo ""
    if [ $exit_code -eq 0 ]; then
        print_success "Tests completed successfully in ${duration}s"
    else
        print_error "Tests failed with exit code $exit_code (${duration}s)"
    fi

    return $exit_code
}

run_all_tests() {
    run_tests "$TEST_DIR/ -v" "Running All Tests"
}

run_unit_tests() {
    run_tests "$TEST_DIR/ -v -m unit" "Running Unit Tests"
}

run_integration_tests() {
    run_tests "$TEST_DIR/ -v -m integration" "Running Integration Tests"
}

run_security_tests() {
    run_tests "$TEST_DIR/ -v -m security" "Running Security Tests"
}

run_validation_tests() {
    # Run all validation rule tests (34+ validation types)
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
        test_paths="$test_paths $TEST_DIR/$file"
    done

    run_tests "$test_paths -v" "Running All Validation Rule Tests (34+ validation types)"
}

run_comprehensive_regression() {
    # Run comprehensive regression test suite (all 35 validation types)
    run_tests "$TEST_DIR/test_comprehensive_regression.py -v" \
        "Running Comprehensive Regression Tests (All 35 Validation Types + Positive/Negative Cases)"
}

run_database_tests() {
    # Run all database-related tests
    local database_test_files=(
        "test_database_integration.py"
        "test_database_validations.py"
        "test_database_profiling_json.py"
    )

    local test_paths=""
    for file in "${database_test_files[@]}"; do
        test_paths="$test_paths $TEST_DIR/$file"
    done

    run_tests "$test_paths -v" "Running Database Tests (Integration, Validation, Profiling)"
}

run_profiler_tests() {
    # Run all profiler-related tests
    local profiler_test_files=(
        "test_profiler.py"
        "test_polars_profiler.py"
        "test_database_profiling_json.py"
    )

    local test_paths=""
    for file in "${profiler_test_files[@]}"; do
        test_paths="$test_paths $TEST_DIR/$file"
    done

    run_tests "$test_paths -v" "Running Profiler Tests (Files + Databases)"
}

run_cli_tests() {
    run_tests "$TEST_DIR/ -v -m cli" "Running CLI Tests"
}

run_performance_tests() {
    run_tests "$TEST_DIR/ -v -m performance" "Running Performance Tests"
}

run_fast_tests() {
    run_tests "$TEST_DIR/ -v -m 'not slow'" "Running Fast Tests (Excluding Slow)"
}

run_with_coverage() {
    print_section "Running Tests with Coverage Analysis"

    python3 -m pytest $TEST_DIR/ \
        --cov=validation_framework \
        --cov-report=html \
        --cov-report=term-missing \
        --cov-report=term:skip-covered \
        --cov-fail-under=$MIN_COVERAGE \
        -v

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        print_success "Coverage requirements met (>= ${MIN_COVERAGE}%)"

        if [ -d "$COVERAGE_DIR" ]; then
            print_info "HTML coverage report: ${COVERAGE_DIR}/index.html"

            # Try to open coverage report in browser
            if command -v xdg-open &> /dev/null; then
                read -p "Open coverage report in browser? (y/n): " -n 1 -r
                echo
                if [[ $REPLY =~ ^[Yy]$ ]]; then
                    xdg-open "${COVERAGE_DIR}/index.html" 2>/dev/null &
                fi
            fi
        fi
    else
        print_error "Coverage below minimum threshold or tests failed"
    fi

    return $exit_code
}

run_specific_file() {
    local file_path="$1"

    if [ ! -f "$file_path" ]; then
        print_error "Test file not found: $file_path"
        return 1
    fi

    run_tests "$file_path -v" "Running Specific Test File: $file_path"
}

run_parallel_tests() {
    print_section "Running Tests in Parallel"

    python3 -m pytest $TEST_DIR/ -v -n auto

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        print_success "Parallel tests completed successfully"
    else
        print_error "Parallel tests failed"
    fi

    return $exit_code
}

################################################################################
# Interactive Menu
################################################################################

show_menu() {
    clear
    print_header

    echo "Select test suite to run:"
    echo ""

    echo "  1) Run All Tests"
    echo "     → Complete test suite (389+ tests, ~12 minutes)"
    echo ""
    echo "  2) Run Unit Tests Only"
    echo "     → Test individual components in isolation"
    echo ""
    echo "  3) Run Integration Tests Only"
    echo "     → Test components working together"
    echo ""
    echo "  4) Run Security Tests Only"
    echo "     → Test security features and input validation"
    echo ""
    echo "  5) Run All Validation Rule Tests"
    echo "     → Test all 34+ validation types (~1-2 minutes)"
    echo ""
    echo "  6) Run Comprehensive Regression Tests ⭐ NEW"
    echo "     → All 35 validation types + positive/negative cases (~30 seconds)"
    echo ""
    echo "  7) Run Database Tests"
    echo "     → Database integration, validation, and profiling tests"
    echo ""
    echo "  8) Run Profiler Tests"
    echo "     → File and database profiling tests"
    echo ""
    echo "  9) Run CLI Tests Only"
    echo "     → Test command-line interface"
    echo ""
    echo "  10) Run Performance Tests Only"
    echo "     → Test performance benchmarks"
    echo ""
    echo "  11) Run Fast Tests (Skip Slow)"
    echo "     → Quick validation (skips performance tests)"
    echo ""
    echo "  12) Run with Coverage Report"
    echo "     → All tests + detailed coverage analysis (requires ≥43%)"
    echo ""
    echo "  13) Run Specific Test File"
    echo "     → Run a single test file by path"
    echo ""
    echo "  14) Run Tests in Parallel"
    echo "     → Faster execution using multiple cores"
    echo ""
    echo "  15) View Test Statistics"
    echo "     → Show test counts and file listing"
    echo ""
    echo "  16) Clean Test Artifacts"
    echo "     → Remove coverage reports and cache files"
    echo ""
    echo "  0) Exit"
    echo ""
    echo -n "Enter choice [0-16]: "
}

view_test_statistics() {
    print_section "Test Suite Statistics"

    print_info "Collecting test information..."

    # Count tests by type
    local total_tests=$(python3 -m pytest --collect-only -q $TEST_DIR/ 2>/dev/null | grep "test session" -A 1 | tail -1 | awk '{print $1}')
    local unit_tests=$(python3 -m pytest --collect-only -q -m unit $TEST_DIR/ 2>/dev/null | grep "test session" -A 1 | tail -1 | awk '{print $1}')
    local integration_tests=$(python3 -m pytest --collect-only -q -m integration $TEST_DIR/ 2>/dev/null | grep "test session" -A 1 | tail -1 | awk '{print $1}')
    local security_tests=$(python3 -m pytest --collect-only -q -m security $TEST_DIR/ 2>/dev/null | grep "test session" -A 1 | tail -1 | awk '{print $1}')

    echo ""
    echo -e "${WHITE}Test Counts:${NC}"
    echo "  Total Tests:       ${total_tests:-N/A}"
    echo "  Unit Tests:        ${unit_tests:-N/A}"
    echo "  Integration Tests: ${integration_tests:-N/A}"
    echo "  Security Tests:    ${security_tests:-N/A}"

    # Count test files
    local test_files=$(find $TEST_DIR -name "test_*.py" | wc -l)
    echo ""
    echo -e "${WHITE}Test Files:${NC}"
    echo "  Total Test Files:  $test_files"

    # List test files
    echo ""
    echo -e "${WHITE}Test Files:${NC}"
    find $TEST_DIR -name "test_*.py" -exec basename {} \; | sort | sed 's/^/  - /'

    echo ""
    read -p "Press Enter to continue..."
}

clean_test_artifacts() {
    print_section "Cleaning Test Artifacts"

    local removed_count=0

    # Remove coverage files
    if [ -d "$COVERAGE_DIR" ]; then
        rm -rf "$COVERAGE_DIR"
        print_success "Removed coverage HTML directory"
        ((removed_count++))
    fi

    if [ -f ".coverage" ]; then
        rm -f ".coverage"
        print_success "Removed .coverage file"
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

    # Remove temporary test files from tests directory
    find $TEST_DIR -name "*.pyc" -delete 2>/dev/null
    find $TEST_DIR -name ".DS_Store" -delete 2>/dev/null
    find $TEST_DIR -name "tmp*" -delete 2>/dev/null

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
                2) run_unit_tests ;;
                3) run_integration_tests ;;
                4) run_security_tests ;;
                5) run_validation_tests ;;
                6) run_comprehensive_regression ;;
                7) run_database_tests ;;
                8) run_profiler_tests ;;
                9) run_cli_tests ;;
                10) run_performance_tests ;;
                11) run_fast_tests ;;
                12) run_with_coverage ;;
                13)
                    echo ""
                    echo -n "Enter test file path: "
                    read file_path
                    run_specific_file "$file_path"
                    ;;
                14) run_parallel_tests ;;
                15) view_test_statistics ;;
                16) clean_test_artifacts ;;
                0)
                    echo ""
                    print_info "Exiting test runner. Goodbye!"
                    exit 0
                    ;;
                *)
                    print_error "Invalid choice. Please try again."
                    ;;
            esac

            echo ""
            read -p "Press Enter to continue..."
        done
    else
        # Command line mode
        case "$1" in
            --all)
                run_all_tests
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
            --fast)
                run_fast_tests
                ;;
            --coverage)
                run_with_coverage
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
