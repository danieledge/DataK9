#!/bin/bash

################################################################################
#
#    ____        _        _  ______     _____         _     ____
#   |  _ \  __ _| |_ __ _| |/ / __ )   |_   _|__  ___| |_  |  _ \ _   _ _ __  _ __   ___ _ __
#   | | | |/ _` | __/ _` | ' /|  _ \ ____| |/ _ \/ __| __| | |_) | | | | '_ \| '_ \ / _ \ '__|
#   | |_| | (_| | || (_| | . \| |_) |____| |  __/\__ \ |_  |  _ <| |_| | | | | | | |  __/ |
#   |____/ \__,_|\__\__,_|_|\_\____/     |_|\___||___/\__| |_| \_\\__,_|_| |_|_| |_|\___|_|
#
#   DataK9 Unified Test Runner - Your K9 guardian for test quality
#
################################################################################
#
# USAGE:
#   ./run_tests.sh                 Interactive menu
#   ./run_tests.sh --quick         Fast tests only (skip slow)
#   ./run_tests.sh --datasets      Test against Titanic & Transactions datasets
#   ./run_tests.sh --profiler      Profiler tests only
#   ./run_tests.sh --coverage      Full coverage report
#   ./run_tests.sh --help          Show all options
#
################################################################################

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
DIM='\033[2m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_DIR="$PROJECT_ROOT/tests"
REPORTS_DIR="$PROJECT_ROOT/test-reports"
COVERAGE_DIR="$REPORTS_DIR/coverage"

# Load coverage threshold from pytest.ini
MIN_COVERAGE=$(grep -oP 'fail_under\s*=\s*\K\d+' "$PROJECT_ROOT/pytest.ini" 2>/dev/null || echo "48")

################################################################################
# Output Functions
################################################################################

print_logo() {
    echo -e "${CYAN}"
    if [ -f "$PROJECT_ROOT/resources/images/ascii-art.txt" ]; then
        cat "$PROJECT_ROOT/resources/images/ascii-art.txt"
    else
        echo "   ____        _        _  __ ___  "
        echo "  |  _ \\  __ _| |_ __ _| |/ // _ \\ "
        echo "  | | | |/ _\` | __/ _\` | ' /| (_) |"
        echo "  | |_| | (_| | || (_| | . \\ \\__, |"
        echo "  |____/ \\__,_|\\__\\__,_|_|\\_\\  /_/ "
    fi
    echo -e "${NC}"
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}                    T E S T   R U N N E R${NC}"
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_section() {
    echo ""
    echo -e "${WHITE}───────────────────────────────────────────────────────────────${NC}"
    echo -e "  ${CYAN}$1${NC}"
    echo -e "${WHITE}───────────────────────────────────────────────────────────────${NC}"
    echo ""
}

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
fail() { echo -e "  ${RED}✗${NC} $1"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $1"; }
info() { echo -e "  ${BLUE}ℹ${NC} $1"; }
dim()  { echo -e "  ${DIM}$1${NC}"; }

################################################################################
# Test Functions
################################################################################

run_tests() {
    local test_args="$1"
    local description="$2"
    local start_time=$(date +%s)

    print_section "$description"
    mkdir -p "$REPORTS_DIR"
    cd "$PROJECT_ROOT"

    # Use eval to properly handle quoted arguments like -m 'not slow'
    eval "python3 -m pytest $test_args"

    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo ""
    if [ $exit_code -eq 0 ]; then
        ok "Completed in ${duration}s"
    else
        fail "Failed (exit code $exit_code) after ${duration}s"
    fi

    return $exit_code
}

################################################################################
# Test Suites
################################################################################

run_quick() {
    run_tests "$TEST_DIR -v -m 'not slow' --tb=short" \
        "Quick Tests - Fast feedback for development (excludes slow tests)"
}

run_profiler() {
    run_tests "$TEST_DIR/unit/profiler/ -v --tb=short" \
        "Profiler Tests - Type inference, statistics, quality metrics, ML analysis"
}

run_datasets() {
    run_tests "$TEST_DIR/datasets/ -v --tb=short" \
        "Dataset Tests - Titanic & Transactions (CSV + Parquet) format verification"
}

run_validations() {
    run_tests "$TEST_DIR/unit/validations/ -v --tb=short" \
        "Validation Rule Tests - All 36 validation types (mandatory, range, regex, etc.)"
}

run_integration() {
    run_tests "$TEST_DIR/integration/ -v --tb=short" \
        "Integration Tests - End-to-end profiler workflows and feature interactions"
}

run_cli() {
    run_tests "$TEST_DIR/cli/ -v --tb=short" \
        "CLI Tests - Command-line interface, argument parsing, output formats"
}

run_core() {
    run_tests "$TEST_DIR/unit/core/ -v --tb=short" \
        "Core Tests - Engine, config, registry, results, and core framework components"
}

run_loaders() {
    run_tests "$TEST_DIR/unit/loaders/ -v --tb=short" \
        "Loader Tests - CSV, Excel, JSON, Parquet, and database loaders"
}

run_all() {
    run_tests "$TEST_DIR -v --tb=short" \
        "All Tests - Complete test suite including slow tests"
}

run_coverage() {
    print_section "Coverage Analysis - All tests with coverage threshold enforcement"
    mkdir -p "$COVERAGE_DIR"
    cd "$PROJECT_ROOT"

    info "Coverage threshold: ${MIN_COVERAGE}%"
    echo ""

    python3 -m pytest "$TEST_DIR/" \
        --cov=validation_framework \
        --cov-report=html:"$COVERAGE_DIR" \
        --cov-report=term-missing \
        --cov-fail-under="$MIN_COVERAGE" \
        -v --tb=short

    local exit_code=$?

    echo ""
    if [ $exit_code -eq 0 ]; then
        ok "Coverage threshold met (≥${MIN_COVERAGE}%)"
        info "HTML report: $COVERAGE_DIR/index.html"
    else
        fail "Coverage below ${MIN_COVERAGE}% or tests failed"
    fi

    return $exit_code
}

run_file() {
    local file="$1"
    if [ ! -f "$file" ]; then
        fail "File not found: $file"
        return 1
    fi
    run_tests "$file -v --tb=short" "Running: $file"
}

################################################################################
# Utilities
################################################################################

check_environment() {
    print_section "Environment Check"

    local all_ok=true

    if command -v python3 &> /dev/null; then
        ok "Python $(python3 --version 2>&1 | awk '{print $2}')"
    else
        fail "Python 3 not found"
        all_ok=false
    fi

    if python3 -m pytest --version &> /dev/null; then
        ok "pytest $(python3 -m pytest --version 2>&1 | head -1 | awk '{print $2}')"
    else
        fail "pytest not installed"
        all_ok=false
    fi

    if [ -d "$TEST_DIR" ]; then
        local count=$(find "$TEST_DIR" -name "test_*.py" | wc -l)
        ok "$count test files found"
    else
        fail "Test directory not found"
        all_ok=false
    fi

    # Check test data
    if [ -d "$TEST_DIR/data" ]; then
        local csv_count=$(ls "$TEST_DIR/data/"*.csv 2>/dev/null | wc -l)
        local pq_count=$(ls "$TEST_DIR/data/"*.parquet 2>/dev/null | wc -l)
        ok "Test data: $csv_count CSV, $pq_count Parquet files"
    else
        warn "Test data directory not found (tests/data/)"
    fi

    echo ""
    if [ "$all_ok" = true ]; then
        ok "Environment ready"
    else
        fail "Please resolve issues above"
    fi
}

clean_artifacts() {
    print_section "Cleaning Artifacts"

    [ -d "$REPORTS_DIR" ] && rm -rf "$REPORTS_DIR" && ok "Removed test reports"
    [ -d "$PROJECT_ROOT/.pytest_cache" ] && rm -rf "$PROJECT_ROOT/.pytest_cache" && ok "Removed pytest cache"
    find "$PROJECT_ROOT" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null && ok "Removed __pycache__"

    local tmp_count=$(ls "$PROJECT_ROOT"/tmp* 2>/dev/null | wc -l)
    if [ $tmp_count -gt 0 ]; then
        rm -f "$PROJECT_ROOT"/tmp*
        ok "Removed $tmp_count temporary files"
    fi

    echo ""
    ok "Cleanup complete"
}

show_stats() {
    print_section "Test Statistics"
    cd "$PROJECT_ROOT"

    info "Collecting statistics..."
    echo ""

    local total=$(python3 -m pytest --collect-only -q "$TEST_DIR/" 2>/dev/null | tail -1 | grep -oP '\d+(?= test)')
    echo -e "  ${WHITE}Total tests:${NC}     ${CYAN}${total:-N/A}${NC}"

    local files=$(find "$TEST_DIR" -name "test_*.py" | wc -l)
    echo -e "  ${WHITE}Test files:${NC}      ${CYAN}$files${NC}"
    echo -e "  ${WHITE}Coverage min:${NC}    ${CYAN}${MIN_COVERAGE}%${NC}"
    echo ""

    info "Test categories:"
    find "$TEST_DIR" -name "test_*.py" -exec basename {} \; | sort | sed 's/^/    /'
}

################################################################################
# Interactive Menu
################################################################################

show_menu() {
    clear
    print_logo

    echo -e "${YELLOW}QUICK OPTIONS${NC} ${DIM}(for development - fast feedback)${NC}"
    echo "  1) Quick Tests          All tests except slow (~5 min)"
    echo "                          ${DIM}Synthetic fixtures + in-memory tests${NC}"
    echo ""
    echo "  2) Profiler Tests       Type inference, statistics, ML analysis (~3 min)"
    echo "                          ${DIM}Tests profiler engine, calculators, analyzers${NC}"
    echo ""
    echo "  3) Dataset Tests        Titanic & Transactions datasets (~3 min)"
    echo "                          ${DIM}Tests CSV/Parquet format consistency, real data${NC}"
    echo ""
    echo "  4) Validation Tests     All 36 validation rule types (~2 min)"
    echo "                          ${DIM}Mandatory, range, regex, cross-file, etc.${NC}"
    echo ""

    echo -e "${YELLOW}COMPREHENSIVE${NC} ${DIM}(for pre-commit/CI - thorough)${NC}"
    echo "  5) All Tests            Complete test suite (~15 min)"
    echo "                          ${DIM}Includes slow tests and large dataset tests${NC}"
    echo ""
    echo "  6) Coverage Report      All tests + ${MIN_COVERAGE}% threshold (~15 min)"
    echo "                          ${DIM}Generates HTML report in test-reports/coverage/${NC}"
    echo ""

    echo -e "${YELLOW}SPECIALIZED${NC} ${DIM}(targeted testing)${NC}"
    echo "  7) Integration Tests    End-to-end workflow tests"
    echo "  8) CLI Tests            Command-line interface tests"
    echo "  9) Core Tests           Engine, config, registry, results"
    echo "  l) Loader Tests         CSV, Excel, JSON, Parquet, database loaders"
    echo ""

    echo -e "${YELLOW}UTILITIES${NC}"
    echo "  f) Run Specific File    Enter path to a test file"
    echo "  e) Environment Check    Verify Python, pytest, test data"
    echo "  s) View Statistics      Test counts and categories"
    echo "  c) Clean Artifacts      Remove cache, reports, temp files"
    echo ""

    echo "  0) Exit"
    echo ""
    echo -e "${WHITE}─────────────────────────────────────────────────────────────${NC}"
    echo -n "  Select: "
}

interactive_menu() {
    while true; do
        show_menu
        read choice

        case $choice in
            1) run_quick ;;
            2) run_profiler ;;
            3) run_datasets ;;
            4) run_validations ;;
            5) run_all ;;
            6) run_coverage ;;
            7) run_integration ;;
            8) run_cli ;;
            9) run_core ;;
            l|L) run_loaders ;;
            f|F)
                echo ""
                echo -n "  Enter test file path: "
                read file
                run_file "$file"
                ;;
            e|E) check_environment ;;
            s|S) show_stats ;;
            c|C) clean_artifacts ;;
            0)
                echo ""
                info "Goodbye!"
                exit 0
                ;;
            *)
                warn "Invalid choice"
                sleep 1
                ;;
        esac

        echo ""
        read -p "  Press Enter to continue..."
    done
}

################################################################################
# Help
################################################################################

show_help() {
    echo "DataK9 Test Runner"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "QUICK OPTIONS (fast feedback for development):"
    echo "  --quick, -q          All tests except slow ones (~5 min)"
    echo "                       Uses generated fixtures, in-memory tests"
    echo ""
    echo "  --profiler, -p       Profiler component tests (~3 min)"
    echo "                       Type inference, statistics, quality metrics, ML analysis"
    echo ""
    echo "  --datasets, -d       Dataset profiling tests (~3 min)"
    echo "                       Titanic & Transactions in CSV and Parquet formats"
    echo "                       Verifies format consistency and real data handling"
    echo ""
    echo "  --validations, -v    Validation rule tests (~2 min)"
    echo "                       Tests all 36 validation types (mandatory, range, etc.)"
    echo ""
    echo "COMPREHENSIVE OPTIONS (thorough testing for CI/pre-commit):"
    echo "  --all, -a            Complete test suite (~15 min)"
    echo "                       Includes slow tests and large dataset tests"
    echo ""
    echo "  --coverage, -c       Coverage report with threshold (~15 min)"
    echo "                       Enforces minimum ${MIN_COVERAGE}% coverage"
    echo "                       Generates HTML report in test-reports/coverage/"
    echo ""
    echo "SPECIALIZED OPTIONS:"
    echo "  --integration, -i    Integration tests only"
    echo "                       End-to-end profiler workflows"
    echo ""
    echo "  --cli                CLI tests only"
    echo "                       Command-line interface, arguments, outputs"
    echo ""
    echo "  --core               Core framework tests only"
    echo "                       Engine, config, registry, results"
    echo ""
    echo "  --loaders            Loader tests only"
    echo "                       CSV, Excel, JSON, Parquet, database loaders"
    echo ""
    echo "UTILITIES:"
    echo "  --file <path>        Run specific test file"
    echo "  --check              Check environment (Python, pytest, data)"
    echo "  --stats              Show test counts and categories"
    echo "  --clean              Remove cache, reports, temp files"
    echo "  --help, -h           Show this help"
    echo ""
    echo "TEST DATA (in tests/data/samples/):"
    echo "  csv/titanic.csv          Passenger survival data (891 rows, 12 columns)"
    echo "  parquet/titanic.parquet  Same data in Parquet format"
    echo "  csv/transactions.csv     E-commerce transactions (1000 rows, 4 columns)"
    echo "  parquet/transactions.parquet  Same data in Parquet format"
    echo ""
    echo "EXAMPLES:"
    echo "  $0 --quick           Quick development feedback"
    echo "  $0 --datasets        Test real dataset profiling"
    echo "  $0 --coverage        Pre-commit/CI validation"
    echo "  $0                   Interactive menu (recommended)"
    echo ""
}

################################################################################
# Main
################################################################################

main() {
    if ! python3 -m pytest --version &> /dev/null; then
        fail "pytest not installed. Run: pip install pytest pytest-cov"
        exit 1
    fi

    case "${1:-}" in
        --quick|-q)        run_quick ;;
        --profiler|-p)     run_profiler ;;
        --datasets|-d)     run_datasets ;;
        --validations|-v)  run_validations ;;
        --integration|-i)  run_integration ;;
        --cli)             run_cli ;;
        --core)            run_core ;;
        --loaders)         run_loaders ;;
        --all|-a)          run_all ;;
        --coverage|-c)     run_coverage ;;
        --file)
            shift
            run_file "$1"
            ;;
        --check)           check_environment ;;
        --stats)           show_stats ;;
        --clean)           clean_artifacts ;;
        --help|-h)         show_help ;;
        "")                interactive_menu ;;
        *)
            fail "Unknown option: $1"
            echo ""
            show_help
            exit 1
            ;;
    esac

    exit $?
}

main "$@"
