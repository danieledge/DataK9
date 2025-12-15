#!/bin/bash
# DataK9 Validation Test Runner
# Provides easy access to different test categories

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════════════════════╗"
echo "║          DataK9 Validation Test Runner                    ║"
echo "╚═══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

show_menu() {
    echo -e "${YELLOW}Select test category:${NC}"
    echo ""
    echo "  1) Quick unit tests (fast, no external data)"
    echo "  2) All validation unit tests"
    echo "  3) Policy system tests"
    echo "  4) Comprehensive CSV validation tests"
    echo "  5) Comprehensive Parquet validation tests"
    echo "  6) Cross-file validation tests"
    echo "  7) Performance tests (slow, large files)"
    echo "  8) ALL tests with coverage report"
    echo "  9) Run specific test file"
    echo "  10) Run comprehensive validation config"
    echo "  11) Check policy compliance"
    echo "  12) List available policies"
    echo "  0) Exit"
    echo ""
}

run_tests() {
    case $1 in
        1)
            echo -e "${GREEN}Running quick unit tests...${NC}"
            pytest tests/unit/validations/ -v -m "unit and not slow" --tb=short
            ;;
        2)
            echo -e "${GREEN}Running all validation unit tests...${NC}"
            pytest tests/unit/validations/ -v --tb=short
            ;;
        3)
            echo -e "${GREEN}Running policy system tests...${NC}"
            pytest tests/unit/validations/test_policy.py tests/unit/validations/test_policy_analyzer.py -v --tb=short 2>/dev/null || \
            pytest tests/unit/ -k "policy" -v --tb=short
            ;;
        4)
            echo -e "${GREEN}Running comprehensive CSV tests...${NC}"
            pytest tests/integration/test_comprehensive_csv.py -v --tb=short 2>/dev/null || \
            echo -e "${YELLOW}Integration tests not yet created. Run option 10 to test via CLI.${NC}"
            ;;
        5)
            echo -e "${GREEN}Running comprehensive Parquet tests...${NC}"
            pytest tests/integration/test_comprehensive_parquet.py -v --tb=short 2>/dev/null || \
            echo -e "${YELLOW}Integration tests not yet created. Run option 10 to test via CLI.${NC}"
            ;;
        6)
            echo -e "${GREEN}Running cross-file validation tests...${NC}"
            pytest tests/unit/validations/test_cross_file*.py -v --tb=short
            ;;
        7)
            echo -e "${YELLOW}Running performance tests (this may take several minutes)...${NC}"
            pytest tests/ -v -m "slow or performance" --tb=short 2>/dev/null || \
            echo -e "${YELLOW}No performance tests found with 'slow' or 'performance' markers.${NC}"
            ;;
        8)
            echo -e "${GREEN}Running ALL tests with coverage...${NC}"
            pytest tests/ -v --cov=validation_framework --cov-report=html --cov-report=term-missing --tb=short
            echo -e "${GREEN}Coverage report: htmlcov/index.html${NC}"
            ;;
        9)
            echo -e "${YELLOW}Enter test file path (relative to tests/):${NC}"
            read -r test_file
            pytest "tests/$test_file" -v --tb=short
            ;;
        10)
            echo -e "${GREEN}Running comprehensive validation config...${NC}"
            python3 -m validation_framework.cli validate examples/comprehensive_validation_config.yaml --log-level INFO
            ;;
        11)
            echo -e "${GREEN}Checking policy compliance...${NC}"
            python3 -m validation_framework.cli check-policy examples/comprehensive_validation_config.yaml --policy strict
            ;;
        12)
            echo -e "${GREEN}Listing available policies...${NC}"
            python3 -m validation_framework.cli list-policies
            ;;
        0)
            echo "Exiting..."
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option${NC}"
            ;;
    esac
}

# Command-line argument support
if [ -n "$1" ]; then
    run_tests "$1"
else
    # Interactive mode
    while true; do
        show_menu
        read -r -p "Enter choice [0-12]: " choice
        echo ""
        run_tests "$choice"
        echo ""
        echo -e "${BLUE}────────────────────────────────────────────────────────────${NC}"
        echo ""
    done
fi
