#!/bin/bash

###############################################################################
# DataK9 Ultimate Performance Demo
# Author: Daniel Edge
# Description: Comprehensive validation stress test on large IBM AML datasets
#              Uses existing comprehensive_large_test_config.yaml
###############################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_TMP="$SCRIPT_DIR/demo-tmp"
TEST_DATA_DIR="/home/daniel/www/test-data"
CONFIG_PATH="$SCRIPT_DIR/test-data/configs/ultimate_validation_showcase.yaml"
RESULTS_FILE="$DEMO_TMP/ultimate_demo_results.txt"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

###############################################################################
# Display Functions
###############################################################################

show_logo() {
    clear
    echo -e "${CYAN}${BOLD}"
    cat << 'EOF'
╔═══════════════════════════════════════════════════════════════════╗
║                  DataK9 Ultimate Stress Test                       ║
║           31 Validation Types on 357M Rows                         ║
║         (IBM AML Banking Transactions - 2 Files, 10.1 GB)          ║
╚═══════════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    echo
}

show_header() {
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
}

log_metric() {
    local metric_name="$1"
    local metric_value="$2"
    echo "$metric_name: $metric_value" >> "$RESULTS_FILE"
    echo -e "${GREEN}  ✓${NC} $metric_name: ${BOLD}$metric_value${NC}"
}

log_info() {
    echo -e "${CYAN}  ℹ${NC} $1"
}

log_error() {
    echo -e "${RED}  ✗${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}  ⚠${NC} $1"
}

###############################################################################
# Setup
###############################################################################

setup_demo() {
    show_header "Setup & System Information"

    # Create demo directory
    mkdir -p "$DEMO_TMP"

    # Initialize results file
    cat > "$RESULTS_FILE" << EOF
═══════════════════════════════════════════════════════════════
DataK9 Ultimate Stress Test Results
Date: $(date)
═══════════════════════════════════════════════════════════════

EOF

    # System info
    echo "SYSTEM INFORMATION" >> "$RESULTS_FILE"
    log_metric "CPU" "$(grep 'model name' /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)"
    log_metric "RAM" "$(free -h | awk '/^Mem:/ {print $2}')"
    log_metric "Python Version" "$(python3 --version | cut -d' ' -f2)"
    log_metric "DataK9 Version" "1.0"

    echo "" >> "$RESULTS_FILE"
    echo
}

###############################################################################
# Check Available Files
###############################################################################

show_download_instructions() {
    echo
    echo -e "${RED}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}${BOLD}  ⚠ REQUIRED TEST DATA NOT FOUND${NC}"
    echo -e "${RED}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
    echo -e "${YELLOW}This ultimate stress test requires the IBM AML Banking dataset.${NC}"
    echo -e "${YELLOW}The files are too large for GitHub (5+ GB each, 52 GB total).${NC}"
    echo
    echo -e "${CYAN}${BOLD}REQUIRED FILES:${NC}"
    echo -e "  • HI-Large_Trans.parquet   (5.1 GB, 179M rows)"
    echo -e "  • LI-Large_Trans.parquet   (5.0 GB, 177M rows)"
    echo
    echo -e "${CYAN}${BOLD}DOWNLOAD OPTIONS:${NC}"
    echo
    echo -e "${GREEN}Option 1: Kaggle CLI (Recommended)${NC}"
    echo -e "  1. Install Kaggle: ${BOLD}pip3 install kaggle${NC}"
    echo -e "  2. Setup API key: https://www.kaggle.com/settings (Create New API Token)"
    echo -e "  3. Save to: ${BOLD}~/.kaggle/kaggle.json${NC}"
    echo -e "  4. Run:"
    echo
    echo -e "     ${BOLD}mkdir -p $TEST_DATA_DIR${NC}"
    echo -e "     ${BOLD}cd $TEST_DATA_DIR${NC}"
    echo -e "     ${BOLD}kaggle datasets download -d ealtman2019/ibm-transactions-for-anti-money-laundering-aml${NC}"
    echo -e "     ${BOLD}unzip ibm-transactions-for-anti-money-laundering-aml.zip${NC}"
    echo -e "     ${BOLD}rm ibm-transactions-for-anti-money-laundering-aml.zip${NC}"
    echo
    echo -e "${GREEN}Option 2: Manual Download${NC}"
    echo -e "  1. Visit: ${BOLD}https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml${NC}"
    echo -e "  2. Click 'Download' (requires Kaggle account)"
    echo -e "  3. Extract to: ${BOLD}$TEST_DATA_DIR${NC}"
    echo
    echo -e "${GREEN}Option 3: Use Smaller Datasets${NC}"
    echo -e "  • Run ${BOLD}./demo.sh${NC} and choose option 5, 6, or 7 for smaller datasets"
    echo -e "  • Small files (5-7M rows) are included in the repository"
    echo
    echo -e "${CYAN}${BOLD}MORE INFO:${NC}"
    echo -e "  • Setup guide: ${BOLD}TEST_DATA_SETUP.md${NC}"
    echo -e "  • Dataset info: ${BOLD}https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml${NC}"
    echo
    echo -e "${MAGENTA}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
}

check_files() {
    show_header "Checking Available Test Files"

    echo "AVAILABLE FILES" >> "$RESULTS_FILE"

    local files_found=0
    local required_files=("HI-Large_Trans.parquet" "LI-Large_Trans.parquet")
    local missing_files=()

    # Check for required large Parquet files
    for file in "${required_files[@]}"; do
        if [[ -f "$TEST_DATA_DIR/$file" ]]; then
            local size=$(du -h "$TEST_DATA_DIR/$file" | cut -f1)
            log_metric "$file" "$size"
            ((files_found++))
        else
            missing_files+=("$file")
            log_error "$file - NOT FOUND"
        fi
    done

    # Check for optional files (nice to have but not required)
    if [[ -f "$TEST_DATA_DIR/HI-Medium_Trans.parquet" ]]; then
        local size=$(du -h "$TEST_DATA_DIR/HI-Medium_Trans.parquet" | cut -f1)
        log_metric "HI-Medium (Parquet)" "$size (optional)"
        ((files_found++))
    fi

    if [[ -f "$TEST_DATA_DIR/HI-Small_Trans.parquet" ]]; then
        local size=$(du -h "$TEST_DATA_DIR/HI-Small_Trans.parquet" | cut -f1)
        log_metric "HI-Small (Parquet)" "$size (optional)"
        ((files_found++))
    fi

    # Check comprehensive config
    if [[ -f "$CONFIG_PATH" ]]; then
        log_metric "Validation Config" "Found (ultimate_validation_showcase.yaml)"
    else
        log_error "Validation config not found at: $CONFIG_PATH"
        echo
        log_error "Config files are included in the repository."
        log_error "Please ensure you're running from the correct directory."
        exit 1
    fi

    # If required files are missing, show download instructions
    if [[ ${#missing_files[@]} -gt 0 ]]; then
        echo "" >> "$RESULTS_FILE"
        echo
        show_download_instructions
        exit 1
    fi

    echo "" >> "$RESULTS_FILE"
    echo
}

###############################################################################
# Run Comprehensive Validation
###############################################################################

run_comprehensive_validation() {
    show_header "Running Comprehensive Validation"

    echo "COMPREHENSIVE VALIDATION TEST" >> "$RESULTS_FILE"

    log_info "Configuration: ultimate_validation_showcase.yaml"
    log_info "Target: HI-Large + LI-Large (357M rows across 2 files)"
    log_info "Validation Types: 31 available validation types"
    echo

    local output_html="$DEMO_TMP/ultimate_validation_report_${TIMESTAMP}.html"
    local output_json="$DEMO_TMP/ultimate_validation_results_${TIMESTAMP}.json"
    local output_log="$DEMO_TMP/ultimate_validation_output_${TIMESTAMP}.log"

    log_info "Starting validation (this may take 10-30 minutes)..."
    log_info "Progress logged to: $output_log"
    echo

    # Run validation with detailed timing
    local start_time=$(date +%s)

    if /usr/bin/time -v python3 -m validation_framework.cli validate \
        "$CONFIG_PATH" \
        -o "$output_html" \
        -j "$output_json" 2>&1 | tee "$output_log"; then

        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        local minutes=$((duration / 60))
        local seconds=$((duration % 60))

        echo
        echo "═══════════════════════════════════════════════════════════════" >> "$RESULTS_FILE"
        log_metric "Status" "SUCCESS"
        log_metric "Duration" "${minutes}m ${seconds}s (${duration} seconds total)"
        log_metric "HTML Report" "$output_html"
        log_metric "JSON Results" "$output_json"
        log_metric "Full Log" "$output_log"

        # Extract key metrics from JSON if available
        if [[ -f "$output_json" ]]; then
            echo "" >> "$RESULTS_FILE"
            echo "VALIDATION METRICS" >> "$RESULTS_FILE"

            # Use Python to extract metrics safely
            python3 << PYTHON_EOF
import json
try:
    with open('$output_json') as f:
        data = json.load(f)

    # Overall metrics
    total_val = data.get('total_validations', 0)
    passed = sum(1 for f in data.get('files', []) for v in f.get('validation_results', []) if v.get('passed'))
    failed = total_val - passed

    # File metrics
    for file_data in data.get('files', []):
        file_name = file_data.get('file_name', 'Unknown')
        row_count = file_data.get('metadata', {}).get('total_rows', 0)
        file_val = len(file_data.get('validation_results', []))
        exec_time = file_data.get('execution_time', 0)

        print(f"File: {file_name}")
        print(f"  Rows Processed: {row_count:,}")
        print(f"  Validations: {file_val}")
        print(f"  Execution Time: {exec_time:.2f}s")
        if exec_time > 0 and row_count > 0:
            throughput = row_count / exec_time
            print(f"  Throughput: {throughput:,.0f} rows/sec")
        print()

    print(f"Total Validations: {total_val}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

except Exception as e:
    print(f"Could not parse JSON: {e}")
PYTHON_EOF
        fi

        # Extract memory usage from time output
        if grep -q "Maximum resident set size" "$output_log"; then
            local max_mem=$(grep "Maximum resident set size" "$output_log" | awk '{print $NF}')
            local max_mem_mb=$((max_mem / 1024))
            log_metric "Peak Memory" "${max_mem_mb} MB"
        fi

        echo "" >> "$RESULTS_FILE"

    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))

        echo
        log_error "Validation failed or was interrupted after ${duration} seconds"
        log_metric "Status" "FAILED"
        log_metric "Duration" "${duration} seconds"
        log_metric "Log File" "$output_log"
        echo "" >> "$RESULTS_FILE"
        return 1
    fi
}

###############################################################################
# Summary
###############################################################################

show_summary() {
    show_header "Test Summary"

    echo
    echo -e "${GREEN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}${BOLD}  ✓ Ultimate Stress Test Complete!${NC}"
    echo -e "${GREEN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
    echo -e "${CYAN}  📊 Full Results:${NC}"
    echo -e "    ${BOLD}cat $RESULTS_FILE${NC}"
    echo
    echo -e "${CYAN}  📁 Output Files:${NC}"
    echo -e "    ${BOLD}ls -lh $DEMO_TMP/ultimate_*${NC}"
    echo
    echo -e "${YELLOW}  Key Performance Metrics:${NC}"
    echo

    # Show key metrics
    if [[ -f "$RESULTS_FILE" ]]; then
        grep -E "Duration|Peak Memory|Throughput|Rows Processed|Status" "$RESULTS_FILE" | sed 's/^/    /' || true
    fi

    echo
    echo -e "${MAGENTA}${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${MAGENTA}${BOLD}  🐕 DataK9 - Production-Ready Performance${NC}"
    echo -e "${MAGENTA}${BOLD}═══════════════════════════════════════════════════════════════${NC}"
    echo
}

###############################################################################
# Main Execution
###############################################################################

main() {
    show_logo
    setup_demo
    check_files
    run_comprehensive_validation
    show_summary
}

# Trap errors
trap 'echo -e "${RED}${BOLD}Error occurred. Check logs in $DEMO_TMP${NC}"' ERR

# Run the demo
main
