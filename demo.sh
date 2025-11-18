#!/bin/bash

###############################################################################
# DataK9 Demo Script - Brilliant First-Time User Experience
# Author: Daniel Edge
# Description: Interactive demo showcasing DataK9 validation and profiling
#              with 5 dataset tiers (Tiny → Ultimate)
###############################################################################

set -e  # Exit on error

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_TMP="$SCRIPT_DIR/demo-tmp"
LOGO_FILE="$SCRIPT_DIR/resources/images/datak9.txt"

###############################################################################
# Display Functions
###############################################################################

show_logo() {
    clear
    if [[ -f "$LOGO_FILE" ]]; then
        echo -e "${CYAN}"
        cat "$LOGO_FILE"
        echo -e "${NC}"
    else
        echo -e "${CYAN}${BOLD}"
        echo "╔═══════════════════════════════════════════════════════════╗"
        echo "║                    DataK9 Framework                       ║"
        echo "║         Your K9 Guardian for Data Quality                 ║"
        echo "╚═══════════════════════════════════════════════════════════╝"
        echo -e "${NC}"
    fi
    echo
}

show_header() {
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${BLUE}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
}

show_info() {
    echo -e "${CYAN}ℹ${NC}  $1"
}

show_success() {
    echo -e "${GREEN}✓${NC}  $1"
}

show_warning() {
    echo -e "${YELLOW}⚠${NC}  $1"
}

show_error() {
    echo -e "${RED}✗${NC}  $1"
}

show_command() {
    echo
    echo -e "${MAGENTA}${BOLD}▶ Command:${NC}"
    echo -e "${YELLOW}$1${NC}"
    echo
}

###############################################################################
# Dataset Tier Definitions
###############################################################################

declare -A TIER_NAMES=(
    [1]="Tiny"
    [2]="Small"
    [3]="Medium"
    [4]="Large"
    [5]="Ultimate"
)

declare -A TIER_PATHS=(
    [1]="test-data/tiny"
    [2]="test-data/small"
    [3]="test-data/medium"
    [4]="test-data/large"
    [5]="test-data/ultimate"
)

declare -A TIER_DESCRIPTIONS=(
    [1]="Sample datasets for quick testing (~2,500 rows)"
    [2]="E-commerce transactions (100,000 rows, 17.5 MB)"
    [3]="IBM AML Small - Banking transactions (5M rows, ~500 MB)"
    [4]="IBM AML Medium - Banking transactions (31M rows, ~3 GB)"
    [5]="IBM AML Large - Banking transactions (179M rows, 16 GB CSV / 5.1 GB Parquet)"
)

declare -A TIER_FILES=(
    [1]="customers.csv|accounts.csv|transactions.csv"
    [2]="ecommerce_transactions.csv|ecommerce_transactions.parquet"
    [3]="HI-Small_Trans.csv|HI-Small_Trans.parquet|LI-Small_Trans.csv|LI-Small_Trans.parquet"
    [4]="HI-Medium_Trans.csv|HI-Medium_Trans.parquet|LI-Medium_Trans.csv|LI-Medium_Trans.parquet"
    [5]="HI-Large_Trans.csv|HI-Large_Trans.parquet|LI-Large_Trans.csv|LI-Large_Trans.parquet"
)

###############################################################################
# Main Menu
###############################################################################

show_main_menu() {
    show_logo
    show_header "Welcome to DataK9 Demo"

    echo -e "${BOLD}DataK9${NC} is a production-ready framework for data quality validation"
    echo "and profiling. This demo helps you explore its capabilities with real datasets."
    echo
    echo -e "${CYAN}Choose an operation:${NC}"
    echo
    echo "  1) 🔍 Run Validation    - Check data quality with custom rules"
    echo "  2) 📊 Run Profile       - Analyze data characteristics and patterns"
    echo "  3) ℹ️  About DataK9      - Learn more about the framework"
    echo "  0) Exit"
    echo
    echo -e -n "${BOLD}Enter your choice [0-3]:${NC} "
    read -r choice

    case $choice in
        1) select_dataset "validate" ;;
        2) select_dataset "profile" ;;
        3) show_about ;;
        0) exit 0 ;;
        *) show_error "Invalid choice. Please try again."; sleep 2; show_main_menu ;;
    esac
}

###############################################################################
# About DataK9
###############################################################################

show_about() {
    show_logo
    show_header "About DataK9"

    echo -e "${BOLD}DataK9 Data Quality Framework${NC}"
    echo "Version: 1.0"
    echo
    echo -e "${CYAN}Features:${NC}"
    echo "  • 34 built-in validation types across 10 categories"
    echo "  • Advanced data profiling with anomaly detection"
    echo "  • Memory-efficient chunked processing"
    echo "  • Support for CSV, Excel, JSON, Parquet, and databases"
    echo "  • Beautiful HTML reports with interactive visualizations"
    echo "  • DataK9 Studio - Visual configuration IDE"
    echo
    echo -e "${CYAN}Performance:${NC}"
    echo "  • Handles files from 1 MB to 200+ GB"
    echo "  • Processes 179M rows in ~4 minutes (with optimizations)"
    echo "  • Parquet format: 10x faster than CSV for large files"
    echo
    echo -e "${CYAN}Dataset Tiers Available:${NC}"
    for i in 1 2 3 4 5; do
        echo "  ${i}) ${TIER_NAMES[$i]}: ${TIER_DESCRIPTIONS[$i]}"
    done
    echo
    echo -e "${CYAN}Documentation:${NC}"
    echo "  • User Guide: docs/USER_GUIDE.md"
    echo "  • Architecture: ARCHITECTURE_REFERENCE.md"
    echo "  • Validation Catalog: docs/VALIDATION_CATALOG.md"
    echo
    echo -e -n "Press Enter to return to main menu..."
    read -r
    show_main_menu
}

###############################################################################
# Dataset Selection
###############################################################################

select_dataset() {
    local operation=$1

    show_logo
    show_header "Select Dataset Tier"

    if [[ "$operation" == "validate" ]]; then
        echo -e "${BOLD}Validation${NC} checks your data against quality rules"
        echo "Example: Empty file check, schema validation, outlier detection, etc."
    else
        echo -e "${BOLD}Profiling${NC} analyzes data characteristics and patterns"
        echo "Example: Data types, distributions, correlations, anomalies, etc."
    fi
    echo
    echo -e "${CYAN}Choose a dataset tier:${NC}"
    echo

    for i in 1 2 3 4 5; do
        echo "  ${i}) ${TIER_NAMES[$i]}"
        echo "     ${TIER_DESCRIPTIONS[$i]}"
        echo
    done

    echo "  0) Back to main menu"
    echo
    echo -e -n "${BOLD}Enter your choice [0-5]:${NC} "
    read -r tier_choice

    case $tier_choice in
        1|2|3|4|5) select_file "$operation" "$tier_choice" ;;
        0) show_main_menu ;;
        *) show_error "Invalid choice. Please try again."; sleep 2; select_dataset "$operation" ;;
    esac
}

###############################################################################
# File Selection
###############################################################################

select_file() {
    local operation=$1
    local tier=$2
    local tier_name="${TIER_NAMES[$tier]}"
    local tier_path="${TIER_PATHS[$tier]}"

    show_logo
    show_header "Select File - $tier_name Tier"

    echo -e "${CYAN}Available files in $tier_name tier:${NC}"
    echo

    # Get available files
    IFS='|' read -ra FILES <<< "${TIER_FILES[$tier]}"
    local file_count=0
    local -a available_files

    for file in "${FILES[@]}"; do
        local full_path="$SCRIPT_DIR/$tier_path/$file"
        if [[ -f "$full_path" ]] || [[ -L "$full_path" ]]; then
            file_count=$((file_count + 1))
            available_files+=("$file")

            # Get file size (use -L to follow symlinks)
            local size=""
            if [[ -f "$full_path" ]] || [[ -L "$full_path" ]]; then
                size=$(du -Lh "$full_path" 2>/dev/null | cut -f1)
            fi

            echo "  ${file_count}) ${file}"
            if [[ -n "$size" ]]; then
                echo "     Size: $size"
            fi
            echo
        fi
    done

    if [[ $file_count -eq 0 ]]; then
        show_error "No files found in $tier_name tier"
        echo
        show_warning "Make sure datasets are organized in: $tier_path/"
        echo
        echo -e -n "Press Enter to go back..."
        read -r
        select_dataset "$operation"
        return
    fi

    echo "  0) Back to tier selection"
    echo
    echo -e -n "${BOLD}Enter your choice [0-$file_count]:${NC} "
    read -r file_choice

    if [[ "$file_choice" == "0" ]]; then
        select_dataset "$operation"
    elif [[ "$file_choice" =~ ^[0-9]+$ ]] && [[ $file_choice -ge 1 ]] && [[ $file_choice -le $file_count ]]; then
        local selected_file="${available_files[$((file_choice - 1))]}"
        local full_path="$SCRIPT_DIR/$tier_path/$selected_file"

        if [[ "$operation" == "validate" ]]; then
            run_validation "$full_path" "$selected_file" "$tier_name"
        else
            run_profile "$full_path" "$selected_file" "$tier_name"
        fi
    else
        show_error "Invalid choice. Please try again."
        sleep 2
        select_file "$operation" "$tier"
    fi
}

###############################################################################
# Run Validation
###############################################################################

run_validation() {
    local file_path=$1
    local file_name=$2
    local tier_name=$3

    show_logo
    show_header "Running Validation - $tier_name Tier"

    # Create demo-tmp directory
    mkdir -p "$DEMO_TMP"

    # Generate a simple validation config
    local config_file="$DEMO_TMP/validation_config.yaml"
    local report_file="$DEMO_TMP/validation_report.html"
    local json_file="$DEMO_TMP/validation_report.json"

    show_info "Preparing validation for: $file_name"
    show_info "Tier: $tier_name"
    echo

    # Check if this is Ultimate tier - use comprehensive config
    if [[ "$tier_name" == "Ultimate" ]]; then
        local ultimate_config="$SCRIPT_DIR/test-data/configs/ultimate_validation_showcase.yaml"
        if [[ -f "$ultimate_config" ]]; then
            show_info "Ultimate tier detected - using comprehensive validation config"
            show_info "Testing all 31 validation types (excluding SQL validations)"
            echo

            # Show the command
            local cmd="python3 -m validation_framework.cli validate '$ultimate_config'"
            show_command "$cmd"

            show_info "Starting comprehensive validation (this may take several minutes)..."
            echo
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo

            # Run validation with the ultimate config
            if python3 -m validation_framework.cli validate "$ultimate_config" -o "$report_file" -j "$json_file"; then
                echo
                echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                echo
                show_success "Ultimate validation completed successfully!"
                show_info "Processed 357M rows across 2 files with 31 validation types"
                echo
                show_info "Reports generated:"
                echo "  • HTML Report: $report_file"
                echo "  • JSON Summary: $json_file"
                echo

                if [[ -f "$report_file" ]]; then
                    show_info "Open the HTML report in your browser to view detailed results"
                fi
            else
                echo
                echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
                echo
                show_error "Ultimate validation failed"
            fi

            echo
            echo -e -n "Press Enter to return to main menu..."
            read -r
            show_main_menu
            return
        else
            show_warning "Ultimate config not found at: $ultimate_config"
            show_info "Falling back to basic validation"
            echo
        fi
    fi

    # For non-Ultimate tiers or if Ultimate config not found, use basic config
    # Determine optimal chunk size based on file size
    local chunk_size=50000
    if [[ -f "$file_path" ]] || [[ -L "$file_path" ]]; then
        local file_size_mb=$(du -Lm "$file_path" 2>/dev/null | cut -f1)
        if [[ $file_size_mb -gt 1000 ]]; then
            # Large files (>1GB): Use 1M rows per chunk
            chunk_size=1000000
            show_info "Large file detected (${file_size_mb}MB) - using optimized chunk size: 1M rows"
        elif [[ $file_size_mb -gt 100 ]]; then
            # Medium files (>100MB): Use 500K rows per chunk
            chunk_size=500000
            show_info "Medium file detected (${file_size_mb}MB) - using chunk size: 500K rows"
        else
            show_info "Small file detected (${file_size_mb}MB) - using chunk size: 50K rows"
        fi
    fi
    echo

    # Create validation config
    cat > "$config_file" << EOF
validation_job:
  name: "DataK9 Demo - $tier_name Validation"
  description: "Quick validation demo for $file_name"

  files:
    - name: "$file_name"
      path: "$file_path"
      format: "${file_name##*.}"

      validations:
        - type: "EmptyFileCheck"
          severity: "ERROR"
          enabled: true
          params:
            check_data_rows: true

        - type: "SchemaValidation"
          severity: "WARNING"
          enabled: true
          params:
            allow_extra_columns: true

  processing:
    chunk_size: $chunk_size
    max_sample_failures: 100

  output:
    html_report: "$report_file"
    json_summary: "$json_file"
EOF

    show_success "Configuration created: $config_file"
    echo

    # Show the command
    local cmd="python3 -m validation_framework.cli validate '$config_file'"
    show_command "$cmd"

    show_info "Starting validation..."
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo

    # Run validation
    if python3 -m validation_framework.cli validate "$config_file"; then
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_success "Validation completed successfully!"
        echo
        show_info "Reports generated:"
        echo "  • HTML Report: $report_file"
        echo "  • JSON Summary: $json_file"
        echo

        if [[ -f "$report_file" ]]; then
            show_info "Open the HTML report in your browser to view detailed results"
        fi
    else
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_error "Validation failed"
    fi

    echo
    echo -e -n "Press Enter to return to main menu..."
    read -r
    show_main_menu
}

###############################################################################
# Run Profile
###############################################################################

run_profile() {
    local file_path=$1
    local file_name=$2
    local tier_name=$3

    show_logo
    show_header "Running Profile - $tier_name Tier"

    # Create demo-tmp directory
    mkdir -p "$DEMO_TMP"

    local report_file="$DEMO_TMP/profile_report.html"
    local json_file="$DEMO_TMP/profile_report.json"

    show_info "Preparing profile for: $file_name"
    show_info "Tier: $tier_name"
    echo

    # Show the command
    local cmd="python3 -m validation_framework.cli profile '$file_path' -o '$report_file' -j '$json_file'"
    show_command "$cmd"

    show_info "Starting profiler..."
    echo
    show_info "The profiler will analyze:"
    echo "  • Column data types and statistics"
    echo "  • Value distributions and patterns"
    echo "  • Missing values and anomalies"
    echo "  • Correlations and dependencies"
    echo "  • PII detection (emails, phones, SSN, etc.)"
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo

    # Run profiler
    if python3 -m validation_framework.cli profile "$file_path" -o "$report_file" -j "$json_file"; then
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_success "Profiling completed successfully!"
        echo
        show_info "Reports generated:"
        echo "  • HTML Report: $report_file"
        echo "  • JSON Summary: $json_file"
        echo

        if [[ -f "$report_file" ]]; then
            show_info "Open the HTML report in your browser to explore:"
            echo "  • Interactive data distribution charts"
            echo "  • Expandable column detail cards"
            echo "  • Suggested validation rules"
            echo "  • Anomaly detection results"
        fi
    else
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_error "Profiling failed"
    fi

    echo
    echo -e -n "Press Enter to return to main menu..."
    read -r
    show_main_menu
}

###############################################################################
# Main Entry Point
###############################################################################

main() {
    # Check if running from correct directory
    if [[ ! -d "validation_framework" ]]; then
        echo -e "${RED}Error: Please run this script from the data-validation-tool directory${NC}"
        exit 1
    fi

    # Check if Python module is available
    if ! python3 -c "import validation_framework" 2>/dev/null; then
        echo -e "${RED}Error: validation_framework module not found${NC}"
        echo "Please ensure the framework is properly installed"
        exit 1
    fi

    show_main_menu
}

# Run main function
main
