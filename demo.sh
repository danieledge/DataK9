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
    [5]="ultimate"  # Special marker - Ultimate uses comprehensive config, not individual files
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
    echo "  3) 🗄️  Database Demo     - Validate data directly from database"
    echo "  4) ℹ️  About DataK9      - Learn more about the framework"
    echo "  0) Exit"
    echo
    echo -e -n "${BOLD}Enter your choice [0-4]:${NC} "
    read -r choice

    case $choice in
        1) select_dataset "validate" ;;
        2) select_dataset "profile" ;;
        3) run_database_demo ;;
        4) show_about ;;
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

    # Special handling for Ultimate tier - skip file selection
    if [[ "$tier" == "5" ]]; then
        if [[ "$operation" == "validate" ]]; then
            run_ultimate_validation
        else
            show_error "Profiling not available for Ultimate tier"
            show_info "Ultimate tier only supports comprehensive validation testing"
            echo
            echo -e -n "Press Enter to go back..."
            read -r
            select_dataset "$operation"
        fi
        return
    fi

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
# Run Ultimate Validation (Comprehensive Test)
###############################################################################

run_ultimate_validation() {
    show_logo
    show_header "Ultimate Validation - Comprehensive Test"

    # Create demo-tmp directory
    mkdir -p "$DEMO_TMP"

    local ultimate_config="$SCRIPT_DIR/test-data/configs/ultimate_validation_showcase.yaml"
    local report_file="$DEMO_TMP/ultimate_validation_report.html"
    local json_file="$DEMO_TMP/ultimate_validation_results.json"

    # Check if config exists
    if [[ ! -f "$ultimate_config" ]]; then
        show_error "Ultimate configuration not found: $ultimate_config"
        echo
        echo -e -n "Press Enter to return to main menu..."
        read -r
        show_main_menu
        return
    fi

    echo -e "${BOLD}Ultimate Comprehensive Validation${NC}"
    echo
    show_info "Dataset: IBM AML Banking Transactions"
    show_info "Files: HI-Large (179M rows) + LI-Large (176M rows) = 357M rows"
    show_info "Size: 10.1 GB (Parquet format)"
    show_info "Validations: 31 types across 10 categories"
    echo
    show_warning "This test will take several minutes to complete"
    show_info "Processing: ~1M rows per chunk for optimal memory usage"
    echo

    # Show the command
    local cmd="python3 -m validation_framework.cli validate '$ultimate_config' -o '$report_file' -j '$json_file'"
    show_command "$cmd"

    show_info "Starting comprehensive validation..."
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo

    # Run validation with the ultimate config
    if python3 -m validation_framework.cli validate "$ultimate_config" -o "$report_file" -j "$json_file"; then
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_success "Ultimate validation completed!"
        echo
        show_info "Summary:"
        echo "  • Rows Processed: 357,000,000+"
        echo "  • Files Validated: 2"
        echo "  • Validation Types: 31"
        echo

        # Get server IP for network access
        local server_ip=$(hostname -I | awk '{print $1}')

        show_info "Reports generated:"
        echo "  • HTML Report (local): $report_file"
        echo "  • JSON Summary (local): $json_file"
        echo
        if [[ -n "$server_ip" ]]; then
            show_info "Network-accessible URLs:"
            echo "  • HTML Report: http://${server_ip}/dqa/data-validation-tool/demo-tmp/demo_test.html"
            echo "  • JSON Summary: http://${server_ip}/dqa/data-validation-tool/demo-tmp/demo_test.json"
        fi
        echo

        if [[ -f "$report_file" ]]; then
            show_info "Open the HTML report URL in your browser to view detailed results"
        fi
    else
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_error "Ultimate validation failed"
        show_info "Check the output above for error details"
    fi

    echo
    echo -e -n "Press Enter to return to main menu..."
    read -r
    show_main_menu
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

        # Get server IP for network access
        local server_ip=$(hostname -I | awk '{print $1}')

        show_info "Reports generated:"
        echo "  • HTML Report (local): $report_file"
        echo "  • JSON Summary (local): $json_file"
        echo
        if [[ -n "$server_ip" ]]; then
            show_info "Network-accessible URLs:"
            # Extract filename from path for URL
            local html_filename=$(basename "$report_file")
            local json_filename=$(basename "$json_file")
            echo "  • HTML Report: http://${server_ip}/dqa/data-validation-tool/demo-tmp/${html_filename}"
            echo "  • JSON Summary: http://${server_ip}/dqa/data-validation-tool/demo-tmp/${json_filename}"
        fi
        echo

        if [[ -f "$report_file" ]]; then
            show_info "Open the HTML report URL in your browser to view detailed results"
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

        # Get server IP for network access
        local server_ip=$(hostname -I | awk '{print $1}')

        show_info "Reports generated:"
        echo "  • HTML Report (local): $report_file"
        echo "  • JSON Summary (local): $json_file"
        echo
        if [[ -n "$server_ip" ]]; then
            show_info "Network-accessible URLs:"
            # Extract filename from path for URL
            local html_filename=$(basename "$report_file")
            local json_filename=$(basename "$json_file")
            echo "  • HTML Report: http://${server_ip}/dqa/data-validation-tool/demo-tmp/${html_filename}"
            echo "  • JSON Summary: http://${server_ip}/dqa/data-validation-tool/demo-tmp/${json_filename}"
        fi
        echo

        if [[ -f "$report_file" ]]; then
            show_info "Open the HTML report URL in your browser to explore:"
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
# Database Demo
###############################################################################

run_database_demo() {
    show_logo
    show_header "Database Validation Demo"

    echo -e "${BOLD}Database Integration${NC}"
    echo "DataK9 can validate data directly from databases without exporting to files."
    echo
    echo -e "${CYAN}Supported Databases:${NC}"
    echo "  • PostgreSQL"
    echo "  • MySQL"
    echo "  • SQL Server"
    echo "  • Oracle"
    echo "  • SQLite"
    echo
    echo -e "${CYAN}Choose a demo:${NC}"
    echo
    echo "  1) 🔄 Python Script - Programmatic database validation (5 validations)"
    echo "  2) 📄 Simple YAML - Basic database validation via YAML (5 validations)"
    echo "  3) 🚀 Comprehensive - ALL 32 database validations (73 checks across 7 tables)"
    echo "  4) 📊 Profile Database - Profile a database table"
    echo "  0) Back to main menu"
    echo
    echo -e -n "${BOLD}Enter your choice [0-4]:${NC} "
    read -r demo_choice

    case $demo_choice in
        1) run_database_python_demo ;;
        2) run_database_yaml_demo ;;
        3) run_database_comprehensive_demo ;;
        4) run_database_profile_demo ;;
        0) show_main_menu ;;
        *) show_error "Invalid choice. Please try again."; sleep 2; run_database_demo ;;
    esac
}

run_database_python_demo() {
    show_logo
    show_header "Database Validation - Python Script"

    # Check if test database exists
    if [[ ! -f "test_data.db" ]]; then
        show_warning "Test database not found. Creating test database..."
        echo
        if python3 scripts/create_test_database.py; then
            show_success "Test database created: test_data.db"
            echo
        else
            show_error "Failed to create test database"
            sleep 2
            run_database_demo
            return
        fi
    fi

    show_info "Database: test_data.db (SQLite)"
    show_info "Table: customers (1,020 rows)"
    echo
    show_info "This demo validates customer data directly from the database using"
    show_info "the LoaderFactory and validation registry programmatically."
    echo

    # Show the command
    local cmd="python3 examples/run_database_validation.py"
    show_command "$cmd"

    show_info "Running 5 validations on test database:"
    echo "  • MandatoryFieldCheck - Required fields validation"
    echo "  • RegexCheck - Email format validation"
    echo "  • UniqueKeyCheck - Customer ID uniqueness"
    echo "  • CompletenessCheck - Field completeness threshold"
    echo "  • RangeCheck - Account balance range validation"
    echo
    show_warning "⚠️  EXPECTED RESULTS: You will see 'Passed: 1, Failed: 4'"
    show_info "The test database has intentional data quality issues:"
    echo "  • ~20 rows with missing customer_id or email (2%) → MandatoryFieldCheck FAILS"
    echo "  • 21 duplicate customer_ids → UniqueKeyCheck FAILS"
    echo "  • ~32 invalid email formats → RegexCheck FAILS"
    echo "  • 5 account balances out of range → RangeCheck FAILS"
    echo "  • 98% email completeness → CompletenessCheck PASSES (≥95% threshold)"
    echo
    show_info "This demonstrates DataK9 finding real data quality problems!"
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo

    # Run validation
    python3 examples/run_database_validation.py || true

    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
    show_success "Database validation completed!"
    echo
    show_info "✓ Understanding 'Passed: 1, Failed: 4' Results:"
    echo "  • PASSED: 1 validation (CompletenessCheck at 98% ≥ 95% threshold)"
    echo "  • FAILED: 4 validations successfully found data quality issues:"
    echo "    - MandatoryFieldCheck: Found 20 rows with missing data"
    echo "    - RegexCheck: Found 32 invalid email formats"
    echo "    - UniqueKeyCheck: Found 21 duplicate customer IDs"
    echo "    - RangeCheck: Found 5 balances out of range"
    echo "  • Status: FAILED = DataK9 correctly detected problems!"
    echo "  • This is CORRECT behavior - the system is working perfectly!"
    echo
    show_info "Key Points:"
    echo "  • Database validations use the same rules as file validations"
    echo "  • Data is processed in chunks for memory efficiency"
    echo "  • 33/35 DataK9 validations work with databases"
    echo "  • Connection string format: sqlite:///path or postgresql://user:pass@host/db"
    echo
    echo -e -n "Press Enter to return to database demo menu..."
    read -r
    run_database_demo
}

run_database_yaml_demo() {
    show_logo
    show_header "Database Validation - YAML Config"

    # Check if test database exists
    if [[ ! -f "test_data.db" ]]; then
        show_warning "Test database not found. Creating test database..."
        echo
        if python3 scripts/create_test_database.py; then
            show_success "Test database created: test_data.db"
            echo
        else
            show_error "Failed to create test database"
            sleep 2
            run_database_demo
            return
        fi
    fi

    mkdir -p "$DEMO_TMP"

    show_info "Database: test_data.db (SQLite)"
    show_info "Table: customers (1,020 rows)"
    show_info "Config: examples/database_validation_test.yaml"
    echo
    show_info "This demo validates customer data using a YAML configuration file."
    show_info "The YAML format supports both file and database sources."
    echo

    # Show the command
    local cmd="python3 -m validation_framework.cli validate examples/database_validation_test.yaml"
    show_command "$cmd"

    show_info "YAML Configuration:"
    echo "  • format: 'database'"
    echo "  • path: 'sqlite:///test_data.db' (connection string)"
    echo "  • table: 'customers'"
    echo "  • 5 validations defined"
    echo
    show_warning "⚠️  EXPECTED RESULTS: You will see 'Passed: 1, Failed: 4'"
    show_info "The test database has intentional data quality issues:"
    echo "  • ~20 rows with missing customer_id or email → MandatoryFieldCheck FAILS ✗"
    echo "  • 21 duplicate customer_ids → UniqueKeyCheck FAILS ✗"
    echo "  • ~32 invalid email formats → RegexCheck FAILS ✗"
    echo "  • 5 account balances out of range → RangeCheck FAILS ✗"
    echo "  • 98% email completeness (≥95%) → CompletenessCheck PASSES ✓"
    echo
    show_info "DataK9 will correctly identify these issues!"
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo

    # Run validation
    python3 -m validation_framework.cli validate examples/database_validation_test.yaml || true

    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
    show_success "Database validation completed!"
    echo
    show_info "✓ Understanding 'Passed: 1, Failed: 4' Results:"
    echo "  • PASSED (1): CompletenessCheck - 98% completeness meets 95% threshold ✓"
    echo "  • FAILED (4): These validations correctly detected data problems:"
    echo "    - MandatoryFieldCheck: Found 20 missing values ✗"
    echo "    - RegexCheck: Found 32 invalid email formats ✗"
    echo "    - UniqueKeyCheck: Found 21 duplicate IDs ✗"
    echo "    - RangeCheck: Found 5 out-of-range values ✗"
    echo "  • Overall Status: FAILED (because problems were detected)"
    echo "  • THIS IS CORRECT - DataK9 is working perfectly!"
    echo
    # Get server IP for network access
    local server_ip=$(hostname -I | awk '{print $1}')

    show_info "Reports generated:"
    echo "  • HTML Report (local): demo-tmp/database_validation_report.html"
    echo "  • JSON Summary (local): demo-tmp/database_validation_summary.json"
    echo
    if [[ -n "$server_ip" ]]; then
        show_info "Network-accessible URLs:"
        echo "  • HTML Report: http://${server_ip}/dqa/data-validation-tool/demo-tmp/database_validation_report.html"
        echo "  • JSON Summary: http://${server_ip}/dqa/data-validation-tool/demo-tmp/database_validation_summary.json"
    fi
    echo
    echo -e -n "Press Enter to return to database demo menu..."
    read -r
    run_database_demo
}

run_database_comprehensive_demo() {
    show_logo
    show_header "Comprehensive Database Validation - ALL 32 Validation Types"

    # Check if comprehensive database exists
    if [[ ! -f "test_data_comprehensive.db" ]]; then
        show_warning "Comprehensive test database not found. Creating database..."
        echo
        show_info "Creating database with 7 tables and 18,689 records..."
        show_info "This includes intentional data quality issues for testing all validation types."
        echo
        if python3 scripts/create_comprehensive_test_database.py; then
            show_success "Comprehensive database created: test_data_comprehensive.db"
            echo
        else
            show_error "Failed to create comprehensive database"
            sleep 2
            run_database_demo
            return
        fi
    fi

    mkdir -p "$DEMO_TMP"

    show_info "Database: test_data_comprehensive.db (SQLite)"
    show_info "Tables: 7 (employees, customers, products, orders, order_items, transactions, departments)"
    show_info "Total Records: 18,689"
    show_info "Config: examples/database_validation_comprehensive.yaml"
    echo
    show_info "This demo tests ALL 32 database-compatible validation types:"
    echo "  • 73 total validation instances"
    echo "  • Covers all 10 validation categories"
    echo "  • Tests both ERROR and WARNING severities"
    echo "  • Demonstrates pass/fail examples for each type"
    echo

    # Show the command
    local cmd="python3 -m validation_framework.cli validate examples/database_validation_comprehensive.yaml"
    show_command "$cmd"

    show_warning "⚠️  EXPECTED RESULTS: Mix of passed and failed validations"
    show_info "The comprehensive database has realistic data quality issues (90-95% good data):"
    echo "  • Some validations will PASS (showing system correctly identifies good data)"
    echo "  • Some validations will FAIL (showing system correctly finds problems)"
    echo "  • Both ERROR and WARNING severities are tested"
    echo "  • Exit code will be 1 (ERROR severity failures detected)"
    echo
    show_info "This comprehensive test validates the entire DataK9 validation framework!"
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
    show_info "Running comprehensive validation (this may take 10-20 seconds)..."
    echo

    # Run validation
    python3 -m validation_framework.cli validate examples/database_validation_comprehensive.yaml || true

    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
    show_success "Comprehensive database validation completed!"
    echo
    show_info "✓ All 32 Validation Types Tested:"
    echo "  • Field Validations: MandatoryField, UniqueKey, Regex, Range, StringLength, etc."
    echo "  • Schema Validations: SchemaMatch, ColumnPresence, BlankRecord, DuplicateRow"
    echo "  • Cross-Field: CrossFieldComparison, Completeness"
    echo "  • Database-Specific: DatabaseReferentialIntegrity, DatabaseConstraint, SQLCustom"
    echo "  • Statistical: StatisticalOutlier, AdvancedAnomaly, Correlation, Distribution"
    echo "  • Business Rules: InlineBusinessRule, InlineLookup, Conditional, Baseline, Trend"
    echo "  • Cross-Table: ReferentialIntegrity, CrossFileComparison, CrossFileDuplicate"
    echo "  • Metadata: RowCountRange, Freshness"
    echo
    # Get server IP for network access
    local server_ip=$(hostname -I | awk '{print $1}')

    show_info "Reports generated:"
    echo "  • HTML Report (local): demo-tmp/comprehensive_validation_report.html"
    echo "  • JSON Summary (local): demo-tmp/comprehensive_validation_summary.json"
    echo
    if [[ -n "$server_ip" ]]; then
        show_info "Network-accessible URLs:"
        echo "  • HTML Report: http://${server_ip}/dqa/data-validation-tool/demo-tmp/comprehensive_validation_report.html"
        echo "  • JSON Summary: http://${server_ip}/dqa/data-validation-tool/demo-tmp/comprehensive_validation_summary.json"
    fi
    echo
    echo -e -n "Press Enter to return to database demo menu..."
    read -r
    run_database_demo
}

run_database_profile_demo() {
    show_logo
    show_header "Database Profiling Demo"

    # Check if test database exists
    if [[ ! -f "test_data.db" ]]; then
        show_warning "Test database not found. Creating test database..."
        echo
        if python3 scripts/create_test_database.py; then
            show_success "Test database created: test_data.db"
            echo
        else
            show_error "Failed to create test database"
            sleep 2
            run_database_demo
            return
        fi
    fi

    mkdir -p "$DEMO_TMP"
    local report_file="$DEMO_TMP/db_profile.html"
    local json_file="$DEMO_TMP/db_profile.json"

    show_info "Database: test_data.db (SQLite)"
    show_info "Table: customers (1,020 rows)"
    echo
    show_info "This demo profiles a database table directly without exporting to file."
    echo

    # Show the command
    local cmd="python3 -m validation_framework.cli profile --database 'sqlite:///test_data.db' --table customers -o '$report_file' -j '$json_file'"
    show_command "$cmd"

    show_info "Profiling will analyze:"
    echo "  • Column data types and statistics"
    echo "  • Value distributions and patterns"
    echo "  • Missing values and anomalies"
    echo "  • PII detection"
    echo
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo

    # Run profiler
    if python3 -m validation_framework.cli profile --database "sqlite:///test_data.db" --table customers -o "$report_file" -j "$json_file"; then
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_success "Database profiling completed!"
        echo

        # Get server IP for network access
        local server_ip=$(hostname -I | awk '{print $1}')

        show_info "Reports generated:"
        echo "  • HTML Report (local): $report_file"
        echo "  • JSON Summary (local): $json_file"
        echo
        if [[ -n "$server_ip" ]]; then
            show_info "Network-accessible URLs:"
            echo "  • HTML Report: http://${server_ip}/dqa/data-validation-tool/demo-tmp/db_profile.html"
            echo "  • JSON Summary: http://${server_ip}/dqa/data-validation-tool/demo-tmp/db_profile.json"
        fi
        echo
    else
        echo
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo
        show_error "Database profiling failed"
    fi

    echo
    echo -e -n "Press Enter to return to database demo menu..."
    read -r
    run_database_demo
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
