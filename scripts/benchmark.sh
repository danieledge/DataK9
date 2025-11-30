#!/usr/bin/env bash
#
# DataK9 Benchmark Runner
# =======================
# A TUI-based benchmark harness for testing the DataK9 data profiler
# against well-known public datasets.
#
# Usage: ./benchmark.sh
#
# Directories:
#   benchmark/data/   - Downloaded datasets (NOT committed to git)
#   benchmark/output/ - Profiler outputs (NOT committed to git)
#
# The PROFILE_CMD below is wired to the actual DataK9 profiler CLI.
#

set -euo pipefail

# ============================================================
# CONFIGURATION
# ============================================================

# DataK9 profiler command - this is the real CLI used in this repo
# Usage: python3 -m validation_framework.cli profile <input> -o <output.html> -j <output.json>
PROFILE_CMD="python3 -m validation_framework.cli profile"

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/benchmark/data"
OUTPUT_DIR="${SCRIPT_DIR}/benchmark/output"

# ============================================================
# DATASET DEFINITIONS (single source of truth)
# ============================================================
# Format: id|name|url|filename|description|benchmarks

DATASETS=(
    "titanic|Titanic (Survival)|https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv|titanic.csv|Mixed numeric + categorical passenger data with real-world missingness (e.g., Age, Cabin).|Good for testing missingness handling, categorical distributions, target-like flags, and small-N behavior."
    "adult|Adult Income (Census)|https://raw.githubusercontent.com/plotly/datasets/master/adult.csv|adult.csv|Census-like mixed dataset with both numeric and categorical variables and irregular missingness encoding.|Good for messy mixed-type handling, valid-values suggestions, and simple fairness-structure analysis."
    "nyc_taxi|NYC Yellow Taxi (Jan 2019)|https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv|yellow_tripdata_2019-01.csv|Trip-level transport data with pickup/dropoff timestamps, monetary amounts, and geospatial-like fields.|Good for temporal gap logic, large-row-count stress, skewed numeric distributions, and outlier handling."
    "telco|Telco Customer Churn (IBM)|https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv|Telco-Customer-Churn.csv|Subscription-level customer dataset with many categorical fields and a churn-like target.|Good for high-cardinality categoricals, target imbalance, and target-feature association testing."
)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

# Parse dataset entry
get_field() {
    local entry="$1"
    local field_num="$2"
    echo "$entry" | cut -d'|' -f"$field_num"
}

# Ensure directories exist
ensure_dirs() {
    mkdir -p "$DATA_DIR"
    mkdir -p "$OUTPUT_DIR"
}

# Check for download tools
check_download_tool() {
    if command -v curl &> /dev/null; then
        echo "curl"
    elif command -v wget &> /dev/null; then
        echo "wget"
    else
        echo ""
    fi
}

# Download a file
download_file() {
    local url="$1"
    local dest="$2"
    local tool
    tool=$(check_download_tool)

    if [[ -z "$tool" ]]; then
        echo "ERROR: Neither curl nor wget is available. Please install one."
        return 1
    fi

    echo "Downloading: $url"
    echo "Destination: $dest"
    echo ""

    if [[ "$tool" == "curl" ]]; then
        curl -L -o "$dest" "$url" --progress-bar
    else
        wget -O "$dest" "$url" --show-progress
    fi

    if [[ -f "$dest" ]]; then
        local size
        size=$(du -h "$dest" | cut -f1)
        echo ""
        echo "Download complete: $dest ($size)"
        return 0
    else
        echo "ERROR: Download failed"
        return 1
    fi
}

# Download dataset if not exists
download_dataset() {
    local entry="$1"
    local id name url filename
    id=$(get_field "$entry" 1)
    name=$(get_field "$entry" 2)
    url=$(get_field "$entry" 3)
    filename=$(get_field "$entry" 4)

    local dest="${DATA_DIR}/${filename}"

    echo "Dataset: $name"
    echo ""

    if [[ -f "$dest" ]]; then
        local size
        size=$(du -h "$dest" | cut -f1)
        echo "File already exists: $dest ($size)"
        echo "Skipping download."
        return 0
    fi

    download_file "$url" "$dest"
}

# Run profiler on dataset
run_profile() {
    local entry="$1"
    local id name filename
    id=$(get_field "$entry" 1)
    name=$(get_field "$entry" 2)
    filename=$(get_field "$entry" 4)

    local input_path="${DATA_DIR}/${filename}"
    local output_html="${OUTPUT_DIR}/${id}-profile.html"
    local output_json="${OUTPUT_DIR}/${id}-profile.json"

    echo "Running profiler on: $name"
    echo ""
    echo "Input:  $input_path"
    echo "Output: $output_html"
    echo "        $output_json"
    echo ""

    if [[ ! -f "$input_path" ]]; then
        echo "ERROR: Input file not found: $input_path"
        echo "Please download the dataset first."
        return 1
    fi

    # Verify PROFILE_CMD is set
    if [[ -z "$PROFILE_CMD" ]]; then
        echo "ERROR: PROFILE_CMD is not configured"
        return 1
    fi

    echo "Executing: $PROFILE_CMD \"$input_path\" -o \"$output_html\" -j \"$output_json\""
    echo ""
    echo "═══════════════════════════════════════════════════════════════════════════"

    # Run the profiler
    if $PROFILE_CMD "$input_path" -o "$output_html" -j "$output_json"; then
        echo "═══════════════════════════════════════════════════════════════════════════"
        echo ""
        echo "Profiling complete!"
        echo "  HTML: $output_html"
        echo "  JSON: $output_json"
        return 0
    else
        echo "═══════════════════════════════════════════════════════════════════════════"
        echo ""
        echo "ERROR: Profiler exited with non-zero status"
        return 1
    fi
}

# Clear screen
clear_screen() {
    clear 2>/dev/null || printf '\033[2J\033[H'
}

# Pause for user
pause() {
    echo ""
    read -r -p "Press Enter to continue..."
}

# ============================================================
# MENU FUNCTIONS
# ============================================================

# Show dataset detail and submenu
show_dataset_menu() {
    local entry="$1"
    local id name url filename description benchmarks
    id=$(get_field "$entry" 1)
    name=$(get_field "$entry" 2)
    url=$(get_field "$entry" 3)
    filename=$(get_field "$entry" 4)
    description=$(get_field "$entry" 5)
    benchmarks=$(get_field "$entry" 6)

    while true; do
        clear_screen
        echo "══════════════════════════════════════════════════════════════════════════"
        echo "  Dataset: $name"
        echo "══════════════════════════════════════════════════════════════════════════"
        echo ""
        echo "  ID:          $id"
        echo "  Source URL:  $url"
        echo "  Local file:  ${DATA_DIR}/${filename}"
        echo ""
        echo "  Description:"
        echo "    $description"
        echo ""
        echo "  Benchmarks:"
        echo "    $benchmarks"
        echo ""

        # Check if file exists
        if [[ -f "${DATA_DIR}/${filename}" ]]; then
            local size
            size=$(du -h "${DATA_DIR}/${filename}" | cut -f1)
            echo "  Status: Downloaded ($size)"
        else
            echo "  Status: Not downloaded"
        fi

        # Check if output exists
        if [[ -f "${OUTPUT_DIR}/${id}-profile.html" ]]; then
            echo "  Output: Profiled (${OUTPUT_DIR}/${id}-profile.html)"
        else
            echo "  Output: Not profiled yet"
        fi

        echo ""
        echo "══════════════════════════════════════════════════════════════════════════"
        echo ""
        echo "  1) Download only"
        echo "  2) Download and run profiler"
        echo "  3) Back to main menu"
        echo ""
        read -r -p "  Select option: " choice

        case "$choice" in
            1)
                clear_screen
                echo "Downloading dataset..."
                echo ""
                download_dataset "$entry"
                pause
                ;;
            2)
                clear_screen
                echo "Downloading dataset (if needed) and running profiler..."
                echo ""
                download_dataset "$entry"
                echo ""
                run_profile "$entry"
                pause
                ;;
            3)
                return
                ;;
            *)
                echo "Invalid option"
                sleep 1
                ;;
        esac
    done
}

# Run all datasets
run_all() {
    clear_screen
    echo "══════════════════════════════════════════════════════════════════════════"
    echo "  Running ALL Benchmark Datasets"
    echo "══════════════════════════════════════════════════════════════════════════"
    echo ""

    local total=${#DATASETS[@]}
    local current=0
    local failed=0

    for entry in "${DATASETS[@]}"; do
        ((current++))
        local name
        name=$(get_field "$entry" 2)

        echo ""
        echo "────────────────────────────────────────────────────────────────────────────"
        echo "  [$current/$total] $name"
        echo "────────────────────────────────────────────────────────────────────────────"
        echo ""

        if ! download_dataset "$entry"; then
            echo "WARNING: Download failed for $name"
            ((failed++))
            continue
        fi

        echo ""

        if ! run_profile "$entry"; then
            echo "WARNING: Profiling failed for $name"
            ((failed++))
        fi
    done

    echo ""
    echo "══════════════════════════════════════════════════════════════════════════"
    echo "  Benchmark Complete"
    echo "══════════════════════════════════════════════════════════════════════════"
    echo ""
    echo "  Total datasets: $total"
    echo "  Successful: $((total - failed))"
    echo "  Failed: $failed"
    echo ""
    echo "  Outputs saved to: $OUTPUT_DIR"

    pause
}

# Main menu
main_menu() {
    ensure_dirs

    while true; do
        clear_screen
        echo ""
        echo "  ╔════════════════════════════════════════════════════════════════════╗"
        echo "  ║           DataK9 Data Profiler Benchmark Menu                      ║"
        echo "  ╚════════════════════════════════════════════════════════════════════╝"
        echo ""

        local i=1
        for entry in "${DATASETS[@]}"; do
            local name
            name=$(get_field "$entry" 2)
            printf "    %d) %s\n" "$i" "$name"
            ((i++))
        done

        echo ""
        echo "    a) Run ALL datasets (download + profile)"
        echo "    q) Quit"
        echo ""
        echo "  ════════════════════════════════════════════════════════════════════════"
        echo ""
        read -r -p "    Select option: " choice

        case "$choice" in
            [1-9])
                local idx=$((choice - 1))
                if [[ $idx -lt ${#DATASETS[@]} ]]; then
                    show_dataset_menu "${DATASETS[$idx]}"
                else
                    echo "Invalid option"
                    sleep 1
                fi
                ;;
            a|A)
                run_all
                ;;
            q|Q)
                echo ""
                echo "Goodbye!"
                exit 0
                ;;
            *)
                echo "Invalid option"
                sleep 1
                ;;
        esac
    done
}

# ============================================================
# MAIN ENTRY POINT
# ============================================================

main_menu
