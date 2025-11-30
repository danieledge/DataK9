#!/bin/bash
# Setup script for DataK9 AI Summary feature
# Installs llama-cpp-python and downloads a small, fast GGUF model

set -e

echo "============================================"
echo "DataK9 AI Summary Setup"
echo "============================================"
echo ""

# Default model directory
MODEL_DIR="${HOME}/.local/share/models"
MODEL_NAME="qwen2.5-1.5b-instruct-q4_k_m.gguf"
MODEL_URL="https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF/resolve/main/qwen2.5-1.5b-instruct-q4_k_m.gguf"

# Step 1: Install llama-cpp-python
echo "[1/3] Installing llama-cpp-python..."
if python3 -c "import llama_cpp" 2>/dev/null; then
    echo "  ✓ llama-cpp-python already installed"
else
    echo "  Installing llama-cpp-python (this may take a few minutes)..."
    pip3 install llama-cpp-python --break-system-packages 2>/dev/null || pip3 install llama-cpp-python
    echo "  ✓ llama-cpp-python installed"
fi

# Step 2: Create model directory
echo ""
echo "[2/3] Setting up model directory..."
mkdir -p "$MODEL_DIR"
echo "  ✓ Model directory: $MODEL_DIR"

# Step 3: Download model if not present
echo ""
echo "[3/3] Checking for GGUF model..."
MODEL_PATH="$MODEL_DIR/$MODEL_NAME"

if [ -f "$MODEL_PATH" ]; then
    echo "  ✓ Model already exists: $MODEL_PATH"
else
    echo "  Downloading Qwen2.5-1.5B model (~1GB)..."
    echo "  This model offers the best balance of quality and speed."
    echo ""

    # Try wget first, fall back to curl
    if command -v wget &> /dev/null; then
        wget -q --show-progress -O "$MODEL_PATH" "$MODEL_URL"
    elif command -v curl &> /dev/null; then
        curl -L --progress-bar -o "$MODEL_PATH" "$MODEL_URL"
    else
        echo "  ✗ Error: Neither wget nor curl found. Please install one."
        exit 1
    fi

    echo "  ✓ Model downloaded: $MODEL_PATH"
fi

# Verify installation
echo ""
echo "============================================"
echo "Setup Complete!"
echo "============================================"
echo ""
echo "Model location: $MODEL_PATH"
echo ""
echo "To use AI summaries in your profile reports:"
echo "  python3 -m validation_framework.cli profile data.csv --beta-llm"
echo ""
echo "Or set the environment variable:"
echo "  export DATAK9_LLM_MODEL=$MODEL_PATH"
echo ""
