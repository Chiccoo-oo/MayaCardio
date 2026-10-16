#!/usr/bin/env bash
# ==============================================================================
# MayaCardio Bedside Device & Cloud Installer
# Configures a fresh Linux / ARM64 board (e.g. Raspberry Pi OS Bookworm)
# ==============================================================================
set -e

echo "=== Installing MayaCardio Dependencies and Services ==="

# Check Python version
PYTHON_BIN=$(command -v python3 || true)
if [ -z "$PYTHON_BIN" ]; then
    echo "Error: Python 3 is not installed. Run: sudo apt update && sudo apt install -y python3 python3-pip"
    exit 1
fi

PY_VERSION=$("$PYTHON_BIN" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo "Detected Python $PY_VERSION"

# Setup installation directory
INSTALL_DIR="/opt/mayacardio"
DATA_DIR="/var/lib/mayacardio"
CONFIG_DIR="/etc/mayacardio"

sudo mkdir -p "$INSTALL_DIR"
sudo mkdir -p "$DATA_DIR"
sudo mkdir -p "$CONFIG_DIR"

# Copy source code
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Copying files from $REPO_DIR to $INSTALL_DIR..."
sudo cp -r "$REPO_DIR"/* "$INSTALL_DIR/"

# Make pipeline executable and symlink to /usr/local/bin
sudo chmod +x "$INSTALL_DIR/pipeline"
sudo chmod +x "$INSTALL_DIR/pipeline.py"
sudo ln -sf "$INSTALL_DIR/pipeline" /usr/local/bin/pipeline

# Install systemd unit files if systemd is active
if command -v systemctl >/dev/null 2>&1; then
    echo "Installing systemd service units..."
    sudo cp "$INSTALL_DIR/systemd/mayacardio-pipeline.service" /etc/systemd/system/
    sudo cp "$INSTALL_DIR/systemd/mayacardio-cloud.service" /etc/systemd/system/
    sudo systemctl daemon-reload
    echo "Systemd services installed."
    echo "To enable and start the device daemon on boot:"
    echo "  sudo systemctl enable --now mayacardio-pipeline"
fi

echo ""
echo "=== MayaCardio installation completed successfully! ==="
echo "Test pipeline CLI by running:"
echo "  pipeline status"
