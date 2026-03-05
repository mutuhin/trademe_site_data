#!/bin/bash
# ================================================================
#  Trade Me Motors Scraper - Setup & Run Script
# ================================================================
# Usage:
#   chmod +x setup_and_run.sh
#   ./setup_and_run.sh          # Full setup + run
#   ./setup_and_run.sh --run    # Run only (skip setup)
#   ./setup_and_run.sh --cron   # Install daily cron job
# ================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
PYTHON="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

setup() {
    echo "🔧 Setting up virtual environment..."
    python3 -m venv "$VENV_DIR"

    echo "📦 Installing dependencies..."
    $PIP install --upgrade pip
    $PIP install -r "$SCRIPT_DIR/requirements.txt"

    echo "🌐 Installing Playwright Chromium browser..."
    $PYTHON -m playwright install chromium

    echo "✅ Setup complete!"
}

run() {
    echo "🚀 Running Trade Me Motors Scraper..."
    $PYTHON "$SCRIPT_DIR/trademe_scraper.py" --output "$SCRIPT_DIR/output" "$@"
}

install_cron() {
    # Run daily at 6:00 AM
    CRON_CMD="0 6 * * * cd $SCRIPT_DIR && $PYTHON trademe_scraper.py --output $SCRIPT_DIR/output >> $SCRIPT_DIR/logs/cron.log 2>&1"

    mkdir -p "$SCRIPT_DIR/logs"

    # Add to crontab (avoid duplicates)
    (crontab -l 2>/dev/null | grep -v "trademe_scraper.py"; echo "$CRON_CMD") | crontab -

    echo "✅ Daily cron job installed (runs at 6:00 AM)"
    echo "   Logs: $SCRIPT_DIR/logs/cron.log"
    echo ""
    echo "   To verify: crontab -l"
    echo "   To remove: crontab -l | grep -v trademe_scraper | crontab -"
}

# Parse arguments
case "$1" in
    --run)
        shift
        run "$@"
        ;;
    --cron)
        install_cron
        ;;
    --setup)
        setup
        ;;
    *)
        setup
        run "$@"
        ;;
esac
