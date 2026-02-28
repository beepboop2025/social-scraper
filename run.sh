#!/bin/bash
# Social Media Scraper - Quick Start Script

set -e

echo "==================================="
echo "  Social Media Scraper Setup"
echo "==================================="
echo ""

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python version: $python_version"

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -q -r requirements.txt
echo "✓ Dependencies installed"

# Create directories
mkdir -p data media
echo "✓ Created data directories"

# Check for config
if [ ! -f "config.json" ]; then
    echo ""
    echo "Creating sample configuration..."
    python3 main.py --init-config
    echo "✓ Created config.json"
    echo ""
    echo "⚠️  IMPORTANT: Edit config.json with your credentials:"
    echo "   - Twitter: Add cookies.json path or use --login twitter"
    echo "   - Telegram: Add api_id and api_hash from https://my.telegram.org"
    echo ""
fi

echo ""
echo "==================================="
echo "  Quick Start Commands"
echo "==================================="
echo ""
echo "1. Twitter Login:"
echo "   python3 main.py --login twitter"
echo ""
echo "2. Scrape Twitter:"
echo "   python3 main.py --scrape twitter --query 'breaking news'"
echo ""
echo "3. Scrape Telegram:"
echo "   python3 main.py --scrape telegram --channel @bbcnews"
echo ""
echo "4. Scrape Both:"
echo "   python3 main.py --scrape news"
echo ""
echo "5. Stream Mode:"
echo "   python3 main.py --scrape news --stream"
echo ""
echo "6. Run Examples:"
echo "   python3 examples.py"
echo ""
echo "==================================="
