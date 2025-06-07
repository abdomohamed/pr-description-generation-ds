#!/bin/bash

# Setup script for GitHub PR Data Extraction

echo "🚀 Setting up GitHub PR Data Extraction"
echo "======================================"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed."
    exit 1
fi

echo "✅ Python 3 found"

# Install dependencies
echo "📦 Installing dependencies..."
pip3 install -r requirements.txt

if [ $? -eq 0 ]; then
    echo "✅ Dependencies installed successfully"
else
    echo "❌ Failed to install dependencies"
    exit 1
fi

# Create output directory
mkdir -p extracted_data
echo "✅ Created output directory: extracted_data"

# Check for GitHub token
if [ -z "$GITHUB_TOKEN" ]; then
    echo ""
    echo "⚠️  GitHub token not found in environment variables"
    echo ""
    echo "To use this script, you need a GitHub personal access token."
    echo "Follow these steps:"
    echo ""
    echo "1. Go to: https://github.com/settings/personal-access-tokens/new"
    echo "2. Create a token with 'public_repo' and 'read:org' permissions"
    echo "3. Set it as an environment variable:"
    echo "   export GITHUB_TOKEN='your_token_here'"
    echo ""
    echo "Or edit config.py and set GITHUB_TOKEN = 'your_token_here'"
    echo ""
else
    echo "✅ GitHub token found in environment variables"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Set your GitHub token (if not already done)"
echo "2. Customize config.py if needed"
echo "3. Run: python3 extract_pr_data.py"
echo "4. Or run examples: python3 examples.py"
echo ""
