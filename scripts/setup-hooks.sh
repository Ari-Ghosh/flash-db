#!/bin/bash

# Setup Git hooks for local development
# Run this script after cloning the repository: ./scripts/setup-hooks.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GIT_HOOKS_DIR="$SCRIPT_DIR/../.git/hooks"

if [ ! -d "$GIT_HOOKS_DIR" ]; then
    echo "❌ .git/hooks directory not found"
    echo "   Make sure you're in the repository root"
    exit 1
fi

# Install pre-commit hook
echo "Installing pre-commit hook..."
cp "$SCRIPT_DIR/pre-commit" "$GIT_HOOKS_DIR/pre-commit"
chmod +x "$GIT_HOOKS_DIR/pre-commit"
echo "✅ Pre-commit hook installed"

echo ""
echo "Git hooks setup complete!"
echo "Hooks will run automatically before each commit."
echo ""
echo "To skip hooks (not recommended):"
echo "  git commit --no-verify"
