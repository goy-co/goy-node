#!/usr/bin/env bash
# Setup script for Git hooks in this repository

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

chmod +x "$SCRIPT_DIR/commit-msg"

# Configure git to use .githooks directory
git -C "$REPO_ROOT" config core.hooksPath .githooks

# Also copy hook to .git/hooks for fallback compatibility
if [ -d "$REPO_ROOT/.git/hooks" ]; then
    cp "$SCRIPT_DIR/commit-msg" "$REPO_ROOT/.git/hooks/commit-msg"
    chmod +x "$REPO_ROOT/.git/hooks/commit-msg"
fi

echo "✔ Git hooks configured successfully! Conventional Commits validation is active."
