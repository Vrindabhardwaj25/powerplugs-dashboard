#!/bin/bash
# deploy.sh — Refresh dashboard data + push all files to GitHub/Render
# Usage: ./deploy.sh "optional commit message"

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DIR="/Users/vrindabhardwaj/Downloads/ultrahuman_mcp_package_20251112v1"

echo "=== Powerplugs Dashboard Deploy ==="
echo ""

# 1. Run refresh to generate latest HTML with fresh data
echo "📊 Refreshing dashboard data..."
cd "$LOCAL_DIR"
python3 refresh_dashboard.py
echo ""

# 2. Copy all files to git repo
echo "📁 Syncing files to repo..."
cp "$LOCAL_DIR/powerplugs_dashboard.html" "$SCRIPT_DIR/index.html"
cp "$LOCAL_DIR/dashboard_template.html" "$SCRIPT_DIR/dashboard_template.html"
cp "$LOCAL_DIR/refresh_dashboard.py" "$SCRIPT_DIR/refresh_dashboard.py"

# 3. Stage and commit
cd "$SCRIPT_DIR"
git add index.html dashboard_template.html refresh_dashboard.py

if git diff --staged --quiet; then
  echo "✅ No changes to commit — everything is up to date."
  exit 0
fi

MSG="${1:-Update dashboard $(date '+%Y-%m-%d %H:%M')}"
git commit -m "$MSG

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"

# 4. Pull + push (handle any remote changes)
echo ""
echo "🚀 Pushing to GitHub..."
git pull --rebase origin main
git push origin main

echo ""
echo "✅ Deployed! Render will auto-deploy in ~30 seconds."
echo "🔗 https://powerplugs-dashboard.onrender.com"
