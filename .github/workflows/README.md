# GitHub Actions Workflows

## Open Tenders Workflow

Automatically processes Spanish public procurement data and updates the target repository.

### How to Test

1. **Manual Test (Recommended)**:
   - Go to your repository → Actions tab
   - Click "Open Tenders Data Pipeline"
   - Click "Run workflow" → "Run workflow"

2. **Check Results**:
   - Monitor the workflow run in real-time
   - Download artifacts from the Actions tab
   - Verify data was pushed to `{target-repo}/data/open_tenders.json`

### Setup Required

**Repository Secrets** (Settings → Secrets and variables → Actions):

- `PAT`: GitHub Personal Access Token with write access to target repository
- `SPPD_OT_REPO`: Target repository name (e.g., `username/repo-name`)
- `TARGET_BRANCH`: Target branch (optional, defaults to `main`)

### Schedule

- **Manual**: Run anytime via Actions tab
- **Automatic**: Daily at 8:30 AM CET (7:30 AM UTC)

### What It Does

1. ✅ Verifies data source URLs are accessible
2. ✅ Runs the open tenders pipeline (`src/open_tenders/main.py`)
3. ✅ Validates JSON output file
4. ✅ Pushes data to target repository's `data/open_tenders.json`
5. ✅ Uploads artifacts for 7 days

### Troubleshooting

- **Permission errors**: Check PAT has `repo` scope for target repository
- **Repository not found**: Verify `SPPD_OT_REPO` secret format (username/repo-name)
- **URL failures**: Data sources may be temporarily down
- **File issues**: Check workflow logs for specific errors 