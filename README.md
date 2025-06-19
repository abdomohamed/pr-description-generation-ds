# GitHub PR Data Extraction

This script extracts pull request data from GitHub repositories, specifically designed to work with Microsoft's GitHub organization repositories.

## Features

- ✅ Configurable selection of top N popular repositories
- ✅ Comprehensive PR data extraction including commits, comments, and file diffs
- ✅ Automatic change type detection (fix/feature/doc)
- ✅ Rate limiting and error handling
- ✅ Multiple output formats (JSON, CSV)
- ✅ Filtering by programming languages
- ✅ Customizable extraction parameters
- ✅ Batch processing with incremental saving
- ✅ Component-level idempotency (tracks commits, comments, and file diffs separately)

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure GitHub token:**
   
   You need a GitHub personal access token to use the GitHub API. Create one at:
   https://github.com/settings/personal-access-tokens/new
   
   Required permissions: `public_repo`, `read:org`
   
   Set your token in one of these ways:
   
   **Option A: Environment variable (recommended)**
   ```bash
   export GITHUB_TOKEN="your_github_token_here"
   ```
   
   **Option B: Edit config.py**
   ```python
   GITHUB_TOKEN = "your_github_token_here"
   ```

3. **Customize configuration:**
   
   Edit `config.py` to adjust extraction parameters:
   ```python
   # Repository Selection
   TOP_N_REPOSITORIES = 10        # Number of top repositories
   MIN_STARS = 100               # Minimum stars required
   ORGANIZATION = "microsoft"     # Target organization
   
   # PR Configuration
   MAX_PRS_PER_REPO = 50         # Max PRs per repository
   PR_STATE = "closed"           # open, closed, or all
   
   # Data Extraction
   INCLUDE_PR_COMMENTS = True    # Include PR comments
   INCLUDE_COMMITS = True        # Include commit data
   INCLUDE_PR_FILE_DIFFS = True  # Include file diffs
   MAX_COMMENTS_PER_PR = 20      # Max comments per PR
   MAX_COMMITS_PER_PR = 10       # Max commits per PR
   MAX_FILES_PER_PR = 50         # Max files per PR
   MAX_PATCH_SIZE = 10000        # Max patch size in characters
   
   # Output Configuration
   OUTPUT_FORMAT = "json"        # json or csv
   OUTPUT_DIRECTORY = "./extracted_data"
   ```

## Usage

Run the extraction script:

```bash
python extract_pr_data.py
```

The script will:

1. **Fetch top repositories** from Microsoft organization based on stars
2. **Extract pull requests** from each repository
3. **Gather detailed data** including:
   - PR metadata (title, description, change type)
   - Commit information
   - PR comments
   - Repository languages
   - Tags from labels
   - File diffs
4. **Save results** in the specified format

## Output Format

The script generates data in the following structure:

```json
{
  "id": "microsoft/vscode_12345",
  "repository_url": "https://github.com/microsoft/vscode",
  "repository_language": ["TypeScript", "JavaScript"],
  "repository_name": "vscode",
  "title": "Fix editor scrolling issue",
  "description": "This PR fixes a scrolling issue in the editor...",
  "change_type": "fix",
  "commits": {
    "abc123": {
      "commit_message": "Fix scrolling behavior in editor"
    }
  },
  "pr_comments": [
    {
      "id": "789456",
      "body": "LGTM! This fixes the issue.",
      "author": "reviewer123",
      "date": "2023-10-01T12:00:00Z"
    }
  ],
  "file_diffs": [
    {
      "file_path": "src/editor.js",
      "diff": "@@ -10,6 +10,7 @@\n function updateEditor() {\n   // code to update editor\n+  console.log('Editor updated');\n }\n"
    }
  ],
  "tags": ["bug", "editor"]
}
```

## Configuration Options

### Repository Selection
- `TOP_N_REPOSITORIES`: Number of top repositories to select
- `MIN_STARS`: Minimum stars required for repository selection
- `REPOSITORY_SORT_BY`: Sort repositories by (stars, forks, updated, created)
- `ORGANIZATION`: Target GitHub organization

### Pull Request Filtering
- `MAX_PRS_PER_REPO`: Maximum PRs to extract per repository
- `PR_STATE`: State of PRs to extract (open, closed, all)
- `MIN_CHANGED_FILES`: Minimum number of changed files in PR

### Language Filtering
- `INCLUDE_ONLY_LANGUAGES`: Only include repositories with these languages
- `EXCLUDE_LANGUAGES`: Exclude repositories with these languages

### Data Extraction
- `INCLUDE_PR_COMMENTS`: Whether to include PR comments
- `INCLUDE_COMMITS`: Whether to include commit information
- `INCLUDE_PR_FILE_DIFFS`: Whether to include file diffs
- `MAX_COMMENTS_PER_PR`: Maximum comments to extract per PR
- `MAX_COMMITS_PER_PR`: Maximum commits to extract per PR
- `BATCH_SIZE`: Number of PRs to process in each batch (also used for incremental saving)
- `ENABLE_IDEMPOTENT_EXTRACTION`: Skip PRs that have already been processed in previous runs
- `COMPONENT_LEVEL_IDEMPOTENCY`: Track which components (commits, comments, files) have been processed

### Component-Level Idempotent Extraction

The script supports component-level idempotent operation, meaning it tracks which specific components (commits, comments, file diffs) have been downloaded for each PR. This provides several advantages:

- Efficient updates when adding new data types (like file diffs)
- Resuming interrupted extractions without re-downloading everything
- Memory efficiency through intelligent caching of PR data
- Detailed tracking statistics about component updates

To enable component-level idempotency, set both `ENABLE_IDEMPOTENT_EXTRACTION = True` and `COMPONENT_LEVEL_IDEMPOTENCY = True` in `config.py`. When enabled, the script will:

1. Scan the output directory for existing PR data files
2. Build a cache of PR components already processed
3. Only download missing components for each PR
4. Track statistics about which components were added or updated

You can test the component-level idempotent extraction feature with:
```bash
python test_idempotent.py --component-level
```

### Rate Limiting
- `REQUESTS_PER_MINUTE`: GitHub API rate limit
- `DELAY_BETWEEN_REQUESTS`: Delay between individual requests

## Change Type Detection

The script automatically categorizes PRs into three types:

- **fix**: Bug fixes, patches, hotfixes
- **feature**: New features, enhancements, additions
- **doc**: Documentation updates, README changes

Detection is based on:
1. PR labels (highest priority)
2. Keywords in PR title and description
3. Default to "feature" if uncertain

## Rate Limiting

The script includes built-in rate limiting to respect GitHub's API limits:
- Automatic detection of rate limit exceeded
- Intelligent waiting when limits are hit
- Configurable delays between requests

## Error Handling

- Comprehensive logging of all operations
- Graceful handling of API errors
- Continuation of extraction even if individual PRs fail
- Detailed error messages for troubleshooting

## Output Files

The script generates files in the `extracted_data` directory:

- `pr_data_YYYYMMDD_HHMMSS.json`: Main output file
- `pr_data_YYYYMMDD_HHMMSS.csv`: CSV format (if selected)
- `pr_data_detailed_YYYYMMDD_HHMMSS.json`: Detailed JSON (when CSV is primary format)

## Example Command Line Usage

```bash
# Basic usage with environment variable
export GITHUB_TOKEN="your_token_here"
python extract_pr_data.py

# Or modify config.py and run
python extract_pr_data.py
```

## Troubleshooting

1. **Authentication Error**: Ensure your GitHub token is valid and has required permissions
2. **Rate Limit**: The script handles rate limits automatically, but consider reducing `REQUESTS_PER_MINUTE`
3. **No Data**: Check if the organization exists and has public repositories
4. **Missing Dependencies**: Run `pip install -r requirements.txt`

## Performance Tips

- Reduce `MAX_PRS_PER_REPO` for faster extraction
- Use `EXCLUDE_LANGUAGES` to filter out unwanted repositories
- Set `INCLUDE_PR_COMMENTS=False` if comments aren't needed
- Increase `DELAY_BETWEEN_REQUESTS` if experiencing rate limits
- Enable `ENABLE_IDEMPOTENT_EXTRACTION` to skip already processed PRs when resuming interrupted extractions
- Use an appropriate `BATCH_SIZE` to balance memory usage and saving frequency
