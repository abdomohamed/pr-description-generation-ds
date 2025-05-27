"""
Configuration file for GitHub PR data extraction
"""

# GitHub API Configuration
GITHUB_TOKEN = "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"  # Set your GitHub token here or use environment variable
GITHUB_API_BASE_URL = "https://api.github.com"
ORGANIZATION = "microsoft"  # Target organization

# Repository Selection Configuration
TOP_N_REPOSITORIES = 1  # Number of top repositories to select
MIN_STARS = 100  # Minimum stars for repository selection
REPOSITORY_SORT_BY = "stars"  # Options: stars, forks, updated, created
REPOSITORY_ORDER = "desc"  # Options: asc, desc

# Pull Request Configuration
MAX_PRS_PER_REPO = 10  # Maximum PRs to extract per repository
PR_STATE = "closed"  # Options: open, closed, all
PR_SORT_BY = "updated"  # Options: created, updated, popularity
PR_ORDER = "desc"  # Options: asc, desc

# Data Extraction Configuration
INCLUDE_PR_COMMENTS = True
INCLUDE_COMMITS = True
MAX_COMMENTS_PER_PR = 20
MAX_COMMITS_PER_PR = 10

# Output Configuration
OUTPUT_FORMAT = "json"  # Options: json, csv
OUTPUT_DIRECTORY = "./extracted_data"
BATCH_SIZE = 100  # Number of PRs to process in each batch

# Rate Limiting Configuration
REQUESTS_PER_MINUTE = 5000  # GitHub API rate limit
DELAY_BETWEEN_REQUESTS = 0.1  # Seconds between requests

# Filtering Configuration
EXCLUDE_LANGUAGES = ["HTML", "CSS"]  # Languages to exclude
INCLUDE_ONLY_LANGUAGES = []  # If specified, only include these languages
MIN_CHANGED_FILES = 1  # Minimum number of changed files in PR

# Performance Configuration
USE_ASYNC_REQUESTS = True  # Use async HTTP requests for better performance
MAX_CONCURRENT_REQUESTS = 10  # Maximum concurrent requests
ENABLE_PARALLEL_PROCESSING = True  # Enable parallel processing of repositories
MAX_WORKER_THREADS = 4  # Maximum worker threads for parallel processing

# Enhanced Pagination Configuration
ENABLE_FULL_PAGINATION = True  # Enable full pagination for large datasets
MAX_PAGES_PER_ENDPOINT = 50  # Maximum pages to fetch per API endpoint
ITEMS_PER_PAGE = 100  # Items per page (GitHub max is 100)

# Logging Configuration
LOG_LEVEL = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_TO_FILE = True  # Log to file in addition to console
LOG_FILE_PATH = "./extracted_data/extraction.log"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
ENABLE_REQUEST_LOGGING = False  # Log all HTTP requests (verbose)

# Enhanced Change Type Detection
ENHANCED_CHANGE_TYPE_DETECTION = True  # Use improved change type detection
ANALYZE_COMMIT_MESSAGES = True  # Analyze commit messages for change type
CHANGE_TYPE_CONFIDENCE_THRESHOLD = 0.6  # Confidence threshold for change type detection

# Advanced Filtering
MIN_REPOSITORY_SIZE_KB = 10  # Minimum repository size in KB
MAX_REPOSITORY_SIZE_KB = 1000000  # Maximum repository size in KB (1GB)
EXCLUDE_FORKS = True  # Exclude forked repositories
EXCLUDE_ARCHIVED = True  # Exclude archived repositories

# Retry Configuration
MAX_RETRIES = 3  # Maximum retries for failed requests
RETRY_DELAY = 2  # Initial retry delay in seconds
EXPONENTIAL_BACKOFF = True  # Use exponential backoff for retries
