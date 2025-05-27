"""
Enhanced async GitHub API client with parallel processing, caching, and NLP features
"""

import asyncio
import aiohttp
import time
import logging
from typing import List, Dict, Optional, Any, Tuple, Set
from dataclasses import dataclass
from datetime import datetime, timedelta
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from asyncio_throttle import Throttler
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import json
import hashlib
from functools import lru_cache

# Advanced dependencies for caching and NLP
try:
    from cachetools import TTLCache, LRUCache
    import redis
    CACHING_AVAILABLE = True
except ImportError:
    CACHING_AVAILABLE = False

try:
    import nltk
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import PorterStemmer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    NLP_AVAILABLE = True
except ImportError:
    NLP_AVAILABLE = False

# Configure logging
def setup_logging(log_level: str = "INFO", log_to_file: bool = True, 
                 log_file_path: str = "./extracted_data/extraction.log",
                 log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"):
    """Setup enhanced logging configuration"""
    
    # Create logs directory if needed
    if log_to_file:
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # Console handler
            logging.FileHandler(log_file_path) if log_to_file else logging.NullHandler()
        ]
    )
    
    # Reduce noise from external libraries
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

@dataclass
class Repository:
    """Enhanced repository data structure"""
    name: str
    full_name: str
    url: str
    languages: List[str]
    stars: int
    forks: int
    description: str
    size_kb: int
    is_fork: bool
    is_archived: bool
    created_at: str
    updated_at: str

@dataclass
class Commit:
    """Enhanced commit data structure"""
    sha: str
    message: str
    author: str
    date: str
    additions: int
    deletions: int
    changed_files: int

@dataclass
class Comment:
    """Enhanced comment data structure"""
    id: str
    body: str
    author: str
    date: str
    comment_type: str  # 'issue' or 'review'
    position: Optional[int] = None  # For review comments

@dataclass
class ChangeTypeResult:
    """Result of change type detection"""
    change_type: str
    confidence: float
    reasons: List[str]

class EnhancedGitHubAPIClient:
    """Enhanced GitHub API client with async support, caching, and NLP features"""
    
    def __init__(self, token: str, base_url: str = "https://api.github.com",
                 max_concurrent_requests: int = 10, enable_request_logging: bool = False,
                 cache_type: str = "memory", cache_ttl: int = 3600, 
                 enable_nlp: bool = True):
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.base_url = base_url
        self.max_concurrent_requests = max_concurrent_requests
        self.logger = logging.getLogger(__name__)
        self.enable_request_logging = enable_request_logging
        
        if not self.token:
            raise ValueError("GitHub token is required. Set GITHUB_TOKEN environment variable or pass token parameter.")
        
        # Setup throttler for rate limiting
        self.throttler = Throttler(rate_limit=5000, period=3600)  # 5000 requests per hour
        
        # Initialize advanced caching
        self.cache = AdvancedCache(cache_type=cache_type, ttl=cache_ttl)
        
        # Initialize NLP detector
        self.nlp_detector = NLPChangeTypeDetector() if enable_nlp else None
        
        # Connection pool for better performance
        self.connector_limit = max_concurrent_requests * 2
        
        # Performance tracking
        self.performance_stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "api_calls": 0,
            "batch_operations": 0
        }
        
        # Change type detection patterns
        self._setup_change_type_patterns()
    
    def _setup_change_type_patterns(self):
        """Setup patterns for enhanced change type detection"""
        self.change_type_patterns = {
            'fix': {
                'keywords': [
                    'fix', 'bug', 'patch', 'hotfix', 'resolve', 'close', 'closes', 'fixes',
                    'repair', 'correct', 'issue', 'problem', 'error', 'crash', 'failure',
                    'broken', 'regression', 'security', 'vulnerability', 'memory leak'
                ],
                'labels': [
                    'bug', 'bugfix', 'fix', 'hotfix', 'patch', 'security', 'critical',
                    'regression', 'memory-leak', 'crash', 'error'
                ],
                'commit_patterns': [
                    r'\bfix\b', r'\bbug\b', r'\bpatch\b', r'\bhotfix\b', r'\bresolve[sd]?\b',
                    r'\bclose[sd]?\b', r'\bissue\s*#?\d+', r'\bfixes?\s*#?\d+'
                ]
            },
            'feature': {
                'keywords': [
                    'add', 'feature', 'implement', 'new', 'create', 'introduce', 'support',
                    'enable', 'enhance', 'improvement', 'upgrade', 'extend', 'expand',
                    'capability', 'functionality', 'option', 'api', 'endpoint'
                ],
                'labels': [
                    'feature', 'enhancement', 'new-feature', 'improvement', 'api',
                    'new', 'addition', 'capability', 'functionality'
                ],
                'commit_patterns': [
                    r'\badd\b', r'\bfeature\b', r'\bimplement\b', r'\bnew\b', r'\bcreate\b',
                    r'\bintroduce\b', r'\benhance\b', r'\bimprove\b'
                ]
            },
            'doc': {
                'keywords': [
                    'doc', 'documentation', 'readme', 'comment', 'comments', 'guide',
                    'tutorial', 'example', 'sample', 'markdown', 'wiki', 'manual',
                    'specification', 'spec', 'changelog', 'license'
                ],
                'labels': [
                    'documentation', 'docs', 'readme', 'wiki', 'guide', 'tutorial',
                    'example', 'changelog', 'license'
                ],
                'commit_patterns': [
                    r'\bdoc\b', r'\bdocumentation\b', r'\breadme\b', r'\bcomment\b',
                    r'\bguide\b', r'\btutorial\b', r'\bexample\b'
                ]
            }
        }
    
    async def _make_async_request(self, session: aiohttp.ClientSession, 
                                url: str, params: dict = None) -> aiohttp.ClientResponse:
        """Make an async request with rate limiting and retries"""
        
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
        )
        async def _request():
            async with self.throttler:
                if self.enable_request_logging:
                    self.logger.debug(f"Making request to: {url}")
                
                headers = {
                    "Authorization": f"token {self.token}",
                    "Accept": "application/vnd.github.v3+json",
                    "User-Agent": "Enhanced-PR-Data-Extractor"
                }
                
                async with session.get(url, params=params, headers=headers, 
                                     timeout=aiohttp.ClientTimeout(total=30)) as response:
                    
                    # Handle rate limiting
                    if response.status == 403:
                        rate_limit_remaining = response.headers.get('X-RateLimit-Remaining', '0')
                        if rate_limit_remaining == '0':
                            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                            wait_time = max(reset_time - int(time.time()), 60)
                            self.logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                            return await _request()
                    
                    response.raise_for_status()
                    return response
        
        return await _request()
    
    async def get_organization_repositories_async(self, org: str, top_n: int = 10,
                                                min_stars: int = 100, sort_by: str = "stars",
                                                order: str = "desc", max_pages: int = 50,
                                                exclude_forks: bool = True,
                                                exclude_archived: bool = True,
                                                min_size_kb: int = 10,
                                                max_size_kb: int = 1000000) -> List[Repository]:
        """Get repositories with full pagination and enhanced filtering"""
        
        self.logger.info(f"Fetching repositories for organization: {org} (async)")
        
        async with aiohttp.ClientSession() as session:
            repositories = []
            page = 1
            
            while len(repositories) < top_n and page <= max_pages:
                url = f"{self.base_url}/orgs/{org}/repos"
                params = {
                    "type": "public",
                    "sort": sort_by,
                    "direction": order,
                    "per_page": 100,
                    "page": page
                }
                
                try:
                    response = await self._make_async_request(session, url, params)
                    repos_data = await response.json()
                    
                    if not repos_data:
                        break
                    
                    # Process repositories in parallel
                    tasks = []
                    for repo in repos_data:
                        if len(repositories) >= top_n:
                            break
                        
                        # Apply basic filters
                        if repo["stargazers_count"] < min_stars:
                            continue
                        if exclude_forks and repo["fork"]:
                            continue
                        if exclude_archived and repo["archived"]:
                            continue
                        if repo["size"] < min_size_kb or repo["size"] > max_size_kb:
                            continue
                        
                        tasks.append(self._process_repository_async(session, repo))
                    
                    # Wait for all repository processing to complete
                    if tasks:
                        processed_repos = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        for repo in processed_repos:
                            if isinstance(repo, Repository):
                                repositories.append(repo)
                                if len(repositories) >= top_n:
                                    break
                    
                    page += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to fetch repositories page {page}: {e}")
                    break
        
        self.logger.info(f"Found {len(repositories)} repositories")
        return repositories[:top_n]
    
    async def _process_repository_async(self, session: aiohttp.ClientSession, 
                                      repo_data: dict) -> Repository:
        """Process a single repository with language detection"""
        try:
            # Get repository languages
            languages = await self._get_repository_languages_async(session, repo_data["languages_url"])
            
            return Repository(
                name=repo_data["name"],
                full_name=repo_data["full_name"],
                url=repo_data["html_url"],
                languages=languages,
                stars=repo_data["stargazers_count"],
                forks=repo_data["forks_count"],
                description=repo_data["description"] or "",
                size_kb=repo_data["size"],
                is_fork=repo_data["fork"],
                is_archived=repo_data["archived"],
                created_at=repo_data["created_at"],
                updated_at=repo_data["updated_at"]
            )
        except Exception as e:
            self.logger.warning(f"Failed to process repository {repo_data.get('name', 'unknown')}: {e}")
            raise
    
    async def _get_repository_languages_async(self, session: aiohttp.ClientSession, 
                                           languages_url: str, repo_full_name: str = None) -> List[str]:
        """Get programming languages used in a repository with caching (async)"""
        
        # Try cache first
        cache_key = self.cache.cache_key_for_repo_languages(repo_full_name or languages_url)
        cached_languages = self.cache.get(cache_key)
        
        if cached_languages is not None:
            self.performance_stats["cache_hits"] += 1
            self.logger.debug(f"Cache hit for languages: {repo_full_name}")
            return cached_languages
        
        self.performance_stats["cache_misses"] += 1
        
        try:
            response = await self._make_async_request(session, languages_url)
            languages_data = await response.json()
            languages = list(languages_data.keys())
            
            # Cache the result
            self.cache.set(cache_key, languages)
            
            return languages
        except Exception as e:
            self.logger.warning(f"Failed to get languages: {e}")
            return []
    
    async def get_pull_requests_async(self, session: aiohttp.ClientSession,
                                    repo_full_name: str, max_prs: int = 50,
                                    state: str = "closed", sort_by: str = "updated",
                                    order: str = "desc", max_pages: int = 50) -> List[Dict]:
        """Get pull requests with full pagination (async)"""
        
        self.logger.info(f"Fetching pull requests for {repo_full_name} (async)")
        
        pull_requests = []
        page = 1
        
        while len(pull_requests) < max_prs and page <= max_pages:
            url = f"{self.base_url}/repos/{repo_full_name}/pulls"
            params = {
                "state": state,
                "sort": sort_by,
                "direction": order,
                "per_page": min(100, max_prs - len(pull_requests)),
                "page": page
            }
            
            try:
                response = await self._make_async_request(session, url, params)
                prs_data = await response.json()
                
                if not prs_data:
                    break
                
                pull_requests.extend(prs_data)
                
                if len(prs_data) < params["per_page"]:
                    break
                
                page += 1
                
            except Exception as e:
                self.logger.warning(f"Failed to fetch PRs page {page} for {repo_full_name}: {e}")
                break
        
        return pull_requests[:max_prs]
    
    async def get_pull_request_commits_async(self, session: aiohttp.ClientSession,
                                           repo_full_name: str, pr_number: int,
                                           max_commits: int = 10) -> List[Commit]:
        """Get commits for a pull request with enhanced data (async)"""
        url = f"{self.base_url}/repos/{repo_full_name}/pulls/{pr_number}/commits"
        
        try:
            response = await self._make_async_request(session, url)
            commits_data = await response.json()
            
            commits = []
            for commit_data in commits_data[:max_commits]:
                commit = Commit(
                    sha=commit_data["sha"],
                    message=commit_data["commit"]["message"],
                    author=commit_data["commit"]["author"]["name"],
                    date=commit_data["commit"]["author"]["date"],
                    additions=commit_data["stats"].get("additions", 0),
                    deletions=commit_data["stats"].get("deletions", 0),
                    changed_files=len(commit_data.get("files", []))
                )
                commits.append(commit)
            
            return commits
            
        except Exception as e:
            self.logger.warning(f"Failed to get commits for PR {pr_number}: {e}")
            return []
    
    async def get_pull_request_comments_async(self, session: aiohttp.ClientSession,
                                            repo_full_name: str, pr_number: int,
                                            max_comments: int = 20,
                                            max_pages: int = 10) -> List[Comment]:
        """Get comments for a pull request with full pagination (async)"""
        comments = []
        
        # Get issue comments (general PR comments)
        try:
            comments.extend(await self._get_issue_comments_async(
                session, repo_full_name, pr_number, max_comments, max_pages
            ))
        except Exception as e:
            self.logger.warning(f"Failed to get issue comments for PR {pr_number}: {e}")
        
        # Get review comments (code-specific comments)
        try:
            remaining_comments = max_comments - len(comments)
            if remaining_comments > 0:
                comments.extend(await self._get_review_comments_async(
                    session, repo_full_name, pr_number, remaining_comments, max_pages
                ))
        except Exception as e:
            self.logger.warning(f"Failed to get review comments for PR {pr_number}: {e}")
        
        return comments[:max_comments]
    
    async def _get_issue_comments_async(self, session: aiohttp.ClientSession,
                                      repo_full_name: str, pr_number: int,
                                      max_comments: int, max_pages: int) -> List[Comment]:
        """Get issue comments with pagination"""
        comments = []
        page = 1
        
        while len(comments) < max_comments and page <= max_pages:
            url = f"{self.base_url}/repos/{repo_full_name}/issues/{pr_number}/comments"
            params = {"per_page": min(100, max_comments - len(comments)), "page": page}
            
            response = await self._make_async_request(session, url, params)
            comments_data = await response.json()
            
            if not comments_data:
                break
            
            for comment_data in comments_data:
                comment = Comment(
                    id=str(comment_data["id"]),
                    body=comment_data["body"],
                    author=comment_data["user"]["login"],
                    date=comment_data["created_at"],
                    comment_type="issue"
                )
                comments.append(comment)
            
            if len(comments_data) < params["per_page"]:
                break
            
            page += 1
        
        return comments
    
    async def _get_review_comments_async(self, session: aiohttp.ClientSession,
                                       repo_full_name: str, pr_number: int,
                                       max_comments: int, max_pages: int) -> List[Comment]:
        """Get review comments with pagination"""
        comments = []
        page = 1
        
        while len(comments) < max_comments and page <= max_pages:
            url = f"{self.base_url}/repos/{repo_full_name}/pulls/{pr_number}/comments"
            params = {"per_page": min(100, max_comments - len(comments)), "page": page}
            
            response = await self._make_async_request(session, url, params)
            comments_data = await response.json()
            
            if not comments_data:
                break
            
            for comment_data in comments_data:
                comment = Comment(
                    id=str(comment_data["id"]),
                    body=comment_data["body"],
                    author=comment_data["user"]["login"],
                    date=comment_data["created_at"],
                    comment_type="review",
                    position=comment_data.get("position")
                )
                comments.append(comment)
            
            if len(comments_data) < params["per_page"]:
                break
            
            page += 1
        
        return comments
    
    def determine_change_type_enhanced(self, title: str, body: str, labels: List[str],
                                     commits: List[Commit] = None,
                                     confidence_threshold: float = 0.6, 
                                     use_nlp: bool = True) -> ChangeTypeResult:
        """Enhanced change type detection with NLP and confidence scoring"""
        
        scores = {'fix': 0.0, 'feature': 0.0, 'doc': 0.0}
        reasons = []
        
        # Normalize inputs
        title_lower = title.lower()
        body_lower = (body or "").lower()
        label_names = [label.get("name", "").lower() for label in labels if isinstance(label, dict)]
        if isinstance(labels, list) and labels and isinstance(labels[0], str):
            label_names = [label.lower() for label in labels]
        
        # Score based on labels (highest weight)
        for change_type, patterns in self.change_type_patterns.items():
            for label in label_names:
                for pattern_label in patterns['labels']:
                    if pattern_label in label:
                        scores[change_type] += 0.4
                        reasons.append(f"Label '{label}' matches {change_type}")
        
        # Score based on title and body keywords
        combined_text = f"{title_lower} {body_lower}"
        for change_type, patterns in self.change_type_patterns.items():
            for keyword in patterns['keywords']:
                if keyword in combined_text:
                    weight = 0.3 if keyword in title_lower else 0.1
                    scores[change_type] += weight
                    reasons.append(f"Keyword '{keyword}' found in {'title' if keyword in title_lower else 'body'}")
        
        # Score based on commit messages (if provided)
        commit_messages = []
        if commits:
            for commit in commits:
                commit_msg_lower = commit.message.lower()
                commit_messages.append(commit.message)
                for change_type, patterns in self.change_type_patterns.items():
                    for pattern in patterns['commit_patterns']:
                        if re.search(pattern, commit_msg_lower, re.IGNORECASE):
                            scores[change_type] += 0.2
                            reasons.append(f"Commit pattern '{pattern}' matches {change_type}")
        
        # ADVANCED NLP ANALYSIS
        if use_nlp and self.nlp_detector and self.nlp_detector.is_initialized:
            try:
                # Extract semantic features
                nlp_features = self.nlp_detector.extract_semantic_features(
                    title, body, commit_messages
                )
                
                # Add NLP similarity scores
                for feature_name, similarity in nlp_features.items():
                    if "fix" in feature_name:
                        scores["fix"] += similarity * 0.3
                        reasons.append(f"NLP semantic similarity to fix patterns: {similarity:.2f}")
                    elif "feature" in feature_name:
                        scores["feature"] += similarity * 0.3
                        reasons.append(f"NLP semantic similarity to feature patterns: {similarity:.2f}")
                    elif "doc" in feature_name:
                        scores["doc"] += similarity * 0.3
                        reasons.append(f"NLP semantic similarity to doc patterns: {similarity:.2f}")
                
                # Analyze commit sentiment and patterns
                if commit_messages:
                    commit_analysis = self.nlp_detector.analyze_commit_sentiment(commit_messages)
                    
                    if commit_analysis.get("urgent_commits", 0) > 0:
                        scores["fix"] += 0.2
                        reasons.append(f"Urgent commit indicators detected")
                    
                    if commit_analysis.get("test_commits", 0) > len(commit_messages) * 0.5:
                        scores["feature"] += 0.1
                        reasons.append(f"High test commit ratio indicates feature development")
                    
                    if commit_analysis.get("refactor_commits", 0) > 0:
                        scores["feature"] += 0.15
                        reasons.append(f"Refactoring commits detected")
                
            except Exception as e:
                self.logger.warning(f"NLP analysis failed: {e}")
                reasons.append("NLP analysis failed, using pattern-based detection")
        
        # ADVANCED METADATA ANALYSIS
        # Analyze text length and complexity
        title_length = len(title.split())
        body_length = len((body or "").split())
        
        if title_length <= 5 and any(word in title_lower for word in ["fix", "patch", "hotfix"]):
            scores["fix"] += 0.1
            reasons.append("Short fix-type title detected")
        
        if body_length > 50:  # Long descriptions often indicate features
            scores["feature"] += 0.05
            reasons.append("Detailed description suggests feature implementation")
        
        if "readme" in combined_text or "doc" in combined_text:
            scores["doc"] += 0.2
            reasons.append("Documentation keywords detected")
        
        # Determine final result with enhanced logic
        max_score = max(scores.values())
        if max_score >= confidence_threshold:
            change_type = max(scores, key=scores.get)
        else:
            change_type = "feature"  # Default
            reasons.append("Confidence below threshold, defaulting to feature")
        
        return ChangeTypeResult(
            change_type=change_type,
            confidence=max_score,
            reasons=reasons
        )
    
    def extract_tags_enhanced(self, pr_data: Dict) -> List[str]:
        """Enhanced tag extraction from PR labels and metadata"""
        tags = []
        labels = pr_data.get("labels", [])
        
        # Extract from labels
        for label in labels:
            tag_name = label.get("name", "") if isinstance(label, dict) else str(label)
            if tag_name:
                tags.append(tag_name)
        
        # Add inferred tags based on PR metadata
        if pr_data.get("draft"):
            tags.append("draft")
        
        if pr_data.get("mergeable_state") == "dirty":
            tags.append("merge-conflict")
        
        # Add size-based tags
        additions = pr_data.get("additions", 0)
        deletions = pr_data.get("deletions", 0)
        total_changes = additions + deletions
        
        if total_changes > 1000:
            tags.append("large-change")
        elif total_changes < 10:
            tags.append("small-change")
        
        return list(set(tags))  # Remove duplicates

# Parallel processing utilities
class ParallelProcessor:
    """Utility class for parallel processing of repositories"""
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
    
    def process_repositories_parallel(self, repositories: List[Repository],
                                    process_func, **kwargs) -> List[Any]:
        """Process repositories in parallel using ThreadPoolExecutor"""
        
        self.logger.info(f"Processing {len(repositories)} repositories with {self.max_workers} workers")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_repo = {
                executor.submit(process_func, repo, **kwargs): repo 
                for repo in repositories
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_repo):
                repo = future_to_repo[future]
                try:
                    result = future.result()
                    if result:
                        results.extend(result if isinstance(result, list) else [result])
                except Exception as e:
                    self.logger.error(f"Failed to process repository {repo.name}: {e}")
        
        return results

# Advanced caching classes
class AdvancedCache:
    """Advanced caching system with multiple backends"""
    
    def __init__(self, cache_type: str = "memory", ttl: int = 3600, max_size: int = 1000):
        self.cache_type = cache_type
        self.ttl = ttl
        self.max_size = max_size
        self.logger = logging.getLogger(__name__)
        
        # Initialize cache backend
        if CACHING_AVAILABLE and cache_type == "memory":
            self.cache = TTLCache(maxsize=max_size, ttl=ttl)
        elif CACHING_AVAILABLE and cache_type == "redis":
            try:
                self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
                self.redis_client.ping()
                self.cache = None  # Use Redis directly
                self.logger.info("Redis cache initialized")
            except Exception as e:
                self.logger.warning(f"Redis not available, falling back to memory cache: {e}")
                self.cache = TTLCache(maxsize=max_size, ttl=ttl)
        else:
            # Fallback to simple dict cache
            self.cache = {}
            self.logger.warning("Advanced caching not available, using simple dict cache")
    
    def _make_key(self, *args) -> str:
        """Create a cache key from arguments"""
        key_str = "|".join(str(arg) for arg in args)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            if hasattr(self, 'redis_client') and self.cache is None:
                # Redis backend
                data = self.redis_client.get(key)
                return json.loads(data) if data else None
            else:
                # Memory backend
                return self.cache.get(key)
        except Exception as e:
            self.logger.warning(f"Cache get error: {e}")
            return None
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache"""
        try:
            if hasattr(self, 'redis_client') and self.cache is None:
                # Redis backend
                self.redis_client.setex(key, self.ttl, json.dumps(value))
            else:
                # Memory backend
                self.cache[key] = value
        except Exception as e:
            self.logger.warning(f"Cache set error: {e}")
    
    def cache_key_for_repo_languages(self, repo_full_name: str) -> str:
        """Generate cache key for repository languages"""
        return self._make_key("repo_languages", repo_full_name)
    
    def cache_key_for_pr_data(self, repo_full_name: str, pr_number: int) -> str:
        """Generate cache key for PR data"""
        return self._make_key("pr_data", repo_full_name, pr_number)

class NLPChangeTypeDetector:
    """Advanced NLP-based change type detection"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.is_initialized = False
        
        if NLP_AVAILABLE:
            try:
                # Download required NLTK data
                nltk.download('punkt', quiet=True)
                nltk.download('stopwords', quiet=True)
                nltk.download('wordnet', quiet=True)
                
                self.stemmer = PorterStemmer()
                self.stop_words = set(stopwords.words('english'))
                self.vectorizer = TfidfVectorizer(
                    max_features=1000,
                    stop_words='english',
                    ngram_range=(1, 2)
                )
                
                # Train vectorizer with common patterns
                self._initialize_patterns()
                self.is_initialized = True
                self.logger.info("NLP change type detector initialized")
                
            except Exception as e:
                self.logger.warning(f"NLP initialization failed: {e}")
                self.is_initialized = False
        else:
            self.logger.warning("NLP libraries not available")
    
    def _initialize_patterns(self):
        """Initialize NLP patterns for change type detection"""
        training_texts = [
            # Fix patterns
            "fix bug in authentication module",
            "resolve memory leak issue",
            "patch security vulnerability",
            "correct typo in documentation",
            "hotfix critical error",
            
            # Feature patterns  
            "add new user management feature",
            "implement oauth authentication",
            "create api endpoint for data",
            "introduce new configuration option",
            "enable advanced logging",
            
            # Documentation patterns
            "update readme with examples",
            "add installation guide",
            "improve api documentation",
            "create user manual",
            "write tutorial for beginners"
        ]
        
        try:
            self.vectorizer.fit(training_texts)
        except Exception as e:
            self.logger.warning(f"Pattern initialization failed: {e}")
    
    def preprocess_text(self, text: str) -> str:
        """Preprocess text for NLP analysis"""
        if not text:
            return ""
        
        try:
            # Tokenize and clean
            tokens = word_tokenize(text.lower())
            # Remove stopwords and stem
            tokens = [self.stemmer.stem(token) for token in tokens 
                     if token.isalnum() and token not in self.stop_words]
            return " ".join(tokens)
        except Exception:
            # Fallback to simple preprocessing
            return text.lower()
    
    def extract_semantic_features(self, title: str, body: str, 
                                commits: List[str] = None) -> Dict[str, float]:
        """Extract semantic features from text using NLP"""
        if not self.is_initialized:
            return {}
        
        try:
            # Combine all text
            combined_text = f"{title} {body or ''}"
            if commits:
                combined_text += " " + " ".join(commits)
            
            # Preprocess
            processed_text = self.preprocess_text(combined_text)
            
            # Extract TF-IDF features
            features = self.vectorizer.transform([processed_text])
            
            # Calculate semantic similarity to known patterns
            fix_patterns = ["fix bug error patch resolve correct"]
            feature_patterns = ["add new implement create introduce enable"]
            doc_patterns = ["update readme documentation guide manual tutorial"]
            
            similarities = {}
            for pattern_type, patterns in [
                ("fix", fix_patterns),
                ("feature", feature_patterns), 
                ("doc", doc_patterns)
            ]:
                pattern_features = self.vectorizer.transform(patterns)
                similarity = cosine_similarity(features, pattern_features).max()
                similarities[f"nlp_{pattern_type}_similarity"] = float(similarity)
            
            return similarities
            
        except Exception as e:
            self.logger.warning(f"NLP feature extraction failed: {e}")
            return {}
    
    def analyze_commit_sentiment(self, commit_messages: List[str]) -> Dict[str, Any]:
        """Analyze commit message patterns and sentiment"""
        if not commit_messages or not self.is_initialized:
            return {}
        
        try:
            # Analyze commit patterns
            urgent_keywords = ["hotfix", "critical", "urgent", "emergency", "asap"]
            refactor_keywords = ["refactor", "restructure", "reorganize", "cleanup"]
            test_keywords = ["test", "testing", "spec", "unittest", "integration"]
            
            analysis = {
                "urgent_commits": sum(1 for msg in commit_messages 
                                    if any(kw in msg.lower() for kw in urgent_keywords)),
                "refactor_commits": sum(1 for msg in commit_messages 
                                      if any(kw in msg.lower() for kw in refactor_keywords)),
                "test_commits": sum(1 for msg in commit_messages 
                                  if any(kw in msg.lower() for kw in test_keywords)),
                "total_commits": len(commit_messages),
                "avg_commit_length": sum(len(msg) for msg in commit_messages) / len(commit_messages)
            }
            
            return analysis
            
        except Exception as e:
            self.logger.warning(f"Commit sentiment analysis failed: {e}")
            return {}
    
    # OPTIMIZED BULK OPERATIONS USING asyncio.gather
    
    async def get_multiple_repositories_optimized(self, session: aiohttp.ClientSession,
                                                 repo_urls: List[str]) -> List[Repository]:
        """Get multiple repositories in parallel using asyncio.gather"""
        self.logger.info(f"Fetching {len(repo_urls)} repositories in parallel")
        
        # Create tasks for all repositories
        tasks = [self._fetch_single_repository(session, url) for url in repo_urls]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        repositories = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.warning(f"Failed to fetch repository {repo_urls[i]}: {result}")
            elif result:
                repositories.append(result)
        
        self.performance_stats["batch_operations"] += 1
        return repositories
    
    async def _fetch_single_repository(self, session: aiohttp.ClientSession, 
                                     repo_url: str) -> Optional[Repository]:
        """Fetch a single repository with error handling"""
        try:
            response = await self._make_async_request(session, repo_url)
            repo_data = await response.json()
            
            # Get languages in parallel if needed
            languages = await self._get_repository_languages_async(
                session, repo_data["languages_url"], repo_data["full_name"]
            )
            
            return Repository(
                name=repo_data["name"],
                full_name=repo_data["full_name"],
                url=repo_data["html_url"],
                languages=languages,
                stars=repo_data["stargazers_count"],
                forks=repo_data["forks_count"],
                description=repo_data["description"] or "",
                size_kb=repo_data["size"],
                is_fork=repo_data["fork"],
                is_archived=repo_data["archived"],
                created_at=repo_data["created_at"],
                updated_at=repo_data["updated_at"]
            )
        except Exception as e:
            self.logger.warning(f"Failed to fetch repository {repo_url}: {e}")
            return None
    
    async def get_bulk_pr_data_optimized(self, session: aiohttp.ClientSession,
                                       repo_full_name: str, pr_numbers: List[int],
                                       max_concurrent: int = None) -> List[Dict]:
        """Get multiple PR data in parallel using asyncio.gather with concurrency control"""
        
        if not pr_numbers:
            return []
        
        max_concurrent = max_concurrent or min(len(pr_numbers), self.max_concurrent_requests)
        self.logger.info(f"Fetching {len(pr_numbers)} PRs with {max_concurrent} concurrent requests")
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_pr_with_semaphore(pr_number):
            async with semaphore:
                return await self._fetch_single_pr_comprehensive(session, repo_full_name, pr_number)
        
        # Create all tasks
        tasks = [fetch_pr_with_semaphore(pr_num) for pr_num in pr_numbers]
        
        # Execute with progress tracking
        results = []
        batch_size = max_concurrent
        
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for j, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    pr_number = pr_numbers[i + j]
                    self.logger.warning(f"Failed to fetch PR {pr_number}: {result}")
                elif result:
                    results.append(result)
            
            # Small delay between batches to be respectful to API
            if i + batch_size < len(tasks):
                await asyncio.sleep(0.1)
        
        self.performance_stats["batch_operations"] += 1
        return results
    
    async def _fetch_single_pr_comprehensive(self, session: aiohttp.ClientSession,
                                           repo_full_name: str, pr_number: int) -> Optional[Dict]:
        """Fetch comprehensive PR data including commits and comments"""
        
        # Check cache first
        cache_key = self.cache.cache_key_for_pr_data(repo_full_name, pr_number)
        cached_data = self.cache.get(cache_key)
        
        if cached_data is not None:
            self.performance_stats["cache_hits"] += 1
            return cached_data
        
        self.performance_stats["cache_misses"] += 1
        
        try:
            # Create all tasks for parallel execution
            pr_url = f"{self.base_url}/repos/{repo_full_name}/pulls/{pr_number}"
            commits_url = f"{self.base_url}/repos/{repo_full_name}/pulls/{pr_number}/commits"
            comments_url = f"{self.base_url}/repos/{repo_full_name}/issues/{pr_number}/comments"
            review_comments_url = f"{self.base_url}/repos/{repo_full_name}/pulls/{pr_number}/comments"
            
            # Execute all requests in parallel using asyncio.gather
            pr_task = self._make_async_request(session, pr_url)
            commits_task = self._make_async_request(session, commits_url)
            comments_task = self._make_async_request(session, comments_url)
            review_comments_task = self._make_async_request(session, review_comments_url)
            
            # Wait for all requests to complete
            pr_response, commits_response, comments_response, review_comments_response = await asyncio.gather(
                pr_task, commits_task, comments_task, review_comments_task,
                return_exceptions=True
            )
            
            # Process responses
            pr_data = await pr_response.json() if not isinstance(pr_response, Exception) else {}
            commits_data = await commits_response.json() if not isinstance(commits_response, Exception) else []
            comments_data = await comments_response.json() if not isinstance(comments_response, Exception) else []
            review_comments_data = await review_comments_response.json() if not isinstance(review_comments_response, Exception) else []
            
            # Combine all data
            comprehensive_data = {
                "pr_data": pr_data,
                "commits": commits_data[:10],  # Limit commits
                "comments": comments_data[:20],  # Limit comments
                "review_comments": review_comments_data[:20]  # Limit review comments
            }
            
            # Cache the result
            self.cache.set(cache_key, comprehensive_data)
            
            return comprehensive_data
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch comprehensive PR data for {pr_number}: {e}")
            return None
    
    async def create_optimized_session(self) -> aiohttp.ClientSession:
        """Create an optimized aiohttp session with connection pooling"""
        connector = aiohttp.TCPConnector(
            limit=self.connector_limit,
            limit_per_host=min(30, self.connector_limit),
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        timeout = aiohttp.ClientTimeout(total=60, connect=10)
        
        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "Authorization": f"token {self.token}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "Enhanced-PR-Data-Extractor"
            }
        )
    
    async def close_session(self, session: aiohttp.ClientSession):
        """Close aiohttp session properly"""
        if session and not session.closed:
            await session.close()
    
    async def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        cache_size = len(self.cache.cache) if hasattr(self.cache, 'cache') else 0
        
        return {
            **self.performance_stats,
            "cache_size": cache_size,
            "cache_hit_ratio": (
                self.performance_stats["cache_hits"] / 
                (self.performance_stats["cache_hits"] + self.performance_stats["cache_misses"])
                if (self.performance_stats["cache_hits"] + self.performance_stats["cache_misses"]) > 0 
                else 0
            ),
            "total_api_calls": self.performance_stats["api_calls"],
            "nlp_enabled": self.nlp_detector.is_initialized if self.nlp_detector else False
        }
    
    def reset_performance_stats(self):
        """Reset performance tracking counters"""
        self.performance_stats = {
            "api_calls": 0,
            "cache_hits": 0, 
            "cache_misses": 0,
            "batch_operations": 0,
            "parallel_requests": 0
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform a health check of the GitHub API connection"""
        try:
            async with await self.create_optimized_session() as session:
                response = await self._make_async_request(session, f"{self.base_url}/rate_limit")
                rate_limit_data = await response.json()
                
                return {
                    "status": "healthy",
                    "rate_limit": rate_limit_data,
                    "cache_enabled": self.cache is not None,
                    "nlp_enabled": self.nlp_detector.is_initialized if self.nlp_detector else False
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
