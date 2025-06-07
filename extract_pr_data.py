"""
Enhanced main script for extracting GitHub PR data with async processing
"""

import asyncio
import json
import os
import csv
import time
from datetime import datetime
from typing import List, Dict
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from github_client import (
    EnhancedGitHubAPIClient,  setup_logging, Repository
)
import config

class EnhancedPRDataExtractor:
    """Enhanced PR data extractor with async and parallel processing"""
    
    def __init__(self, github_token: str = None):
        # Setup logging first
        self.logger = setup_logging(
            log_level=config.LOG_LEVEL,
            log_to_file=config.LOG_TO_FILE,
            log_file_path=config.LOG_FILE_PATH,
            log_format=config.LOG_FORMAT
        )
        
        self.client = EnhancedGitHubAPIClient(
            token=github_token or config.GITHUB_TOKEN,
            enable_request_logging=config.ENABLE_REQUEST_LOGGING
        )
        
        self.output_dir = config.OUTPUT_DIRECTORY
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize processed PR IDs
        self.processed_pr_ids = {}
        if config.ENABLE_IDEMPOTENT_EXTRACTION:
            self.logger.info("Idempotent extraction enabled. Loading previously processed PR IDs...")
            self.processed_pr_ids = self._load_processed_pr_ids()
    
    async def extract_data_async(self) -> List[Dict]:
        """Main async method to extract PR data"""
        self.logger.info("Starting enhanced PR data extraction process")
        start_time = time.time()
        
        try:
            # Step 1: Get top repositories (async)
            repositories = await self.client.get_organization_repositories_async(
                org=config.ORGANIZATION,
                top_n=config.TOP_N_REPOSITORIES,
                min_stars=config.MIN_STARS,
                sort_by=config.REPOSITORY_SORT_BY,
                order=config.REPOSITORY_ORDER,
                max_pages=config.MAX_PAGES_PER_ENDPOINT if config.ENABLE_FULL_PAGINATION else 10,
                exclude_forks=config.EXCLUDE_FORKS,
                exclude_archived=config.EXCLUDE_ARCHIVED,
                min_size_kb=config.MIN_REPOSITORY_SIZE_KB,
                max_size_kb=config.MAX_REPOSITORY_SIZE_KB
            )
            
            if not repositories:
                self.logger.error("No repositories found")
                return []
            
            self.logger.info(f"Selected repositories: {[repo.name for repo in repositories]}")
            
            # Step 2: Filter repositories by language
                        # Setp 1.5: Filter repositories by include list
            if config.INCLUDE_REPOSITORIES:
                self.logger.info(f"Applying include list: {config.INCLUDE_REPOSITORIES}")
                filtered_repos = self._filter_repositories_by_include_list(repositories, config.INCLUDE_REPOSITORIES)
                
                if not filtered_repos:
                    self.logger.error("No repositories remaining after include list filtering")
                    return []
            
            # filtered_repos = repositories #self._filter_repositories_by_language(repositories)
            
            if not filtered_repos:
                self.logger.error("No repositories remaining after language filtering")
                return []
            
            # Step 3: Extract PR data
            if config.ENABLE_PARALLEL_PROCESSING:
                all_pr_data = await self._extract_prs_parallel(filtered_repos)
            else:
                all_pr_data = await self._extract_prs_sequential(filtered_repos)
            
            extraction_time = time.time() - start_time
            self.logger.info(f"Extracted data from {len(all_pr_data)} pull requests in {extraction_time:.2f} seconds")
            
            # Step 4: Final save data (in case any PRs were processed after the last batch save)
            self._save_data(all_pr_data, is_final=True)
            
            return all_pr_data
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            raise
    
    def _filter_repositories_by_include_list(self, repositories: List[Repository], include_list: List[str]) -> List[Repository]:
        """
        Filter repositories to only include those whose full_name or name is in the include_list.
        If include_list is empty, return all repositories.
        """
        if not include_list:
            return repositories
        include_set = set(include_list)
        filtered = [
            repo for repo in repositories
            if repo.full_name in include_set or repo.name in include_set
        ]
        self.logger.info(f"Filtered to {len(filtered)} repositories after applying include list")
        return filtered
    
    
    def _filter_repositories_by_language(self, repositories: List[Repository]) -> List[Repository]:
        """Filter repositories by programming language"""
        filtered = []
        
        for repo in repositories:
            # Check include list
            if config.INCLUDE_ONLY_LANGUAGES:
                if not any(lang in repo.languages for lang in config.INCLUDE_ONLY_LANGUAGES):
                    self.logger.debug(f"Skipping {repo.name} - language not in include list")
                    continue
            
            # Check exclude list
            if config.EXCLUDE_LANGUAGES:
                if any(lang in repo.languages for lang in config.EXCLUDE_LANGUAGES):
                    self.logger.debug(f"Skipping {repo.name} - language in exclude list")
                    continue
            
            filtered.append(repo)
        
        self.logger.info(f"Filtered to {len(filtered)} repositories after language filtering")
        return filtered
    
    async def _extract_prs_parallel(self, repositories: List[Repository]) -> List[Dict]:
        """Extract PRs from multiple repositories in parallel"""
        self.logger.info("Using parallel processing for PR extraction")
        
        # Create semaphore to limit concurrent repository processing
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REPOSITORY)
        
        async def process_repo_with_semaphore(repo):
            async with semaphore:
                return await self._extract_repository_prs_async(repo)
        
        # Process all repositories concurrently
        tasks = [process_repo_with_semaphore(repo) for repo in repositories]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful results
        all_pr_data = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Failed to process repository {repositories[i].name}: {result}")
            elif isinstance(result, list):
                all_pr_data.extend(result)
        
        return all_pr_data
    
    async def _extract_prs_sequential(self, repositories: List[Repository]) -> List[Dict]:
        """Extract PRs from repositories sequentially"""
        self.logger.info("Using sequential processing for PR extraction")
        
        all_pr_data = []
        
        for repo in repositories:
            try:
                repo_pr_data = await self._extract_repository_prs_async(repo)
                all_pr_data.extend(repo_pr_data)
            except Exception as e:
                self.logger.error(f"Failed to extract data from {repo.name}: {e}")
                continue
        
        return all_pr_data
    
    async def _extract_repository_prs_async(self, repo: Repository) -> List[Dict]:
        """Extract PR data from a single repository (async)"""
        self.logger.info(f"Processing repository: {repo.name}")
        return await self._process_repository_prs(repo)

    async def _process_repository_prs(self, repo: Repository) -> List[Dict]:
        """Process PRs for a single repository in batches"""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            # Get pull requests
            prs = await self.client.get_pull_requests_async(
                session=session,
                url=repo.pulls_url,
                repo_full_name=repo.full_name,
                max_prs=config.MAX_PRS_PER_REPO,
                state=config.PR_STATE,
                sort_by=config.PR_SORT_BY,
                order=config.PR_ORDER,
                max_pages=config.MAX_PAGES_PER_ENDPOINT if config.ENABLE_FULL_PAGINATION else 10
            )
            
            if not prs:
                self.logger.warning(f"No PRs found for {repo.name}")
                return []
            
            # Filter out already processed PRs if idempotent extraction is enabled
            original_pr_count = len(prs)
            if config.ENABLE_IDEMPOTENT_EXTRACTION and self.processed_pr_ids:
                already_processed_pr_numbers = self.processed_pr_ids.get(repo.name, set())
                if already_processed_pr_numbers:
                    prs = [pr for pr in prs if f"{repo.full_name}_{pr.get('number')}" not in already_processed_pr_numbers]
                    skipped_count = original_pr_count - len(prs)
                    if skipped_count > 0:
                        self.logger.info(f"Skipping {skipped_count} already processed PRs from {repo.name}")
            
            if not prs:
                self.logger.info(f"No new PRs to process for {repo.name} after filtering already processed PRs")
                return []
                
            # Process PRs in batches
            batch_size = config.BATCH_SIZE
            all_processed_prs = []
            
            self.logger.info(f"Processing {len(prs)} PRs from {repo.name} in batches of {batch_size}")
            
            # Process PRs in batches
            for i in range(0, len(prs), batch_size):
                batch_prs = prs[i:i+batch_size]
                batch_number = i // batch_size + 1
                total_batches = (len(prs) + batch_size - 1) // batch_size
                
                self.logger.info(f"Processing batch {batch_number}/{total_batches} with {len(batch_prs)} PRs from {repo.name}")
                
                # Process batch with concurrency control
                semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_PULL_REQUESTS)
                
                async def process_pr_with_semaphore(pr):
                    async with semaphore:
                        return await self._process_pull_request_async(session, repo, pr)
                
                # Process current batch concurrently
                tasks = [process_pr_with_semaphore(pr) for pr in batch_prs]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Collect successful results from this batch
                batch_data = []
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        self.logger.warning(f"Failed to process PR {batch_prs[j].get('number', 'unknown')}: {result}")
                    elif result is not None:
                        batch_data.append(result)
                
                # Save this batch
                if batch_data:
                    batch_name = f"{repo.name}_batch_{batch_number}_of_{total_batches}"
                    self.logger.info(f"Saving batch {batch_number}/{total_batches} with {len(batch_data)} processed PRs from {repo.name}")
                    self._save_data(batch_data, is_incremental=True, batch_name=batch_name)
                    
                    # Add batch data to overall results
                    all_processed_prs.extend(batch_data)
            
            self.logger.info(f"Successfully processed {len(all_processed_prs)} PRs from {repo.name} in {total_batches} batches")
            return all_processed_prs
    
    async def _process_pull_request_async(self, session, repo: Repository, pr_data: Dict) -> Dict:
        """Process a single pull request (async)"""
        pr_number = pr_data["number"]
        id = f"{repo.full_name}_{pr_data.get('number')}"
        try:
            # Check if this PR has already been processed (double-check at PR level)
            if config.ENABLE_IDEMPOTENT_EXTRACTION:
                repo_prs = self.processed_pr_ids.get(repo.name, set())
                if id in repo_prs:
                    self.logger.debug(f"Skipping already processed PR {id} from {repo.full_name}")
                    return None
            
            # Gather commits and comments concurrently
            tasks = []
            
            # Process commits and comments in batches
            commits = []
            comments = []
            
            # Get commits in batches if enabled
            if config.INCLUDE_COMMITS:
                self.logger.debug(f"Fetching commits for PR {pr_number} in batches")
                batch_size = min(20, config.MAX_COMMITS_PER_PR)  # Smaller batch size for commits
                
                # Initial fetch to get first batch and total count
                initial_commits = await self.client.get_pull_request_commits_async(
                    session=session,
                    repo_full_name=repo.full_name,
                    pr_number=pr_number,
                    max_commits=batch_size,
                    page=1
                )
                
                commits.extend(initial_commits)
                
                # Check if we need more batches
                total_fetched = len(initial_commits)
                if total_fetched == batch_size and total_fetched < config.MAX_COMMITS_PER_PR:
                    remaining = config.MAX_COMMITS_PER_PR - total_fetched
                    
                    # Fetch remaining commits in batches
                    page = 2
                    while total_fetched < config.MAX_COMMITS_PER_PR:
                        batch_size = min(20, remaining)
                        commit_batch = await self.client.get_pull_request_commits_async(
                            session=session,
                            repo_full_name=repo.full_name,
                            pr_number=pr_number,
                            max_commits=batch_size,
                            page=page
                        )
                        
                        if not commit_batch:
                            break  # No more commits
                        
                        commits.extend(commit_batch)
                        total_fetched += len(commit_batch)
                        remaining -= len(commit_batch)
                        page += 1
                        
                        self.logger.debug(f"Fetched {len(commit_batch)} more commits for PR {pr_number}, total: {total_fetched}")
                        
                        # Process this batch of commits immediately to avoid memory issues
                        for commit in commit_batch:
                            # Process each commit if needed
                            pass
            
            # Get comments in batches if enabled
            if config.INCLUDE_PR_COMMENTS:
                self.logger.debug(f"Fetching comments for PR {pr_number} in batches")
                batch_size = min(30, config.MAX_COMMENTS_PER_PR)  # Smaller batch size for comments
                
                # Fetch issue comments
                total_comments_fetched = 0
                page = 1
                
                while total_comments_fetched < config.MAX_COMMENTS_PER_PR:
                    remaining = config.MAX_COMMENTS_PER_PR - total_comments_fetched
                    comment_batch_size = min(batch_size, remaining)
                    
                    # Fetch issue comments batch
                    issue_comments = await self.client._get_issue_comments_async(
                        session=session,
                        repo_full_name=repo.full_name,
                        pr_number=pr_number,
                        max_comments=comment_batch_size,
                        max_pages=1,
                        page=page
                    )
                    
                    if not issue_comments:
                        break  # No more issue comments
                    
                    comments.extend(issue_comments)
                    total_comments_fetched += len(issue_comments)
                    page += 1
                    
                    self.logger.debug(f"Fetched {len(issue_comments)} issue comments for PR {pr_number}, total: {total_comments_fetched}")
                
                # Fetch review comments if we still have room
                if total_comments_fetched < config.MAX_COMMENTS_PER_PR:
                    page = 1
                    
                    while total_comments_fetched < config.MAX_COMMENTS_PER_PR:
                        remaining = config.MAX_COMMENTS_PER_PR - total_comments_fetched
                        comment_batch_size = min(batch_size, remaining)
                        
                        # Fetch review comments batch
                        review_comments = await self.client._get_review_comments_async(
                            session=session,
                            repo_full_name=repo.full_name,
                            pr_number=pr_number,
                            max_comments=comment_batch_size,
                            max_pages=1,
                            page=page
                        )
                        
                        if not review_comments:
                            break  # No more review comments
                        
                        comments.extend(review_comments)
                        total_comments_fetched += len(review_comments)
                        page += 1
                        
                        self.logger.debug(f"Fetched {len(review_comments)} review comments for PR {pr_number}, total: {total_comments_fetched}")
                        
                        # Process this batch of comments immediately to avoid memory issues
                        for comment in review_comments:
                            # Process each comment if needed
                            pass
            
            # Process commits data
            commits_dict = {}
            for commit in commits:
                commits_dict[commit.sha] = {
                    "commit_message": commit.message,
                    "author": commit.author,
                    "date": commit.date,
                }
            
            # Process comments data
            comments_list = []
            for comment in comments:
                comments_list.append({
                    "id": comment.id,
                    "body": comment.body,
                    "author": comment.author,
                    "date": comment.date,
                    "type": comment.comment_type,
                    "position": comment.position
                })
            
            # Enhanced change type detection
            if config.ENHANCED_CHANGE_TYPE_DETECTION:
                change_result = self.client.determine_change_type_enhanced(
                    title=pr_data["title"],
                    body=pr_data["body"],
                    labels=pr_data.get("labels", []),
                    commits=commits if config.ANALYZE_COMMIT_MESSAGES else None,
                    confidence_threshold=config.CHANGE_TYPE_CONFIDENCE_THRESHOLD
                )
                change_type = change_result.change_type
                change_confidence = change_result.confidence
                change_reasons = change_result.reasons
            else:
                # Fallback to basic detection
                change_type = self._determine_change_type_basic(pr_data)
                change_confidence = 0.5
                change_reasons = ["Basic detection used"]
            
            # Enhanced tag extraction
            tags = self.client.extract_tags_enhanced(pr_data)
            
            # Create the structured data
            structured_pr_data = {
                "id": f"{repo.full_name}_{pr_number}",
                "repository_url": repo.url,
                "repository_language": repo.languages,
                "repository_name": repo.name,
                "repository_full_name": repo.full_name,  # Add full_name for idempotent tracking
                "repository_stars": repo.stars,
                "repository_forks": repo.forks,
                "title": pr_data["title"],
                "description": pr_data["body"] or "",
                "change_type": change_type,
                "change_type_confidence": change_confidence,
                "change_type_reasons": change_reasons,
                "commits": commits_dict,
                "pr_comments": comments_list,
                "tags": tags,
                "pr_number": pr_number,
                "pr_state": pr_data["state"],
                "created_at": pr_data["created_at"],
                "updated_at": pr_data["updated_at"],
                "merged_at": pr_data.get("merged_at"),
                "author": pr_data["user"]["login"],
                "draft": pr_data.get("draft", False)
            }
            
            return structured_pr_data
            
        except Exception as e:
            self.logger.warning(f"Failed to process PR {pr_number} in {repo.name}: {e}")
            return None
    
    def _determine_change_type_basic(self, pr_data: Dict) -> str:
        """Basic change type determination for fallback"""
        title_lower = pr_data["title"].lower()
        body_lower = (pr_data["body"] or "").lower()
        
        # Simple keyword matching
        if any(word in title_lower or word in body_lower for word in ["fix", "bug", "patch"]):
            return "fix"
        elif any(word in title_lower or word in body_lower for word in ["doc", "readme", "documentation"]):
            return "doc"
        else:
            return "feature"
    
    def _save_data(self, data: List[Dict], is_incremental=False, is_final=False, batch_name=None):
        """Save extracted data with enhanced metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create metadata
        metadata = {
            "extraction_timestamp": timestamp,
            "total_prs": len(data),
            "is_incremental_save": is_incremental,
            "is_final_save": is_final,
            "batch_name": batch_name,
            "configuration": {
                "organization": config.ORGANIZATION,
                "top_n_repositories": config.TOP_N_REPOSITORIES,
                "max_prs_per_repo": config.MAX_PRS_PER_REPO,
                "include_commits": config.INCLUDE_COMMITS,
                "include_comments": config.INCLUDE_PR_COMMENTS,
                "enhanced_detection": config.ENHANCED_CHANGE_TYPE_DETECTION,
                "parallel_processing": config.ENABLE_PARALLEL_PROCESSING,
                "async_requests": config.USE_ASYNC_REQUESTS,
                "batch_size": config.BATCH_SIZE
            },
            "statistics": self._generate_statistics(data)
        }
        
        # Generate a suffix for the filename based on save type
        file_suffix = ""
        if is_incremental:
            if batch_name:
                file_suffix = f"_{batch_name}"
            else:
                file_suffix = f"_batch_{timestamp}"
        elif is_final:
            file_suffix = "_final"
        
        if config.OUTPUT_FORMAT.lower() == "json":
            filename = f"enhanced_pr_data{file_suffix}_{timestamp}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            output_data = {
                "metadata": metadata,
                "data": data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"{'Incremental' if is_incremental else 'Final' if is_final else 'Enhanced'} data saved to {filepath}")
            
            # Update processed PR IDs tracking for idempotent extraction
            if config.ENABLE_IDEMPOTENT_EXTRACTION:
                self._update_processed_pr_ids(data)
        
        elif config.OUTPUT_FORMAT.lower() == "csv":
            filename = f"enhanced_pr_data{file_suffix}_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            if data:
                flattened_data = self._flatten_data_for_csv(data)
                
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    if flattened_data:
                        writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
                        writer.writeheader()
                        writer.writerows(flattened_data)
                
                self.logger.info(f"{'Incremental' if is_incremental else 'Final' if is_final else 'Enhanced'} data saved to {filepath}")
                
                # Also save metadata separately
                metadata_filepath = os.path.join(self.output_dir, f"metadata{file_suffix}_{timestamp}.json")
                with open(metadata_filepath, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                # Save detailed JSON for reference
                json_filename = f"enhanced_pr_data_detailed{file_suffix}_{timestamp}.json"
                json_filepath = os.path.join(self.output_dir, json_filename)
                
                output_data = {
                    "metadata": metadata,
                    "data": data
                }
                
                with open(json_filepath, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    def _flatten_data_for_csv(self, data: List[Dict]) -> List[Dict]:
        """Flatten complex data structure for CSV export"""
        flattened = []
        
        for item in data:
            flattened_item = {
                "id": item["id"],
                "repository_url": item["repository_url"],
                "repository_language": "|".join(item["repository_language"]),
                "repository_name": item["repository_name"],
                "repository_stars": item.get("repository_stars", 0),
                "repository_forks": item.get("repository_forks", 0),
                "title": item["title"],
                "description": item["description"][:500] + "..." if len(item["description"]) > 500 else item["description"],
                "change_type": item["change_type"],
                "change_type_confidence": item.get("change_type_confidence", 0),
                "commits_count": len(item["commits"]),
                "comments_count": len(item["pr_comments"]),
                "tags": "|".join(item["tags"]),
                "pr_number": item.get("pr_number"),
                "pr_state": item.get("pr_state"),
                "created_at": item.get("created_at"),
                "updated_at": item.get("updated_at"),
                "merged_at": item.get("merged_at"),
                "author": item.get("author"),
                "draft": item.get("draft", False)
            }
            flattened.append(flattened_item)
        
        return flattened
    
    def _generate_statistics(self, data: List[Dict]) -> Dict:
        """Generate statistics about the extracted data"""
        if not data:
            return {}
        
        # Repository statistics
        repo_counts = {}
        language_counts = {}
        change_type_counts = {}
        author_counts = {}
        
        total_commits = 0
        total_comments = 0;
        
        for item in data:
            # Repository counts
            repo_name = item["repository_name"]
            repo_counts[repo_name] = repo_counts.get(repo_name, 0) + 1;
            
            # Language counts
            for lang in item["repository_language"]:
                language_counts[lang] = language_counts.get(lang, 0) + 1;
            
            # Change type counts
            change_type = item["change_type"]
            change_type_counts[change_type] = change_type_counts.get(change_type, 0) + 1;
            
            # Author counts
            author = item.get("author", "unknown")
            author_counts[author] = author_counts.get(author, 0) + 1;
            
            # Aggregate statistics
            total_commits += len(item.get("commits", {}));
            total_comments += len(item.get("pr_comments", []));
        
        return {
            "repository_distribution": dict(sorted(repo_counts.items(), key=lambda x: x[1], reverse=True)),
            "language_distribution": dict(sorted(language_counts.items(), key=lambda x: x[1], reverse=True)),
            "change_type_distribution": dict(sorted(change_type_counts.items(), key=lambda x: x[1], reverse=True)),
            "top_authors": dict(sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            "aggregate_stats": {
                "total_commits": total_commits,
                "total_comments": total_comments,
                "avg_commits_per_pr": total_commits / len(data),
                "avg_comments_per_pr": total_comments / len(data)
            }
        }
    
    def _load_processed_pr_ids(self):
        """
        Scan the output directory for existing PR data files and build a set of already
        processed PR IDs to avoid duplicate processing. Returns a dictionary with 
        repo_full_name as keys and sets of PR numbers as values.
        """
        processed_prs = {}
        
        if not os.path.exists(self.output_dir):
            self.logger.info("Output directory does not exist yet. No previous PRs to load.")
            return processed_prs
            
        try:
            # Look for json files in the output directory
            json_files = [f for f in os.listdir(self.output_dir) 
                        if f.startswith("enhanced_pr_data") and f.endswith(".json")]
            
            if not json_files:
                self.logger.info("No existing PR data files found.")
                return processed_prs
                
            self.logger.info(f"Found {len(json_files)} existing PR data files. Loading processed PR IDs...")
            
            total_prs: int = 0
            for filename in json_files:
                filepath = os.path.join(self.output_dir, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        
                    if 'data' not in file_data:
                        continue
                        
                    # Process each PR entry
                    for pr in file_data['data']:
                        if 'repository_name' in pr and 'id' in pr:
                            repo_name = pr['repository_name']
                            id = pr['id']
                            
                            if repo_name not in processed_prs:
                                processed_prs[repo_name] = set()
                                
                            processed_prs[repo_name].add(id)
                            total_prs += 1
                            
                except Exception as e:
                    self.logger.warning(f"Error loading PR IDs from {filename}: {e}")
                    continue
            
            # Log summary of loaded PRs
            total_repos = len(processed_prs)
            
            self.logger.info(f"Loaded {total_prs} processed PR IDs from {total_repos} repositories.")
            
            # Log detail for each repository
            for repo_name, pr_set in processed_prs.items():
                self.logger.debug(f"Repository {repo_name}: {len(pr_set)} processed PRs")
                
        except Exception as e:
            self.logger.error(f"Error loading processed PR IDs: {e}")
            return {}
            
        return processed_prs

    def _update_processed_pr_ids(self, data: List[Dict]):
        """
        Update the dictionary of processed PR IDs with new data.
        This is called after each save to maintain an up-to-date record
        of which PRs have been processed.
        """
        if not config.ENABLE_IDEMPOTENT_EXTRACTION or not data:
            return
            
        update_count = 0
        for pr in data:
            repo_name = pr['repository_name']
            id = pr['id']
            
            if repo_name not in self.processed_pr_ids:
                self.processed_pr_ids[repo_name] = set()
                
            if id not in self.processed_pr_ids[repo_name]:
                self.processed_pr_ids[repo_name].add(id)
                update_count += 1
                    
        if update_count > 0:
            self.logger.debug(f"Added {update_count} PRs to the processed PR tracking cache")

def main():
    """Main function with async support"""
    async def run_extraction():
        try:
            # Initialize extractor
            extractor = EnhancedPRDataExtractor()
            
            # Extract data
            print("\n" + "="*60)
            print("ENHANCED PR DATA EXTRACTION")
            print("="*60)
            print(f"Batch size for incremental saves: {config.BATCH_SIZE} PRs")
            print(f"Output directory: {config.OUTPUT_DIRECTORY}")
            print(f"Output format: {config.OUTPUT_FORMAT}")
            print(f"Async processing: {config.USE_ASYNC_REQUESTS}")
            print(f"Parallel processing: {config.ENABLE_PARALLEL_PROCESSING}")
            print("="*60 + "\n")
            
            data = await extractor.extract_data_async()
            
            if data:
                extractor.logger.info(f"Successfully extracted {len(data)} pull requests")
                
                # Print enhanced summary
                print("\n" + "="*60)
                print("ENHANCED EXTRACTION SUMMARY")
                print("="*60)
                print(f"Total PRs extracted: {len(data)}")
                print(f"Output directory: {config.OUTPUT_DIRECTORY}")
                print(f"Output format: {config.OUTPUT_FORMAT}")
                print(f"Async processing: {config.USE_ASYNC_REQUESTS}")
                print(f"Parallel processing: {config.ENABLE_PARALLEL_PROCESSING}")
                print(f"Enhanced detection: {config.ENHANCED_CHANGE_TYPE_DETECTION}")
                
                # Repository summary
                repo_counts = {}
                for item in data:
                    repo_name = item["repository_name"]
                    repo_counts[repo_name] = repo_counts.get(repo_name, 0) + 1
                
                print(f"\nPRs per repository:")
                for repo, count in sorted(repo_counts.items()):
                    print(f"  {repo}: {count}")
                
                # Change type summary with confidence
                change_type_data = {}
                for item in data:
                    change_type = item["change_type"]
                    confidence = item.get("change_type_confidence", 0)
                    if change_type not in change_type_data:
                        change_type_data[change_type] = {"count": 0, "total_confidence": 0}
                    change_type_data[change_type]["count"] += 1
                    change_type_data[change_type]["total_confidence"] += confidence
                
                print(f"\nChange types (with average confidence):")
                for change_type, data_item in sorted(change_type_data.items()):
                    avg_confidence = data_item["total_confidence"] / data_item["count"]
                    print(f"  {change_type}: {data_item['count']} (avg confidence: {avg_confidence:.2f})")
                
            else:
                extractor.logger.warning("No data extracted")
        
        except Exception as e:
            logging.error(f"Enhanced extraction failed: {e}")
            raise
    
    # Run the async extraction
    asyncio.run(run_extraction())

if __name__ == "__main__":
    main()
