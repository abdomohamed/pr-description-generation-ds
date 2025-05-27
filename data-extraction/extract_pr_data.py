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
    EnhancedGitHubAPIClient, ParallelProcessor, setup_logging,
    Repository, ChangeTypeResult
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
            max_concurrent_requests=config.MAX_CONCURRENT_REQUESTS,
            enable_request_logging=config.ENABLE_REQUEST_LOGGING
        )
        
        self.output_dir = config.OUTPUT_DIRECTORY
        self.parallel_processor = ParallelProcessor(max_workers=config.MAX_WORKER_THREADS)
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
    
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
            filtered_repos = self._filter_repositories_by_language(repositories)
            
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
            
            # Step 4: Save data
            self._save_data(all_pr_data)
            
            return all_pr_data
            
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            raise
    
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
        semaphore = asyncio.Semaphore(config.MAX_WORKER_THREADS)
        
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
        
        async with self.client.throttler:
            async with asyncio.timeout(300):  # 5 minute timeout per repository
                return await self._process_repository_prs(repo)
    
    async def _process_repository_prs(self, repo: Repository) -> List[Dict]:
        """Process PRs for a single repository"""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            # Get pull requests
            prs = await self.client.get_pull_requests_async(
                session=session,
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
            
            # Process PRs with concurrency control
            semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS // 2)
            
            async def process_pr_with_semaphore(pr):
                async with semaphore:
                    return await self._process_pull_request_async(session, repo, pr)
            
            # Process all PRs concurrently
            tasks = [process_pr_with_semaphore(pr) for pr in prs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect successful results
            pr_data_list = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.warning(f"Failed to process PR {prs[i].get('number', 'unknown')}: {result}")
                elif result is not None:
                    pr_data_list.append(result)
            
            self.logger.info(f"Successfully processed {len(pr_data_list)} PRs from {repo.name}")
            return pr_data_list
    
    async def _process_pull_request_async(self, session, repo: Repository, pr_data: Dict) -> Dict:
        """Process a single pull request (async)"""
        pr_number = pr_data["number"]
        
        try:
            # Filter by minimum changed files
            if pr_data.get("changed_files", 0) < config.MIN_CHANGED_FILES:
                return None
            
            # Gather commits and comments concurrently
            tasks = []
            
            # Get commits if enabled
            if config.INCLUDE_COMMITS:
                tasks.append(self.client.get_pull_request_commits_async(
                    session=session,
                    repo_full_name=repo.full_name,
                    pr_number=pr_number,
                    max_commits=config.MAX_COMMITS_PER_PR
                ))
            else:
                tasks.append(asyncio.create_task(asyncio.coroutine(lambda: [])()))
            
            # Get comments if enabled
            if config.INCLUDE_PR_COMMENTS:
                tasks.append(self.client.get_pull_request_comments_async(
                    session=session,
                    repo_full_name=repo.full_name,
                    pr_number=pr_number,
                    max_comments=config.MAX_COMMENTS_PER_PR,
                    max_pages=config.MAX_PAGES_PER_ENDPOINT if config.ENABLE_FULL_PAGINATION else 5
                ))
            else:
                tasks.append(asyncio.create_task(asyncio.coroutine(lambda: [])()))
            
            # Wait for both to complete
            commits, comments = await asyncio.gather(*tasks)
            
            # Process commits data
            commits_dict = {}
            for commit in commits:
                commits_dict[commit.sha] = {
                    "commit_message": commit.message,
                    "author": commit.author,
                    "date": commit.date,
                    "additions": commit.additions,
                    "deletions": commit.deletions,
                    "changed_files": commit.changed_files
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
                "additions": pr_data.get("additions", 0),
                "deletions": pr_data.get("deletions", 0),
                "changed_files": pr_data.get("changed_files", 0),
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
    
    def _save_data(self, data: List[Dict]):
        """Save extracted data with enhanced metadata"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create metadata
        metadata = {
            "extraction_timestamp": timestamp,
            "total_prs": len(data),
            "configuration": {
                "organization": config.ORGANIZATION,
                "top_n_repositories": config.TOP_N_REPOSITORIES,
                "max_prs_per_repo": config.MAX_PRS_PER_REPO,
                "include_commits": config.INCLUDE_COMMITS,
                "include_comments": config.INCLUDE_PR_COMMENTS,
                "enhanced_detection": config.ENHANCED_CHANGE_TYPE_DETECTION,
                "parallel_processing": config.ENABLE_PARALLEL_PROCESSING,
                "async_requests": config.USE_ASYNC_REQUESTS
            },
            "statistics": self._generate_statistics(data)
        }
        
        if config.OUTPUT_FORMAT.lower() == "json":
            filename = f"enhanced_pr_data_{timestamp}.json"
            filepath = os.path.join(self.output_dir, filename)
            
            output_data = {
                "metadata": metadata,
                "data": data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Enhanced data saved to {filepath}")
        
        elif config.OUTPUT_FORMAT.lower() == "csv":
            filename = f"enhanced_pr_data_{timestamp}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            if data:
                flattened_data = self._flatten_data_for_csv(data)
                
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    if flattened_data:
                        writer = csv.DictWriter(f, fieldnames=flattened_data[0].keys())
                        writer.writeheader()
                        writer.writerows(flattened_data)
                
                self.logger.info(f"Enhanced data saved to {filepath}")
                
                # Also save metadata separately
                metadata_filepath = os.path.join(self.output_dir, f"metadata_{timestamp}.json")
                with open(metadata_filepath, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                # Save detailed JSON for reference
                json_filename = f"enhanced_pr_data_detailed_{timestamp}.json"
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
                "additions": item.get("additions", 0),
                "deletions": item.get("deletions", 0),
                "changed_files": item.get("changed_files", 0),
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
        
        total_additions = 0
        total_deletions = 0
        total_changed_files = 0
        total_commits = 0
        total_comments = 0
        
        for item in data:
            # Repository counts
            repo_name = item["repository_name"]
            repo_counts[repo_name] = repo_counts.get(repo_name, 0) + 1
            
            # Language counts
            for lang in item["repository_language"]:
                language_counts[lang] = language_counts.get(lang, 0) + 1
            
            # Change type counts
            change_type = item["change_type"]
            change_type_counts[change_type] = change_type_counts.get(change_type, 0) + 1
            
            # Author counts
            author = item.get("author", "unknown")
            author_counts[author] = author_counts.get(author, 0) + 1
            
            # Aggregate statistics
            total_additions += item.get("additions", 0)
            total_deletions += item.get("deletions", 0)
            total_changed_files += item.get("changed_files", 0)
            total_commits += len(item.get("commits", {}))
            total_comments += len(item.get("pr_comments", []))
        
        return {
            "repository_distribution": dict(sorted(repo_counts.items(), key=lambda x: x[1], reverse=True)),
            "language_distribution": dict(sorted(language_counts.items(), key=lambda x: x[1], reverse=True)),
            "change_type_distribution": dict(sorted(change_type_counts.items(), key=lambda x: x[1], reverse=True)),
            "top_authors": dict(sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            "aggregate_stats": {
                "total_additions": total_additions,
                "total_deletions": total_deletions,
                "total_changed_files": total_changed_files,
                "total_commits": total_commits,
                "total_comments": total_comments,
                "avg_additions_per_pr": total_additions / len(data),
                "avg_deletions_per_pr": total_deletions / len(data),
                "avg_changed_files_per_pr": total_changed_files / len(data),
                "avg_commits_per_pr": total_commits / len(data),
                "avg_comments_per_pr": total_comments / len(data)
            }
        }

def main():
    """Main function with async support"""
    async def run_extraction():
        try:
            # Initialize extractor
            extractor = EnhancedPRDataExtractor()
            
            # Extract data
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
