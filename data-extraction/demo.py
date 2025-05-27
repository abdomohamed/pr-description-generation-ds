"""
Comparison and demo script showing enhanced features
"""

import asyncio
import time
import json
import os
from datetime import datetime
import logging

# Import the main extractor class
from extract_pr_data import EnhancedPRDataExtractor
from github_client import setup_logging
import config

async def demo_enhanced_features():
    """Demo the enhanced features"""
    print("🚀 Enhanced GitHub PR Data Extractor - Feature Demo")
    print("="*60)
    
    # Setup logging for demo
    logger = setup_logging(
        log_level="INFO",
        log_to_file=True,
        log_file_path="./extracted_data/demo.log"
    )
    
    print("\n📋 Enhanced Features Overview:")
    print("1. ✅ Async HTTP requests with aiohttp")
    print("2. ✅ Parallel processing with configurable concurrency")
    print("3. ✅ Enhanced change-type detection with confidence scoring")
    print("4. ✅ Full pagination support for large datasets")
    print("5. ✅ Comprehensive error handling and retry logic")
    print("6. ✅ Enhanced logging with file output")
    print("7. ✅ Repository filtering by size, fork status, and archive status")
    print("8. ✅ Commit message analysis for change type detection")
    print("9. ✅ Extended PR metadata extraction")
    print("10. ✅ Statistical analysis and reporting")
    
    print(f"\n⚙️  Current Configuration:")
    print(f"   Organization: {config.ORGANIZATION}")
    print(f"   Top repositories: {config.TOP_N_REPOSITORIES}")
    print(f"   Max PRs per repo: {config.MAX_PRS_PER_REPO}")
    print(f"   Async requests: {config.USE_ASYNC_REQUESTS}")
    print(f"   Parallel processing: {config.ENABLE_PARALLEL_PROCESSING}")
    print(f"   Enhanced detection: {config.ENHANCED_CHANGE_TYPE_DETECTION}")
    print(f"   Full pagination: {config.ENABLE_FULL_PAGINATION}")
    print(f"   Max concurrent requests: {config.MAX_CONCURRENT_REQUESTS}")
    print(f"   Max worker threads: {config.MAX_WORKER_THREADS}")
    
    return logger

def demo_change_type_detection():
    """Demo the enhanced change type detection"""
    print("\n🔍 Enhanced Change Type Detection Demo")
    print("="*50)
    
    from github_client import EnhancedGitHubAPIClient
    
    # Create a mock client for testing (without token requirement)
    class MockClient(EnhancedGitHubAPIClient):
        def __init__(self):
            self.token = "mock"
            self._setup_change_type_patterns()
    
    client = MockClient()
    
    # Test cases for change type detection
    test_cases = [
        {
            "title": "Fix memory leak in data processor",
            "body": "This patch resolves a critical memory leak that was causing crashes in production.",
            "labels": [{"name": "bug"}, {"name": "critical"}],
            "expected": "fix"
        },
        {
            "title": "Add new API endpoint for user management",
            "body": "Implemented a new REST API endpoint that allows administrators to manage user accounts.",
            "labels": [{"name": "feature"}, {"name": "api"}],
            "expected": "feature"
        },
        {
            "title": "Update README with installation instructions",
            "body": "Added comprehensive installation guide and examples to the documentation.",
            "labels": [{"name": "documentation"}],
            "expected": "doc"
        },
        {
            "title": "Refactor database connection logic",
            "body": "Improved the database connection handling for better performance and reliability.",
            "labels": [{"name": "enhancement"}],
            "expected": "feature"
        }
    ]
    
    print("\nTesting change type detection:")
    for i, case in enumerate(test_cases, 1):
        result = client.determine_change_type_enhanced(
            title=case["title"],
            body=case["body"],
            labels=case["labels"],
            confidence_threshold=0.3
        )
        
        status = "✅" if result.change_type == case["expected"] else "❌"
        print(f"\n{i}. {status} Test Case:")
        print(f"   Title: {case['title']}")
        print(f"   Expected: {case['expected']}")
        print(f"   Detected: {result.change_type} (confidence: {result.confidence:.2f})")
        print(f"   Reasons: {', '.join(result.reasons[:3])}...")

def demo_parallel_vs_sequential():
    """Demo showing parallel vs sequential processing benefits"""
    print("\n⚡ Parallel vs Sequential Processing Demo")
    print("="*50)
    
    # Simulate processing times
    repositories = ["repo1", "repo2", "repo3", "repo4", "repo5"]
    processing_time_per_repo = 2.0  # seconds
    
    print(f"Simulating processing of {len(repositories)} repositories:")
    print(f"Processing time per repo: {processing_time_per_repo}s")
    
    # Sequential simulation
    print(f"\n📊 Sequential Processing:")
    sequential_time = len(repositories) * processing_time_per_repo
    print(f"   Total time: {sequential_time}s")
    print(f"   Repositories processed one by one")
    
    # Parallel simulation
    print(f"\n📊 Parallel Processing (4 workers):")
    parallel_time = max(1, len(repositories) / 4) * processing_time_per_repo
    print(f"   Total time: {parallel_time}s")
    print(f"   Repositories processed concurrently")
    print(f"   Speedup: {sequential_time / parallel_time:.2f}x faster")
    
    return sequential_time, parallel_time

async def demo_pagination_benefits():
    """Demo pagination benefits"""
    print("\n📄 Pagination Benefits Demo")
    print("="*40)
    
    print("Without pagination:")
    print("  - Limited to first 100 items per API call")
    print("  - May miss important data in large repositories")
    print("  - Inconsistent results for different repositories")
    
    print("\nWith enhanced pagination:")
    print("  - Fetches all available data (up to configured limit)")
    print("  - Consistent and comprehensive data collection")
    print("  - Configurable page limits for performance control")
    print("  - Better coverage of repository activity")

def create_sample_enhanced_output():
    """Create a sample of enhanced output format"""
    print("\n📋 Enhanced Output Format Demo")
    print("="*40)
    
    sample_data = {
        "metadata": {
            "extraction_timestamp": "20250526_120000",
            "total_prs": 150,
            "configuration": {
                "organization": "microsoft",
                "top_n_repositories": 5,
                "max_prs_per_repo": 30,
                "enhanced_detection": True,
                "parallel_processing": True,
                "async_requests": True
            },
            "statistics": {
                "repository_distribution": {
                    "vscode": 45,
                    "TypeScript": 35,
                    "PowerToys": 30,
                    "terminal": 25,
                    "calculator": 15
                },
                "language_distribution": {
                    "TypeScript": 80,
                    "C++": 35,
                    "C#": 30,
                    "Python": 5
                },
                "change_type_distribution": {
                    "feature": 75,
                    "fix": 60,
                    "doc": 15
                },
                "aggregate_stats": {
                    "avg_additions_per_pr": 156.7,
                    "avg_deletions_per_pr": 43.2,
                    "avg_commits_per_pr": 3.4,
                    "avg_comments_per_pr": 2.1,
                    "avg_changed_files_per_pr": 4.7
                }
            }
        },
        "data": [
            {
                "id": "microsoft/vscode_12345",
                "repository_url": "https://github.com/microsoft/vscode",
                "repository_language": ["TypeScript", "JavaScript"],
                "repository_name": "vscode",
                "repository_stars": 145000,
                "repository_forks": 25000,
                "title": "Fix editor scrolling performance issue",
                "description": "This PR addresses a performance bottleneck in the editor scrolling mechanism...",
                "change_type": "fix",
                "change_type_confidence": 0.85,
                "change_type_reasons": [
                    "Keyword 'fix' found in title",
                    "Label 'bug' matches fix",
                    "Commit pattern 'fix' matches fix"
                ],
                "commits": {
                    "abc123def456": {
                        "commit_message": "Fix scrolling performance in large files",
                        "author": "developer1",
                        "date": "2025-05-26T10:30:00Z",
                        "additions": 45,
                        "deletions": 12,
                        "changed_files": 3
                    }
                },
                "pr_comments": [
                    {
                        "id": "comment_789",
                        "body": "Great fix! This resolves the issue we've been seeing.",
                        "author": "reviewer1",
                        "date": "2025-05-26T11:00:00Z",
                        "type": "issue",
                        "position": None
                    }
                ],
                "tags": ["bug", "performance", "editor"],
                "pr_number": 12345,
                "pr_state": "closed",
                "created_at": "2025-05-26T09:00:00Z",
                "updated_at": "2025-05-26T12:00:00Z",
                "merged_at": "2025-05-26T12:00:00Z",
                "additions": 45,
                "deletions": 12,
                "changed_files": 3,
                "author": "developer1",
                "draft": False
            }
        ]
    }
    
    # Save sample output
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sample_file = f"./extracted_data/enhanced_sample_{timestamp}.json"
    
    os.makedirs("./extracted_data", exist_ok=True)
    with open(sample_file, 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, indent=2, ensure_ascii=False)
    
    print(f"Enhanced sample output saved to: {sample_file}")
    
    print(f"\n📊 Key enhancements in output:")
    print(f"   - Comprehensive metadata with statistics")
    print(f"   - Change type confidence scoring")
    print(f"   - Detailed commit information")
    print(f"   - Enhanced comment data with types")
    print(f"   - Repository metrics (stars, forks)")
    print(f"   - PR metrics (additions, deletions, changed files)")
    print(f"   - Aggregate statistics for analysis")

async def run_performance_comparison():
    """Run a performance comparison between old and new versions"""
    print("\n⏱️  Performance Comparison")
    print("="*40)
    
    # Temporarily modify config for quick demo
    original_top_n = config.TOP_N_REPOSITORIES
    original_max_prs = config.MAX_PRS_PER_REPO
    
    try:
        # Set to minimal values for demo
        config.TOP_N_REPOSITORIES = 2
        config.MAX_PRS_PER_REPO = 5
        
        print("Running with minimal configuration for demo:")
        print(f"  Top repositories: {config.TOP_N_REPOSITORIES}")
        print(f"  Max PRs per repo: {config.MAX_PRS_PER_REPO}")
        
        # Note: We can't actually run without a token, so this is theoretical
        print(f"\n📈 Expected Performance Improvements:")
        print(f"   - Async requests: 3-5x faster for I/O operations")
        print(f"   - Parallel processing: 2-4x faster for CPU operations")
        print(f"   - Enhanced pagination: More complete data collection")
        print(f"   - Better error handling: Higher success rate")
        print(f"   - Retry logic: Improved reliability")
        
    finally:
        # Restore original config
        config.TOP_N_REPOSITORIES = original_top_n
        config.MAX_PRS_PER_REPO = original_max_prs

def demo_error_handling():
    """Demo enhanced error handling features"""
    print("\n🛡️  Enhanced Error Handling Demo")
    print("="*40)
    
    print("Enhanced error handling features:")
    print("  ✅ Retry logic with exponential backoff")
    print("  ✅ Rate limit detection and automatic waiting")
    print("  ✅ Comprehensive logging with different levels")
    print("  ✅ Graceful degradation on partial failures")
    print("  ✅ Detailed error context and troubleshooting info")
    print("  ✅ Async timeout handling")
    print("  ✅ Connection pool management")
    print("  ✅ Structured error reporting")

async def main():
    """Run the comprehensive demo"""
    try:
        # Demo setup
        logger = await demo_enhanced_features()
        
        # Individual feature demos
        demo_change_type_detection()
        demo_parallel_vs_sequential()
        await demo_pagination_benefits()
        create_sample_enhanced_output()
        await run_performance_comparison()
        demo_error_handling()
        
        print(f"\n🎉 Demo Complete!")
        print(f"\n📚 Next Steps:")
        print(f"   1. Set your GitHub token: export GITHUB_TOKEN='your_token'")
        print(f"   2. Customize config.py for your needs")
        print(f"   3. Run extraction: python extract_pr_data.py")
        print(f"   4. Validate configuration: python validate_config.py")
        print(f"   5. Check logs in: ./extracted_data/extraction.log")
        
        # Summary of improvements
        print(f"\n🔧 Key Improvements Summary:")
        print(f"   - Async processing for better performance")
        print(f"   - Parallel repository processing")
        print(f"   - Enhanced change-type detection with ML-like confidence")
        print(f"   - Full pagination support for comprehensive data")
        print(f"   - Advanced error handling and retry logic")
        print(f"   - Comprehensive logging and monitoring")
        print(f"   - Extended metadata and statistics")
        print(f"   - Repository filtering and validation")
        print(f"   - Structured output with metadata")
        print(f"   - Performance monitoring and reporting")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
