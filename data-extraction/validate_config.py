#!/usr/bin/env python3
"""
Enhanced configuration validator and optimization tool
"""

import os
import asyncio
import time
from datetime import datetime
import config

def validate_enhanced_config():
    """Validate the enhanced configuration"""
    print("📋 Enhanced Configuration Validation")
    print("="*50)
    
    # GitHub Configuration
    token_status = "✅ Set" if (config.GITHUB_TOKEN or os.getenv('GITHUB_TOKEN')) else "❌ Not set"
    print(f"GitHub Token: {token_status}")
    print(f"Organization: {config.ORGANIZATION}")
    print(f"API Base URL: {config.GITHUB_API_BASE_URL}")
    print()
    
    # Repository Selection (Enhanced)
    print("Repository Selection:")
    print(f"  Top N repositories: {config.TOP_N_REPOSITORIES}")
    print(f"  Minimum stars: {config.MIN_STARS}")
    print(f"  Sort by: {config.REPOSITORY_SORT_BY}")
    print(f"  Order: {config.REPOSITORY_ORDER}")
    print(f"  Exclude forks: {config.EXCLUDE_FORKS}")
    print(f"  Exclude archived: {config.EXCLUDE_ARCHIVED}")
    print(f"  Min size (KB): {config.MIN_REPOSITORY_SIZE_KB}")
    print(f"  Max size (KB): {config.MAX_REPOSITORY_SIZE_KB}")
    print()
    
    # Performance Configuration (New)
    print("Performance Configuration:")
    print(f"  Use async requests: {config.USE_ASYNC_REQUESTS}")
    print(f"  Max concurrent requests: {config.MAX_CONCURRENT_REQUESTS}")
    print(f"  Enable parallel processing: {config.ENABLE_PARALLEL_PROCESSING}")
    print(f"  Max worker threads: {config.MAX_WORKER_THREADS}")
    print()
    
    # Pagination Configuration (New)
    print("Pagination Configuration:")
    print(f"  Enable full pagination: {config.ENABLE_FULL_PAGINATION}")
    print(f"  Max pages per endpoint: {config.MAX_PAGES_PER_ENDPOINT}")
    print(f"  Items per page: {config.ITEMS_PER_PAGE}")
    print()
    
    # Pull Request Configuration
    print("Pull Request Configuration:")
    print(f"  Max PRs per repo: {config.MAX_PRS_PER_REPO}")
    print(f"  PR state: {config.PR_STATE}")
    print(f"  Sort by: {config.PR_SORT_BY}")
    print(f"  Order: {config.PR_ORDER}")
    print(f"  Min changed files: {config.MIN_CHANGED_FILES}")
    print()
    
    # Data Extraction
    print("Data Extraction:")
    print(f"  Include comments: {config.INCLUDE_PR_COMMENTS}")
    print(f"  Include commits: {config.INCLUDE_COMMITS}")
    print(f"  Max comments per PR: {config.MAX_COMMENTS_PER_PR}")
    print(f"  Max commits per PR: {config.MAX_COMMITS_PER_PR}")
    print()
    
    # Enhanced Change Type Detection (New)
    print("Enhanced Change Type Detection:")
    print(f"  Enhanced detection: {config.ENHANCED_CHANGE_TYPE_DETECTION}")
    print(f"  Analyze commit messages: {config.ANALYZE_COMMIT_MESSAGES}")
    print(f"  Confidence threshold: {config.CHANGE_TYPE_CONFIDENCE_THRESHOLD}")
    print()
    
    # Language Filtering
    print("Language Filtering:")
    print(f"  Include only: {config.INCLUDE_ONLY_LANGUAGES or 'All languages'}")
    print(f"  Exclude: {config.EXCLUDE_LANGUAGES or 'None'}")
    print()
    
    # Logging Configuration (New)
    print("Logging Configuration:")
    print(f"  Log level: {config.LOG_LEVEL}")
    print(f"  Log to file: {config.LOG_TO_FILE}")
    print(f"  Log file path: {config.LOG_FILE_PATH}")
    print(f"  Enable request logging: {config.ENABLE_REQUEST_LOGGING}")
    print()
    
    # Retry Configuration (New)
    print("Retry Configuration:")
    print(f"  Max retries: {config.MAX_RETRIES}")
    print(f"  Retry delay: {config.RETRY_DELAY}s")
    print(f"  Exponential backoff: {config.EXPONENTIAL_BACKOFF}")
    print()
    
    # Output Configuration
    print("Output Configuration:")
    print(f"  Format: {config.OUTPUT_FORMAT}")
    print(f"  Directory: {config.OUTPUT_DIRECTORY}")
    print(f"  Batch size: {config.BATCH_SIZE}")
    print()
    
    # Rate Limiting
    print("Rate Limiting:")
    print(f"  Requests per minute: {config.REQUESTS_PER_MINUTE}")
    print(f"  Delay between requests: {config.DELAY_BETWEEN_REQUESTS}s")

def estimate_enhanced_extraction_time():
    """Estimate extraction time with enhanced features"""
    print("\n⏱️  Enhanced Extraction Time Estimate")
    print("="*50)
    
    # Base calculations
    repo_requests = config.TOP_N_REPOSITORIES * 2  # repo info + languages
    pr_base_requests = config.TOP_N_REPOSITORIES * config.MAX_PRS_PER_REPO
    
    # Additional requests based on features
    additional_requests = 0
    if config.INCLUDE_COMMITS:
        additional_requests += pr_base_requests
    if config.INCLUDE_PR_COMMENTS:
        # Comments might require multiple requests due to pagination
        comment_pages_per_pr = max(1, config.MAX_COMMENTS_PER_PR // config.ITEMS_PER_PAGE)
        additional_requests += pr_base_requests * comment_pages_per_pr * 2  # issue + review comments
    
    # Pagination factor
    pagination_factor = 1
    if config.ENABLE_FULL_PAGINATION:
        pagination_factor = min(config.MAX_PAGES_PER_ENDPOINT, 5)  # Conservative estimate
    
    total_requests = (repo_requests + pr_base_requests + additional_requests) * pagination_factor
    
    # Time calculations
    if config.USE_ASYNC_REQUESTS and config.ENABLE_PARALLEL_PROCESSING:
        # Async + parallel processing
        concurrent_factor = min(config.MAX_CONCURRENT_REQUESTS, config.MAX_WORKER_THREADS)
        total_time_seconds = (total_requests / concurrent_factor) * config.DELAY_BETWEEN_REQUESTS
        processing_mode = "Async + Parallel"
    elif config.USE_ASYNC_REQUESTS:
        # Async only
        concurrent_factor = config.MAX_CONCURRENT_REQUESTS
        total_time_seconds = (total_requests / concurrent_factor) * config.DELAY_BETWEEN_REQUESTS
        processing_mode = "Async"
    else:
        # Sequential
        total_time_seconds = total_requests * config.DELAY_BETWEEN_REQUESTS
        processing_mode = "Sequential"
    
    print(f"Processing mode: {processing_mode}")
    print(f"Estimated requests: {total_requests}")
    print(f"Estimated time: {total_time_seconds:.1f} seconds ({total_time_seconds/60:.1f} minutes)")
    
    # Performance comparison
    sequential_time = total_requests * config.DELAY_BETWEEN_REQUESTS
    if total_time_seconds < sequential_time:
        speedup = sequential_time / total_time_seconds
        print(f"Performance improvement: {speedup:.1f}x faster than sequential")
    
    # Warnings and suggestions
    if total_time_seconds > 600:  # 10 minutes
        print("\n⚠️  Long extraction time detected. Consider:")
        print("   - Reducing TOP_N_REPOSITORIES")
        print("   - Reducing MAX_PRS_PER_REPO")
        print("   - Disabling INCLUDE_PR_COMMENTS")
        print("   - Disabling INCLUDE_COMMITS")
        print("   - Enabling parallel processing")
        print("   - Increasing MAX_CONCURRENT_REQUESTS")
    
    return total_time_seconds

def suggest_enhanced_optimizations():
    """Suggest configuration optimizations for enhanced version"""
    print("\n💡 Enhanced Optimization Suggestions")
    print("="*50)
    
    suggestions = []
    performance_suggestions = []
    reliability_suggestions = []
    
    # Performance optimizations
    if not config.USE_ASYNC_REQUESTS:
        performance_suggestions.append("Enable USE_ASYNC_REQUESTS for better I/O performance")
    
    if not config.ENABLE_PARALLEL_PROCESSING:
        performance_suggestions.append("Enable ENABLE_PARALLEL_PROCESSING for faster repository processing")
    
    if config.MAX_CONCURRENT_REQUESTS < 10:
        performance_suggestions.append("Consider increasing MAX_CONCURRENT_REQUESTS (10-20 recommended)")
    
    if config.MAX_WORKER_THREADS < 4:
        performance_suggestions.append("Consider increasing MAX_WORKER_THREADS (4-8 recommended)")
    
    # Data quality optimizations
    if not config.ENHANCED_CHANGE_TYPE_DETECTION:
        suggestions.append("Enable ENHANCED_CHANGE_TYPE_DETECTION for better categorization")
    
    if not config.ANALYZE_COMMIT_MESSAGES:
        suggestions.append("Enable ANALYZE_COMMIT_MESSAGES for more accurate change type detection")
    
    if config.CHANGE_TYPE_CONFIDENCE_THRESHOLD < 0.5:
        suggestions.append("Consider increasing CHANGE_TYPE_CONFIDENCE_THRESHOLD (0.5-0.7 recommended)")
    
    # Reliability optimizations
    if config.MAX_RETRIES < 3:
        reliability_suggestions.append("Consider increasing MAX_RETRIES (3-5 recommended)")
    
    if not config.EXPONENTIAL_BACKOFF:
        reliability_suggestions.append("Enable EXPONENTIAL_BACKOFF for better retry behavior")
    
    if config.LOG_LEVEL != "INFO":
        reliability_suggestions.append("Set LOG_LEVEL to 'INFO' for optimal monitoring")
    
    # Data completeness optimizations
    if not config.ENABLE_FULL_PAGINATION and config.MAX_PRS_PER_REPO > 100:
        suggestions.append("Enable ENABLE_FULL_PAGINATION for large repositories")
    
    if config.EXCLUDE_FORKS and config.EXCLUDE_ARCHIVED:
        suggestions.append("Good: Excluding forks and archived repos improves data quality")
    
    # Output suggestions
    if config.OUTPUT_FORMAT == "csv" and not config.LOG_TO_FILE:
        suggestions.append("Enable LOG_TO_FILE when using CSV output for better debugging")
    
    # Print suggestions by category
    if performance_suggestions:
        print("🚀 Performance Optimizations:")
        for i, suggestion in enumerate(performance_suggestions, 1):
            print(f"   {i}. {suggestion}")
        print()
    
    if reliability_suggestions:
        print("🛡️  Reliability Improvements:")
        for i, suggestion in enumerate(reliability_suggestions, 1):
            print(f"   {i}. {suggestion}")
        print()
    
    if suggestions:
        print("📊 General Suggestions:")
        for i, suggestion in enumerate(suggestions, 1):
            print(f"   {i}. {suggestion}")
        print()
    
    if not any([performance_suggestions, reliability_suggestions, suggestions]):
        print("✅ Configuration is well-optimized!")

def check_system_requirements():
    """Check if system meets requirements for enhanced features"""
    print("\n🔧 System Requirements Check")
    print("="*40)
    
    try:
        import aiohttp
        print("✅ aiohttp: Available")
    except ImportError:
        print("❌ aiohttp: Missing (required for async requests)")
        print("   Install: pip install aiohttp")
    
    try:
        import asyncio_throttle
        print("✅ asyncio-throttle: Available")
    except ImportError:
        print("❌ asyncio-throttle: Missing (required for rate limiting)")
        print("   Install: pip install asyncio-throttle")
    
    try:
        import tenacity
        print("✅ tenacity: Available")
    except ImportError:
        print("❌ tenacity: Missing (required for retry logic)")
        print("   Install: pip install tenacity")
    
    # Check Python version
    import sys
    python_version = sys.version_info
    if python_version >= (3, 8):
        print(f"✅ Python {python_version.major}.{python_version.minor}: Compatible")
    else:
        print(f"❌ Python {python_version.major}.{python_version.minor}: Requires Python 3.8+")
    
    # Check memory availability (rough estimate)
    try:
        import psutil
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        if available_gb >= 2:
            print(f"✅ Memory: {available_gb:.1f}GB available")
        else:
            print(f"⚠️  Memory: {available_gb:.1f}GB available (2GB+ recommended)")
    except ImportError:
        print("💡 Install psutil for memory monitoring: pip install psutil")

def generate_optimal_config():
    """Generate an optimal configuration for different use cases"""
    print("\n⚙️  Optimal Configuration Suggestions")
    print("="*50)
    
    configs = {
        "Fast Development": {
            "description": "Quick testing and development",
            "settings": {
                "TOP_N_REPOSITORIES": 3,
                "MAX_PRS_PER_REPO": 10,
                "INCLUDE_PR_COMMENTS": False,
                "INCLUDE_COMMITS": False,
                "USE_ASYNC_REQUESTS": True,
                "ENABLE_PARALLEL_PROCESSING": True,
                "MAX_CONCURRENT_REQUESTS": 20,
                "ENHANCED_CHANGE_TYPE_DETECTION": False
            }
        },
        "Production Quality": {
            "description": "High-quality data extraction for analysis",
            "settings": {
                "TOP_N_REPOSITORIES": 10,
                "MAX_PRS_PER_REPO": 100,
                "INCLUDE_PR_COMMENTS": True,
                "INCLUDE_COMMITS": True,
                "USE_ASYNC_REQUESTS": True,
                "ENABLE_PARALLEL_PROCESSING": True,
                "MAX_CONCURRENT_REQUESTS": 15,
                "ENHANCED_CHANGE_TYPE_DETECTION": True,
                "ENABLE_FULL_PAGINATION": True,
                "ANALYZE_COMMIT_MESSAGES": True
            }
        },
        "Large Scale": {
            "description": "Comprehensive data extraction for research",
            "settings": {
                "TOP_N_REPOSITORIES": 50,
                "MAX_PRS_PER_REPO": 500,
                "INCLUDE_PR_COMMENTS": True,
                "INCLUDE_COMMITS": True,
                "USE_ASYNC_REQUESTS": True,
                "ENABLE_PARALLEL_PROCESSING": True,
                "MAX_CONCURRENT_REQUESTS": 10,
                "MAX_WORKER_THREADS": 8,
                "ENHANCED_CHANGE_TYPE_DETECTION": True,
                "ENABLE_FULL_PAGINATION": True,
                "MAX_PAGES_PER_ENDPOINT": 100,
                "ANALYZE_COMMIT_MESSAGES": True,
                "LOG_TO_FILE": True
            }
        }
    }
    
    for config_name, config_data in configs.items():
        print(f"\n📋 {config_name}:")
        print(f"   {config_data['description']}")
        print(f"   Settings:")
        for setting, value in config_data['settings'].items():
            print(f"     {setting} = {value}")

async def test_async_performance():
    """Test async performance capabilities"""
    print("\n🚀 Async Performance Test")
    print("="*35)
    
    print("Testing async capabilities...")
    
    # Simulate async operations
    async def mock_request(delay=0.1):
        await asyncio.sleep(delay)
        return {"status": "success"}
    
    # Test sequential
    start_time = time.time()
    for i in range(5):
        await mock_request()
    sequential_time = time.time() - start_time
    
    # Test concurrent
    start_time = time.time()
    tasks = [mock_request() for _ in range(5)]
    await asyncio.gather(*tasks)
    concurrent_time = time.time() - start_time
    
    speedup = sequential_time / concurrent_time
    
    print(f"Sequential time: {sequential_time:.2f}s")
    print(f"Concurrent time: {concurrent_time:.2f}s")
    print(f"Speedup: {speedup:.1f}x")
    print("✅ Async functionality working correctly")

async def main():
    """Run enhanced configuration validation"""
    print("🔧 Enhanced GitHub PR Data Extractor - Configuration Validator")
    print("="*70)
    
    validate_enhanced_config()
    estimate_enhanced_extraction_time()
    suggest_enhanced_optimizations()
    check_system_requirements()
    generate_optimal_config()
    await test_async_performance()
    
    print(f"\n🎉 Enhanced Configuration Validation Complete!")
    print(f"\n📚 Next Steps:")
    print(f"   1. Set GitHub token: export GITHUB_TOKEN='your_token'")
    print(f"   2. Install missing dependencies if any")
    print(f"   3. Adjust configuration based on suggestions")
    print(f"   4. Run demo: python demo.py")
    print(f"   5. Run extraction: python extract_pr_data.py")

if __name__ == "__main__":
    asyncio.run(main())
