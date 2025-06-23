#!/usr/bin/env python3
"""
PR Data Analysis Script

This script analyzes PR data from JSON files with prefix 'enhanced_pr_data' in the output directory.
It counts total PRs, unique PRs by ID, and PRs with files, comments, and commits.
Uses streaming JSON parser and incremental processing to minimize memory usage.
"""

import os
import glob
import gc
import json
import ijson  # Streaming JSON parser
from collections import Counter

def analyze_pr_data(directory="./output"):
    """
    Analyze PR data from JSON files in the specified directory.
    Uses memory-efficient streaming to prevent OOM errors.
    
    Args:
        directory (str): Path to directory containing PR data files
        
    Returns:
        dict: Statistics about the PRs
    """
    # Stats to collect
    stats = {
        "total_prs": 0,
        "unique_prs": set(),
        "prs_with_files": 0,
        "prs_with_comments": 0,
        "prs_with_commits": 0,
        "prs_with_all_components": 0,
        "repos": Counter(),
        "authors": Counter()
    }
    
    # Find all enhanced_pr_data files
    file_pattern = os.path.join(directory, "enhanced_pr_data*.json")
    data_files = glob.glob(file_pattern)
    
    print(f"Found {len(data_files)} data files to analyze")
    
    # Process each file
    for file_path in data_files:
        print(f"Processing {os.path.basename(file_path)}...")
        
        try:
            # Check if we need to clear memory
            if len(stats["unique_prs"]) > 100000:
                # Convert to count and clear the set to save memory
                print("Memory optimization: Converting unique_prs set to count")
                stats["unique_pr_count"] = len(stats["unique_prs"])
                stats["unique_prs"] = set()
                gc.collect()
            
            # Use streaming JSON parser to process file incrementally
            with open(file_path, 'r', encoding='utf-8') as f:
                # First count total PRs
                pr_count = 0
                for _ in ijson.items(f, 'data.item'):
                    pr_count += 1
                stats["total_prs"] += pr_count
                
                # Rewind file and process PRs
                f.seek(0)
                
                # Process each PR one at a time without loading entire file
                for pr in ijson.items(f, 'data.item'):
                    # Count unique PRs by ID
                    if "id" in pr:
                        stats["unique_prs"].add(pr["id"])
                    
                    # Count PRs with specific components
                    has_files = bool(pr.get("files", []))
                    has_comments = bool(pr.get("pr_comments", []))
                    has_commits = bool(pr.get("commits", {}))
                    
                    if has_files:
                        stats["prs_with_files"] += 1
                    if has_comments:
                        stats["prs_with_comments"] += 1
                    if has_commits:
                        stats["prs_with_commits"] += 1
                    
                    # Count PRs with all components
                    if has_files and has_comments and has_commits:
                        stats["prs_with_all_components"] += 1
                    
                    # Collect repository statistics
                    if "repository_full_name" in pr:
                        stats["repos"][pr["repository_full_name"]] += 1
                    
                    # Collect author statistics
                    if "author" in pr:
                        stats["authors"][pr["author"]] += 1
                    
                    # Free memory for this PR
                    del pr
                    
                # Force garbage collection after each file
                gc.collect()
                    
        except json.JSONDecodeError:
            print(f"Error: Could not parse {file_path} as JSON")
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Combine any saved unique_pr_count with current set
    if "unique_pr_count" in stats:
        stats["unique_pr_count"] += len(stats["unique_prs"])
    else:
        stats["unique_pr_count"] = len(stats["unique_prs"])
    
    return stats

def print_report(stats):
    """
    Print a formatted report of PR statistics.
    
    Args:
        stats (dict): Statistics about the PRs
    """
    print("\n" + "="*50)
    print("PR DATA ANALYSIS REPORT")
    print("="*50)
    
    print(f"\nTotal PRs found: {stats['total_prs']}")
    print(f"Unique PRs by ID: {stats['unique_pr_count']}")
    
    print("\nPRs with components:")
    print(f"  - With files: {stats['prs_with_files']} ({percentage(stats['prs_with_files'], stats['total_prs'])}%)")
    print(f"  - With comments: {stats['prs_with_comments']} ({percentage(stats['prs_with_comments'], stats['total_prs'])}%)")
    print(f"  - With commits: {stats['prs_with_commits']} ({percentage(stats['prs_with_commits'], stats['total_prs'])}%)")
    print(f"  - With all components: {stats['prs_with_all_components']} ({percentage(stats['prs_with_all_components'], stats['total_prs'])}%)")
    
    print("\nTop 5 repositories by PR count:")
    for repo, count in stats["repos"].most_common(5):
        print(f"  - {repo}: {count} PRs")
    
    print("\nTop 5 authors by PR count:")
    for author, count in stats["authors"].most_common(5):
        print(f"  - {author}: {count} PRs")

def percentage(part, whole):
    """Calculate percentage and format to 2 decimal places"""
    return round((part / whole) * 100, 2) if whole else 0

if __name__ == "__main__":
    print("Analyzing PR data files...")
    stats = analyze_pr_data()
    print_report(stats)
    
    # Create a set from unique_prs for memory efficiency before exiting
    if "unique_prs" in stats:
        stats["unique_prs"] = list(stats["unique_prs"])
    
    # Optional: Save results to file
    with open("pr_stats_results.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)
    
    print("\nAnalysis complete. Results saved to pr_stats_results.json")
