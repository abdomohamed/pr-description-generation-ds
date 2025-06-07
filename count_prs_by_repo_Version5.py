import os
import json
from collections import defaultdict
import re
from tkinter import N

def count_prs_by_repository(directory_path):
    """
    Iterate over JSON files matching the pattern 'enhanced_pr_data_*.json' in the given directory 
    and count PRs by repository name.
    
    Args:
        directory_path (str): Path to the directory containing JSON files
        
    Returns:
        dict: A dictionary with repository names as keys and PR counts as values
    """
    # Dictionary to store counts with repository name as key
    repo_counts = {}
    
    # Pattern for matching files
    file_pattern = re.compile(r'^enhanced_pr_data_(.*).json$')
    
    # Track processed files
    processed_files = 0
    
    # Iterate through all files in the directory
    for filename in os.listdir(directory_path):
        # Only process files matching the enhanced_pr_data_*.json pattern
        if file_pattern.match(filename):
            file_path = os.path.join(directory_path, filename)
            
            try:
                # Open and parse the JSON file
                with open(file_path, 'r', encoding='utf-8') as file:
                    json_content = json.load(file)
                    
                    # Check if the JSON has a 'data' field containing an array of PRs
                    if 'data' in json_content and isinstance(json_content['data'], list):
                        pr_list = json_content['data']
                        
                        # Process each PR in the data array
                        for pr in pr_list:
                            repo_name = extract_repo_name(pr)
                            if(repo_name not in repo_counts):
                                repo_counts[repo_name] = set()
                            
                            repo_counts[repo_name].add(pr['id'])
                        
                    else:
                        # Handle case where the file might not have the expected structure
                        print(f"Warning: {filename} does not have the expected 'data' array structure")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
    
    if processed_files == 0:
        print(f"Warning: No files matching 'enhanced_pr_data_*.json' pattern found in {directory_path}")
        
    return repo_counts

def extract_repo_name(pr_data):
    """
    Extract repository name from PR data according to the actual structure in the repository.
    
    Args:
        pr_data (dict): PR data from JSON
        
    Returns:
        str: Repository name or None if not found
    """
    
    # Check for repository field directly
    if 'repository_name' in pr_data:
        return pr_data['repository_name']
    
    return None

def main():
    # Path to directory containing JSON files
    directory_path = 'output'
    
    # Count PRs by repository
    repo_counts = count_prs_by_repository(directory_path)
    
    
    
    if repo_counts:
        # Print results
        print("\nPR counts by repository:")
        print("------------------------")
        total_prs = 0
        for repo, pr_ids in repo_counts.items():
            total_prs += len(pr_ids)
            print(f"{repo}: {len(pr_ids)} PRs")
        
        # Summary
        total_repos = len(repo_counts)
        print("\nSummary:")
        print(f"Total repositories: {total_repos}")
        print(f"Total PRs: {total_prs}")
    else:
        print("No PR data found.")

if __name__ == "__main__":
    print(f"PR Count Script - Run by {os.environ.get('USER', 'abdomohamed')} on {os.popen('date -u +%Y-%m-%d\ %H:%M:%S').read().strip()}")
    main()