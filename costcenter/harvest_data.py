import argparse
import requests
import re
import yaml
import snowflake.connector
import os
import base64
from typing import List, Optional, Dict
from upload2Snowflake import update_snowflake
import fnmatch

BASE_URL = "https://api.github.com" # (replace with)GitHub API base URL
# MAX_REPOS = 100  # Replace with your desired maximum number of repositories

class ScanRepo:
    def __init__(self, token: str):    
        self.base_url = BASE_URL.rstrip('/')
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json'
        }

    def get_repos(self, org_name: str) -> List[Dict]:
        # Get non-archived repositories
        repos = []
        page = 1
        while True:
            # url = f"{self.base_url}/orgs/{org_name}/repos"
            url = f"{self.base_url}/users/{org_name}/repos"
            params = {
                'page': page,
                'per_page': 100,
                'type': 'all'  # Get all repos
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            # filter through non-archived repos only
            current_repos = [repo for repo in response.json() if not repo.get('archived', False)]
            
            if not current_repos:
                break
                
            repos.extend(current_repos)
            ### capacity limiter for testing purposes
            # if len(repos) >= MAX_REPOS:
            #     repos = repos[:MAX_REPOS]
            #     break
            ### capacity limiter for testing purposes 
            page += 1
            
        return repos

    ### Recursively search for files in repository
    def locate_file(self, org_name: str, repo_name: str, file_patterns: List[str], path: str = '', depth: int = 0, max_depth: int = 10, exclude_folders: List[str] = None) -> List[Dict]:
        
        results = []  # Initialize results list

        # excluding certain folder(s) from the search
        if exclude_folders is None:
            # list folder name(s); allows wildcards
            exclude_folders = ['.git*', 'src', 'docs'] 

        # print(f"Displaying depth(max:{max_depth}): {depth} | Repo: {org_name} | dir-path: {path}")
        # this limits the search recursion depth of sub-directories 
        if depth > max_depth:
            return results
                
        url = f"{self.base_url}/repos/{org_name}/{repo_name}/contents/{path}"
        response = requests.get(url, headers=self.headers)
        
        if response.status_code == 403:
            print(f"Access forbidden for URL: {url}")
            return results
        
        if response.status_code == 404:
            return results
            
        response.raise_for_status()
        contents = response.json()
        
        # If contents is not a list, it's a file
        if not isinstance(contents, list):
            contents = [contents]

        # 1. check files in current directory
        for item in contents:
            if item['type'] == 'file' and any(item['name'].lower() == pattern.lower() for pattern in file_patterns):
                results.append ({
                    'filename': item['name'],
                    'path': item['path'],
                    'download_url': item['download_url'],
                    'content': self.get_file_content(item['url'])
                })

        # 2. recursively check subdirectories
        for item in contents:
            if item['type'] == 'dir' and not any(fnmatch.fnmatch(item['name'], pattern) for pattern in exclude_folders):
                sub_results = self.locate_file(org_name, repo_name, file_patterns, item['path'], depth + 1, max_depth, exclude_folders)
                results.extend(sub_results)

        return results
    
    # Get decoded content of a file
    def get_file_content(self, url: str) -> Optional[str]:
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        content = response.json()
        if content.get('encoding') == 'base64':
            return base64.b64decode(content['content']).decode('utf-8')
        return content.get('content')

    # Search for costcenter.yaml file(s) that reside in any sub-dir; and extract expense-id from it
    def get_expense_id(self, content: str) -> Optional[str]:
        
        if not content:
            return []        
        try:
            """ 
            Goes through content of costcenter.yaml file by converting its content
            to python object to extract exp-id. ***Safe standard YAML***
            """
            yaml_content = yaml.safe_load(content)
            expense_ids = []

            if isinstance(yaml_content, dict):
                ###  Try variations of exp-id keys  ###
                exp_id_keys = ['exp-id', 'expid', 'EXP-ID']
                for key in exp_id_keys:
                    if key in yaml_content:
                        value = yaml_content[key]
                        return str(value)
                
                ### Recursive search through nested sub-directories ###
                def search_exp_id(data):
                    results = []
                    if isinstance(data, dict):
                        for key, value in data.items():
                            if key.lower() in [k.lower() for k in exp_id_keys]:
                                results.append(str(value))
                            if isinstance(value, (dict, list)):
                                results.extend(search_exp_id(value))
                                
                    elif isinstance(data, list):
                        for item in data:
                            results.extend(search_exp_id(item))
                            
                    return results
            
                expense_ids = search_exp_id(yaml_content)
            
            return expense_ids
        
        except yaml.YAMLError as e:
            return []   
    
    # Search for exp-id pattern in prod-owner.md file content
    def get_prod_owner_exp_id(self, content: str) -> Optional[str]:
        
        if not content:
            return []
            
        try:
            # RegeEx pattern - match CC or cc followed by 7 digits
            pattern = r'(?i)CC\d{7}'
            matches = re.findall(pattern, content)
        
            # Use a dictionary to store unique exp-ids in a case-insensitive manner
            unique_exp_ids = {}
            for match in matches:
                lower_match = match.lower()
                if lower_match not in unique_exp_ids:
                    unique_exp_ids[lower_match] = match
            
            # Return the values of the dictionary as a list
            return list(unique_exp_ids.values())
                        
        except Exception as e:
            print(f"Error searching prod-owner.md content: {e}")
            return []


def main():
    parser = argparse.ArgumentParser(description='Scan GitHub repositories')
    parser.add_argument('--orgs', nargs='+', required=True, help='organization/user names')
    parser.add_argument('--token', required=True, help='GitHub token')
    
    args = parser.parse_args()
    
    scanner = ScanRepo(args.token)
    all_repo_data = []
    
    for org_name in args.orgs:
        print(f"\nScanning Org/User: {org_name}")
        repos = scanner.get_repos(org_name)
        
        for repo in repos:
            repo_name = repo['name']
            repo_url = repo['html_url']
            
            print(f"\nOrg. name: {org_name}")
            print(f"Repository: {repo_name}")
            print(f"URL: {repo_url}")
                        
            # Check for costcenters
            costcenter_info_list = scanner.locate_file(
                org_name, 
                repo_name, 
                ['costcenter.yaml', 'costcenter.yml'] # variations of costcenter.yaml
            )
            
            # Using function: get_expense_id to search for exp-id in costcenter yaml file(s)
            for costcenter_info in costcenter_info_list:
                print(f"Costcenter YAML path: {costcenter_info['path']}")
                expense_ids = scanner.get_expense_id(costcenter_info['content'])
                if expense_ids: ### if expense_ids is not None
                    for expense_id in expense_ids:
                        print(f"File name: {costcenter_info['filename']} | Expense ID: {expense_id}")
                        # Compile Snowflake data for costcenter.yaml 
                        repo_data = {
                            'org_name': org_name,
                            'repo_name': repo_name,
                            'repo_url': repo_url,
                            'exp_id': expense_id,
                            'costcenter_path': costcenter_info['path'],
                            'prod_owner_path': None,
                            'filename': costcenter_info['filename'],
                            'type': 'CostCenter'
                        }
                        all_repo_data.append(repo_data)
                else:
                    pass  # Do nothing if expense_ids is None
            else:
                pass
                       
            # Check for prod-owner.md files
            prod_owner_info_list = scanner.locate_file(
                org_name,
                repo_name,
                ['prod-owner.md', 'PROD-OWNER.md', 'PROD-OWNER.MD']
            )
            
            # if prod-owner.md exists, prints it out
            for prod_owner_info in prod_owner_info_list:
                print(f"Product Owner MD path: {prod_owner_info['path']}")
                # calling function to search exp_id in prod-owner.md
                prod_owner_exp_ids = scanner.get_prod_owner_exp_id(prod_owner_info['content'])
                for prod_owner_exp_id in prod_owner_exp_ids:
                    print(f"File name: {prod_owner_info['filename']} | Expense ID: {prod_owner_exp_id}")
                    # Compile Snowflake data for prod-owner.md
                    repo_data = {
                        'org_name': org_name,
                        'repo_name': repo_name,
                        'repo_url': repo_url,
                        'exp_id': prod_owner_exp_id,
                        'costcenter_path': None,
                        'prod_owner_path': prod_owner_info['path'],
                        'filename': prod_owner_info['filename'],
                        'type': 'Product-Ownership'
                    }
                    all_repo_data.append(repo_data)                            
              
            ### Add unique record into Snowflake for repos without any of the other 2 records
            if not any([costcenter_info_list, prod_owner_info_list]):
                repo_data = {
                    'org_name': org_name,
                    'repo_name': repo_name,
                    'repo_url': repo_url,
                    'exp_id': None,
                    'costcenter_path': None,
                    'prod_owner_path': None,
                    'filename': None,
                    'type': None
                }
            all_repo_data.append(repo_data)

    ### Compile, Insert/Update into Snowflake ###
    update_snowflake(all_repo_data)

if __name__ == '__main__':
    main()
