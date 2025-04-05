import requests
import argparse

"""
    This script compiles deployment targets that are in Healthy & Healthy with warnings states only
    To run this script, use the following command:
        'python retrieve-octo-servers.py --apikey= XXXXXXXXX'
"""

def get_octopus_machines(api_key):
    #Octopus URL
    octopus_url = 'https://example.octopus.com'

    # Octopus API endpoint to get all machines
    endpoint = f'{octopus_url}/api/machines/all'

    # Headers for authentication
    headers = {
        'X-Octopus-ApiKey': api_key
    }

    # Make the GET request
    response = requests.get(endpoint, headers=headers)

    # Check if the GET request was successful
    if response.status_code == 200:
        servers = response.json()
        exclude_status = ['Unhealthy', 'Unavailable']
        server_count = 0
        for server in servers:
            # Includes healthy & healthy with warnings servers only            
            if not (server['IsDisabled'] or server['HealthStatus'] in exclude_status):
                print((server['Name']))
                server_count += 1                
        print(f'Healthy Server Count: {server_count}')
    else:
        print(f"Failed to retrieve servers list: {response.status_code}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Get Octopus deployment targets')
    parser.add_argument('--apikey', required=True, help='Octopus API token')
    args = parser.parse_args()

    get_octopus_machines(args.apikey)
