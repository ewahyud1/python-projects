import os
import boto3
import json
import snowflake.connector
from typing import List, Dict

# Connects to AWS Secrets Manager to retrieve Snowflake credentials
def retrieve_secrets(secret_name, region):
    
    # initialize Secrets Manager boto3 client
    client = boto3.client('secretsmanager', region_name=region)
    
    try:
        get_secret = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return
    
    secret = get_secret['SecretString']
    secret_dict = json.loads(secret)

    # setting secrets environment variables
    for key, value in secret_dict.items():
        os.environ[key] = value
  

##### Connect to Snowflake and update table records #####
def update_snowflake(data: List[Dict]):

    ## Retrieve Snowflake credentials from AWS Secrets Manager
    retrieve_secrets('/path-to/secrets', 'ap-southeast-1') 
    
    """Update Snowflake table with repository information."""
    conn = snowflake.connector.connect(
        user= os.getenv('USER'),
        account= f"{os.getenv('ACCOUNT')}.{os.getenv('REGION')}",
        role= os.getenv('ROLE'),
        warehouse= os.getenv('WAREHOUSE'),
        database= os.getenv('DATABASE'),
        region= os.getenv('REGION'),
        password= os.getenv('PASSWD'),
        schema= os.getenv('DB_SCHEMA')
    )
        
    try:
        cursor = conn.cursor()
        for repo_info in data:
            # Ensure all values are of supported types | convert all to string
            org_name = str(repo_info['org_name'])
            repo_name = str(repo_info['repo_name'])
            repo_url = str(repo_info['repo_url'])
            costcenter_id = str(repo_info.get('exp_id') or '')
            costcenter_path = str(repo_info.get('costcenter_path') or '')
            prod_owner_path = str(repo_info.get('prod_owner_path') or '')
            filename = str(repo_info.get('filename') or '')
            type = str(repo_info.get('type') or '')

            # checks if record exists in Snowflake DB
            # using the DB Table Columns: GITHUB_ORG, GITHUB_REPO_NAME, REPO_URL, COSTCENTER_ID, COSTCENTER_PATH, PROD_OWNER_PATH, FILENAME, TYPE
            cursor.execute("""
                SELECT REPO_URL FROM GITHUB_DATA
                WHERE GITHUB_ORG = %s
                AND GITHUB_REPO_NAME = %s
                AND REPO_URL = %s
                AND COSTCENTER_ID = %s
                AND COSTCENTER_PATH = %s
                AND PROD_OWNER_path = %s
                AND FILENAME = %s
                AND TYPE = %s
            """, (org_name, repo_name, repo_url, costcenter_id, costcenter_path, prod_owner_path, filename, type))

            exists = cursor.fetchone()
            # Compare records - current_values (in Snowflake) vs new_values (from newly harvested data)
            if exists:
                current_values = {
                    'repo_url': exists[0],
                    'costcenter_path': exists[1],
                    'prod_owner_path': exists[2],
                    'filename': exists[3],
                    'type': exists[4]
                }
                new_values = {
                    'repo_url': repo_url,
                    'costcenter_path': costcenter_path,
                    'prod_owner_path': prod_owner_path,
                    'filename': filename,
                    'type': type
                }
                # update only if record exist & values have changed!
                if current_values != new_values:
                    query = """
                        UPDATE GITHUB_DATA
                        SET REPO_URL = %s,
                            COSTCENTER_PATH = %s,
                            PROD_OWNER_PATH = %s,
                            FILENAME = %s,
                            TYPE = %s
                        WHERE GITHUB_ORG = %s
                        AND GITHUB_REPO_NAME = %s
                        AND REPO_URL = %s
                        AND COSTCENTER_ID = %s
                        AND COSTCENTER_PATH = %s
                        AND PROD_OWNER_path = %s
                        AND FILENAME = %s
                        AND TYPE = %s
                    """ 
                    params = (repo_url, costcenter_path, prod_owner_path, filename, type, org_name, repo_name, repo_url, costcenter_id, costcenter_path, prod_owner_path, filename, type)
                    print("Update Query:", query)
                    print("Update parameters:", params)
                    cursor.execute(query, params)
                    conn.commit()
            else:
                # Insert new records if does not already exists
                query = """
                    INSERT INTO GITHUB_DATA (
                        GITHUB_ORG,
                        GITHUB_REPO_NAME,
                        REPO_URL,
                        COSTCENTER_ID,
                        COSTCENTER_PATH,
                        PROD_OWNER_path,
                        FILENAME,
                        TYPE
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = (org_name, repo_name, repo_url, costcenter_id, costcenter_path, prod_owner_path, filename, type)                
                print("Insert Query:", query)
                print("Insert parameters:", params)
                cursor.execute(query, params)
                conn.commit()
    finally:
        conn.close()
    
    pass
