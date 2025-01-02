import boto3
import configparser
import json
from botocore.exceptions import ClientError
import pandas as pd
import subprocess

# Load configuration from dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

# AWS credentials and configuration
AWS_KEY = config.get('AWS', 'KEY')
AWS_SECRET = config.get('AWS', 'SECRET')
REGION = config.get('AWS', 'REGION')

# Redshift cluster configuration
CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
NUM_NODES = int(config.get('CLUSTER', 'NUM_NODES'))
DB_NAME = config.get('CLUSTER', 'DB_NAME')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = int(config.get('CLUSTER', 'DB_PORT'))
CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
IAM_ROLE_NAME = config.get('IAM_ROLE', 'ROLE_NAME')

# Create clients
iam = boto3.client('iam', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, region_name=REGION)
redshift = boto3.client('redshift', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, region_name=REGION)

def delete_infra():
    """
    Start delete_infra.py script to clean up the infrastructure
    """
    try:
        subprocess.run(["python", "delete_infra.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to execute delete_infra.py: {e}")

def create_IAM_role():
    """
    Create an IAM role
    """
    try:
        print("Creating a new IAM Role")
        iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps({
                'Statement': [{
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}
                }],
                'Version': '2012-10-17'
            })
        )
        print("Created IAM role successfully.")
    except ClientError as e:
        print(e)
        raise

def attach_policy():
    """
    Attach the necessary policy to the IAM role
    """
    try:
        print("Attaching Policy")
        iam.attach_role_policy(
            RoleName=IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']
        print("Policy attached")
    except ClientError as e:
        print(e)
        raise

def get_IAM_role_arn():
    """
    Get the IAM role ARN

    Returns:
        roleArn: IAM role ARN
    """
    try:
        print("Getting the IAM role ARN")
        roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
        print(f"IAM role ARN: {roleArn}")
        return roleArn
    except ClientError as e:
        print(e)
        raise

def create_redshift_cluster(roleArn):
    """
    Create a Redshift cluster
    Args:
        roleArn: IAM role ARN
    """
    try:
        print("Creating a new Redshift cluster")
        response = redshift.create_cluster(
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            IamRoles=[roleArn],
            AutomatedSnapshotRetentionPeriod=0  # Disable automated snapshots
        )
        print("Redshift cluster created")
    except ClientError as e:
        print(e)
        raise

def main():
    """
    Main function to set up the infrastructure for the data warehouse project.
    This function performs the following steps:
    1. Creates an IAM role.
    2. Attaches the necessary policy to the IAM role.
    3. Retrieves the IAM role ARN.
    4. Creates a Redshift cluster using the IAM role ARN.
    5. Waits for the Redshift cluster to become available.
    6. Retrieves the cluster endpoint and role ARN.
    7. Displays the cluster properties.
    8. Updates the configuration file (dwh.cfg) with the new cluster endpoint and role ARN.
    If any exception occurs during the process, it prints the exception message and starts the delete_infra.py script to clean up the infrastructure.
    Raises:
        Exception: If any error occurs during the infrastructure setup process.
    """
    try:
        # Create IAM Role
        create_IAM_role()

        # Attach Policy
        attach_policy()

        # Get IAM role ARN
        roleArn = get_IAM_role_arn()

        # Create Redshift Cluster
        create_redshift_cluster(roleArn)

        # Wait for the cluster to be available
        redshift.get_waiter('cluster_available').wait(ClusterIdentifier=CLUSTER_IDENTIFIER)
        print("Redshift cluster available")

        # Get the cluster endpoint and role ARN
        cluster_info = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
        endpoint = cluster_info['Endpoint']['Address']
        cluster_status = cluster_info['ClusterStatus']
        print(f"Cluster status: {cluster_status}")
        role_arn = cluster_info['IamRoles'][0]['IamRoleArn']

        # Display cluster properties
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        for k in keysToShow:
            print(f"{k}: {cluster_info[k]}")

        # Update dwh.cfg with the new cluster endpoint and role ARN
        config.set('CLUSTER', 'HOST', endpoint)
        config.set('IAM_ROLE', 'ARN', role_arn)

        with open('dwh.cfg', 'w') as configfile:
            config.write(configfile)

        print("Infrastructure setup complete. Redshift cluster endpoint and IAM role ARN have been updated in dwh.cfg.")
    except Exception as e:
        print(e)
        # Start delete_infra.py script
        delete_infra()

if __name__ == "__main__":
    main()