import boto3
import configparser
from botocore.exceptions import ClientError
import pandas as pd

# Load configuration from dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

# AWS credentials and configuration
AWS_KEY = config.get('AWS', 'KEY')
AWS_SECRET = config.get('AWS', 'SECRET')
REGION = config.get('AWS', 'REGION')

# Redshift cluster configuration
CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
IAM_ROLE_NAME = config.get('IAM_ROLE', 'ROLE_NAME')

# Create clients
iam = boto3.client('iam', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, region_name=REGION)
redshift = boto3.client('redshift', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET, region_name=REGION)

# Delete Redshift Cluster
try:
    print("Deleting Redshift Cluster")
    redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=CLUSTER_IDENTIFIER)
    print("Redshift Cluster deleted")
except ClientError as e:
    print(e)

# Detach and delete IAM Role
try:
    print("Detaching policy from IAM Role")
    iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    
    print("Deleting IAM Role")
    iam.delete_role(RoleName=IAM_ROLE_NAME)
    print("IAM Role deleted")
except ClientError as e:
    print(e)

# Remove ARN and HOST entries from dwh.cfg
config.remove_option('CLUSTER', 'HOST')
config.remove_option('IAM_ROLE', 'ARN')

with open('dwh.cfg', 'w') as configfile:
    config.write(configfile)

print("Infrastructure deletion complete.")