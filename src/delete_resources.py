import boto3
import time
import configparser
from create_resources import config_file, s3_arn_policy

# Reading cfg file
config = configparser.ConfigParser()
config.read(config_file)

# Setting up Access Key and Secret Key
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')

# Define AWS Services
redshift_client = boto3.client('redshift', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam_client = boto3.client('iam', aws_access_key_id=KEY, aws_secret_access_key=SECRET)
ec2_client = boto3.client('ec2', region_name='us-west-2', aws_access_key_id=KEY, aws_secret_access_key=SECRET)

def delete_redshift_cluster(config):
    """Deletes AWS Redshift Cluster

    Args:
        config (ConfigParser object): Configuration File to define Resource configuration

    Returns:
        dictionary: AWS Redshift Information
    """
    try:
        response = redshift_client.delete_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTERIDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
    except:
        print("Redshift Cluster '%s' does not exist!" % (config.get('CLUSTER', 'CLUSTERIDENTIFIER')))
        return None
    else:
        return response['Cluster']

def wait_for_cluster_deletion(cluster_id):
    """Verifies if AWS Redshift Cluster was deleted

    Args:
        cluster_id (dictionary): AWS Redshift Cluster Information
    """
    while True:
        try:
            redshift_client.describe_clusters(ClusterIdentifier=cluster_id)
        except:
            break
        else:
            time.sleep(60)

def delete_iam_role(config, arn_policy):
    """Deletes AWS IAM Role

    Args:
        config (ConfigParser object): Configuration File to define Resource configuration
        arn_policy (string): ARN Policy you want to detach from the IAM Role
    """
    try:
        iam_client.detach_role_policy(
            RoleName=config.get('SECURITY', 'ROLE_NAME'),
            PolicyArn=s3_arn_policy
        )
        iam_client.delete_role(RoleName=config.get('SECURITY', 'ROLE_NAME'))
        print('IAM Role deleted.')
    except:
        print("IAM Role '%s' does not exist!" % (config.get('SECURITY', 'ROLE_NAME')))

def delete_security_group(config):
    """Deletes AWS VPC Security Group

    Args:
        config (ConfigParser object): Configuration File to define Resource configuration
    """
    try:
        ec2_client.delete_security_group(GroupId=config.get('SECURITY', 'SG_ID'))
        print('Security Group deleted.')
    except:
        print("Security Group '%s' does not exist!" % (config.get('SECURITY', 'SG_ID')))

def delete_resources():
    """Initiate Resources Deletion"""

    config = configparser.ConfigParser()
    config.read(config_file)

    cluster_info = delete_redshift_cluster(config)

    if cluster_info is not None:
        print(f'Deleting Redshift cluster: {cluster_info["ClusterIdentifier"]}')
        print(f'Redshift Cluster status: {cluster_info["ClusterStatus"]}')

        print('Waiting for Redshift cluster to be deleted...')
        wait_for_cluster_deletion(cluster_info['ClusterIdentifier'])
        print('Redshift Cluster deleted.')

    delete_iam_role(config,s3_arn_policy)

    delete_security_group(config)

if __name__ == "__main__":
    delete_resources()