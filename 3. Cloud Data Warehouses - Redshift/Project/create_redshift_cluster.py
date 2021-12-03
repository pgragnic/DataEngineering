import pandas as pd
import boto3
import json
import time

"""
Description: Get all parameters from dwh.cfg file
"""

import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


"""
Description: Create IAM and Redshift clients
"""

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )


"""
Description: Create and attach IAM role
"""

from botocore.exceptions import ClientError

# Create the role
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
            'Effect': 'Allow',
            'Principal': {'Service': 'redshift.amazonaws.com'}}],
            'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)

# Attach the role to a policy

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                    PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                    )['ResponseMetadata']['HTTPStatusCode']

roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']


"""
Description: Create the Redshift cluster
"""

try:
    response = redshift.create_cluster(        
        # add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # add parameters for identifiers & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        # add parameter for role (to allow s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)


"""
Description: Describe the cluster to see its status
Run this block several times until the cluster status becomes `Available`
"""

cluster_available = False
time_wait = 0

print ('Wait for the Cluster availability to continue the process...')

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
print ('Cluster Status: ',myClusterProps.get('ClusterStatus'))

while cluster_available == False :
    time.sleep(30)
    time_wait += 30
    print(time_wait, " seconds")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print ('Cluster Status: ',myClusterProps.get('ClusterStatus'))
    if myClusterProps.get('ClusterStatus') == 'available':
        cluster_available = True
        print ('Cluster ', myClusterProps.get('ClusterStatus', ' available !') )