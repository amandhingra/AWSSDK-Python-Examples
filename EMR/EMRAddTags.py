#!/bin/python3
import logging
import sys

import boto3

# Instantiate Logger for detailed information
logger = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)

# Instantiate Session from credentials stored in environment/InstanceProfile/.aws etc and EMR Client
session = boto3.Session(region_name='eu-west-1')
client = session.client('emr')

# Check for Clusters in non-terminated state
response = client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])

# Log details
for cluster in response['Clusters']:
    logger.info("ClusterId: %15s, ClusterName: %18s, ClusterStatus: %s " % (
    cluster['Id'], cluster['Name'], cluster['Status']['State']))

# Add tags as required
response = client.add_tags(ResourceId='<ClusterId_Here>', Tags=[{'Key': '<Key>', 'Value': '<Value>'}])
logger.info(response)
