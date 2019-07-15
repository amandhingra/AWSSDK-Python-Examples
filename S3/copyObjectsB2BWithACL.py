#!/usr/bin/python3
import logging
import sys

import boto3

# Instantiate Logger for detailed information
logger = logging.getLogger()
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger.setLevel(logging.INFO)

# Initialize
client = boto3.client("s3")
bucketOne = sys.argv[1]  # 'Source-Bucket-Here'
bucketTwo = sys.argv[2]  # 'Destination-Bucket-Here'

kwargs = {'Bucket': bucketOne}
listOfObjects = []
failedObjects = []

"""Lists objects in bucket in rounds of 1000(max limit by boto3)
   Copies objects to bucketTwo and then copies ACL to those objects
   Will be treated as new object. Does not delete object from bucketOne
   Logs to screen and prints failed object keys at last"""

while True:
    response = client.list_objects_v2(**kwargs)
    for s3object in response["Contents"]:
        listOfObjects.append(s3object["Key"])
        object_acl_response = client.get_object_acl(Bucket=bucketOne, Key=s3object["Key"])
        object_acl = {"Grants": object_acl_response["Grants"], "Owner": object_acl_response["Owner"]}
        copy_response = client.copy_object(Bucket=bucketTwo, CopySource={"Bucket": bucketOne, "Key": s3object["Key"]},
                                           Key=s3object["Key"], MetadataDirective="COPY")
        if copy_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            failedObjects.append(s3object["Key"])
            logger.info('ERROR: Copy Unsuccessful for Key: ' + s3object["Key"])
        else:
            put_acl_response = client.put_object_acl(Bucket=bucketTwo, Key=s3object["Key"],
                                                     AccessControlPolicy=object_acl)
            if put_acl_response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                logger.error('ERROR: Copy ACL Unsuccessful for Key: ' + s3object["Key"])
            else:
                logger.info('Copy Successful for Key: ' + s3object["Key"])
    try:
        kwargs["ContinuationToken"] = response["NextContinuationToken"]
    except KeyError:
        break
logger.info('Failed Objects: ')
for k in failedObjects:
    print(k)
