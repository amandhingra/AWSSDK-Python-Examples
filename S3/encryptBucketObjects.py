#! /usr/bin/python3
# Run as python3 encryptBucketObjects.py <bucket-name> <text-file-for-failed-objects>
import sys
from datetime import datetime

import boto3

print(sys.version)
# Initialize
client = boto3.client("s3")
s3 = boto3.resource("s3")

"""
    List Bucket. 
    Currently does 1000 objects Maximum as per http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects
"""
myBucket = sys.argv[1]
contents = client.list_objects(Bucket=myBucket)["Contents"]
failedKeys = []

""" Encrypts existing objects with AES256 which are not already AES256 Encrypted
    This essentially copies files to itself. If Versioning enabled, creates a new version
    If Objects encrypted with custom keys, this script will encrypt those with AES256 Server Side
    Lifecycle policies will treat the objects as a new object
"""
for s3object in contents:
    failedKeys.append(myBucket + "/" + s3object["Key"])
    storageClass = s3object["StorageClass"]
    head = client.head_object(Bucket=myBucket, Key=s3object['Key'])
    if "ServerSideEncryption" in head and head["ServerSideEncryption"] == "AES256":
        print(str(datetime.now()) + "\t" + s3object[
            "Key"] + '  having storage class: ' + storageClass + ' is already encrypted.')
    else:
        response = s3.Object(myBucket, s3object['Key']).copy_from(CopySource={"Bucket": myBucket, "Key": s3object[
            "Key"]},
                                                                  MetadataDirective="REPLACE",
                                                                  ServerSideEncryption="AES256",
                                                                  StorageClass=storageClass)
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(str(datetime.now()) + "\t" + 'INFO: Encrypted ' + s3object["Key"] + "\t" + storageClass)
        else:
            print(str(datetime.now()) + "\t" + 'ERROR: Failed to Encrypt ' + str(s3object["Key"]))
            failedKeys.append(myBucket + "/" + s3object["Key"])
    del storageClass, head

# Writes Failed Keys to external File for further investigation/Manual work
f = open(sys.argv[2], 'w')
[f.write(file + '\n') for file in failedKeys]
f.close()
