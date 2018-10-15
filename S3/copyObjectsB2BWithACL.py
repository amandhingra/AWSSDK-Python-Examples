#!/usr/bin/python3
import sys
import boto3
from datetime import datetime

#Initialize
client = boto3.client('s3')
bucketOne	=	sys.argv[1]	#'Source-Bucket-Here'
bucketTwo	=	sys.argv[2]	#'Destination-Bucket-Here'

kwargs={'Bucket':bucketOne}
listOfObjects=[]
failedObjects=[]

#Lists objects in bucket in rounds of 1000(max limit by boto3)
#Copies objects to bucketTwo and then copies ACL to those objects
#Will be treated as new object. Does not delete object from bucketOne
#Logs to screen and prints failed object keys at last
while True:
	response=client.list_objects_v2(**kwargs)
	for object in response['Contents']:
		listOfObjects.append(object['Key'])
		object_acl_response=client.get_object_acl(Bucket=bucketOne,Key=object['Key'])
		object_acl={}
		object_acl['Grants']=object_acl_response['Grants']
		object_acl['Owner']=object_acl_response['Owner']
		copy_response=client.copy_object(Bucket=bucketTwo,CopySource={'Bucket':bucketOne,'Key':object['Key']},Key=object['Key'],MetadataDirective='COPY')
		if copy_response['ResponseMetadata']['HTTPStatusCode']!=200:
			failedObjects.append(object['Key'])
			print(str(datetime.now()) + "\t" +"ERROR: Copy Unsuccessful for Key: "+object['Key'])
		else:
			put_acl_response=client.put_object_acl(Bucket=bucketTwo,Key=object['Key'],AccessControlPolicy=object_acl)
			if put_acl_response['ResponseMetadata']['HTTPStatusCode']!=200:
				print(str(datetime.now()) + "\t" +"ERROR: Copy ACL Unsuccessful for Key: "+object['Key'])
			else:
				print(str(datetime.now()) + "\t" +"INFO: Copy Successful for Key: "+object['Key'])
	try:
		kwargs['ContinuationToken']=response['NextContinuationToken']
	except KeyError:
		break
print("Failed Objects: ")
for k in failedObjects:
	print(k)