import boto3
import json

print('Loading function')

COUNTER_TABLE_NAME = "lambda.pf.counter"

res = boto3.resource('dynamodb')
counter_table_resource = res.Table(COUNTER_TABLE_NAME)

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    counter = {}
    for record in event['Records']:
        # print(record)
        eventSeq = int(record['dynamodb']['SequenceNumber'])
        key = record['dynamodb']['Keys']['pk']['S']

        try:
            if record['eventName'] == 'INSERT':

                counter_table_resource.update_item(
                    Key={'pk':key},
                    UpdateExpression="SET seqNum = :eventSeq",
                    ConditionExpression="attribute_not_exists(seqNum) OR seqNum < :eventSeq",
                    ExpressionAttributeValues={":eventSeq":eventSeq}
                )
        except res.meta.client.exceptions.ConditionalCheckFailedException as e:
            print("[FAILED] FAILED")
            print("[FAILED]" + str(e))
            attemptedEventSeq = counter_table_resource.get_item(
                Key={'pk':key},
                ConsistentRead=True
                )['Item']['seqNum']
            print("[FAILED] existingEventSeq: %10s attemptedEventSeq: %10s " % (record['dynamodb']['SequenceNumber'], attemptedEventSeq))

        if key not in counter.keys():
            counter[key] = 1
        else:
            counter[key] += 1
        # counter[key] = 1 if key not in counter.keys() else counter[key] += 1
        # print(record['dynamodb']['Keys'])
        # print("DynamoDB Record: " + json.dumps(record['dynamodb']['Keys'], indent=2))
    print("[COUNTER] " + str(counter))
    return 'Successfully processed {} records.'.format(len(event['Records']))
