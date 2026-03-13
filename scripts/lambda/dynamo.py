"""

"""

import json
import boto3
import uuid

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourTableName')

def lambda_handler(event, context):
    # (CORS handling code from previous step goes here)
    
    try:
        body = json.loads(event.get('body', '{}'))
        
        # Add a unique ID if your form doesn't have one
        body['id'] = str(uuid.uuid4())
        
        table.put_item(Item=body)
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'message': 'Saved to DynamoDB!'})
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}