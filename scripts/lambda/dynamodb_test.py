"""
If you ever want to work on a plane or without an internet connection, 
you can run DynamoDB Local (a small Java program or Docker container) 
on your home lab. You just change one line in your Python script: 
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
"""
import boto3

# Boto3 automatically finds your home lab credentials
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

def test_connection():
    # Replace with one of your 5 table names
    table = dynamodb.Table('SiteA_Table')
    
    try:
        # Try a simple "ping" to the cloud
        response = table.scan(Limit=1)
        print("Successfully hit the Cloud DynamoDB!")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()