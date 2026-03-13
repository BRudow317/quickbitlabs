import json
import pymysql
import os

# Best practice: Use Environment Variables for DB credentials
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']

def lambda_handler(event, context):
    # (CORS handling code from previous step goes here)
    
    try:
        body = json.loads(event.get('body', '{}'))
        
        # Connect to the database
        conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, db=DB_NAME)
        
        with conn.cursor() as cur:
            sql = "INSERT INTO submissions (name, email) VALUES (%s, %s)"
            cur.execute(sql, (body['name'], body['email']))
            conn.commit()
        
        conn.close()
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'message': 'Saved to MariaDB!'})
        }
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}