import boto3
import json
import os
import pymysql
import csv
from io import StringIO

def lambda_handler(event, context):
    if 'Records' not in event:
        print("Event does not contain 'Records' key")
        print("Event contents:", event)
        return {
            'statusCode': 400,
            'body': 'Invalid event structure'
        }

    # Environment variables (configure in Lambda)
    S3_BUCKET = os.environ['S3_BUCKET']
    RDS_HOST = os.environ['RDS_HOST']
    RDS_USER = os.environ['RDS_USER']
    RDS_PASSWORD = os.environ['RDS_PASSWORD']
    RDS_DB_NAME = os.environ['RDS_DB_NAME']
    RDS_TABLE_NAME = os.environ['RDS_TABLE_NAME']

    # Initialize S3 client
    s3 = boto3.client('s3')

    # Get the S3 object key from the event
    s3_key = event['Records'][0]['s3']['object']['key']
    
    try:
        # Get the S3 object
        s3_response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
        csv_content = s3_response['Body'].read().decode('utf-8')

        # Parse CSV data
        csv_file = StringIO(csv_content)
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)  # Skip the header row
        data = list(csv_reader)

        # Establish RDS connection
        conn = pymysql.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, db=RDS_DB_NAME)
        
        with conn.cursor() as cursor:
            # Construct the SQL insert statement
            placeholders = ', '.join(['%s'] * len(header))
            sql = f"INSERT INTO {RDS_TABLE_NAME} ({', '.join(header)}) VALUES ({placeholders})"

            # Execute the bulk insert
            cursor.executemany(sql, data)
            conn.commit()

        return {
            'statusCode': 200,
            'body': json.dumps('Data inserted successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        if 'conn' in locals():
            conn.close()
