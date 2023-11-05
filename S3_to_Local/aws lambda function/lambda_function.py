import boto3
import time
import datetime
from datetime import date, timedelta
import subprocess
import json



def lambda_handler(event, context):
    
    s3_file_list = []
    
    s3_client=boto3.client('s3')
    for object in s3_client.list_objects_v2(Bucket='test-scrapy-rolex')['Contents']:
        s3_file_list.append(object['Key'])
    
   
    
    # #required file list to check
    required_file_list = ['files.csv']
    
    # # scan S3 bucket
    if s3_file_list!=required_file_list:
    
        s3_file_url = ['s3://' + 'test_scrapy_rolex/' + a for a in s3_file_list]
        print(f'file_url: {s3_file_url}')
        
        table_name = [a.split('.')[0] for a in s3_file_list]
        print(f'tablename: {table_name}')
    
        
        data = json.dumps({'conf':{a:b for a,b in zip(table_name,s3_file_url)}})
        
        print(f'json: {data}')
    
    
    # send signal to Airflow
        
        endpoint= 'http://3.22.116.22:8080/api/v1/dags/rolex_pipeline/dagRuns'
    
        subprocess.run(['curl', '-X', 'POST', endpoint,'-H','Content-Type: application/json','--user','admin:airflow','--data',data])
      
        print('Files were sent to Airflow')
    else:
        print(s3_file_list)
       