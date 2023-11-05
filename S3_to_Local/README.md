## Use AWS lambda function to trigger Airflow RESTAPI and move files between S3 and local drive with S3 trigger

### Steps
1. Set up EC2 instance.
2. Set up virtual environment.
3. Download Airflow and config requirements.
4. Use MySql as Airflow database.
6. Write a dag using the s3 hook operator.
6. Set up AWS lambda function to send a POST request to the Airflow RESTAPI and call a specific dag.
7. Set up S3 Trigger for AWS Lambda function.


### Output
1. A new file(s) is dropped in the S3 bucket.
2. The Lambda function is triggered and the dag is run.


### Issues
1. Configuring Airflow config file to allow basic authentication for RESTAPI.
2. Use of static IP in EC2 when testing.

### Business Requirements
1. Files can be transferred via batch or via trigger.


