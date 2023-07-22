
from __future__ import annotations

import json
from datetime import datetime

import boto3

import os
from airflow.models.connection import Connection

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule
#from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "emr_example"

EXECUTION_ROLE_ARN_KEY = "arn:aws:iam::412152202091:user/Jennifer"

CONFIG_NAME= "EMR Runtime Role Security Configuration"


conn = Connection(
    conn_id="aws_default",
    conn_type="aws",
    login="AKIAV75RPQNVV7VFSQV5",  # Reference to AWS Access Key ID
    password="0+fe/Lef9hEkjnN5elNUDECTPpvQGT6AUmtMxzYx",  # Reference to AWS Secret Access Key
    extra={
        # Specify extra parameters here
        "region_name": "us-east-2",
    },
)

JOB_FLOW_OVERRIDES = {
    "Name": "PiCalc",
    "ReleaseLabel": "emr-6.7.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    'VisibleToAllUsers': True
}

SPARK_STEPS = [
    {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        },
    }
]


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 5, 28),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    

    #test_context = sys_test_context_task()


    #env_id = test_context[ENV_ID_KEY]
    config_name = {CONFIG_NAME}
    execution_role_arn = {EXECUTION_ROLE_ARN_KEY}
    
    s3_bucket = f"test-emr-bucket"

    JOB_FLOW_OVERRIDES["LogUri"] = f"s3://{s3_bucket}/"
    JOB_FLOW_OVERRIDES["SecurityConfiguration"] = config_name
    #JOB_FLOW_OVERRIDES["Instances"]["InstanceGroups"][0]["CustomAmiId"] = get_ami_id()

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    #create_security_configuration = configure_security_config(config_name)

    # [START howto_operator_emr_create_job_flow]
    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
)    

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_job_flow.output,
        steps=SPARK_STEPS,
        execution_role_arn=execution_role_arn,
)

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_job_flow.output,
)


#create_job_flow.set_upstream(create_s3_bucket)
add_steps.set_upstream(create_job_flow)
remove_cluster.set_upstream(add_steps)





