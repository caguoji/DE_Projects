import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator,EmrCreateJobFlowOperator, EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor

JOB_FLOW_OVERRIDES = {
    "Name": "WCD_Midterm",
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

            #  {
            #     "Name": "Work node",
            #     "Market": "ON_DEMAND",
            #     "InstanceRole": "CORE",
            #     "InstanceType": "m5.xlarge",
            #     "InstanceCount": 1,
            # },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    'VisibleToAllUsers': True
}

# this is just an example of how to use SPARK_STEPS, you need to define your own steps
SPARK_STEPS = [
    {
        'Name': 'wcd_data_engineer',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
                # '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                # '--num-executors', '2',
                # '--driver-memory', '512m',
                # '--executor-memory', '3g',
                # '--executor-cores', '2',
                's3://wcd-midterm-artifacts/ETL_Inventory.py',
                # '--spark_name', 'WCD-Midterm',
                # '--input_bucket', "s3://wcd-midterm/artifacts",
              
                # '--path_output', 's3://wcd-midterm-output-chichi',
                # '-c', 'job',
                # '-m', 'append',
                #'--input-options', 'header=true',
                '{{ds}}',
                "{{ task_instance.xcom_pull('parse_request', key='sales') }}",
                "{{ task_instance.xcom_pull('parse_request', key='calendar') }}",
                "{{ task_instance.xcom_pull('parse_request', key='inventory') }}",
                "{{ task_instance.xcom_pull('parse_request', key='product') }}",
                "{{ task_instance.xcom_pull('parse_request', key='store') }}",
            ]
        }
    }

]

#CLUSTER_ID = "j-2NT2ZIL4GUO37"

DEFAULT_ARGS = {
    'owner': 'wcd_data_engineer',
    'depends_on_past': False,
    'email': ['airflow_data_eng@wcd.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_s3_files(**kwargs):
    #retrieve calendar

    cal_data_loc = kwargs['dag_run'].conf['calendar']
    kwargs['ti'].xcom_push(key = 'calendar', value = cal_data_loc )

    #retrieve sales
    sales_data_loc = kwargs['dag_run'].conf['sales']
    kwargs['ti'].xcom_push(key = 'sales', value = sales_data_loc)

    #retrieve inventory
    inv_data_loc = kwargs['dag_run'].conf['inventory']
    kwargs['ti'].xcom_push(key = 'inventory', value = inv_data_loc)

    #retrieve product
    prod_data_loc = kwargs['dag_run'].conf['product']
    kwargs['ti'].xcom_push(key = 'product', value = prod_data_loc)

    #retrieve store
    store_data_loc = kwargs['dag_run'].conf['store']
    kwargs['ti'].xcom_push(key = 'store', value = store_data_loc)

    print(store_data_loc)
    #print(kwargs['dag_run'].conf['store'])


dag = DAG(
    'midterm_dag',
    start_date = airflow.utils.dates.days_ago(0),
    default_args = DEFAULT_ARGS,
    dagrun_timeout = timedelta(hours=1),
    schedule_interval = None
)

parse_request = PythonOperator(task_id = 'parse_request',
                                provide_context = True, # Airflow will pass a set of keyword arguments that can be used in your function
                                python_callable = retrieve_s3_files,
                                dag = dag
                                ) 


create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        # aws_conn_id = "aws_default",
        # emr_conn_id = "emr_default"
)



step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    #job_flow_id = CLUSTER_ID,
    job_flow_id=create_job_flow.output,
    # aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)


step_checker = EmrStepSensor(
    task_id = 'watch_step',
    #job_flow_id = CLUSTER_ID,
    job_flow_id="{{ task_instance.xcom_pull('create_job', key='return_value')}}",
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0]}}",
    # aws_conn_id = "aws_default",
    poke_interval = 30,
    timeout = 60*10, 
    dag = dag
)

remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_job', key='return_value')}}",
)

create_job_flow.set_upstream(parse_request)
step_adder.set_upstream(create_job_flow)
step_checker.set_upstream(step_adder)
remove_cluster.set_upstream(step_checker)
