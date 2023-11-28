from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.models import Variable
import os

# Configurations
EMR_LOGS = Variable.get("EMR_S3_BUCKET")
BUCKET_NAME = Variable.get("AIRFLOW_BUCKET")
os.environ['AWS_DEFAULT_REGION'] = Variable.get("AWS_REGION")
SPARK_STEPS = [
    {
        "Name": "Simple echo Test",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash",
                "-c",
                "echo 'Hello World!' > /home/hadoop/test.txt",
            ],
        },
    },
    {
        "Name": "View result",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash",
                "-c",
                "cat /home/hadoop/test.txt",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Demo Ephemeral EMR Cluster",
    "ReleaseLabel": "emr-6.11.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "BootstrapActions": [
        {
            "Name": "Just an example",
            "ScriptBootstrapAction": {
                "Args": ["Hello World! This is Demo Ephemeral EMR Cluster"],
                "Path": "file:/bin/echo",
            },
        }
    ],
    "LogUri": "s3n://{}/emr_logs/".format(EMR_LOGS),
    "SecurityConfiguration": Variable.get("EMR_SECURITY_CONFIG"),
    "Tags": [ 
      { 
        "Key": "ssmmanaged",
        "Value": "no see CSRC_DBC_933_EC2_SSM_MANAGED"
      },
      {
        "Key": "CSRC_DBC_933",
        "Value": "CSRC_DBC_933_EC2_SSM_MANAGED"
      },
      {
        "Key": "CSRC_DBC_935",
        "Value": "CSRC_DBC_935_EC2_SSM_MANAGED"
      }  
    ],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
            {
                "Name": "ON_TASK",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": Variable.get("EMR_SUBNET_ID"),
    },
    "JobFlowRole": Variable.get("EMR_JOBFLOW_ROLE"),
    "ServiceRole": Variable.get("EMR_SERVICE_ROLE"),
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spark_submit_airflow",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
    tags=["example-tag"],
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline
