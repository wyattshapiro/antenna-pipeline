from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from operators.local_to_s3 import LocalToS3Operator
from operators.s3_to_local import S3ToLocalOperator
from airflow.utils.trigger_rule import TriggerRule
from scripts.dag_util import construct_s3_and_local_file_path
from scripts.source_a__1__transform_raw_subscriptions import (
    identify_service_for_subscription
)
from config.source_a__1__transform_raw_subscriptions import (
    EXECUTION_DATE,
    LOCAL_FILE_PATH_RAW_DATA,
    S3_BUCKET_RAW_DATA
)
from datetime import datetime

doc = """
### Summary
This DAG takes raw subscription data from Source A and identifies key subscription events such as sign-ups and cancellations.

### Main Steps

 1. Extracts raw data from S3
 2. Transforms raw data

    - Identifies Service (ex. Netflix, Hulu, etc)
    - Identifies Signup/Cancellation
    - Handles Status (New, Update, Delete)

 3. Loads transformed data to S3

### Details
**Source**: Source A<br/>
**Entity**: Subscription<br/>
**Order**: 1<br/>
**Upstream DAG**: None<br/>
**Parallel DAG(s)**: None<br/>
**Downstream DAG(s)**: None<br/>
"""

# define file outputs
FILES = {
    'raw_subscriptions': {'file_name': 'ANTENNA_Data_Engineer_Test_Data.csv'},
    'service_matching_rules': {'file_name': 'ANTENNA_Data_Engineer_Matching_Rules.csv'},
    'transformed_subscriptions': {'file_name': 'transformed_ANTENNA_Data_Engineer_Test_Data.csv'}
}
FILES = construct_s3_and_local_file_path(FILES, LOCAL_FILE_PATH_RAW_DATA, EXECUTION_DATE)

# define DAG
default_args = {
    'owner': 'dev',
    'depends_on_past': False,
    'start_date': datetime(2020, 2, 20),
    'retries': 0
}

with DAG('source_a__1__transform_raw_subscriptions',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    dag.doc_md = doc

    # define tasks
    create_local_file_directory = BashOperator(
        task_id='create_local_file_directory',
        bash_command='mkdir -p {}/{}'.format(LOCAL_FILE_PATH_RAW_DATA, EXECUTION_DATE)
    )

    download_raw_subscriptions_from_s3 = S3ToLocalOperator(
        task_id='download_raw_subscriptions_from_s3',
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET_RAW_DATA,
        s3_key=FILES['raw_subscriptions']['s3_key'],
        local_file_path=FILES['raw_subscriptions']['local_file_path']
    )

    download_service_matching_rules_from_s3 = S3ToLocalOperator(
        task_id='download_service_matching_rules_from_s3',
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET_RAW_DATA,
        s3_key=FILES['service_matching_rules']['s3_key'],
        local_file_path=FILES['service_matching_rules']['local_file_path']
    )

    # download_signup_matching_rules_from_s3 = S3ToLocalOperator(
    #     task_id='download_signup_matching_rules_from_s3',
    #     s3_conn_id='',  # set in environment variable
    #     s3_bucket=S3_BUCKET_RAW_DATA,
    #     s3_key=FILES['signup_matching_rules']['s3_key'],
    #     local_file_path=FILES['signup_matching_rules']['local_file_path']
    # )

    identify_services = PythonOperator(
        task_id='identify_services',
        python_callable=identify_service_for_subscription,
        op_kwargs={
            'input_file_path': FILES['raw_subscriptions']['local_file_path'],
            'matching_file_path': FILES['service_matching_rules']['local_file_path'],
            'output_file_path': FILES['transformed_subscriptions']['local_file_path']
        }
    )

    # identify_signups = DummyOperator(
    #     task_id='identify_signups',
    # )

    # merge_subscriptions = DummyOperator(
    #     task_id='merge_subscriptions',
    # )

    check_transformed_subscriptions = DummyOperator(
        task_id='check_transformed_subscriptions',
    )

    upload_transformed_subscriptions_to_s3 = LocalToS3Operator(
        task_id='upload_transformed_subscriptions_to_s3',
        trigger_rule=TriggerRule.NONE_FAILED,
        s3_conn_id='',  # set in environment variable
        s3_bucket=S3_BUCKET_RAW_DATA,
        s3_key=FILES['transformed_subscriptions']['s3_key'],
        local_file_path=FILES['transformed_subscriptions']['local_file_path']
    )

    # define order of tasks
    create_local_file_directory >> download_raw_subscriptions_from_s3
    download_raw_subscriptions_from_s3 >> download_service_matching_rules_from_s3
    download_service_matching_rules_from_s3 >> identify_services
    identify_services >> check_transformed_subscriptions
    check_transformed_subscriptions >> upload_transformed_subscriptions_to_s3
    # identify_services >> check_transformed_subscriptions
    # download_raw_subscriptions_from_s3 >> download_signup_matching_rules_from_s3
    # download_signup_matching_rules_from_s3 >> identify_signups
    # identify_services >> merge_subscriptions
    # identify_signups >> merge_subscriptions
    # merge_subscriptions >> check_transformed_subscriptions
