from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from operators.local_to_s3 import LocalToS3Operator
from operators.s3_to_local import S3ToLocalOperator
from airflow.utils.trigger_rule import TriggerRule
import scripts.source_a__1__transform_raw_subscriptions as func
from config.source_a__1__transform_raw_subscriptions import (
    EXECUTION_DATE,
    LOCAL_FILE_PATH_RAW_DATA,
    S3_BUCKET_RAW_DATA
)
from datetime import datetime

doc = """
### Summary
A pipeline that takes raw subscription data from Source A and identifies key subscription details such as service, sign-up/cancellation, and free trial/paid.

### Main Steps

 1. Extract raw data from S3
 2. Transform raw data

    - Handle Status (New, Update, Delete)
    - Identify Service (ex. Netflix, Hulu, etc)
    - Identify Signup/Cancellation
    - Identify Free Trial/Paid

 3. Load transformed data to S3

### Details
**Source**: Source A<br/>
**Entity**: Subscription<br/>
**Order**: 1<br/>
**Upstream DAG**: None<br/>
**Parallel DAG(s)**: None<br/>
**Downstream DAG(s)**: None<br/>
"""

# define file paths
FILES = {
    'service_matching_rules': {
        'file_name': 'ANTENNA_Data_Engineer_Matching_Rules.csv',
        's3_bucket': S3_BUCKET_RAW_DATA,
        's3_key_path': 'ANTENNA_Data_Engineer_Matching_Rules.csv',
        'local_file_path': f'{LOCAL_FILE_PATH_RAW_DATA}/ANTENNA_Data_Engineer_Matching_Rules.csv'
    },
    'subscriptions_raw': {
        'file_name': 'ANTENNA_Data_Engineer_Test_Data.csv',
        's3_bucket': S3_BUCKET_RAW_DATA,
        's3_key_path': f'{EXECUTION_DATE}/ANTENNA_Data_Engineer_Test_Data.csv',
        'local_file_path': f'{LOCAL_FILE_PATH_RAW_DATA}/{EXECUTION_DATE}/ANTENNA_Data_Engineer_Test_Data.csv'
    },
    'subscriptions_latest': {
        'file_name': 'ANTENNA_Data_Engineer_Test_Data_latest.csv',
        's3_bucket': S3_BUCKET_RAW_DATA,
        's3_key_path': f'{EXECUTION_DATE}/ANTENNA_Data_Engineer_Test_Data_latest.csv',
        'local_file_path': f'{LOCAL_FILE_PATH_RAW_DATA}/{EXECUTION_DATE}/ANTENNA_Data_Engineer_Test_Data_latest.csv'
    },
    'subscriptions_with_service': {
        'file_name': 'ANTENNA_Data_Engineer_Test_Data_with_service.csv',
        's3_bucket': S3_BUCKET_RAW_DATA,
        's3_key_path': f'{EXECUTION_DATE}/ANTENNA_Data_Engineer_Test_Data_with_service.csv',
        'local_file_path': f'{LOCAL_FILE_PATH_RAW_DATA}/{EXECUTION_DATE}/ANTENNA_Data_Engineer_Test_Data_with_service.csv'
    }
}

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

    download_subscriptions_raw_from_s3 = S3ToLocalOperator(
        task_id='download_subscriptions_raw_from_s3',
        s3_conn_id='',  # set in environment variable
        s3_bucket=FILES['subscriptions_raw']['s3_bucket'],
        s3_key=FILES['subscriptions_raw']['s3_key_path'],
        local_file_path=FILES['subscriptions_raw']['local_file_path']
    )

    download_service_matching_rules_from_s3 = S3ToLocalOperator(
        task_id='download_service_matching_rules_from_s3',
        s3_conn_id='',  # set in environment variable
        s3_bucket=FILES['service_matching_rules']['s3_bucket'],
        s3_key=FILES['service_matching_rules']['s3_key_path'],
        local_file_path=FILES['service_matching_rules']['local_file_path']
    )

    filter_latest_subscription_event = PythonOperator(
        task_id='filter_latest_subscription_event',
        python_callable=func.filter_latest_subscription_event,
        op_kwargs={
            'input_file_path': FILES['subscriptions_raw']['local_file_path'],
            'output_file_path': FILES['subscriptions_latest']['local_file_path']
        }
    )

    get_subscription_service = PythonOperator(
        task_id='get_subscription_service',
        python_callable=func.get_subscription_service,
        op_kwargs={
            'input_file_path': FILES['subscriptions_latest']['local_file_path'],
            'service_matching_file_path': FILES['service_matching_rules']['local_file_path'],
            'output_file_path': FILES['subscriptions_with_service']['local_file_path']
        }
    )

    check_subscriptions_transformed = DummyOperator(
        task_id='check_subscriptions_transformed',
    )

    upload_subscriptions_transformed_to_s3 = LocalToS3Operator(
        task_id='upload_subscriptions_transformed_to_s3',
        trigger_rule=TriggerRule.NONE_FAILED,
        s3_conn_id='',  # set in environment variable
        s3_bucket=FILES['subscriptions_with_service']['s3_bucket'],
        s3_key=FILES['subscriptions_with_service']['s3_key_path'],
        local_file_path=FILES['subscriptions_with_service']['local_file_path'],
        replace=True
    )

    # define order of tasks
    create_local_file_directory >> [download_subscriptions_raw_from_s3, download_service_matching_rules_from_s3]
    download_subscriptions_raw_from_s3 >> filter_latest_subscription_event
    filter_latest_subscription_event >> get_subscription_service
    download_service_matching_rules_from_s3 >> get_subscription_service
    get_subscription_service >> check_subscriptions_transformed
    check_subscriptions_transformed >> upload_subscriptions_transformed_to_s3

