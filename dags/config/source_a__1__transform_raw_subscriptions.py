from datetime import datetime
import pathlib

# EXECUTION_DATE = '{{ next_ds }}'
EXECUTION_DATE = '2021-02-20'

DIR_BASE = pathlib.Path().cwd()
LOCAL_FILE_PATH_RAW_DATA = DIR_BASE.joinpath('files/source_a')
LOCAL_FILE_PATH_RAW_DATA_WITH_DATE = LOCAL_FILE_PATH_RAW_DATA.joinpath(EXECUTION_DATE)

S3_BUCKET_RAW_DATA = 'antenna-source-a-subscription-us-east-2'
