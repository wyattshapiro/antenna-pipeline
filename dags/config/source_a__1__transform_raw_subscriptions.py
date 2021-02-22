from datetime import datetime
import pathlib

DIR_BASE = pathlib.Path().cwd()

# EXECUTION_DATE = '{{ next_ds }}'
EXECUTION_DATE = '2021-02-20'

LOCAL_FILE_PATH_RAW_DATA = DIR_BASE.joinpath('files/source_a').resolve()

S3_BUCKET_RAW_DATA = 'antenna-source-a-subscription-us-east-2'
