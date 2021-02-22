from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class S3ToLocalOperator(BaseOperator):
    """Loads a file from S3 to a local directory.

    Attributes:
        s3_conn_id: string, connection ID to S3
        s3_bucket: string, name of bucket to download from
        s3_key: string, key of the file to download
        local_file_path: string, path to the downloaded file
    """

    template_fields = ['s3_key', 'local_file_path']

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 local_file_path,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.local_file_path = local_file_path

    def execute(self, context):
        print('Downloading {}/{} to {} '.format(self.s3_bucket, self.s3_key, self.local_file_path))

        hook = S3Hook(self.s3_conn_id)

        # Check if key exists, download if so
        if hook.check_for_key(self.s3_key, self.s3_bucket):
            s3_file_object = hook.get_key(self.s3_key, self.s3_bucket)
            s3_file_object.download_file(self.local_file_path)
        else:
            raise ValueError('File not found in S3')

        print('Download successful')
