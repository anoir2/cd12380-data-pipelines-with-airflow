from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
        table="",
        aws_credentials_id="",
        redshift_connection_id="",
        s3_bucket="",
        s3_path= "",
        json_path="",
        *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_connection_id = redshift_connection_id
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.json_path = json_path

    def execute(self, context):
        s3_uri = f"s3://{self.s3_bucket}/{self.s3_path}"

        self.log.info(f"Loading {self.table} staging table from {s3_uri}")
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)

        staging_events_copy = f"""
            copy {self.table}
            from '{s3_uri}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            json '{self.json_path}'
            COMPUPDATE OFF;
                    """
        
        redshift.run(staging_events_copy)

        self.log.info(f"Staging table {self.table} loaded successfully.")
