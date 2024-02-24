from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_connection_id = redshift_connection_id
        self.tables = tables

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_connection_id)

        for table in self.tables:
            self.log.info(f"Checking Data Quality for {table} table")

            count_query = f"SELECT COUNT(*) FROM {table}"
            try:
                records = redshift_hook.get_records(count_query)
            except Exception as e:
                self.log.error("Error on executing count query. The error is: ", e)
                raise e

            record_counts = records[0][0]
            if record_counts < 1:
                raise ValueError(f"Data quality check failed: Table {table} is empty!")
            
            self.log.info(f"Table {table} have {record_counts} records")
        
        self.log.info("Data quality check OK")