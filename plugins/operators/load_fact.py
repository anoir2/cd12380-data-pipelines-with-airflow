from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_connection_id,
                 insert_sql_query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_connection_id = redshift_connection_id
        self.insert_sql_query = insert_sql_query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        self.log.info(f"Loading fact table {self.table}")
        sql = f"""INSERT INTO {self.table} 
                    {self.insert_sql_query}; 
                    COMMIT;"""
                    
        try:
            redshift.run(sql)
        except Exception as e:
            self.log.error("Error on executing query. Query", sql," The error is: ", e)
            raise e

