from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_connection_id,
                 insert_sql_query,
                 truncate_before_insert,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_connection_id = redshift_connection_id
        self.insert_sql_query = insert_sql_query
        self.truncate_before_insert = truncate_before_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_connection_id)
        if self.truncate_before_insert:
            self.log.info(f"Truncating dimension table {self.table}")
            sql = f"TRUNCATE TABLE {self.table};COMMIT;"
            self.__execute_sql(redshift, sql)
        else:
            self.log.info(f"Truncate disabled in the table {self.table}. Append-only mode enabled.")


        self.log.info(f"Loading dimension table {self.table}")
        sql = f"""INSERT INTO {self.table} 
                    {self.insert_sql_query}; 
                    COMMIT;"""
        self.__execute_sql(redshift, sql)
        self.log.info(f"Dimension table {self.table} loaded successfully.")

    
    def __execute_sql(self, redshift, sql):
        try:
            redshift.run(sql)
        except Exception as e:
            self.log.error("Error on executing query. Query", sql," The error is: ", e)
            raise e
