from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from abc import ABC, abstractmethod
from helpers import Ok, Error, Result

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_connection_id="",
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_connection_id = redshift_connection_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(self.redshift_connection_id)
        check_failed = False

        for check in self.checks:
            result = check.perform_check(redshift)
            if not result.is_ok():
                check_failed = True
                self.log.error("Data Validation failed. The error is: ", result)

            exec_msg = result.unwrap()
            self.log.info(exec_msg)

        if check_failed:
            raise ValueError("Some data validation check failed, check the logs.")
        
        self.log.info("Data quality check OK")

class DataQualityCheck(ABC):
    def __init__(self, query):
        self.query = query

    @abstractmethod
    def perform_check(self, redshift) -> Result:
        pass

    def execute_query(self, redshift) -> Result:
        try:
            records = redshift.get_records(self.query)
        except Exception as e:
            return Error("Error on executing query. Query: ", self.query, 
                           "The error is: ", e)
        
        return Ok(records)

class TableIsEmptyCheck(DataQualityCheck):
    def __init__(self, table):
        super().__init__(f"SELECT COUNT(*) FROM {table}")
        self.table = table
    
    def perform_check(self, redshift) -> Result:
        result = self.execute_query(redshift)
        if(not result.is_ok()):
            return result
        
        records = result.unwrap()
        record_counts = records[0][0]
        if record_counts < 1:
            return Error(f"Data quality check failed: Table {self.table} is empty!")
        
        return Ok(f"Table {self.table} have {record_counts} records")

class QueryHaveExpectedResultCheck(DataQualityCheck):
    def __init__(self, query, expected_result):
        super().__init__(query)
        self.expected_result = expected_result

    def perform_check(self, redshift) -> Result:
        result = self.execute_query(redshift)
        if(not result.is_ok()):
            return result
        
        records = result.unwrap()
        record_counts = records[0][0]
        if record_counts != self.expected_result:
            return Error("Data quality check failed: ",
                             "query do not have the expected result",
                             "Query: ", self.query,
                             "Expected result: ", self.expected_result,
                             "Actual result: ", record_counts)
        
        return Ok(f"Query {self.query} have the expected result {self.expected_result}")