from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):
    """
    Description: Operator checks loaded dimension and fact tables for data quality. First checks for rows greater than 0 and then checks against expected redults
        
    Parameters redshift_conn_id = redshift endpoint used to connect to redshift cluster
    table_list = list of tables to check for rows greater than 0
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "redshift",
                 table_list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.table_list:
            records = redshift_hook.get_records(f"Select count(*) from {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                                 
            num_records = records[0][0]                                 
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
                                 
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")    