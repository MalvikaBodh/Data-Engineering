from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Description: Operator loads dimension tables by selecting data from staging tables
        
    Parameters redshift_conn_id = redshift endpoint used to connect to redshift cluster
        table = target table name in redshift
        truncate_data = truncate dimension tables if there is any existing data in them
        sql_query= sql_query used for selecting data from staging tables and loading them into the target table
    """
    ui_color = '#80BD9E'
    truncate_table = """
                     truncate table {};
                     """
    insert_sql = """
                 INSERT INTO {} {};
                 """
    ui_color = '#80BD9E'


    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "redshift",
                 table = "",
                 truncate_data = True,
                 sql_query = "",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate_data = truncate_data
        self.sql_query= sql_query

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate_data:
            redshift_hook.run(LoadDimensionOperator.truncate_table.format(self.table))
        redshift_hook.run(LoadDimensionOperator.insert_sql.format(self.table, self.sql_query))
        
        self.log.info('LoadDimensionOperator is now implemented')
    