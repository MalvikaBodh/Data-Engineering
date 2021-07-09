from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Description: Operator loads fact table by selecting data from staging table
        
    Parameters redshift_conn_id = redshift endpoint used to connect to redshift cluster
        table = target table name in redshift
        sql_query= sql_query used for copying data over to the target table
    """    
    ui_color = '#F98866'
    insert_sql = """
                 INSERT INTO {} {};
                 """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "redshift",
                 table = "",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query= sql_query
        
        
    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(LoadFactOperator.insert_sql.format(self.table, self.sql_query))
        
        self.log.info('LoadFactOperator is now implemented')