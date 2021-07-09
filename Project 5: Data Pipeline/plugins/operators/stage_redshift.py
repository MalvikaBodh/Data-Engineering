from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Description: Operator loads json song and log data from S3 bucket and stores it on redshift tables
        
    Parameters redshift_conn_id = redshift endpoint used to connect to redshift cluster
        aws_credentials_id = AWS key and secret Access key ID and Secret access key
        table = target table name in redshift
        s3_bucket = s3_bucket hlding json data files
        s3_key = folder location in s3
        copy_json_option = json data file format type r location
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT as json '{}'
        TIMEFORMAT AS 'epochmillisecs'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="auto",
                 *args, **kwargs):


        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
        """ 
        Deleting existing tables from the database
        """
        
        redshift.run("DELETE FROM {}".format(self.table))
        
        """
        Copying data from S3 into a staging location in Redshift
        """
        self.log.info("Staging JSON data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
        self.table,
        s3_path,
        credentials.access_key,
        credentials.secret_key,
        self.copy_json_option
        )
        redshift.run(formatted_sql)


