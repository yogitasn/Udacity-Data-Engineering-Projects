from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql_parquet = """
        COPY {}
        FROM '{}'
        IAM_ROLE '{}'
        FORMAT AS PARQUET;
    """
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        CSV
        IGNOREHEADER {}
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 create_stmt="",
                 IAM_ROLE="",
                 file_format="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.create_stmt=create_stmt
        self.IAM_ROLE=IAM_ROLE
        self.file_format=file_format
       
    def execute(self, context):
        #IAM_ROLE='arn:aws:iam::972068528963:user/dwhadmin'
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.create_stmt)
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
         
        if self.file_format=='parquet':
            formatted_sql_parquet = StageToRedshiftOperator.copy_sql_parquet.format(
                    self.table,
                    s3_path,
                    self.IAM_ROLE
             )
            redshift.run(formatted_sql_parquet)
        else:
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                    self.table,
                    s3_path,
                    credentials.access_key,
                    credentials.secret_key,
                    self.ignore_headers
            )
            redshift.run(formatted_sql)
