from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                     # Define your operators params (with defaults) here
                     # Example:
                     # conn_id = your-connection-name
                    create_stmt,
                    insert_stmt,
                    table="",
                    redshift_conn_id="",
                    append_data="",
                   *args, **kwargs):

            super(LoadDimensionOperator, self).__init__(*args, **kwargs)
            self.create_stmt = create_stmt
            self.insert_stmt=insert_stmt
            self.table_name = table
            self.redshift_conn_id = redshift_conn_id
            self.append_data=append_data
            
    def execute(self, context):
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            redshift.run(self.create_stmt)
            if self.append_data == True:
                sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.insert_stmt)
                redshift.run(sql_statement)
            else:
                sql_statement = 'DELETE FROM %s' % self.table_name
                redshift.run(sql_statement)
                
            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.insert_stmt)
            redshift.run(sql_statement)             
            