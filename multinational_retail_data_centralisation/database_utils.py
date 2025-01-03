import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
# import pandas as pd

# class DatabaseConnector will use to connect and upload tables to the database
class DatabaseConnector:

    def read_db_creds(self, file):
        """ reads the credentials yaml file and returns a dictionary of the credentials """
        with open(file, 'r') as f:
            creds_data = yaml.safe_load(f)
        return creds_data

    def init_db_engine(self):
        """ this method will read the credentials from the return of read_db_creds method and initialise and
        return an sqlalchemy database engine """
        creds_data = self.read_db_creds('db_creds.yaml') # reads the credentials
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_data['RDS_HOST']
        USER = creds_data['RDS_USER']
        PASSWORD = creds_data['RDS_PASSWORD']
        PORT = creds_data['RDS_PORT']
        DATABASE = creds_data['RDS_DATABASE']        
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")        
        engine.connect()
        return engine

    def list_db_tables(self):
        """ this method will list all tables in the database so that the data can be extract from the relevent tables """
        engine = self.init_db_engine()
        inspector = inspect(engine)
        table_name = inspector.get_table_names()
        return table_name

    def upload_to_db(self, df, table_name,):
        """ takes pandas DataFrame and table name to upload to as an argument
        use the upload_to_db method to store the data in your sales_data database
        in a table named dim_users """
        creds_data = self.read_db_creds('local_creds.yaml')
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds_data['LOCAL_HOST']
        USER = creds_data['LOCAL_USER']
        PASSWORD = creds_data['LOCAL_PASSWORD']
        PORT = creds_data['LOCAL_PORT']
        DATABASE = creds_data['LOCAL_DATABASE']
        localengine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        localengine.connect()
        df.to_sql(name=table_name, con=localengine, if_exists='replace')
        print('Table Uploaded')

if __name__ == '__main__':
    conn = DatabaseConnector()
