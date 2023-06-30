import pyodbc
from sqlalchemy import create_engine #better for writting dfs to db
from sqlalchemy.engine.url import URL
#import mysql.connector

class DatabaseFactory:
    def __init__(self, default_type=None, default_host=None, default_username=None, default_password=None, default_database=None, default_driver='SQL SERVER'):
        self.default_type = default_type
        self.default_host = default_host
        self.default_username = default_username
        self.default_password = default_password
        self.default_database = default_database
        self.default_driver = default_driver

    def create_connection(self, database_type=None, host=None, username=None, password=None, database=None, driver=None):
        """
        Creates a database connection based on the specified database type.

        Parameters:
            database_type (str): The type of database to connect to. Defaults to the default_type specified in the constructor.
            host (str): The hostname or IP address of the database server. Defaults to the default_host specified in the constructor.
            username (str): The username to use when connecting to the database. Defaults to the default_username specified in the constructor.
            password (str): The password to use when connecting to the database. Defaults to the default_password specified in the constructor.
            database (str): The name of the database to connect to. Defaults to the default_database specified in the constructor.
            driver (str): The ODBC driver to use for connecting to the database. Only used for MSSQL connections. Defaults to the default_driver specified in the constructor.

        Returns:
            A database connection object.
        """
        database_type = database_type or self.default_type
        host = host or self.default_host
        username = username or self.default_username
        password = password or self.default_password
        database = database or self.default_database
        driver = driver or self.default_driver

        if database_type == 'mssql':
            if not username or not password:#try trusted connection
                conn_str = f"DRIVER={{{driver}}};SERVER={host};DATABASE={database};Trusted_Connection=yes;"
            else:
                conn_str = f"DRIVER={{{driver}}};SERVER={host};DATABASE={database};UID={username};PWD={password}"
            #return pyodbc.connect(conn_str)
            self.connection = pyodbc.connect(conn_str)
            return self
        #elif database_type == 'mysql':
            #return mysql.connector.connect(host=host, user=username, password=password, database=database)
        else:
            raise ValueError("Invalid database type. Must be 'mssql' or 'mysql'.")
    def close(self):
        self.connection.close()
    def commit(self):
        self.connection.commit()
    def execute(self,query):
        """
        Executes a query against the database.
        """
        print(query)
        if self.default_type == 'mssql':
            cursor = self.connection.cursor()
            cursor.execute(query)
            return cursor
        else:
            raise ValueError(f'Invalid database type. {self.default_type} not implemented yet,')
    
    def write_df(self,df,database_name=None,table_name=None):
        """
        Writes a dataframe to a database table.
        """
        #print("writing results")
        if self.default_type == 'mssql':
            conn_str ='DRIVER={SQL Server};SERVER='+self.default_host+';DATABASE='+(database_name or self.default_database)+';ENCRYPT=no;UID='+self.default_username+';PWD='+self.default_password
            conn_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
            engine = create_engine(conn_url, fast_executemany=True)
            df.to_sql(name=table_name, schema="dbo", con = engine, index = False, if_exists = 'append',chunksize=5000)
        else:
            raise ValueError(f'Invalid database type. {self.default_type} not implemented yet,')

'''
factory = DatabaseFactory(default_type='mssql', default_host='localhost', default_username='myuser', default_password='mypassword', default_database='mydatabase', default_driver='{ODBC Driver 17 for SQL Server}')
connection = factory.create_connection()
'''