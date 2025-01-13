from snowflake import connector
from snowflake.connector import pandas_tools
import pandas as pd


def build_snowflake_connector(secret, database=None, schema=None):
    database = secret['DATABASE'] if database is None else database
    schema = secret['SCHEMA'] if schema is None else schema
    ctx = connector.connect(
        user=secret['USERNAME'],
        password=secret['PASSWORD'],
        account=secret['ACCOUNT'],
        warehouse=secret['WAREHOUSE'],
        database=database,
        schema=schema,
        role=secret['ROLE'])
    return ctx

def retrieve_query_res(query, ctx):
    cur = ctx.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    return df


def execute_query(query, ctx):
    cur = ctx.cursor()
    cur.execute(query)


def not_all_uppercase_letters(s):
    # Filter only alphabetic characters and check if they are uppercase
    return any([char.islower() for char in s if char.isalpha()])


def get_table_description(table_name, ctx):
    cur = ctx.cursor()
    cur.execute(f'DESCRIBE TABLE {table_name};')
    df = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
    names_to_quote = df['name'].apply(not_all_uppercase_letters).values
    # print(names_to_quote)
    df.loc[names_to_quote, 'name'] = '"' + df.loc[names_to_quote, 'name'] + '"'
    return df


def insert_dataframe(table_name: str, df: pd.DataFrame, ctx, create_table=True, append_table=True,
                     verbose=True):
    table_statement = pd.io.sql.get_schema(df, table_name).replace('CREATE', 'CREATE OR REPLACE')
    # table_statement = table_statement.replace(table_name, f'"{table_name}"')
    if create_table:
        if append_table:
            table_statement = table_statement.replace('CREATE OR REPLACE TABLE', 'CREATE TABLE IF NOT EXISTS')
        if verbose:
            print('Executing ', table_statement)
        execute_query(table_statement, ctx)

    success, nchunks, nrows, _ = pandas_tools.write_pandas(ctx, df, table_name, use_logical_type=True)
    if verbose:
        print('Wrote ', nrows, ' rows of data')


def check_if_table_exists(qual_table_name, ctx):
    exists = False
    try:
        df = retrieve_query_res(f'SELECT * FROM {qual_table_name} LIMIT 1;', ctx)
        exists = True
    except:
        exists = False
    return exists


def get_column_names_and_types(table_name, ctx):
    df = get_table_description(table_name, ctx)
    column_names = list(df['name'])
    column_types = list(df['type'])
    return column_names, column_types

class SFUtils:

    res = None
    acc = None
    global_namespace = None

    def __init__(self, params):
        self.params = params
        self.warehouse = params['WAREHOUSE']
        self.role = params['ROLE']
        self.schema = params['SCHEMA']
        self.database = params['DATABASE']
        self.username = params['USERNAME']
        self.password = params['PASSWORD']
        self.account = params['ACCOUNT']

    def switch_schema(self, schema: str):
        self.schema = schema

    def switch_database(self, database: str):
        self.database = database

    def get_conection(self, params=None):
        if params is None:
            params = {}
        user = params.get('USERNAME', self.username)
        password = params.get('PASSWORD', self.password)
        account = params.get('ACCOUNT', self.account)
        warehouse = params.get('WAREHOUSE', self.warehouse)
        role = params.get('ROLE', self.role)
        schema = params.get('SCHEMA', self.schema)
        database = params.get('DATABASE', self.database)
        conn = connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            warehouse=warehouse,
            role=role,
            schema=schema)
        return conn

    def get_cursor(self, params=None):
        if params is None:
            params = {}
        conn = self.get_conection(params)
        cursor = conn.cursor()
        return cursor

    def retrieve_query_res(self, query: str, **kwargs) -> pd.DataFrame:
        conn = self.get_conection(kwargs)
        curr = conn.cursor()
        curr.execute(f'{query}')
        df = curr.fetch_pandas_all()
        curr.close()
        conn.close()
        return df

    def execute_query(self, query: str, **kwargs):
        print(f'Executing')
        conn = self.get_conection(kwargs)
        curr = conn.cursor()
        curr.execute(query)
        curr.close()
        conn.close()

    def get_table_info(self, table: str, is_view=False, name_qualified=False, **kwargs):
        conn = self.get_conection(kwargs)
        table_name = table
        if not name_qualified:
            table_name = f'{self.database}.{self.schema}.{table}'
        print('Getting info for ', table_name)
        query = f'DESCRIBE {"VIEW" if is_view else "TABLE"} {table_name}'
        curr = conn.cursor()
        print('Executing ', query)
        curr.execute(query)
        rows = []
        columns = [d[0] for d in curr.description]
        print('Processing table with ', len(columns), 'columns')
        for row in curr:
            rows.append(row)
        print('Processed ', len(rows), ' rows')
        curr.close()
        conn.close()
        df = pd.DataFrame(rows, columns=columns)
        return df

    def insert_dataframe(self, table_name: str, df: pd.DataFrame, create_table=True, append_table=True,
                         verbose=True, **kwargs):
        with self.get_conection(kwargs) as ctx:
            insert_dataframe(table_name, df, ctx, create_table, append_table, verbose)


def create_snowflake_accessor(params: dict, gl) -> SFUtils:
    """

    Parameters
    ----------
    params : dict
        Dictionary containing the credentials to connect to the snowflake data lake. Contains the fields
        USERNAME, PASSWORD, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA, ROLE
    gl : dict
        Dictionary of globally accessible objects and functions

    Returns
    -------
    acc : SnowflakeTableAccessorV2
        SnowFlakeAccessor object to facilitate queries on the database with credentials mentioned in params

    """
    acc = SFUtils(params)
    SFUtils.acc = acc
    SFUtils.global_namespace = gl
    # SQL_CACHE['acc'] = acc
    return acc