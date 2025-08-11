import pandas as pd
import snowflake.connector
from sqlalchemy import create_engine
import os




def read_google_sheet(sheet_id, gid=None):

    if gid:
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}'
    else:
        url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
    
    try:
        return pd.read_csv(url)
    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        raise

def create_snowflake_connection_sqlalchemy():
    # Creates a Snowflake connection using SQLAlchemy engine.

    user = os.getenv('DBT_SECRET__USER')
    password = os.getenv('PASSWORD')
    account = "ah02912.eu-west-2.aws"
    warehouse = "SCHEDULER_WH"
    database = "ANALYTICS_DB"
    schema = "FIN_DATA"
    
    # Create SQLAlchemy connection string
    connection_string = f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}"
    
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    return engine

def create_snowflake_connection_direct():
    conn = snowflake.connector.connect(
        user=os.getenv('DBT_SECRET__USER'),
        password=os.getenv('PASSWORD'),
        account="ah02912.eu-west-2.aws",
        warehouse="SCHEDULER_WH",
        database="ANALYTICS_DB",
        schema="FIN_DATA"
    )
    
    return conn

def load_data_to_snowflake(sheet_id, table_name, conn, gid=None):
    
    df = read_google_sheet(sheet_id, gid=gid)

    # Writing to snowflake table, using CREATE/REPLACE strategy
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print(f"Data loaded into Snowflake table {table_name} successfully.")


if __name__ == "__main__":
    
    #inputs
    sheet_id = input("Enter Google Sheet ID: ")
    table_name = input("Enter Snowflake table name: ")
    gid = input("Enter sheet GID (press ENTER for first sheet in the document): ")

    #connection
    conn = create_snowflake_connection_sqlalchemy() 

    #loading data
    load_data_to_snowflake(
        sheet_id=sheet_id,
        table_name=table_name,
        conn=conn,
        gid=gid
    )