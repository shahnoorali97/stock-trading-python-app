import requests
import os
from dotenv import load_dotenv
load_dotenv()
import snowflake.connector
from datetime import datetime
import time
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

LIMIT = 1000
DS = '2025-10-16'


def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}'
    response = requests.get(url)
    tickers = []

    # Check if the request was successful
    if response.status_code == 429:
        print("Rate limit exceeded on initial request. Waiting 60 seconds before retrying...")
        time.sleep(60)
        response = requests.get(url)
    
    if response.status_code != 200:
        print(f"API request failed with status code: {response.status_code}")
        print(f"Response: {response.text}")
        return

    data = response.json()
    print("Full response structure:")
    print(data)

    # Check if the response has the expected structure
    if 'results' not in data:
        print("No 'results' key found in response. Available keys:")
        print(list(data.keys()))
        return

    print(f"Next URL: {data.get('next_url', 'None')}")
    for ticker in data['results']:
        ticker['data_date'] = DS
        tickers.append(ticker)

    while 'next_url' in data and data['next_url']:
        print('requesting next page', data['next_url'])
        
        # Add rate limiting - wait 1 second between requests
        time.sleep(1)
        
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        
        if response.status_code == 429:
            print("Rate limit exceeded. Waiting 60 seconds before retrying...")
            time.sleep(60)
            response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        
        if response.status_code != 200:
            print(f"Next page request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            break
            
        data = response.json()
        print("Next page response:")
        print(data)
        
        if 'results' not in data:
            print("No 'results' key found in next page response")
            break
            
        for ticker in data['results']:
            ticker['data_date'] = DS
            tickers.append(ticker)

    example_ticker =  {'ticker': 'ZWS', 
        'name': 'Zurn Elkay Water Solutions Corporation', 
        'market': 'stocks', 
        'locale': 'us', 
        'primary_exchange': 'XNYS', 
        'type': 'CS', 
        'active': True, 
        'currency_name': 'usd', 
        'cik': '0001439288', 
        'composite_figi': 'BBG000H8R0N8', 	'share_class_figi': 'BBG001T36GB5', 	
        'last_updated_utc': '2025-09-11T06:11:10.586204443Z',
        'data_date': '2025-09-25'
        }

    fieldnames = list(example_ticker.keys())

    # Load to Snowflake instead of CSV
    load_to_snowflake(tickers, fieldnames)
    print(f'Loaded {len(tickers)} rows to Snowflake')



def load_to_snowflake(rows, fieldnames):
    # Build connection kwargs from environment variables
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
    }
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    if account:
        connect_kwargs['account'] = account

    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    database = os.getenv('SNOWFLAKE_DATABASE')
    schema = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    if warehouse:
        connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if schema:
        connect_kwargs['schema'] = schema
    if role:
        connect_kwargs['role'] = role

    print("Snowflake connection parameters:")
    print(f"User: {connect_kwargs.get('user', 'NOT SET')}")
    print(f"Account: {connect_kwargs.get('account', 'NOT SET')}")
    print(f"Database: {connect_kwargs.get('database', 'NOT SET')}")
    print(f"Schema: {connect_kwargs.get('schema', 'NOT SET')}")
    print(f"Warehouse: {connect_kwargs.get('warehouse', 'NOT SET')}")
    print(f"Role: {connect_kwargs.get('role', 'NOT SET')}")
    print(f"Password: {'*' * len(connect_kwargs.get('password', '')) if connect_kwargs.get('password') else 'NOT SET'}")
    
    conn = snowflake.connector.connect( 
        user=connect_kwargs['user'],
        password=connect_kwargs['password'],
        account=connect_kwargs['account'],
        database=connect_kwargs['database'],
        schema=connect_kwargs['schema'],
        role=connect_kwargs['role'],
        session_parameters={
        "CLIENT_TELEMETRY_ENABLED": False,
        }
    )
    try:
        cs = conn.cursor()
        try:
            table_name = os.getenv('SNOWFLAKE_TABLE', 'stock_tickers')

            # Define typed schema based on example_ticker
            type_overrides = {
                'ticker': 'VARCHAR',
                'name': 'VARCHAR',
                'market': 'VARCHAR',
                'locale': 'VARCHAR',
                'primary_exchange': 'VARCHAR',
                'type': 'VARCHAR',
                'active': 'BOOLEAN',
                'currency_name': 'VARCHAR',
                'cik': 'VARCHAR',
                'composite_figi': 'VARCHAR',
                'share_class_figi': 'VARCHAR',
                'last_updated_utc': 'TIMESTAMP_NTZ',
                'data_date': 'VARCHAR'
            }
            columns_sql_parts = []
            for col in fieldnames:
                col_type = type_overrides.get(col, 'VARCHAR')
                columns_sql_parts.append(f'"{col.upper()}" {col_type}')

            # First, try to drop the table if it exists to ensure clean schema
            drop_table_sql = f'DROP TABLE IF EXISTS {table_name}'
            print(f"Dropping existing table: {drop_table_sql}")
            cs.execute(drop_table_sql)
            
            create_table_sql = f'CREATE TABLE {table_name} ( ' + ', '.join(columns_sql_parts) + ' )'
            print(f"Creating table with SQL: {create_table_sql}")
            cs.execute(create_table_sql)

            column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
            placeholders = ', '.join([f'%({c})s' for c in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ( {column_list} ) VALUES ( {placeholders} )'

            # Conform rows to fieldnames - ensure all field names are present
            transformed = []
            for t in rows:
                row = {}
                for k in fieldnames:
                    row[k] = t.get(k, None)
                transformed.append(row)

            if transformed:
                cs.executemany(insert_sql, transformed)
        finally:
            cs.close()
    finally:
        conn.close()


if __name__ == '__main__':
    run_stock_job()