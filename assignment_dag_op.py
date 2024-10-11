from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging

# Define the DAG
with DAG(
    dag_id='vantage_stock_data_pipeline_operator',
    default_args={'owner': 'saurabh_suman', 'start_date': days_ago(1)},
    schedule_interval='@daily',
    catchup=False,
    tags=['ETL', 'stock_data']
) as dag:

    @task
    def fetch_data(symbol='AAPL'):
        try:
            vantage_api = Variable.get('alpha_vantage_api_key')
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api}'
            response = requests.get(url)
            response.raise_for_status() 
            data = response.json()

            
            results = []
            for date, stock_info in data["Time Series (Daily)"].items():
                stock_info["date"] = date
                stock_info["symbol"] = symbol
                results.append(stock_info)

            logging.info(f"Fetched data successfully for {symbol}")
            return results

        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
            raise

    create_warehouse_db_schema = SnowflakeOperator(
        task_id='create_warehouse_db_schema',
        snowflake_conn_id='snowflake_conn',
        sql="""
        -- Create the warehouse if it doesn't exist
        CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
        WITH WAREHOUSE_SIZE = 'SMALL'
        AUTO_SUSPEND = 300
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE;

        -- Create the database and schema if they don't exist
        CREATE DATABASE IF NOT EXISTS vantage_db;
        CREATE SCHEMA IF NOT EXISTS vantage_db.api_schema;
        """,
        warehouse='COMPUTE_WH'
    )

    @task
    def create_table_and_insert_data(results):
        try:
            # Prepare the SQL insert queries dynamically from the fetched data
            insert_values = []
            for record in results:
                open_price = float(record['1. open'])
                high = float(record['2. high'])
                low = float(record['3. low'])
                close = float(record['4. close'])
                volume = int(record['5. volume'])
                date = record['date']
                symbol = record['symbol']

                insert_values.append(f"({open_price}, {high}, {low}, {close}, {volume}, '{date}', '{symbol}')")

            # Combine all inserts into one statement
            insert_sql = f"""
                INSERT INTO vantage_db.api_schema.vantage_api (open, high, low, close, volume, date, symbol)
                VALUES {', '.join(insert_values)};
            """

            # SQL for creating the table and inserting data
            table_insert_sql = f"""
            BEGIN TRANSACTION;

            USE DATABASE vantage_db;
            USE SCHEMA api_schema;

            -- Create the table if it doesn't exist
            CREATE OR REPLACE TABLE vantage_db.api_schema.vantage_api (
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume INT,
                date VARCHAR(512) PRIMARY KEY,
                symbol VARCHAR(20)
            );

            -- Insert data
            {insert_sql}

            COMMIT;
            """

            logging.info("Table creation and data insertion SQL prepared successfully.")
            return table_insert_sql

        except Exception as e:
            logging.error(f"Error in creating table or inserting data: {str(e)}")
            raise

    try:
        run_table_insert = SnowflakeOperator(
            task_id='run_table_insert',
            snowflake_conn_id='snowflake_conn',
            sql=create_table_and_insert_data(fetch_data('AAPL')),  # Generate the SQL dynamically
            database='vantage_db',
            schema='api_schema',
            warehouse='COMPUTE_WH'
        )
    except Exception as e:
        logging.error(f"Error running the table insert transaction: {str(e)}")
        raise

    try:
        validate_data = SnowflakeOperator(
            task_id='validate_data',
            snowflake_conn_id='snowflake_conn',
            sql="""
            USE DATABASE vantage_db;
            USE SCHEMA api_schema;
            USE WAREHOUSE COMPUTE_WH;
            SELECT COUNT(*), AVG(close) 
            FROM vantage_db.api_schema.vantage_api;
            """,
            warehouse='COMPUTE_WH'
        )
    except Exception as e:
        logging.error(f"Error validating the data: {str(e)}")
        raise

    # Define the workflow sequence
    stock_data = fetch_data(symbol='AAPL')
    create_warehouse_db_schema >> run_table_insert >> validate_data
