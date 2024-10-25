from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'saurabh',
    'start_date': days_ago(1),
}

dag_load = DAG(
    'snowflake_create_tables_and_load_data',
    default_args=default_args,
    description='Create tables if not exist and load data from S3 into Snowflake',
    schedule_interval=None,
    catchup=False,
)

create_tables_sql = """
CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel (
    userId int NOT NULL,
    sessionId varchar(32) PRIMARY KEY,
    channel varchar(32) DEFAULT 'direct'
);

CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp (
    sessionId varchar(32) PRIMARY KEY,
    ts timestamp
);

CREATE OR REPLACE STAGE dev.raw_data.blob_stage
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
"""

load_data_sql = """
COPY INTO dev.raw_data.user_session_channel
FROM @dev.raw_data.blob_stage/user_session_channel.csv;

COPY INTO dev.raw_data.session_timestamp
FROM @dev.raw_data.blob_stage/session_timestamp.csv;
"""

create_tables = SnowflakeOperator(
    task_id='create_tables',
    snowflake_conn_id='snowflake_conn',
    sql=create_tables_sql,
    dag=dag_load,
)

load_data = SnowflakeOperator(
    task_id='load_data_into_tables',
    snowflake_conn_id='snowflake_conn',
    sql=load_data_sql,
    dag=dag_load,
)

dag_summary = DAG(
    'elt_create_session_summary',
    default_args=default_args,
    description='Create session_summary table and insert records by joining two tables',
    schedule_interval=None,
    catchup=False,
)

create_table_task = SnowflakeOperator(
    task_id='create_session_summary_table',
    snowflake_conn_id='snowflake_conn',
    sql="""
    CREATE SCHEMA IF NOT EXISTS dev.analytics;
    CREATE TABLE IF NOT EXISTS dev.analytics.session_summary (
        userId INT,
        sessionId VARCHAR(32),
        channel VARCHAR(32),
        ts TIMESTAMP
    );
    """,
    dag=dag_summary,
)

insert_data_task = SnowflakeOperator(
    task_id='insert_into_session_summary',
    snowflake_conn_id='snowflake_conn',
    sql="""
    INSERT INTO dev.analytics.session_summary (userId, sessionId, channel, ts)
    SELECT 
        usc.userId, 
        usc.sessionId, 
        usc.channel, 
        st.ts
    FROM dev.raw_data.user_session_channel usc
    JOIN dev.raw_data.session_timestamp st 
    ON usc.sessionId = st.sessionId
    WHERE NOT EXISTS (
        SELECT 1 FROM dev.analytics.session_summary ss 
        WHERE ss.sessionId = usc.sessionId
    );
    """,
    dag=dag_summary,
)


create_tables >> load_data

create_table_task >> insert_data_task
