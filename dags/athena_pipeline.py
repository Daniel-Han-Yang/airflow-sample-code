from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

S3_BUCKET = getenv("S3_BUCKET", "test-bucket")
S3_KEY = getenv("S3_KEY", "key")
ATHENA_TABLE = getenv("ATHENA_TABLE", "test_table")
ATHENA_DATABASE = getenv("ATHENA_DATABASE", "default")

QUERY_DROP_TABLE = f"""
DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}}
"""

QUERY_DROP_OUTPUT_TABLE = f"""
DROP TABLE IF EXISTS output_{ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}}
"""

QUERY_CREATE_TABLE = f"""
CREATE EXTERNAL TABLE {ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}}(
PassengerId string,
Survived string,
Pclass string,
Name string,
Sex string,
Age string,
SibSp string,
Parch string,
Ticket string,
Fare string,
Cabin string,
Embarked string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://{S3_BUCKET}/{S3_KEY}/raw/'
TBLPROPERTIES ("skip.header.line.count"="1")
"""

QUERY_PROCESS_TABLE = f"""
CREATE TABLE output_{ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}}
WITH (
     format = 'TEXTFILE',
     external_location = 's3://{S3_BUCKET}/{S3_KEY}/output/',
     field_delimiter = ',',
as
select count(*) as num_passenger, survived, sex 
from {ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}} group by survived, sex
"""


with DAG(
        dag_id='example_athena',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=['example'],
        catchup=False,
) as dag:

    delete_table = AthenaOperator(
        task_id='delete_input_table_if_exist',
        query=QUERY_DROP_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        max_tries=None,
    )

    create_table = AthenaOperator(
        task_id='create_input_table',
        query=QUERY_CREATE_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        max_tries=None,
    )

    # Convert to ORC for processing speed.

    delete_output_table = AthenaOperator(
        task_id='delete_output_table_if_exist',
        query=QUERY_DROP_OUTPUT_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        max_tries=None,
    )

    process_table = AthenaOperator(
        task_id='process_and_create_output_table',
        query=QUERY_PROCESS_TABLE,
        database=ATHENA_DATABASE,
        output_location=f's3://{S3_BUCKET}/{S3_KEY}',
        max_tries=None,
    )

    delete_table >> create_table >> delete_output_table >> process_table

