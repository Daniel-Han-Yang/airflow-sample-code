from datetime import datetime, timedelta
from os import getenv

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator

S3_BUCKET = getenv("S3_BUCKET", "test-bucket")
S3_KEY = getenv("S3_KEY", "key")
ATHENA_TABLE = getenv("ATHENA_TABLE", "test_table")
ATHENA_DATABASE = getenv("ATHENA_DATABASE", "default")
INPUT_FILES_PREFIX = f'{S3_KEY}/{{{{ ds_nodash }}}}/raw/'
ORC_FILES_PREFIX = f'{S3_KEY}/{{{{ ds_nodash }}}}/orc/'
OUTPUT_FILES_PREFIX = f'{S3_KEY}/{{{{ ds_nodash }}}}/output/'

QUERY_DROP_INPUT_TABLE = f"""
DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}}
"""

QUERY_DROP_ORC_TABLE = f"""
DROP TABLE IF EXISTS {ATHENA_DATABASE}.orc_{ATHENA_TABLE}_{{{{ ds_nodash }}}}
"""

QUERY_DROP_OUTPUT_TABLE = f"""
DROP TABLE IF EXISTS {ATHENA_DATABASE}.output_{ATHENA_TABLE}_{{{{ ds_nodash }}}}
"""

QUERY_CREATE_INPUT_TABLE = f"""
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
LOCATION 's3://{S3_BUCKET}/{INPUT_FILES_PREFIX}'
TBLPROPERTIES ("skip.header.line.count"="1")
"""

QUERY_CREATE_ORC_TABLE = f"""
CREATE TABLE {ATHENA_DATABASE}.orc_{ATHENA_TABLE}_{{{{ ds_nodash }}}}
WITH (
    format='ORC',
    orc_compression='SNAPPY',
    external_location = 's3://{S3_BUCKET}/{ORC_FILES_PREFIX}'
)
AS
select
*
from {ATHENA_DATABASE}.{ATHENA_TABLE}_{{{{ ds_nodash }}}};
"""

QUERY_CREATE_OUTPUT_TABLE = f"""
CREATE TABLE {ATHENA_DATABASE}.output_{ATHENA_TABLE}_{{{{ ds_nodash }}}}
WITH (
     format = 'TEXTFILE',
     external_location = 's3://{S3_BUCKET}/{OUTPUT_FILES_PREFIX}',
     field_delimiter = ','
)
as
select count(*) as num_passenger, survived, sex 
from {ATHENA_DATABASE}.orc_{ATHENA_TABLE}_{{{{ ds_nodash }}}} group by survived, sex
"""


with DAG(
        dag_id='example_athena',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=['example'],
        catchup=False,
        default_args={
            'database': ATHENA_DATABASE,
            'output_location': f's3://{S3_BUCKET}/{S3_KEY}/query_result/',
            'max_tries': None
        }
) as dag:

    with TaskGroup(group_id='overlay_table_on_input_files') as tg_overlay:
        # Create and Overlay Athena table on sources files
        drop_input_table = AthenaOperator(
            task_id='drop_input_table_if_exist',
            query=QUERY_DROP_INPUT_TABLE,
        )

        create_input_table = AthenaOperator(
            task_id='create_input_table',
            query=QUERY_CREATE_INPUT_TABLE,
        )

        drop_input_table >> create_input_table

    with TaskGroup(group_id='convert_file_to_orc') as tg_orc:
        # Optional - Convert to ORC for improved query processing speed.
        clear_orc_data = S3DeleteObjectsOperator(
            task_id='clear_orc_data',
            bucket=S3_BUCKET,
            prefix=ORC_FILES_PREFIX
        )

        drop_orc_table = AthenaOperator(
            task_id='drop_orc_table_if_exist',
            query=QUERY_DROP_ORC_TABLE,
        )

        create_orc_table = AthenaOperator(
            task_id='process_and_create_orc_table',
            query=QUERY_CREATE_ORC_TABLE,
        )

        clear_orc_data >> drop_orc_table >> create_orc_table

    with TaskGroup(group_id='process_files_using_ctas') as tg_output:
        # Read and transform ORC data using CTAS Query
        clear_output_data = S3DeleteObjectsOperator(
            task_id='clear_output_data',
            bucket=S3_BUCKET,
            prefix=OUTPUT_FILES_PREFIX
        )

        drop_output_table = AthenaOperator(
            task_id='drop_output_table_if_exist',
            query=QUERY_DROP_OUTPUT_TABLE,
        )

        process_create_output_table = AthenaOperator(
            task_id='process_and_create_output_table',
            query=QUERY_CREATE_OUTPUT_TABLE,
        )

        clear_output_data >> drop_output_table >> process_create_output_table

    tg_overlay >> tg_orc >> tg_output

