import tempfile
import datetime as dt
import csv
import os
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 6,
    "retry_delay": dt.timedelta(minutes=10),
}

GET_ROUTES_QUERY = """
SELECT replace(trolz.departure_zone,'#') AS plant, 
        trolz.transportation_zone, 
        trolz.shipping_condition,
        trolz.route,
        tvrot.bezei AS route_description,
        coalesce(tvro.distz,0.0) AS distance,
        tvro.medst AS distance_uom,
        trolz.refresh_date,
        CAST(ROUND(coalesce(tvro.traztd,0)/240000,0) as integer) AS Transit_days,
        CAST(ROUND(coalesce(tvro.tdvztd,0)/240000,0) as integer) AS Log_Lead_time
FROM db_logistics.vw_global_trolz_parquet AS trolz
LEFT JOIN db_logistics.tb_global_tvrot_parquet AS tvrot
    ON tvrot.spras = 'E'
        AND tvrot.route = trolz.route
LEFT JOIN db_logistics.tb_global_tvro_parquet AS tvro 
    ON tvro.route = trolz.route 
WHERE trolz.route <> '999999' 
"""
PG_LOAD_ROUTES_SQL = """
insert into sales.transportation_zones (
plant,
transportation_zone,
shipping_condition,
route,
route_description,
distance,
distance_uom,
refresh_date,
transit_days,
log_lead_time)
select 
plant,
transportation_zone, 
shipping_condition, 
route, 
route_description, 
distance, 
distance_uom, 
refresh_date,
transit_days,
log_lead_time
from sales.transportation_zones_staging
"""

BUCKET_NAME = "gln-airflow"
ROUTE_FILENAME = "gln_routes"


def load_athena_to_postgres(**kwargs):
    s3_hook = S3Hook("aws_default")
    # get folder in s3 bucket
    bucket_prefix = kwargs["p_buckpref"]
    the_folder = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=bucket_prefix)
    select_statement = "SELECT * FROM S3Object s"
    select_result = s3_hook.select_key(
        key=the_folder[0],
        bucket_name=BUCKET_NAME,
        expression=select_statement,
        input_serialization={"CSV": {"FileHeaderInfo": "USE"}},
        output_serialization={"CSV": {"RecordDelimiter": "\n", "FieldDelimiter": "\t"}},
    )
    fname = kwargs["p_filename"]
    with tempfile.NamedTemporaryFile(
        mode="w+b", delete=False, suffix=".csv", prefix=fname, dir="/tmp"
    ) as temp:
        with open(temp.name, "w") as f:
            for line in select_result:
                f.write(line)
    print("The CSV File of Routes is: ", temp.name)
    pg_staging_table = kwargs["p_staging_table"]
    pg_target_table = kwargs["p_target_table"]
    pg_target_sql = kwargs["p_target_sql"]
    pg_hook = PostgresHook(postgres_conn_id="postgres_sales", supports_autocommit=True)
    pg_hook.run(sql=f"truncate table {pg_staging_table};", autocommit=True)
    pg_hook.run(sql=f"truncate table {pg_target_table};", autocommit=True)
    pg_hook.bulk_load(table=f"{pg_staging_table}", tmp_file=temp.name)
    print("Postgres sales.transportation_zones_staging loaded")
    pg_hook.run(sql=pg_target_sql, autocommit=True)


with DAG(
    dag_id="athena_to_pg_dag_v1",
    description="Commercial DAG",
    start_date=dt.datetime(2019, 7, 29),
    schedule_interval="55 11 * * 1-5",
    default_args=DEFAULT_ARGS,
    catchup=False,
) as dag:

    start_task = DummyOperator(task_id="start_task")

    get_routes_data_task = AWSAthenaOperator(
        task_id="get_routes_data_task",
        aws_conn_id="aws_default",
        query=GET_ROUTES_QUERY,
        database="db_logistics",
        output_location=f"s3://gln-airflow/commercial/athena-routes-data/{dt.datetime.now():%Y-%m-%d}",
    )

    load_routes_task = PythonOperator(
        task_id="load_routes_task",
        python_callable=load_athena_to_postgres,
        op_kwargs={
            "p_filename": ROUTE_FILENAME,
            "p_buckpref": f"commercial/athena-routes-data/{dt.datetime.now():%Y-%m-%d}",
            "p_staging_table": "sales.transportation_zones_staging",
            "p_target_table": "sales.transportation_zones",
            "p_target_sql": PG_LOAD_ROUTES_SQL,
        },
    )

    start_task.set_downstream(get_routes_data_task)
    get_routes_data_task.set_downstream(load_routes_task)
