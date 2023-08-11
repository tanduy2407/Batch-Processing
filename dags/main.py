from etl_staging import download_files, extract, transform, load, init_spark, generate_dim_table
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt.datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'spark_dag',
    default_args=default_args,
    description='ETL with Spark',
    schedule_interval=dt.timedelta(minutes=50),
)


def etl_main():
	download_files()
	spark = init_spark()
	dfs, df_zone = extract(spark)
	dim_dfs = generate_dim_table(dfs[0])
	dim_tables_name = ['payment', 'ratecode', 'vendor']
	for tbls, name in zip(dim_dfs, dim_tables_name):
		load(tbls, name)
	load(df_zone, 'taxi_zone_lookup')

	tables_name = ['green_taxi', 'yellow_taxi']
	for df, name in zip(dfs, tables_name):
		transform(df)
		load(df, name)


with dag:
    etl = PythonOperator(
        task_id='etl_with_spark',
        python_callable=etl_main,
        dag=dag,
    )
    etl
