from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from etl_staging import start_spark, load
import json


def read_data(spark, schema: str, table_name:str) -> DataFrame:
	with open('config/config.json', 'r') as config:
		config_data = json.load(config)
		host = config_data[schema]['host']
		port = config_data[schema]['port']
		database = config_data[schema]['database']
		user = config_data[schema]['user']
		password = config_data[schema]['password']

	url = 'jdbc:postgresql://{0}:{1}/{2}'.format(host, port, database)
	properties = {
		'user': user,
		'password': password,
		'driver': 'org.postgresql.Driver'}
	df = spark.read.jdbc(url=url, table=table_name, properties=properties)
	print(f'Read data from {table_name} successfully!')
	return df


def join_data(left, right, left_on: str, right_on: str, how='inner') -> DataFrame:
	df = left.join(right, left[left_on] == right[right_on], how)
	return df


def etl_production():
	spark = start_spark()
	schema_staging = 'staging'
	green_taxi = read_data(spark, schema_staging, table_name='green_taxi')
	yellow_taxi = read_data(spark, schema_staging, table_name='yellow_taxi')
	taxi_zone = read_data(spark, schema_staging, table_name='taxi_zone_lookup').select('LocationID', 'Zone', 'Borough')
	join_data_green = join_data(green_taxi, taxi_zone, 'PULocationID', 'LocationID')
	join_data_green = join_data_green.withColumnRenamed('Zone', 'pickup_zone').withColumnRenamed('Borough', 'pickup_borough')
	join_data_green = join_data(green_taxi, taxi_zone, 'DOLocationID', 'LocationID')
	join_data_green = join_data_green.withColumnRenamed('Zone', 'dropoff_zone').withColumnRenamed('Borough', 'dropoff_borough')	

	join_data_yellow = join_data(yellow_taxi, taxi_zone, 'PULocationID', 'LocationID')
	join_data_yellow = join_data_yellow.withColumnRenamed('Zone', 'pickup_zone').withColumnRenamed('Borough', 'pickup_borough')
	join_data_yellow = join_data(yellow_taxi, taxi_zone, 'DOLocationID', 'LocationID')
	join_data_yellow = join_data_yellow.withColumnRenamed('Zone', 'dropoff_zone').withColumnRenamed('Borough', 'dropoff_borough')	
	schema_production = 'production'

	pass