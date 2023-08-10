from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from etl_staging import start_spark, load
import json


def read_data(spark, table_name:str) -> DataFrame:
	with open('config/config_production.json', 'r') as config_file:
		config_data = json.load(config_file)
		host = config_data['host']
		port = config_data['port']
		database = config_data['database']
		user = config_data['user']
		password = config_data['password']

	url = 'jdbc:postgresql://{0}:{1}/{2}'.format(host, port, database)
	properties = {
		'user': user,
		'password': password,
		'driver': 'org.postgresql.Driver'}
	df = spark.read.jdbc(url=url, table=table_name, properties=properties)
	print(f'Read data from {table_name} successfully!')
	return df


def join_data(left, right, left_on, right_on, how='inner'):
	df = left.join(right, left[left_on] == right[right_on], how)
	return df


def etl_production():
	spark = start_spark()
	green_taxi = read_data(spark, 'green_taxi')
	yellow_taxi = read_data(spark, 'yellow_taxi')
	taxi_zone = read_data(spark, 'taxi_zone_lookup')

	pass