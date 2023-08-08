import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json


year = 2022


def download_files():
	try:
		os.makedirs('driver')
		os.makedirs('data/green_taxi')
		os.makedirs('data/yellow_taxi')
	except OSError as err:
		print(f'Error: {err}')

	os.system(
		'wget -O driver/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar')
	os.system(
		'wget -Odata/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')
	
	print('Start download parquet files')
	for i in range(1, 3):
		month = f'{i:02}'
		url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{0}-{1}.parquet'.format(
			year, month)
		csv_name = 'data/green_taxi/green_tripdata_{0}-{1}.parquet'.format(
			year, month)
		os.system(f'wget -O {csv_name} {url}')

	for i in range(1, 3):
		month = f'{i:02}'
		url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{0}-{1}.parquet'.format(
			year, month)
		csv_name = 'data/yellow_taxi/yellow_tripdata_{0}-{1}.parquet'.format(
			year, month)
		os.system(f'wget -O {csv_name} {url}')
	print('Download successfully')


def init_spark():
	spark = SparkSession.builder \
		.appName("ETL-with-spark") \
		.config("spark.jars", "driver/postgresql-42.6.0.jar") \
		.getOrCreate()
	return spark


def extract(spark):
	green_taxi = spark.read.parquet('data/green_taxi')
	yellow_taxi = spark.read.parquet('data/yellow_taxi')
	df_zone = spark.read.csv('data/taxi_zone_lookup.csv',
							 inferSchema=True, header=True).filter(col('Borough') != 'Unknown')
	return [green_taxi, yellow_taxi], df_zone


def generate_dim_table(df):
	for i in ['passenger_count', 'RateCodeID', 'payment_type', 'trip_type']:
		df = df.withColumn(i, col(i).cast(IntegerType()))
	payment_type_mapping = when(col('id') == 1, 'Credit card'). \
		when(col('id') == 2, 'Cash'). \
		when(col('id') == 3, 'No charge'). \
		when(col('id') == 4, 'Dispute'). \
		when(col('id') == 5, 'Unknown').otherwise('Voided trip')
	df_payment = df.select('payment_type').withColumnRenamed(
		'payment_type', 'id').distinct().dropna().orderBy(col('id'))
	df_payment = df_payment.withColumn('payment_type', payment_type_mapping)

	rate_code_mapping = when(col('id') == 1, 'Standard rate'). \
		when(col('id') == 2, 'JFK'). \
		when(col('id') == 3, 'Newark'). \
		when(col('id') == 4, 'Nassau or Westchester'). \
		when(col('id') == 5, 'Negotiated fare').otherwise('Group ride')
	df_ratecode = df.select('RateCodeID').withColumnRenamed(
		'RateCodeID', 'id').distinct().dropna().orderBy(col('id'))
	df_ratecode = df_ratecode.withColumn('rate_type', rate_code_mapping)

	df_vendor = df.select('VendorID').withColumnRenamed(
		'VendorID', 'id').distinct().dropna().orderBy(col('id'))
	df_vendor = df_vendor.withColumn('vendor', when(col('id') == 1, 'Creative Mobile Technologies, LLC').
									 when(col('id') == 2, 'VeriFone Inc.'))
	return [df_payment, df_ratecode, df_vendor]


def transform(df):
	trip_time_in_mins = unix_timestamp(
		col('lpep_dropoff_datetime')) - unix_timestamp(col('lpep_pickup_datetime'))
	df = df.withColumn('trip_time_in_mins', round(trip_time_in_mins / 60, 2))
	return df


def load(df, table_name: str):
	# Read credentials from the config file
	with open('config/config.json', 'r') as config_file:
		config_data = json.load(config_file)
	host = config_data['host']
	port = config_data['port']
	print(host, port)
	database = config_data['database']
	user = config_data['user']
	password = config_data['password']

	url = 'jdbc:postgresql://{0}:{1}/{2}'.format(host, port, database)
	properties = {
		'user': user,
		'password': password,
		'driver': 'org.postgresql.Driver'}
	df.write.jdbc(url=url, table=table_name, mode='overwrite',
				  properties=properties)
	print(f'Insert data into {table_name} successfully!')



def etl_main():
	download_files()
	spark = init_spark()
	dfs, df_zone = extract(spark)
	dim_tables = generate_dim_table(dfs[0])
	dim_tables_name = ['payment', 'ratecode', 'vendor']
	for tbls, name in zip(dim_tables, dim_tables_name):
		load(tbls, name)
	load(df_zone, 'taxi_zone_lookup')

	tables_name = ['green_taxi', 'yellow_taxi']
	for df, name in zip(dfs, tables_name):
		transform(df)
		load(df, name)


etl_main()
