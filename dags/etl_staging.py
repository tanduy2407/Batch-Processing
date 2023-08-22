import os
from pyspark.sql import SparkSession, DataFrame
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
		'curl -o driver/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar')
	os.system(
		'curl -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')

	print('Start download parquet files')
	for i in range(1, 3):
		month = f'{i:02}'
		url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{0}-{1}.parquet'.format(
			year, month)
		csv_name = 'data/green_taxi/green_tripdata_{0}-{1}.parquet'.format(
			year, month)
		os.system(f'curl -o {csv_name} {url}')

	for i in range(1, 3):
		month = f'{i:02}'
		url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{0}-{1}.parquet'.format(
			year, month)
		csv_name = 'data/yellow_taxi/yellow_tripdata_{0}-{1}.parquet'.format(
			year, month)
		os.system(f'curl -o {csv_name} {url}')
	print('Download successfully')


def start_spark(app_name='Batch-Processing', spark_config_dict={}):
	"""
	Starts an Apache Spark session with optional configuration.

	This function initializes an Apache Spark session with the provided
	application name and optional configuration settings.

	Parameters:
			app_name (str): The name of the Spark application (default is 'Batch-Processing').
			spark_config_dict (dict): A dictionary containing Spark configuration key-value pairs.

	Returns:
			pyspark.sql.SparkSession: A Spark session configured with the provided settings.
	"""
	spark_builder = SparkSession.builder \
		.appName(app_name)
	for key, value in spark_config_dict.items():
		spark_builder.config(key, value)
	spark_session = spark_builder.getOrCreate()
	return spark_session


def extract(spark):
	"""
	Extracts data from various sources using Apache Spark.

	This function reads parquet files for green and yellow taxi data and
	a CSV file for taxi zone lookup data using the provided Spark session.

	Parameters:
			spark (pyspark.sql.SparkSession): The Spark session to use for reading data.

	Returns:
			tuple: A tuple containing two DataFrames representing green and yellow taxi data,
					   and a DataFrame containing taxi zone lookup data.
	"""
	green_taxi = spark.read.parquet('data/green_taxi')
	yellow_taxi = spark.read.parquet('data/yellow_taxi')
	df_zone = spark.read.csv('data/taxi_zone_lookup.csv',
							 inferSchema=True, header=True).filter(col('Borough') != 'Unknown')
	return [green_taxi, yellow_taxi], df_zone


def generate_dim_table(df):
	for i in ['RateCodeID', 'payment_type', 'trip_type']:
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


def transform(df, color_taxi: str) -> DataFrame:
	"""
	Transforms a DataFrame containing taxi trip data.

	This function takes a DataFrame containing taxi trip data and applies
	transformations such as renaming columns, calculating trip duration in
	minutes, converting units, and calculating average velocity.

	Parameters:
			df (pyspark.sql.DataFrame): The input DataFrame containing taxi trip data.
			color_taxi (str): The color of the taxi ('green_taxi' or 'yellow_taxi').

	Returns:
			pyspark.sql.DataFrame: The transformed DataFrame with added columns.
	"""
	if color_taxi == 'green_taxi':
		df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime').withColumnRenamed(
			'lpep_dropoff_datetime', 'dropoff_datetime')
	if color_taxi == 'yellow_taxi':
		df = df.withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime').withColumnRenamed(
			'tpep_dropoff_datetime', 'dropoff_datetime')
	trip_time_in_mins = unix_timestamp(
		col('dropoff_datetime')) - unix_timestamp(col('pickup_datetime'))
	df = df.withColumn('trip_time_in_mins', round(
		trip_time_in_mins / 60, 2))  # calculate time in minutes
	df = df.withColumn('passenger_count', col(
		'passenger_count').cast(IntegerType()))
	df = df.withColumn('trip_distance_in_km', round(
		col('trip_distance') * 1.6, 2))  # convert miles into km
	time_in_hours = col('trip_time_in_mins') / 60
	df = df.withColumn('average_velocity',  round(
		col('trip_distance_in_km') / time_in_hours, 2))  # calculate average speed
	return df


def load(df, model: str, table_name: str):
	"""
	Loads a DataFrame into a specified database table.

	This function takes a DataFrame and inserts its content into a specified
	database table using JDBC connection. The connection details are read from
	a configuration file.

	Parameters:
		df (pyspark.sql.DataFrame): The DataFrame to be loaded into the database.
		model (str): The model or environment for which to fetch database credentials.
		table_name (str): The name of the database table to insert data into.

	Returns:
		None
	"""
	with open('config/config.json', 'r') as config:
		config_data = json.load(config)
	host = config_data[model]['host']
	port = config_data[model]['port']
	database = config_data[model]['database']
	user = config_data[model]['user']
	password = config_data[model]['password']

	url = 'jdbc:postgresql://{0}:{1}/{2}'.format(host, port, database)
	properties = {
		'user': user,
		'password': password,
		'driver': 'org.postgresql.Driver'}
	df.write.jdbc(url=url, table=table_name, mode='overwrite',
				  properties=properties)
	print(f'Insert data into {table_name} successfully!')


def main_staging():
	schema = 'staging'
	download_files()
	config_dict = {'spark.jars': 'driver/postgresql-42.6.0.jar',
				   'spark.executor.heartbeatInterval': '100000ms',
				   'spark.executor.memory': '4g',
				   'spark.driver.memory': '4g'}
	spark = start_spark(spark_config_dict=config_dict)
	dfs, df_zone = extract(spark)
	dim_dfs = generate_dim_table(dfs[0])
	dim_tables_name = ['payment', 'ratecode', 'vendor']
	for tbls, name in zip(dim_dfs, dim_tables_name):
		load(tbls, schema, table_name=name)
	load(df_zone, schema, table_name='taxi_zone_lookup')

	tables_name = ['green_taxi', 'yellow_taxi']
	for df, name in zip(dfs, tables_name):
		df = transform(df, name)
		load(df, schema, table_name=name)
	spark.stop()