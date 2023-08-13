from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from etl_staging import start_spark, load
import json


def read_data(spark, model: str, table_name:str) -> DataFrame:
	"""
	Reads data from a database table into a DataFrame.

	This function reads data from a specified database table using JDBC connection
	and returns the content as a DataFrame. The connection details are fetched from
	a configuration file.

	Parameters:
		spark (pyspark.sql.SparkSession): The Spark session to use for reading data.
		model (str): The model or environment for which to fetch database credentials.
		table_name (str): The name of the database table to read data from.

	Returns:
		pyspark.sql.DataFrame: The DataFrame containing the data read from the database.
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
	df = spark.read.jdbc(url=url, table=table_name, properties=properties)
	print(f'Read data from {table_name} successfully!')
	return df


def join_data(left, right, left_on: str, right_on: str, cols_to_lookup: List[str], how='inner') -> DataFrame:
	"""
	Joins two DataFrames based on specified columns and returns selected columns.

	This function performs a join operation between two DataFrames using the specified
	columns as join keys. The result DataFrame includes the selected columns specified
	in 'cols_to_lookup'.

	Parameters:
		left (pyspark.sql.DataFrame): The left DataFrame for the join operation.
		right (pyspark.sql.DataFrame): The right DataFrame for the join operation.
		left_on (str): The column in the left DataFrame to join on.
		right_on (str): The column in the right DataFrame to join on.
		cols_to_lookup (List[str]): List of column names to include in the output DataFrame.
		how (str): The type of join to perform ('inner', 'outer', 'left', 'right').

	Returns:
		pyspark.sql.DataFrame: The resulting DataFrame after the join operation
							   with selected columns.
	"""
	df = left.join(right, left[left_on] == right[right_on], how)
	if cols_to_lookup:
		df = df.select(cols_to_lookup)
	return df


def monthly_report(df):
	"""
	Generates a monthly report based on taxi trip data.

	This function takes a DataFrame containing taxi trip data and generates a
	monthly report with aggregated information such as revenue, trip counts,
	average passenger count, and average trip distance.

	Parameters:
		df (pyspark.sql.DataFrame): The input DataFrame containing taxi trip data.

	Returns:
		pyspark.sql.DataFrame: A DataFrame representing the generated monthly report.
	"""
	cols = ['VendorID', 'RatecodeID', 'pickup_datetime', 'pickup_borough', 'pickup_zone', 'dropoff_datetime', 'dropoff_borough', 'dropoff_zone', 'passenger_count',
		'trip_distance_in_km', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'payment_type']
	df = df.select(cols)
	df = df.withColumn('revenue_month', month('pickup_datetime'))
	monthly_df = df.groupBy(['pickup_zone', 'revenue_month']).agg(round(sum('fare_amount'), 2).alias('revenue_monthly_fare'),
																   round(sum('extra'), 2).alias('revenue_monthly_extra'),
																   round(sum('mta_tax'), 2).alias('revenue_monthly_mta_tax'),
																   round(sum('tip_amount'), 2).alias('revenue_monthly_tip_amount'),
																   sum('tolls_amount').alias('revenue_monthly_tolls_amount'),
																   round(sum('total_amount'), 2).alias('revenue_monthly_total_amount'),
																   round(sum('mta_tax'), 2).alias('congestion_surcharge'),
																   count('*').alias('total_monthly_trips'),
																   round(avg('passenger_count'), 2).alias('avg_monthly_passenger_count'),
																   round(avg('trip_distance_in_km'), 2).alias('avg_monthly_trip_distance'))
	return monthly_df


def main_production():
	spark = start_spark()
	schema_staging = 'staging'
	green_taxi = read_data(spark, schema_staging, table_name='green_taxi')
	yellow_taxi = read_data(spark, schema_staging, table_name='yellow_taxi')
	taxi_zone = read_data(spark, schema_staging, table_name='taxi_zone_lookup').select('LocationID', 'Zone', 'Borough')
	join_data_green = join_data(green_taxi, taxi_zone, 'PULocationID', 'LocationID', [])
	join_data_green = join_data_green.withColumnRenamed('Zone', 'pickup_zone').withColumnRenamed('Borough', 'pickup_borough').drop('LocationID')
	join_data_green = join_data(join_data_green, taxi_zone, 'DOLocationID', 'LocationID', [])
	join_data_green = join_data_green.withColumnRenamed('Zone', 'dropoff_zone').withColumnRenamed('Borough', 'dropoff_borough').drop('LocationID')
	green_monthly = monthly_report(join_data_green)	

	join_data_yellow = join_data(yellow_taxi, taxi_zone, 'PULocationID', 'LocationID', [])
	join_data_yellow = join_data_yellow.withColumnRenamed('Zone', 'pickup_zone').withColumnRenamed('Borough', 'pickup_borough').drop('LocationID')
	join_data_yellow = join_data(join_data_yellow, taxi_zone, 'DOLocationID', 'LocationID', [])
	join_data_yellow = join_data_yellow.withColumnRenamed('Zone', 'dropoff_zone').withColumnRenamed('Borough', 'dropoff_borough').drop('LocationID')
	yellow_monthly = monthly_report(join_data_yellow)	

	schema_production = 'production'
	load(green_monthly, schema_production, table_name='green_monthly_report')
	load(yellow_monthly, schema_production, table_name='yellow_monthly_report')
	spark.stop()