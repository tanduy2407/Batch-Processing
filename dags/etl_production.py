from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from etl_staging import start_spark, load
import json


def read_data(spark, model: str, table_name:str) -> DataFrame:
	# Read credentials from the config file
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
	df = left.join(right, left[left_on] == right[right_on], how)
	if cols_to_lookup:
		df = df.select(cols_to_lookup)
	return df


def monthly_report(df):
	cols = ['VendorID', 'RatecodeID', 'pickup_datetime', 'pickup_borough', 'pickup_zone', 'dropoff_datetime', 'dropoff_borough', 'dropoff_zone', 'passenger_count',
        'trip_distance_in_km', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type']
	df = df.select(cols)
	df = df.withColumn('revenue_month', month('pickup_datetime'))
	monthly_df = df.groupBy(['pickup_zone', 'revenue_month']).agg(sum('fare_amount').alias('revenue_monthly_fare'),
                                                                   sum('extra').alias('revenue_monthly_extra'),
                                                                   sum('mta_tax').alias('revenue_monthly_mta_tax'),
                                                                   sum('tip_amount').alias('revenue_monthly_tip_amount'),
                                                                   sum('tolls_amount').alias('revenue_monthly_tolls_amount'),
                                                                   sum('total_amount').alias('revenue_monthly_total_amount'),
                                                                   sum('mta_tax').alias('congestion_surcharge'),
                                                                   count('*').alias('total_monthly_trips'),
                                                                   avg('passenger_count').alias('avg_monthly_passenger_count'),
                                                                   avg('trip_distance_in_km').alias('avg_monthly_trip_distance'))
	return monthly_df


def etl_production():
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

	# join_data_yellow = join_data(yellow_taxi, taxi_zone, 'PULocationID', 'LocationID', [])
	# join_data_yellow = join_data_yellow.withColumnRenamed('Zone', 'pickup_zone').withColumnRenamed('Borough', 'pickup_borough').drop('LocationID')
	# join_data_yellow = join_data(join_data_yellow, taxi_zone, 'DOLocationID', 'LocationID', [])
	# join_data_yellow = join_data_yellow.withColumnRenamed('Zone', 'dropoff_zone').withColumnRenamed('Borough', 'dropoff_borough').drop('LocationID')
	schema_production = 'production'
	load(green_monthly, schema_production, table_name='green_monthly_report')

# if __name__ == 'main':
etl_production()