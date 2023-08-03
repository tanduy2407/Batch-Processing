import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


year = 2022


def init_spark():
    spark = SparkSession.builder \
        .appName("ETL-with-spark") \
        .getOrCreate()
    return spark


def extract(spark):
    os.system(
        'curl -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')

    try:
        os.makedirs('data/green_taxi')
        os.makedirs('data/yellow_taxi')
    except OSError as err:
        print(f'Error: {err}')

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
    green_taxi = spark.read.parquet('data/green_taxi')
    yellow_taxi = spark.read.parquet('data/yellow_taxi')
    df_zone = spark.read.csv('data/taxi_zone_lookup.csv',
                             inferSchema=True, header=True).filter(col('Borough') != 'Unknown')
    return [green_taxi, yellow_taxi], df_zone


def transform(df):
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

    trip_time_in_mins = unix_timestamp(
        col('lpep_dropoff_datetime')) - unix_timestamp(col('lpep_pickup_datetime'))
    df = df.withColumn('trip_time_in_mins', round(trip_time_in_mins / 60, 2))
    return [df_payment, df_ratecode, df_vendor, df]


def load(df, table: str):
    host = ''
    port = ''
    database = ''
    user = ''
    password = ''
    url = 'jdbc:postgresql://{0}:{1}/{2}'.format(host, port, database)
    properties = {
        'user': user,
        'password': password,
        'driver': 'org.postgresql.Driver'}
    df.write.jdbc(url=url, table=table, mode='overwrite',
                  properties=properties)


def etl_main():
    spark = init_spark()
    dfs, df_zone = extract(spark)
    for df in dfs:
        tables = transform(df)
    table_names = ['payment_type', 'rate_type', 'vendor']
    for table, table_names in zip(tables):
        pass