import os
from pyspark.sql import SparkSession

year = 2022
def init_spark():
    spark = SparkSession.builder \
        .appName("ETL-with-spark") \
        .getOrCreate()
    return spark

def extract():
    os.system('curl -o data/taxi_zone_lookup.csv https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv')

    try:
        os.makedirs('data/green_taxi')
        os.makedirs('data/yellow_taxi')
    except OSError as err:
        print(f'Error: {err}')

    print('Start download parquet files')
    for i in range(1, 3):
        month = f'{i:02}'
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{0}-{1}.parquet'.format(year, month)
        csv_name = 'data/green_taxi/green_tripdata_{0}-{1}.parquet'.format(year, month)
        os.system(f'curl -o {csv_name} {url}')

    for i in range(1, 3):
        month = f'{i:02}'
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{0}-{1}.parquet'.format(year, month)
        csv_name = 'data/green_taxi/yellow_tripdata_{0}-{1}.parquet'.format(year, month)
        os.system(f'curl -o {csv_name} {url}')
        
    print('Download successfully')
    green_taxi = spark.read.parquet('data/green_taxi')
    yellow_taxi = spark.read.parquet('data/yellow_taxi')
    return green_taxi, yellow_taxi

def tranform(df):
    pass

def load():
    pass

spark = init_spark()