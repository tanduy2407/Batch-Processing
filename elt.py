import os
import logging
from pyspark.sql import SparkSession

def init_spark():
    spark = SparkSession.builder \
        .appName("ETL-with-spark") \
        .getOrCreate()
    return spark


def download_files():
    year = '2020'
    logging.info(f'Start download parquet files')
    for i in range(1, 13):
        month = f'{i:02}'
        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{}-{}.parquet'.format(year, month)
        csv_name = 'data/green_tripdata_{}-{}.parquet'.format(year, month)
        os.system(f'curl -o {csv_name} {url}')
    logging.info('Download successfully')


def extract(spark):
    year = '2020'
    for i in range(1, 13):
        month = f'{i:02}'
        parquet_file_path = 'data/green_tripdata_{}-{}.parquet'.format(year, month)
        df = spark.read.parquet(parquet_file_path)
    return df

def tranform(df):
    pass

def load():
    pass