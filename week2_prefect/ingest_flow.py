#!/usr/bin/env python
# coding: utf-8

from datetime import timedelta
from time import time
import os
import pandas as pd
from prefect import task, flow
from prefect.tasks import  task_input_hash
from prefect_sqlalchemy.database import SqlAlchemyConnector

@task(name="Download archive", log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_raw_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df = pd.read_csv(csv_name)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    return df


@task(name="Simple transforming", log_prints=True)
def transform_data(df):
    print(f"pre: passenger count missing: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: passenger count missing: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(name="Loading to db", log_prints=True)
def load_to_db(df, table_name):
    print(f"Loading data to {table_name}")
    # block-name: docker-postgres
    connection_block = SqlAlchemyConnector.load("docker-postgres")
    with connection_block.get_connection(begin=False) as engine:
        t_start = time()
        df.to_sql(name=table_name, con=engine, if_exists='replace')
        t_end = time()
        print('inserted another chunk, took %.3f second' % (t_end - t_start))


@flow(name="Ingest data #1")
def main_flow(url, table_name):
    raw_data = extract_raw_data(url)
    clean_data = transform_data(raw_data)
    load_to_db(clean_data, table_name)


if __name__ == '__main__':
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"
    main_flow(url, table_name="taxi_data")
