#!/usr/bin/env python
# coding: utf-8
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task

@task(log_prints=True)
def extract_data(params):
    url = params.url
    csv_name = 'output.csv'
    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    if 'tpep_dropoff_datetime' in df.columns:
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        
    if 'lpep_dropoff_datetime' in df.columns:
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    return df

@task(log_prints=True)
def transform(data):
    data = data[data['passenger_count'] != 0]
    return data

@task(log_prints=True, retries=3)
def load(params, df):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    host = params.host
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # while True:
    #     try:
    #         if 'tpep_dropoff_datetime' in df.columns:
    #             df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #             df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    #         if 'lpep_dropoff_datetime' in df.columns:
    #             df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    #             df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            
    #         print('done: '+str(df.index.max()))

    #         df = next(df_iter)
    #     except:
    #         print('No more rows')
    #         break

@flow(name="Ingest Data")
def main_flow():
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table in postgres')
    parser.add_argument('--url', help='url for the csv')

    args = parser.parse_args()
    raw_data = extract_data(args)
    data = transform(raw_data)
    load(args, data)

if __name__ == "__main__":
    main_flow()