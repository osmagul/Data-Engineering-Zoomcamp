#!/usr/bin/env python
# coding: utf-8
import argparse
import os
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    host = params.host
    url = params.url
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    if 'tpep_dropoff_datetime' in df.columns:
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        
    if 'lpep_dropoff_datetime' in df.columns:
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        try:
            if 'tpep_dropoff_datetime' in df.columns:
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

            if 'lpep_dropoff_datetime' in df.columns:
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            print('done: '+str(df.index.max()))

            df = next(df_iter)
        except:
            print('No more rows')
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table in postgres')
    parser.add_argument('--url', help='url for the csv')

    args = parser.parse_args()

    main(args)
