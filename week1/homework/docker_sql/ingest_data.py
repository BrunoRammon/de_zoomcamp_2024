import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    file_name = 'green_tripdata_2019-09.csv.gz'
    os.system(f'wget {url} -O {file_name}')

    df_iter = pd.read_csv(f'{file_name}', iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:

        try:
            t_start = time()

            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to postgres')
    parser.add_argument('--user', help='user name for postgres', required=True)
    parser.add_argument('--password', help='username for postgres', required=True)
    parser.add_argument('--host', help='host for postgres', required=True)
    parser.add_argument('--port', help='port for postgres', required=True)
    parser.add_argument('--db', help='database name for postgres', required=True)
    parser.add_argument('--table_name',
                        help='table name where we will write the results to',
                        required=True)
    parser.add_argument('--url', help='url of the file', required=True)

    args = parser.parse_args()
    main(args)
