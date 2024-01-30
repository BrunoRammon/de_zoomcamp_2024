import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
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

    file_name = 'yellow_tripdata_2021-01.parquet'
    os.system(f'wget {url} -O {file_name}')

    data_head = pd.read_parquet(f'{file_name}').head(0)
    data_head.to_sql(name=table_name, con=engine, if_exists='replace')

    parquet_file = pq.ParquetFile(f'{file_name}')
    for batch in parquet_file.iter_batches(100000):
        t_start = time()
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        elapsed = t_end - t_start
        print(f'inserted another chunk, took {elapsed:.3f} seconds')

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
