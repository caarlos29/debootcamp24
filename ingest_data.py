import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import argparse
import os

def main(params):
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db = params.db
	table_name = params.table_name
	url = params.url

	#pq_name = 'yellow_tripdata_2021-01.parquet'

	os.system(f'curl -k {url} -O')

	df = pq.ParquetFile('yellow_tripdata_2021-01.parquet')

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
	engine.connect()

	for batch in df.iter_batches(batch_size=100000):

		batch_df = batch.to_pandas()

		with engine.begin() as conn:
			batch_df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

	parser.add_argument('--user', help='username for postgres')
	parser.add_argument('--password', help='pass for postgres')
	parser.add_argument('--host', help='host for postgres')
	parser.add_argument('--port', help='port for postgres')
	parser.add_argument('--db', help='db for postgres')
	parser.add_argument('--table_name', help='name for postgres')
	parser.add_argument('--url', help='url for postgres')

	args = parser.parse_args()

	main(args)
