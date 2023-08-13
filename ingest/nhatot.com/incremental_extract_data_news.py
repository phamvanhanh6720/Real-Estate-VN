import os
import time
from datetime import datetime


import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from utils import fetch_data_in_batches, upload_folder_to_s3


def incremental_extract_data_news(
        database_name: str = 'nhatotDB',
        extract_time_file: str = 'extract_time.txt'):

    collection_name = 'data.news'
    extract_date = str(datetime.now().date())

    dir_path = os.path.join('../migration_data', database_name, collection_name, extract_date)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    prefix_s3 = f'raw/nhatot/{collection_name}/date={extract_date}'
    is_full_extraction = False

    if not os.path.exists(extract_time_file):
        file = open(extract_time_file, 'w')
        file.close()
        is_full_extraction = True

    query = None
    if is_full_extraction:
        extract_time = datetime.now()
        file = open(extract_time_file, 'a+')
        file.write(str(extract_time) + '\n')
        file.close()

        query = {}

    else:
        file = open(extract_time_file, 'r')
        data_lines = file.readlines()
        data_lines = [line.strip('\n ') for line in data_lines]
        data_lines = [line for line in data_lines if line != '']
        last_extract_time = data_lines[-1]
        last_extract_time = datetime.fromisoformat(last_extract_time)
        file.close()

        extract_time = datetime.now()
        file = open(extract_time_file, 'a+')
        file.write(str(extract_time) + '\n')
        file.close()

        query = {'crawl_time': {'$lte': extract_time, '$gt': last_extract_time}}

    for i, batch in enumerate(fetch_data_in_batches(database_name, collection_name, 10000, query)):
        print(f'Writing batch {i + 1} to file...')
        normalized_batch = []
        for data in batch:
            normed_data = {}
            normed_data['_id'] = str(data['_id'])
            selected_fields = ['list_id', 'list_time', 'account_id', 'account_name',
                               'subject', 'category', 'category_name',
                               'area', 'area_name', 'region', 'region_name',
                               'price', 'price_string', 'rooms', 'size',
                               'region_v2', 'area_v2', 'ward', 'ward_name',
                               'price_million_per_m2', 'longitude', 'latitude', 'phone'
                               ]

            for field in selected_fields:
                normed_data[field] = data['ad'][field] if field in data['ad'].keys() else None
            normed_data['crawl_time'] = data['crawl_time'] if 'crawl_time' in data.keys() else None

            normalized_batch.append(normed_data)

        data_df = pd.DataFrame(normalized_batch)
        table = pa.Table.from_pandas(data_df)
        pq.write_table(table, os.path.join(dir_path, 'batch_' + str(i + 1) + '_' + str(time.time_ns()) + '.parquet'))

    upload_folder_to_s3(
        bucket_name='realestate-vn-data-lake',
        folder_path=dir_path,
        prefix=prefix_s3
    )


if __name__ == '__main__':
    incremental_extract_data_news()
