import os
import json
from datetime import datetime
from configparser import ConfigParser

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mongodb_extract import fetch_data_in_batches


config = ConfigParser()
config.read(os.path.join('config.ini'))


def upload_folder_to_s3(bucket_name, folder_path, prefix):
    # Create an AWS session
    session = boto3.Session(
        aws_access_key_id=config.get('AWS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS', 'AWS_SECRET_ACCESS_KEY'),
        region_name=config.get('AWS', 'AWS_DEFAULT_REGION')
    )
    s3 = session.client('s3')

    # Iterate over the files in the folder
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # Construct the full local path of the file
            local_path = os.path.join(root, file)

            # Construct the S3 key by removing the folder path prefix
            s3_key = os.path.join(prefix, file)

            # Upload the file to S3
            s3.upload_file(local_path, bucket_name, s3_key)


def write_json_file(data, file_path):
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)


if __name__ == '__main__':
    database_name = 'batdongsanDB'
    collection_name = 'loc.streets'

    extract_date = str(datetime.now().date())
    is_fully_extracted = True

    if is_fully_extracted:
        dir_path = os.path.join('migration_data', database_name, collection_name)
        prefix_s3 = f'landing/batdongsan/{collection_name}'
    else:
        dir_path = os.path.join('migration_data', database_name, collection_name, extract_date)
        prefix_s3 = f'landing/batdongsan/{collection_name}/date={extract_date}'

    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    for i, batch in enumerate(fetch_data_in_batches(database_name, collection_name, 10000)):
        # data = pd.DataFrame(batch)
        # data['_id'] = data['_id'].apply(lambda x: str(x))
        #
        # table = pa.Table.from_pandas(data)
        write_json_file(batch, os.path.join(dir_path, 'batch_' + str(i + 1) + '.json'))
        # pq.write_table(table, os.path.join(dir_path, 'batch_' + str(i + 1) + '.parquet'))

    upload_folder_to_s3(
        bucket_name='realestate-vn-data-lake',
        folder_path=dir_path,
        prefix=prefix_s3
    )


