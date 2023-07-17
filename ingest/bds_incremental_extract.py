import os
import json
from datetime import datetime
from configparser import ConfigParser

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import unicodedata

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
    collection_name = 'loc.districts'

    extract_date = str(datetime.now().date())
    is_fully_extracted = True

    if is_fully_extracted:
        dir_path = os.path.join('migration_data', database_name, collection_name)
        prefix_s3 = f'raw/batdongsan/{collection_name}'
    else:
        dir_path = os.path.join('migration_data', database_name, collection_name, extract_date)
        prefix_s3 = f'raw/batdongsan/{collection_name}/date={extract_date}'

    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    def convert_to_float(x):
        try:
            return float(x)
        except:
            return None

    def convert_to_int(x):
        try:
            return int(x)
        except:
            return None

    for i, batch in enumerate(fetch_data_in_batches(database_name, collection_name, 10000)):

        print(f'Writing batch {i + 1} to file...')
        data = pd.DataFrame(batch)
        data['_id'] = data['_id'].apply(lambda x: str(x))

        text_columns = data.select_dtypes(include='object').columns
        for column in text_columns:
            # Apply Unicode normalization to the values in the column
            data[column] = data[column].apply(lambda x: unicodedata.normalize('NFKC', str(x)).strip())

        # data['price'] = data['price'].apply(lambda x: convert_to_float(x))
        # data['price_per_m2'] = data['price_per_m2'].apply(lambda x: convert_to_float(x))
        # data['no_bedroom'] = data['no_bedroom'].apply(lambda x: convert_to_int(x))
        # data['no_toilet'] = data['no_toilet'].apply(lambda x: convert_to_int(x))
        # data['area'] = data['area'].apply(lambda x: convert_to_float(x))
        # data['phone_number'].astype('str')

        # data.to_csv(os.path.join(dir_path, 'batch_' + str(i + 1) + '.csv'), index=False)
        table = pa.Table.from_pandas(data)
        pq.write_table(table, os.path.join(dir_path, 'batch_' + str(i + 1) + '.parquet'))

    upload_folder_to_s3(
        bucket_name='realestate-vn-data-lake',
        folder_path=dir_path,
        prefix=prefix_s3
    )


