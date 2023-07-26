import os
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import unicodedata

from utils import fetch_data_in_batches, upload_folder_to_s3


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


def fully_extract_data(
        collection_name,
        database_name: str = 'batdongsanDB',
        extract_date: str = None):
    if extract_date is None:
        extract_date = str(datetime.now().date())

    dir_path = os.path.join('../migration_data', database_name, collection_name, extract_date)
    prefix_s3 = f'raw/batdongsan/{collection_name}/date={extract_date}'
    is_normalized = True if collection_name == 'data.news' else False

    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

    for i, batch in enumerate(fetch_data_in_batches(database_name, collection_name, 10000)):

        print(f'Writing batch {i + 1} to file...')
        data = pd.DataFrame(batch)
        data['_id'] = data['_id'].apply(lambda x: str(x))

        text_columns = data.select_dtypes(include='object').columns
        for column in text_columns:
            # Apply Unicode normalization to the values in the column
            data[column] = data[column].apply(lambda x: unicodedata.normalize('NFKC', str(x)).strip())

        if is_normalized:
            data['price'] = data['price'].apply(lambda x: convert_to_float(x))
            data['price_per_m2'] = data['price_per_m2'].apply(lambda x: convert_to_float(x))
            data['no_bedroom'] = data['no_bedroom'].apply(lambda x: convert_to_int(x))
            data['no_toilet'] = data['no_toilet'].apply(lambda x: convert_to_int(x))
            data['area'] = data['area'].apply(lambda x: convert_to_float(x))
            data['phone_number'].astype('str')

        table = pa.Table.from_pandas(data)
        pq.write_table(table, os.path.join(dir_path, 'batch_' + str(i + 1) + '.parquet'))

    upload_folder_to_s3(
        bucket_name='realestate-vn-data-lake',
        folder_path=dir_path,
        prefix=prefix_s3
    )


if __name__ == '__main__':
    fully_extract_data(
        collection_name='loc.districts',
        extract_date=None
    )
