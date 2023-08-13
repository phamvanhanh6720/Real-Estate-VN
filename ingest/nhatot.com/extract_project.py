import os
import json
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm

from utils import upload_folder_to_s3


api_endpoint = 'https://gateway.chotot.com/v1/public/api-pty/project/suggest'

extract_date = str(datetime.now().date())
dir_path = f'../migration_data/nhatotDB'

cities_data_path = '../migration_data/nhatotDB/loc.cities/2023-07-26/batch_1.csv'
cities_df = pd.read_csv(cities_data_path)
cities_id = cities_df['id'].tolist()


projects_list = []
for city_id in tqdm(cities_id):
    offset = 0
    status = 'active'
    on_filter = True
    limit = 50
    region_v2 = city_id

    base_api_endpoint = f'{api_endpoint}?offset={offset}&status={status}&on_filter={on_filter}&limit={limit}&region_v2={region_v2}'
    response = requests.get(base_api_endpoint)
    response = json.loads(response.text)

    total = response['total']
    for i in range(int(total // limit)):
        offset = i * limit
        base_api_endpoint = f'{api_endpoint}?offset={offset}&status={status}&on_filter={on_filter}&limit={limit}&region_v2={region_v2}'
        response = requests.get(base_api_endpoint)
        response = json.loads(response.text)

        projects = response['projects']
        projects_list.extend(projects)

project_df = pd.DataFrame(projects_list)
project_dir_path = os.path.join(dir_path, 'data.projects', extract_date)
if not os.path.exists(project_dir_path):
    os.makedirs(project_dir_path, exist_ok=True)

project_df.to_csv(os.path.join(project_dir_path, 'batch_1.csv'), index=False)


upload_folder_to_s3(
    bucket_name='realestate-vn-data-lake',
    folder_path=project_dir_path,
    prefix='raw/nhatot/data.projects/date=' + extract_date
)