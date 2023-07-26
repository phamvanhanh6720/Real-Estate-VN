import os
import json
import requests
import pandas as pd
from datetime import datetime

from utils import upload_folder_to_s3


def normalize_district(text):
    text = text.replace('Huyện', '').replace('Thị xã', '').replace('Thành phố', '').replace('Tp', '').replace('Thành Phố', '').strip()
    if len(text.split(' ')) > 2:
        text = text.replace('Quận', '').strip()

    return text


api_endpoint = 'https://gateway.chotot.com/v1/public/web-proxy-api/loadRegions'

response = requests.get(api_endpoint)
response = json.loads(response.text)

extract_date = str(datetime.now().date())
dir_path = f'../migration_data/nhatotDB'


cities_data = []
districts_data = []
for city_key in response['regionFollowId']['entities']['regions'].keys():
    city_data = response['regionFollowId']['entities']['regions'][city_key]

    id = city_data['id']
    name = city_data['name'].replace('Tp', ' ').strip()
    geo_region = city_data['geo_region']
    geo = city_data['geo']

    cities_data.append([id, name, geo_region, geo])

    for district_key in city_data['area'].keys():
        district_data = city_data['area'][district_key]

        district_id = district_data['id']
        district_name = district_data['name']
        district_name = normalize_district(district_name)

        district_geo_region = district_data['geo_region']
        district_geo = district_data['geo']

        districts_data.append([district_id, district_name, district_geo_region, district_geo, id, name])

city_df = pd.DataFrame(cities_data, columns=['id', 'name', 'geo_region', 'geo'])
district_df = pd.DataFrame(districts_data, columns=['district_id', 'district_name', 'district_geo_region', 'district_geo', 'city_id', 'city_name'])

city_dir_path = os.path.join(dir_path, 'loc.cities', extract_date)
district_dir_path = os.path.join(dir_path, 'loc.districts', extract_date)
if not os.path.exists(city_dir_path):
    os.makedirs(city_dir_path, exist_ok=True)

if not os.path.exists(district_dir_path):
    os.makedirs(district_dir_path, exist_ok=True)

city_df.to_csv(os.path.join(city_dir_path, 'batch_1.csv'), index=False)
district_df.to_csv(os.path.join(district_dir_path, 'batch_1.csv'), index=False)

upload_folder_to_s3(
    bucket_name='realestate-vn-data-lake',
    folder_path=city_dir_path,
    prefix='raw/nhatot/loc.cities/date=' + extract_date
)

upload_folder_to_s3(
    bucket_name='realestate-vn-data-lake',
    folder_path=district_dir_path,
    prefix='raw/nhatot/loc.districts/date=' + extract_date
)
