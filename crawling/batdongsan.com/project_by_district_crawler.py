import os
import json
from configparser import ConfigParser

import pymongo
from bs4 import BeautifulSoup
import undetected_chromedriver as uc


# read config
config = ConfigParser()
config.read(os.path.join('config', 'config.ini'))

MAX_SLEEP_TIME: int = int(config.get('TIME', 'MAX_SLEEP_TIME'))
HOSTNAME = config.get('URL', 'HOST_NAME')
COLLECTION = config.get('DB', 'COLLECTION')

PAGE_LOAD_TIMEOUT = int(config.get('TIME', 'PAGE_LOAD_TIMEOUT'))
SCRIPT_LOAD_TIMEOUT = int(config.get('TIME', 'SCRIPT_LOAD_TIMEOUT'))

mongodb_connection = pymongo.MongoClient(
    host=config.get('DB', 'HOST'),
    port=int(config.get('DB', 'PORT')),
    username=config.get('DB', 'USERNAME'),
    password=config.get('DB', 'PASSWORD'),
    authSource='admin',
    authMechanism='SCRAM-SHA-1',
)
db_connection = mongodb_connection[config.get('DB', 'DATABASE')]


options = uc.ChromeOptions()
# options.add_argument('--headless=new')
options.headless = False
options.add_argument('--disable-gpu')
options.page_load_strategy = 'normal'
driver = uc.Chrome(
    use_subprocess=False,
    options=options
    )


data = db_connection['loc.districts'].find({})
districts = [item['districtId'] for item in data]

batch_size = 30
for i in range(0, len(districts), batch_size):
    batch_data = districts[i:min(i+batch_size, len(districts))]

    api = 'https://batdongsan.com.vn/Product/ProductSearch/GetProjectsByDistrictIds?'
    request_url = api
    for idx, district_id in enumerate(batch_data):
        request_url += f'districtIds[{idx}]={district_id}&'

    request_url = request_url.strip('&')
    driver.get(request_url)

    html_content = driver.page_source
    soup = BeautifulSoup(html_content, features='html.parser')

    response_data = json.loads(soup.text)
    for item in response_data:
        try:
            db_connection['data.projects_by_districts'].insert_one(item)
        except Exception as e:
            print(e)
            continue