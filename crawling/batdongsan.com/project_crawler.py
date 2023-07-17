import json
import os
import pika
import json
from configparser import ConfigParser
from datetime import datetime

import pymongo
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

from utils import scroll_down, get_main_page

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

# bot = telebot.TeleBot(config.get('TELEBOT', 'TOKEN'))

# rabbitmq_connection = pika.BlockingConnection(
#     pika.ConnectionParameters(
#         host='localhost',
#         port='5672'
#     )
# )
#
# channel = rabbitmq_connection.channel()

options = uc.ChromeOptions()
# options.add_argument('--headless=new')
options.headless = False
options.add_argument('--disable-gpu')
options.page_load_strategy = 'normal'
driver = uc.Chrome(
    use_subprocess=False,
    options=options
    )
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
driver.set_script_timeout(SCRIPT_LOAD_TIMEOUT)

start_page = 0
seed_url = 'https://batdongsan.com.vn/du-an-bat-dong-san'
main_url = f'{seed_url}/p{start_page}'

get_main_page(driver, main_url)
scroll_down(driver, int(MAX_SLEEP_TIME))

html_content = driver.page_source
soup = BeautifulSoup(html_content, features='html.parser')

pagination_components = soup.find_all('a', class_='re__pagination-number')
max_page = max([int(item.text.strip('\n ')) for item in pagination_components])

for page in range(start_page, max_page + 1):
    main_url = f'{seed_url}/p{page}'
    get_main_page(driver, main_url)
    scroll_down(driver, int(MAX_SLEEP_TIME))

    html_content = driver.page_source
    soup = BeautifulSoup(html_content, features='html.parser')

    project_items = soup.find_all('div', class_='re__prj-card-full')

    project_list = []
    for item in project_items:
        project_data = {}

        project_name = item.find('a').get('title')
        prject_href = item.find('a').get('href')

        project_name_id = prject_href.split('/')[-1].split('-pj')[0]
        project_id = int(prject_href.split('/')[-1].split('-pj')[-1])
        project_status = item.find('div', class_='re__prj-tag-info')
        if project_status is not None:
            project_status = project_status.find('label').text.strip('\n ')

        detail_location = item.find('div', class_='re__prj-card-location').get('title')
        location_array = detail_location.split(',')

        province = location_array[-1].strip() if len(location_array) >= 1 else None
        district = location_array[-2].strip() if len(location_array) >= 2 else None
        ward = location_array[-3].strip() if len(location_array) >= 3 else None

        project_data['project_name'] = project_name
        project_data['project_name_id'] = project_name_id
        project_data['project_id'] = project_id

        project_data['project_status'] = project_status
        project_data['url'] = HOSTNAME + prject_href
        project_data['update_time'] = datetime.now()

        project_data['province'] = province
        project_data['district'] = district
        project_data['ward'] = ward
        project_data['detail_location'] = detail_location

        project_list.append(project_data)

    for data in project_list:
        db_connection['data.projects_v2'].insert_one(data)

    print('Crawl page done: ', page)
