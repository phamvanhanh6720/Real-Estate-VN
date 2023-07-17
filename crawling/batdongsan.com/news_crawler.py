import json
import os
import pika
import json
import logging
from configparser import ConfigParser
from datetime import datetime

import pymongo
from copy import deepcopy
import undetected_chromedriver as uc

from utils import scroll_down, parser_log, create_visualize_figure, crawl_each_news_item


# create logger
crawling_date = datetime.today().date()
logging.basicConfig(filename=f'logs/batdongsan{crawling_date}.log',
                    format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

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

rabbitmq_connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        port='5672'
    )
)

channel = rabbitmq_connection.channel()

options = uc.ChromeOptions()
options.add_argument('--headless=new')
options.headless = False
options.add_argument('--disable-gpu')
options.page_load_strategy = 'normal'
driver = uc.Chrome(
    use_subprocess=False,
    options=options
    )
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
driver.set_script_timeout(SCRIPT_LOAD_TIMEOUT)

# def extract_news_info(ch, method, properties, body):
channel.basic_qos(prefetch_count=1)
while True:
    method, _, body = channel.basic_get(queue='news_queue', auto_ack=False)
    data = body.decode()
    news_data = json.loads(data)

    try:
        detail_news_data = crawl_each_news_item(
            driver=driver,
            url=news_data['url'],
            max_sleep_time=MAX_SLEEP_TIME,
            news_data=deepcopy(news_data)
        )

        detail_news_data['crawl_time'] = datetime.now()
        db_connection[COLLECTION].insert_one(detail_news_data)
        print(f"Success: {detail_news_data['url']}")
        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Fail: {news_data['url']}")
        logger.warning(f"Fail: {e} {news_data['url']}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# channel.basic_consume('news_queue', on_message_callback=extract_news_info)
# channel.start_consuming()
channel.close()
rabbitmq_connection.close()
mongodb_connection.close()
print('News crawler finished!')
