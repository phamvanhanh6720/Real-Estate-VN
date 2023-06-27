import json
import os
import pika
import json
from configparser import ConfigParser

import numpy as np
import telebot
from bs4 import BeautifulSoup
from copy import deepcopy
import undetected_chromedriver as uc

from utils import scroll_down, parser_log, create_visualize_figure, crawl_each_news_item
from connector import Database


if __name__ == '__main__':
    # create logger
    # crawling_date = datetime.date.today()
    # logging.basicConfig(filename=f'logs/batdongsan{crawling_date}.log',
    #                     format='%(asctime)s - %(message)s',
    #                     datefmt='%d-%b-%y %H:%M:%S')
    # logger = logging.getLogger(__name__)
    # logger.setLevel(logging.INFO)

    # read config
    config = ConfigParser()
    config.read(os.path.join('config', 'config.ini'))

    MAX_SLEEP_TIME: int = int(config.get('TIME', 'MAX_SLEEP_TIME'))
    HOSTNAME = config.get('URL', 'HOST_NAME')
    # COLLECTION = config.get('DB', 'COLLECTION')
    # RAW_COLLECTION = config.get('DB', 'RAW_COLLECTION')

    PAGE_LOAD_TIMEOUT = int(config.get('TIME', 'PAGE_LOAD_TIMEOUT'))
    SCRIPT_LOAD_TIMEOUT = int(config.get('TIME', 'SCRIPT_LOAD_TIMEOUT'))

    # db_connection = Database(
    #     host=config.get('DB', 'HOST'),
    #     port=int(config.get('DB', 'PORT')),
    #     username=config.get('DB', 'USERNAME'),
    #     password=config.get('DB', 'PASSWORD'),
    #     authSource='admin',
    #     authMechanism='SCRAM-SHA-1',
    #     database=config.get('DB', 'DATABASE')
    # ).get_db()
    # logger.info('Connect to MongoDB done')

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

    def extract_news_info(ch, method, properties, body):
        data = body.decode()
        news_data = json.loads(data)

        try:
            crawl_each_news_item(
                driver=driver,
                url=news_data['url'],
                max_sleep_time=MAX_SLEEP_TIME,
                news_data=deepcopy(news_data)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            print(e)
            print(f"Fail: {news_data['url']}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume('news_queue', on_message_callback=extract_news_info)
    channel.start_consuming()
