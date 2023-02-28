import os
import time
import random
import logging
import datetime
import threading
from copy import deepcopy
from configparser import ConfigParser

import numpy as np
import telebot
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

from utils import scroll_down, parser_log, create_visualize_figure, crawl_each_news_item
from connector import Database


def extract_raw_news(db, thread):
    options = uc.ChromeOptions()
    options.headless = False
    options.add_argument('--disable-gpu')
    options.page_load_strategy = 'normal'
    driver = uc.Chrome(use_subprocess=False,
                       options=options,
                       driver_executable_path=f'/home/phamvanhanh6720/PycharmProjects/Real-Estate-VN/crawling/batdongsan.com/driver/chromedriver_{thread}'
                       )
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.set_script_timeout(SCRIPT_LOAD_TIMEOUT)

    failed_count = 0
    is_failed = False
    while True:
        if is_failed:
            print("Create new driver")
            options = uc.ChromeOptions()
            options.headless = False
            options.add_argument('--disable-gpu')
            options.page_load_strategy = 'normal'
            driver = uc.Chrome(use_subprocess=False,
                               options=options,
                               driver_executable_path=f'/home/phamvanhanh6720/PycharmProjects/Real-Estate-VN/crawling/batdongsan.com/driver/chromedriver_{thread}'
                               )
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            driver.set_script_timeout(SCRIPT_LOAD_TIMEOUT)
            is_failed = False

        raw_news = db[RAW_COLLECTION].find({'is_done': False}).sort("_id", -1).limit(1)[0]
        if raw_news is None:
            break

        print(f"Start: {raw_news['url']}")
        news_data = deepcopy(raw_news)
        news_data.pop('_id')
        # check news have already crawled
        if db[COLLECTION].find_one({'url': raw_news['url'], 'published_date': raw_news['published_date']}) is not None:
            raw_news['is_done'] = True
            db[RAW_COLLECTION].update_one({'_id': raw_news['_id']}, {"$set": raw_news})
            print(f"Already exist: {raw_news['url']}")
            continue

        try:
            crawl_each_news_item(driver, news_data['url'], MAX_SLEEP_TIME, news_data, page_load_time_out=PAGE_LOAD_TIMEOUT)
            db[COLLECTION].insert_one(news_data)

            raw_news['is_done'] = True
            db[RAW_COLLECTION].update_one({'_id': raw_news['_id']}, {"$set": raw_news})
            print(f"Done: {news_data['url']}")
        except Exception as e:
            print(e)
            if 'iterable' in str(e):
                db[RAW_COLLECTION].delete_one({'_id': raw_news['_id']})
            print(f"Extract Failed: {news_data['url']}")
            # failed_count += 1
            is_failed = True
            driver.quit()
            del driver
            print("Quit")

    driver.close()

if __name__ == '__main__':
    # create logger
    crawling_date = datetime.date.today()
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
    COLLECTION = config.get('DB', 'COLLECTION')
    RAW_COLLECTION = config.get('DB', 'RAW_COLLECTION')

    MAX_DONE_PAGES = int(config.get('STOP', 'MAX_DONE_PAGES'))
    PAGE_LOAD_TIMEOUT = int(config.get('TIME', 'PAGE_LOAD_TIMEOUT'))
    SCRIPT_LOAD_TIMEOUT = int(config.get('TIME', 'SCRIPT_LOAD_TIMEOUT'))

    db_connection = Database(
        host=config.get('DB', 'HOST'),
        port=int(config.get('DB', 'PORT')),
        username=config.get('DB', 'USERNAME'),
        password=config.get('DB', 'PASSWORD'),
        authSource='admin',
        authMechanism='SCRAM-SHA-1',
        database=config.get('DB', 'DATABASE')
    ).get_db()
    # logger.info('Connect to MongoDB done')

    # bot = telebot.TeleBot(config.get('TELEBOT', 'TOKEN'))

    no_threads = int(config.get('THREAD', 'NUM_THREADS'))

    extract_raw_news(
        db=db_connection,
        thread=2
    )
    print("CMN")

    del db_connection
