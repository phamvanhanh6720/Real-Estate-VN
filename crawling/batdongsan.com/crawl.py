import os
import time
import random
import logging
import datetime
import threading
from configparser import ConfigParser

import telebot
from bs4 import BeautifulSoup
import undetected_chromedriver as uc

from utils import scroll_down, parser_log, create_visualize_figure, get_main_page
from connector import Database


def extract_news_data(element, driver):
    url = element.get('href')
    url = f'{HOSTNAME}{url}'
    title = element.get('title')

    price_element = element.find('span', class_='re__card-config-price')
    price = price_element.text if price_element else None
    if price is not None:
        if 'tỷ' in price:
            price = float(price.strip(' tỷ'))
        elif 'triệu' in price:
            price = float(price.strip(' triệu')) / 1000

    price_per_m2_element = element.find('span', class_='re__card-config-price_per_m2')
    price_per_m2 = price_per_m2_element.text if price_per_m2_element else None
    if price_per_m2 is not None:
        price_per_m2 = float(price_per_m2.strip(' triệu/m²'))

    area_element = element.find('span', class_='re__card-config-area')
    area = area_element.text if area_element else None
    if area is not None:
        area = float(area.strip(' m²'))

    no_bedroom_element = element.find('span', class_='re__card-config-bedroom')
    no_bedroom = no_bedroom_element.find('span').text if no_bedroom_element else None

    no_toilet_element = element.find('span', class_='re__card-config-toilet')
    no_toilet = no_toilet_element.find('span').text if no_toilet_element else None

    location_element = element.find('div', class_='re__card-location')
    location = location_element.text.strip('\n ') if location_element else None

    published_date_element = element.find('span', class_='re__card-published-info-published-at')
    published_date = published_date_element.get('aria-label') if published_date_element else None

    if '-prj-' in url:
        project = url.split('-prj-')[-1].split('/')[0]
    else:
        project = None
    news_data = {
        'url': url,
        'title': title,
        'price': price,
        'price_per_m2': price_per_m2,
        'area': area,
        'location': location,
        'no_bedroom': no_bedroom,
        'no_toilet': no_toilet,
        'published_date': published_date,
        'project': project
    }
    # crawl_each_news_item(driver, url, MAX_SLEEP_TIME, news_data, page_load_time_out=PAGE_LOAD_TIMEOUT)

    return news_data


def fetch_raw_news(db, seed_url_list, log, thread):
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

    for seed_url in seed_url_list:
        real_estate_type = None
        if 'can-ho-chung-cu' in seed_url:
            real_estate_type = 'can-ho-chung-cu'
        elif 'nha-rieng' in seed_url:
            real_estate_type = 'nha-rieng'
        elif 'nha-biet-thu-lien-ke' in seed_url:
            real_estate_type = 'nha-biet-thu-lien-ke'
        elif 'nha-mat-pho' in seed_url:
            real_estate_type = 'nha-mat-pho'
        elif 'shophouse-nha-pho-thuong-mai' in seed_url:
            real_estate_type = 'shophouse-nha-pho-thuong-mai'
        elif 'dat-dat-nen' in seed_url:
            real_estate_type = 'dat-dat-nen'
        real_estate_type = real_estate_type.replace('-', '_')

        # while True:
        start_page = 0
        no_saved_news = 0

        is_failed = False
        failed_count = 0
        while True:
            if failed_count >= 10:
                failed_count = 0
                start_page += 1

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

            main_url = f'{seed_url}/p{start_page}'
            log.info(f"Start crawl pages: {main_url}")
            try:
                get_main_page(driver, main_url)
            except Exception as e:
                print(e)

                failed_count += 1
                is_failed = True
                driver.quit()

                del driver
                continue

            scroll_down(driver, int(MAX_SLEEP_TIME))

            html_content = driver.page_source
            soup = BeautifulSoup(html_content, features="html.parser")

            if soup.find('div', class_='re__srp-empty js__srp-empty') or no_saved_news > MAX_DONE_PAGES:
                log.info(f"Fetched all news of {seed_url}")
                break

            news_elements = soup.find_all('a', class_='js__product-link-for-product-id')
            for ele in news_elements:
                try:
                    news_data = extract_news_data(ele, driver)
                    news_data['real_estate_type'] = real_estate_type
                    news_data['is_done'] = False

                    if db[COLLECTION].find_one({'url': news_data['url'], 'published_date': news_data['published_date']}) is not None:
                        no_saved_news += 1
                        log.info(f"{real_estate_type} - Already exist: {HOSTNAME}{ele.get('href')}")
                        print(f"{real_estate_type} - Already exist: {HOSTNAME}{ele.get('href')}")
                        continue

                    try:
                        db[RAW_COLLECTION].insert_one(news_data)
                        log.info(f"{real_estate_type} - Fetch raw: {HOSTNAME}{ele.get('href')}")
                        print(f"{real_estate_type} - Fetch raw: {HOSTNAME}{ele.get('href')}")
                    except:
                        no_saved_news += 1
                        log.info(f"{real_estate_type} - Already exist: {HOSTNAME}{ele.get('href')}")
                        print(f"{real_estate_type} - Already exist: {HOSTNAME}{ele.get('href')}")

                except Exception as e:
                    print(e)
                    log.info(f"{real_estate_type} - Fail: {HOSTNAME}{ele.get('href')}")
                    print(f"{real_estate_type} - Fail: {HOSTNAME}{ele.get('href')}")

            time.sleep(random.randint(0, MAX_SLEEP_TIME))
            start_page += 1

    driver.close()


if __name__ == '__main__':
    # create logger
    crawling_date = datetime.date.today()
    logging.basicConfig(filename=f'logs/batdongsan_raw_{crawling_date}.log',
                        format='%(asctime)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

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
    logger.info('Connect to MongoDB done')

    bot = telebot.TeleBot(config.get('TELEBOT', 'TOKEN'))

    # seed url list
    seed_urls_file = config.get('URL', 'SEED_URL_FILE')
    with open(os.path.join('config', seed_urls_file), 'r') as file:
        seed_urls_list = file.readlines()
    seed_urls_list = [url.strip(' \n') for url in seed_urls_list if url != '']

    no_threads = int(config.get('THREAD', 'NUM_THREADS'))

    fetch_raw_news(
        db=db_connection,
        seed_url_list=seed_urls_list,
        log=logger,
        thread=1
    )

    counting_reports, total_time = parser_log(log_file=f'logs/batdongsan_raw_{crawling_date}.log')
    create_visualize_figure(
        counting_reports=counting_reports,
        save_file=f'daily_reports/batdongsan_raw_{crawling_date}.png'
    )

    bot.send_message(
        chat_id=int(config.get('TELEBOT', 'CHAT_ID')),
        text=f'REPORT - Batdongsan.com - {crawling_date} \n Total time: {total_time} hours'
    )

    bot.send_photo(
        chat_id=int(config.get('TELEBOT', 'CHAT_ID')),
        photo=open(f'daily_reports/batdongsan_raw_{crawling_date}.png', 'rb')
    )
    bot.stop_bot()
