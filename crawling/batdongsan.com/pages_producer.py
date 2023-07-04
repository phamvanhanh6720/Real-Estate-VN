import os
import pika
from configparser import ConfigParser

from bs4 import BeautifulSoup
import undetected_chromedriver as uc

from utils import scroll_down, get_main_page


if __name__ == '__main__':
    # read config
    config = ConfigParser()
    config.read(os.path.join('config', 'config.ini'))

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port='5672')
    )
    channel = connection.channel()
    channel.exchange_declare(exchange='exchange', exchange_type='direct')
    channel.exchange_declare('dlx_exchange', 'direct')

    channel.queue_declare(
        queue='pages_queue',
        durable=True,
        arguments={
            'x-dead-letter-exchange': 'dlx_exchange',
            'x-dead-letter-routing-key': 'dlx_page_routing'
        }
    )
    channel.queue_declare('dead_letter_queue')

    channel.queue_bind('dead_letter_queue', 'dlx_exchange', 'dlx_page_routing')
    channel.queue_bind(queue='pages_queue', exchange='exchange', routing_key='page_routing')

    PAGE_LOAD_TIMEOUT = int(config.get('TIME', 'PAGE_LOAD_TIMEOUT'))
    SCRIPT_LOAD_TIMEOUT = int(config.get('TIME', 'SCRIPT_LOAD_TIMEOUT'))
    MAX_SLEEP_TIME: int = int(config.get('TIME', 'MAX_SLEEP_TIME'))
    # PAGE_LOAD_TIMEOUT = 10
    # SCRIPT_LOAD_TIMEOUT = 5
    # MAX_SLEEP_TIME = 2

    seed_urls_file = config.get('URL', 'SEED_URL_FILE')
    with open(os.path.join('config', seed_urls_file), 'r') as file:
        seed_urls_list = file.readlines()
    seed_urls_list = [url.strip(' \n') for url in seed_urls_list if url != '']
    # seed_urls_list = ['https://batdongsan.com.vn/ban-can-ho-chung-cu-ha-noi']

    # create selenium driver
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.page_load_strategy = 'normal'
    driver = uc.Chrome(use_subprocess=False,
                       options=options
                       )
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.set_script_timeout(SCRIPT_LOAD_TIMEOUT)

    for seed_url in seed_urls_list:
        start_page = 0
        main_url = f'{seed_url}/p{start_page}'

        get_main_page(driver, main_url)
        scroll_down(driver, int(MAX_SLEEP_TIME))

        html_content = driver.page_source
        soup = BeautifulSoup(html_content, features='html.parser')

        pagination_components = soup.find_all('a', class_='re__pagination-number')
        max_page = max([int(item.text.strip('\n ')) for item in pagination_components])

        for page_th in range(max_page + 1):
            channel.basic_publish(
                exchange='exchange',
                routing_key='page_routing',
                body=f'{seed_url}/p{page_th}',
                properties=pika.BasicProperties(
                    delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                )
            )

    driver.close()
