import os
import pika
import json
from configparser import ConfigParser

from bs4 import BeautifulSoup
import undetected_chromedriver as uc

from utils import scroll_down, get_main_page
from connector import Database


if __name__ == '__main__':
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

    # rabbitmq connection
    rabbitmq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='localhost',
            port='5672'
        )
    )
    channel = rabbitmq_connection.channel()

    channel.queue_declare(
        queue='news_queue',
        durable=True,
        arguments={
            'x-dead-letter-exchange': 'dlx_exchange',
            'x-dead-letter-routing-key': 'dlx_new_routing'
        }
    )

    channel.queue_declare('dead_letter_news_queue')
    channel.queue_bind('dead_letter_news_queue', 'dlx_exchange', 'dlx_new_routing')
    channel.queue_bind(queue='news_queue', exchange='exchange', routing_key='news_routing')

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

    def callback(ch, method, properties, body):
        page_url = body.decode()
        real_estate_type = None
        if 'can-ho-chung-cu' in page_url:
            real_estate_type = 'can-ho-chung-cu'
        elif 'nha-rieng' in page_url:
            real_estate_type = 'nha-rieng'
        elif 'nha-biet-thu-lien-ke' in page_url:
            real_estate_type = 'nha-biet-thu-lien-ke'
        elif 'nha-mat-pho' in page_url:
            real_estate_type = 'nha-mat-pho'
        elif 'shophouse-nha-pho-thuong-mai' in page_url:
            real_estate_type = 'shophouse-nha-pho-thuong-mai'
        elif 'dat-dat-nen' in page_url:
            real_estate_type = 'dat-dat-nen'

        real_estate_type = real_estate_type.replace('-', '_')

        try:
            get_main_page(driver, page_url)

            scroll_down(driver, int(MAX_SLEEP_TIME))

            html_content = driver.page_source
            soup = BeautifulSoup(html_content, features="html.parser")

            if soup.find('div', class_='re__srp-empty js__srp-empty'):
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                news_elements = soup.find_all('a', class_='js__product-link-for-product-id')
                for ele in news_elements:
                    url = ele.get('href')
                    news_url = f'{HOSTNAME}{url}'

                    published_date_element = ele.find('span', class_='re__card-published-info-published-at')
                    published_date = published_date_element.get('aria-label') if published_date_element else None

                    # if db_connection[COLLECTION].find_one(
                    #         {'url': news_url, 'published_date': published_date}) is not None:
                    if False:
                        print("Already exist")
                    else:
                        ch.basic_publish(
                            exchange='exchange',
                            routing_key='news_routing',
                            body=json.dumps({
                                'url': news_url,
                                'published_date': published_date,
                                'real_estate_type': real_estate_type
                            }),
                            properties=pika.BasicProperties(
                                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                            )
                        )

                ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f'Fail: {page_url}')
            print(e)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='pages_queue', on_message_callback=callback)
    channel.start_consuming()
