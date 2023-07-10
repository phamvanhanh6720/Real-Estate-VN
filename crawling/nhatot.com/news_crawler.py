import os
import pika
import json
import requests
from datetime import datetime
from configparser import ConfigParser

import pymongo


config = ConfigParser()
config.read(os.path.join('config', 'config.ini'))

COLLECTION = config.get('DB', 'COLLECTION')
API_GATEWAY = config.get('API', 'DATA_API')

mongodb_connection = pymongo.MongoClient(
    host=config.get('DB', 'HOST'),
    port=int(config.get('DB', 'PORT')),
    username=config.get('DB', 'USERNAME'),
    password=config.get('DB', 'PASSWORD'),
    authSource='admin',
    authMechanism='SCRAM-SHA-1',
)
db_connection = mongodb_connection[config.get('DB', 'DATABASE')]

rabbitmq_connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        port='5672'
    )
)

channel = rabbitmq_connection.channel()

channel.basic_qos(prefetch_count=1)
while True:
    method_frame, _, body = channel.basic_get(queue='nhatot_news_queue', auto_ack=False)
    if method_frame is None:
        # No more messages in the queue, break the loop
        break

    data_id = body.decode()
    data_id = json.loads(data_id)

    try:
        list_id = data_id['list_id']
        list_time = data_id['list_time']
        url = API_GATEWAY + str(list_id)

        # check if already in collection
        if db_connection[COLLECTION].find_one({'list_id': list_id, 'list_time': list_time}) \
                is not None:

            print(f"Already in collection: {list_id}")
        else:
            response = requests.get(url)
            if response.status_code == 200:
                detail_data = json.loads(response.text)
                detail_data['list_id'] = list_id
                detail_data['list_time'] = list_time

                detail_data['crawl_time'] = datetime.now()

                db_connection[COLLECTION].insert_one(detail_data)
                print(f'Success: {list_id}')
            else:
                raise Exception('Request failed with status code: ' + str(response.status_code))

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    except Exception as e:
        print(f"Failed: {list_id}")
        print(e)
        channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=False)

channel.close()
mongodb_connection.close()
rabbitmq_connection.close()
print('Crawling is done')