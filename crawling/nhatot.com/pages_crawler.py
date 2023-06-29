import requests
import json
import pika
from tqdm import tqdm

if __name__ == '__main__':
    rabbitmq_connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='localhost',
            port='5672'
        )
    )

    channel = rabbitmq_connection.channel()

    channel.exchange_declare('nhatot_exchange', exchange_type='direct')
    channel.exchange_declare('nhatot_dlx_exchange', exchange_type='direct')
    channel.queue_declare(
        queue='nhatot_news_queue',
        durable=True,
        arguments={
            'x-dead-letter-exchange': 'nhatot_dlx_exchange',
            'x-dead-letter-routing-key': 'nhatot_dlx_news_routing'
        }
    )

    channel.queue_declare('nhatot_dead_letter_news_queue')
    channel.queue_bind('nhatot_dead_letter_news_queue', 'nhatot_dlx_exchange', 'nhatot_dlx_news_routing')
    channel.queue_bind(queue='nhatot_news_queue', exchange='nhatot_exchange', routing_key='nhatot_news_routing')

    region_v2 = 12000
    cg = 1000
    start_partition = 0
    page = 1
    api_gateway = 'https://gateway.chotot.com/v1/public/ad-listing?region_v2={}&cg={}&o={}&page={}&st=s,k&limit=20&w=1&key_param_included=true'

    api_url = api_gateway.format(region_v2, cg, start_partition, page)
    response = requests.get(api_url)
    data = json.loads(response.text)

    total_news = data['total']
    max_pages = total_news // 20 + 1
    for page_th in tqdm(range(1, max_pages + 1)):
        start_partition = (page_th - 1) * 20
        api_url = api_gateway.format(region_v2, cg, start_partition, page_th)

        response = requests.get(api_url)
        data = json.loads(response.text)

        if len(data['ads']) != 0:
            for item in data['ads']:
                channel.basic_publish(
                    exchange='nhatot_exchange',
                    routing_key='nhatot_news_routing',
                    body=json.dumps({'list_id': item['list_id'], 'list_time': item['list_time']}),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    )
                )

    rabbitmq_connection.close()