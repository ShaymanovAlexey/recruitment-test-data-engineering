import datetime
import json
import threading
import time
from urllib.parse import urlparse
import requests
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic


class Producer(threading.Thread):
    def __init__(self, url, params):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.url = url
        self.params = params

    def stop(self):
        self.stop_event.set()

    def run(self):
        # выполняется get запрос к API
        response = requests.get(self.url, self.params)
        # в data записывается response в json формате
        data = response.json()

        # добавляем в data ключ created_at со значением текущей даты
        data['created_at'] = str(datetime.datetime.now())
        # парсим ссылку переданную в self.url на части и в ключ source_key добавляем распарсенную базовую ссылку ресурса
        parsed_uri = urlparse(self.url)
        data['source_key'] = f'{parsed_uri.scheme}://{parsed_uri.netloc}'

        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while not self.stop_event.is_set():
            producer.send('my-topic', data)
            time.sleep(1)

        producer.close()


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def _save_to_json(self, data):
        with open("res_file.json", "a") as f:
            json.dump(str(data), f)

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['my-topic'])

        while not self.stop_event.is_set():
            for message in consumer:
                self._save_to_json(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')

        topic = NewTopic(name='my-topic',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception:
        pass

    # в url указываем ссылку на API iTunes
    #url = 'https://itunes.apple.com/search'
    url = 'https://music.yandex.ru/search'

    # в params ключ значения, по которым нужно вести поиск в API
    params = {'text': 'anacondaz', 'type': 'tracks'}
    tasks = [
        Producer(url, params),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == '__main__':
    main()
