#!/usr/bin/env python
from random import choice
from confluent_kafka import Producer

if __name__=='__main__':
    config = {
        # User-specific properties that you must set
        'bootstrap.servers': 'localhost:9092',
        # Fixed properties
        'acks': 'all'
    }

    # creating producer instance
    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print(f'ERROR: Message failed delivery: {err}')
        else:
            print(f"producer event to topic {msg.topic()}: key = {msg.key().decode('utf-8'):12} calue = {msg.value().decode('utf-8'):12}")
    
    # produce data by selecting random values from these lists

    topic = "purchases"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0

    for _ in range(100000):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1
    
    # block untill the messages are sent
    producer.poll(10000)
    producer.flush()