from kafka import KafkaConsumer
from json import loads

def consume_topic1_message():
    consumer = KafkaConsumer(
        'YelpTopic1',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    RestaurantDict = {}
    for message in consumer:
        RestaurantDict = {**RestaurantDict, **message.value}
    return RestaurantDict

def consume_topic2_message():
    consumer = KafkaConsumer(
        'ZomatoTopic2',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    RestaurantDict = {}
    for message in consumer:
        RestaurantDict = {**RestaurantDict, **message.value}
    return RestaurantDict