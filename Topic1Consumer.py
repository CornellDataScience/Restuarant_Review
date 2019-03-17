from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'restaurant_review',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=10000
    )
print('running')
for message in consumer:
    print(message.value)