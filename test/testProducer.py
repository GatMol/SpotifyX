from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import msgpack

# producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# # produce keyed messages to enable hashed partitioning
# producer.send('my-topic', key=b'foo', value=b'bar')

# # produce asynchronously
# for _ in range(100):
#     producer.send('my-topic', b'msg')

# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)

# def on_send_error(excp):
#     # log.error('I am an errback', exc_info=excp)
#     print('I am an errback', exc_info=excp)
#     # handle exception

# # produce asynchronously with callbacks
# producer.send('my-topic', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

# # block until all async messages are sent
# producer.flush()

# # configure multiple retries
# producer = KafkaProducer(retries=5)

# produce json messages
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'), bootstrap_servers=['localhost:9092'])
producer.flush()
# producer.send('json-topic', {'key': 'value'}).add_callback(on_send_success).add_errback(on_send_error)

while True:
    producer.send('json-topic', {'key': 'value'})