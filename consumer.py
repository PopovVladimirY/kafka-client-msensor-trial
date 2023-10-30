from kafka import KafkaConsumer
import io
import avro.schema
from avro.io import DatumReader
import datetime 

broker='192.168.200.12:9092'
client_id = 'test'
msensor1_topic = "msensor2_topic"

avro_schema = '''{
    "namespace": "msensor.avro",
    "type": "record",
    "name": "msensor",
    "fields": [
        {"name": "timestamp", "type": ["float", "null"]},
        {"name": "wakeup_count", "type": ["int", "null"]},
        {"name": "temperature",  "type": ["float", "null"]},
        {"name": "pressure", "type": ["float", "null"]},
        {"name": "humidity", "type": ["float", "null"]},
        {"name": "battery", "type": ["float", "null"]},
        {"name": "soil", "type": ["float", "null"]}
    ]
}'''

schema = avro.schema.parse(avro_schema)

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(msensor1_topic,
                         group_id=client_id,
                         bootstrap_servers=broker,
#                         auto_offset_reset='earliest',
                         enable_auto_commit=False)

for msg in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`

    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    msensor = reader.read(decoder)

    print("\n%s:%d:%d: key=%s" % (msg.topic, msg.partition, msg.offset, msg.key))

    d = datetime.datetime.fromtimestamp(msensor['timestamp'])
    print(f"  Date/Time: {d}")
    print(f" Boot Count: {msensor['wakeup_count']}")
    print(f"Temperature: {msensor['temperature']:.1f} °C")
    print(f"   Pressure: {msensor['pressure']:.0f} Pa")
    print(f"   Humidity: {msensor['humidity']:.1f} %")
    print(f"    Battery: {msensor['battery']:.2f} V")
    print(f"       Soil: {msensor['soil']:.1f} %")


'''
# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)

# Subscribe to a regex topic pattern
consumer = KafkaConsumer()
consumer.subscribe(pattern='^awesome.*')

# Use multiple consumers in parallel w/ 0.9 kafka brokers
# typically you would run each on a different server / process / CPU
consumer1 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')
consumer2 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')
'''