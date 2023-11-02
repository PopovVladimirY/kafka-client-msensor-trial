from kafka import KafkaConsumer
import io
import avro.schema
from avro.io import DatumReader
import datetime 

broker='192.168.200.12:9092'
client_id = 'test-msensor'
msensor_topic = "msensor_live_topic"

avro_schema = '''{
    "namespace": "msensor.avro",
    "type": "record",
    "name": "msensor",
    "fields": [
        {"name": "device", "type": ["string", "null"]},
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
consumer = KafkaConsumer(group_id=client_id,
                         bootstrap_servers=broker)
#                        auto_offset_reset='earliest')g
#                         ,enable_auto_commit=False)

consumer.subscribe(pattern='^.*msensor_live_topic$')

for msg in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`

    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    msensor = reader.read(decoder)

    print("\n%s:%d:%d: key=%s" % (msg.topic, msg.partition, msg.offset, msg.key))

    d = datetime.datetime.fromtimestamp(msensor['timestamp'])
    print(f"     Device: {msensor['device']}")
    print(f"  Date/Time: {d}")
    print(f" Boot Count: {msensor['wakeup_count']}")
    print(f"Temperature: {msensor['temperature']:.1f} °C")
    print(f"   Pressure: {msensor['pressure']:.0f} Pa")
    print(f"   Humidity: {msensor['humidity']:.1f} %")
    print(f"    Battery: {msensor['battery']:.2f} V")
    print(f"       Soil: {msensor['soil']:.1f} %")
