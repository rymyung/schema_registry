from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from schemas import person_pb2
from core.constants import BASE_SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVER_URL


# Schema Registry 설정
schema_registry_conf = {
    "url": BASE_SCHEMA_REGISTRY_URL
}
schema_registry = SchemaRegistryClient(schema_registry_conf)

# Protobuf Deserializer 설정
protobuf_deserializer = ProtobufDeserializer(
    person_pb2.Person,
    conf={"use.deprecated.format": False}
)

# Kafka Consumer 설정
consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVER_URL,
    "key.deserializer": StringDeserializer("utf_8"),
    "value.deserializer": protobuf_deserializer,
    "group.id": "fifth_consumer",
    "auto.offset.reset": "earliest",
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(["person"])

msg = consumer.poll(10)

if msg is not None:
    try:
        person = msg.value()
        if person:
            print(f"Received person: {person.name}, {person.id}, {person.email}")
            if person.address:
                print(f"address: {person.address}")
            if person.phone:
                print(f"phone: {person.phone}")
        else:
            print("No Message")
    except Exception as e:
        print(f"Schema validation failed: {e}")
else:
    print("No message received within the timeout period.")

consumer.close()
