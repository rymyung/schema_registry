from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from schemas import person_pb2
from core.constants import BASE_SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVER_URL


# Schema Registry 설정
schema_registry_conf = {
    "url": BASE_SCHEMA_REGISTRY_URL
}
schema_registry = SchemaRegistryClient(schema_registry_conf)

# Protobuf Serializer 설정
protobuf_serializer = ProtobufSerializer(
    person_pb2.Person, 
    schema_registry_client=schema_registry,
    conf={"use.deprecated.format": False}
)

# kafka Producer 설정
producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVER_URL,
    "key.serializer": StringSerializer("utf_8"), # key를 문자열로 직렬화
    "value.serializer": protobuf_serializer, # value를 Protobuf로 직렬화
}

producer = SerializingProducer(producer_conf)

# Protobuf 메시지 작성
# person = person_pb2.Person(name="Alice", id=1234, email="alice@example.com")
person = person_pb2.Person(name="Johnson", id=2345, email="johnson@example.com", address="LA", phone="000-111-2222")

# kafka로 메시지 전송
producer.produce(topic="person", key=str(person.id), value=person)

# 메시지 전송 완료 대기
producer.flush()
