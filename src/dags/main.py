import avro.schema
from fastapi import FastAPI
from Models.Models import *
from Utils import *
import json
from pykafka import KafkaClient
import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer

app = FastAPI()

# kafkaClient = KafkaClient(hosts="localhost:9092")
# topic = kafkaClient.topics['UserEvents']
# producer_config = {
#     "bootstrap.servers": "localhost:9092",
#     "schema.registry.url": "http://localhost:8081"
    
# }

schema_registry_url = 'http://localhost:8081'

schema_registry_subject = "UserEvents-value"
schema_registry_subject_purchased = "PurchasedItems-value"
kafka_url = "localhost:9092"
kafka_topic = 'UserEvents'





@app.put('/events/')
def SendEvents(event:SendEvent):
    print(event, type(event))
    #eventDict = json.dumps(event)
    # topic_producer = AvroProducer(producer_config, default_value_schema=avro.schema.parse(open("D:\\BigDataApi\\Models\\UserEventsSchema.avsc","rb").read()))
    # topic_producer.produce(topic=topic, value=event.to_dict())
    # topic_producer.flush()

    avro_producer(event.to_dict(),kafka_url,"UserEvents",schema_registry_url,schema_registry_subject)
    

@app.post('/items/')
def SendPurchasedItems(event:PurchasedItems):
    print(event, type(event))
    avro_producer(event.to_dict(),kafka_url,"PurchasedItems",schema_registry_url,schema_registry_subject_purchased)