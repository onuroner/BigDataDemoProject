from datetime import datetime

from pymongo.mongo_client import MongoClient

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer
from Utils import *
import json

#from airflow.operators.python import PythonOperator




    
client = MongoClient("mongodb+srv://onuroner4054:K6gOX03HjbCSZUvy@onurcluster.cpndim2.mongodb.net/")
db = client["events"]


def save_data_to_mongo(record):
    print("Insert started...")
    collection = db["send_event_events"]
    collection.insert_one(record)
    print("Event inserted successfully.")


def consume_messages():
    print("Consume started....")
    #sr_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient({'url': 'http://localhost:8081'})
    #schema_str = get_schema_from_schema_registry(schema_registry_url = 'http://localhost:8081', schema_registry_subject = "UserEvents-value")

    avro_deserializer = AvroDeserializer(schema_registry_client)


    consumer_conf = {'bootstrap.servers': 'localhost:9092',
                    'value.deserializer': avro_deserializer,
                    'group.id': 'user-event-consumer',
                    'auto.offset.reset': "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['UserEvents'])
    

    while True:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1.0)
        print(msg)
        if msg is None:
            continue

        data = msg.value()
        
        event = SendEvent(data['user_id'], data['session_id'], data['event_name'], data['timestamp'], data['product_id'], data['price'], data['discount'])
        print(event)
        save_data_to_mongo(data)

consume_messages()




