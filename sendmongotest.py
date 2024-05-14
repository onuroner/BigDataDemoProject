from datetime import datetime

from pymongo.mongo_client import MongoClient
#from airflow import DAG
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer
from src.dags.Utils import *

from airflow.operators.python import PythonOperator



# with DAG(
#     dag_id="homework",
#     start_date=datetime(2022, 1, 1),
#     catchup=False,
#     schedule_interval="*/2 * * * *"
# ) as dag:
    
client = MongoClient("mongodb+srv://onuroner4054:K6gOX03HjbCSZUvy@onurcluster.cpndim2.mongodb.net/")
db = client["events"]


def save_data_to_mongo(record):

    collection = db["send_event_events"]
    collection.insert_one(record.__dict__)
    print("Event inserted successfully")


def consume_messages():
    
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
        if msg is None:
            break

        data = msg.value()
        save_data_to_mongo(data)

#send_data = PythonOperator(task_id="save_data_to_mongo", python_callable=consume_messages,  dag=dag)


