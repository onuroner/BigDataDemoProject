from datetime import datetime

from pymongo.mongo_client import MongoClient
from airflow import DAG
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer
from Utils import *
import json

from airflow.operators.python import PythonOperator



with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="*/2 * * * *"
) as dag:
    
    client = MongoClient("mongodb+srv://onuroner4054:K6gOX03HjbCSZUvy@onurcluster.cpndim2.mongodb.net/")
db = client["events"]


def save_data_to_mongo(record):
    print("Insert started...")
    collection = db["send_event_events"]
    collection.insert_one(record)
    print("Event inserted successfully.")

def update_aggregations():
    
    collection = db["send_event_events"]
    
    user_ids = collection.distinct("user_id")

    for user_id in user_ids:
        # Her bir UserId için EventName ve sayısını hesapla
        pipeline = [
            {"$match": {"user_id": user_id}},
            {"$group": {"_id": "$event_name", "EventCount": {"$sum": 1}}}
        ]
        events = collection.aggregate(pipeline)

        # Her bir EventName için EventCount'u güncelle veya ekle
        for event in events:
            event_name = event["_id"]
            event_count = event["EventCount"]

            # Güncellenecek veya eklenecek belgeyi bul
            query = {"UserId": user_id, "EventName": event_name}
            update = {"$set": {"EventCount": event_count}}

            # Eğer belge bulunursa güncelle, bulunmazsa ekle
            db["user_events_counts"].update_one(query, update, upsert=True)

    # MongoDB bağlantısını kapat
    client.close()


def consume_messages():
    print("Consume started....")
    #sr_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
    #schema_str = get_schema_from_schema_registry(schema_registry_url = 'http://localhost:8081', schema_registry_subject = "UserEvents-value")

    avro_deserializer = AvroDeserializer(schema_registry_client)


    consumer_conf = {'bootstrap.servers': 'broker:29092',
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
            consumer.close()
            break

        data = msg.value()
        
        
        save_data_to_mongo(data)

send_data = PythonOperator(task_id="send_data_to_mongo", python_callable=consume_messages,  dag=dag)
aggregation = PythonOperator(task_id="aggregation", python_callable=update_aggregations, dag=dag)

send_data >> aggregation


