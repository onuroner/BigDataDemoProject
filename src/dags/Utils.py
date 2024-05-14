from faker import Faker
import random
from datetime import datetime
from Models.Models import *
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer

from faker import Faker
fake = Faker()
Faker.seed(0)
kafka_topic = 'UserEvents'

def create_fake_event():
    user_id = fake.pyint(1, 10)
    session_id = fake.pyint()
    event_name = random.choice(list(SendEventNames))
    timestamp = fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    product_id = fake.pyint(1, 10)
    price = random.uniform(1, 1000)
    discount = random.uniform(0, 1)

    return SendEvent(user_id, session_id, event_name, timestamp, product_id, price, discount)

def create_fake_purchased_items():
    sessionId = fake.pyint(100000, 999999)
    timestamp = fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    userId = fake.pyint(0, 10)
    totalPrice = 0
    orderId = fake.pyint(100000, 999999)
    products = create_fake_products()

    for product in products:
        totalPrice += product["item_count"] * product["item_price"]

    return PurchasedItems(sessionId, timestamp, userId, totalPrice, orderId, products)

def create_fake_products():
    products = []
    product_count = fake.random_int(1,10)
    for i in range(product_count):
        product_id = fake.pyint(1, 10)
        item_count = fake.pyint(1, 10)
        item_price = random.uniform(1, 1000)
        item_discount = random.uniform(0, 1)
        product = Product(product_id, item_count, item_price, item_discount)
        product_dict = product.to_dict()
        products.append(product_dict)

    return products

def get_schema_from_schema_registry(schema_registry_url, schema_registry_subject):
    sr = SchemaRegistryClient({'url': schema_registry_url})
    latest_version = sr.get_latest_version(schema_registry_subject)

    return sr, latest_version

def avro_producer(decoded_json, kafka_url,topic, schema_registry_url, schema_registry_subject):
    # schema registry
    sr, latest_version = get_schema_from_schema_registry(schema_registry_url, schema_registry_subject)


    value_avro_serializer = AvroSerializer(schema_registry_client = sr,
                                          schema_str = latest_version.schema.schema_str,
                                          conf={
                                              'auto.register.schemas': False
                                            }
                                          )

    # Kafka Producer
    producer = SerializingProducer({
        'bootstrap.servers': kafka_url,
        'security.protocol': 'plaintext',
        'value.serializer': value_avro_serializer,
        'delivery.timeout.ms': 120000, # set it to 2 mins
        'enable.idempotence': 'true'
    })

                    
    try:
        # print(decoded_line + '\n')
        producer.produce(topic=topic, value=decoded_json)

        # Trigger any available delivery report callbacks from previous produce() calls
        events_processed = producer.poll(1)
        print(f"events_processed: {events_processed}")

        messages_in_queue = producer.flush(1)
        print(f"messages_in_queue: {messages_in_queue}")
    except Exception as e:
        print(e)

def dict_to_send_event(obj, ctx):
    """
    Converts object literal(dict) to a User instance.

    Args:
        obj (dict): Object literal(dict)

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    """

    if obj is None:
        return None

    return SendEvent(obj.user_id, obj.session_id, obj.event_name, obj.timestamp, obj.product_id, obj.price, obj.discount)
