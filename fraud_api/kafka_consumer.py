from confluent_kafka import Consumer, KafkaException
import json
from utility import *
from kafka_producer import *
from views import TransactionViewSet,AlertViewSet

def handle_message(topic,message):
    if "transaction-create" in topic:
        txn_list_objects = TransactionViewSet.create_transactions(message["transactions"])
        
        produce_message(
            "alert-create",{"alerts":{
            "transaction_list":txn_list_objects,
            "user":message["transactions"]["user"],
           }})
    elif "alert-create" in topic:
        AlertViewSet.create_alerts(message["alerts"])
        

def consume_message(topic):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': topic,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    break

            print(f"Received message: {msg.value().decode('utf-8')}")
            handle_message(topic,msg.value())

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
