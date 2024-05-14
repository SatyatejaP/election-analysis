import random
import string

import psycopg2
import requests
import simplejson as json
from confluent_kafka.admin import AdminClient, NewTopic


with open("config.json") as cnf:
    config = json.load(cnf)

postgres_cnf: dict = config['postgres']
sample_data_cnf: dict = config['sample_data']
kafka_cnf: dict = config['kafka']
voting_cnf: dict = config['voting']
sample_data_cnf: dict = config['sample_data']

def createKafkaTopic(topic_name):
    admin_client = AdminClient({'bootstrap.servers': kafka_cnf['bootstrap_servers']})
    existing_topics = admin_client.list_topics().topics

    if topic_name not in existing_topics:
        admin_client.create_topics([NewTopic(topic_name)])
        print(f"Topic '{topic_name}' created successfully.")
    else:
        print(f"Topic '{topic_name}' already exists.")

def getDbConn():
    return psycopg2.connect(
        database=postgres_cnf['database'],
        user=postgres_cnf['user'],
        password=postgres_cnf['password'],
        host=postgres_cnf['host'],
        port=postgres_cnf['port'],
    )

def consume_messages( consumer, topic):
    consumer.subscribe([topic])
    data = []
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            print("No new messages in topic[{}]".format(topic))
            break
        elif msg.error():
            print("Kafka Consumer error for topic[{}]: {}".format(topic, msg.error()))
            break
        else:
            data.append(json.loads(msg.value().decode('utf-8')))
    return data
