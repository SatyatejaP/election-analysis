import random
import threading
import time

import psycopg2
import simplejson as json
from confluent_kafka import SerializingProducer
from utils import createKafkaTopic, postgres_cnf, kafka_cnf, voting_cnf, getDbConn

voting_kafka_producer = None


def getKafkaProducer():
    global voting_kafka_producer
    if voting_kafka_producer is None:
        voting_kafka_producer = SerializingProducer({
            'bootstrap.servers': kafka_cnf['bootstrap_servers'],
        })

    return voting_kafka_producer


def onDelivery(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to Topic[{msg.topic()}] partition[{msg.partition()}]')


def voting():
    # creates voting kafka topic if not exists
    createKafkaTopic(kafka_cnf['voting_topic'])

    # paginate the voters table
    conn = getDbConn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM {}.voters ".format(postgres_cnf['schema']))
    total_records = cursor.fetchone()[0]

    # paginating over voters
    page_size = voting_cnf['votes_per_iteration']
    delay_time = voting_cnf['iteration_time_in_ms']

    num_pages = total_records // page_size

    for i in range(num_pages):
        page_number = i + 1
        offset = (page_number - 1) * page_size
        cursor.execute(
            """SELECT * FROM {}.voters LIMIT {} OFFSET {}"""
            .format(postgres_cnf['schema'], page_size, offset))
        voters = list(map(lambda x: {'id': x[0], 'name': x[1], 'gender': x[2], 'age': x[3], 'city': x[4],
                                     'state': x[5], 'pincode': x[6], 'phone_number': x[7],
                                     'constituency_id': x[8]}, cursor.fetchall()))

        constituencies = set(map(lambda x: x['constituency_id'], voters))

        # query candidates table to get consitituency to candidates mapping
        cursor.execute(f"""
            SELECT c.constituency_id, array_agg(c.id) as cands 
            FROM {postgres_cnf['schema']}.candidates as c
            WHERE c.constituency_id IN ({','.join(list(map(lambda x: f"'{x}'", constituencies)))})
            GROUP BY c.constituency_id
        """)
        cons = dict(cursor.fetchall())
        for voter in voters:
            vote = voter | {'candidate_id': random.choice(cons[voter['constituency_id']]),
                            'voted_time': int(time.time())}
            # pushing vote to kafka
            try:
                producer = getKafkaProducer()
                producer.produce(kafka_cnf['voting_topic'],
                                 key=vote['id'],
                                 value=json.dumps(vote),
                                 on_delivery=onDelivery)
                producer.poll(0)
            except Exception as e:
                print(f"pushing vote to kafka failed with {e}")
                continue

            #pushing vote to db
            cursor = conn.cursor()
            sql = f"""
                           INSERT INTO {postgres_cnf['schema']}.votes (voter_id, candidate_id, voting_time)
                           VALUES (%s, %s, %s)
                    """
            cursor.execute(sql, (vote['id'], vote['candidate_id'], vote['voted_time']))
            conn.commit()

        time.sleep(delay_time // 1000)


if __name__ == '__main__':
    voting()
