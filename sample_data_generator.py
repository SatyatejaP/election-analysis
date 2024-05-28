import random
import string
import uuid

import requests
import simplejson as json
from datetime import datetime
from utils import postgres_cnf, sample_data_cnf, getDbConn

random_user_url = 'https://randomuser.me/api?nat=in'
constituencies: any = None
parties: any = None


def get_constituencies():
    global constituencies
    if constituencies is None:
        with open('resources/constituencies.json', 'r') as f:
            data = json.load(f)

        constituencies = random.sample(data, sample_data_cnf['max_total_constituencies'])

    return constituencies


def get_parties():
    global parties
    if parties is None:
        with open('resources/parties.json', 'r') as f:
            data = json.load(f)
        major_parties = list(filter(lambda x: x['id'] in ['BJP', 'INC'], data))
        minor_parties = list(filter(lambda x: x['id'] not in ['BJP', 'INC'], data))
        random_parties = random.sample(minor_parties,
                                       sample_data_cnf['max_total_parties'] - 2)

        parties = major_parties + random_parties

    return parties


def generate_candidates(num: int, constituency_id: string):
    response = requests.get(random_user_url + f'&results={num}')
    if response.status_code != 200:
        raise Exception("Failed fetching candidate resources")
    pars: list = list(map(lambda x: x['id'], get_parties()))
    filtered_parties = list(filter(lambda x: x not in ['INC', 'BJP'], pars))

    candidates = []
    for i in range(num):
        data = response.json()['results'][i]
        party_id = None
        if i == 0:
            isBJPContesting = random.randint(0, 10) % 2 == 0
            if isBJPContesting:
                party_id = 'BJP'
        elif i == 1:
            isINCContesting = random.randint(0, 10) % 2 == 0
            if isINCContesting:
                party_id = 'INC'

        if party_id is None:
            party_id = random.choice(filtered_parties)

        candidates.append((
            str(uuid.uuid4()),
            f"{data['name']['title']} {data['name']['first']} {data['name']['last']}",
            data['gender'],
            data['dob']['age'],
            data['picture']['large'],
            party_id,
            constituency_id,
        ))
    return candidates


def push_candidates(conn, candidates):
    cursor = conn.cursor()
    sql = f"""
                            INSERT INTO {postgres_cnf['schema']}.candidates (id, name, gender, age, photo_url, 
                            party_id, constituency_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(sql, candidates)


def populate_candidates():
    cons = list(map(lambda x: x['id'], get_constituencies()))
    conn = getDbConn()

    for con in cons:
        num = random.randint(sample_data_cnf['min_candidates_per_constituency'],
                             sample_data_cnf['max_candidates_per_constituency'])
        if num % 2 == 0:
            num += 1
        print(f"generating [{num}] candidates for constituency: {con}")
        candidates = generate_candidates(num, con)
        push_candidates(conn, candidates)

    conn.commit()
    conn.close()


def push_voters(conn, voters):
    cursor = conn.cursor()
    sql = f""" INSERT INTO {postgres_cnf['schema']}.voters (id, name, gender, age, city, state, pincode, phone_number, constituency_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.executemany(sql, voters)


def generate_voters(num: int, con: string):
    voters = []
    response = requests.get(random_user_url + f'&results={num}')
    if response.status_code == 200:
        for i in range(num):
            data = response.json()['results'][i]
            voters.append((
                str(uuid.uuid4()),
                data['name']['title'] + ' ' + data['name']['first'] + ' ' + data['name']['last'],
                data['gender'],
                data['dob']['age'],
                data['location']['city'],
                data['location']['state'],
                data['location']['postcode'],
                data['phone'],
                con
            ))
    else:
        raise Exception("Failed fetching voter resources")

    return voters


def populate_voters():
    conn = getDbConn()
    constituencies = map(lambda x: x['id'], get_constituencies())

    for con in constituencies:
        num = random.randint(sample_data_cnf['min_voters_per_constituency'],
                             sample_data_cnf['max_voters_per_constituency'])

        voters = generate_voters(num, con)
        push_voters(conn, voters)

        conn.commit()
        print(f"generated [{num}] voters for {con}")

    conn.close()


def populate_constituencies():
    cons = get_constituencies()
    conn = getDbConn()
    for con in cons:
        cursor = conn.cursor()
        sql = f"""
               INSERT INTO {postgres_cnf['schema']}.constituencies (id, name, state)
               VALUES (%s, %s, %s)
        """
        cursor.execute(sql, (con['id'], con['name'], con['state']))
        conn.commit()


def populate_parties():
    cons = get_parties()
    conn = getDbConn()
    for con in cons:
        cursor = conn.cursor()
        sql = f"""
               INSERT INTO {postgres_cnf['schema']}.parties (id, name)
               VALUES (%s, %s)
        """
        cursor.execute(sql, (con['id'], con['name']))
        conn.commit()


def createTables():
    with open('resources/create_tables.sql', 'r') as f:
        sql = f.read().replace("{schema}", postgres_cnf['schema'])

    queries = list(filter(lambda x: (x and x.strip()), sql.split(";")))

    conn = getDbConn()
    for query in queries:
        cursor = conn.cursor()
        print(query)
        cursor.execute(query)
        conn.commit()
    conn.close()


if __name__ == '__main__':
    start_time = datetime.now().time()
    createTables()
    populate_parties()
    populate_constituencies()
    populate_candidates()
    populate_voters()
    end_time = datetime.now().time()
    print(f'Start time :{start_time} and end time: {end_time}')
