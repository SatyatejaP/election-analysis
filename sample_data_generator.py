import random
import string

import requests
import simplejson as json
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


def generate_candidate(num: int, party_id: string, constituency_id: string):
    response = requests.get(random_user_url + '&gender=' + ('male' if num % 2 == 0 else 'female'))

    if response.status_code == 200:
        data = response.json()['results'][0]
        return {
            'id': data['id']['value'],
            'name': f"{data['name']['title']} {data['name']['first']} {data['name']['last']}",
            'gender': data['gender'],
            'age': data['dob']['age'],
            'photo_url': data['picture']['large'],
            'party_id': party_id,
            'constituency_id': constituency_id,
        }
    else:
        raise Exception("Failed fetching candidate resources")


def push_candidate(conn, cand):
    cursor = conn.cursor()
    sql = f"""
                            INSERT INTO {postgres_cnf['schema']}.candidates (id, name, gender, age, photo_url, party_id, constituency_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
    cursor.execute(sql, (cand['id'], cand['name'], cand['gender'], cand['age'], cand['photo_url'], cand['party_id'],
                         cand['constituency_id']))
    conn.commit()


def populate_candidates():
    cons = list(map(lambda x: x['id'], get_constituencies()))
    pars: list = list(map(lambda x: x['id'], get_parties()))
    filtered_parties = list(filter(lambda x: x not in ['INC', 'BJP'], pars))

    conn = getDbConn()

    for con in cons:
        num = random.randint(sample_data_cnf['min_candidates_per_constituency'],
                             sample_data_cnf['max_candidates_per_constituency'])
        if num % 2 == 0:
            num += 1
        print(f"generating [{num}] candidates for constituency: {con}")
        isBJPContesting = random.randint(0, 10) % 2 == 0
        isINCContesting = random.randint(0, 10) % 2 == 0

        if isBJPContesting:
            num -= 1
            cand = generate_candidate(num, 'BJP', con)
            push_candidate(conn, cand)

        if isINCContesting:
            num -= 1
            cand = generate_candidate(num, 'INC', con)
            push_candidate(conn, cand)

        for i in range(num):
            party_id = random.choice(filtered_parties)
            cand = generate_candidate(i, party_id, con)
            push_candidate(conn, cand)

    conn.commit()
    conn.close()


def push_voter(conn, voter):
    cursor = conn.cursor()
    sql = f"""
                            INSERT INTO {postgres_cnf['schema']}.voters (id, name, gender, age, city, state, pincode, phone_number, constituency_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
    cursor.execute(sql, (
        voter['id'], voter['name'], voter['gender'], voter['age'], voter['city'], voter['state'], voter['pincode'],
        voter['phone_number'], voter['constituency_id']))


def generate_voter(num: int, con: string):
    response = requests.get(random_user_url + '&gender=' + ('male' if num % 2 == 0 else 'female'))
    if response.status_code == 200:
        data = response.json()['results'][0]
        return {
            'id': data['id']['value'],
            'name': data['name']['title'] + ' ' + data['name']['first'] + ' ' + data['name']['last'],
            'gender': data['gender'],
            'age': data['dob']['age'],
            'city': data['location']['city'],
            'state': data['location']['state'],
            'pincode': data['location']['postcode'],
            'phone_number': data['phone'],
            'constituency_id': con,
        }
    else:
        raise Exception("Failed fetching voter resources")


def populate_voters():
    conn = getDbConn()
    constituencies = map(lambda x: x['id'], get_constituencies())

    for con in constituencies:
        num = random.randint(sample_data_cnf['min_voters_per_constituency'],
                             sample_data_cnf['max_voters_per_constituency'])
        print(f"populating [{num}] voters for {con}")

        for i in range(num):
            voter = generate_voter(i, con)
            push_voter(conn, voter)
        conn.commit()
        print(f"populated [{num}] voters for {con}")

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
    createTables()
    populate_parties()
    populate_constituencies()
    populate_candidates()
    populate_voters()
