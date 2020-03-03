import random
import time
import json
import db
import pubsub_factory
import settings

from datetime import datetime

publisher = pubsub_factory.create_publisher()
topic_path = publisher.topic_path(settings.PROJECT_ID, 'accounts')

futures = dict()


def run():
    simulate()


def simulate():
    print('Simulating transactions...')
    try:
        conn = db.connect()
        cursor = conn.cursor()
        cursor.execute('SELECT parties.id, balance FROM parties, accounts')
        result_list = cursor.fetchall()
        while True:
            transaction_date = datetime.now()
            random_row = random.choice(result_list)
            sql = 'UPDATE accounts SET balance=%s, updated_at=%s WHERE party_id=%s'
            random_balance_amount = random.randrange(0, int(random_row[1]))
            party_id = random_row[0]
            params = (random_balance_amount, transaction_date.__str__(), party_id)
            cursor.execute(sql, params)
            conn.commit()
            body = {'party_id': random_row[0],
                    'balance': random_balance_amount,
                    'transaction_time': transaction_date.__str__()}
            str_body = json.dumps(body)
            data = str_body.encode('utf-8')
            future = publisher.publish(topic_path, data=data)
            print(future.result())
            time.sleep(random.randrange(0, 16))
    except Exception:
        print('ERROR')
