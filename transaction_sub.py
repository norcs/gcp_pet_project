import base64
import json
import time

import db
import pubsub_factory
import settings

from google.cloud import bigquery
from google.cloud import pubsub
from google.oauth2 import service_account


key_path = settings.KEY_PATH

credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)

dataset_id = 'transactions'


def run():
    receive_transactions()


def write_transaction_to_bq(party_id, balance, tx_timestamp):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table('fact_transactions')
    table = client.get_table(table_ref)

    transaction = [
        (party_id, balance, tx_timestamp)
    ]

    errors = client.insert_rows(table, transaction)
    if not errors:
        print('Inserted succesfully')
    else:
        print('Errors:')
        for error in errors:
            print(error)


def collect_transaction(message):
    message_str = message.data.decode('UTF-8')
    json_obj = json.loads(message_str)
    party_id = json_obj.get('party_id')
    balance = json_obj.get('balance')
    tx_timestamp = json_obj.get('transaction_time')

    write_transaction_to_bq(party_id, balance, tx_timestamp)


def receive_transactions():
    subscriber = pubsub_factory.create_subscriber()
    subscription_path = subscriber.subscription_path(
        settings.PROJECT_ID, 'my-sub')

    def callback(message):
        collect_transaction(message)
        message.ack()

    future = subscriber.subscribe(subscription_path, callback=callback)
    print('Listening for messages on {}'.format(subscription_path))

    try:
        future.result()
    except Exception as e:
        print(
            'Listening for messages on {} threw an Exception: {}'.format(
                'my-sub', e))
        raise

    while True:
        time.sleep(60)


