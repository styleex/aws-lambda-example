import os
import traceback

import pika
import psycopg2

from pika.adapters.blocking_connection import BlockingChannel
from cerberus import validator
from typing import List
import json
import logging

log = logging.getLogger()
logging.basicConfig(level=logging.WARNING)


def provide_amqp(amqp_url) -> BlockingChannel:
    if not hasattr(provide_amqp, '_conn'):
        provide_amqp._conn = pika.BlockingConnection(pika.URLParameters(amqp_url))
        provide_amqp._channel = provide_amqp._conn.channel()

    return provide_amqp._channel


def provide_db(db_dsn: str):
    if not hasattr(provide_db, '_conn'):
        provide_db._conn = psycopg2.connect(db_dsn)

        table_sql = """
        create table if not exists events (
            ts timestamp, 
            payload TEXT,
            queue TEXT,
            success boolean,
            error TEXT
        )
        """
        with provide_db._conn:
            with provide_db._conn.cursor() as cur:
                cur.execute(table_sql)

    return provide_db._conn


def send_messages_handler(channel: BlockingChannel, db_conn, request):
    schema = {
        'payload': {'type': 'string', 'required': True},
        'routing_keys': {'type': 'list', 'required': True, 'schema': {
            'type': 'string'
        }}
    }

    v = validator.Validator(schema, allow_unknown=True)
    if not v.validate(request):
        raise ValueError('Invalid request: {}'.format(v.errors))

    payload = request['payload']  # type: str
    routing_keys = request['routing_keys']  # type: List[str]

    success_count = 0
    failed_count = 0
    insert_data = []
    for rk in routing_keys:
        try:
            channel.basic_publish('', rk, payload.encode('utf-8'))
            insert_data.append((payload, rk, True, None))
            success_count += 1
        except:
            log.exception('Failed to publish msg to queue %s', rk)
            insert_data.append((payload, rk, False, traceback.format_exc()))
            failed_count += 1

    try:
        with db_conn:
            with db_conn.cursor() as cur:
                cur.executemany('insert into events(ts, payload, queue, success, error) '
                                'values (now(), %s, %s, %s, %s)', insert_data)
    except:
        log.exception('Failed to commit publish log to postgresql. Ignored.')

    return {
        'success_count': success_count,
        'failed_count': failed_count,
    }


def main(event, context):
    amqp_url = os.environ.get('AMQP_URL', None)
    if not amqp_url:
        raise RuntimeError('AMQP_URL must be set')

    db_url = os.environ.get('POSTGRESQL_DB_URL', None)
    if not db_url:
        raise RuntimeError('POSTGRESQL_DB_URL must be set')

    amqp_channel = provide_amqp(amqp_url)
    db_conn = provide_db(db_url)

    request = json.loads(event['body'])

    try:
        data = send_messages_handler(amqp_channel, db_conn, request)
    except Exception as escp:
        log.exception('Failed to process request: %s', request)

        return {
            'status_code': 200,
            'body': json.dumps({
                'success': False,
                'error': '{}: {}'.format(str(escp.__class__.__name__), str(escp)),
                'traceback': traceback.format_exc(),
            })
        }

    response = {
        'success': True,
    }
    if data:
        response['result'] = data

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }


def auth(event, context):
    ALLOWED_TOKENS = ['teststatictoken']

    auth_token = event.get('authorizationToken', '')
    method_arn = event.get('methodArn', '')

    return {
        'principalId': 'test',
        'policyDocument': {
            'Version': '2012-10-17',
            'Statement': [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": "Allow" if auth_token in ALLOWED_TOKENS else "Deny",
                    "Resource": method_arn
                }
            ]
        },
        # 'context': context,
    }


if __name__ == "__main__":
    for i in range(2):
        ret = main({
            'body': json.dumps({
                "payload": "any_string",
                "routing_keys": [
                    "key1",
                    "key2"
                ]
            })
        }, '')
        print(json.dumps(ret))
