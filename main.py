import os
import traceback
import random
import string
from random import randint
import json

import MySQLdb
import pika

QUEUE_NAME = 'datagen'
if os.getenv('DG_APP_NAME', None):
    QUEUE_NAME += '.' + os.getenv('DG_APP_NAME')

def connect_db():
    db_name = os.getenv('DB_NAME', 'sample-app')
    user = os.getenv('MYSQL_ROOT_USER', 'root')
    passwd = os.getenv('MYSQL_ROOT_PASSWORD', 'admin')
    host = os.getenv('MYSQL_TIER', 'db')

    conn = MySQLdb.connect(host, user, passwd, db_name)

    return conn


def init_tables():
    conn = connect_db()
    cur = conn.cursor()
    query = """ CREATE TABLE IF NOT EXISTS samples (
                    id tinyint(4) NOT NULL AUTO_INCREMENT,
                    value int(11) DEFAULT '0',
                    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    PRIMARY KEY (id)
                ) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=50 """
    cur.execute(query)
    conn.close()


def get_records(query):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    return rows


def insert_records(rows):
    conn = connect_db()
    cur = conn.cursor()
    for i in range(rows):
        cur.execute("INSERT into samples (value) values ({0})".format(str(randint(1, 10000))))
    conn.commit()
    cur.close()


def process_msg(ch, method, properties, body):
    try:
        data = json.loads(body)
        print "Received data " + body

        if data['action'] == 'insert':
            insert_records(data['records'])
            rec = 1
        elif data['action'] == 'get':
            rec = get_records('SELECT id, value, CAST(created_at as CHAR) from samples')

        ch.basic_publish(exchange='',
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=json.dumps(rec))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except:
        print "Failed to process message"
        traceback.print_exc()


def mq_listen():
    rmq_host = os.getenv('RMQ_TIER', 'rmq')
    connection = pika.BlockingConnection(pika.ConnectionParameters(rmq_host))

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(process_msg, queue=QUEUE_NAME)
    print(' [*] Waiting for messages.')
    channel.start_consuming()


if __name__ == '__main__':
    init_tables()
    mq_listen()
