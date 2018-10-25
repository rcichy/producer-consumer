import json
import os
import sys

from utils import validate_env, get_redis_conn

REQUIRED_VARS = ['REDIS_HOST', 'REDIS_PORT', 'REDIS_CHANNEL']


def get_data(filename):
    """
    Read the file under filename and return its contents as list of dicts

    :param filename: str
    :return: list of dicts
    """
    with open(filename, 'r') as f:
        items = f.readlines()
    data = [{'name': item.split(',')[0], 'email': item.split(',')[1].rstrip('\n')} for item in items]
    return data


def publish_data(redis_connection, data):
    """
    Push contents to the tasks key and notify consumers.

    :param redis_connection: StrictRedis
    :param data:
    :return:
    """
    for message in data:
        message = json.dumps(message)
        if message:
            redis_connection.lpush(os.environ.get('REDIS_TASKS_KEY', 'tasks'), message)
            redis_connection.publish(os.environ.get('REDIS_CHANNEL', 'notification-channel'), 'new-task')


def produce_loop(filename):
    """
    Read the file under filename and push its contents throught message bus to consumers.

    :param filename: str
    :return: None
    """
    validate_env(REQUIRED_VARS)
    data = get_data(filename)
    redis_connection = get_redis_conn()
    publish_data(redis_connection, data)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python producer.py /path/to/data.csv")
    else:
        filename = sys.argv[1]
        produce_loop(filename)
