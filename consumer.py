import json
import os

from utils import validate_env, get_redis_conn, connect_database

REQUIRED_VARS = ['REDIS_HOST', 'REDIS_PORT', 'DATABASE_URI', 'REDIS_PROCESSING_KEY']


def subscribe(redis_connection):
    """
    Subscript to the notification channel.

    :param redis_connection: StrictRedis
    :return:
    """
    sub = redis_connection.pubsub()
    sub.subscribe(os.environ.get('REDIS_CHANNEL', 'notification-channel'))
    return sub


def send_to_database(table, name, email):
    """
    Save row to database.

    :param table: str
    :param name: str
    :param email: str
    :return: None
    """
    sql = f"""
            INSERT INTO {table} VALUES (%(name)s, %(email)s);
        """
    cursor = connect_database()
    cursor.execute(sql, {'name': name, 'email': email})


def process_task(data):
    """
    Load data from json, save it the DB and remove from the processing list.

    :param data: str
    :return:
    """
    if not data:
        return
    data = json.loads(data)
    name = data['name']
    email = data['email']
    send_to_database('test_table', name, email)
    get_redis_conn().lpop(os.environ['REDIS_PROCESSING_KEY'])


def process_new_task(redis_connection):
    """
    Get a new task from the tasks list, push it to your processing list and process it.

    :param redis_connection: StrictRedis
    :return: None
    """
    data = redis_connection.rpoplpush(
        os.environ.get('REDIS_TASKS_KEY', 'tasks'),
        os.environ['REDIS_PROCESSING_KEY']
    )
    process_task(data)


def consume_loop():
    """
    Process old tasks and listen for new tasks to process.

    :return: None
    """
    validate_env(REQUIRED_VARS)
    redis_connection = get_redis_conn()

    while True:
        # process old tasks
        data = redis_connection.lpop(os.environ['REDIS_PROCESSING_KEY'])
        if not data:
            break
        process_task(data)

    sub = subscribe(redis_connection)
    while True:
        # process new tasks
        for message in sub.listen():
            if message['type'] == 'message' and message['data'] == b'new-task':
                process_new_task(redis_connection)


if __name__ == '__main__':
    consume_loop()
