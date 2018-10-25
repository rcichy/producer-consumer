import os

import psycopg2
from psycopg2.extras import DictCursor
from redis import StrictRedis

_redis_conn = None
_cursor = None


def validate_env(required_vars):
    """
    Check required enviroment variables.

    :param required_vars: list of variable names
    :return: None
    """
    for env_var in required_vars:
        if os.environ.get(env_var, None) is None:
            raise Exception(f"Invalid configuration: no env variable {env_var}")


def get_redis_conn():
    """
    Get redis client.

    :return: StrictRedis
    """
    global _redis_conn
    if not _redis_conn:
        _redis_conn = StrictRedis(os.environ['REDIS_HOST'], os.environ['REDIS_PORT'])
    return _redis_conn


def connect_database():
    """
    Get postgres cursor.

    :return: cursor
    """
    global _cursor
    if not _cursor:
        connection = psycopg2.connect(os.environ['DATABASE_URI'])
        connection.autocommit = True
        _cursor = connection.cursor(cursor_factory=DictCursor)
    return _cursor
