from pecan import conf  # noqa
from mongoengine import connect as mongo_connect
from elasticsearch import Elasticsearch
from cryptography.fernet import Fernet

es = None
cipher = None


def init_model(config):
    """
    This is a stub method which is called at application startup time.

    If you need to bind to a parsed database configuration, set up tables or
    ORM classes, or perform any database initialization, this is the
    recommended place to do it.

    For more information working with databases, and some common recipes,
    see https://pecan.readthedocs.io/en/latest/databases.html
    :param config:
    """
    custom_config = getattr(config, 'custom_config', {})
    mongo_config = getattr(custom_config, 'mongo_config', {})
    es_config = getattr(custom_config, 'es_config', {})
    cipher_secret = getattr(custom_config, 'cipher_secret', {})
    mongo_connect(host=mongo_config['host'], port=mongo_config['port'],
                  username=mongo_config['global_db_username'], password=mongo_config['global_db_password'],
                  db='global', alias='global')
    es_options = []
    for option in es_config:
        es_opt = {'host': option[1]['host'], 'port': option[1]['port']}
        es_options.append(es_opt)
    global es, cipher
    es = Elasticsearch(es_options)
    cipher = Fernet(cipher_secret)
