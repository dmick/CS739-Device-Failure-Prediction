from pecan import conf  # noqa
from mongoengine import connect


def init_model():
    """
    This is a stub method which is called at application startup time.

    If you need to bind to a parsed database configuration, set up tables or
    ORM classes, or perform any database initialization, this is the
    recommended place to do it.

    For more information working with databases, and some common recipes,
    see https://pecan.readthedocs.io/en/latest/databases.html
    """
    # TODO: Read from app-config
    connect(db='global', host='devicefailureprediction_mongo_1', username='test-user',
            password='test-password', alias='global')
