import datetime

from mongoengine import *

from schemas.User import User


class HostInfo(Document):
    date_created = DateTimeField(required=True)
    deleted = BooleanField(required=True, default=False)
    enabled = BooleanField(required=True, default=True)
    host_id = StringField(required=True, unique=True)
    host_secret = StringField(required=True)
    meta = {
        'indexes': [
            'host_id'
        ],
        'collection': 'hostInfo',
        'db_alias': 'global',
    }
