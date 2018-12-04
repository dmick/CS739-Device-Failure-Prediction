import datetime
from mongoengine import *
from cryptography.fernet import Fernet
import uuid
import json

# TODO: Change and Move to config (should never be part of git repo)
CIPHER_KEY = b'-lQIXhsB87t-6WGXte9brqGZa0WpNsYtgwbx0xEPQcU='
cipher = Fernet(CIPHER_KEY)

# Fields which are not to be returned to user
BLACKLISTED_HOST_INFO_FIELDS = ['date_created', 'deleted', 'enabled', '_id']


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


def register_host(should_return_raw=False):
    client_id = str(uuid.uuid4())
    create_time_utc = datetime.datetime.utcnow()
    payload = {
        'host_id': client_id,
        'created_at': create_time_utc.strftime('%B %d %Y - %H:%M:%S')
    }
    secret_base_text = json.dumps(payload)
    host_info = HostInfo(
        host_id=client_id,
        host_secret=cipher.encrypt(str.encode(secret_base_text)),
        date_created=create_time_utc
    )
    saved_host = host_info.save()
    if should_return_raw:
        return saved_host
    saved_host = saved_host.to_mongo().to_dict()
    saved_host['_id'] = str(saved_host['_id'])
    return saved_host
