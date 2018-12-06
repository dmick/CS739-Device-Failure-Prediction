import pecan
import json
import cs739devicefailureprediction.model.HostInfo as host_info_service
from cs739devicefailureprediction.model import es

status_map = {
    500: 'Internal Server Error!',
    404: 'Page Not Found!',
    401: 'Unauthorized!',
}
DEVICE_STORE_FIELDS_TO_SEND = ['_id', '_version', 'result']


class RootController(object):
    @pecan.expose(method='POST', template='json', route='register-host')
    def register_host_info(self):
        saved_host_info = host_info_service.register_host()
        for field in host_info_service.BLACKLISTED_HOST_INFO_FIELDS:
            saved_host_info.pop(field, None)
        return saved_host_info

    @pecan.expose(method='POST', template='json', route='store-device-metrics')
    def store_metrics(self):
        body = pecan.request.body
        host_id = pecan.request.headers.get('host_id')
        host_secret = pecan.request.headers.get('host_secret')
        if host_id is None or host_secret is None:
            pecan.abort(401)
        host = host_info_service.fetch_host_by_secret(str.encode(host_secret))
        if host is None or host['host_id'] != host_id:
            pecan.abort(401)
        body = json.loads(body.decode())
        response = {}
        es_response = es.index(index="test-index", doc_type='test-doc', body=body)
        for field in DEVICE_STORE_FIELDS_TO_SEND:
            response[field] = es_response[field]
        return response

    @pecan.expose('error.html')
    def error(self, status):
        try:
            status = int(status)
        except ValueError:  # pragma: no cover
            status = 500
        message = getattr(status_map.get(status), 'explanation', '')
        return dict(status=status, message=message)
