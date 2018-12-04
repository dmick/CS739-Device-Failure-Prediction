from pecan import expose, redirect
from cs739devicefailureprediction.model.HostInfo import register_host, BLACKLISTED_HOST_INFO_FIELDS

status_map = {
    500: 'Internal Server Error!',
    404: 'Page Not Found!',
    401: 'Unauthorized!',
}


class RootController(object):
    @expose(method='POST', template='json', route='register-host')
    def register_host_info(self):
        saved_host_info = register_host()
        for field in BLACKLISTED_HOST_INFO_FIELDS:
            saved_host_info.pop(field, None)
        return saved_host_info

    @expose(method='POST', template='json', route='store-device-metrics')
    def store_metrics(self):
        return {'msg': 'Metrics stored Successfully!'}

    @expose('error.html')
    def error(self, status):
        try:
            status = int(status)
        except ValueError:  # pragma: no cover
            status = 500
        message = getattr(status_map.get(status), 'explanation', '')
        return dict(status=status, message=message)
