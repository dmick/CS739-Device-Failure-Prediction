import json
import requests
import pprint
import subprocess

START_TEST_TS = 1544400000001


def get_smartctl_data(disk):
    result = subprocess.run(['/usr/local/sbin/smartctl', '-x', '--json', disk], stdout=subprocess.PIPE)
    return json.loads(result.stdout)


def store_smartctl_metric(base_url, host_id, host_secret, disk):
    headers = {
        'Content-type': 'application/json',
        'host_id': host_id,
        'host_secret': host_secret
    }
    smartctl_json_map = get_smartctl_data(disk)
    payload = {}
    payload['smartctl_json'] = smartctl_json_map
    payload['hints'] = {}
    payload['hints']['other_hosts'] = [host_id]
    payload['hints']['is_backblaze'] = True
    payload['hints']['backblaze_ts'] = START_TEST_TS
    payload['hints']['backblaze_failure_label'] = True

    print('Sending Data: ')
    response = requests.post(base_url + 'store-device-metrics', data=json.dumps(payload), headers=headers)
    print('Response from server is: ')
    pprint.pprint(response.json())


def post_metrics():
    base_url = 'http://localhost:8080/'
    host_id = '5c6ec02f-4db2-4136-ab78-1cb86c68bde0'
    host_secret = 'gAAAAABcByEyUpXumdwtAxl5QyTPFTLTKN0nBcS9Z_ETb3Z9mAnvr46C-w-gXvkwtiVXyPooSr4fMqQqA-IQjFyTfg3LKQwP4qLvgKWMtH1bt8LV74EiEgOIrsaaXGrWBPF0-xAi0fmuoMz-lvPMVdw8rnHNGw9lewpFdxfl_aOemoplpKMowkfnwg5t6RWhyt0Mtmjspd5qZOrwYUHcXsQNcTWEe-B2mg=='
    disk = 'disk0s2'
    store_smartctl_metric(base_url, host_id, host_secret, disk)


def register_host():
    base_url = 'http://localhost:8080/'
    response = requests.post(base_url + 'register-host')
    print('Response from server is: ')
    pprint.pprint(response.json())


if __name__ == '__main__':
    register_host()
