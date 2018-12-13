import requests
import json
from datetime import datetime
import math
import pandas as pd
import os
import pprint

EPOCH = datetime(1970, 1, 1, 0, 0, 0)

json_skeleton = {
    "model_family": "Apple SD/SM/TS...E/F/G SSDs",
    "model_name": "APPLE SSD SM0256G",
    "vendor": "BackBlaze",
    "serial_number": "S29CNYBH505806",
    "user_capacity": {
        "bytes": 251000193024
    },
    "ata_smart_attributes": {
        "revision": 1,
        "table": [
        ]
    },
}


class BackBlazeParser:
    def __init__(self, base_url=None, host_id=None, host_secret=None):
        self.base_url = base_url
        self.host_id = host_id
        self.host_secret = host_secret
        if self.base_url is None:
            self.base_url = 'http://localhost:8080/'
        if self.host_id is None:
            self.host_id = 'b0c5b707-0442-4c5b-a225-79cda7c04db1'
        if self.host_secret is None:
            self.host_secret = 'gAAAAABcEuDPCe_jptctme269UF3OCySOWrhbcI4d5IdG-uYpZIiLOCb5QKQV2hlpiAG7rK_ll0fPiucDUqMkrW1Z7UUfrIHe-KqCGUy2CMHPxM2QV1gFNMZE4AoPNEH6UgdphjBeO5tJZQ_oewvU_e74WyzwsUblnIDT3jiw-C7ipSnK3W-H8MXEzx-sESlQjjsdHotw54PJ_uDsVjYurvaBUktDS9Udg=='
        self.headers = {
            'Content-type': 'application/json',
            'host_id': self.host_id,
            'host_secret': self.host_secret
        }

    def store_smartctl_metric(self, payload):
        requests.post(self.base_url + 'store-device-metrics', data=json.dumps(payload), headers=self.headers)

    def parse_root_dir(self, root_dir):
        dirs = os.listdir(root_dir)
        to_process_dirs = []
        for dir in dirs:
            abs_path = os.path.join(root_dir, dir)
            if dir.endswith('.zip') or os.path.isfile(abs_path):
                continue
            to_process_dirs.append(abs_path)
        self.parse_all_directories(to_process_dirs)

    def parse_all_directories(self, dirs):
        for dir_path in dirs:
            print('Processing Directory: ' + str(dir_path))
            all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
            csv_files = [os.path.join(dir_path, f) for f in all_files if f.endswith('.csv')]
            for file_name in csv_files:
                self.parse_csv(file_name)

    def parse_csv(self, file_name):
        print('\tProcessing File: ' + str(file_name))
        data = pd.read_csv(file_name)
        data.head()
        for index, row in data.iterrows():
            json1 = {}
            json1["model_name"] = row["model"]
            json1["vendor"] = "BackBlaze"
            json1["serial_number"] = row["serial_number"]
            json1["model_family"] = row["model"]
            json1["user_capacity"] = {}
            json1["user_capacity"]["bytes"] = row["capacity_bytes"]
            json1["ata_smart_attributes"] = {}
            json1["ata_smart_attributes"]["table"] = []
            for i in range(1, 256):
                attr = {}
                sraw = "smart_{0}_raw".format(str(i))
                snorm = "smart_{0}_normalized".format(str(i))
                if sraw in data.columns and (not (math.isnan(row[snorm]) and math.isnan(row[sraw]))):
                    attr["id"] = i
                    if not math.isnan(row[snorm]):
                        attr["value"] = row[snorm]
                    else:
                        if not math.isnan(row[sraw]):
                            attr["raw"] = {}
                            attr["raw"]["value"] = row[sraw]
                            attr["raw"]["string"] = row[sraw]
                    json1["ata_smart_attributes"]["table"].append(attr)
            payload = {}
            payload['smartctl_json'] = json1
            payload['hints'] = {}
            payload['hints']['is_backblaze'] = True
            dt = row["date"]
            d = datetime(int(dt[:4]), int(dt[5:7]), int(dt[8:11]))
            seconds_since_epoch = (d - EPOCH).total_seconds()
            payload['hints']['backblaze_ts'] = seconds_since_epoch * 1000
            failure_flag = row["failure"]
            # print(type(failure_flag))
            if failure_flag == 1:
                payload['hints']['backblaze_failure_label'] = True
            else:
                payload['hints']['backblaze_failure_label'] = False
            self.store_smartctl_metric(payload)
            # pprint.pprint(payload)


if __name__ == '__main__':
    parser = BackBlazeParser()
    parser.parse_root_dir('/home/pkapoor/CS739-Device-Failure-Prediction/data/backblaze')
