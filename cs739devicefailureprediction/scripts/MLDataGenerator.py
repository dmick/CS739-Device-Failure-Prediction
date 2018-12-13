import elasticsearch
from elasticsearch import helpers
import csv
import os

BACKBLAZE_MATCH_CLAUSE = {"match": {"hints.is_backblaze": "true"}}


def get_es_search_query(must_clause):
    es_search_query = {
        "query": {
            "bool": {
                "must": must_clause
            }
        }
    }
    return es_search_query


def get_date_range_clause(date_field, start_time, end_time):
    return {
        "range": {
            date_field: {
                "gte": start_time,
                "lte": end_time
            }
        }
    }


def get_datefield(is_backblaze):
    date_field = 'server_ts'
    if is_backblaze:
        date_field = 'hints.backblaze_ts'
    return date_field


def get_filters(is_backblaze, start_time, end_time):
    date_field = get_datefield(is_backblaze)
    date_range_clause = get_date_range_clause(date_field, start_time, end_time)
    must_clause = []
    if is_backblaze:
        must_clause.append(BACKBLAZE_MATCH_CLAUSE)
    must_clause.append(date_range_clause)
    return must_clause


def get_bool_to_int(bool_flag):
    if bool_flag is None:
        return 0
    if bool_flag:
        return 1
    return 0


class MLDataGenerator:
    def __init__(self, es_options=None, from_index='test-index-formatted', from_type='test-doc'):
        if es_options is None:
            es_options = [dict(host='localhost', port=9200)]
        self.from_index = from_index
        self.from_type = from_type
        self.es = elasticsearch.Elasticsearch(es_options)

    def gen_data(self, from_ts, upto_ts, is_backblaze=True):
        must_clause = get_filters(is_backblaze, from_ts, upto_ts)
        es_search_query = get_es_search_query(must_clause)
        es_response = helpers.scan(client=self.es, scroll='2m', query=es_search_query,
                                   index=self.from_index,
                                   doc_type=self.from_type)
        self.gen_csv(is_backblaze, from_ts, upto_ts, es_response)

    def gen_csv(self, is_backblaze, from_ts, upto_ts, es_response):
        cwd = os.getcwd()
        with open(os.path.join(cwd, 'data', 'tmp', str(from_ts) + '_' + str(upto_ts) + '.csv'), 'w') as csvfile:
            fieldnames = ['date', 'serial_number', 'model', 'capacity_bytes', 'failure_backblaze', 'failure_assumption']
            for i in range(1, 256):
                fieldnames.append('smart_{0}_normalized'.format(str(i)))
                fieldnames.append('smart_{0}_raw'.format(str(i)))
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for hit in es_response:
                writer.writerow(self.get_csv_row(is_backblaze, hit))

    def get_csv_row(self, is_backblaze, hit):
        source = hit['_source']
        hints = source['hints']
        smartctl_json = source['smartctl_json']
        processed_info = source['processed_info']
        if is_backblaze:
            date = source['server_ts']
        else:
            date = hints['backblaze_ts']
        row = {
            'date': date,
            'serial_number': smartctl_json['serial_number'],
            'model': smartctl_json['model_family'],
            'capacity_bytes': smartctl_json['user_capacity']['bytes'],
            'failure_backblaze': get_bool_to_int(hints['backblaze_failure_label']),
            'failure_assumption': get_bool_to_int(hints.get('fail_assumption', False))
        }
        smart_values = processed_info['ata_smart_attributes_value']
        smart_raw_values = processed_info['ata_smart_attributes_raw_value']
        for smart_id, value in smart_values.iteritems():
            row['smart_{0}_normalized'.format(smart_id)] = value
        for smart_id, value in smart_raw_values.iteritems():
            row['smart_{0}_raw'.format(smart_id)] = value
        return row


if __name__ == '__main__':
    generator = MLDataGenerator()
    generator.gen_data(1544400000000, 1544918400000)
