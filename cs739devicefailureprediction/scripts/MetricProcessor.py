import elasticsearch
from elasticsearch import helpers
import copy

BACKBLAZE_MATCH_CLAUSE = {"match": {"hints.is_backblaze": "true"}}

MILLIS_IN_DAY = 24 * 60 * 60 * 1000


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


class MetricProcessor:
    def __init__(self, es_options=None,
                 from_index='test-index',
                 from_type='test-doc',
                 to_index='test-index-formatted',
                 ):
        if es_options is None:
            es_options = [dict(host='localhost', port=9200)]
        self.from_index = from_index
        self.from_type = from_type
        self.to_index = to_index
        self.to_type = from_type
        self.es = elasticsearch.Elasticsearch(es_options)

    def process(self, from_time, upto_time, is_backblaze=True):
        start_time = from_time
        counter = 1
        while start_time < upto_time:
            print("Counter: " + str(counter))
            end_time = start_time + MILLIS_IN_DAY - 1
            print('Processing Start Date: ' + str(start_time) + ' and End Date: ' + str(end_time))
            if end_time > upto_time:
                print('Yes')
                end_time = upto_time
            must_clause = get_filters(is_backblaze, start_time, end_time)
            es_search_query = get_es_search_query(must_clause)
            from_index_curr_day_es_response = helpers.scan(client=self.es, scroll='2m', query=es_search_query,
                                                           index=self.from_index,
                                                           doc_type=self.from_type)
            # from_index_curr_day_es_response = self.es.search(index=self.from_index,
            #                                                  doc_type=self.from_type,
            #                                                  body=es_search_query, scroll='1m')
            # print("Got %d Hits for from:" % from_index_curr_day_es_response['hits']['total'])
            self.process_internal(is_backblaze, start_time, from_index_curr_day_es_response)
            start_time = end_time + 1
            counter = counter + 1

    def process_internal(self, is_backblaze, start_time, from_index_curr_day_es_response):
        # Fetch Previous day data to update label
        end_time = start_time - 1
        start_time = start_time - MILLIS_IN_DAY
        must_clause = get_filters(is_backblaze, start_time, end_time)
        es_search_query = get_es_search_query(must_clause)
        to_index_prev_day_es_response = None
        try:
            to_index_prev_day_es_response = helpers.scan(client=self.es, scroll='2m', query=es_search_query,
                                                         index=self.to_index,
                                                         doc_type=self.to_type)
            # to_index_prev_day_es_response = self.es.search(index=self.to_index, doc_type=self.to_type,
            #                                                body=es_search_query)
            # print("Got %d Hits for to:" % to_index_prev_day_es_response['hits']['total'])
        except elasticsearch.exceptions.NotFoundError as e:
            print('To Index Not Found which was: ' + self.to_index)

        self.do_process_internal(from_index_curr_day_es_response, to_index_prev_day_es_response)

    def do_process_internal(self, from_index_curr_day_es_response, prev_day_dev_id_to_responses):
        curr_day_dev_id_to_responses = self.create_dev_id_to_responses(from_index_curr_day_es_response)
        prev_day_dev_id_to_responses = self.create_dev_id_to_responses(prev_day_dev_id_to_responses)
        self.update_failed_assumption_hint(curr_day_dev_id_to_responses,
                                           prev_day_dev_id_to_responses)
        print('Storing processed attributed to to_index')
        # Store current days records to processed index
        for key, value in curr_day_dev_id_to_responses.iteritems():
            print("Storing it for day: " + str(key))
            for hit in value:
                source = hit.get('_source', None)
                if source is None:
                    continue
                destination = copy.deepcopy(source)
                dst_smartctl = destination.get('smartctl_json', None)
                if dst_smartctl is None:
                    continue
                smart_attributes = dst_smartctl.get('ata_smart_attributes', None)
                if smart_attributes is None:
                    continue
                ata_smart_attributes_value = {}
                ata_smart_attributes_raw_value = {}
                ata_smart_attributes_raw_string_value = {}
                smart_attribute_tables = smart_attributes.get('table', [])
                for attr in smart_attribute_tables:
                    id = attr.get('id', None)
                    if id is None:
                        continue
                    value = attr.get('value', None)
                    raw_value_map = attr.get('raw', {})
                    raw_value = raw_value_map.get('value', None)
                    raw_string_value = raw_value_map.get('string', None)
                    if value is not None:
                        ata_smart_attributes_value[id] = value
                    if raw_value is not None:
                        ata_smart_attributes_raw_value[id] = raw_value
                    if raw_string_value is not None:
                        ata_smart_attributes_raw_string_value[id] = raw_value
                dst_processed_info = {
                    'ata_smart_attributes_value': ata_smart_attributes_value,
                    'ata_smart_attributes_raw_value': ata_smart_attributes_raw_value,
                    'ata_smart_attributes_raw_string_value': ata_smart_attributes_raw_string_value,
                    'warehouse_doc_id': hit['_id']
                }
                destination['processed_info'] = dst_processed_info
                es_response = self.es.index(index=self.to_index, doc_type=self.to_type, body=destination)

    def update_failed_assumption_hint(self, curr_day_dev_id_to_responses, prev_day_dev_id_to_responses):
        # Update previous days mapping based on if the device metric was pushed
        print('Updating fail assumption flag')
        for key, value in prev_day_dev_id_to_responses.iteritems():
            curr_day_responses = curr_day_dev_id_to_responses.get(key, None)
            device_failed_assumption = False
            if curr_day_responses is None or len(curr_day_responses) == 0:
                device_failed_assumption = True
            for hit in value:
                self.es.update(index=self.to_index, doc_type=self.to_type, id=hit['_id'], body={
                    "doc": {
                        "hints": {
                            "fail_assumption": device_failed_assumption,
                        }
                    }
                })
        print('Updating Fail Assumption updated')

    def create_dev_id_to_responses(self, es_response):
        dev_id_to_responses = {}
        if es_response is None:
            return dev_id_to_responses
        try:
            for hit in es_response:
                dev_id = self.get_device_id(hit)
                if dev_id is None:
                    continue
                dev_responses = dev_id_to_responses.get(dev_id, [])
                dev_responses.append(hit)
                dev_id_to_responses[dev_id] = dev_responses
        except elasticsearch.exceptions.NotFoundError as e:
            print('To Index Not Found which was: ' + self.to_index)
        return dev_id_to_responses

    def get_device_id(self, hit):
        source = hit.get('_source', None)
        if source is None:
            return None
        smartctl_json = source.get('smartctl_json', {})
        vendor = smartctl_json.get('vendor', '')
        model = smartctl_json.get('model_name', '')
        serial = smartctl_json.get('serial_number', '')
        dev_id = vendor + '_' + model + '_' + serial
        return dev_id


if __name__ == '__main__':
    processor = MetricProcessor()
    processor.process(1544400000000, 1544918400000)
