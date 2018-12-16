import csv
import os
import pandas as pd
import math
import random


class DataSampler:
    def __init__(self, root_dir, dst_dir, dst_file, blacklist_folders=None, multiplier=4, ):
        self.multiplier = multiplier
        self.dst_file = dst_file
        if blacklist_folders is None:
            blacklist_folders = []
        self.root_dir = root_dir
        self.dst_dir = dst_dir
        self.blacklist_folders = blacklist_folders
        self.failed_rows = []
        self.success_rows = []
        random.seed(723)

    def sample_all(self):
        dirs = os.listdir(self.root_dir)
        to_process_dirs = []
        for dir in dirs:
            abs_path = os.path.join(self.root_dir, dir)
            if dir.endswith('.zip') or os.path.isfile(abs_path):
                continue
            if dir in self.blacklist_folders:
                print('Skipping Dir: ' + str(dir))
                continue
            to_process_dirs.append(abs_path)
        self.parse_all_directories(to_process_dirs)
        self.write_results()

    def parse_all_directories(self, to_process_dirs):
        for dir_path in to_process_dirs:
            print('Processing Directory: ' + str(dir_path))
            all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
            csv_files = [os.path.join(dir_path, f) for f in all_files if f.endswith('.csv')]
            csv_files.sort()
            for file_name in csv_files:
                self.parse_csv(file_name)

    def parse_csv(self, file_name):
        print('\tProcessing File: ' + str(file_name))
        data = pd.read_csv(file_name)
        data.head()
        failure_rows = []
        success_rows = []
        total_count = 0
        for index, row in data.iterrows():
            if row['failure'] == 1:
                failure_rows.append(self.get_csv_row(data, row))
            else:
                success_rows.append(self.get_csv_row(data, row))
            total_count = total_count + 1
        failure_count = len(failure_rows)
        required_success_count = int(self.multiplier * failure_count)
        if required_success_count > len(success_rows):
            required_success_count = len(success_rows)
        random.shuffle(success_rows)
        self.success_rows.extend(success_rows[1:required_success_count])
        self.failed_rows.extend(failure_rows)

    def get_csv_row(self, data, row):
        base_row = {
            "date": row["date"],
            "serial_number": row["serial_number"],
            "model": row["model"],
            "capacity_bytes": row["capacity_bytes"],
            "failure": row["failure"],
        }
        for i in range(1, 256):
            col_name = 'smart_{0}_normalized'.format(str(i))
            if col_name in data.columns and not math.isnan(row[col_name]):
                base_row[col_name] = row[col_name]
            col_name = 'smart_{0}_raw'.format(str(i))
            if col_name in data.columns and not math.isnan(row[col_name]):
                base_row[col_name] = row[col_name]
        return base_row

    def write_results(self):
        with open(os.path.join(self.dst_dir, self.dst_file), 'w') as csvfile:
            fieldnames = ['date', 'serial_number', 'failure', 'model', 'capacity_bytes', 'failure']
            for i in range(1, 256):
                fieldnames.append('smart_{0}_normalized'.format(str(i)))
                fieldnames.append('smart_{0}_raw'.format(str(i)))
            fin_list = self.failed_rows + self.success_rows
            random.shuffle(fin_list)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(fin_list)


if __name__ == '__main__':
    sampler = DataSampler(
        '/home/pkapoor/CS739-Device-Failure-Prediction/data/backblaze_processed',
        '/home/pkapoor/CS739-Device-Failure-Prediction/data/backblaze_ml',
        'train.csv',
        blacklist_folders=['data_Q1_2018', 'data_Q2_2018', 'data_Q3_2018', 'data_Q1_2017', 'data_Q2_2017',
                           'data_Q3_2017', 'data_Q4_2017', 'data_Q1_2016', 'data_Q2_2016', 'data_Q3_2016',
                           'data_Q4_2016']
    )
    sampler.sample_all()
