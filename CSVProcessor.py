import csv
import os
import pandas as pd
import math


class CSVProcessor:
    def __init__(self, root_dir, dst_dir, blacklist_folders=None):
        if blacklist_folders is None:
            blacklist_folders = []
        self.root_dir = root_dir
        self.dst_dir = dst_dir
        self.blacklist_folders = blacklist_folders

    def parse_all(self):
        dirs = os.listdir(self.root_dir)
        to_process_dirs = []
        for dir in dirs:
            abs_path = os.path.join(self.root_dir, dir)
            if dir.endswith('.zip') or os.path.isfile(abs_path):
                continue
            if dir in self.blacklist_folders:
                continue
            to_process_dirs.append(abs_path)
        self.parse_all_directories(to_process_dirs)

    def parse_all_directories(self, to_process_dirs):
        for dir_path in to_process_dirs:
            print('Processing Directory: ' + str(dir_path))
            all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
            csv_files = [os.path.join(dir_path, f) for f in all_files if f.endswith('.csv')]
            for file_name in csv_files:
                self.parse_csv(file_name)

    def parse_csv(self, file_name):
        print('\tProcessing File: ' + str(file_name))
        with open(os.path.join(self.dst_dir, file_name), 'w') as csvfile:
            final_rows = []
            fieldnames = ['date', 'serial_number', 'model', 'capacity_bytes', 'failure_backblaze', 'failure_assumption']
            for i in range(1, 256):
                fieldnames.append('smart_{0}_normalized'.format(str(i)))
                fieldnames.append('smart_{0}_raw'.format(str(i)))

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            data = pd.read_csv(file_name)
            data.head()
            for index, row in data.iterrows():
                final_rows.append(self.get_csv_row(data, row))
            writer.writerows(final_rows)

    def get_csv_row(self, data, row):
        base_row = {
            "date": row["date"],
            "serial_number": row["serial_number"],
            "model": row["model"],
            "capacity_bytes": row["capacity_bytes"],
        }
        for i in range(1, 256):
            col_name = 'smart_{0}_normalized'.format(str(i))
            if col_name in data.columns and not math.isnan(row[col_name]):
                base_row[col_name] = row[col_name]
            col_name = 'smart_{0}_raw'.format(str(i))
            if col_name in data.columns and not math.isnan(row[col_name]):
                base_row[col_name] = row[col_name]
        return base_row


if __name__ == '__main__':
    parser = CSVProcessor('/home/pkapoor/CS739-Device-Failure-Prediction/data/backblaze',
                          '/home/pkapoor/CS739-Device-Failure-Prediction/data/backblaze_processed')
    parser.parse_all()
