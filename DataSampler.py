import csv
import os
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
        print("To process Dirs: " + str(to_process_dirs))
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
        with open(file_name, 'r') as csvfile:
            line_count = 0
            csv_reader = csv.reader(csvfile, delimiter=',')
            header_to_index = None
            failure_rows = []
            success_rows = []
            for row in csv_reader:
                if line_count == 0:
                    header_to_index = {}
                    col_index = 0
                    for col in row:
                        header_to_index[col] = col_index
                        col_index = col_index + 1
                    line_count = line_count + 1
                else:
                    if row[4] == '1':
                        failure_rows.append(self.get_csv_row(header_to_index, row))
                    else:
                        success_rows.append(self.get_csv_row(header_to_index, row))
                    line_count = line_count + 1
        failure_count = len(failure_rows)
        required_success_count = int(self.multiplier * failure_count)
        if required_success_count > len(success_rows):
            required_success_count = len(success_rows)
        random.shuffle(success_rows)
        self.success_rows.extend(success_rows[1:required_success_count])
        self.failed_rows.extend(failure_rows)

    def get_csv_row(self, header_to_index, row):
        rv = []
        rv.append(row[0])
        rv.append(row[1])
        rv.append(row[2])
        rv.append(row[3])
        rv.append(row[4])
        for i in range(1, 256):
            col_name = 'smart_{0}_normalized'.format(str(i))
            index = header_to_index.get(col_name, -1)
            if index == -1:
                rv.append('')
            else:
                rv.append(row[index])
            col_name = 'smart_{0}_raw'.format(str(i))
            index = header_to_index.get(col_name, -1)
            if index == -1:
                rv.append('')
            else:
                rv.append(row[index])
        return rv

    def write_results(self):
        with open(os.path.join(self.dst_dir, self.dst_file), 'w') as csvfile:
            fieldnames = ['date', 'serial_number', 'model', 'capacity_bytes', 'failure']
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
        '/home/pkapoor/CS739-Device-Failure-Prediction/data/backblaze',
        '/home/pkapoor/new_disk_mnt/data',
        'train_Q4_2016.csv',
        blacklist_folders=['data_Q1_2018', 'data_Q2_2018', 'data_Q3_2018', 'data_Q1_2017', 'data_Q2_2017',
                           'data_Q3_2017', 'data_Q4_2017', 'data_Q1_2016', 'data_Q2_2016',
                           'data_Q3_2016', '2014', '2013', '2015']
    )
    sampler.sample_all()
