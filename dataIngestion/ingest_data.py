import os
import sys
import inspect
import json
import argparse

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
common_utils = parent_dir + '/' + 'utilities/'
common_utils_dir = os.path.dirname(common_utils)

sys.path.insert(0, current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(1, common_utils_dir)

from utils import CommonUtilities
from mongo_utils import MongoUtilities
from settings import *

class IngestDataToDB():
    def __init__(self):
        self.args = self.define_args()
        self.utils_obj = CommonUtilities()
        self.log = self.utils_obj.init_logger(LOG_FILE)
        self.mongo_obj = MongoUtilities(self.log, MONGO_CREDENTIALS)


    def define_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-ff', '--file-format', type=str, required=True,
            help="Specify format the input file (csv/tsv/json)")
        parser.add_argument('-inp', '--input-filepath', type=str, required=True,
            help="Absolute path of input file")

        args = parser.parse_args()
        return args

    def read_from_csv(self):
        with open(self.args.input_filepath) as fp:
            reader = csv.reader(fp)
            header = next(reader)

            records = [dict(zip(header, row)) for row in reader]

        return records

    def read_from_json(self):
        data = json.load(open(self.args.input_filepath))
        return list(data.values())

    def start_process(self):
        if self.args.file_format == 'csv':
            records = self.read_from_csv()

        elif self.args.file_format == 'json':
            records = self.read_from_json()

        for rec in records:
            split_value = rec['symbol'].strip().split('/')
            rec['symbol'] = split_value[0]
            rec['currency'] = split_value[1]

            for k, v in rec.items():
                if v is None:
                    rec[k] = 0

        self.mongo_obj.upload_to_mongo(records, DATA_DB, DATA_COLLECTION)

if __name__ == '__main__':
    IngestDataToDB().start_process()
