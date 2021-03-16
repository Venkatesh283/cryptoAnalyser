from pymongo import MongoClient
from pymongo.errors import BulkWriteError

class MongoUtilities():
    def __init__(self, log, mongo_credential):
        self.log = log
        self.client = MongoClient("mongodb://{0}:{1}@{2}:{3}".format(
            mongo_credential['user'], mongo_credential['password'],
            mongo_credential['host'], mongo_credential['port']))

    def upload_to_mongo(self, data, db, collection):
        coll_obj = self.client[db][collection]

        if isinstance(data, list):
            try:
                coll_obj.insert_many(data, ordered=False)
            except BulkWriteError as exe:
                details = exe.details
                message = details['writeErrors'][0]['errmsg']
                self.log.error('upload_to_mongo', message=message)

        else:
            try:
                coll_obj.insert(data)
            except DuplicateKeyError:
                self.log.error('upload_to_mongo', message="duplicate key error")

    def search(self, query, db, collection, options={}):
        coll_obj = self.client[db][collection]

        cur = coll_obj.find(query, options)
        return cur

    def perform_aggregation(self, query, db, collection):
        coll_obj = self.client[db][collection]
        cur = coll_obj.aggregate(query)

        return cur

    def update(self, query, data, db, collection):
        coll_obj = self.client[db][collection]
        ack = coll_obj.update(query, {'$set': data})

        return ack
