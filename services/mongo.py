from pymongo import MongoClient

from model.data import Data


class MongoDBService:
    def __init__(self):
        self.client = MongoClient('mongodb://admin:adminpassword@localhost:27017/')
        self.db = self.client['mydatabas']
        self.collection = self.db['teste']

    def insert_data(self, data: Data):
        document = {
            "ispb": data.ispb,
            "name": data.name,
            "code": data.code,
            "fullname": data.fullname
        }
        self.collection.insert_one(document)
