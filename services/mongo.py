from typing import List

from pymongo import MongoClient

from model.data import Data


class MongoDBService:
    def __init__(self):
        self.client = MongoClient('mongodb+srv://admin:admin@cluster.kqngb1m.mongodb.net/')
        self.db = self.client['db_no_sql']
        self.collection = self.db['lista_bancos']

    def insert_data(self, data: Data):
        document = {
            "ispb": data.ispb,
            "name": data.name,
            "code": data.code,
            "fullname": data.fullname
        }
        self.collection.insert_one(document)

    def insert_all(self, data_list: List[Data]):
        documents = []
        for data in data_list:
            document = {
                "ispb": data.ispb,
                "name": data.name,
                "code": data.code,
                "fullname": data.fullname
            }
            documents.append(document)

        if documents:
            self.collection.insert_many(documents)