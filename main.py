from model.data import Data
from services.cassandra import CassandraService
from services.mongo import MongoDBService
from services.neo4j import Neo4jService
from brasilapy import BrasilAPI
from typing import List


def load_db():
    client = BrasilAPI()
    res = client.get_banks()
    response = transform_to_data_list(banks_list=res)

    mongo_service = MongoDBService()
    mongo_service.insert_all(data_list=response)

    neo4j_service = Neo4jService()
    neo4j_service.insert_all(data_list=response)

    casandra_service = CassandraService()
    casandra_service.insert_all(data_list=response)


def transform_to_data_list(banks_list) -> List[Data]:
    data_list = []
    for bank_info in banks_list:
        data_instance = Data(
            ispb=bank_info.get('ispb', ''),
            name=bank_info.get('name', ''),
            code=bank_info.get('code', ''),
            fullname=bank_info.get('fullname', '')
        )
        data_list.append(data_instance)
    return data_list


if __name__ == '__main__':
    load_db()
