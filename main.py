
from model.data import Data
from services.cassandra import CassandraService
from services.mongo import MongoDBService
from services.neo4j import Neo4jService


def load_db():
    #mongo_service = MongoDBService()
    #neo4j_service = Neo4jService()

    casandra_service = CassandraService(username="user", password="user")

    data = Data(ispb="12", name="blabla", code="teste", fullname="teste")

    #mongo_service.insert_data(data=data)
    #neo4j_service.insert_data(data=data)

    casandra_service.insert_data(data=data)


if __name__ == '__main__':
    load_db()
