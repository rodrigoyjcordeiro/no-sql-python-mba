from typing import List

from model.data import Data
from neo4j import GraphDatabase


class Neo4jService:
    def __init__(self):
        #self.driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'neo4jpassword'))
        self.driver = GraphDatabase.driver('neo4j+s://11b614b9.databases.neo4j.io:7687', auth=('neo4j', 'cUKw1fdnPE7Nl3Ql1jhEQyVZVOA97xduKpLdKGzRODM'))

    def close(self):
        self.driver.close()

    def insert_data(self, data: Data):
        with self.driver.session() as session:
            query = (
                "CREATE (d:Data {ispb: $ispb, name: $name, code: $code, fullname: $fullname})"
            )
            session.run(query, parameters=data.to_dict())

    def insert_all(self, data_list: List[Data]):
        with self.driver.session() as session:
            for data in data_list:
                query = (
                    "CREATE (d:Data {ispb: $ispb, name: $name, code: $code, fullname: $fullname})"
                )
                session.run(query, parameters=data.to_dict())