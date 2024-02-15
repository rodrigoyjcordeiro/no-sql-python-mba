from model.data import Data
from neo4j import GraphDatabase


class Neo4jService:
    def __init__(self):
        self.driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'neo4jpassword'))

    def close(self):
        self.driver.close()

    def insert_data(self, data: Data):
        with self.driver.session() as session:
            query = (
                "CREATE (d:Data {ispb: $ispb, name: $name, code: $code, fullname: $fullname})"
            )
            session.run(query, parameters=data.to_dict())
