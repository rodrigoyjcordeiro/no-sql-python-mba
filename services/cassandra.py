from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from model.data import Data


class CassandraService:
    def __init__(self, keyspace, username, password, contact_points=['localhost'], port=9042):
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        self.cluster = Cluster(contact_points, auth_provider=auth_provider, port=port)
        self.session = self.cluster.connect(keyspace)

    def insert_data(self, data: Data):
        query = "INSERT INTO sua_tabela (ispb, name, code, fullname) VALUES (%s, %s, %s, %s);"
        self.execute_query(query, tuple(data.to_dict().values()))

    def execute_query(self, query, values=None):
        try:
            self.session.execute(query, values)
        except Exception as e:
            print(f"Erro ao executar a consulta: {e}")

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()