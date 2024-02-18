import os
from typing import List

from cassandra.cluster import Cluster, ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

from model.data import Data


class CassandraService:
    def __init__(self, is_local=False, keyspace="atividade_nosql", username="", password="",
                 contact_points=['localhost'], port=9042):
        self.session = Cluster(
            cloud={
                'secure_connect_bundle': '/home/rodrigocordeiro/Documentos/GitHub/no-sql-python-mba/secure-connect-mba2024-nosql.zip',
                'use_default_tempdir': False
            },
            auth_provider=PlainTextAuthProvider(
                "token",
                "AstraCS:GDtMZDpkWSZtMkbAqwZdxxdz:921a0fc8f880fc108d37dcfd4e886f789bb371c7eb40d22c6770c76d5f2fe1a9"),

        ).connect()

        # Conectar ao keyspace
        self.session.set_keyspace(keyspace)

        # Drop da tabela se existir
        self.drop_table_if_exists()

        # Criação da tabela
        self.create_table_if_not_exists()

    def drop_keyspace_if_exists(self, keyspace):
        query = f"DROP KEYSPACE IF EXISTS {keyspace}"
        self.execute_query(query)

    def create_keyspace_if_not_exists(self, keyspace):
        query = f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        self.execute_query(query)

    def drop_table_if_exists(self):
        query = "DROP TABLE IF EXISTS lista_banco"
        self.execute_query(query)

    def create_table_if_not_exists(self):
        query = """
            CREATE TABLE IF NOT EXISTS lista_banco (
                ispb TEXT PRIMARY KEY,
                name TEXT,
                code TEXT,
                fullname TEXT
            )
        """
        self.execute_query(query)

    def insert_data(self, data: Data):
        query = f"INSERT INTO lista_banco (ispb, name, code, fullname) VALUES (%s, %s, %s, %s);"
        self.execute_query(query, (data.ispb, data.name, data.code, data.fullname))

    def execute_query(self, query, values=None):
        try:
            statement = SimpleStatement(query, consistency_level=ConsistencyLevel.TWO)
            self.session.execute(statement, values)
        except Exception as e:
            print(f"Erro ao executar a consulta: {e}")

    def insert_all(self, data_list: List[Data]):
        query = "INSERT INTO lista_banco (ispb, name, code, fullname) VALUES (?, ?, ?, ?);"
        for data in data_list:
            self.execute_query(query, (data.ispb, data.name, data.code, data.fullname))

    def close_connection(self):
        self.session.shutdown()
        self.cluster.shutdown()
