import psycopg2
import psycopg2.extras
from psycopg2 import sql

class OpenConnection:

    def __init__(self, publication, subscription):
        self.publication = publication
        self.subscription = subscription

    def connect_pg(self, connection):
        conn_string = ("host='%s' dbname='%s' port='%s' user='%s'" % (connection.host, connection.database, connection.port, connection.user))
        return psycopg2.connect(conn_string)

    def get_connection_pub(self):
        return self.connect_pg(self.publication.connection)
        
    def get_connection_sub(self):
        return self.connect_pg(self.subscription.connection)