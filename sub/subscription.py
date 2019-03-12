import psycopg2
import psycopg2.extras
from psycopg2 import sql

class Subscription:
    
    def __init__(self, replication):
        self.replication = replication
        self.subscription = replication.subscription

    def connect_pg(self): 
        conn_string = ("host='%s' dbname='%s' user='%s' password='%s'" % (self.subscription.host, self.subscription.database.name, self.subscription.user, self.subscription.password))
            
        print("Connecting to database\n	->%s" % (conn_string))
        connection = psycopg2.connect(conn_string)
        return connection.cursor()

    def exists_subscription(self, cursor):
        query = sql.SQL("select * from pg_subscription where subname = {} ").format(
        sql.Literal(self.subscription.name))

        cursor.execute(query)
        return len(cursor.fetchall()) > 1

    def create_subscription(self, cursor):
        query = sql.SQL('CREATE SUBSCRIPTION {} ' +
                        "CONNECTION 'host={} port={} user={} dbname={}" +
                        'PUBLICATION = {}' +
                        "WITH(slot_name = {}, create_slot = false)"
                ).format(
                    sql.Literal(self.replication.subscription.name),
                    sql.Literal(self.replication.subscription.host),
                    sql.Literal(self.replication.subscription.user),
                    sql.Literal(self.replication.subscription.database),
                    sql.Literal(self.replication.publication.name),
                    sql.Literal('slot_test'))

        cursor.execute(query)
        self.refresh_subscription(cursor, self.replication.subscription)

    def refresh_subscription(self, cursor):
        query = sql.SQL('ALTER SUBSCRIPTION {} REFRESH PUBLICATION').format(sql.Literal(self.subscription.name))
        cursor.execute(query)

    def run(self):
        replication = self.replication
        print("INICIANDO atualização do subscription")

        subscription = replication.subscription
        cursor = self.connect_pg(subscription)

        if self.exists_subscription():
            self.refresh_subscription(cursor)
        else:
            self.create_subscription(cursor)

        print("FINALIZADO atualização do subscription")

