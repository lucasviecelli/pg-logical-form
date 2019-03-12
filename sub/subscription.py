import psycopg2
import psycopg2.extras
from psycopg2 import sql

def connect_pg(subscription):
    conn_string = ("host='%s' dbname='%s' user='%s' password='%s'" % (subscription.host, subscription.database.name, subscription.user, subscription.password))
        
    print("Connecting to database\n	->%s" % (conn_string))
    connection = psycopg2.connect(conn_string)
    return connection.cursor()

def exists_subscription(cursor, subscription_name):
    query = sql.SQL("select * from pg_subscription where subname = {} ").format(
    sql.Literal(subscription_name))

    cursor.execute(query)
    return len(cursor.fetchall()) > 1

def create_subscription(cursor, replication):
    query = sql.SQL('CREATE SUBSCRIPTION {} ' +
                    "CONNECTION 'host={} port={} user={} dbname={}" +
                    'PUBLICATION = {}' +
                    "WITH(slot_name = {}, create_slot = false)"
            ).format(
                sql.Literal(replication.subscription.name),
                sql.Literal(replication.subscription.host),
                sql.Literal(replication.subscription.user),
                sql.Literal(replication.subscription.database),
                sql.Literal(replication.publication.name),
                sql.Literal('slot_test'))

    cursor.execute(query)
    refresh_subscription(cursor, replication.subscription)

def refresh_subscription(cursor, subscription):
    query = sql.SQL('ALTER SUBSCRIPTION {} REFRESH PUBLICATION').format(sql.Literal(subscription.name))
    cursor.execute(query)

def exec(replication):
    print("INICIANDO atualização do subscription")

    subscription = replication.subscription
    cursor = connect_pg(subscription)

    if exists_subscription(subscription.name):
        refresh_subscription(cursor, subscription)
    else:
        create_subscription(cursor, replication)

    print("FINALIZADO atualização do subscription")

