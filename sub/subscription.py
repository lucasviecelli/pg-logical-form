import psycopg2
import psycopg2.extras
from psycopg2 import sql
from lib.slot import Slot

class Subscription:
    
    def __init__(self, replication, connection, publication_has_any_changes):
        self.replication = replication
        self.subscription = replication.subscription
        self.cursor = connection.cursor()
        self.publication_has_any_changes = publication_has_any_changes
        self.commands_add = []
        self.commands_change = []
        self.commands_remove = []

    def exists_subscription(self):
        query = sql.SQL("select * from pg_subscription where subname = {} ").format(
            sql.Literal(self.subscription.name))

        self.cursor.execute(query)
        return len(self.cursor.fetchall()) >= 1

    def create_subscription(self):
        self.set_role()

        query = sql.SQL('CREATE SUBSCRIPTION {} \n' +
                        "   CONNECTION 'host={} port={} user={} dbname={}'\n" +
                        '   PUBLICATION {} \n' +
                        "   WITH(slot_name = {}, create_slot = false);"
                ).format(
                    sql.Identifier(self.subscription.name),
                    sql.SQL(self.replication.publication.connection.host),
                    sql.Literal(self.replication.publication.connection.port),
                    sql.SQL(self.replication.publication.connection.user),
                    sql.SQL(self.replication.publication.connection.database),
                    sql.Identifier(self.replication.publication.name),
                    sql.Identifier(self.replication.publication.slot_name)).as_string(self.cursor)

        self.commands_add.append(query)
        self.refresh_subscription()

    def set_role(self):
        query = sql.SQL('SET ROLE {};').format(sql.Literal(self.replication.subscription.owner.user)).as_string(self.cursor)
        self.commands_add.append(query)

    def refresh_subscription(self):
        query = sql.SQL('ALTER SUBSCRIPTION {} REFRESH PUBLICATION;').format(sql.Identifier(self.subscription.name)).as_string(self.cursor)
        self.commands_add.append(query)

    def change_subscription(self):
        print("Not implemented")

    def has_any_changes(self):
        return len(self.commands_add) > 0 or len(self.commands_change) > 0

    def run(self):
        if self.exists_subscription():
            if self.publication_has_any_changes:
                self.refresh_subscription()
        else:
            self.create_subscription()


