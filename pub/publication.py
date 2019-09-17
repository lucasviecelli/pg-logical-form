import psycopg2
import psycopg2.extras
from psycopg2 import sql
from lib.slot import Slot

class Publication:
    
    def __init__(self, replication, connection):
        self.publication = replication.publication
        self.subscription = replication.subscription
        self.cursor = connection.cursor()
        self.commands_add = []
        self.commands_change = []
        self.commands_remove = []
            
    def valid_exists_publication_with_name(self):
        self.cursor.execute("SELECT * FROM pg_publication WHERE pubname = '%s' " % (self.publication.name))
        return len(self.cursor.fetchall()) >= 1
    
    def add_table_with_publication(self, tablename):
        query = sql.SQL("ALTER PUBLICATION {} ADD TABLE {};").format(
            sql.Identifier(self.publication.name), 
            sql.SQL(tablename)).as_string(self.cursor)

        self.commands_change.append(query)

    def modify_publication(self):
        tables = self.check_which_tables_not_exists_in_publication()

        if len(tables) <= 0:
            exit

        if tables:
            self.commands_change.append(self.set_role())

            for table in tables:
                self.add_table_with_publication(table)        
        
    def check_which_tables_not_exists_in_publication(self):
        tables = []

        for table in self.publication.tables:
            query = sql.SQL("SELECT * FROM pg_publication_tables WHERE pubname = {} AND schemaname = {} AND tablename = {}").format(
                sql.Literal(self.publication.name), 
                sql.Literal(table.split('.')[0]), 
                sql.Literal(table.split('.')[1]))

            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            if len(rows) <= 0:
                tables.append(table)

        return tables

    def create_publication(self):
        self.commands_add.append(self.set_role())

        string_tables = '   ' + (',\n      '.join(self.publication.tables))
        self.commands_add.append(("CREATE PUBLICATION \"%s\" \n" +
                                  "   FOR TABLE \n" +
                                  "   %s;") % (self.publication.name, string_tables))


        # if self.publication.connection.host != self.subscription.connection.host:
        pg_slot = Slot(self.cursor, self.publication.slot_name)

        if pg_slot.verify_slot_not_exists():
            self.commands_add.append(pg_slot.create_slot())
            
    def set_role(self):
        return ("SET ROLE '%s';" % self.publication.owner.user)

    def has_any_changes(self):
        return len(self.commands_add) > 0 or len(self.commands_change) > 0

    def run(self):
        if self.valid_exists_publication_with_name():
            self.modify_publication()
        else:
            self.create_publication()
