import psycopg2
import psycopg2.extras
from psycopg2 import sql

class Publication:
    def __init__(self, publication):
        self.publication = publication
        self.commands_add = []
        self.commands_change = []
        self.commands_remove = []

    def connect_pg(self, publication):
        conn_string = ("host='%s' dbname='%s' user='%s' password='%s'" % (publication.host, publication.database.name, publication.user, publication.password))
            
        print("Connecting to database\n	->%s" % (conn_string))
        connection = psycopg2.connect(conn_string)
        return connection.cursor()
            
    def valid_exists_publication_with_name(self, cursor):
        # cursor = connection.cursor('cursor_unique_name', cursor_factory = psycopg2.extras.DictCursor)
        cursor.execute("SELECT * FROM pg_publication WHERE pubname = '%s' " % (self.publication.name))
        return len(cursor.fetchall()) >= 1

    
    def add_table_with_publication(self, pubname, tablename, cursor):
        query = sql.SQL("ALTER PUBLICATION {} ADD TABLE {} ").format(
            sql.Literal(pubname), 
            sql.Literal(tablename)).as_string

        self.commands_change.append(query)

    def modify_publication(self, publication, cursor):
        tables = self.check_which_tables_not_exists_in_publication(publication, cursor)

        if len(tables) <= 0:
            print("Todas as tabelas estão criadas no publication")
            exit

        for table in tables:
            self.add_table_with_publication(publication.name, table, cursor)
        
        
    def check_which_tables_not_exists_in_publication(self, publication, cursor):
        tables = []

        for table in publication.database.tables:
            query = sql.SQL("select * from pg_publication_tables WHERE pubname = {} AND schemaname = {} AND tablename = {}").format(
                sql.Literal(publication.name), 
                sql.Literal(table.split('.')[0]), 
                sql.Literal(table))

            cursor.execute(query)
            rows = cursor.fetchall()

            if len(cursor.fetchall()) >= 1:
                tables.append(rows[0]['tablename'])

        return tables

    def create_publication(self, publication, cursor):
        string_tables = ', '.join(publication.database.tables)
        self.commands_add.append("CREATE PUBLICATION %s FOR TABLE %s;" % (publication.name, string_tables))        

    def run(self):
        cursor = self.connect_pg(self.publication)

        if self.valid_exists_publication_with_name(cursor):
            self.modify_publication(self.publication, cursor)
        else:
            self.create_publication(self.publication, cursor)
