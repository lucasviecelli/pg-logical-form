import psycopg2
import psycopg2.extras
from psycopg2 import sql

class Publication:
    def connect_pg(publication):
        conn_string = ("host='%s' dbname='%s' user='%s' password='%s'" % (publication.host, publication.database.name, publication.user, publication.password))
            
        print("Connecting to database\n	->%s" % (conn_string))
        connection = psycopg2.connect(conn_string)
        return connection.cursor()
            
    def valid_exists_publication_with_name(pubname, cursor):
        # cursor = connection.cursor('cursor_unique_name', cursor_factory = psycopg2.extras.DictCursor)
        cursor.execute("SELECT * FROM pg_publication WHERE pubname = '%s' " % (pubname))
        return len(cursor.fetchall()) > 1

    
    def add_table_with_publication(pubname, tablename, cursor):
        query = sql.SQL("ALTER PUBLICATION {} ADD TABLE {} ").format(
            sql.Literal(pubname), 
            sql.Literal(tablename))

        try:
            cursor.execute(query)
            return True
        except Exception as e:
            print(("Erro ao incluir a tabela %s no publication %s", (pubname, tablename)))
            print(e)
            return False

    def modify_publication(publication, cursor):
        print("implementar modify")
        tables = check_which_tables_exists_in_publication(publication, cursor)

        if len(tables) <= 0:
            print("Todas as tabelas estão criadas no publication")
            exit

        for table in tables:
            add_table_with_publication(publication.name, table, cursor)
        
        
    def check_which_tables_exists_in_publication(publication, cursor):
        tables = []

        for table in publication.database.tables:
            query = sql.SQL("select * from pg_publication_tables WHERE pubname = {} AND schemaname = {} tablename = {}").format(
                sql.Literal(publication.name), 
                sql.Literal(table.split('.')[0]), 
                sql.Literal(table))

            cursor.execute(query)
            rows = cursor.fetchall()

            tables.append(rows[0]['tablename'])

        return rows

    def create_publication(publication, cursor):
        string_tables = ', '.join(publication.database.tables)

        create_pub = ("CREATE PUBLICATION %s FOR TABLE %s;" % (publication.name, string_tables))    
        cursor.execute(create_pub)

    def exec(self, publication):
        print("AQUI")

        cursor = connect_pg(publication)

        # if valid_exists_publication_with_name(publication.name, cursor):
        if True:
            modify_publication(publication, cursor)
        else:
            create_publication(publication, cursor)
