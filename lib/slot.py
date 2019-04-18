from psycopg2 import sql

class Slot:

    def __init__(self, cursor, slot_name):
        self.cursor = cursor
        self.slot_name = slot_name

    def verify_slot_not_exists(self):
        self.cursor.execute("SELECT * FROM pg_replication_slots WHERE slot_name = '%s' " % (self.slot_name))
        return len(self.cursor.fetchall()) == 0

    def create_slot(self):
        return sql.SQL("SELECT * FROM pg_create_logical_replication_slot({}, {});").format(
            sql.Literal(self.slot_name),
            sql.Literal('pgoutput')
        ).as_string(self.cursor)