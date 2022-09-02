"""
Simple Postgresql app that has functions for the most common user or admin tasks.

Methods include print out for whole table, insert, batch insert, delete, and WHERE query.

Need a local or web postgresql server to test or use. More secure than the SQLite
app on its own, but I decided to keep it simpler. More complex commands collating
multiple tables or running big queries would need to be run through the
cursor.execute() function.
"""

import psycopg2
from psycopg2 import sql, OperationalError


def connect(db_name:str,db_user:str,db_password:str,db_host='localhost',port=5433):
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            port=port,
            password=db_password,
            host=db_host)
    except OperationalError as e:
        print(f"The error {e} happened")
    return conn


server_name,user_name,user_pass = 'server', 'user', 'pass'  # end user needs to fill these in with their own info!
conn = connect(server_name,user_name,user_pass)
conn.set_session(autocommit=True)


class Pg_database:
    def __init__(self, table_name:str):
        self.cur = conn.cursor()
        self.table = table_name

    def tidy_print(self):
        self.cur.execute(sql.SQL("SELECT * FROM {table};").format(
            table=sql.Identifier(self.table)))
        for item in self.cur.fetchall():
            print(item)

    def insert_item(self, column_names:list, item_values:list):
        values_placeholder = '%s' + (',%s' * (len(item_values)-1))
        columns_list = [sql.Identifier(item) for item in column_names]
        exec_str = "INSERT INTO {table} ({columns}) VALUES(%s);" % values_placeholder
        self.cur.execute(sql.SQL(exec_str).format(
            table=sql.Identifier(self.table),
            columns=sql.SQL(', ').join(columns_list)
        ), item_values)

    def batch_insert(self, column_names:list, item_values_batch:list):
        for ins_item in item_values_batch:
            self.insert_item(column_names, ins_item)

    def delete_item(self, primary_key, item_id):
        self.cur.execute(sql.SQL("DELETE FROM {table} WHERE {pkey} = %s").format(
            table=sql.Identifier(self.table),
            pkey=sql.Identifier(primary_key)
        ), (item_id,))

    def where_query(self, primary_key, comp_operator, target_val, order_key, asc=True):
        sql_str = "SELECT * FROM {table} WHERE {pkey} %s " % (comp_operator if len(comp_operator) < 4 else '=')
        if not asc:
            self.cur.execute(sql.SQL(sql_str+'%s ORDER BY {order_key} DESC;').format(
                table=sql.Identifier(self.table),
                pkey=sql.Identifier(primary_key),
                order_key=sql.Identifier(order_key),
            ), (target_val,))
        if asc:
            self.cur.execute(sql.SQL(sql_str + '%s ORDER BY {order_key};').format(
                table=sql.Identifier(self.table),
                pkey=sql.Identifier(primary_key),
                order_key=sql.Identifier(order_key),
            ), (target_val,))
        return self.cur.fetchall()


conn.close()
