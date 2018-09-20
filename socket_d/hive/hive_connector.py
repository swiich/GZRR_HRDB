# coding=utf-8


# 代码有任何变动需要同时修改 /usr/local/python3/lib/python3.6/hive_connector.py
from impala.dbapi import connect


def get_hive_cursor(host, database):
    conn = connect(
        host=host,
        port=10000,
        database=database,
        auth_mechanism='PLAIN'
    )
    return conn.cursor()


def execute_sql(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def execute_sql_insert(cursor, sql):
    cursor.execute(sql)


