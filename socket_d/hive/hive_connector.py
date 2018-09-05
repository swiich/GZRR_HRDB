# coding=utf-8

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
