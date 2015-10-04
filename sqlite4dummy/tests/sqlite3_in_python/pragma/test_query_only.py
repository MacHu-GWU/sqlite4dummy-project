#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
``PRAGMA query_only = 1`` 命令能让数据库不接受任何Insert, Update, Delete。但不知道
为什么, 没有成功的让这个命令工作起来。
"""

from pprint import pprint as ppt
import sqlite3
import unittest
import os

def example1():
    dbfile = ":memory:"
    connect = sqlite3.connect(dbfile)
    cursor = connect.cursor()
    
    create_table_sql = \
    """
    CREATE TABLE test
    (
        _id INTEGER,
        _int INTEGER
    )
    """
    cursor.execute(create_table_sql)
    cursor.execute("PRAGMA query_only = true")
     
    data = [(i, i) for i in range(10)]
    cursor.executemany("INSERT INTO test VALUES (?,?)", data)
    connect.commit()
    print(cursor.execute("SELECT COUNT(*) FROM (SELECT _id FROM test)").fetchone())
    connect.close()

if __name__ == "__main__":    
    example1()
