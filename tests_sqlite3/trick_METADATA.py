#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Execute example functions one by one for demo.
"""

from __future__ import print_function
from pprint import pprint as ppt
import sqlite3
import json

connect = sqlite3.connect(":memory:")
cursor = connect.cursor()

def example1_special_table_sqlite_master():
    """sqlite3中有一个特殊的隐藏表sqlite_master, 里面储存了所有的表, index这一类的
    metadata信息。
    
    我们可以使用下面的命令得到所有的表名::
    
        >>> cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table';")
    
    Doc: https://www.sqlite.org/pragma.html
    """
    cursor.execute("""
    CREATE TABLE employee 
    (
        _id INTEGER PRIMARY KEY,
        email TEXT NOT NULL UNIQUE, 
        age INTEGER,
        height REAL,
        profile BLOB        
    )
    """)
    print("sqlite_master表中的内容:")
    for record in cursor.execute("SELECT * FROM sqlite_master;"):
        print(record)
        
    print("sqlite_master表中的列代表什么含义:")
    ppt(list(cursor.execute("PRAGMA table_info(sqlite_master)")))
    
# example1_special_table_sqlite_master()

def example2_get_table_column_info():
    """PRAGMA table_info(table-name)返回表中的列的详细信息, 我们可以用下面的命令
    得到表的所有列信息::
    
        >>> cursor.execute("PRAGMA table_info(table-name)")
        >>> for record in cursor.fetchall():
        ...     print(record)
    
    record中每个元素的含义。
    0. column series number
    1. column name
    2. data type
    3. whether the column can be NULL
    4. default value for the column
    5. is primary key
    """
    cursor.execute("CREATE TABLE test (_id INTEGER PRIMARY KEY NOT NULL UNIQUE);")
    print(list(cursor.execute("PRAGMA table_info(test)")))
    
# example2_get_table_column_info()