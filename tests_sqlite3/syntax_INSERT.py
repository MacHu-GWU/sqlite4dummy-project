#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Execute example functions one by one for demo.
"""

from __future__ import print_function
from pprint import pprint as ppt
import sqlite3
import random
import time

def example1_four_basic_insert_syntax():
    """四种基本INSERT语法。
    
    注意: 在设置有且只有一个主键, 而且是整数类型时, sqlite3会在insert时自动自
    增1。
    """
    connect = sqlite3.connect(":memory:")
    cursor = connect.cursor()
    cursor.execute("CREATE TABLE test (_id INTEGER PRIMARY KEY NOT NULL, name TEXT, value REAL)")
    ## 在sqlite3中?相当于格式字符串输出符%s
    cursor.execute("INSERT INTO test VALUES (?, ?, ?)", (1, "coke", 3.49) ) # syntax1
    cursor.execute("INSERT INTO test VALUES (2, 'banana', 0.69)") # syntax2
    cursor.execute("INSERT INTO test (name, value) VALUES (?, ?)", ("apple", 5.19) ) # syntax3
    cursor.execute("INSERT INTO test (name, value) VALUES ('water', 1.35)") # syntax4
    cursor.execute("SELECT * FROM test")
    for row in cursor.fetchall():
        print(row)
    
# example1_four_basic_insert_syntax()

def example2_bulk_insert():
    """批量插入, 使用cursor.executemany()方法能获得更快的速度。尤其是在
    每执行完一条之后立刻commit时效率更高。
    """
    connect = sqlite3.connect(":memory:")
    cursor = connect.cursor()
    cursor.execute("CREATE TABLE test (_id INTEGER, content TEXT);")
    
    N = 1000*1000
    records1 = [(i, "Hello World!") for i in range(N)]
    records2 = [(i, "Hello World!") for i in range(N, 2*N)]
    
    st = time.clock()
    cursor.executemany("INSERT INTO test VALUES (?,?)", records1)
    print("bulk insert elaspe %.4f sec." % (time.clock() - st))
    
    st = time.clock()
    for record in records2:
        cursor.execute("INSERT INTO test VALUES (?, ?)", record)
    print("regular elaspe %.4f sec." % (time.clock() - st))
    
# example2_bulk_insert()

def example3_primary_key_conflict():
    """处理主键冲突以及各种插入执行失败的情况。
    """
    connect = sqlite3.connect(":memory:")
    cursor = connect.cursor()
    cursor.execute("CREATE TABLE test (_id INTEGER PRIMARY KEY, number INTEGER)")
    records = [(1, 10), (3, 10), (5, 10)] # insert some records at begin
    cursor.executemany("INSERT INTO test VALUES (?, ?)", records)
    
    records = [(2, 10), (3, 10), (4, 10)]
    try:
        cursor.executemany("INSERT INTO test VALUES (?, ?)", records)
    except sqlite3.IntegrityError: # bulk insert failed
        for record in records:
            try:
                cursor.execute("INSERT INTO test VALUES (?, ?)", record)
            except sqlite3.IntegrityError:
                print("cannot insert %s" % repr(record))
            
    ppt(list(cursor.execute("SELECT * FROM test")))

example3_primary_key_conflict()