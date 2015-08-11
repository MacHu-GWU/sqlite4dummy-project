#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Execute example functions one by one for demo.
"""

from __future__ import print_function
from pprint import pprint as ppt
import sqlite3

connect = sqlite3.connect(":memory:")
cursor = connect.cursor()
cursor.execute("""CREATE TABLE test (a INTEGER, b INTEGER, c REAL, d TEXT, PRIMARY KEY (a));""")
records = [(i, i, i*1.0, chr(i+64)) for i in range(1, 10+1)]
cursor.executemany("INSERT INTO test VALUES (?,?,?,?);", records)

def example1_absolute_update():
    """对一条记录进行更新。
    """
    print("Before:")
    ppt(list(cursor.execute("SELECT * FROM test WHERE a = 1;")))
    
    print("After:")
    cursor.execute("UPDATE test SET b = 100 WHERE a = 1;")
    ppt(list(cursor.execute("SELECT * FROM test WHERE a = 1;")))
    
# example1_absolute_update()

def example2_batch_absolute_update():
    """对满足WHERE条件的所有记录进行更新。
    """
    print("Before:")
    ppt(list(cursor.execute("SELECT * FROM test;")))
    
    print("After:")
    cursor.execute("UPDATE test SET b = 100 WHERE a <= 5;")
    ppt(list(cursor.execute("SELECT * FROM test;")))
    
# example2_batch_absolute_update()

def example3_relative_update():
    """使用计算操作进行相对更新, 例如自增。
    """
    print("Before:")
    ppt(list(cursor.execute("SELECT * FROM test WHERE a <= 3;")))
    
    print("After:")
    cursor.execute("UPDATE test SET b = b + 100 WHERE a = 1;")
    cursor.execute("UPDATE test SET c = b + c WHERE a = 2;")
    cursor.execute("UPDATE test SET d = d || 'Hello' WHERE a = 3;")
    ppt(list(cursor.execute("SELECT * FROM test WHERE a <= 3;")))
    
# example3_relative_update()