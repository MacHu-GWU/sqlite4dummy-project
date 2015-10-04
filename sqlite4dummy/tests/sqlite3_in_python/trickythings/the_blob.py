#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint as ppt
import sqlite3
import pickle
import binascii
import sys

if sys.version_info[0] == 3:
    pk_protocol = 3
else:
    pk_protocol = 2
    
connect = sqlite3.connect(":memory:")
connect.text_factory = str # 必须如此做
cursor = connect.cursor()
select_all_sql = "SELECT * FROM test"

def example1():
    create_table_sql = \
    """
    CREATE TABLE test
    (
        _id INTEGER,
        _bytes BLOB DEFAULT X'80025d7100284b014b024b03652e'
    )
    """
    cursor.execute(create_table_sql)
    cursor.execute("INSERT INTO test (_id) VALUES (?)", (0, ))
    cursor.execute("INSERT INTO test (_id, _bytes) VALUES (?,?)", 
                   (1, pickle.dumps({"a": 1}, protocol=pk_protocol)))
    
    results = cursor.execute(select_all_sql).fetchall()
    
    print(type(results[0][1]))
    print(type(results[1][1]))
    
example1()