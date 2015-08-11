#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint as ppt
import sqlite3
import random
import time

def example_select_count():
    connect = sqlite3.connect(":memory:")
    cursor = connect.cursor()
    cursor.execute("CREATE TABLE test (_id INTEGER, value INTEGER)")
    cursor.executemany("INSERT INTO test VALUES (?,?);", [
        (10-i, random.randint(100, 200)) for i in range(10)
        ])
    
    query = "SELECT count(_id) FROM (SELECT * FROM test WHERE _id > 5);"
    for record in cursor.execute(query):
        print(record)
    
example_select_count()