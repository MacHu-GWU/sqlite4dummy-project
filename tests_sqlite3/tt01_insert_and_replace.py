#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3

connect = sqlite3.connect(":memory:")
cursor = connect.cursor()

def insert_or_replace_into_example():
    """
    """
    sql = """
    CREATE TABLE employee 
    (
    _id INTEGER, 
    name TEXT, 
    role TEXT, 
    PRIMARY KEY (_id)
    );
    """
    cursor.execute(sql)
    
    
    cursor.execute("INSERT INTO employee VALUES (?,?,?)", (1, "John", "CEO"))
    print(list(cursor.execute("SELECT * FROM employee")))
    
    # insert不成功时, 会调用replace替代整行。如果没有被定义的列会用Null填充。
    cursor.execute("INSERT OR REPLACE INTO employee (_id, name) VALUES (?,?);", 
                   (1, "Tom"))
    print(list(cursor.execute("SELECT * FROM employee")))

# insert_or_replace_into_example()

def upsert_example():