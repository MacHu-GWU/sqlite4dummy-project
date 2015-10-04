#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INSERT OR REPLACE简介:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

INSERT OR REPLACE的逻辑是: 如果无法成功INSERT, 那么则根据无法成功的conflict, 
删除原数据, 然后重新Insert。

与之相似的是UPSERT (在sqlite3中不原生支持)。不同的是, Upsert是尝试Update, 
并不会对其他没定义的列造成影响。而INSERT OR REPLACE会删除整行数据。

P.S. REPLACE是INSERT OR REPLACE的简写。
"""

from __future__ import print_function
from sqlite4dummy.tests.basetest import BaseUnittest
import unittest
import sqlite3

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database()
        
    def test_INSERT_OR_REPLACE(self):
        cursor = self.cursor
        
        # http://www.w3schools.com/sql/sql_create_table.asp
        create_table_sql = \
        """
        CREATE TABLE employee
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            role TEXT,
            name TEXT
        )
        """
        cursor.execute(create_table_sql)
        
        data = [(1, "coder", "John"), (2, "sales", "Mike")]
        cursor.executemany("INSERT INTO employee VALUES (?,?,?)", data)
        
        cursor.execute(
            "REPLACE INTO employee (_id, role) VALUES (?,?)",
            (2, "manager"))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 2").\
              fetchone()) # (2, 'manager', None)
        
        cursor.execute(
            "REPLACE INTO employee (_id) VALUES (?)",
            (3,)
        )
        print(cursor.execute("SELECT * FROM employee WHERE _id = 3").\
              fetchone()) # (3, None, None)
        
if __name__ == "__main__":
    unittest.main()