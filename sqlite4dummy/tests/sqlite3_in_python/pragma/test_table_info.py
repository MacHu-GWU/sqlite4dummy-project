#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PRAGMA命令中的table_info能返回table的详细信息, 返回的临时表的结构如下:

- col1: column id
- col2: column name
- col3: data type
- col4: is NOT NULL
- col5: DEFAULT VALUE in string
- col6: is PRIMARY KEY
"""

from pprint import pprint as ppt
import sqlite3
import unittest

class TableInfoUnittest(unittest.TestCase):
    def setUp(self):
        self.connect = sqlite3.connect(":memory:")
        self.cursor = self.connect.cursor()
        # http://www.w3schools.com/sql/sql_create_table.asp
        create_table_sql = \
        """
        CREATE TABLE test
        (
            _id INTEGER NOT NULL,
            _int INTEGER DEFAULT 1024,
            _float REAL DEFAULT 1.414,
            _str TEXT DEFAULT "Hello World",
            _date DATE,
            _datetime DATETIME NOT NULL,
            _bytes BLOB,
            PRIMARY KEY (_id)
        )
        """
        self.cursor.execute(create_table_sql)
        
        # http://www.w3schools.com/sql/sql_create_index.asp
        create_index_sql = \
        """
        CREATE INDEX fast_query
        ON test (_int DESC, _float DESC, _date ASC, _datetime ASC)
        """
        self.cursor.execute(create_index_sql)
        
    def test_all(self):
        connect = self.connect
        cursor = self.cursor
        
        ppt(cursor.execute("PRAGMA table_info(test)").fetchall())
        
    def tearDown(self):
        self.connect.close()
if __name__ == "__main__":
    unittest.main()