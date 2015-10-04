#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
``PRAGMA index_info(index-name)`` 能返回index的详细信息, 返回的临时表的结构如下:

- col1: The rank of the column within the index. (0 means left-most.)
- col2: The rank of the column within the table being indexed.
- col3: The name of the column being indexed.

Ref: https://www.sqlite.org/pragma.html#pragma_index_list


``PRAGMA index_list(table-name)`` 能返回与table有关的所有index的基本信息:

- col1: A sequence number assigned to each index for internal tracking purposes.
- col2: The name of the index.
- col3: "1" if the index is UNIQUE and "0" if not.

Ref: https://www.sqlite.org/pragma.html#pragma_index_info
"""

from pprint import pprint as ppt
import sqlite3
import unittest

class Unittest(unittest.TestCase):
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
        
    def test_index_info(self):
        ppt(self.cursor.execute("PRAGMA index_info(fast_query)").fetchall())
    
    def test_index_list(self):
        ppt(self.cursor.execute("PRAGMA index_list(test)").fetchall())
        
    def tearDown(self):
        self.connect.close()
        
if __name__ == "__main__":
    unittest.main()