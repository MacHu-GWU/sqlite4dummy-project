#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint as ppt
import unittest
import sqlite3
import pickle

class CreateUnittest(unittest.TestCase):
    def test_create_table_create_index(self):
        """Test create table and create index.
        """
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
        # http://www.w3schools.com/sql/sql_create_table.asp
        create_table_sql = \
        """
        CREATE TABLE test
        (
            _id INTEGER,
            _int INTEGER,
            _float REAL,
            _str TEXT,
            _date DATE,
            _datetime DATETIME,
            _bytes BLOB,
            PRIMARY KEY (_id)
        )
        """
        cursor.execute(create_table_sql)
        
        # http://www.w3schools.com/sql/sql_create_index.asp
        create_index_sql = \
        """
        CREATE INDEX fast_query
        ON test (_int DESC, _float DESC, _date ASC, _datetime ASC)
        """
        cursor.execute(create_index_sql)
        
if __name__ == "__main__":
    unittest.main()