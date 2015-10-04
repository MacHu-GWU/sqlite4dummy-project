#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from sqlite4dummy.tests.basetest import BaseUnittest
import sqlite3 
import unittest

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database_use_row_factory()
        self.create_all_type_table(has_data=True)
        
    def test_select_as(self):
        """SELECT中如果使用SQL函数, 或者使用AS, 那么返回的列名就不是原来的列名了,
        而根据情况的不同有不同的名字。
        """
        cursor = self.cursor
        
        row = cursor.execute("SELECT _int + 1 FROM all_type").fetchone()
        print(row.keys(), list(row))
        
        row = cursor.execute("SELECT _int + _float FROM all_type").fetchone()
        print(row.keys(), list(row))

        row = cursor.execute("SELECT COUNT(_int) FROM all_type").fetchone()
        print(row.keys(), list(row))

        row = cursor.execute("SELECT COUNT(*) FROM all_type").fetchone()
        print(row.keys(), list(row))

        row = cursor.execute("SELECT MAX(_int) FROM all_type").fetchone()
        print(row.keys(), list(row))

        row = cursor.execute("SELECT MIN(_int) FROM all_type").fetchone()
        print(row.keys(), list(row))
        
        row = cursor.execute("SELECT ROUND(_int) FROM all_type").fetchone()
        print(row.keys(), list(row))
        
        row = cursor.execute("SELECT LOWER(_str) FROM all_type").fetchone()
        print(row.keys(), list(row))

        row = cursor.execute("SELECT UPPER(_str) FROM all_type").fetchone()
        print(row.keys(), list(row))

        row = cursor.execute("SELECT _bytes as BYTES FROM all_type").fetchone()
        print(row.keys(), list(row))
        
if __name__ == "__main__":
    unittest.main()