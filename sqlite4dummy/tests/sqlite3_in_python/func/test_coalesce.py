#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
COALESCE函数返回(x1, x2, ...)中第一个不为NULL的值。

Ref: https://www.sqlite.org/lang_corefunc.html
"""

from sqlite4dummy.tests.basetest import BaseUnittest
import unittest

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database()
        
    def test_all(self):
        cursor = self.cursor
        
        # insert some data
        create_sql = "CREATE TABLE test (_id INTEGER PRIMARY KEY, _str TEXT)"
        insert_sql = "INSERT INTO test VALUES (?,?)"
        cursor.execute(create_sql)
        cursor.execute(insert_sql, (1, "abc"))
        
        select_sql = "SELECT COALESCE (NULL, 1)"
        print(cursor.execute(select_sql).fetchone()) # (1,)
        
        select_sql = "SELECT COALESCE (NULL, (SELECT _str FROM test WHERE _id = 1))"
        print(cursor.execute(select_sql).fetchone()) # ('abc',)
        
if __name__ == "__main__":
    unittest.main()