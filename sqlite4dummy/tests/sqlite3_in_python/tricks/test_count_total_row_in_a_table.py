#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from sqlite4dummy.tests.basetest import BaseUnittest
import unittest
import sqlite3
import time
import os

dbfile = "test.sqlite3"

class Unittest(BaseUnittest):
    """
    
    结论: 计算总共有多少列最好的方法是使用:
    SELECT COUNT(*) FROM (SELECT column FROM table)
    """
    def setUp(self):
        self.connect_database()
        
    def test_all(self):
        connect = self.connect
        cursor = self.cursor
        
        cursor.execute(
            "CREATE TABLE test (_int INTEGER PRIMARY KEY, _float REAL, _str TEXT)")
        data = [(i, 3.14, "Hello World") for i in range(1000)]
        cursor.executemany("INSERT INTO test VALUES (?,?,?)", data) 
        connect.commit()
        
        st = time.clock()
        res = cursor.execute("SELECT COUNT(*) FROM (SELECT * FROM test)").fetchone()
        print("SELECT * takes %.6f" % (time.clock() - st))
        
        st = time.clock()
        res = cursor.execute("SELECT COUNT(*) FROM (SELECT _int FROM test)").fetchone()
        print("SELECT COUNT(*) takes %.6f" % (time.clock() - st))
    
if __name__ == "__main__":
    unittest.main() 