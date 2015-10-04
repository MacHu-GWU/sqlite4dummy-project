#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest
import sqlite3
import time

class MyRow(sqlite3.Row):
    def to_dict(self):
        return {k: v for k, v in zip(self.keys(), self)}

class Unittest(unittest.TestCase):
    def test_performance(self):
        """测试使用类定义继承的方法, 使用自定义的row factory。
        """
        data = [(i, 3.14, "abc") for i in range(1000)]
        create_sql = "CREATE TABLE test (_int INTEGER, _float REAL, _str TEXT)"
        insert_sql = "INSERT INTO test VALUES (?,?,?)"
        select_sql = "SELECT * FROM test"

        connect = sqlite3.connect(":memory:")
        connect.row_factory = sqlite3.Row
        cursor = connect.cursor()
        cursor.execute(create_sql)
        cursor.executemany(insert_sql, data)
        st = time.clock()
        for row in cursor.execute(select_sql):
            row["_int"]
        print("row factory takes %.6f" % (time.clock() - st,))
        connect.close()

        connect = sqlite3.connect(":memory:")
        connect.row_factory = MyRow
        cursor = connect.cursor()
        cursor.execute(create_sql)
        cursor.executemany(insert_sql, data)
        st = time.clock()
        for row in cursor.execute(select_sql):
            row["_int"]
        print("MyRow factory takes %.6f" % (time.clock() - st,))
        connect.close()  
        
if __name__ == "__main__":
    unittest.main() 