#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import unittest
import sqlite3
import time

class Unittest(unittest.TestCase):
    def test_function(self):
        """测试row_factory的功能。
        
        sqlite3.Row是最高性能的C实现。
        """
        data = [(i, 3.14, "abc") for i in range(1)]
        create_sql = "CREATE TABLE test (_int INTEGER, _float REAL, _str TEXT)"
        insert_sql = "INSERT INTO test VALUES (?,?,?)"
        select_sql = "SELECT * FROM test"
        
        connect = sqlite3.connect(":memory:")
        connect.row_factory = sqlite3.Row # 用户可以使用connect.row_factory = None关闭该功能
        cursor = connect.cursor()
        cursor.execute(create_sql)
        cursor.executemany(insert_sql, data)

        for row in cursor.execute(select_sql):
            print(row["_int"], row["_float"], row["_str"])
            print(row[0], row[1], row[2])
            print(row.keys())
            print(list(row))

    def test_performance(self):
        """record, row_factory, dict_factory的性能比较。
        
        record最快, row_factory其次, 手动实现的dict factory最慢。
        """
        data = [(i, 3.14, "abc") for i in range(1000)]
        create_sql = "CREATE TABLE test (_int INTEGER, _float REAL, _str TEXT)"
        insert_sql = "INSERT INTO test VALUES (?,?,?)"
        select_sql = "SELECT * FROM test"
        
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
        cursor.execute(create_sql)
        cursor.executemany(insert_sql, data)
        st = time.clock()
        for record in cursor.execute(select_sql):
            record[0]
        print("record takes %.6f" % (time.clock() - st,))
        connect.close()

        connect = sqlite3.connect(":memory:")
        connect.row_factory = sqlite3.Row # 用户可以使用connect.row_factory = None关闭该功能
        cursor = connect.cursor()
        cursor.execute(create_sql)
        cursor.executemany(insert_sql, data)
        st = time.clock()
        for row in cursor.execute(select_sql):
            row["_int"]
        print("row factory takes %.6f" % (time.clock() - st,))
        connect.close()
        
        columns = ["_int", "_float", "_str"]
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
        cursor.execute(create_sql)
        cursor.executemany(insert_sql, data)
        st = time.clock()
        for record in cursor.execute(select_sql):
            doc = dict()
            for key, value in zip(columns, record):
                doc[key] = value
            doc["_int"]
        print("dict factory takes %.6f" % (time.clock() - st,))
        connect.close()

if __name__ == "__main__":
    unittest.main() 