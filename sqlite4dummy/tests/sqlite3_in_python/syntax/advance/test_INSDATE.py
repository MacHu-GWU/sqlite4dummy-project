#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INSERT AND UPDATE (INSDATE)简介
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

尝试进行insert, 如果数据主键冲突, 那么则update除主键以外的其他数据。

问题: 当被insert的不是所有的列时, 只会更新insert的相关列, 其他列的值会被保留。
如果被update的值跟constrain有冲突, 则会抛出异常。

与之相似的是UPSERT:

尝试进行update, 如果数据不存在, 那么则insert一条进去。
问题: 被update的可能仅仅只有几列, 有可能用户的原意是只修改其中几列。但由于如果
被update的行没有被找到, 那么被insert的也仅仅是几列。所以当其他列有NOT NULL限制
时, 会抛出异常。
"""

from __future__ import print_function
from pprint import pprint as ppt
import unittest
import sqlite3
import time

class Unittest(unittest.TestCase):
    def test_UPSERT_implementation(self):
        """测试Insdate在sqlite中的正确实现。
        """
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
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
        
        upsert_sql = \
        """
        REPLACE INTO employee 
            (_id, role, name) 
        VALUES 
            (  
                ?, 
                ?,
                (SELECT name FROM Employee WHERE _id = ?)
            );
        """
        cursor.execute(upsert_sql, (2, "manager", 2))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 2").fetchone())
        
    def test_UPSERT_performance(self):
        """测试Insert and Update的两种实现的性能比较。
        
        结论: 利用REPLACE INTO employee实现。
        """
        connect = sqlite3.connect(":memory:")
        cursor = connect.cursor()
        # http://www.w3schools.com/sql/sql_create_table.asp
        create_table_sql = \
        """
        CREATE TABLE employee
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            role TEXT,
            name TEXT,
            age INTEGER,
            height REAL
        )
        """
        cursor.execute(create_table_sql)
        
        data = [(1, "coder", "John", 30, 1.67), (2, "sales", "Mike", 45, 1.77)]
        cursor.executemany("INSERT INTO employee VALUES (?,?,?,?,?)", data)
        
        insert_sql = "INSERT INTO employee (_id, role) VALUES (?,?)"
        update_sql = "UPDATE employee SET _id = ?, role = ? WHERE _id = ?"        
        

        insdate_sql= \
        """
        REPLACE INTO employee 
            (_id, role, name, age, height) 
        VALUES 
            (  
                ?, 
                ?,
                (SELECT name FROM Employee WHERE _id = ?),
                (SELECT age FROM Employee WHERE _id = ?),
                (SELECT height FROM Employee WHERE _id = ?)
            );
        """
        
        # Performance test
        st = time.clock()
        for i in range(1000):
            try:
                cursor.execute(insert_sql, (2, "manager"))
            except sqlite3.IntegrityError:
                cursor.execute(update_sql, (2, "manager", 2))
        print("insert and update takes %.6f..." % (time.clock() - st))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 2").fetchone())
        
        st = time.clock()
        for i in range(1000):
            cursor.execute(insdate_sql, (2, "CFO", 2, 2, 2))
        print("upsert takes %.6f..." % (time.clock() - st))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 2").fetchone())
        
if __name__ == "__main__":
    unittest.main()