#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
UPSERT简介:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

在Sql中, INSERT OR REPLACE和UPSERT其实是做了类似的事情。
"""

from __future__ import print_function
from sqlite4dummy.tests.basetest import BaseUnittest
import unittest
import sqlite3
import time

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database()
        
    def test_UPSERT(self):
        connect = self.connect
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
        
        update_sql = "UPDATE employee SET role = ? WHERE _id = ?"
        insert_sql = "INSERT INTO employee (role, _id) VALUES (?,?)"
        
        # naive upsert implementation
        cursor.execute(update_sql, ("manager", 3))
        if not cursor.execute("SELECT changes()").fetchone()[0]:
            cursor.execute(insert_sql, ("manager", 3))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 3").\
              fetchone()) # (3, 'manager', None)
        
        # utilize REPLACE INTO
        upsert_sql = \
        """
        REPLACE INTO employee 
            (role, _id, name) 
        VALUES 
            (  
                ?, 
                ?,
                (SELECT name FROM Employee WHERE _id = ?)
            );
        """
        
        cursor.execute(upsert_sql, ("assistant", 4, 4))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 4").\
              fetchone())
    
        # Performance test
        st = time.clock()
        for i in range(1000):
            cursor.execute(update_sql, ("manager", 3))
            if not cursor.execute("SELECT changes()").fetchone()[0]:
                cursor.execute(insert_sql, ("manager", 3))
        print("update or insert takes %.6f..." % (time.clock() - st))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 2").fetchone())
        
        st = time.clock()
        for i in range(1000):
            cursor.execute(upsert_sql, ("assistant", 4, 4))
        print("upsert takes %.6f..." % (time.clock() - st))
        print(cursor.execute("SELECT * FROM employee WHERE _id = 2").fetchone())
        
if __name__ == "__main__":
    unittest.main()