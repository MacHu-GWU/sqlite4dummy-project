#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from pprint import pprint as ppt
import unittest
import sqlite3
import pickle
    
class Unittest(unittest.TestCase):
    def test_UPDATE(self):
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
            profile BLOB
        )
        """
        cursor.execute(create_table_sql)
        
        data = [(1, "coder", "John", None), (2, "sales", "Mike", None)]
        cursor.executemany("INSERT INTO employee VALUES (?,?,?,?)", data)
        
        cursor.execute(
            "UPDATE employee SET role = ?, profile = ? WHERE _id = ?",
            ("manager", pickle.dumps({"age": 32}), 2))
        
        ppt(cursor.execute("SELECT * FROM employee").fetchall())
    
if __name__ == "__main__":
    unittest.main()