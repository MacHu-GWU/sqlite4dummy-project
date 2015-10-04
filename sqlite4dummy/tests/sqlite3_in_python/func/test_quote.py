#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
QUOTE函数返回列所对应的原生SQL表达式。

Ref: https://www.sqlite.org/lang_corefunc.html
"""

from sqlite4dummy.tests.basetest import BaseUnittest
from datetime import datetime, date
import unittest

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database()
        
    def test_all(self):
        cursor = self.cursor
        
        # insert some data
        create_sql = \
        """
        CREATE TABLE test 
        (
            _id INTEGER PRIMARY KEY, 
            _str TEXT,
            _bytes BLOB,
            _date DATE,
            _datetime TIMESTAMP
        )
        """
        insert_sql = "INSERT INTO test VALUES (?,?,?,?,?)"
        cursor.execute(create_sql)
        cursor.execute(insert_sql, 
            (
                1,
                r"""abc`~!@#$%^&*()_+-={}[]|\:;'"<>,.?/""",
                "Hello World".encode("utf-8"),
                date.today(),
                datetime.now(),
            )
        )
        select_sql = \
        """
        SELECT
            quote(_str), quote(_bytes), quote(_date), quote(_datetime)
        FROM
            test
        """
        print(cursor.execute(select_sql).fetchone())
    
    def test_usage(self):
        """QUOTE可以用来获得原生SQL表达式。
        """
        cursor = self.cursor
        
        print(cursor.execute("SELECT QUOTE(?)", 
            (r"""abc`~!@#$%^&*()_+-={}[]|\:;'"<>,.?/""", )).fetchone())
        print(cursor.execute("SELECT QUOTE(?)", 
            ("Hello World".encode("utf-8"), )).fetchone())
        print(cursor.execute("SELECT QUOTE(?)", 
            (date.today(), )).fetchone())
        print(cursor.execute("SELECT QUOTE(?)", 
            (datetime.now(), )).fetchone())
        
if __name__ == "__main__":
    unittest.main()