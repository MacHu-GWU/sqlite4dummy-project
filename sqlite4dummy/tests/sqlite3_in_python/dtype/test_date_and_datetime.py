#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from datetime import datetime, date
import sqlite3
import unittest

class Unittest(unittest.TestCase):
    def test_date_and_datetime(self):
        """在sqlite3中, datetime.date对应的数据类型时DATE, datetime.datetime
        对应的数据类型时TIMESTAMP。sqlite3自带处理时间的adapter和converter。
        而要激活这一特性, 必须定义: ``detect_types=sqlite3.PARSE_DECLTYPES``。
        """
        connect = sqlite3.connect(
            ":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = connect.cursor()

        create_table_sql = \
        """
        CREATE TABLE test
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            _date DATE,
            _datetime TIMESTAMP
        )
        """

        cursor.execute(create_table_sql)
        cursor.execute("INSERT INTO test VALUES (?,?,?)", 
                       (1, date.today(), datetime.now()))
        
        print(cursor.execute("SELECT * FROM test").fetchone())
        
if __name__ == "__main__":
    unittest.main()