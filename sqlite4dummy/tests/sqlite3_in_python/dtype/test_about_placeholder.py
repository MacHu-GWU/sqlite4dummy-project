#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
placeholder是DBAPI中用于占用一个SQL Statement中数据值的位置, 然后将该位置根据编译
器对输入的值进行编译。

在CREATE语句中的DEFAULT部分无法使用placeholder。而在
Insert, Update, Replace, Select, Delete中都可以使用placeholder。
"""

from __future__ import print_function
from sqlite4dummy.tests.basetest import BaseUnittest
from datetime import datetime, date
import sqlite3
import pickle

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database()
        
    def test_all(self):
        connect, cursor = self.connect, self.cursor
        
        # 创建表
        create_table_sql = \
        """
        CREATE TABLE test
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            _int INTEGER,
            _float REAL,
            _str TEXT,
            _bytes BLOB,
            _date DATE,
            _datetime TIMESTAMP 
        )
        """
        cursor.execute(create_table_sql)
        
        # 插入一些测试数据
        int_ = 1024
        float_ = 3.14
        str_ = r"""\/!@#$%^&*()_+-=~`|[]{}><,.'"?"""
        bytes_ = pickle.dumps([1,2,3], protocol=2)
        date_ = date(2000, 1, 1)
        datetime_ = datetime(2015, 10, 1, 18, 30, 0)
        record = (int_, float_, str_, bytes_, date_, datetime_)
        
        # 在Insert语句中使用?
        insert_sql = \
        """
        INSERT INTO test
        (_int, _float, _str, _bytes, _date, _datetime)
        VALUES
        (?,?,?,?,?,?)
        """
        cursor.execute(insert_sql, record)
        
        # 在Select语句中使用?
        select_sql = \
        """
        SELECT * FROM test
        WHERE
        _int = ? AND _float = ? AND _str = ? 
        AND _bytes = ? AND _date = ? AND _datetime= ?
        """
        print(cursor.execute(select_sql, record).fetchone())
        
        # 在复杂一些的Select语句中使用?
        select_sql = \
        """
        SELECT * FROM test
        WHERE (_int + _float) >= ?
        """
        print(cursor.execute(select_sql, (1000, )).fetchone())

        # 在涉及到SQL函数的Select语句中使用?
        select_sql = \
        """
        SELECT * FROM test
        WHERE _int in (?,?,?)
        """
        print(cursor.execute(select_sql, (256, 512, 1024)).fetchone())

        # 在Update语句中使用?
        update_sql = \
        """
        UPDATE test SET _int = _int + ?
        """
        cursor.execute(update_sql, (1000,))
        
        select_sql = \
        """
        SELECT * FROM test
        WHERE _int = ?
        """
        print(cursor.execute(select_sql, (2024, )).fetchone())
        
if __name__ == "__main__":
    import unittest
    unittest.main()