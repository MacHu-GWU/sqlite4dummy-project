#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与 :class:`sqlite4dummy.schema.Insert` 有关的功能。


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy import *
from datetime import datetime, date
from pprint import pprint as ppt
import sqlalchemy
import unittest
import time
import random

class InsertUnittest(unittest.TestCase):
    """Unittest of :class:`sqlite4dummy.schema.Insert`.
    """
    def setUp(self):
        self.metadata = MetaData()
        self.movie = Table("movie", self.metadata, 
            Column("_id", dtype.INTEGER, primary_key=True),
            Column("title", dtype.TEXT, nullable=False, 
                   default="UnknownTitle"),
            Column("year", dtype.INTEGER),
            Column("release_date", dtype.DATE),
            Column("create_time", dtype.DATETIME),
            Column("length", dtype.INTEGER),
            Column("rate", dtype.REAL),
            Column("tag", dtype.PICKLETYPE),
        )
        
        self.engine = Sqlite3Engine(":memory:")
        self.metadata.create_all(self.engine)
        columns = ["_id", "title", "year", "release_date", "create_time",
                   "length", "rate", "tag"]
        data = [
            {
                "_id": 1502712,
                "title": "Fantastic Four",
                "year": 2015,
                "release_date": date(2015, 8, 7),
                "create_time": datetime.now(),
                "length": 100,
                "rate": 4.0,
                "tag": ["Action", "Adventure", "Sci-Fi"],
            },
            {
                "_id": 2120120,
                "title": "Pixels",
                "year": 2015,
                "release_date": date(2015, 7, 24),
                "create_time": datetime.now(),
                "length": 106,
                "rate": 5.5,
                "tag": ["Action", "Comedy", "Sci-Fi"],
            },
            {
                "_id": 381681,
                "title": "Before Sunset",
                "year": 2004,
                "release_date": date(2004, 7, 30),
                "create_time": datetime.now(),
                "length": 80,
                "rate": 8.1,
                "tag": ["Drama", "Romance"],
            },
            {
                "_id": 338564,
                "title": "Infernal Affairs",
                "year": 2002,
                "release_date": date(2002, 12, 12),
                "create_time": datetime.now(),
                "length": 101,
                "rate": 8.1,
                "tag": ["Crime", "Mystery", "Thriller"],
            },
        ]
        self.rows = list()
        for doc in data:
            row = Row(columns, tuple([doc[name] for name in columns]))
            self.rows.append(row)
        self.records = [tuple([doc[name] for name in columns]) for doc in data]

    def test_insert_sql(self):
        """测试是否能正确地生成Insert Sql语句
        """
        ins = self.movie.insert()
        ins.sql_from_record()
        self.assertEqual(ins.sql, 
            "INSERT INTO\tmovie\nVALUES\n\t(?, ?, ?, ?, ?, ?, ?, ?);")
        ins.sql_from_row(self.rows[0])
        self.assertEqual(ins.sql, 
            "INSERT INTO\tmovie\n\t(_id, title, year, release_date, "
            "create_time, length, rate, tag)"
            "\nVALUES\n\t(?, ?, ?, ?, ?, ?, ?, ?);")

    def test_insert_record(self):
        """测试插入一条record。
        """
        ins = self.movie.insert()
        for record in self.records:
            self.engine.insert_record(ins, record)
         
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
             
    def test_insert_row(self):
        """测试插入一条Row。
        """
        ins = self.movie.insert()
        for row in self.rows:
            self.engine.insert_row(ins, row)
             
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
 
    def test_insert_many_record(self):
        """测试批量插入record功能, 自动处理异常。
        """
        ins = self.movie.insert()
        # insert same data set two times
        self.engine.insert_many_record(ins, self.records)
        self.engine.insert_many_record(ins, self.records)
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
 
    def test_insert_many_row(self):
        """测试批量插入Row功能, 自动处理异常。
        """
        ins = self.movie.insert()
        # insert same data set two times
        self.engine.insert_many_row(ins, self.rows)
        self.engine.insert_many_row(ins, self.rows)
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
     
    def test_insert_record_stream(self):
        """测试以生成器模式批量插入record功能, 自动处理异常。
        """
        ins = self.movie.insert()
        # insert same data set two times
        self.engine.insert_record_stream(ins, 
            (record for record in self.records))
        self.engine.insert_record_stream(ins, 
            (record for record in self.records))
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
 
    def test_insert_row_stream(self):
        """测试以生成器模式批量插入row功能, 自动处理异常。
        """
        ins = self.movie.insert()
        # insert same data set two times
        self.engine.insert_row_stream(ins, (row for row in self.rows))
        self.engine.insert_row_stream(ins, (row for row in self.rows))
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
        

if __name__ == "__main__":
    unittest.main()