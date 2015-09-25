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
        print(self.movie.create_table_sql)
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
        """
        """
        ins = self.movie.insert()
        for record in self.records:
            self.engine.insert_record(ins, record)
         
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
             
    def test_insert_row(self):
        """
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
        self.engine.insert_row_stream(ins, 
            (row for row in self.rows))
        self.engine.insert_row_stream(ins, 
            (row for row in self.rows))
        self.assertEqual(
            len(list(self.engine.execute("SELECT * FROM movie"))), 4)
        
class InsertPerformanceUnittest(unittest.TestCase):
    """
 
    **中文文档**
     
    测试分别在批量插入时, 有/无主键冲突的情况下, 有/无 pickletype的情况下, 
    sqlite4dummy的性能是否高于sqlalchemy。
     
    1. not conflict + has pickle type, sqlalchemy胜出, 用时是另一个的2/3
    2. not conflict + no pickle type, 两者不相上下
    3. conflict + has pickle type, sqlite4dummy胜出, 用时是另一个的2/3
    4. conflict + no pickle type, sqlite4dummy胜出, 用时是另一个的1/35
     
    结论: 对于有primary_key重复的情况下, 使用bulk insert一定要使用原生API,
    而不要使用sqlalchemy.
    """
    def setUp(self):
        self.metadata = MetaData()
        self.has_pk = Table("has_pk", self.metadata,
            Column("_id", dtype.INTEGER, primary_key=True),
            Column("_list", dtype.PICKLETYPE),
            )
        self.no_pk = Table("no_pk", self.metadata,
            Column("_id", dtype.INTEGER, primary_key=True),
            Column("_text", dtype.TEXT),
            )
        self.engine = Sqlite3Engine(":memory:")
        self.metadata.create_all(self.engine)
         
        self.sa_metadata = sqlalchemy.MetaData()
        self.sa_has_pk = sqlalchemy.Table("has_pk", self.sa_metadata,
            sqlalchemy.Column("_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("_list", sqlalchemy.PickleType),
            )
        self.sa_no_pk = sqlalchemy.Table("no_pk", self.sa_metadata,
            sqlalchemy.Column("_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("_text", sqlalchemy.TEXT),
            )
        self.sa_engine = sqlalchemy.create_engine("sqlite://", echo=False)
        self.sa_metadata.create_all(self.sa_engine)
         
    def test_sqlite4dummy_vs_sqlalchemy_in_bulk_insert_has_pk_non_repeat(self):
        print("\nNo conflict and has PickleType")
        complexity = 1000
           
        records = [(i, [1,2,3]) for i in range(complexity)]
        rows = [{"_id": i, "_list": [1, 2, 3]} for i in range(complexity)]
           
        ins = self.has_pk.insert()
        st = time.clock()
        self.engine.insert_many_record(ins, records)
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.engine.execute("SELECT * FROM has_pk"))))
         
        ins = self.sa_has_pk.insert()
        st = time.clock()
        self.sa_engine.execute(ins, rows)
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.sa_engine.execute("SELECT * FROM has_pk"))))
         
    def test_sqlite4dummy_vs_sqlalchemy_in_bulk_insert_no_pk_non_repeat(self):
        print("\nNo conflict and non PickleType")
        complexity = 1000
           
        records = [(i, "hello world") for i in range(complexity)]
        rows = [{"_id": i, "_list": "hello world"} for i in range(complexity)]
           
        ins = self.no_pk.insert()
        st = time.clock()
        self.engine.insert_many_record(ins, records)
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.engine.execute("SELECT * FROM no_pk"))))
         
        ins = self.sa_no_pk.insert()
        st = time.clock()
        self.sa_engine.execute(ins, rows)
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.sa_engine.execute("SELECT * FROM no_pk"))))
         
    def test_sqlite4dummy_vs_sqlalchemy_in_bulk_insert_has_pk_repeative(self):
        print("\nPrimary key conflict and has PickleType")
        complexity = 1000
         
        records = [(random.randint(1, complexity), 
                    [1, 2, 3]) for i in range(complexity)]
        rows = [{"_id": random.randint(1, complexity), 
                 "_list": [1, 2, 3]} for i in range(complexity)]
         
        ins = self.has_pk.insert()
        st = time.clock()
        self.engine.insert_many_record(ins, records)
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.engine.execute("SELECT * FROM has_pk"))))
         
        ins = self.sa_has_pk.insert()
        st = time.clock()
        for row in rows:
            try:
                self.sa_engine.execute(ins, row)
            except:
                pass
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.sa_engine.execute("SELECT * FROM has_pk"))))
 
    def test_sqlite4dummy_vs_sqlalchemy_in_bulk_insert_no_pk_repeative(self):
        print("\nPrimary key conflict and non PickleType")
        complexity = 1000
          
        records = [(random.randint(1, complexity), 
                    "hello world") for i in range(complexity)]
        rows = [{"_id": random.randint(1, complexity), 
                 "_list": "hello world"} for i in range(complexity)]
          
        ins = self.no_pk.insert()
        st = time.clock()
        self.engine.insert_many_record(ins, records)
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.engine.execute("SELECT * FROM no_pk"))))
          
        ins = self.sa_no_pk.insert()
        st = time.clock()
        for row in rows:
            try:
                self.sa_engine.execute(ins, row)
            except:
                pass
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print(len(list(self.sa_engine.execute("SELECT * FROM no_pk"))))

if __name__ == "__main__":
    unittest.main()