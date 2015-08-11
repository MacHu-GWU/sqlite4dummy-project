#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlite4dummy import *
from datetime import datetime, date
from pprint import pprint as ppt
import sqlalchemy
import unittest
import time
import random

class SelectUnittest(unittest.TestCase):
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
            "_id": 338564,
            "title": "Fantastic Four",
            "year": 2015,
            "release_date": date(2015, 8, 7),
            "create_time": datetime.now(),
            "length": 100,
            "rate": 4.0,
            "tag": ["Action", "Adventure", "Sci-Fi"],
        },
        {
            "_id": 381681,
            "title": "Pixels",
            "year": 2015,
            "release_date": date(2015, 7, 24),
            "create_time": datetime.now(),
            "length": 106,
            "rate": 5.5,
            "tag": ["Action", "Comedy", "Sci-Fi"],
        },
        {
            "_id": 1502712,
            "title": "Before Sunset",
            "year": 2004,
            "release_date": date(2004, 7, 30),
            "create_time": datetime.now(),
            "length": 80,
            "rate": 8.1,
            "tag": ["Drama", "Romance"],
        },
        {
            "_id": 2120120,
            "title": "Infernal Affairs",
            "year": 2002,
            "release_date": date(2002, 12, 12),
            "create_time": datetime.now(),
            "length": 101,
            "rate": 8.1,
            "tag": ["Crime", "Mystery", "Thriller"],
        },
        ]
        self.records = [tuple([doc[name] for name in columns]) for doc in data]
        self.docs = data
        ins = self.movie.insert()
        self.engine.insert_many_record(ins, self.records)
        self.engine.commit()

    # SELECT SQL syntax
#     def test_select_sql(self):
#         s = Select(self.movie.all)
#         print("{:=^100}".format("select all formatted sql:"))
#         print(s.sql)
#           
#     def test_select_where(self):
#         movie = self.movie
#         s = Select(movie.all).where(
#                 movie.c.year >= 2005, movie.c.rate >= 6.2)
#         print("{:=^100}".format("select where formatted sql:"))
#         print(s.sql)
#     
#     def test_select_order_by(self):
#         movie = self.movie
#         s = Select(self.movie.all).order_by(
#             desc(movie.c.rate), movie.c.title)
#         print("{:=^100}".format("select order by formatted sql:"))
#         print(s.sql)
#     
#     def test_select_limit(self):
#         movie = self.movie
#         s = Select(movie.all).limit(10)
#         print("{:=^100}".format("select limit formatted sql:"))
#         print(s.sql)
#     
#     def test_select_offset(self):
#         movie = self.movie
#         s = Select(movie.all).offset(5)
#         print("{:=^100}".format("select offset formatted sql:"))
#         print(s.sql)
#     
#     def test_select_distinct(self):
#         movie = self.movie
#         s = Select([movie.c._id, movie.c.title]).distinct()
#         print("{:=^100}".format("select distinct formatted sql:"))
#         print(s.sql)
#     
#     def test_select_count(self):
#         movie = self.movie
#         r = func.count(movie.c._id)
#         s = Select([func.count(movie.c._id)])
#         print("{:=^100}".format("select count formatted sql:"))
#         print(s.sql)
# 
#     def test_select_from(self):
#         movie = self.movie
#         r = func.count(movie.c._id)
#         s = Select([func.count(movie.c._id)]).\
#             select_from(Select(movie.all).\
#                         where(movie.c.rate >= 6.0))
#         print("{:=^100}".format("select from formatted sql:"))
#         print(s.sql)
        
    # query with engine
    def test_select_record_in_engine(self):
        """测试SELECT语句是否正常工作
        """
        movie = self.movie
        results = list(self.engine.select(Select(movie.all), return_tuple=True))
        self.assertListEqual(results, self.records)
     
    def test_select_count_in_engine(self):
        """测试SELECT COUNT(xxx) FROM ...是否正常工作
        """
        movie = self.movie
        results = list(self.engine.select(Select([func.count(movie.c._id)])))
        self.assertEqual(results[0][0], 4)
    
    def test_select_limit_in_engine(self):
        """测试SELECT ... FROM ... LIMIT ... 是否正常工作。
        """
        movie = self.movie
        results = list(self.engine.select(Select(movie.all).limit(2)))
        self.assertEqual(len(results), 2)

    def test_select_offset_in_engine(self):
        """测试SELECT ... FROM ... LIMIT ... OFFSET ...是否正常工作。
        """
        movie = self.movie
        results = list(self.engine.select(Select(movie.all).\
                                          limit(10).offset(1)))
        self.assertEqual(len(results), 3)

    def test_select_distinct_in_engine(self):
        """测试SELECT DISTINCT是否正常工作。
        (有两部影片rate重复)
        """
        movie = self.movie
        results = list(self.engine.select(Select([movie.c.rate]).distinct()))
        self.assertEqual(len(results), 3)
      
    def test_select_from_in_engine(self):
        """测试SELECT COUNT(_id) FROM (SELECT * FROM test WHERE ...)嵌套SELECT
        查询是否正常工作。
        """
        movie = self.movie
        s = Select([func.count(movie.c._id)]).\
            select_from(Select(movie.all).\
                        where(movie.c.rate >= 6.0))
        results = list(self.engine.select(s))
        self.assertEqual(results[0][0], 2)
 
    def test_and_or_asc_desc_expression_in_engine(self):
        """测试and, or, asc, desc sql表达式是否正常工作。
        """
        movie = self.movie
        s = Select(movie.all).\
            where(and_(movie.c.year >=2000, movie.c.rate >= 6.0)).\
            order_by(desc(movie.c.rate), asc(movie.c.title))
        results = list(self.engine.select(s))
        self.assertEqual(results[0][0], 1502712)
        self.assertEqual(results[1][0], 2120120)

    # returns Row object
    def test_select_row_in_engine(self):
        """测试SELECT语句在跟Sqlite3Engine.select_row()搭配使用时是否正常工作。
        """
        movie = self.movie
        results = list(self.engine.select_row(Select(movie.all)))
        for row, doc in zip(results, self.docs):
            self.assertDictEqual(row.to_dict(), doc)
    
    def test_select_row_with_count_in_engine(self):
        """
        """
        movie = self.movie
        results = list(self.engine.select_row(Select([func.count(movie.c._id)])))
        self.assertDictEqual(results[0].to_dict(), {"_count": 4})
    
    # return dict like DataFrame
    def test_select_dict_in_engine(self):
        movie = self.movie
        results = self.engine.select_dict(Select(movie.all))
        self.assertEqual(len(results), 8)

    def test_select_dict_with_count_in_engine(self):
        movie = self.movie
        results = self.engine.select_dict(Select([func.count(movie.c._id)]))
        self.assertEqual(results["_count"][0], 4)
        
unittest.main()