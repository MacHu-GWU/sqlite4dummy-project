#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example1::

    from sqlite4dummy.tests.basetest import BaseUnittest
    
    class Unittest(BaseUnittest):
        def setUp(self):
            self.connect_database()
            
        def test_all():
            "Put your test code here"
        

Import Command
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

from sqlite4dummy.tests.basetest import (
    BaseUnittest, 
    AllTypeNoData, AllTypeWithData,
)
"""
from __future__ import print_function, unicode_literals
try:
    from sqlite4dummy import *
except ImportError as e:
    print(e)
from datetime import datetime, date, timedelta
import sqlite3
import pickle
import random
import os
import sys
import string
import unittest

if sys.version_info[0] == 3:
    PK_PROTOCOL = 3
else:
    PK_PROTOCOL = 2

def random_text(length=32):
    """Generate fixed-length random string.
    
    **中文文档**
    
    生成定长随机字符串。
    """
    res = list()
    choice = string.ascii_letters + string.digits
    for _ in range(32):
        res.append(random.choice(choice))
    return "".join(res)

def random_date():
    """Generate random date object.
    
    **中文文档**
    
    生成随机日期。
    """
    return date(2000, 1, 1) + \
        timedelta(days=random.randint(0, 365*15))

def random_datetime():
    """Generate random datetime object.
    
    **中文文档**
    
    生成随机时间。
    """
    return datetime(2000, 1, 1, 0, 0, 0) + \
        timedelta(seconds=random.randint(0, 365*24*3600))

def remove():
    """Remove test database
    
    **中文文档**
    
    删除测试留下的数据库文件。
    """
    try:
        os.remove("test.sqlite3")
    except:
        pass

DB_FILE = "test.sqlite3"
MEMORY = ":memory:"
TOTAL = 1000

class BaseUnittest(unittest.TestCase):
    """纯sqlite3单元测试的基类。
    """
    def connect_database(self, DB_FILE=DB_FILE):
        remove()
        self.connect = sqlite3.connect(
            DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connect.text_factory = str
        self.cursor = self.connect.cursor()
    
    def connect_database_use_row_factory(self, DB_FILE=DB_FILE):
        remove()
        self.connect = sqlite3.connect(
            DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connect.text_factory = str
        self.connect.row_factory = sqlite3.Row
        self.cursor = self.connect.cursor()
    
    def create_all_type_table(self, has_data=True):
        create_sql = \
        """
        CREATE TABLE all_type
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            _int INTEGER,
            _float REAL,
            _str TEXT,
            _bytes BLOB,
            _date DATE,
            _datetime TIMESTAMP,
            _pickle BLOB
        )
        """
        try:
            self.cursor.execute(create_sql)
        except:
            pass
        
        if has_data:
            data = [
                (
                    i,
                    random.randint(1, 1024),
                    random.random(),
                    random_text(16),
                    random_text(16).encode("utf-8"),
                    random_date(),
                    random_datetime(),
                    pickle.dumps([random.randint(1, 1024), 
                                  random.randint(1, 1024), 
                                  random.randint(1, 1024)])
                ) for i in range(1, TOTAL+1)
            ]
            insert_sql = "INSERT INTO all_type VALUES (?,?,?,?,?,?,?,?)"
            
            self.cursor.executemany(insert_sql, data)

        self.connect.commit()

    def create_has_pk_table(self, has_data=True):
        create_sql = \
        """
        CREATE TABLE has_pk
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            _pickle BLOB
        )
        """
        try:
            self.cursor.execute(create_sql)
        except:
            pass
        
        if has_data:
            data = [
                (
                    i,
                    pickle.dumps([random.randint(1, 1024), 
                                  random.randint(1, 1024), 
                                  random.randint(1, 1024)])
                ) for i in range(1, TOTAL+1)
            ]
            insert_sql = "INSERT INTO has_pk VALUES (?,?)"
            
            self.cursor.executemany(insert_sql, data)

        self.connect.commit()

    def create_non_pk_table(self, has_data=True):
        create_sql = \
        """
        CREATE TABLE non_pk
        (
            _id INTEGER PRIMARY KEY NOT NULL,
            _str TEXT
        )
        """
        try:
            self.cursor.execute(create_sql)
        except:
            pass
        
        if has_data:
            data = [
                (
                    i,
                    random_text(16),
                ) for i in range(1, TOTAL+1)
            ]
            insert_sql = "INSERT INTO non_pk VALUES (?,?)"
            
            self.cursor.executemany(insert_sql, data)

        self.connect.commit()
        
    def tearDown(self):
        self.connect.close()
        remove()

class AdvanceUnittest(BaseUnittest):
    """本单元测试基类需要 ``:meth:`sqlite4dummy.schema.MetaData.reflect`` 方法
    正常工作。
    """
    def setUp(self):
        self.connect_database()
        self.create_all_type_table(has_data=True)
        self.create_has_pk_table(has_data=True)
        self.create_non_pk_table(has_data=True)
        self.connect.close()
        
        self.metadata = MetaData()
        self.engine = Sqlite3Engine(DB_FILE)
        self.metadata.reflect(self.engine, pickletype_columns=[
            "all_type._pickle", "has_pk._pickle",
            ])
        self.all_type = self.metadata.get_table("all_type")
        self.has_pk = self.metadata.get_table("has_pk")
        self.non_pk = self.metadata.get_table("non_pk")
        
    def tearDown(self):
        self.engine.close()
        remove()

class AdvanceUnittestNoData(BaseUnittest):
    """本单元测试基类需要 ``:meth:`sqlite4dummy.schema.MetaData.reflect`` 方法
    正常工作。
    """
    def setUp(self):
        self.connect_database(MEMORY)
        self.create_all_type_table(has_data=False)
        self.create_has_pk_table(has_data=False)
        self.create_non_pk_table(has_data=False)
        self.connect.close()
        
        self.metadata = MetaData()
        self.engine = Sqlite3Engine(DB_FILE)
        self.metadata.reflect(self.engine, pickletype_columns=[
            "all_type._pickle", "has_pk._pickle",
            ])
        self.all_type = self.metadata.get_table("all_type")
        self.has_pk = self.metadata.get_table("has_pk")
        self.non_pk = self.metadata.get_table("non_pk")
        
    def tearDown(self):
        self.engine.close()
        remove()

if __name__ == "__main__":
#     class Unittest1(BaseUnittest):
#         def setUp(self):
#             self.connect_database()
#             self.create_all_type_table(has_data=True)
#             
#         def test_all(self):
#             pass
# 
#     class Unittest2(BaseUnittest):
#         def setUp(self):
#             self.connect_database()
#             self.create_non_pk_table(has_data=True)
#             
#         def test_all(self):
#             pass
# 
#     class Unittest3(BaseUnittest):
#         def setUp(self):
#             self.connect_database()
#             self.create_has_pk_table(has_data=True)
#             
#         def test_all(self):
#             pass
#     
#     class Unittest4(AdvanceUnittest):
#         def test_all(self):
#             pass
    
    class Unittest5(AdvanceUnittestNoData):
        def test_all(self):
            pass
        
    unittest.main()