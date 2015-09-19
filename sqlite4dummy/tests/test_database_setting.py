#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本Module用于初始化生成测试要用的数据库。
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
from datetime import datetime, date, timedelta
import random

def random_text(length=32):
    """Generate fixed-length random string.
    
    **中文文档**
    
    生成定长随机字符串。
    """
    res = list()
    choice = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
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

total = 1000

def initial_all_dtype_database(needdata=False):
    """
    
    **中文文档**
    
    初始化带有所有数据类型的数据表。
    """
    metadata = MetaData()
    table = Table("test", metadata,
        Column("_id", dtype.INTEGER, primary_key=True),
        Column("_int", dtype.INTEGER),
        Column("_real", dtype.REAL),
        Column("_text", dtype.TEXT),
        Column("_date", dtype.DATE),
        Column("_datetime", dtype.DATETIME),
        Column("_blob", dtype.BLOB),
        Column("_pickle", dtype.PICKLETYPE),
        )
    engine = Sqlite3Engine(":memory:")
    metadata.create_all(engine)
    
    if needdata:
        ins = table.insert()
        data = [(
            i,
            random.randint(1, total),
            random.random() - 0.5,
            random_text(),
            random_date(),
            random_datetime(),
            random_text().encode("utf-8"),
            [1, 2, 3],
            ) for i in range(1, total+1)]
        engine.insert_many_record(ins, data)
        engine.commit()
    
    return metadata, table, engine

def initial_has_pickle(needdata=False):
    """
    
    **中文文档**
    
    初始化带有PICKLETYPE数据类型的数据表。
    """
    metadata = MetaData()
    table = Table("test", metadata,
        Column("_id", dtype.INTEGER, primary_key=True),
        Column("_list", dtype.PICKLETYPE),
        )
    engine = Sqlite3Engine(":memory:")
    metadata.create_all(engine)
    
    if needdata:
        ins = table.insert()
        data = [(
            i,
            [random.randint(1, 32), 
             random.randint(1, 32), 
             random.randint(1, 32),],
            ) for i in range(1, total+1)]
        engine.insert_many_record(ins, data)
        engine.commit()
        
    return metadata, table, engine

def initial_no_pickle(needdata=False):
    """
    
    **中文文档**
    
    初始化不带有PICKLETYPE数据类型的数据表。
    """
    metadata = MetaData()
    table = Table("test", metadata,
        Column("_id", dtype.INTEGER, primary_key=True),
        Column("_text", dtype.TEXT),
        )
    engine = Sqlite3Engine(":memory:")
    metadata.create_all(engine)
    
    if needdata:
        ins = table.insert()
        data = [(i, random_text()) for i in range(1, total+1)]
        engine.insert_many_record(ins, data)
        engine.commit()
        
    return metadata, table, engine

if __name__ == "__main__":
    import unittest
    
    class TestDatabaseUnittest(unittest.TestCase):
        def test_initial_all_dtype_database(self):
            metadata, table, engine = initial_all_dtype_database(needdata=True)
            self.assertEqual(engine.howmany(table), total)
            engine.prt_all(table)

        def test_initial_has_pickle(self):
            metadata, table, engine = initial_has_pickle(needdata=True)
            self.assertEqual(engine.howmany(table), total)
            engine.prt_all(table)

        def test_initial_no_pickle(self):
            metadata, table, engine = initial_no_pickle(needdata=True)
            self.assertEqual(engine.howmany(table), total)
            engine.prt_all(table)
            
    unittest.main()