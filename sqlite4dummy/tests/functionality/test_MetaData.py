#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与 :class:`sqlite4dummy.schema.MetaData` 有关的方法


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy import *
from sqlite4dummy.tests.basetest import BaseUnittest, AdvanceUnittest, DB_FILE
from datetime import datetime, date
import unittest

class MetaDataUnittest(unittest.TestCase):
    """Unittest of :class:`sqlite4dummy.schema.MetaData`.
    
    MetaData的方法的单元测试。
    """
    def setUp(self):
        self.engine = Sqlite3Engine(":memory:", autocommit=False)
        self.metadata = MetaData()
        
        self.int_ = 1
        self.float_ = 3.14
        self.str_ = r"""\/!@#$%^&*()_+-=~`|[]{}><,.'"?"""
        self.bytes_ = "abc".encode("utf-8")
        self.date_ = date(2000, 1, 1)
        self.datetime_ = datetime(2015, 10, 1, 18, 30, 0, 123)
        self.pickle_ = [1, 2, 3]
        
        self.test = Table("test", self.metadata,
            Column("_id", dtype.INTEGER, primary_key=True, nullable=False),
            Column("_int_with_default", dtype.INTEGER, default=self.int_),
            Column("_float_with_default", dtype.REAL, default=self.float_),
            Column("_str_with_default", dtype.TEXT, default=self.str_),
            Column("_bytes_with_default", dtype.BLOB, default=self.bytes_),
            Column("_date_with_default", dtype.DATE, default=self.date_),
            Column("_datetime_with_default", dtype.DATETIME, default=self.datetime_),
            Column("_pickle_with_default", dtype.PICKLETYPE, default=self.pickle_),
            
            Column("_int", dtype.INTEGER),
            Column("_float", dtype.REAL),
            Column("_str", dtype.TEXT),
            Column("_bytes", dtype.BLOB),
            Column("_date", dtype.DATE),
            Column("_datetime", dtype.DATETIME),
            Column("_pickle", dtype.PICKLETYPE),
            )
        self.metadata.create_all(self.engine)
        
        self.index = Index("test_index", self.metadata, self.test, True, False,
            self.test.c._int,
            self.test.c._float.desc(),
            self.test.c._date,
            desc(self.test.c._datetime),
            )
        self.index.create(self.engine)
        
        self.assertEqual(
            len(self.engine.execute("PRAGMA table_info(test);").fetchall()),
            15,
            )
        self.assertEqual(
            len(self.engine.execute(
                "SELECT * FROM sqlite_master "
                "WHERE type = 'index' AND sql NOT NULL;").fetchall()),
            1,
            )
    
    def tearDown(self):
        self.engine.close()
    
    def test_drop_all(self):
        """测试drop_all是否能drop所有的表。
        """
        self.assertEqual(
            len(self.engine.execute(
                "SELECT * FROM sqlite_master WHERE type = 'table';").fetchall()),
            1,
            )
        self.metadata.drop_all(self.engine)
        self.assertEqual(
            len(self.engine.execute(
                "SELECT * FROM sqlite_master WHERE type = 'table';").fetchall()),
            0,
            )
        self.assertEqual(len(self.metadata.t), 0) # 没有表了
        
    def test_str_repr(self):
#         print(self.metadata)
#         print(repr(self.metadata))
        pass
    
    def test_get_table(self):
        """测试MetaData.get_table(table)方法是否能正确获得Table。
        """
        self.assertEqual(self.metadata.get_table("test"), self.test)
        self.assertRaises(KeyError,
                          self.metadata.get_table, "not_existing_table")
    
    def test_get_index(self):
        """测试MetaData.get_index(index)方法是否能正确获得Index。
        """
        self.assertEqual(self.metadata.get_index("test_index"), self.index)
        self.assertRaises(KeyError,
                          self.metadata.get_index, "not_existing_index")
        
    def test_reflect(self):
        """测试MetaData.reflect(engine)是否能正确解析出Table, Column, Index的
        metadata, 并且解析出Column的default值。
        """
        second_metadata = MetaData()
        second_metadata.reflect(self.engine, 
                pickletype_columns=[
                "test._pickle_with_default",
                "test._pickle",
            ])
        self.assertEqual(second_metadata.get_table("test").\
                         c._int_with_default.default, self.int_)
        self.assertEqual(second_metadata.get_table("test").\
                         c._float_with_default.default, self.float_)
        self.assertEqual(second_metadata.get_table("test").\
                         c._str_with_default.default, self.str_)
        self.assertEqual(second_metadata.get_table("test").\
                         c._bytes_with_default.default, self.bytes_)
        self.assertEqual(second_metadata.get_table("test").\
                         c._date_with_default.default, self.date_)
        self.assertEqual(second_metadata.get_table("test").\
                         c._datetime_with_default.default, self.datetime_)
        self.assertEqual(second_metadata.get_table("test").\
                         c._pickle_with_default.default, self.pickle_)
        
        self.assertEqual(second_metadata.get_index("test_index").\
                         index_name, "test_index")
        self.assertEqual(second_metadata.get_index("test_index").\
                         table_name, "test")
        self.assertEqual(second_metadata.get_index("test_index").\
                         unique, True)
        self.assertEqual(second_metadata.get_index("test_index").\
                         params, self.index.params)

if __name__ == "__main__":
    unittest.main()