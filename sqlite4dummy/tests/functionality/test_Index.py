#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与 :class:`sqlite4dummy.schema.Index` 有关的方法。


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
from sqlite4dummy.tests.basetest import *
import unittest

class IndexUnittest(AdvanceUnittestNoData):
    """Unittest of :class:`sqlite4dummy.schema.Index`.
    """
    def test_display_index(self):
        """测试Index对象能否被成功初始化。
        """
        all_type = self.all_type
        index = Index("muticol_index", self.metadata, 
            [all_type.c._int, all_type.c._float.desc(),
             "_date", "all_type._datetime DESC"],
            table_name=all_type, unique=True, skip_validate=False)
        self.assertEqual(str(index), "muticol_index")
        
#         print(repr(index))
#         print(index.create_index_sql)
        
    def test_create_and_delete_index(self):
        """测试Index被创建前后, sqlite_master中的关于index信息的变化。
        """
        all_type = self.all_type
         
        # 看看创建前总共有多少个Index, 应该是0个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # no index
         
        # 创建一个Index
        index = Index("_int_index", self.metadata, [all_type.c._int])
        index.create(self.engine)
          
        # 看看创建后总共有多少个Index, 应该是1个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 1) # has one index
         
        # 检查Index的名字和表的名字
        self.assertEqual(res[0][1], "_int_index") # read index name
        self.assertEqual(res[0][2], "all_type") # read index table name
         
        # 删掉一个Index
        index.drop(self.engine)
         
        # 看看删除后总共有多少个Index, 应该是0个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # has no index
        
    def test_create_all_and_drop_all(self):
        all_type = self.all_type
         
        # 看看创建前总共有多少个Index, 应该是0个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # no index
         
        # 创建一个Index
        index = Index("_int_index", self.metadata, [all_type.c._int])
        index = Index("_float_index", self.metadata, [all_type.c._float])
        self.metadata.create_all_index(self.engine)
          
        # 看看创建后总共有多少个Index, 应该是1个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 2) # has two index
         
        # 删掉一个Index
        self.metadata.drop_all_index(self.engine)

        # 看看删除后总共有多少个Index, 应该是0个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # has no index
        
if __name__ == "__main__":
    unittest.main()