#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与 :class:`sqlite4dummy.schema.Index` 有关的方法。


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
import unittest

class IndexUnittest(unittest.TestCase):
    """Unittest of :class:`sqlite4dummy.schema.Index`.
    """
    def setUp(self):
        self.metadata = MetaData()
        self.product = Table("product", self.metadata,
            Column("_id", dtype.INTEGER, primary_key=True),
            Column("product_name", dtype.TEXT),
            Column("price", dtype.INTEGER),
            )
        self.engine = Sqlite3Engine(":memory:")
        self.metadata.create_all(self.engine)
        
        records = [
            (1, "pear", 0.52),
            (2, "apple", 0.34),
            (3, "banana", 0.68),
            ]
        ins = self.product.insert()
        self.engine.insert_many_record(ins, records)
    
    def test_display_index(self):
        """测试Index对象能否被成功初始化。
        """
        product = self.product
        index = Index("name_index", self.metadata, product, True, False,
                      product.c.product_name, product.c.price.desc())
        self.assertEqual(str(index), "name_index")
        print(repr(index))
        print(index.create_index_sql)
        
    def test_create_index(self):
        """测试Index被创建前后, sqlite_master中的关于index信息的变化。
        """
        product = self.product
        
        # 看看创建前总共有多少个Index, 应该是0个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # no index
        
        # 创建一个Index
        index = Index("name_index", self.metadata, product, True,
                      product.c.product_name, product.c.price.desc())
        index.create(self.engine)
         
        # 看看创建后总共有多少个Index, 应该是1个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 1) # has one index
        
        # 检查Index的名字和表的名字
        self.assertEqual(res[0][1], "name_index") # read index name
        self.assertEqual(res[0][2], "product") # read index table name
        
        # 删掉一个Index
        index.drop(self.engine)
        
        # 看看删除后总共有多少个Index, 应该是1个
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # has no index
        
if __name__ == "__main__":
    unittest.main()