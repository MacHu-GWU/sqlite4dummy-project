#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与:class:`sqlite4dummy.schema.Index`有关的方法


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
import unittest

class IndexUnittest(unittest.TestCase):
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
        index = Index("name_index", self.metadata,
                      product.c.product_name, product.c.price.desc())
        self.assertEqual(str(index), "name_index")
        print(repr(index))
        
    def test_create_index(self):
        """测试Index被创建前后, sqlite_master中的关于index信息的变化。
        """
        product = self.product
        
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # no index
        
        index = Index("name_index", self.metadata,
                      product.c.product_name, product.c.price.desc())
        index.create(self.engine)
        
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()

        self.assertEqual(len(res), 1) # has one index
        self.assertEqual(res[0][1], "name_index") # read index name
        self.assertEqual(res[0][2], "product") # read index table name
        
        index.drop(self.engine)
        res = self.engine.execute("SELECT * FROM sqlite_master "
                                  "WHERE type = 'index';").fetchall()
        self.assertEqual(len(res), 0) # has no index
        
if __name__ == "__main__":
    unittest.main()