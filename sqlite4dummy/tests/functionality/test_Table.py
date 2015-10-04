#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与:class:`sqlite4dummy.schema.Table`有关的方法


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy import *
from pprint import pprint as ppt
import unittest

class TableUnittest(unittest.TestCase):
    """Unittest of :class:`sqlite4dummy.schema.Table`.
    """
    def setUp(self):
        self.table = Table("employee", MetaData(),
            Column("_id", dtype.TEXT, primary_key=True),
            Column("name", dtype.TEXT, nullable=False),
            Column("date_of_birth", dtype.DATE, nullable=False),
            Column("height", dtype.REAL),
            Column("profile", dtype.PICKLETYPE, default={
                                    "role": list(), 
                                    "department": None,
                                    }),
        )
    
    def test_str(self):
        """测试 __str__ 方法。
        """
        self.assertEqual(self.table.table_name, "employee")
        
    def test_repr(self):
        """测试 __repr__ 方法。
        """
#         print(repr(self.table))
        pass
    
    def test_iter(self):
        """测试对Table的for循环
        """
        for column in self.table:
            self.assertIsInstance(column, Column) # 检查是否是Column对象
            
    def test_get_column(self):
        """测试从Table中取Column的方法。
        """
        self.assertIsInstance(self.table.get_column("_id"), Column)
        self.assertEqual(self.table.get_column("_id").column_name, "_id")
        
        self.assertRaises(AttributeError, self.table.get_column, "NOTHING")
            
    def test_column_name_full_name_and_table_name(self):
        """测试初始化Table之后是否会将: Column.table_name, Column.full_name
        和table_name绑定上, 而 Column.column_name 是否依旧保持不变。
        """
        for column in self.table:
            # 检查column的full_name是否已经是table_name.column_name的形式
            self.assertEqual(str(column), "%s.%s" % (
                self.table.table_name, column.column_name))
            # 检查column的column_name是否真的没有带上table_name
            self.assertFalse(self.table.table_name in column.column_name)
            # 检查column中的table_name是否也被绑定上
            self.assertEqual(column.table_name, self.table.table_name)
            
        for column in self.table.all:
            # 检查column的full_name是否已经是table_name.column_name的形式
            self.assertEqual(str(column), "%s.%s" % (
                self.table.table_name, column.column_name))
            # 检查column的column_name是否真的没有带上table_name
            self.assertFalse(self.table.table_name in column.column_name)
            # 检查column中的table_name是否也被绑定上
            self.assertEqual(column.table_name, self.table.table_name)
    
    def test_create_table_sql(self):
        """测试生成创建Table所需的SQL语句。
        """
#         print(self.table.create_table_sql)
        pass
if __name__ == "__main__":
    unittest.main()