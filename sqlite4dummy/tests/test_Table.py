#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与:class:`sqlite4dummy.schema.Table`有关的方法


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from sqlite4dummy import *
from pprint import pprint as ppt
import unittest

class TableUnittest(unittest.TestCase):
    """Table的方法的单元测试。
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
            Column("memo", dtype.TEXT, default="This guy is lazy, no memo."),
            )
    
    def test_str(self):
        self.assertEqual(self.table.table_name, "employee")
        
    def test_repr(self):
#         print(repr(self.table))
        pass
    
    def test_iter(self):
        """测试对Table的for循环
        """
        for column in self.table:
            self.assertEqual(column.column_name, 
                 self.table.get_column(column.column_name).column_name)
    
    def test_get_column(self):
        """测试从Table中取Column的方法。
        """
        self.assertEqual(self.table.get_column("_id").column_name, "_id")
        self.assertRaises(AttributeError, self.table.get_column, "NOTHING")
        
        
    def test_bind_column(self):
        """测试初始化Table之后是否会将Column.table_name, Column.full_name
        绑定上。
        """
        for column in self.table:
            self.assertEqual(column.table_name, self.table.table_name)
            self.assertEqual(column.full_name,
                             "%s.%s" % (self.table, column.column_name))
        
        for column in self.table.all:
            self.assertEqual(column.full_name,
                             "%s.%s" % (self.table, column.column_name))
            
if __name__ == "__main__":
    unittest.main()