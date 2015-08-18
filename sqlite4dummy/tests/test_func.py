#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与SQL函数有关的功能


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
from sqlite4dummy.tests.test_database_setting import (
    random_text, random_date, random_datetime, 
    initial_all_dtype_database, initial_has_pickle, initial_no_pickle,
    )
import unittest

class SqlFuncUnittest(unittest.TestCase):
    """:module:`sqlite4dummy.func` 的单元测试。
    """
    def setUp(self):
        self.metadata, self.table, self.engine = initial_all_dtype_database(
                                                            needdata=True)
        
    def test_count(self):
        """测试count函数。
        """
        table = self.table
        sel = Select([func.count(table.c._id)]).where(table.c._int >= 500)
        print("{:=^100}".format("SELECT COUNT"))
        print(sel.sql)

        for row in self.engine.select_row(sel):
            print(row.data)
            
    def test_max(self):
        """测试max函数。
        """
        table = self.table
        sel = Select([func.max(table.c._int), table.c._real])
        print("{:=^100}".format("SELECT MAX"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)

    def test_min(self):
        """测试min函数。
        """
        table = self.table
        sel = Select([func.min(table.c._int), table.c._real])
        print("{:=^100}".format("SELECT MIN"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)

    def test_abs(self):
        """测试abs函数。
        """
        table = self.table
        sel = Select([table.c._id, func.abs(table.c._real)]).limit(3)
        print("{:=^100}".format("SELECT ABS"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            
    def test_length(self):
        """测试length函数。
        """
        table = self.table
        sel = Select([table.c._id, func.length(table.c._text)]).limit(3)
        print("{:=^100}".format("SELECT LENGTH"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)

    def test_lower(self):
        """测试lower函数。
        """
        table = self.table
        sel = Select([table.c._id, func.lower(table.c._text)]).limit(3)
        print("{:=^100}".format("SELECT LOWER"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)

    def test_upper(self):
        """测试upper函数。
        """
        table = self.table
        sel = Select([table.c._id, func.upper(table.c._text)]).limit(3)
        print("{:=^100}".format("SELECT UPPER"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            
if __name__ == "__main__":
    unittest.main()