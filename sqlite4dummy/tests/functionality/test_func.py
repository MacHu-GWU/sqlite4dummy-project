#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与SQL函数有关的功能


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
from sqlite4dummy.tests.basetest import *
import unittest

class SqlFuncUnittest(AdvanceUnittestHasData):
    """Unittest of :mod:`sqlite4dummy.func`.
    """        
    def test_count(self):
        """测试count函数。
        """
        table = self.all_type
        sel = Select([func.count(table.c._id)]).where(table.c._int >= 500)
        print("{:=^100}".format("SELECT COUNT"))
        print(sel.sql)

        for row in self.engine.select_row(sel):
            print(row.data)
            break
        
    def test_max(self):
        """测试max函数。
        """
        table = self.all_type
        sel = Select([func.max(table.c._int), table.c._float])
        print("{:=^100}".format("SELECT MAX"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break
        
    def test_min(self):
        """测试min函数。
        """
        table = self.all_type
        sel = Select([func.min(table.c._int), table.c._float])
        print("{:=^100}".format("SELECT MIN"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break
        
    def test_min(self):
        """测试round函数。
        """
        table = self.all_type
        sel = Select([table.c._int, func.round(table.c._float)])
        print("{:=^100}".format("SELECT ROUND"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break
    
    def test_abs(self):
        """测试abs函数。
        """
        table = self.all_type
        sel = Select([table.c._id, func.abs(table.c._float)]).limit(3)
        print("{:=^100}".format("SELECT ABS"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break
            
    def test_length(self):
        """测试length函数。
        """
        table = self.all_type
        sel = Select([table.c._id, func.length(table.c._str)]).limit(3)
        print("{:=^100}".format("SELECT LENGTH"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break

    def test_lower(self):
        """测试lower函数。
        """
        table = self.all_type
        sel = Select([table.c._id, func.lower(table.c._str)]).limit(3)
        print("{:=^100}".format("SELECT LOWER"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break

    def test_upper(self):
        """测试upper函数。
        """
        table = self.all_type
        sel = Select([table.c._id, func.upper(table.c._str)]).limit(3)
        print("{:=^100}".format("SELECT UPPER"))
        print(sel.sql)
        
        for row in self.engine.select_row(sel):
            print(row.data)
            break
            
if __name__ == "__main__":
    unittest.main()