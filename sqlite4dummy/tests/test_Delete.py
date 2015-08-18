#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与Delete有关的功能


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

class DeleteUnittest(unittest.TestCase):
    def setUp(self):
        self.metadata, self.table, self.engine = initial_all_dtype_database(
                                                            needdata=True)
    
    def test_delete_all_sql(self):
        print("{:=^100}".format("delete all formatted sql:"))
        print(self.table.delete().sql)
        pass
    
    def test_delete_where_sql(self):
        print("{:=^100}".format("delete where formatted sql:"))
        print(self.table.delete().\
              where(self.table.c._id >= 500,
                    self.table.c._int <= 500,
                    self.table.c._real >= 0.5,).sql)
        pass
    
    def test_delete_all(self):
        self.assertEqual(
            self.engine.execute(
                "SELECT COUNT(*) FROM (SELECT * FROM %s)" % self.table).\
                fetchone()[0], 
            1000,
            )
        
        # delete 300 records (_id from 701 to 1000)
        self.engine.execute(self.table.delete().\
                            where(self.table.c._id >= 701).sql)

        self.assertEqual(
            self.engine.execute(
                "SELECT COUNT(*) FROM (SELECT * FROM %s)" % self.table).\
                fetchone()[0], 
            700,
            )
        
if __name__ == "__main__":
    unittest.main()