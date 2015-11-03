#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试与 :class:`sqlite4dummy.schema.Delete` 有关的功能


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
from sqlite4dummy.tests.basetest import *
import unittest

# class DeleteUnittest(AdvanceUnittest):
#     """Unittest of :class:`sqlite4dummy.schema.Delete`.
#     """  
#     def test_delete_all_sql(self):
#         print("{:=^100}".format("delete all formatted sql:"))
#         print(self.all_type.delete().sql)
#         print(self.has_pk.delete().sql)
#         print(self.non_pk.delete().sql)
#         pass
#     
#     def test_delete_where_sql(self):
#         print("{:=^100}".format("delete where formatted sql:"))
#         print(self.all_type.delete().\
#               where(self.all_type.c._id >= 500,
#                     self.all_type.c._int <= 500,
#                     self.all_type.c._float >= 0.5,).sql)
#         pass
#
#     def test_delete_all(self):
#         # see how many records in database at beginning
#         self.assertEqual(
#             self.engine.execute(
#                 "SELECT COUNT(*) FROM (SELECT * FROM %s)" % self.all_type).\
#                 fetchone()[0], 
#             1000,
#         )
#          
#         # delete 300 records (_id from 701 to 1000)
#         self.engine.execute(self.all_type.delete().\
#                             where(self.all_type.c._id >= 701).sql)
#          
#         # see if we still got 700 results
#         self.assertEqual(
#             self.engine.execute(
#                 "SELECT COUNT(*) FROM (SELECT * FROM %s)" % self.all_type).\
#                 fetchone()[0], 
#             700,
#         )
        
if __name__ == "__main__":
    unittest.main()