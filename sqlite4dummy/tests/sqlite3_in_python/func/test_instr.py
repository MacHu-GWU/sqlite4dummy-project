#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
instr在sqlite 3.7.15版本被加入。目前Python33中的sqlite版本是3.7.12, 所以还不支持。

查看当前sqlite版本的方法::

    >>> import sqlite3
    >>> sqlite3.sqlite_version
    3.7.15

Ref: https://www.sqlite.org/lang_corefunc.html
"""

from sqlite4dummy.tests_sqlite3 import BaseUnittest
import unittest

class Unittest(BaseUnittest):
    def test_all(self):
        """目前还不支持instr函数。
        """
#         cursor = self.cursor
#         
#         select_sql = "SELECT instr('Hello World', 'W')"
#         print(cursor.execute(select_sql).fetchone())
        
if __name__ == "__main__":
    unittest.main()