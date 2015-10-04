#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本例用于解释三种常用的INSERT和UPDATE混用的操作: 
INSERT OR REPLACE, UPSERT, INSDATE。

**INSERT OR REPLACE**

尝试进行insert, 如果数据主键冲突, 那么则update除主键以外的所有的值。
问题: 当被insert的不是所有的列时, 原数据中的其他列会被删除, 并被重置为default值。

**UPSERT**

尝试进行update, 如果数据不存在, 那么则insert一条进去。
问题: 被update的可能仅仅只有几列, 有可能用户的原意是只修改其中几列。但由于如果
被update的行没有被找到, 那么被insert的也仅仅是几列。所以当其他列有NOT NULL限制
时, 会抛出异常。

**INSDATE**

尝试进行insert, 如果数据主键冲突, 那么则update除主键以外的其他数据。

问题: 当被insert的不是所有的列时, 只会更新insert的相关列, 其他列的值会被保留。
如果被update的值跟constrain有冲突, 则会抛出异常。
"""

from __future__ import print_function
import sqlite3
import unittest

class Unittest(unittest.TestCase):
    def setUp(self):
        self.connect = sqlite3.connect(":memory:")
        self.cursor = self.connect.cursor()
        
    def tearDown(self):
        self.connect.close()
if __name__ == "__main__":
    unittest.main()