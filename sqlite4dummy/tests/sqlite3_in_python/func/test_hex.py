#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
HEX(bytes)函数可以将字节码转化成16进制的hexstr

Ref: https://www.sqlite.org/lang_corefunc.html
"""

from sqlite4dummy.tests.basetest import BaseUnittest
import unittest
import binascii
import pickle

class Unittest(BaseUnittest):
    def setUp(self):
        self.connect_database()
        
    def test_all(self):
        """将bytes转化成hexstr。
        """
        cursor = self.cursor
        
        select_sql = "SELECT HEX(?)"
        bytes_ = pickle.dumps([1, 2, 3], protocol=2)
        record = cursor.execute(select_sql, (bytes_, )).fetchone()
        print(record[0]) # 80025D7100284B014B024B03652E, 在sqlite中是大写
        print(binascii.b2a_hex(bytes_).decode("utf-8")) # 实际上应该是小写
        
if __name__ == "__main__":
    unittest.main()