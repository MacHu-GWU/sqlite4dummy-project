#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals
import binascii
import pickle
import hashlib
import unittest
import sys

if sys.version_info[0] == 3:
    _str_type = str
    _int_types = (int,)
    pk_protocol = 3
    is_py3 = True
else:
    _str_type = basestring
    _int_types = (int, long)
    pk_protocol = 2
    is_py3 = False

class Unittest(unittest.TestCase):
    def test_what_is_unicode_str(self):
        """在Python3中, 字符串被统一为utf-8编码, 而在Python2中并不是。
        在Python2中只有声明了 ``u"abc"``, 这样才是utf-8编码。
        """
        self.assertIsInstance("中文", _str_type)
        with open("temp.txt", "wb") as f:
            f.write("中文".encode("utf-8"))
    
    def test_what_is_bytes(self):
        """bytes其实是一串010101的比特流。而在Python中被print的时候, 会根据
        utf-8编码进行转化后的打印输出。
        """
        self.assertIsInstance("Hello".encode("utf-8"), bytes)
        self.assertIsInstance(pickle.dumps([1, 2, 3]), bytes)
        
    def test_what_is_hexstr(self):
        """对于bytes, 实际上是一连串的0101的字节码。而hexstr是将字节码按每4位
        用0-f十六进制数来表示, 例如字节码:
            00011111 => 1f
            
        Python中可以做这件事的标准库是binascii, 具体的用法是这样的::
        
            >>> import binascii
            >>> binascii.b2a_hex("a".encode("utf-8")) # bytes to hexstr
            b'61' # bytes
            >>> binascii.a2b_hex("61")
            b'a' # bytes, to get the str, use b'a'.decode("utf-8")
        """
        bytes_ = "a".encode("utf-8") # b'61' = 0b01100001
        self.assertIsInstance(binascii.b2a_hex(bytes_), bytes)
        print(binascii.b2a_hex(bytes_)) # b'61'
        
        self.assertIsInstance(binascii.a2b_hex("61"), bytes)
        print(binascii.a2b_hex("61")) # b'a'
        
    def test_md5_with_hexstr(self):
        """Hash算法的本质是将一串0101的字节流映射成另一串定长的0101字节流。
        但为什么我们看到的md5码通常是一个32位的字符串呢? 那是因为md5码实际上的
        长度是128位, 每4位作为一个hexstr, 那么最后看起来就像32位的字符串了。
        """
        bytes_ = pickle.dumps([1, 2, 3], protocol=2)
        m = hashlib.md5()
        m.update(bytes_)
        
        print(m.digest()) # b'\xac\x03\xee2\xf9\xd9\xf6M%\x04\xcb\xd9>\x9179'
        print(m.hexdigest(), # ac03ee32f9d9f64d2504cbd93e913739 <class 'str'>
              type(m.hexdigest()))
        print(binascii.b2a_hex(m.digest()).decode("utf-8"), # ac03ee32f9d9f64d2504cbd93e913739 <class 'str'>
              type(binascii.b2a_hex(m.digest()).decode("utf-8")))
    
    def test_bytes_with_int(self):
        """我们知道, bytes的本质是一串字节流, 换言之可以表示为一个二进制数。那么
        我们是否可以将int和bytes自由转化呢? 方法如下::
        
            # bytes to int
            >>> int.from_bytes("a".encode("utf-8"), byteorder="big")
            97
        """
        # 该测试由于Python2对big int支持不佳, 在Python2中跑不通
        print(int.from_bytes("a".encode("utf-8"), byteorder="big")) # 97
        print(bytes([97,])) # b'a'
        
if __name__ == "__main__":
    unittest.main()