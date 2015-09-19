#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
English Doc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Chinese Doc (中文文档)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

sqlite3中所支持的, 以及我们常用到的数据类型有:

- :class:`TEXT`: 字符串
- :class:`INTEGER`: 整数 (sqlite3不支持布尔值, 用整数的0和1代替)
- :class:`REAL`: 小数
- :class:`BLOB`: bytes, 字节码
- :class:`DATE`: 日期
- :class:`DATETIME`: 日期时间
- :class:`PICKLETYPE`: 任意 
  `pickable <https://docs.python.org/2/library/pickle.html#what-can-be-pickled-and-unpickled>`_ 
  的Python对象。

由于我们定义了一个这些所有 :class:`DataType` 的实例, 所以访问这些数据类型是, 可以
直接使用::

    >>> from sqlite4dummy import * # dtype been imported
    >>> dtype.TEXT
    TEXT
    >>> dtype.DATETIME
    DATETIME


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from datetime import datetime, date
import binascii
import pickle
import sys

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    pk_protocol = 2
else:
    pk_protocol = 3
    
def bytestr2hexstring(bytestr):
    """Convert byte string to hex string.
    
    Example::
    
        >>> bytestr2hexstring(b'\x80\x03]q\x00(K\x01K\x02K\x03e.')
        X'80035d7100284b014b024b03652e'
    """
    res = list()
    for i in bytestr:
        res.append(str(hex(i))[2:].zfill(2))
    return "".join(res)

class BaseDataType():
    """DataType Base Class.
    
    **中文文档 **
    
    所有数据类型的父类。
    """
    def __init__(self):
        self.name = self.__class__.__name__
        
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return "%s()" % self.name
    
    def to_sql_param(self, value):
        """
        
        **中文文档**
        
        将值根据数据类型, 转化成sql可识别的字符形式。
        
        例如::
        
            "Hello World" => "'Hello World'"
        """
        return repr(value)
    
    def from_sql_param(self, text):
        """
        
        **中文文档**
        
        根据sql中的字符串, 根据数据类型, 解析出原始值。
        
        例如::
        
            "'2010-01-01'" => date(2010, 1, 1)
        """
        if text == None:
            return None
        else:
            return eval(text)
    
# sqlite3 built-in data type

class TEXT(BaseDataType):
    """UTF-8 string type.
    """
    sqlite_name = "TEXT"

    def to_sql_param(self, value):
        return "'%s'" % value.replace("'", "''").replace('"', '\"')
    
class INTEGER(BaseDataType):
    """Integer type.
    """
    sqlite_name = "INTEGER"
    
class REAL(BaseDataType):
    """Float type.
    """
    sqlite_name = "REAL"

class BLOB(BaseDataType):
    """Binary type.
    """
    sqlite_name = "BLOB"

    def to_sql_param(self, value):
        return "X%s" % str(binascii.hexlify(value))[1:]

    def from_sql_param(self, text):
        if text == None:
            return None
        else:
            return binascii.unhexlify(text[2:-1])

class DATE(BaseDataType):
    """datetime.date type.
    """
    sqlite_name = "DATE"
    
    def to_sql_param(self, value):
        return "'%s'" % value
    
    def from_sql_param(self, text):
        if text == None:
            return None
        else:
            return datetime.strptime(eval(text), "%Y-%m-%d").date()
    
class DATETIME(BaseDataType):
    """datetime.datetime type
    """
    sqlite_name = "TIMESTAMP"

    def to_sql_param(self, value):
        return "'%s'" % value

    def from_sql_param(self, text):
        if text == None:
            return None
        else:
            return datetime.strptime(eval(text), "%Y-%m-%d %H:%M:%S.%f")
    
class PICKLETYPE(BaseDataType):
    """Any picklable python objects. 
    
    Use pickle.dumps(), pickle.loads() as database IO interface.

    **中文文档**

    任何可被pickle序列化的Python对象。

    """
    sqlite_name = "BLOB"

    def to_sql_param(self, value):
        return "X%s" % str(
            binascii.hexlify(pickle.dumps(value, protocol=pk_protocol))
            )[1:]
        
    def from_sql_param(self, text):
        if text == None:
            return None
        else:
            return pickle.loads(binascii.unhexlify(text[2:-1]))
    
class DataType():
    """A DataType container class. 
    
    So dtype instance can be visit by::
    
        >>> dtype.TEXT
        TEXT
        >>> dtype.DATETIME
        DATETIME
    """
    def __init__(self):
        self.TEXT = TEXT()
        self.INTEGER = INTEGER()
        self.REAL = REAL()
        self.BLOB = BLOB()
        self.DATE = DATE()
        self.DATETIME = DATETIME()
        self.PICKLETYPE = PICKLETYPE()
        
    def get_dtype_by_name(self, name):
        """将sqlite3数据库中存储的data type字符串映射成本模组中定义的data type.
        """
        sqlite3_name_map_to_dtype = {
            "TEXT": self.TEXT,
            "INTEGER": self.INTEGER,
            "REAL": self.REAL,
            "BLOB": self.BLOB,
            "DATE": self.DATE,
            "TIMESTAMP": self.DATETIME,
            }
        return sqlite3_name_map_to_dtype[name]
    
dtype = DataType()

if __name__ == "__main__":
    import unittest
    import sqlite3
    
    class DataTypeUnittest(unittest.TestCase):
        def test_name_space(self):
            self.assertEqual(dtype.TEXT.name, "TEXT")
            self.assertEqual(dtype.INTEGER.name, "INTEGER")
            self.assertEqual(dtype.REAL.name, "REAL")
            self.assertEqual(dtype.BLOB.name, "BLOB")
            self.assertEqual(dtype.DATE.name, "DATE")
            self.assertEqual(dtype.DATETIME.name, "DATETIME")
            self.assertEqual(dtype.PICKLETYPE.name, "PICKLETYPE")

            self.assertEqual(dtype.TEXT.sqlite_name, "TEXT")
            self.assertEqual(dtype.INTEGER.sqlite_name, "INTEGER")
            self.assertEqual(dtype.REAL.sqlite_name, "REAL")
            self.assertEqual(dtype.BLOB.sqlite_name, "BLOB")
            self.assertEqual(dtype.DATE.sqlite_name, "DATE")
            self.assertEqual(dtype.DATETIME.sqlite_name, "TIMESTAMP")
            self.assertEqual(dtype.PICKLETYPE.sqlite_name, "BLOB")
        
        def test_get_dtype_by_name(self):
            self.assertEqual(dtype.get_dtype_by_name("TEXT"), dtype.TEXT)
            self.assertEqual(dtype.get_dtype_by_name("INTEGER"), dtype.INTEGER)
            self.assertEqual(dtype.get_dtype_by_name("REAL"), dtype.REAL)
            self.assertEqual(dtype.get_dtype_by_name("BLOB"), dtype.BLOB)
            self.assertEqual(dtype.get_dtype_by_name("DATE"), dtype.DATE)
            self.assertEqual(dtype.get_dtype_by_name("TIMESTAMP"), dtype.DATETIME)
        
        def test_to_sql_param_and_from_sql_param(self):
            """测试to_sql_param方法使能能将值正确的转换成sql语句。
            """
            connect = sqlite3.connect(":memory:", 
                                      detect_types=sqlite3.PARSE_DECLTYPES)
            cursor = connect.cursor()

            record = (                
                "F-007", 1, 3.14, b"8e01ad49",
                date(2000, 1, 1), datetime(2015, 12, 31, 8, 30, 17, 123), 
                pickle.dumps([1, 2, 3]), None, None, None, None, None, None,
                )
            
            # 创建表格
            create_table_sql = \
            """
            CREATE TABLE test
            (
                _string TEXT PRIMARY KEY,
                
                _int INTEGER DEFAULT %s,
                _float REAL DEFAULT %s,
                _byte BLOB DEFAULT %s,
                _date DATE DEFAULT %s,
                _datetime TIMESTAMP DEFAULT %s,
                _pickle BLOB DEFAULT %s,
                
                _int_null INTEGER,
                _float_null REAL,
                _byte_null BLOB,
                _date_null DATE,
                _datetime_null TIMESTAMP,
                _pickle_null BLOB
            )
            """ % (
            dtype.INTEGER.to_sql_param(record[1]),
            dtype.REAL.to_sql_param(record[2]),
            dtype.BLOB.to_sql_param(record[3]),
            dtype.DATE.to_sql_param(record[4]),
            dtype.DATETIME.to_sql_param(record[5]),
            dtype.PICKLETYPE.to_sql_param([1, 2, 3]),
            )
            cursor.execute(create_table_sql)
            
            # 插入测试数据
            cursor.execute(
                "INSERT INTO test VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);", record)
            

            # 用Select选择, 看to_sql_param方法使能能将值正确的转换成sql语句
            select_sql = \
            """
            SELECT * FROM test
            WHERE
                _string = %s AND
                _int = %s AND
                _float = %s AND
                _byte = %s AND
                _date = %s AND
                _datetime = %s AND
                _pickle = %s
            """ % (
            dtype.TEXT.to_sql_param(record[0]),
            dtype.INTEGER.to_sql_param(record[1]),
            dtype.REAL.to_sql_param(record[2]),
            dtype.BLOB.to_sql_param(record[3]),
            dtype.DATE.to_sql_param(record[4]),
            dtype.DATETIME.to_sql_param(record[5]),
            dtype.PICKLETYPE.to_sql_param([1, 2, 3]),
            )

            self.assertTupleEqual(cursor.execute(select_sql).fetchone(), record)
            
            # 测试是否能从metadata中的DEFAULT值的字符串形式获得原始的default值
            res = list(cursor.execute("PRAGMA table_info(test)"))

            self.assertEqual(dtype.TEXT.from_sql_param(res[0][4]), None)
            
            self.assertEqual(dtype.INTEGER.from_sql_param(res[1][4]), record[1])
            self.assertEqual(dtype.REAL.from_sql_param(res[2][4]), record[2])
            self.assertEqual(dtype.BLOB.from_sql_param(res[3][4]), record[3])
            self.assertEqual(dtype.DATE.from_sql_param(res[4][4]), record[4])
            self.assertEqual(dtype.DATETIME.from_sql_param(res[5][4]), record[5])
            self.assertEqual(dtype.PICKLETYPE.from_sql_param(res[6][4]), [1, 2, 3])
            
            self.assertEqual(dtype.INTEGER.from_sql_param(res[7][4]), None)
            self.assertEqual(dtype.REAL.from_sql_param(res[8][4]), None)
            self.assertEqual(dtype.BLOB.from_sql_param(res[9][4]), None)
            self.assertEqual(dtype.DATE.from_sql_param(res[10][4]), None)
            self.assertEqual(dtype.DATETIME.from_sql_param(res[11][4]), None)
            self.assertEqual(dtype.PICKLETYPE.from_sql_param(res[12][4]), None)
            
    unittest.main()
    